"""Snapshot the View C decay-adjusted forward projection to JSON.

Reads the active setfile, runs the deal-merge portfolio sim over the sanity
window, applies the live haircut + slope-decay factor, and saves results to
output/forward_projection.json. Daily tracker + weekly recap read this file
to compare actual live P&L against expectation.

Per [[feedback_forward_projection_view_c]] — default to decay-adjusted method.

Usage:
  python scripts/save_forward_projection.py
  python scripts/save_forward_projection.py --setfile <path> --balance <usd>
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate

from sim_portfolio_6stream import parse_setfile, extract_streams, run_stream, aggregate

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
HAIRCUT_NP = 0.94
HAIRCUT_PF = 0.25
PROJECTION_PATH = ROOT / "output" / "forward_projection.json"


def slope_to_decay(avg_slope_pct: float) -> float:
    """Map avg per-stream OOS slope to decay factor per feedback_forward_projection_view_c."""
    if avg_slope_pct >= -10:
        return 0.90
    if avg_slope_pct >= -30:
        return 0.80
    if avg_slope_pct >= -50:
        return 0.75
    return 0.65


def read_winner_slope(wfo_dir: Path) -> float | None:
    """Return slope (as decimal, e.g. -0.156) from a WFO winner_p1.json, or None."""
    p = wfo_dir / "winner_p1.json"
    if not p.exists():
        return None
    try:
        return float(json.loads(p.read_text()).get("slope"))
    except Exception:
        return None


def read_per_stream_slopes(wfo_dir: Path, stream_cfgs: list[dict]) -> dict[str, float]:
    """Match deployed stream cfgs against WFO IS+OOS parquets to find each stream's slope.

    Returns {stream_label: slope_pct}. If a parquet can't be matched, slope is None.
    """
    out = {}
    is_files = sorted(wfo_dir.glob("p1_is_W*.parquet"))
    oos_files = sorted(wfo_dir.glob("p1_oos_W*.parquet"))
    if not is_files or not oos_files or len(is_files) != len(oos_files):
        return out
    try:
        import pandas as pd
        # Build per-cell OOS NP sequence across windows
        is_df = pd.concat([pd.read_parquet(f).assign(_W=i) for i, f in enumerate(is_files)])
        oos_df = pd.concat([pd.read_parquet(f).assign(_W=i) for i, f in enumerate(oos_files)])
    except Exception:
        return out

    cfg_keys = ["range_minutes", "fixed_sl_pts", "rr_ratio", "half_tp_ratio"]
    for s in stream_cfgs:
        match_cols = {"range_minutes": s["range_min"], "fixed_sl_pts": s["fixed_sl"],
                       "rr_ratio": s["rr"], "half_tp_ratio": s["htp"]}
        # Filter OOS rows for this stream's cell across windows; compute slope from window-NP sequence
        sub = oos_df
        for k, v in match_cols.items():
            if k in sub.columns:
                sub = sub[sub[k] == v]
        if sub.empty:
            out[s["label"]] = None
            continue
        # Per-window NP
        np_col = "np" if "np" in sub.columns else ("net_profit" if "net_profit" in sub.columns else None)
        if np_col is None:
            out[s["label"]] = None
            continue
        windows = sub.groupby("_W")[np_col].sum().sort_index().values
        if len(windows) < 2 or windows[0] == 0:
            out[s["label"]] = None
            continue
        # Slope = (last - first) / first (per the WFO method)
        slope_dec = (windows[-1] - windows[0]) / abs(windows[0])
        out[s["label"]] = slope_dec * 100  # pct
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setfile", default="configs/sets/dt818_pro_v2.1_9pct_may16_may9.set")
    ap.add_argument("--start", default="2026-03-14")
    ap.add_argument("--end", default="2026-05-02")
    ap.add_argument("--spread", type=int, default=60)
    ap.add_argument("--balance", type=float, default=None,
                    help="Live account balance for scaling. Default = auto-detect via MT5.")
    ap.add_argument("--prev-wfo", default="output/wfo_orb_may9",
                    help="Source WFO dir for S1-S3 (avg slope)")
    ap.add_argument("--curr-wfo", default="output/wfo_orb_may16",
                    help="Source WFO dir for S4-S6 (avg slope)")
    args = ap.parse_args()

    setpath = ROOT / args.setfile
    cfg = parse_setfile(setpath)
    streams = extract_streams(cfg)
    risk = float(cfg["_RiskPct"])

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)

    # Detect live balance if not provided
    balance = args.balance
    if balance is None:
        try:
            import MetaTrader5 as mt5
            if mt5.initialize():
                ai = mt5.account_info()
                if ai is not None:
                    balance = float(ai.balance)
                mt5.shutdown()
        except Exception:
            pass
    if balance is None:
        print("[warn] Could not auto-detect balance; defaulting to $10k baseline.")
        balance = DEPOSIT
    mult = balance / DEPOSIT

    # Per-stream slopes from the WFO parquets (matches DEPLOYED ranks, not just rank-1 winners).
    s_prev = [s for s in streams if int(s["magic"]) in (1111, 2222, 3333)]
    s_curr = [s for s in streams if int(s["magic"]) in (4444, 5555, 6666)]
    prev_slopes = read_per_stream_slopes(ROOT / args.prev_wfo, s_prev)
    curr_slopes = read_per_stream_slopes(ROOT / args.curr_wfo, s_curr)
    per_stream_slopes = {**prev_slopes, **curr_slopes}
    valid = [v for v in per_stream_slopes.values() if v is not None]
    if valid:
        avg_slope_pct = sum(valid) / len(valid)
    else:
        # Fallback: avg the two rank-1 winner slopes
        prev_slope = read_winner_slope(ROOT / args.prev_wfo)
        curr_slope = read_winner_slope(ROOT / args.curr_wfo)
        rk1 = [s for s in (prev_slope, curr_slope) if s is not None]
        avg_slope_pct = (sum(rk1) / len(rk1)) * 100 if rk1 else -40.0
    decay = slope_to_decay(avg_slope_pct)
    combined = HAIRCUT_NP * decay

    print("=" * 100)
    print(f"  SAVE FORWARD PROJECTION (View C)")
    print(f"  Setfile: {setpath.name}   Streams: {len(streams)}   Per-stream risk: {risk:.2f}%")
    print(f"  Live balance: ${balance:,.2f}   Multiplier vs ${DEPOSIT:,.0f} sim: {mult:.2f}x")
    print(f"  Per-stream slopes: {per_stream_slopes if per_stream_slopes else '(fallback to rank-1)'}")
    print(f"  Avg per-stream OOS slope:        {avg_slope_pct:+.1f}%  -> decay factor {decay:.2f}")
    print(f"  Combined haircut (live x decay): {combined:.3f}")
    print("=" * 100)

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        ticks = load_ticks(SYMBOL, start, end, spread_pts=args.spread)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)

        deals = []
        for s in streams:
            r = run_stream(s, risk, ticks, m1, m5, meta)
            for d in r.deals:
                if d.kind == "entry":
                    continue
                deals.append((d.ts, s["label"], d.pnl))

        agg = aggregate(deals)

        # Weekly stats
        weekly = defaultdict(float)
        for ts, _s, p in deals:
            wk = ts.isocalendar()
            weekly[(wk.year, wk.week)] += p
        week_pnls = sorted(weekly.values())
        n_weeks = len(week_pnls)
        weeks_green = sum(1 for p in week_pnls if p > 0)
        wk_mean = sum(week_pnls) / n_weeks if n_weeks else 0
        wk_med = week_pnls[n_weeks // 2] if n_weeks else 0
        wk_p10 = week_pnls[max(0, int(n_weeks * 0.10))] if n_weeks else 0
        wk_p90 = week_pnls[min(n_weeks - 1, int(n_weeks * 0.90))] if n_weeks else 0
        wk_best = week_pnls[-1] if n_weeks else 0
        wk_worst = week_pnls[0] if n_weeks else 0
        wk_std = ((sum((p - wk_mean) ** 2 for p in week_pnls) / n_weeks) ** 0.5) if n_weeks else 0

        def scale(x):
            return x * combined * mult

        proj = {
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "setfile": setpath.name,
            "method": "view_c_decay_adjusted",
            "deposit_sim": DEPOSIT,
            "baseline_balance": balance,
            "balance_multiplier": mult,
            "spread_pts": args.spread,
            "decay_factor": decay,
            "live_haircut_np": HAIRCUT_NP,
            "combined_haircut": combined,
            "avg_oos_slope_pct": avg_slope_pct,
            "per_stream_slopes_pct": per_stream_slopes,
            "sanity_window": {"start": args.start, "end": args.end,
                              "iso_weeks": n_weeks},
            "raw_sim": {
                "portfolio_np": agg["np"], "dd_pct": agg["dd_pct"], "dd_abs": agg["dd_abs"],
                "ndd": agg["ndd"], "pf": agg["pf"], "trades": agg["trades"],
                "wr": agg["wr"],
            },
            "weekly_live": {
                "mean_np": scale(wk_mean), "median_np": scale(wk_med),
                "p10_np": scale(wk_p10), "p90_np": scale(wk_p90),
                "worst_np": scale(wk_worst), "best_np": scale(wk_best),
                "std_np": wk_std * combined * mult,
                "green_week_prob": weeks_green / n_weeks if n_weeks else 0,
                "expected_roi_pct": (scale(wk_mean) / balance * 100) if balance > 0 else 0,
                "n_sample_weeks": n_weeks,
            },
            "daily_live": {
                # 5 trading days/week
                "mean_np": scale(wk_mean) / 5.0,
                "p10_np": scale(wk_p10) / 5.0,
                "p90_np": scale(wk_p90) / 5.0,
                "expected_roi_pct": (scale(wk_mean) / 5.0 / balance * 100) if balance > 0 else 0,
            },
            "tolerance": {
                # Trigger investigation if observed weekly NP worse than this
                "single_week_red_usd": scale(wk_worst) * 1.5,
                "consecutive_red_weeks": 2,
            },
        }

        PROJECTION_PATH.parent.mkdir(parents=True, exist_ok=True)
        PROJECTION_PATH.write_text(json.dumps(proj, indent=2))
        print(f"\n  Saved projection -> {PROJECTION_PATH}")

        # Push to dt818-console (fails-open if .env unconfigured).
        try:
            from zgb_sim.cf_publish import publish_projection
            if publish_projection(proj):
                print(f"  Published to dt818-console.")
        except Exception as e:
            print(f"  [cf_publish] skipped: {type(e).__name__}: {e}")
        print(f"\n  Weekly LIVE expectation: mean ${proj['weekly_live']['mean_np']:+,.0f}"
              f"  (range ${proj['weekly_live']['p10_np']:+,.0f}"
              f"  ..  ${proj['weekly_live']['p90_np']:+,.0f})")
        print(f"  Daily  LIVE expectation: mean ${proj['daily_live']['mean_np']:+,.0f}"
              f"  ROI: {proj['daily_live']['expected_roi_pct']:+.2f}%")
        print(f"  Investigation trigger:   single week worse than ${proj['tolerance']['single_week_red_usd']:+,.0f}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
