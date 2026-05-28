"""Compare 'top-6 from latest WFO' vs 'top-3 prev + top-3 latest WFO' on
multiple clean forward-test windows using v7 architecture (live-relevant).

Forward windows tested (each window is OUT-OF-SAMPLE for BOTH setfile-selection
strategies — neither WFO's training/OOS included the test data):
  W_FWD1: 2026-05-09 -> 2026-05-16   pair = (may2, may9)
  W_FWD2: 2026-05-16 -> 2026-05-23   pair = (may9, may16)
  W_FWD3: 2026-05-23 -> now          pair = (may16, may23)

For each pair (prev_wfo, latest_wfo):
  NO_ROT setfile: top-6 from latest_wfo
  ROT    setfile: top-3 from prev_wfo + top-3 from latest_wfo (filling S1-3 and S4-6)

Each setfile is sim'd with v7 architecture:
  - fractal_confirm=True, fractal_width=5
  - sma_cross_exit=True, sma_fast=8, sma_slow=21
  - STOP-on-extension hedge (ExtPts=100, TPMult=3.0, SLMult=1.0)

Portfolio is deal-merged across 6 streams on shared $10k balance @ 1.5%/stream.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone, date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast
from zgb_sim.wfo_helpers import (WINDOWS_MAY2, WINDOWS_MAY9,
                                  WINDOWS_MAY16, WINDOWS_MAY23,
                                  rank_with_p0)
from zgb_sim.tick_loader import kill_mt5_terminal
from sim_orb_oos_today import fetch_window, fetch_meta
from sim_wfo_hedge_reverse import StopExtensionCfg, simulate_stop_extension_hedges
from sim_orb_oos_today_hedge_v6 import extract_sl_events, ts_arr_from_ticks


DEPOSIT = 10_000.0
SPREAD = 30
RISK_PCT = 1.5      # per stream (× 6 = 9% total)

# v7 architecture (live-relevant for forward projection)
V7_FEATURES = dict(
    fractal_confirm=True, fractal_width=5,
    sma_cross_exit=True, sma_cross_fast=8, sma_cross_slow=21,
)
HEDGE_CFG = StopExtensionCfg(exp_min=240, f1_sec=1800,
                              ext_pts=100, tp_mult=3.0, sl_mult=1.0)

WFOS = {
    "may2":  ("output/wfo_orb_may2",  WINDOWS_MAY2),
    "may9":  ("output/wfo_orb_may9",  WINDOWS_MAY9),
    "may16": ("output/wfo_orb_may16", WINDOWS_MAY16),
    "may23": ("output/wfo_orb_v2_may23", WINDOWS_MAY23),
}


def row_to_cfg(row) -> ORBConfig:
    """Build minimal ORBConfig from a parquet row (parent dims only)."""
    return ORBConfig(
        risk_pct=RISK_PCT,
        range_minutes=int(row["range_minutes"]),
        buffer_pts=0,
        min_range_pts=0, max_range_pts=999_999,
        fixed_sl_pts=int(row["fixed_sl_pts"]),
        rr_ratio=float(row["rr_ratio"]),
        half_tp_ratio=round(float(row["half_tp_ratio"]), 2),
        pending_expire_minutes=int(row["pending_expire_minutes"]),
        daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13,
        comment="ORB",
    )


def rerank_wfo(label: str) -> list[ORBConfig]:
    """Re-rank a WFO's parquets via rank_with_p0 → return top-N configs."""
    rel_dir, windows = WFOS[label]
    wfo_dir = ROOT / rel_dir
    is_per = {}; oos_per = {}
    for wname, _, _, _, _ in windows:
        # Try p1_-prefixed naming first (newer WFOs), fall back to bare (may2 era)
        is_path = wfo_dir / f"p1_is_{wname}.parquet"
        oos_path = wfo_dir / f"p1_oos_{wname}.parquet"
        if not is_path.exists():
            is_path = wfo_dir / f"is_{wname}.parquet"
        if not oos_path.exists():
            oos_path = wfo_dir / f"oos_{wname}.parquet"
        if not is_path.exists() or not oos_path.exists():
            raise RuntimeError(f"missing IS/OOS parquets for {wname} in {wfo_dir.name}")
        is_per[wname] = pd.read_parquet(is_path)
        oos_per[wname] = pd.read_parquet(oos_path)
    candidates = [row_to_cfg(r) for _, r in oos_per[windows[0][0]].iterrows()]
    full_grid = [row_to_cfg(r) for _, r in is_per[windows[0][0]].iterrows()]
    ranked = rank_with_p0(candidates, oos_per, windows, decay_threshold=-0.25,
                           grid_configs=full_grid, is_per_window=is_per)
    return [info["cfg"] for info in ranked]


def apply_v7_features(cfgs: list[ORBConfig]) -> list[ORBConfig]:
    """Stamp v7 sim flags onto each parent cfg (mutates copies)."""
    out = []
    for c in cfgs:
        c2 = ORBConfig(
            risk_pct=c.risk_pct,
            range_minutes=c.range_minutes, buffer_pts=0,
            min_range_pts=0, max_range_pts=999_999,
            fixed_sl_pts=c.fixed_sl_pts, rr_ratio=c.rr_ratio,
            half_tp_ratio=c.half_tp_ratio,
            pending_expire_minutes=c.pending_expire_minutes,
            daily_target_pct=0.0, daily_loss_pct=0.0,
            ldn_enabled=True, ldn_start_hour=7,
            ny_enabled=True, ny_start_hour=13,
            **V7_FEATURES,
            comment="ORB",
        )
        out.append(c2)
    return out


def run_portfolio_on_window(cfgs6: list[ORBConfig], ticks, m1, m5, meta, ticks_arr) -> dict:
    """Run all 6 parent cfgs + hedges on a shared $10k account, deal-merged."""
    deals = []
    per_stream = []
    for sn, cfg in enumerate(cfgs6, start=1):
        r = simulate_fast(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        parent_pnls, sl_events = extract_sl_events(r.deals)
        parent_np = sum(p for _, p in parent_pnls)
        for ts, pnl in parent_pnls:
            deals.append((ts, sn, pnl))
        stream_meta = {
            "magic": sn, "risk_pct": RISK_PCT,
            "fixed_sl_pts": cfg.fixed_sl_pts, "rr_ratio": cfg.rr_ratio,
            "half_tp_ratio": cfg.half_tp_ratio,
            "range_minutes": cfg.range_minutes,
            "pending_expire_minutes": cfg.pending_expire_minutes,
        }
        hedge_pnls_ns = simulate_stop_extension_hedges(sl_events, ticks_arr, stream_meta, HEDGE_CFG)
        hedge_np = sum(p for _, p in hedge_pnls_ns)
        for ts_ns, pnl in hedge_pnls_ns:
            deals.append((int(ts_ns), sn, pnl))
        per_stream.append({
            "stream": f"S{sn}",
            "params": f"R{cfg.range_minutes} SL{cfg.fixed_sl_pts} RR{cfg.rr_ratio} HTP{cfg.half_tp_ratio} E{cfg.pending_expire_minutes}",
            "parent_np": parent_np, "hedge_np": hedge_np,
            "combined": parent_np + hedge_np,
            "parent_trades": len(parent_pnls), "hedge_n": len(hedge_pnls_ns),
        })

    # Deal-merge
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gains = losses = 0.0; wins = 0
    deals.sort(key=lambda x: x[0])
    for _, _sn, p in deals:
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p >= 0: gains += p; wins += 1
        else: losses += -p
    np_ = bal - DEPOSIT
    pf = gains / losses if losses > 0 else float("inf")
    ndd = np_ / dd_abs if dd_abs > 0 else 0.0
    return {
        "np": np_, "dd": dd_abs, "pf": pf, "ndd": ndd,
        "trades": len(deals), "wins": wins,
        "per_stream": per_stream,
    }


# Forward test windows
TESTS = [
    # (label, test_window_start, test_window_end, prev_wfo, latest_wfo)
    ("W_FWD1: 5-9 → 5-16",  date(2026, 5, 9),  date(2026, 5, 16), "may2",  "may9"),
    ("W_FWD2: 5-16 → 5-23", date(2026, 5, 16), date(2026, 5, 23), "may9",  "may16"),
    ("W_FWD3: 5-23 → now",  date(2026, 5, 23), date(2026, 5, 27), "may16", "may23"),
]


def main():
    # Pre-rank each WFO once
    print("Re-ranking WFO outputs...")
    top6_by_wfo = {}
    for label in WFOS:
        try:
            top6_by_wfo[label] = rerank_wfo(label)[:6]
            print(f"  {label}: top-6 OK ({len(top6_by_wfo[label])} candidates)")
        except Exception as e:
            print(f"  {label}: FAILED — {e}")
            top6_by_wfo[label] = []

    # Tick loader prep
    sym, m = fetch_meta(None, account="live")
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])

    results = []
    for label, ws, we, prev_k, latest_k in TESTS:
        start = datetime(ws.year, ws.month, ws.day, tzinfo=timezone.utc)
        end = datetime(we.year, we.month, we.day, tzinfo=timezone.utc)
        if we >= date(2026, 5, 27):
            end = datetime.now(timezone.utc)
        prev_top6 = top6_by_wfo.get(prev_k, [])
        latest_top6 = top6_by_wfo.get(latest_k, [])
        if len(prev_top6) < 3 or len(latest_top6) < 6:
            print(f"\n[{label}] SKIP — need ≥3 prev + ≥6 latest, got "
                  f"{len(prev_top6)} prev + {len(latest_top6)} latest")
            continue

        # Build two cfg sets
        no_rot_cfgs = apply_v7_features(latest_top6[:6])
        rot_cfgs = apply_v7_features(prev_top6[:3] + latest_top6[:3])

        # Fetch ticks once per window
        print(f"\n[{label}] {start.date()} -> {end.date()}  pair=({prev_k},{latest_k})")
        sym, ticks, m1, m5 = fetch_window(sym, start, end, SPREAD, account="live")
        ticks_arr = ts_arr_from_ticks(ticks)

        r_no_rot = run_portfolio_on_window(no_rot_cfgs, ticks, m1, m5, meta, ticks_arr)
        r_rot = run_portfolio_on_window(rot_cfgs, ticks, m1, m5, meta, ticks_arr)
        results.append((label, prev_k, latest_k, r_no_rot, r_rot,
                        no_rot_cfgs, rot_cfgs))

    kill_mt5_terminal()

    # ---------- REPORT ----------
    print()
    print("=" * 115)
    print(f"  ROTATION vs NO-ROTATION — clean forward-OOS portfolio comparison (v7 architecture)")
    print(f"  $10k base, 9% risk (1.5%/stream × 6), 30pt spread, deal-merged across 6 streams")
    print("=" * 115)
    print(f"  {'Test window':<24} {'pair (prev,latest)':<22} "
          f"{'NO-ROT NP':>10} {'ROT NP':>10} {'Δ (rot − norot)':>16} "
          f"{'NO-ROT NDD':>11} {'ROT NDD':>9} {'Verdict':<14}")
    print("  " + "-" * 111)
    for label, pk, lk, no_rot, rot, _, _ in results:
        delta = rot["np"] - no_rot["np"]
        if delta > 0:
            verdict = "ROT wins"
        elif delta < 0:
            verdict = "NO-ROT wins"
        else:
            verdict = "tied"
        print(f"  {label:<24} ({pk},{lk}){'':<{max(0,18-len(pk)-len(lk)-3)}} "
              f"${no_rot['np']:>+8,.0f} ${rot['np']:>+8,.0f} ${delta:>+14,.0f} "
              f"{no_rot['ndd']:>11.2f} {rot['ndd']:>9.2f}  {verdict:<14}")
    print("  " + "-" * 111)
    print()

    # Sum across windows (aggregate verdict)
    total_no_rot = sum(r[3]["np"] for r in results)
    total_rot = sum(r[4]["np"] for r in results)
    wins_no_rot = sum(1 for r in results if r[3]["np"] > r[4]["np"])
    wins_rot = sum(1 for r in results if r[4]["np"] > r[3]["np"])
    print(f"  AGGREGATE (sum of all forward windows):")
    print(f"    NO-ROT total NP: ${total_no_rot:+,.2f}")
    print(f"    ROT    total NP: ${total_rot:+,.2f}")
    print(f"    Delta            ${total_rot - total_no_rot:+,.2f}  "
          f"({'ROT wins' if total_rot > total_no_rot else ('NO-ROT wins' if total_no_rot > total_rot else 'tied')} on total)")
    print(f"    Window-wins      NO-ROT={wins_no_rot}  ROT={wins_rot}")
    print()

    # Per-window setfile composition
    print("=" * 115)
    print(f"  SETFILE COMPOSITION per test window")
    print("=" * 115)
    for label, pk, lk, _, _, no_rot_cfgs, rot_cfgs in results:
        print(f"\n  [{label}]  pair=({pk},{lk})")
        print(f"    NO-ROT (top-6 from {lk}):")
        for i, c in enumerate(no_rot_cfgs, 1):
            print(f"      S{i}: R{c.range_minutes:>3} SL{c.fixed_sl_pts:>4} "
                  f"RR{c.rr_ratio:<4} HTP{c.half_tp_ratio:<4} E{c.pending_expire_minutes:>3}")
        print(f"    ROT (top-3 from {pk} + top-3 from {lk}):")
        for i, c in enumerate(rot_cfgs, 1):
            src = pk if i <= 3 else lk
            print(f"      S{i}: R{c.range_minutes:>3} SL{c.fixed_sl_pts:>4} "
                  f"RR{c.rr_ratio:<4} HTP{c.half_tp_ratio:<4} E{c.pending_expire_minutes:>3}  ({src})")
    print("=" * 115)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
