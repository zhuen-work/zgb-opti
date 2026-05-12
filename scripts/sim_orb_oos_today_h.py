"""OOS today for v2.1_h: parent + retry-hedge.

Uses live ticks for today, runs 6 parent streams + 6 retry hedges with the
deployed v2.1_h 9pct setfile config:
  exp_min=720, f1_sec=0 (disabled), per-stream tp_mult.
"""
from __future__ import annotations
import sys
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import pandas as pd

from sim_orb_oos_today import (PREV_WFO_DIR, CURR_WFO_DIR, SPREAD_LIVE, DEPOSIT,
                                fetch_meta, fetch_window, row_to_cfg)
from sim_wfo_hedge_retry import aggregate  # returns (np, dd_pct, pf) tuple
from zgb_sim.wfo_helpers import WINDOWS_MAY2, WINDOWS_MAY9, rank_with_p0
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast as orb_simulate

from sim_wfo_hedge_retry import (STREAM_CFGS, RetryHedgeCfg,
                                  simulate_retry_hedges, ts_arr_from_ticks,
                                  run_baseline_window)

# Production hedge config (from sim_wfo_hedge_retry_global.py winner, F1=0)
HEDGE_EXP_MIN = 720
HEDGE_F1_SEC = 0
TP_MULT_BY_STREAM = {"S1": 1.0, "S2": 0.75, "S3": 0.75,
                     "S4": 1.0, "S5": 0.75, "S6": 0.75}

# Window: May 11 → now (3-day window matching live deployment span)
end = datetime.now(timezone.utc)
start = datetime(2026, 5, 11, tzinfo=timezone.utc)


def load_top3(wfo_dir, windows):
    def _read(label):
        for prefix in ("", "p1_"):
            p_is = wfo_dir / f"{prefix}is_{label}.parquet"
            p_oos = wfo_dir / f"{prefix}oos_{label}.parquet"
            if p_is.exists() and p_oos.exists():
                return pd.read_parquet(p_is), pd.read_parquet(p_oos)
        raise FileNotFoundError(label)
    is_per, oos_per = {}, {}
    for label, _, _, _, _ in windows:
        is_per[label], oos_per[label] = _read(label)
    cands = [row_to_cfg(r, "ORB", 3.0) for _, r in oos_per["W1"].iterrows()]
    grid = [row_to_cfg(r, "ORB", 3.0) for _, r in is_per["W1"].iterrows()]
    ranked = rank_with_p0(cands, oos_per, windows, decay_threshold=-0.25,
                          grid_configs=grid, is_per_window=is_per)
    return [ranked[i]["cfg"] for i in range(3)]


def main() -> int:
    prev = load_top3(PREV_WFO_DIR, WINDOWS_MAY2)
    curr = load_top3(CURR_WFO_DIR, WINDOWS_MAY9)
    labels = ["S1", "S2", "S3", "S4", "S5", "S6"]
    cfgs_base = prev + curr

    # Map labels → STREAM_CFGS keys (used by simulate_retry_hedges to read fixed_sl/rr)
    # STREAM_CFGS already has S1..S6 keyed; map by ordering
    days = (end - start).days

    print("=" * 100)
    print(f"  v2.1_h OOS today  |  {start.date()} -> {end.strftime('%Y-%m-%d %H:%M UTC')}  ({days}d)")
    print(f"  Hedge cfg: exp_min={HEDGE_EXP_MIN}, F1=disabled, per-stream tp_mult")
    print(f"  Spread {SPREAD_LIVE}pt, $10k, 9% total / 6 = 1.5% per parent stream")
    for lbl, c in zip(labels, cfgs_base):
        tp_m = TP_MULT_BY_STREAM[lbl]
        print(f"    {lbl}: R={c.range_minutes} SL={c.fixed_sl_pts} RR={c.rr_ratio} "
              f"HTP={c.half_tp_ratio}  hedge_tp_mult={tp_m}")
    print("=" * 100)

    try:
        sym_used, m = fetch_meta(None, account="live")
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        sym_used, ticks, m1, m5 = fetch_window(sym_used, start, end, SPREAD_LIVE, account="live")
        print(f"  Symbol: {sym_used}  Ticks: {len(ticks):,}  M1={len(m1):,}  M5={len(m5):,}")
        t_arr = ts_arr_from_ticks(ticks)

        per_stream_risk = 9.0 / 6   # 1.5% per stream at 9% total

        print(f"\n  TotalRisk PerStream     NP(parent)  NP(+retry)   ROI     DD%   NP/DD$  PT  HT")
        # Run only at 9% (matches live)
        deals_parent = []
        deals_with_h = []
        per_s = {}
        for lbl, base_cfg in zip(labels, cfgs_base):
            # Build cfg with production sizing
            cfg = row_to_cfg({
                "range_minutes": base_cfg.range_minutes,
                "fixed_sl_pts": base_cfg.fixed_sl_pts,
                "rr_ratio": base_cfg.rr_ratio,
                "half_tp_ratio": base_cfg.half_tp_ratio,
                "daily_target_pct": 0.0, "daily_loss_pct": 0.0,
            }, lbl, per_stream_risk)
            # Reuse run_baseline_window logic but use this cfg directly
            r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
            sl_events = []
            base_deals = []
            open_positions = []
            for d in r.deals:
                ts_ns = pd.Timestamp(d.ts).value
                if d.kind == "entry":
                    open_positions.append({"entry_ts_ns": ts_ns, "direction": int(d.direction),
                                            "lots": float(d.lots), "entry_price": float(d.price)})
                    continue
                base_deals.append((ts_ns, d.pnl))
                match_idx = -1
                for i, op in enumerate(open_positions):
                    if op["direction"] == int(d.direction):
                        match_idx = i; break
                if match_idx >= 0:
                    op = open_positions.pop(match_idx)
                    if d.kind == "sl":
                        sl_events.append({
                            "ts_ns": ts_ns, "entry_ts_ns": op["entry_ts_ns"],
                            "direction": op["direction"], "sl_price": float(d.price),
                            "entry_price": op["entry_price"], "lots": op["lots"],
                        })

            # Retry hedge for this stream
            tp_mult = TP_MULT_BY_STREAM[lbl]
            hcfg = RetryHedgeCfg(exp_min=HEDGE_EXP_MIN, f1_sec=HEDGE_F1_SEC,
                                  buf_pts=0, tp_mult=tp_mult)
            stream_cfg = {"fixed_sl_pts": base_cfg.fixed_sl_pts,
                          "rr_ratio": base_cfg.rr_ratio,
                          "half_tp_ratio": base_cfg.half_tp_ratio}
            h_deals = simulate_retry_hedges(sl_events, t_arr, stream_cfg, hcfg)

            deals_parent.extend(base_deals)
            deals_with_h.extend(base_deals + h_deals)
            parent_n_tp = sum(1 for d in r.deals if d.kind in ("tp", "htp"))
            parent_n_sl = sum(1 for d in r.deals if d.kind == "sl")
            hedge_n = len(h_deals)
            hedge_wr = sum(1 for _, p in h_deals if p > 0) / hedge_n * 100 if hedge_n else 0
            per_s[lbl] = {
                "parent_np": sum(p for _, p in base_deals),
                "parent_n": len(base_deals),
                "parent_tp": parent_n_tp,
                "parent_sl": parent_n_sl,
                "hedge_np": sum(p for _, p in h_deals),
                "hedge_n": hedge_n,
                "hedge_wr": hedge_wr,
                "tp_mult": tp_mult,
            }

        np_p, dd_p, pf_p = aggregate(deals_parent)
        np_h, dd_h, pf_h = aggregate(deals_with_h)
        # NP/DD$ = NP / DD$ where DD$ = DD% × peak_balance
        peak_p = DEPOSIT + np_p
        peak_h = DEPOSIT + np_h
        ndd_p = (np_p / (dd_p/100 * peak_p)) if dd_p > 0 else 0
        ndd_h = (np_h / (dd_h/100 * peak_h)) if dd_h > 0 else 0
        roi_p = np_p / DEPOSIT * 100
        roi_h = np_h / DEPOSIT * 100

        print(f"\n  Portfolio @ 9% (1.5% per stream):")
        print(f"  {'Variant':<14} {'NP':>10} {'NP-hc':>10} {'ROI':>6} {'DD%':>6} {'NP/DD$':>7} {'Trades':>7}")
        print(f"  {'parent only':<14} ${np_p:>+8,.0f} ${np_p*0.94:>+8,.0f} {roi_p:>+5.1f}% "
              f"{dd_p:>5.1f}% {ndd_p:>7.2f} {len(deals_parent):>7}")
        print(f"  {'+retry hedge':<14} ${np_h:>+8,.0f} ${np_h*0.94:>+8,.0f} {roi_h:>+5.1f}% "
              f"{dd_h:>5.1f}% {ndd_h:>7.2f} {len(deals_with_h):>7}")
        print(f"  {'delta':<14} ${np_h-np_p:>+8,.0f}")

        print(f"\n  Per-stream breakdown:")
        print(f"  {'Stream':<6} {'tp_mult':<7} {'parent NP':>10} ({'tr':>3}) "
              f"{'retry NP':>10} ({'tr':>3}, {'wr':>4})")
        for lbl in labels:
            ps = per_s[lbl]
            print(f"  {lbl:<6} {ps['tp_mult']:<7} ${ps['parent_np']:>+8,.0f} ({ps['parent_n']:>3}) "
                  f"${ps['hedge_np']:>+8,.0f} ({ps['hedge_n']:>3}, {ps['hedge_wr']:>3.0f}%)")

    finally:
        from zgb_sim.tick_loader import kill_mt5_terminal
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
