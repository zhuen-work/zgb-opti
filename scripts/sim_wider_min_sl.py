"""Wider min-SL test.

HYPOTHESIS: trades get stopped out too quickly. Widening the minimum SL gives
more breathing room → fewer premature SLs → higher hit rate on the TP side
(or fewer SL losses, even though TP also moves further away).

METHOD (same counterfactual approach as sim_atr_sl.py):
  1. Run baseline parent sim → get all entries
  2. For each entry: effective_sl_pts = max(min_sl_floor, baseline_sl_pts)
     (so streams whose baseline SL >= floor are unchanged; tighter streams widen)
  3. TP_pts = RR × effective_sl_pts  (RR preserved)
  4. Lots recompute under fixed-% risk: lots = risk_$ / (eff_sl_pts × point × contract)
     → wider SL = fewer lots, same $-risk per trade
  5. Re-walk ticks from entry forward → determine new SL/TP/EXPIRE outcome
  6. Aggregate per-stream + portfolio. Compare to baseline.

This is the "fixed floor" version of ATR-SL — simpler and easier to deploy.

Usage:
  python scripts/sim_wider_min_sl.py
"""
from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.wfo_helpers import WINDOWS_MAY9 as WINDOWS, to_utc

from sim_wfo_hedge_retry import (STREAM_CFGS, make_stream_cfg,
                                  SYMBOL, DEPOSIT, SPREAD, POINT, CONTRACT,
                                  PARENT_RISK_PROD)

from sim_atr_sl import (run_baseline_with_entries, aggregate_pnl, ts_arr_from_ticks)


# Test grid
MIN_SL_FLOORS = [400, 500, 600, 700, 800, 900, 1000, 1200, 1500]
RR = 4.0


def resim_with_min_sl(deals: list, ticks_arr: dict, min_sl_pts: int,
                        baseline_sl_pts: int, rr: float) -> list:
    """Re-simulate each deal with effective_sl = max(min_sl, baseline_sl)."""
    ts_arr = ticks_arr["ts_ns"]
    bid = ticks_arr["bid"]
    ask = ticks_arr["ask"]
    out = []
    walk_max_ns = int(8 * 3600 * 1_000_000_000)
    risk_dollar = DEPOSIT * (PARENT_RISK_PROD / 100.0)
    eff_sl_pts = max(min_sl_pts, baseline_sl_pts)
    eff_sl_dist = eff_sl_pts * POINT
    eff_tp_dist = rr * eff_sl_dist
    new_lots = risk_dollar / (eff_sl_dist * CONTRACT)
    new_lots = max(0.01, round(new_lots / 0.01) * 0.01)

    for d in deals:
        entry_p = d["entry_price"]
        direction = d["direction"]
        if direction == 1:
            sl_price = entry_p - eff_sl_dist
            tp_price = entry_p + eff_tp_dist
        else:
            sl_price = entry_p + eff_sl_dist
            tp_price = entry_p - eff_tp_dist

        i0 = np.searchsorted(ts_arr, d["entry_ts_ns"])
        i1 = np.searchsorted(ts_arr, d["entry_ts_ns"] + walk_max_ns)
        post_bid = bid[i0:i1]
        post_ask = ask[i0:i1]
        if len(post_bid) == 0:
            continue
        if direction == 1:
            sl_hits = np.where(post_bid <= sl_price)[0]
            tp_hits = np.where(post_bid >= tp_price)[0]
        else:
            sl_hits = np.where(post_ask >= sl_price)[0]
            tp_hits = np.where(post_ask <= tp_price)[0]
        sl_first = sl_hits[0] if len(sl_hits) else 10**18
        tp_first = tp_hits[0] if len(tp_hits) else 10**18
        if sl_first == 10**18 and tp_first == 10**18:
            last_mid = (post_bid[-1] + post_ask[-1]) / 2
            new_pnl = direction * (last_mid - entry_p) * CONTRACT * new_lots
            hit = "EXPIRE"
        elif sl_first <= tp_first:
            new_pnl = direction * (sl_price - entry_p) * CONTRACT * new_lots
            hit = "SL"
        else:
            new_pnl = direction * (tp_price - entry_p) * CONTRACT * new_lots
            hit = "TP"
        out.append({"ts_ns": d["ts_ns"], "pnl": float(new_pnl), "hit": hit,
                    "eff_sl_pts": eff_sl_pts, "new_lots": new_lots})
    return out


def main() -> int:
    print("=" * 110)
    print(f"  WIDER MIN-SL TEST  (counterfactual on baseline ORB entries)")
    print(f"  Effective SL = max(min_sl_floor, baseline_sl_pts), RR={RR} fixed, HTP skipped")
    print(f"  Sweep: min_sl floors {MIN_SL_FLOORS}pt")
    print(f"  Baseline per-stream SL_pts: " +
          ", ".join(f"{s}={cfg['fixed_sl_pts']}" for s, cfg in STREAM_CFGS.items()))
    print("=" * 110)

    try:
        from zgb_sim.mt5_accounts import init_account
        init_account("sim")
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        start = to_utc(WINDOWS[0][1])
        end = to_utc(WINDOWS[-1][4])
        print(f"\n  Loading data {start.date()} -> {end.date()}...")
        ticks = load_ticks(SYMBOL, start, end, spread_pts=SPREAD)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)
        print(f"  ticks={len(ticks):,}  M1={len(m1):,}  M5={len(m5):,}")
        ticks_arr = ts_arr_from_ticks(ticks)

        print(f"\n  Running baseline parent sims at {PARENT_RISK_PROD}% per stream...")
        per_stream_deals = {}
        per_stream_baseline = {}
        for stream in STREAM_CFGS:
            deals = run_baseline_with_entries(stream, ticks, m1, m5, meta, PARENT_RISK_PROD)
            per_stream_deals[stream] = deals
            base_list = [{"ts_ns": d["ts_ns"], "pnl": d["baseline_pnl"]} for d in deals]
            np_, dd, ndd, n = aggregate_pnl(base_list)
            per_stream_baseline[stream] = (np_, dd, ndd, n)
            print(f"    {stream}: n={n}, NP=${np_:+,.0f}, DD={dd:.2f}%, NP/DD$={ndd:.2f}")

        # Per-stream
        print(f"\n" + "=" * 110)
        print(f"  PER-STREAM SWEEP")
        print("=" * 110)
        for stream in STREAM_CFGS:
            base_np, base_dd, base_ndd, base_n = per_stream_baseline[stream]
            base_sl = STREAM_CFGS[stream]['fixed_sl_pts']
            print(f"\n  {stream}  (baseline: SL={base_sl}pt  NP=${base_np:+,.0f}  DD={base_dd:.2f}%  NP/DD$={base_ndd:.2f})")
            print(f"    {'min_SL':>7} {'eff_SL':>7} {'NP':>10} {'DD%':>6} {'NP/DD$':>8} {'TP':>4} {'SL':>4} "
                  f"{'EXP':>4} {'vs base NP/DD$':>16}")
            for floor in MIN_SL_FLOORS:
                resimmed = resim_with_min_sl(per_stream_deals[stream], ticks_arr,
                                                floor, base_sl, RR)
                np_, dd, ndd, n = aggregate_pnl(resimmed)
                tps = sum(1 for r in resimmed if r["hit"] == "TP")
                sls = sum(1 for r in resimmed if r["hit"] == "SL")
                exps = sum(1 for r in resimmed if r["hit"] == "EXPIRE")
                d_ndd = ndd - base_ndd
                eff = max(floor, base_sl)
                marker = " ***" if d_ndd > 0 else ""
                print(f"    {floor:>7} {eff:>7} ${np_:>+8,.0f} {dd:>5.2f}% {ndd:>+7.2f} "
                      f"{tps:>4} {sls:>4} {exps:>4} {d_ndd:>+15.2f}{marker}")

        # Portfolio
        print(f"\n" + "=" * 110)
        print(f"  PORTFOLIO COMPARISON")
        print("=" * 110)
        all_baseline = []
        for stream in STREAM_CFGS:
            for d in per_stream_deals[stream]:
                all_baseline.append({"ts_ns": d["ts_ns"], "pnl": d["baseline_pnl"]})
        b_np, b_dd, b_ndd, b_n = aggregate_pnl(all_baseline)
        print(f"\n  {'min_SL floor':<14} {'NP':>10} {'DD%':>6} {'NP/DD$':>7} {'Trades':>7}  "
              f"{'vs base NP':>11}  {'vs base NP/DD$':>15}")
        print(f"  {'baseline':<14} ${b_np:>+8,.0f} {b_dd:>5.2f}% {b_ndd:>+6.2f} {b_n:>7}        --              --")
        for floor in MIN_SL_FLOORS:
            combined = []
            for stream in STREAM_CFGS:
                resimmed = resim_with_min_sl(per_stream_deals[stream], ticks_arr,
                                                floor, STREAM_CFGS[stream]['fixed_sl_pts'], RR)
                combined.extend(resimmed)
            np_, dd, ndd, n = aggregate_pnl(combined)
            d_np = np_ - b_np
            d_ndd = ndd - b_ndd
            marker = " ***" if d_ndd > 0 else ""
            print(f"  min_SL={floor:<6}    ${np_:>+8,.0f} {dd:>5.2f}% {ndd:>+6.2f} {n:>7} ${d_np:>+9,.0f}  {d_ndd:>+14.2f}{marker}")

    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
