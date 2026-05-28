"""MAY9 hedge ON vs OFF comparison across 3 spreads (23/55/70pt).

Re-uses sim_wfo_hedge helpers + already-persisted per-stream hedge winners
(output/wfo_hedge_per_stream_may9/{S1,S2,S3}.json).

Portfolio sim: each stream parent at 1.0% (matches 3% setfile per-stream alloc),
each hedge at 1.0%. Window: full WFO span (Feb 21 -> May 9, 77d).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from datetime import datetime
import numpy as np
import pandas as pd

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.wfo_helpers import WINDOWS_MAY9 as WINDOWS, to_utc

from sim_wfo_hedge import (STREAM_CFGS, HedgeCfg, run_baseline_window,
                            simulate_hedges, ts_arr_from_ticks, slice_window,
                            aggregate, DEPOSIT, SYMBOL, PARENT_RISK_PROD,
                            HEDGE_RISK_PCT)

SPREADS = [30, 55, 70]  # per feedback_default_test_conditions.md (all live = 30pt 2026-05-16)
HEDGE_DIR = ROOT / "output" / "wfo_hedge_per_stream_may9"


def load_winners() -> dict[str, HedgeCfg]:
    out = {}
    for s in ("S1", "S2", "S3"):
        d = json.loads((HEDGE_DIR / f"{s}.json").read_text())
        out[s] = HedgeCfg(buf=int(d["buffer_pts"]), h_sl=int(d["fixed_sl_pts"]),
                          h_rr=float(d["rr_ratio"]), exp=int(d["expire_minutes"]))
    return out


def portfolio_sim(full_ticks, full_m1, full_m5, meta, winners, with_hedge: bool):
    start = to_utc(WINDOWS[0][1])
    end = to_utc(WINDOWS[-1][4])
    ticks = slice_window(full_ticks, "ts", start, end)
    m1 = slice_window(full_m1, "ts", start, end)
    m5 = slice_window(full_m5, "ts", start, end)
    t_arr = ts_arr_from_ticks(ticks) if with_hedge else None

    all_deals = []
    for s in STREAM_CFGS:
        deals, sl_ev = run_baseline_window(s, ticks, m1, m5, meta, PARENT_RISK_PROD)
        if with_hedge:
            h_deals = simulate_hedges(sl_ev, t_arr, winners[s])
            all_deals.extend(deals + h_deals)
        else:
            all_deals.extend(deals)
    np_, dd, pf = aggregate(all_deals)
    ndd = (np_ / (dd / 100 * (DEPOSIT + np_))) if dd > 0 else 0.0
    return np_, dd, ndd, pf, len(all_deals)


def main() -> int:
    print("=" * 100)
    print(f"  MAY9 HEDGE ON vs OFF  |  3 spreads  |  Feb 21 -> May 9 (77d, $10k)")
    print(f"  Per stream: parent {PARENT_RISK_PROD}%, hedge {HEDGE_RISK_PCT}%")
    print("=" * 100)
    winners = load_winners()
    print(f"\n  Per-stream hedge winners (from {HEDGE_DIR.name}):")
    for s, w in winners.items():
        print(f"    {s}: buf={w.buf} h_sl={w.h_sl} h_rr={w.h_rr} exp={w.exp}min")

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        start = to_utc(WINDOWS[0][1])
        end = to_utc(WINDOWS[-1][4])
        full_m1 = load_bars(SYMBOL, "M1", start, end)
        full_m5 = load_bars(SYMBOL, "M5", start, end)

        rows = []
        for sp in SPREADS:
            print(f"\n  -- spread {sp}pt --")
            full_ticks = load_ticks(SYMBOL, start, end, spread_pts=sp)
            no_h = portfolio_sim(full_ticks, full_m1, full_m5, meta, winners, with_hedge=False)
            wi_h = portfolio_sim(full_ticks, full_m1, full_m5, meta, winners, with_hedge=True)
            rows.append({"spread": sp,
                         "noh_NP": no_h[0], "noh_DD": no_h[1], "noh_NDD": no_h[2],
                         "noh_PF": no_h[3], "noh_TR": no_h[4],
                         "wi_NP":  wi_h[0], "wi_DD":  wi_h[1], "wi_NDD":  wi_h[2],
                         "wi_PF":  wi_h[3], "wi_TR":  wi_h[4]})
            print(f"    no-hedge: NP=${no_h[0]:+,.0f}  DD={no_h[1]:.2f}%  NP/DD$={no_h[2]:.2f}  "
                  f"PF={no_h[3]:.2f}  T={no_h[4]}")
            print(f"    +hedge:   NP=${wi_h[0]:+,.0f}  DD={wi_h[1]:.2f}%  NP/DD$={wi_h[2]:.2f}  "
                  f"PF={wi_h[3]:.2f}  T={wi_h[4]}")

        print("\n" + "=" * 100)
        print("  SUMMARY  (M = no-hedge baseline ; H = +hedge ; D = delta)")
        print("=" * 100)
        print(f"  {'Spread':>6} | {'Variant':<8} | {'NP':>10} | {'DD%':>6} | {'NP/DD$':>7} | "
              f"{'PF':>5} | {'Trades':>6}")
        print(f"  {'-'*6}-+-{'-'*8}-+-{'-'*10}-+-{'-'*6}-+-{'-'*7}-+-{'-'*5}-+-{'-'*6}")
        for r in rows:
            print(f"  {r['spread']:>4}pt | {'no-hedge':<8} | ${r['noh_NP']:>+8,.0f} | "
                  f"{r['noh_DD']:>5.2f}% | {r['noh_NDD']:>7.2f} | {r['noh_PF']:>5.2f} | {r['noh_TR']:>6}")
            print(f"  {r['spread']:>4}pt | {'+hedge':<8} | ${r['wi_NP']:>+8,.0f} | "
                  f"{r['wi_DD']:>5.2f}% | {r['wi_NDD']:>7.2f} | {r['wi_PF']:>5.2f} | {r['wi_TR']:>6}")
            d_np = r['wi_NP'] - r['noh_NP']; d_dd = r['wi_DD'] - r['noh_DD']
            d_ndd = r['wi_NDD'] - r['noh_NDD']
            ndd_pct = (d_ndd / r['noh_NDD'] * 100) if r['noh_NDD'] != 0 else 0
            print(f"  {r['spread']:>4}pt | {'delta':<8} | ${d_np:>+8,.0f} | "
                  f"{d_dd:>+5.1f}p | {d_ndd:>+7.2f} ({ndd_pct:+.0f}%)")
            print(f"  {'-'*6}-+-{'-'*8}-+-{'-'*10}-+-{'-'*6}-+-{'-'*7}-+-{'-'*5}-+-{'-'*6}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
