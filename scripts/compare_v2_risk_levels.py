"""V2 (6-stream MAY9+MAY2) at 3% / 6% / 9% across 3 spreads.

Single-EA risk-level comparison.
"""
from __future__ import annotations

import sys
from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.wfo_helpers import to_utc

from sim_wfo_hedge import slice_window, aggregate, DEPOSIT, SYMBOL
from compare_pro_vs_v2 import MAY9_CFGS, MAY2_CFGS, portfolio

SPREADS = [30, 55, 70]  # per feedback_default_test_conditions.md (all live = 30pt 2026-05-16)
TOTAL_RISKS = [3.0, 6.0, 9.0]
START_DATE = date(2026, 2, 21)
END_DATE   = date(2026, 5, 9)


def main() -> int:
    print("=" * 100)
    print(f"  V2 (6-stream MAY9+MAY2) at 3% / 6% / 9% total risk")
    print(f"  {START_DATE} -> {END_DATE} ({(END_DATE-START_DATE).days}d, $10k)")
    print("=" * 100)
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        start = to_utc(START_DATE); end = to_utc(END_DATE)
        full_m1 = load_bars(SYMBOL, "M1", start, end)
        full_m5 = load_bars(SYMBOL, "M5", start, end)

        rows = []
        for sp in SPREADS:
            print(f"\n  -- spread {sp}pt --")
            full_ticks = load_ticks(SYMBOL, start, end, spread_pts=sp)
            ticks = slice_window(full_ticks, "ts", start, end)
            m1 = slice_window(full_m1, "ts", start, end)
            m5 = slice_window(full_m5, "ts", start, end)
            for total in TOTAL_RISKS:
                per = total / 6.0
                v2 = portfolio(ticks, m1, m5, meta, MAY9_CFGS + MAY2_CFGS, per, "v2")
                rows.append((sp, total, v2))
                print(f"    {total:>3}% (per_stream={per:.2f}%): NP=${v2[0]:+,.0f}  "
                      f"DD={v2[1]:.2f}%  NP/DD$={v2[2]:.2f}  PF={v2[3]:.2f}  T={v2[4]}")

        print("\n" + "=" * 100)
        print("  SUMMARY")
        print("=" * 100)
        print(f"  {'Spread':>6} | {'Risk':>4} | {'NP':>10} | {'DD%':>6} | {'NP/DD$':>7} | "
              f"{'PF':>5} | {'Trades':>6}")
        print(f"  {'-'*6}-+-{'-'*4}-+-{'-'*10}-+-{'-'*6}-+-{'-'*7}-+-{'-'*5}-+-{'-'*6}")
        prev_sp = None
        for sp, total, v2 in rows:
            if prev_sp is not None and prev_sp != sp:
                print(f"  {'='*6}-+-{'='*4}-+-{'='*10}-+-{'='*6}-+-{'='*7}-+-{'='*5}-+-{'='*6}")
            prev_sp = sp
            print(f"  {sp:>4}pt | {total:>3}% | ${v2[0]:>+8,.0f} | "
                  f"{v2[1]:>5.2f}% | {v2[2]:>7.2f} | {v2[3]:>5.2f} | {v2[4]:>6}")

        # delta vs 3%
        print("\n" + "=" * 100)
        print("  RISK SCALING (vs 3% baseline, same spread)")
        print("=" * 100)
        print(f"  {'Spread':>6} | {'Risk':>4} | NP_mult | DD_mult | NP/DD$_mult")
        print(f"  {'-'*6}-+-{'-'*4}-+-{'-'*7}-+-{'-'*7}-+-{'-'*11}")
        baseline = {sp: None for sp in SPREADS}
        for sp, total, v2 in rows:
            if total == 3.0:
                baseline[sp] = v2
                continue
            b = baseline[sp]
            np_m = v2[0] / b[0] if b[0] else 0
            dd_m = v2[1] / b[1] if b[1] else 0
            ndd_m = v2[2] / b[2] if b[2] else 0
            print(f"  {sp:>4}pt | {total:>3}% | {np_m:>5.2f}x | {dd_m:>5.2f}x | {ndd_m:>9.2f}x")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
