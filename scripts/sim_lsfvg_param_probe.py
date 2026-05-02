"""LSFVG parameter probe — loosen filters to find where edge (if any) lives.

Smoke at defaults gave only 10 setups over 6 weeks. Try loosening:
  - min_fvg_pts (smaller threshold = more setups)
  - lookback_bars (different pool definitions)
  - signal TF (M15 vs M30 vs M5)
  - Without half-TP split (cleaner attribution)

Goal: find a config that gives 30+ trades with PF > 1.0. If none does, the
sweep+immediate-FVG semantics are too strict and the detection logic needs
rethinking (allow displacement on later bars, or relax sweep criteria).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.lsfvg import LSFVGConfig
from zgb_sim.lsfvg_fast import simulate_fast


SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0


def main() -> int:
    start = datetime(2026, 3, 14, tzinfo=timezone.utc)
    end = datetime(2026, 4, 25, tzinfo=timezone.utc)

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
            tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
            volume_min=m["volume_min"], volume_max=m["volume_max"],
            volume_step=m["volume_step"],
        )
        ticks = load_ticks(SYMBOL, start, end)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)
        m15 = load_bars(SYMBOL, "M15", start, end)
        m30 = load_bars(SYMBOL, "M30", start, end)

        bars_for_tf = {5: m5, 15: m15, 30: m30}

        configs = [
            # name, tf, lookback, min_fvg, max_fvg, sweep_buf, rr, htp
            ("default",       15, 15, 100, 3000, 30, 2.0, 0.5),
            ("loose_fvg",     15, 15,  20, 5000, 30, 2.0, 0.5),
            ("loose_lk10",    15, 10,  20, 5000, 30, 2.0, 0.5),
            ("loose_lk20",    15, 20,  20, 5000, 30, 2.0, 0.5),
            ("loose_no_htp",  15, 15,  20, 5000, 30, 2.0, 0.0),
            ("M5_loose",       5, 15,  20, 3000, 20, 2.0, 0.0),
            ("M30_loose",     30, 15,  50, 8000, 50, 2.0, 0.0),
            ("RR1.5",         15, 15,  20, 5000, 30, 1.5, 0.0),
            ("RR3.0",         15, 15,  20, 5000, 30, 3.0, 0.0),
            ("very_loose",    15, 10,  10, 8000, 30, 2.0, 0.0),
        ]

        print("=" * 100)
        print("  LSFVG PARAMETER PROBE (Mar 14 -> Apr 25, $10k, 3% risk)")
        print("=" * 100)
        print(f"  {'Name':<14} {'TF':>4} {'Lk':>3} {'MinFVG':>7} {'MaxFVG':>7} {'SwBuf':>6} {'RR':>4} {'HTP':>4} | "
              f"{'Setups':>7} {'Trades':>7} {'WR%':>6} {'NP':>10} {'DD%':>6} {'PF':>6} {'NP/DD':>7}")
        print("  " + "-" * 98)

        for name, tf, lk, mn, mx, sb, rr, htp in configs:
            cfg = LSFVGConfig(
                risk_pct=3.0,
                signal_tf_minutes=tf,
                lookback_bars=lk,
                min_fvg_pts=mn,
                max_fvg_pts=mx,
                sweep_buffer_pts=sb,
                rr_ratio=rr,
                half_tp_ratio=htp,
                pending_expire_bars=4,
            )
            bars = bars_for_tf[tf]
            r = simulate_fast(ticks, bars, m1, cfg, meta, initial_balance=DEPOSIT)
            wr = (r.tp_count / r.trades * 100.0) if r.trades > 0 else 0.0
            ndd = r.net_profit / r.max_drawdown if r.max_drawdown > 0 else 0.0
            # diag is printed by simulate_fast itself; we show summary line
            print(f"  {name:<14} M{tf:<3} {lk:>3} {mn:>7} {mx:>7} {sb:>6} {rr:>4.1f} {htp:>4.1f} | "
                  f"{'-':>7} {r.trades:>7} {wr:>5.1f}% ${r.net_profit:>+8,.0f} {r.max_drawdown_pct:>5.1f}% "
                  f"{r.profit_factor:>6.2f} {ndd:>7.1f}")

        print("  " + "-" * 98)
        print("\n  Pick configs with Trades >= 30 AND PF > 1.0 for further WFO.")
        print("  If none qualifies, detection logic itself needs revisiting (e.g., allow")
        print("  displacement on later bars, scan FVG within window after sweep, etc.).")

    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
