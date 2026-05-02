"""DD-matched comparison: S1 (M30) vs S2-M15.

S2-M15 sanity DD = 13.8%. S1 (M30) sanity DD at 3% risk = 5.5%.
Scale S1's risk_pct to match S2-M15's DD, then compare absolute NP.

Linear scaling assumption: DD scales linearly with risk_pct since FBO has
no daily target/loss caps and lot is well below volume_max.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.fbo_s1 import FBOS1Config
from zgb_sim.fbo_s1_fast import simulate_fast


SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0


def make_s1(risk_pct: float) -> FBOS1Config:
    return FBOS1Config(
        risk_pct=risk_pct,
        fractal_bars=8,
        take_profit_pts=25_000,
        stop_loss_pts=10_000,
        half_tp_ratio=0.3,
        sma_period=10,
        pending_expire_bars=2,
        signal_tf_minutes=30,
        comment="FBO_A",
    )


def make_s2_m15() -> FBOS1Config:
    return FBOS1Config(
        risk_pct=3.0,
        fractal_bars=8,
        take_profit_pts=4_000,
        stop_loss_pts=4_000,
        half_tp_ratio=0.6,
        sma_period=50,
        pending_expire_bars=4,
        signal_tf_minutes=15,
        comment="FBO_B",
    )


def main() -> int:
    start = datetime(2026, 3, 14, tzinfo=timezone.utc)
    end = datetime(2026, 4, 25, tzinfo=timezone.utc)

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )

        print("Loading data once...")
        ticks = load_ticks(SYMBOL, start, end)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m30 = load_bars(SYMBOL, "M30", start, end)
        m15 = load_bars(SYMBOL, "M15", start, end)
        print(f"  ticks={len(ticks):,}  M1={len(m1):,}  M30={len(m30):,}  M15={len(m15):,}\n")

        # Run S2-M15 baseline
        s2_m15 = simulate_fast(ticks, m15, m1, make_s2_m15(), meta, initial_balance=DEPOSIT)
        print(f"S2-M15 (3% risk):       {s2_m15.summary()}")

        # Sweep S1 risk to find DD match
        target_dd = s2_m15.max_drawdown_pct
        print(f"\nTarget DD: {target_dd:.2f}%\n")

        results = []
        for risk in (3.0, 5.0, 7.0, 7.5, 8.0, 9.0, 10.0):
            r = simulate_fast(ticks, m30, m1, make_s1(risk), meta, initial_balance=DEPOSIT)
            results.append((risk, r))
            mark = "  <-- close to target" if abs(r.max_drawdown_pct - target_dd) < 1.0 else ""
            print(f"S1 (M30) {risk:>4.1f}% risk:  {r.summary()}{mark}")

        # Pick best match
        best = min(results, key=lambda x: abs(x[1].max_drawdown_pct - target_dd))
        s1_match_risk, s1_match_result = best

        print("\n" + "=" * 72)
        print("  DD-MATCHED COMPARISON (Mar 14 -> Apr 25, $10k continuous)")
        print("=" * 72)
        print(f"\n  {'Stream':<12}  {'Risk%':>6}  {'NP':>12}  {'ROI%':>7}  "
              f"{'DD%':>6}  {'PF':>5}  {'Trades':>6}  {'TP/SL':>9}")
        print(f"  {'-' * 72}")
        print(f"  {'S1 (M30)':<12}  {s1_match_risk:>6.1f}  ${s1_match_result.net_profit:>+10,.0f}  "
              f"{s1_match_result.net_profit/DEPOSIT*100:>+6.1f}  "
              f"{s1_match_result.max_drawdown_pct:>5.1f}  "
              f"{s1_match_result.profit_factor:>5.2f}  "
              f"{s1_match_result.trades:>6}  "
              f"{s1_match_result.tp_count}/{s1_match_result.sl_count}")
        print(f"  {'S2-M15':<12}  {3.0:>6.1f}  ${s2_m15.net_profit:>+10,.0f}  "
              f"{s2_m15.net_profit/DEPOSIT*100:>+6.1f}  "
              f"{s2_m15.max_drawdown_pct:>5.1f}  "
              f"{s2_m15.profit_factor:>5.2f}  "
              f"{s2_m15.trades:>6}  "
              f"{s2_m15.tp_count}/{s2_m15.sl_count}")

        diff = s1_match_result.net_profit - s2_m15.net_profit
        print(f"\n  Difference: S1 - S2-M15 = ${diff:+,.0f}  "
              f"({'S1 wins' if diff > 0 else 'S2-M15 wins'} on equal-DD basis)")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
