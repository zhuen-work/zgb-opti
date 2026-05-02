"""DD-match Balanced (FBO S1+S2) to Aggressive (FBO S1+S2+FVG S2).

Balanced @ 3% risk → DD 17.9%, NP +$6,736
Aggressive @ 3% risk → DD 25.8%, NP +$9,634

Sweep Balanced risk_pct upward to find match point with Aggressive's DD,
then compare absolute returns.
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
from zgb_sim.fvg import FVGConfig
from zgb_sim.all_streams_combined import simulate_all_streams


def fbo_s1(risk_pct: float) -> FBOS1Config:
    return FBOS1Config(
        risk_pct=risk_pct, fractal_bars=8, take_profit_pts=25_000,
        stop_loss_pts=10_000, half_tp_ratio=0.3, sma_period=10,
        pending_expire_bars=2, signal_tf_minutes=30, comment="FBO_A",
    )


def fbo_s2_m15(risk_pct: float) -> FBOS1Config:
    return FBOS1Config(
        risk_pct=risk_pct, fractal_bars=8, take_profit_pts=4_000,
        stop_loss_pts=4_000, half_tp_ratio=0.6, sma_period=50,
        pending_expire_bars=4, signal_tf_minutes=15, comment="FBO_B",
    )


def fvg_s2(risk_pct: float = 3.0) -> FVGConfig:
    return FVGConfig(
        risk_pct=risk_pct, min_size_pts=1500, max_age_bars=100, max_zones=3,
        rr_ratio=5.0, sl_buffer_pts=20, half_tp_ratio=0.0,
        pending_expire_bars=2, signal_tf_minutes=240, comment="FVG_B",
    )


def main() -> int:
    full_start = datetime(2026, 3, 14, tzinfo=timezone.utc)
    full_end = datetime(2026, 4, 25, tzinfo=timezone.utc)

    try:
        m = symbol_meta("XAUUSD")
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )
        print("Loading data once...")
        ticks = load_ticks("XAUUSD", full_start, full_end)
        m1 = load_bars("XAUUSD", "M1", full_start, full_end)
        m30 = load_bars("XAUUSD", "M30", full_start, full_end)
        m15 = load_bars("XAUUSD", "M15", full_start, full_end)
        h4 = load_bars("XAUUSD", "H4", full_start, full_end)
        h1 = load_bars("XAUUSD", "H1", full_start, full_end)
        print()

        # Aggressive baseline @ 3% risk
        print("=" * 72)
        print("  AGGRESSIVE BASELINE (FBO S1 + FBO S2 + FVG S2 @ 3%)")
        print("=" * 72)
        agg = simulate_all_streams(
            ticks, m30, m15, h1, h4, m1,
            fbo_s1(3.0), fbo_s2_m15(3.0), None, fvg_s2(3.0),
            meta, initial_balance=10_000.0,
        )
        print(f"  NP=${agg.summary.net_profit:+,.2f}  DD={agg.combined_dd_pct:.2f}%  "
              f"PF={agg.summary.profit_factor:.2f}  Trades={agg.summary.trades}")
        target_dd = agg.combined_dd_pct

        # Balanced sweep
        print("\n" + "=" * 72)
        print(f"  BALANCED RISK SWEEP (FBO S1 + FBO S2 only) — target DD {target_dd:.2f}%")
        print("=" * 72)
        results = []
        for risk in (3.0, 3.5, 4.0, 4.5, 5.0, 5.5):
            r = simulate_all_streams(
                ticks, m30, m15, h1, h4, m1,
                fbo_s1(risk), fbo_s2_m15(risk), None, None,
                meta, initial_balance=10_000.0,
            )
            results.append((risk, r))
            mark = "  <-- close" if abs(r.combined_dd_pct - target_dd) < 1.5 else ""
            print(f"  Balanced @ {risk:>4.1f}%:  NP=${r.summary.net_profit:>+9,.0f}  "
                  f"DD={r.combined_dd_pct:>5.2f}%  PF={r.summary.profit_factor:.2f}  "
                  f"Trades={r.summary.trades}{mark}")

        # Pick best DD match
        best_risk, best_r = min(results, key=lambda x: abs(x[1].combined_dd_pct - target_dd))

        print("\n" + "=" * 72)
        print(f"  DD-MATCHED COMPARISON (target DD ~{target_dd:.1f}%)")
        print("=" * 72)
        print(f"\n  {'Setup':<40}  {'Risk%':>6}  {'NP':>10}  {'DD%':>6}  {'PF':>5}  {'Trades':>7}")
        print(f"  {'-'*78}")
        print(f"  {'Balanced (FBO S1+S2 only) DD-matched':<40}  "
              f"{best_risk:>6.1f}  ${best_r.summary.net_profit:>+8,.0f}  "
              f"{best_r.combined_dd_pct:>5.2f}  {best_r.summary.profit_factor:>5.2f}  "
              f"{best_r.summary.trades:>7}")
        print(f"  {'Aggressive (+ FVG S2)':<40}  "
              f"{3.0:>6.1f}  ${agg.summary.net_profit:>+8,.0f}  "
              f"{agg.combined_dd_pct:>5.2f}  {agg.summary.profit_factor:>5.2f}  "
              f"{agg.summary.trades:>7}")

        diff = best_r.summary.net_profit - agg.summary.net_profit
        winner = "Balanced (DD-matched)" if diff > 0 else "Aggressive"
        print(f"\n  Difference: Balanced - Aggressive = ${diff:+,.0f}")
        print(f"  {winner} wins on equal-DD basis.")

        # Per-stream breakdown for the matched balanced
        print(f"\n  Balanced @ {best_risk}% per-stream:")
        for name, ps in best_r.per_stream.items():
            print(f"    {name:<8}  NP=${ps['np']:>+9,.0f}  trades={ps['trades']:>3}  "
                  f"TP={ps['tp']:>3}  SL={ps['sl']:>3}")
        print(f"\n  Aggressive @ 3% per-stream:")
        for name, ps in agg.per_stream.items():
            print(f"    {name:<8}  NP=${ps['np']:>+9,.0f}  trades={ps['trades']:>3}  "
                  f"TP={ps['tp']:>3}  SL={ps['sl']:>3}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
