"""ORB sanity ET: 3-way config compare at spread 60 vs 70.

Configs:
  LIVE  = DT818_pro live ORB params (R60/SL400/RR3.0/HTP0.0/Tgt27/Loss18)
  W60   = prior spread=60 WFO winner  (R60/SL400/RR3.0/HTP0.0/Tgt9/Loss6)
  W70   = new spread=70 WFO winner    (R90/SL350/RR2.0/HTP0.0/Tgt9/Loss6)

Period: continuous Mar 14 -> Apr 25, $10k, 3% risk, ORB standalone.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig, simulate

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0


def cfg_live():
    return ORBConfig(risk_pct=3.0, range_minutes=60, buffer_pts=0,
                     min_range_pts=200, max_range_pts=5000,
                     fixed_sl_pts=400, rr_ratio=3.0, half_tp_ratio=0.0,
                     pending_expire_minutes=240,
                     daily_target_pct=27.0, daily_loss_pct=18.0,
                     ldn_enabled=True, ldn_start_hour=7,
                     ny_enabled=True, ny_start_hour=13, comment="ORB")


def cfg_w60():
    return ORBConfig(risk_pct=3.0, range_minutes=60, buffer_pts=0,
                     min_range_pts=200, max_range_pts=5000,
                     fixed_sl_pts=400, rr_ratio=3.0, half_tp_ratio=0.0,
                     pending_expire_minutes=240,
                     daily_target_pct=9.0, daily_loss_pct=6.0,
                     ldn_enabled=True, ldn_start_hour=7,
                     ny_enabled=True, ny_start_hour=13, comment="ORB")


def cfg_w70():
    return ORBConfig(risk_pct=3.0, range_minutes=90, buffer_pts=0,
                     min_range_pts=200, max_range_pts=5000,
                     fixed_sl_pts=350, rr_ratio=2.0, half_tp_ratio=0.0,
                     pending_expire_minutes=240,
                     daily_target_pct=9.0, daily_loss_pct=6.0,
                     ldn_enabled=True, ldn_start_hour=7,
                     ny_enabled=True, ny_start_hour=13, comment="ORB")


def run(label, cfg, ticks, m5, m1, meta):
    r = simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
    return dict(label=label, np=r.net_profit, dd=r.max_drawdown,
                dd_pct=r.max_drawdown_pct, trades=r.trades,
                tp=r.tp_count, sl=r.sl_count, other=r.other_count,
                pf=r.profit_factor)


def main() -> int:
    start = datetime(2026, 3, 14, tzinfo=timezone.utc)
    end = datetime(2026, 4, 25, tzinfo=timezone.utc)

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)

        configs = [("LIVE", cfg_live()), ("W60", cfg_w60()), ("W70", cfg_w70())]

        print("=" * 96)
        print("  ORB ET: 3 configs × 2 spreads  (Mar 14 -> Apr 25, $10k, 3% risk standalone)")
        print("=" * 96)

        # Param table
        print("\n  Params:")
        print(f"  {'Config':<6} {'Range':>6} {'BufPt':>6} {'FixSL':>6} {'RR':>5} {'HTP':>5} "
              f"{'Tgt%':>6} {'Loss%':>6}")
        for name, c in configs:
            print(f"  {name:<6} {c.range_minutes:>6} {c.buffer_pts:>6} {c.fixed_sl_pts:>6} "
                  f"{c.rr_ratio:>5} {c.half_tp_ratio:>5} "
                  f"{c.daily_target_pct:>5}% {c.daily_loss_pct:>5}%")

        # Run all combos
        results = {}  # (label, spread) -> result
        for spread in (60, 70):
            ticks = load_ticks(SYMBOL, start, end, spread_pts=spread)
            for name, cfg in configs:
                results[(name, spread)] = run(name, cfg, ticks, m5, m1, meta)

        # Performance table
        print(f"\n  Performance:")
        print(f"  {'Config':<6} {'Spread':>7} {'NP':>10} {'ROI%':>7} {'DD$':>9} {'DD%':>6} "
              f"{'PF':>6} {'Tr':>4} {'TP':>4} {'SL':>4} {'Oth':>5}")
        print("-" * 96)
        for spread in (60, 70):
            for name, _ in configs:
                r = results[(name, spread)]
                roi = r["np"] / DEPOSIT * 100
                print(f"  {name:<6} {spread:>5}pt {r['np']:>+10,.0f} {roi:>+6.1f}% "
                      f"{r['dd']:>+9,.0f} {r['dd_pct']:>5.1f}% "
                      f"{r['pf']:>6.2f} {r['trades']:>4} {r['tp']:>4} "
                      f"{r['sl']:>4} {r['other']:>5}")
            print()

        # Spread sensitivity
        print("  Spread sensitivity (70pt vs 60pt):")
        for name, _ in configs:
            r60 = results[(name, 60)]
            r70 = results[(name, 70)]
            print(f"    {name:<6} NP {r70['np']-r60['np']:+,.0f}  "
                  f"DD {r70['dd_pct']-r60['dd_pct']:+.1f}pp  "
                  f"PF {r70['pf']-r60['pf']:+.2f}  "
                  f"Trades {r70['trades']-r60['trades']:+d}")

        # Cross-config delta vs LIVE
        print("\n  Δ vs LIVE (same spread):")
        for spread in (60, 70):
            live = results[("LIVE", spread)]
            for name, _ in configs:
                if name == "LIVE": continue
                r = results[(name, spread)]
                print(f"    {name}@{spread}pt:  NP {r['np']-live['np']:+,.0f}  "
                      f"DD {r['dd_pct']-live['dd_pct']:+.1f}pp  "
                      f"PF {r['pf']-live['pf']:+.2f}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
