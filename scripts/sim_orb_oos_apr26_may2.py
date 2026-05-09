"""ORB-only OOS test: Apr 26 -> May 2, 2026 (one trading week past the WFO IS).

Uses current production params from the live setfiles:
  Range=90, FixSL=350, RR=2.0, HalfTP=0.0, target=9, loss=6 (3% baseline).

Checks how the live ORB config actually performed on the most recent week
that wasn't part of any WFO sweep.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
WFO_BASELINE = 3.0


def run_orb(risk: float, ticks, m1, m5, meta, range_min: int, fixed_sl: int, rr: float) -> dict:
    cfg = ORBConfig(
        risk_pct=risk, range_minutes=range_min, buffer_pts=0,
        min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=fixed_sl, rr_ratio=rr, half_tp_ratio=0.0,
        pending_expire_minutes=240,
        daily_target_pct=9.0 * (risk / WFO_BASELINE),
        daily_loss_pct=6.0 * (risk / WFO_BASELINE),
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13, comment="ORB",
    )
    r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
    return dict(np=r.net_profit, dd_pct=r.max_drawdown_pct, trades=r.trades,
                pf=r.profit_factor)


def main() -> int:
    # End set to May 1 23:50 UTC — cache covers through 23:56; May 2 is Sat (non-trading).
    start = datetime(2026, 4, 26, tzinfo=timezone.utc)
    end = datetime(2026, 5, 1, 23, 50, tzinfo=timezone.utc)
    days = (end - start).days

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)
        print("=" * 80)
        print(f"  ORB-only OOS: Apr 26 -> May 2 ({days}d, $10k)")
        print(f"  Live config: Range=60/SL=400/RR=3.0  (matches account 11507157)")
        print("=" * 80)

        for sp in (70, 35, 23):
            ticks = load_ticks(SYMBOL, start, end, spread_pts=sp)
            print(f"\n  --- Spread {sp}pt ---")
            print(f"  {'Risk':>5} {'NP':>10} {'ROI':>8} {'DD%':>6} {'NP/DD':>7} {'Trades':>7} {'PF':>5}")
            for risk in (2.0, 3.0, 4.5):
                r = run_orb(risk, ticks, m1, m5, meta, 60, 400, 3.0)
                roi = r["np"] / DEPOSIT * 100
                ndd = r["np"] / max(r["dd_pct"], 0.01) if r["dd_pct"] > 0 else 0
                print(f"  {risk:>4}%  {r['np']:>+10,.0f} {roi:>+7.1f}% "
                      f"{r['dd_pct']:>5.1f}% {ndd:>7.2f} {r['trades']:>7} {r['pf']:>5.2f}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
