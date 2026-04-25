"""Run BOTH S1 and S2 winners concurrently on a shared account balance.

Reports per-window OOS performance + full-span sanity ET.
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone, date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import load_ticks, load_bars, symbol_meta, kill_mt5_terminal
from zgb_sim.scalper_v1 import S1Config, SymbolMeta
from zgb_sim.scalper_v1_combined import simulate_combined


# Latest WFO winners
S1 = S1Config(
    risk_pct=1.0, donchian_bars=30,
    take_profit_pts=150, stop_loss_pts=50, half_tp_ratio=0.6,
    pending_expire_bars=2, start_hour=14, end_hour=22,
    daily_target_pct=9.0, daily_loss_pct=12.0,
    block_fri_pm=True, hedge_mode=False,
)
S2 = S1Config(
    risk_pct=1.0, donchian_bars=15,
    take_profit_pts=200, stop_loss_pts=50, half_tp_ratio=0.6,
    pending_expire_bars=2, start_hour=1, end_hour=8,
    daily_target_pct=3.0, daily_loss_pct=12.0,
    block_fri_pm=True, hedge_mode=False,
)

WINDOWS = [
    ("W1", date(2026, 3, 14), date(2026, 3, 28)),
    ("W2", date(2026, 3, 28), date(2026, 4, 11)),
    ("W3", date(2026, 4, 11), date(2026, 4, 25)),
]
DEPOSIT = 100.0


def _utc(d):
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def main():
    try:
        m = symbol_meta("XAUUSD")
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )

        print("=" * 72)
        print("  COMBINED OOS — S1 + S2 sharing one account balance")
        print(f"  S1: Donch=30 TP=150 SL=50 HTP=0.6 Tgt=9%  hours 14-22 UTC")
        print(f"  S2: Donch=15 TP=200 SL=50 HTP=0.6 Tgt=3%  hours 1-8 UTC")
        print(f"  Risk: 1% per stream, Deposit ${DEPOSIT}")
        print("=" * 72)

        results = []
        for label, start, end in WINDOWS:
            print(f"\n--- {label} OOS ({start} -> {end}) ---")
            ticks = load_ticks("XAUUSD", _utc(start), _utc(end))
            m1 = load_bars("XAUUSD", "M1", _utc(start), _utc(end))
            m5 = load_bars("XAUUSD", "M5", _utc(start), _utc(end))
            t0 = time.time()
            r = simulate_combined(ticks, m5, m1, [S1, S2], meta, DEPOSIT, ["S1", "S2"])
            print(f"  {label} done in {time.time()-t0:.1f}s")
            print(f"  COMBINED: {r.summary()}")
            results.append((label, r))

        # Full-span sanity ET
        print("\n" + "=" * 72)
        print("  FULL OOS SPAN: Mar 14 -> Apr 25")
        print("=" * 72)
        ticks = load_ticks("XAUUSD", _utc(date(2026, 3, 14)), _utc(date(2026, 4, 25)))
        m1 = load_bars("XAUUSD", "M1", _utc(date(2026, 3, 14)), _utc(date(2026, 4, 25)))
        m5 = load_bars("XAUUSD", "M5", _utc(date(2026, 3, 14)), _utc(date(2026, 4, 25)))
        t0 = time.time()
        r_full = simulate_combined(ticks, m5, m1, [S1, S2], meta, DEPOSIT, ["S1", "S2"])
        print(f"  Done in {time.time()-t0:.1f}s")
        print(f"  COMBINED FULL-SPAN: {r_full.summary()}")

        # Per-window summary table
        print("\n" + "=" * 72)
        print("  COMBINED PER-WINDOW SUMMARY")
        print("=" * 72)
        print(f"  {'Window':<8}{'NP':>10}{'ROI%':>8}{'PF':>7}{'DD%':>7}{'Trades':>8}")
        for label, r in results:
            roi = r.net_profit / DEPOSIT * 100.0
            print(f"  {label:<8}{r.net_profit:>+10,.2f}{roi:>+7.1f}%"
                  f"{r.profit_factor:>7.2f}{r.max_drawdown_pct:>6.1f}%{r.trades:>8}")
        total_np = sum(r.net_profit for _, r in results)
        avg_roi = total_np / (DEPOSIT * len(results)) * 100.0
        print(f"  {'Total':<8}{total_np:>+10,.2f}{avg_roi:>+7.1f}% (avg/win)")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
