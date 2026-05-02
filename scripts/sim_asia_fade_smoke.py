"""Asia-fade smoke test: param-probe over a small grid on Feb 14 -> Apr 25.

Goal: confirm the strategy generates trades and shows positive expectancy
before committing to a full WFO. 70-day period, $10k, 3% risk, spread=70.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.asia_fade import AsiaFadeConfig, simulate as af_simulate

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
RISK = 3.0


def main() -> int:
    start = datetime(2026, 2, 14, tzinfo=timezone.utc)
    end = datetime(2026, 4, 25, tzinfo=timezone.utc)

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        ticks = load_ticks(SYMBOL, start, end)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)

        print("=" * 92)
        print("  AsiaFade smoke probe (Feb 14 -> Apr 25, $10k, 3% risk, spread=70, 70 days)")
        print("=" * 92)

        # Tiny probe: 4 combos covering corners of the param space (slow pure-Python sim).
        # If any are profitable we Numba-jit + run a full WFO.
        results = []
        for sl_pts, rr, buf in (
            (150, 1.0, 0),    # tight scalp
            (250, 1.5, 30),   # balanced
            (400, 2.0, 30),   # wide swing
            (250, 2.0, 0),    # mid-SL high-RR
        ):
            for _ in (0,):  # single iteration; structure preserved for compatibility
                for __ in (0,):
                    cfg = AsiaFadeConfig(
                        risk_pct=RISK,
                        range_start_hour=0, range_end_hour=6,
                        fade_close_hour=13,
                        buffer_pts=buf, sl_pts=sl_pts, rr_ratio=rr,
                        half_tp_ratio=0.0,
                        min_range_pts=200, max_range_pts=10000,
                        daily_target_pct=9.0, daily_loss_pct=6.0,
                        comment="AsiaFade",
                    )
                    r = af_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
                    results.append((sl_pts, rr, buf, r))

        # Rank by NP/DD
        rows = []
        for sl_pts, rr, buf, r in results:
            ndd = (r.net_profit / r.max_drawdown) if r.max_drawdown > 0 else 0
            wr = (r.tp_count / r.trades * 100) if r.trades > 0 else 0
            rows.append((sl_pts, rr, buf, r, ndd, wr))
        rows.sort(key=lambda t: -t[4])  # by NP/DD desc

        days = (end - start).days
        print(f"\n  {'SL':>5} {'RR':>5} {'Buf':>4} {'Days':>5} {'NP':>9} {'ROI%':>7} "
              f"{'DD%':>6} {'NP/DD':>7} {'Tr':>4} {'TP':>4} {'SL':>4} {'WR%':>5} {'PF':>5}")
        for sl_pts, rr, buf, r, ndd, wr in rows:
            roi = r.net_profit / DEPOSIT * 100
            print(f"  {sl_pts:>5} {rr:>5.1f} {buf:>4} {days:>5} {r.net_profit:>+9,.0f} "
                  f"{roi:>+6.1f}% {r.max_drawdown_pct:>5.1f}% {ndd:>7.2f} "
                  f"{r.trades:>4} {r.tp_count:>4} {r.sl_count:>4} {wr:>4.1f}% "
                  f"{r.profit_factor:>5.2f}")

        # Count profitable
        n_prof = sum(1 for *_, r, _, _ in rows if r.net_profit > 0)
        print(f"\n  Profitable combos: {n_prof}/{len(rows)}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
