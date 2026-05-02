"""Apply a synthetic fixed spread to ticks (override real bid/ask) and compare.

Tests sim's spread sensitivity. Phase B winner config, Mar 14 -> Apr 25 span.
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone, date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd

from zgb_sim.tick_loader import load_ticks, load_bars, symbol_meta, kill_mt5_terminal
from zgb_sim.scalper_v1 import S1Config, SymbolMeta
from zgb_sim.scalper_v1_fast import simulate_fast


CFG = S1Config(
    risk_pct=1.0, donchian_bars=35,
    take_profit_pts=300, stop_loss_pts=40, half_tp_ratio=0.7,
    pending_expire_bars=2, start_hour=14, end_hour=22,
    daily_target_pct=15.0, daily_loss_pct=6.0,
    block_fri_pm=True, hedge_mode=False,
)
DEPOSIT = 100.0


def apply_spread(ticks: pd.DataFrame, spread_pts: int, point: float) -> pd.DataFrame:
    """Override real bid/ask with a fixed synthetic spread around mid-price."""
    mid = (ticks["bid"] + ticks["ask"]) / 2
    half = spread_pts * point / 2.0
    out = ticks.copy()
    out["bid"] = mid - half
    out["ask"] = mid + half
    return out


def real_spread_stats(ticks: pd.DataFrame, point: float):
    spread_pts = (ticks["ask"] - ticks["bid"]) / point
    return {
        "mean": float(spread_pts.mean()),
        "median": float(spread_pts.median()),
        "p95": float(spread_pts.quantile(0.95)),
        "max": float(spread_pts.max()),
    }


def main():
    try:
        m = symbol_meta("XAUUSD")
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )

        start = datetime(2026, 3, 14, tzinfo=timezone.utc)
        end = datetime(2026, 4, 25, tzinfo=timezone.utc)

        print("Loading data...")
        ticks = load_ticks("XAUUSD", start, end)
        m1 = load_bars("XAUUSD", "M1", start, end)
        m5 = load_bars("XAUUSD", "M5", start, end)

        rs = real_spread_stats(ticks, meta.point)
        print(f"\nReal spread stats (pts): mean={rs['mean']:.1f}  "
              f"median={rs['median']:.1f}  p95={rs['p95']:.1f}  max={rs['max']:.0f}")

        results = []

        # Real spread (no override)
        print("\n[Real spread]")
        t0 = time.time()
        r = simulate_fast(ticks, m5, m1, CFG, meta, DEPOSIT)
        print(f"  {time.time()-t0:.1f}s  {r.summary()}")
        results.append(("Real (variable)", r))

        # Fixed 45 pts
        print("\n[Fixed spread 45 pts]")
        ticks_45 = apply_spread(ticks, 45, meta.point)
        t0 = time.time()
        r = simulate_fast(ticks_45, m5, m1, CFG, meta, DEPOSIT)
        print(f"  {time.time()-t0:.1f}s  {r.summary()}")
        results.append(("Fixed 45", r))

        # Fixed 90 pts
        print("\n[Fixed spread 90 pts]")
        ticks_90 = apply_spread(ticks, 90, meta.point)
        t0 = time.time()
        r = simulate_fast(ticks_90, m5, m1, CFG, meta, DEPOSIT)
        print(f"  {time.time()-t0:.1f}s  {r.summary()}")
        results.append(("Fixed 90", r))

        # Summary
        print("\n" + "=" * 72)
        print("  SPREAD COMPARISON (Phase B winner, Mar 14 -> Apr 25)")
        print("=" * 72)
        print(f"  {'Variant':<22}{'NP':>10}{'ROI%':>9}{'PF':>7}{'DD%':>8}{'Trades':>8}")
        for name, r in results:
            roi = r.net_profit / DEPOSIT * 100.0
            print(f"  {name:<22}{r.net_profit:>+10,.2f}{roi:>+8.1f}%"
                  f"{r.profit_factor:>7.2f}{r.max_drawdown_pct:>7.1f}%{r.trades:>8}")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
