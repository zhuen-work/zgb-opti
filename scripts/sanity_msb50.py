"""Sanity check for MSB50_v1 simulator. Runs the 2x2 mechanical matrix on a
recent window and prints summary stats.

Usage:
    python -m scripts.sanity_msb50 [--start YYYY-MM-DD] [--end YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from zgb_sim.tick_loader import load_ticks, load_bars, symbol_meta
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.msb50 import (
    MSB50Config, simulate_msb50,
    RANGE_IMPULSE, RANGE_SESSION,
    MSB_MINOR_SWING, MSB_DONCHIAN,
)


SYMBOL = "XAUUSD"


def to_utc(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-04-01")
    ap.add_argument("--end", default="2026-04-25")
    ap.add_argument("--spread", type=int, default=60)
    ap.add_argument("--balance", type=float, default=10_000.0)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    start = to_utc(args.start); end = to_utc(args.end)
    print(f"MSB50_v1 sanity: {SYMBOL} {args.start} -> {args.end} (spread={args.spread}pt, $${args.balance:,.0f})")
    print("=" * 100)

    m = symbol_meta(SYMBOL)
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])

    print(f"  Loading ticks + M5 bars...")
    ticks = load_ticks(SYMBOL, start, end, spread_pts=args.spread)
    m5 = load_bars(SYMBOL, "M5", start, end)
    print(f"  ticks={len(ticks):,}  m5={len(m5):,}  "
          f"first={ticks['ts'].iloc[0]}  last={ticks['ts'].iloc[-1]}")

    matrix = [
        (RANGE_IMPULSE, MSB_MINOR_SWING),
        (RANGE_IMPULSE, MSB_DONCHIAN),
        (RANGE_SESSION, MSB_MINOR_SWING),
        (RANGE_SESSION, MSB_DONCHIAN),
    ]
    print()
    print(f"{'range':>10} {'msb':>14} | {'NP':>10} {'ROI%':>7} {'PF':>6} "
          f"{'DD%':>6} {'Trd':>5} {'TP':>4} {'SL':>4}")
    print("-" * 100)
    for rmode, mmode in matrix:
        cfg = MSB50Config(
            risk_pct=1.0,
            pivot_n=2,
            range_mode=rmode,
            msb_mode=mmode,
            tol_pts=10,
            n_donch=20,
            sl_buffer_pts=50,
            rr_ratio=2.0,
            max_spread_pts=70,
            arm_timeout_bars=24,
        )
        res = simulate_msb50(ticks, m5, cfg, meta, initial_balance=args.balance,
                             debug=args.debug)
        roi = res.net_profit / args.balance * 100.0
        print(f"{rmode:>10} {mmode:>14} | {res.net_profit:>+10,.0f} {roi:>+6.1f}% "
              f"{res.profit_factor:>6.2f} {res.max_drawdown_pct:>5.1f}% "
              f"{res.trades:>5} {res.tp_count:>4} {res.sl_count:>4}")

    print()


if __name__ == "__main__":
    main()
