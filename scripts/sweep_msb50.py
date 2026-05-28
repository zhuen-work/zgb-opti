"""Quick param sweep for MSB50_v1. Goal: find any (range_mode x msb_mode x rr x
sl_buffer x tol x pivot_n) cell that shows positive edge before declaring the
strategy dead.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from itertools import product

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
    ap.add_argument("--top", type=int, default=15)
    args = ap.parse_args()

    start = to_utc(args.start); end = to_utc(args.end)
    print(f"MSB50_v1 sweep: {SYMBOL} {args.start} -> {args.end} (spread={args.spread}pt)")
    print("=" * 100)

    m = symbol_meta(SYMBOL)
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])
    print("  Loading data...")
    ticks = load_ticks(SYMBOL, start, end, spread_pts=args.spread)
    m5 = load_bars(SYMBOL, "M5", start, end)
    print(f"  ticks={len(ticks):,}  m5={len(m5):,}\n")

    grid = list(product(
        [RANGE_IMPULSE, RANGE_SESSION],          # range_mode
        [MSB_MINOR_SWING, MSB_DONCHIAN],         # msb_mode
        [2, 3],                                   # pivot_n
        [5, 20, 50],                              # tol_pts
        [50, 200],                                # sl_buffer_pts
        [1.5, 2.0, 3.0],                          # rr_ratio
        [10, 30],                                 # n_donch (only when msb_mode=donchian)
    ))
    # Dedup: n_donch only matters when msb_mode=donchian
    seen = set()
    deduped = []
    for combo in grid:
        rmode, mmode, pn, tol, slb, rr, nd = combo
        key = (rmode, mmode, pn, tol, slb, rr, nd if mmode == MSB_DONCHIAN else None)
        if key in seen: continue
        seen.add(key)
        deduped.append(combo)
    print(f"  Configs: {len(deduped)}\n")

    results = []
    for i, (rmode, mmode, pn, tol, slb, rr, nd) in enumerate(deduped):
        cfg = MSB50Config(
            risk_pct=1.0, pivot_n=pn,
            range_mode=rmode, msb_mode=mmode,
            tol_pts=tol, n_donch=nd,
            sl_buffer_pts=slb, rr_ratio=rr,
            max_spread_pts=70, arm_timeout_bars=24,
        )
        res = simulate_msb50(ticks, m5, cfg, meta, initial_balance=args.balance)
        roi = res.net_profit / args.balance * 100.0
        np_dd = (res.net_profit / res.max_drawdown) if res.max_drawdown > 0 else 0.0
        results.append({
            "rmode": rmode, "mmode": mmode, "pn": pn, "tol": tol,
            "slb": slb, "rr": rr, "nd": nd if mmode == MSB_DONCHIAN else None,
            "np": res.net_profit, "roi": roi, "pf": res.profit_factor,
            "dd": res.max_drawdown_pct, "trades": res.trades,
            "tp": res.tp_count, "sl": res.sl_count, "np_dd": np_dd,
        })
        if (i + 1) % 20 == 0:
            print(f"  [{i+1}/{len(deduped)}] best so far: NP={max(r['np'] for r in results):+.0f}")

    print()
    results.sort(key=lambda r: r["np"], reverse=True)
    print(f"TOP {args.top} BY NET PROFIT:")
    print(f"{'rng':>8} {'msb':>13} {'pn':>3} {'tol':>4} {'slb':>4} {'rr':>4} {'nd':>4} | "
          f"{'NP':>9} {'ROI':>6} {'PF':>5} {'DD%':>5} {'Trd':>4} {'TP':>3} {'SL':>3} {'NP/DD':>6}")
    print("-" * 120)
    for r in results[:args.top]:
        nd_str = str(r["nd"]) if r["nd"] is not None else "-"
        print(f"{r['rmode']:>8} {r['mmode']:>13} {r['pn']:>3} {r['tol']:>4} "
              f"{r['slb']:>4} {r['rr']:>4} {nd_str:>4} | "
              f"{r['np']:>+9,.0f} {r['roi']:>+5.1f}% {r['pf']:>5.2f} "
              f"{r['dd']:>4.1f}% {r['trades']:>4} {r['tp']:>3} {r['sl']:>3} {r['np_dd']:>6.2f}")

    # Also dump bottom 5 for sanity
    print(f"\nBOTTOM 5:")
    for r in results[-5:]:
        nd_str = str(r["nd"]) if r["nd"] is not None else "-"
        print(f"{r['rmode']:>8} {r['mmode']:>13} {r['pn']:>3} {r['tol']:>4} "
              f"{r['slb']:>4} {r['rr']:>4} {nd_str:>4} | "
              f"{r['np']:>+9,.0f} {r['roi']:>+5.1f}% {r['pf']:>5.2f} "
              f"{r['dd']:>4.1f}% {r['trades']:>4} {r['tp']:>3} {r['sl']:>3} {r['np_dd']:>6.2f}")

    # Stats summary
    pos_count = sum(1 for r in results if r["np"] > 0)
    print(f"\nSUMMARY: {pos_count}/{len(results)} configs positive  "
          f"best NP=${results[0]['np']:+,.0f}  worst NP=${results[-1]['np']:+,.0f}")


if __name__ == "__main__":
    main()
