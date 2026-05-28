"""Inspect live spread history vs sim's fixed-30pt assumption.

Pulls live XAUUSD.sc ticks for the window Mon 14:28 → Wed 09:02 UTC,
computes spread (ask-bid) in points per tick, reports summary stats,
and spotlights the spread at each parent SL event from today's live trades.

Run: python scripts/check_live_spread_today.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import MetaTrader5 as mt5
from zgb_sim.mt5_accounts import init_account

SYMBOL = "XAUUSD.sc"
POINT = 0.01
PARENT_MAGICS = {1111, 2222, 3333, 4444, 5555, 6666}

# Window aligned to live_check
START = datetime(2026, 5, 25, 14, 28, tzinfo=timezone.utc)
END   = datetime.now(timezone.utc)


def fetch_ticks(start: datetime, end: datetime) -> pd.DataFrame:
    rng = mt5.copy_ticks_range(SYMBOL, start, end, mt5.COPY_TICKS_INFO)
    if rng is None or len(rng) == 0:
        return pd.DataFrame()
    df = pd.DataFrame(rng)
    df["ts"] = pd.to_datetime(df["time_msc"], unit="ms", utc=True)
    df["spread_pts"] = ((df["ask"] - df["bid"]) / POINT).round().astype(int)
    return df[["ts", "bid", "ask", "spread_pts"]]


def fetch_parent_sls(start: datetime, end: datetime) -> list:
    """Return list of dicts with parent SL OUT-deal info for the window."""
    deals = mt5.history_deals_get(start, end)
    if deals is None:
        return []
    out = []
    for d in deals:
        if d.symbol != SYMBOL: continue
        if d.entry != mt5.DEAL_ENTRY_OUT: continue
        if d.magic not in PARENT_MAGICS: continue
        if d.profit >= 0: continue   # losers only (SL)
        ts = datetime.fromtimestamp(d.time, tz=timezone.utc)
        out.append({"ts": ts, "magic": d.magic, "profit": d.profit, "price": d.price, "volume": d.volume})
    out.sort(key=lambda r: r["ts"])
    return out


def main():
    init_account("live")
    try:
        ticks = fetch_ticks(START, END)
        sls = fetch_parent_sls(START, END)
    finally:
        mt5.shutdown()

    print("=" * 100)
    print(f"  Live spread inspection -- {SYMBOL}")
    print(f"  Window: {START.isoformat()} -> {END.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Sim assumption: 30 pts fixed")
    print("=" * 100)
    if ticks.empty:
        print("  No tick data returned.")
        return 1
    print(f"  Total ticks loaded: {len(ticks):,}")
    s = ticks["spread_pts"]
    print()
    print("  --- Spread distribution (pts) ---")
    print(f"  min   : {int(s.min())}")
    print(f"  p10   : {int(np.percentile(s, 10))}")
    print(f"  p25   : {int(np.percentile(s, 25))}")
    print(f"  median: {int(np.percentile(s, 50))}")
    print(f"  mean  : {s.mean():.1f}")
    print(f"  p75   : {int(np.percentile(s, 75))}")
    print(f"  p90   : {int(np.percentile(s, 90))}")
    print(f"  p99   : {int(np.percentile(s, 99))}")
    print(f"  max   : {int(s.max())}")
    print()

    # Buckets
    print("  --- Spread buckets ---")
    buckets = [(0, 20), (20, 30), (30, 40), (40, 60), (60, 100), (100, 200), (200, 500), (500, 10000)]
    for lo, hi in buckets:
        n = ((s >= lo) & (s < hi)).sum()
        pct = n / len(s) * 100
        bar = "#" * int(pct / 1.5)
        print(f"   {lo:>4} - {hi:>5} pts: {pct:>5.1f}%  ({n:,})  {bar}")
    print()

    # Spread at each parent SL event
    if sls:
        print("  --- Spread at parent-SL moments ---")
        print(f"  {'When (UTC)':<20} {'Magic':>5} {'Lots':>5} {'P&L':>9} {'SL Px':>10} {'Spread pts':>10}")
        ticks_ts = ticks["ts"].values  # numpy datetime64[ns]
        spreads = ticks["spread_pts"].values
        for ev in sls:
            ts_ns = np.datetime64(ev["ts"].replace(tzinfo=None), "ns")
            idx = int(np.searchsorted(ticks_ts, ts_ns, side="right")) - 1
            if idx < 0: idx = 0
            sp = int(spreads[idx])
            tag = "  <-- WIDE" if sp >= 50 else ""
            print(f"  {ev['ts'].strftime('%Y-%m-%d %H:%M:%S'):<20} {ev['magic']:>5} "
                  f"{ev['volume']:>5.2f} ${ev['profit']:>+7.0f} {ev['price']:>10.2f} {sp:>10}{tag}")
        sl_spreads = []
        for ev in sls:
            ts_ns = np.datetime64(ev["ts"].replace(tzinfo=None), "ns")
            idx = int(np.searchsorted(ticks_ts, ts_ns, side="right")) - 1
            if idx >= 0:
                sl_spreads.append(int(spreads[idx]))
        if sl_spreads:
            print()
            print(f"  Median spread at SL moments: {int(np.median(sl_spreads))} pts")
            print(f"  Mean spread at SL moments  : {np.mean(sl_spreads):.1f} pts")
            print(f"  vs sim assumption          : 30 pts")
            print(f"  Spread excess vs sim       : {np.mean(sl_spreads) - 30:+.1f} pts average")
    else:
        print("  No parent SL events found in window.")
    print()

    # Compare spread at SL moments vs typical
    if sls and sl_spreads:
        median_overall = int(np.percentile(s, 50))
        median_at_sl = int(np.median(sl_spreads))
        print("  --- SL vs typical spread ---")
        print(f"  Overall median spread        : {median_overall} pts")
        print(f"  Median spread @ SL events    : {median_at_sl} pts")
        if median_at_sl > median_overall * 1.5:
            print(f"  >>> SLs cluster at WIDE-spread moments ({median_at_sl/median_overall:.1f}x typical)")
        elif median_at_sl > median_overall:
            print(f"  SLs slightly above typical ({median_at_sl/median_overall:.1f}x)")
        else:
            print(f"  SLs are at typical/below typical spread")
    print("=" * 100)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
