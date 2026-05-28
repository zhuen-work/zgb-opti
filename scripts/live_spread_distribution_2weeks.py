"""Audit per-hour live spread distribution for XAUUSD.sc over the last 2 weeks.

Tests whether the "30pt live-match" assumption holds across LDN + NY sessions
+ news windows.
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

END = datetime.now(timezone.utc)
START = END - timedelta(days=14)


def main():
    init_account("live")
    try:
        rng = mt5.copy_ticks_range(SYMBOL, START, END, mt5.COPY_TICKS_INFO)
    finally:
        mt5.shutdown()

    if rng is None or len(rng) == 0:
        print("No ticks returned.")
        return 1

    df = pd.DataFrame(rng)
    df["ts"] = pd.to_datetime(df["time_msc"], unit="ms", utc=True)
    df["spread_pts"] = ((df["ask"] - df["bid"]) / POINT).round().astype(int)
    df["hour_utc"] = df["ts"].dt.hour
    df["dow"] = df["ts"].dt.day_name()
    print(f"Loaded {len(df):,} ticks from {df['ts'].min()} to {df['ts'].max()}")

    print()
    print("=" * 110)
    print(f"  Per-hour spread distribution (XAUUSD.sc, last 14 days, {len(df):,} ticks)")
    print("=" * 110)
    print(f"  {'Hour UTC':<10} {'Session':<12} {'Median':>7} {'Mean':>7} {'p75':>5} {'p90':>5} "
          f"{'p99':>5} {'Max':>5} {'>30pt%':>8} {'>50pt%':>8}")
    print("  " + "-" * 92)

    def session(h):
        if 0 <= h < 7: return "Asia/early"
        if 7 <= h < 12: return "LDN"
        if 12 <= h < 17: return "LDN+NY overlap"
        if 17 <= h < 22: return "NY late"
        return "rollover"

    by_hour = df.groupby("hour_utc")
    for hr in range(24):
        if hr not in by_hour.groups:
            continue
        g = by_hour.get_group(hr)
        s = g["spread_pts"]
        n = len(s)
        if n < 100:
            continue
        med = int(np.percentile(s, 50))
        mean = s.mean()
        p75 = int(np.percentile(s, 75))
        p90 = int(np.percentile(s, 90))
        p99 = int(np.percentile(s, 99))
        mx = int(s.max())
        gt30 = (s > 30).mean() * 100
        gt50 = (s > 50).mean() * 100
        sess = session(hr)
        flag = "  <-- WIDE" if med > 30 else ("  <-- 30pt+" if p90 > 30 else "")
        print(f"  {hr:>2}:00 UTC  {sess:<12} {med:>7d} {mean:>7.1f} {p75:>5d} {p90:>5d} "
              f"{p99:>5d} {mx:>5d} {gt30:>7.1f}% {gt50:>7.1f}%{flag}")
    print()

    print("=" * 110)
    print(f"  Overall summary")
    print("=" * 110)
    s = df["spread_pts"]
    print(f"  All-tick median: {int(np.percentile(s, 50))}  mean: {s.mean():.1f}")
    print(f"  p75: {int(np.percentile(s, 75))}  p90: {int(np.percentile(s, 90))}  "
          f"p99: {int(np.percentile(s, 99))}  max: {int(s.max())}")
    print(f"  % ticks > 30pt: {(s > 30).mean() * 100:.2f}%")
    print(f"  % ticks > 50pt: {(s > 50).mean() * 100:.2f}%")
    print(f"  % ticks > 100pt: {(s > 100).mean() * 100:.2f}%")

    # Active trading hours (LDN + NY where the EA actually trades)
    active = df[df["hour_utc"].isin(range(7, 17))]
    sa = active["spread_pts"]
    print(f"\n  Active hours (07:00-17:00 UTC) only — {len(active):,} ticks:")
    print(f"    median: {int(np.percentile(sa, 50))}  mean: {sa.mean():.1f}  "
          f"p90: {int(np.percentile(sa, 90))}  p99: {int(np.percentile(sa, 99))}  "
          f"max: {int(sa.max())}")
    print(f"    % > 30pt: {(sa > 30).mean() * 100:.2f}%  "
          f"% > 50pt: {(sa > 50).mean() * 100:.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
