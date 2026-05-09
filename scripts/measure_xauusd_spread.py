"""Empirical XAUUSD spread distribution from cached tick history.

Computes actual (ask - bid) per tick across Feb 14 -> May 1, 2026.
Reports overall stats + per-hour-of-day + worst spikes.
Used to size the stress-test bands for WFO winner spread-shock validation.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "output" / "sim_cache"

POINT = 0.01  # XAUUSD point size


def main() -> int:
    files = sorted(CACHE_DIR.glob("ticks_XAUUSD_2026*.parquet"))
    print(f"Loading {len(files)} tick parquets ...")
    dfs = []
    for p in files:
        d = pd.read_parquet(p, columns=["ts", "bid", "ask"])
        dfs.append(d)
    df = pd.concat(dfs, ignore_index=True)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df[(df["ts"] >= "2026-02-14") & (df["ts"] < "2026-05-02")].copy()
    df["spread_pts"] = ((df["ask"] - df["bid"]) / POINT).round().astype("int32")
    df["hour"] = df["ts"].dt.hour
    df["dow"] = df["ts"].dt.dayofweek

    print(f"Total ticks (Feb 14 -> May 1): {len(df):,}")
    print()

    print("=" * 70)
    print("OVERALL SPREAD DISTRIBUTION (points; 1pt = $0.01 = $1/lot)")
    print("=" * 70)
    pcts = [50, 75, 90, 95, 99, 99.9]
    qs = np.percentile(df["spread_pts"], pcts)
    print(f"  Mean:          {df['spread_pts'].mean():.1f} pt")
    print(f"  Median (p50):  {qs[0]:.0f} pt")
    print(f"  p75:           {qs[1]:.0f} pt")
    print(f"  p90:           {qs[2]:.0f} pt")
    print(f"  p95:           {qs[3]:.0f} pt")
    print(f"  p99:           {qs[4]:.0f} pt")
    print(f"  p99.9:         {qs[5]:.0f} pt")
    print(f"  Max:           {df['spread_pts'].max()} pt")
    print()

    print("=" * 70)
    print("PER HOUR-OF-DAY (UTC)")
    print("=" * 70)
    print(f"  {'Hour':>4} {'Med':>6} {'p90':>6} {'p95':>6} {'p99':>6} {'Max':>6} {'Ticks':>10}")
    g = df.groupby("hour")["spread_pts"]
    for hr in range(24):
        if hr not in g.groups: continue
        s = g.get_group(hr)
        med = int(np.percentile(s, 50))
        p90 = int(np.percentile(s, 90))
        p95 = int(np.percentile(s, 95))
        p99 = int(np.percentile(s, 99))
        mx = int(s.max())
        print(f"  {hr:>4} {med:>6} {p90:>6} {p95:>6} {p99:>6} {mx:>6} {len(s):>10,}")
    print()

    print("=" * 70)
    print("TOP 15 SPREAD SPIKES (longest sustained > p99)")
    print("=" * 70)
    p99_threshold = int(np.percentile(df["spread_pts"], 99))
    print(f"  p99 = {p99_threshold} pt — listing individual ticks above 3x p99")
    huge = df[df["spread_pts"] > 3 * p99_threshold].nlargest(15, "spread_pts")
    if len(huge) == 0:
        big = df.nlargest(15, "spread_pts")
        print(f"  (no ticks > {3*p99_threshold} pt; showing top 15 by spread)")
        for _, r in big.iterrows():
            print(f"  {r['ts']}  spread={r['spread_pts']:>4} pt  bid={r['bid']:.2f} ask={r['ask']:.2f}")
    else:
        for _, r in huge.iterrows():
            print(f"  {r['ts']}  spread={r['spread_pts']:>4} pt  bid={r['bid']:.2f} ask={r['ask']:.2f}")
    print()

    print("=" * 70)
    print("STRESS-TEST BAND RECOMMENDATIONS")
    print("=" * 70)
    p95 = int(np.percentile(df["spread_pts"], 95))
    p99 = int(np.percentile(df["spread_pts"], 99))
    p99_9 = int(np.percentile(df["spread_pts"], 99.9))
    median = int(np.percentile(df["spread_pts"], 50))
    print(f"  Median ({median}pt) -- baseline live-match calibration")
    print(f"  p95 ({p95}pt)    -- typical adverse condition")
    print(f"  p99 ({p99}pt)    -- meaningful spike (1% of trades)")
    print(f"  p99.9 ({p99_9}pt) -- rare news-driven spike (0.1% of trades)")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
