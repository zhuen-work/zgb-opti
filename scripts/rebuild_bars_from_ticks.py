"""Rebuild bar parquet caches from tick parquet caches.

Workaround when MT5 IPC is unavailable. Resamples ticks (using mid price)
to OHLC bars at M1/M5/M15/M30 and writes to the standard cache locations.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "output" / "sim_cache"


def main():
    # Concatenate all tick months
    months = []
    for f in sorted(CACHE.glob("ticks_XAUUSD_*.parquet")):
        df = pd.read_parquet(f)
        months.append(df)
        print(f"  Loaded {f.name}: {len(df):,} ticks")
    ticks = pd.concat(months, ignore_index=True)
    ticks["mid"] = (ticks["bid"] + ticks["ask"]) / 2
    ticks["dt"] = pd.to_datetime(ticks["ts"])
    if ticks["dt"].dt.tz is None:
        ticks["dt"] = ticks["dt"].dt.tz_localize("UTC")
    ticks = ticks.set_index("dt")
    print(f"\n  Total ticks: {len(ticks):,}")
    print(f"  Range: {ticks.index.min()} -> {ticks.index.max()}")

    rules = {"M1": "1min", "M5": "5min", "M15": "15min", "M30": "30min"}
    for tf, rule in rules.items():
        bars = ticks["mid"].resample(rule).agg(["first", "max", "min", "last"]).dropna()
        bars.columns = ["open", "high", "low", "close"]
        bars = bars.reset_index().rename(columns={"dt": "ts"})
        if bars["ts"].dt.tz is None:
            bars["ts"] = bars["ts"].dt.tz_localize("UTC")
        out = CACHE / f"bars_XAUUSD_{tf}.parquet"
        bars.to_parquet(out, index=False)
        print(f"  {tf}: {len(bars):,} bars -> {out.name}")
        print(f"    Range: {bars['ts'].min()} -> {bars['ts'].max()}")


if __name__ == "__main__":
    main()
