"""Check the hour-of-day distribution in WFO tick parquets vs MT5 fresh-pull.

If parquets are broker-time-labeled-as-UTC, hour=7 in parquet = real UTC 04:00.
If parquets are real-UTC, hour=7 in parquet = real UTC 07:00.
"""
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

# WFO uses load_ticks which caches to output/sim_cache/.parquet
cache_dir = ROOT / "output" / "sim_cache"
print(f"Cache dir: {cache_dir}")
files = sorted(cache_dir.glob("*.parquet"))
print(f"Found {len(files)} parquet files\n")

if not files:
    print("No tick parquets found.")
    sys.exit(1)

# Pick the most recent (largest, latest)
target = max(files, key=lambda p: p.stat().st_mtime)
print(f"Inspecting: {target.name}")
df = pd.read_parquet(target)
print(f"  rows: {len(df):,}")
print(f"  ts range: {df['ts'].min()} -> {df['ts'].max()}")
print(f"  ts dtype: {df['ts'].dtype}")
print()

# Hour distribution
df["hr"] = pd.to_datetime(df["ts"]).dt.hour
hr_counts = df.groupby("hr").size()
print("Hour-of-day tick counts (top by count):")
for hr in range(24):
    n = hr_counts.get(hr, 0)
    bar = "#" * int(n / hr_counts.max() * 50) if hr_counts.max() > 0 else ""
    marker = ""
    if hr == 7:
        marker = " <-- WFO ldn_start_hour"
    if hr == 13:
        marker = " <-- WFO ny_start_hour"
    print(f"  {hr:02d}:00  {n:>10,}  {bar}{marker}")

# Sanity: gold has lowest activity around 21-22 UTC (US close, Asia not yet open) in REAL UTC.
# In BROKER UTC+3, lowest activity hours would shift to 00-01 in the labels.
print()
print("Interpretation:")
print("  REAL UTC: lowest tick volume is hours 21-22 (US close → Asia open gap).")
print("  BROKER UTC+3 labels: lowest hours would be 00-01 instead.")
zero_hr = df.groupby("hr").size().idxmin()
print(f"  This parquet's lowest hour: {zero_hr:02d}:00")
print(f"  -> If lowest = 21 or 22: parquet is REAL UTC")
print(f"  -> If lowest = 00 or 01: parquet is BROKER-TIME labeled-as-UTC")
