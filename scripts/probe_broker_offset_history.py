"""Phase 1: Verify broker timestamp offset is structural across multiple days.

Checks:
1. Latest tick timestamp vs system UTC right now
2. Last 5 days' worth: pull 1 tick from each day, compare timestamp to known-good wall clock
3. Find a known event (NY market open, when XAUUSD volume spikes) and check
   what hour it appears at in MT5 timestamps vs known real-UTC time
"""
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.mt5_accounts import init_account
from zgb_sim.tick_loader import kill_mt5_terminal
import MetaTrader5 as mt5
import pandas as pd

print("=" * 90)
print("  Phase 1: Broker offset verification")
print("=" * 90)

spec = init_account("live")
sym = spec.symbol
try:
    sys_utc = datetime.now(timezone.utc)
    print(f"\n[1] System UTC now: {sys_utc}")
    mt5.symbol_select(sym, True)
    import time; time.sleep(2)
    si = mt5.symbol_info_tick(sym)
    if si:
        tick_t = datetime.fromtimestamp(si.time, tz=timezone.utc)
        offset_h = (tick_t - sys_utc).total_seconds() / 3600
        print(f"    Latest {sym} tick: {tick_t}")
        print(f"    Offset (broker - real UTC): {offset_h:+.2f} hours")

    # [2] Pull tick stream for last 5 days, find min/max timestamps per day
    print(f"\n[2] Tick stream presence per day (last 7 days):")
    print(f"    A consistent +3h offset means tick timestamps are broker-time labeled-as-UTC.")
    end = sys_utc
    start = end - timedelta(days=7)
    arr = mt5.copy_ticks_range(sym, start, end, mt5.COPY_TICKS_ALL)
    if arr is not None and len(arr):
        df = pd.DataFrame(arr)
        df["ts"] = pd.to_datetime(df["time_msc"], unit="ms", utc=True)
        df["date"] = df["ts"].dt.date
        for d in sorted(df["date"].unique()):
            day = df[df["date"] == d]
            print(f"    {d}: {len(day):>7,} ticks  earliest={day.ts.min().strftime('%H:%M')}  latest={day.ts.max().strftime('%H:%M')}")

    # [3] Volume-by-hour heatmap. Real LDN open = ~07-08 UTC, NY open = 13-14 UTC.
    # If broker-time labeled, peaks shift +3h to 10-11 and 16-17.
    print(f"\n[3] Hour-of-day tick volume distribution (last 7 days):")
    if arr is not None and len(arr):
        df["hr"] = df["ts"].dt.hour
        hr = df.groupby("hr").size()
        max_n = hr.max()
        for h in range(24):
            n = hr.get(h, 0)
            bar = "#" * int(n / max_n * 50) if max_n else ""
            mark = ""
            if h in (7, 8): mark += " [real-UTC LDN open]"
            if h in (10, 11): mark += " [+3h shift if broker-labeled]"
            if h in (13, 14): mark += " [real-UTC NY open]"
            if h in (16, 17): mark += " [+3h shift if broker-labeled]"
            print(f"    {h:02d}:00 {n:>9,}  {bar}{mark}")

    # [4] Verdict
    print(f"\n[4] Verdict:")
    if si:
        if abs(offset_h - 3) < 0.5:
            print(f"    Broker is ~3h AHEAD of real UTC (broker = UTC+3, currently EEST or DST).")
        elif abs(offset_h) < 0.1:
            print(f"    Broker is at REAL UTC (no offset). Memory was correct.")
        elif abs(offset_h - 2) < 0.5:
            print(f"    Broker is ~2h AHEAD of real UTC (broker = UTC+2, EET winter).")
        else:
            print(f"    Broker offset = {offset_h:+.2f}h (unexpected).")
finally:
    mt5.shutdown()
    kill_mt5_terminal()
