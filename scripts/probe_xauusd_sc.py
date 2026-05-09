"""Probe what tick data Vantage actually exposes for XAUUSD.sc."""
from __future__ import annotations
import sys
from datetime import datetime, timedelta, timezone
import MetaTrader5 as mt5
import pandas as pd

SYM = "XAUUSD.sc"

if not mt5.initialize():
    print(f"init failed: {mt5.last_error()}")
    sys.exit(1)

try:
    print(f"\nTerminal info:")
    ti = mt5.terminal_info()
    print(f"  name: {ti.name}  build: {ti.build}  connected: {ti.connected}")
    print(f"  data_path: {ti.data_path}")
    print(f"  company: {ti.company}")

    print(f"\n{SYM} info:")
    si = mt5.symbol_info(SYM)
    if si is None:
        print(f"  not found")
        sys.exit(1)
    print(f"  digits={si.digits} point={si.point} spread={si.spread} trade_mode={si.trade_mode}")
    print(f"  bid={si.bid} ask={si.ask} last={si.last}")
    print(f"  visible={si.visible} session_deals={si.session_deals}")

    if not si.visible:
        mt5.symbol_select(SYM, True)
        print("  (selected)")

    # Try copy_ticks_from with N most recent
    print(f"\nLast 100 ticks via copy_ticks_from(now-1d, 100):")
    end = datetime.now(timezone.utc)
    ticks = mt5.copy_ticks_from(SYM, end - timedelta(days=1), 100, mt5.COPY_TICKS_ALL)
    if ticks is None or len(ticks) == 0:
        print(f"  empty. last_error: {mt5.last_error()}")
    else:
        print(f"  got {len(ticks)} ticks")
        df = pd.DataFrame(ticks)
        df["ts"] = pd.to_datetime(df["time_msc"], unit="ms", utc=True)
        print(df.head(5).to_string())
        print(f"  ts range: {df['ts'].min()}  ->  {df['ts'].max()}")

    # Try copy_rates_range for M1 bars
    print(f"\nM1 bars Apr 28 -> May 2 via copy_rates_range:")
    s = datetime(2026, 4, 28, tzinfo=timezone.utc)
    e = datetime(2026, 5, 2, tzinfo=timezone.utc)
    bars = mt5.copy_rates_range(SYM, mt5.TIMEFRAME_M1, s, e)
    if bars is None or len(bars) == 0:
        print(f"  empty. last_error: {mt5.last_error()}")
    else:
        df = pd.DataFrame(bars)
        df["ts"] = pd.to_datetime(df["time"], unit="s", utc=True)
        print(f"  got {len(df)} bars")
        print(f"  ts range: {df['ts'].min()}  ->  {df['ts'].max()}")
        print(f"  close range: {df['close'].min():.2f}  ->  {df['close'].max():.2f}")

    # Try copy_ticks_range Apr 28 -> May 2
    print(f"\nTicks Apr 28 -> May 2 via copy_ticks_range:")
    ticks = mt5.copy_ticks_range(SYM, s, e, mt5.COPY_TICKS_ALL)
    if ticks is None or len(ticks) == 0:
        print(f"  empty. last_error: {mt5.last_error()}")
    else:
        print(f"  got {len(ticks)} ticks")

finally:
    mt5.shutdown()
