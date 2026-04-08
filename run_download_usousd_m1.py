"""Download USOUSD M1 data from MT5 broker — fetches as far back as possible."""
import MetaTrader5 as mt5
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

SYMBOL    = "USOUSD"
TIMEFRAME = mt5.TIMEFRAME_M1

OUTPUT_DIR = Path("output/market_data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    print("Connecting to MT5 (must be already running)...")
    if not mt5.initialize():
        print(f"initialize() failed: {mt5.last_error()}")
        return

    info = mt5.terminal_info()
    print(f"Connected: {info.name}  build={info.build}")

    if not mt5.symbol_select(SYMBOL, True):
        print(f"symbol_select failed: {mt5.last_error()}")
        mt5.shutdown()
        return

    print(f"Downloading {SYMBOL} M1 — fetching as far back as possible (paginated)...")
    CHUNK = 50000
    all_chunks = []
    offset = 0
    while True:
        chunk = mt5.copy_rates_from_pos(SYMBOL, TIMEFRAME, offset, CHUNK)
        if chunk is None or len(chunk) == 0:
            break
        all_chunks.append(chunk)
        earliest = pd.to_datetime(chunk[-1]["time"], unit="s", utc=True)
        print(f"  Fetched {offset + len(chunk):,} bars  (earliest: {earliest.date()})")
        if len(chunk) < CHUNK:
            break
        offset += CHUNK

    mt5.shutdown()

    if not all_chunks:
        print("No data returned.")
        return

    rates = np.concatenate(all_chunks)
    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df = df.rename(columns={"time": "datetime", "tick_volume": "tick_vol"})
    df = df[["datetime", "open", "high", "low", "close", "tick_vol", "spread"]]
    df = df.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)

    csv_path     = OUTPUT_DIR / "usousd_m1_full.csv"
    parquet_path = OUTPUT_DIR / "usousd_m1_full.parquet"
    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    print(f"\nBars total  : {len(df):,}")
    print(f"Date range  : {df['datetime'].iloc[0].date()}  to  {df['datetime'].iloc[-1].date()}")
    print(f"Saved CSV   : {csv_path}")
    print(f"Saved Parquet: {parquet_path}")

if __name__ == "__main__":
    main()
