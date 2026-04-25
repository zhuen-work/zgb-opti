"""Tick + bar loader. Caches to parquet so MT5 only needs to run once per date range."""
from __future__ import annotations

import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd


def kill_mt5_terminal() -> None:
    """Kill any running terminal64.exe (MT5). Safe to call when none running."""
    try:
        subprocess.run(
            ["taskkill", "/IM", "terminal64.exe", "/F"],
            capture_output=True, text=True, check=False, timeout=10,
        )
    except Exception:
        pass


CACHE_DIR = Path(__file__).resolve().parents[2] / "output" / "sim_cache"


def _ticks_cache_path(symbol: str, year: int, month: int) -> Path:
    return CACHE_DIR / f"ticks_{symbol}_{year:04d}{month:02d}.parquet"


def _bars_cache_path(symbol: str, tf: str) -> Path:
    return CACHE_DIR / f"bars_{symbol}_{tf}.parquet"


def _pull_ticks_month(symbol: str, year: int, month: int) -> pd.DataFrame:
    import MetaTrader5 as mt5
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    # end = first of next month
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    arr = mt5.copy_ticks_range(symbol, start, end, mt5.COPY_TICKS_ALL)
    if arr is None or len(arr) == 0:
        raise RuntimeError(f"No ticks for {symbol} {year}-{month:02d}: {mt5.last_error()}")
    df = pd.DataFrame(arr)
    # time_msc → timestamp
    df["ts"] = pd.to_datetime(df["time_msc"], unit="ms", utc=True)
    # keep what we need
    df = df[["ts", "bid", "ask"]].copy()
    df["bid"] = df["bid"].astype(np.float64)
    df["ask"] = df["ask"].astype(np.float64)
    return df.reset_index(drop=True)


def _pull_bars(symbol: str, tf: str, start: datetime, end: datetime) -> pd.DataFrame:
    import MetaTrader5 as mt5
    tf_map = {"M1": mt5.TIMEFRAME_M1, "M5": mt5.TIMEFRAME_M5}
    arr = mt5.copy_rates_range(symbol, tf_map[tf], start, end)
    if arr is None or len(arr) == 0:
        raise RuntimeError(f"No bars for {symbol} {tf}: {mt5.last_error()}")
    df = pd.DataFrame(arr)
    df["ts"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df = df[["ts", "open", "high", "low", "close"]].copy()
    return df.reset_index(drop=True)


def load_ticks(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    """Load ticks [start, end) UTC. Caches per-month in parquet. Returns df with ts, bid, ask."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    # Figure out which months we need
    months = set()
    d = datetime(start.year, start.month, 1, tzinfo=timezone.utc)
    end_utc = end if end.tzinfo else end.replace(tzinfo=timezone.utc)
    while d < end_utc:
        months.add((d.year, d.month))
        # next month
        if d.month == 12:
            d = datetime(d.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            d = datetime(d.year, d.month + 1, 1, tzinfo=timezone.utc)

    parts = []
    need_mt5 = False
    for y, m in sorted(months):
        p = _ticks_cache_path(symbol, y, m)
        if p.exists():
            parts.append(pd.read_parquet(p))
        else:
            need_mt5 = True
            break

    if need_mt5:
        import MetaTrader5 as mt5
        if not mt5.initialize():
            raise RuntimeError(f"MT5 init failed: {mt5.last_error()}")
        try:
            mt5.symbol_select(symbol, True)
            parts = []
            for y, m in sorted(months):
                p = _ticks_cache_path(symbol, y, m)
                if p.exists():
                    parts.append(pd.read_parquet(p))
                    continue
                print(f"  Pulling ticks {symbol} {y}-{m:02d}...")
                df = _pull_ticks_month(symbol, y, m)
                df.to_parquet(p, index=False)
                parts.append(df)
        finally:
            mt5.shutdown()
            kill_mt5_terminal()  # user rule: never leave MT5 running

    ticks = pd.concat(parts, ignore_index=True)
    start_utc = start if start.tzinfo else start.replace(tzinfo=timezone.utc)
    end_utc = end if end.tzinfo else end.replace(tzinfo=timezone.utc)
    ticks = ticks[(ticks["ts"] >= start_utc) & (ticks["ts"] < end_utc)].reset_index(drop=True)
    return ticks


def load_bars(symbol: str, tf: str, start: datetime, end: datetime) -> pd.DataFrame:
    """Load M1 or M5 bars [start, end] UTC. Cached in one parquet per (symbol, tf)."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = _bars_cache_path(symbol, tf)
    start_utc = start if start.tzinfo else start.replace(tzinfo=timezone.utc)
    end_utc = end if end.tzinfo else end.replace(tzinfo=timezone.utc)

    if p.exists():
        df = pd.read_parquet(p)
        have_start = df["ts"].min()
        have_end = df["ts"].max()
        if have_start <= start_utc and have_end >= end_utc:
            return df[(df["ts"] >= start_utc) & (df["ts"] <= end_utc)].reset_index(drop=True)

    # Pull full range with small buffer on either side (Donchian lookback)
    import MetaTrader5 as mt5
    if not mt5.initialize():
        raise RuntimeError(f"MT5 init failed: {mt5.last_error()}")
    try:
        mt5.symbol_select(symbol, True)
        pad = timedelta(days=2)
        df = _pull_bars(symbol, tf, start_utc - pad, end_utc + pad)
    finally:
        mt5.shutdown()
        kill_mt5_terminal()  # user rule: never leave MT5 running

    df.to_parquet(p, index=False)
    return df[(df["ts"] >= start_utc) & (df["ts"] <= end_utc)].reset_index(drop=True)


def symbol_meta(symbol: str) -> dict:
    """Fetch relevant symbol metadata (point, tick size/value, stops level, lot limits)."""
    import MetaTrader5 as mt5
    if not mt5.initialize():
        raise RuntimeError("MT5 init failed")
    try:
        mt5.symbol_select(symbol, True)
        info = mt5.symbol_info(symbol)
        if info is None:
            raise RuntimeError(f"No symbol info for {symbol}")
        return {
            "point": info.point,
            "digits": info.digits,
            "tick_size": info.trade_tick_size,
            "tick_value": info.trade_tick_value,
            "stops_level": info.trade_stops_level,
            "volume_min": info.volume_min,
            "volume_max": info.volume_max,
            "volume_step": info.volume_step,
            "contract_size": info.trade_contract_size,
        }
    finally:
        mt5.shutdown()
        kill_mt5_terminal()  # user rule: never leave MT5 running
