"""Tick + bar loader. Caches to parquet so MT5 only needs to run once per date range."""
from __future__ import annotations

import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd


# Standard sim spread (pts) — applied to loaded ticks by default.
# Real Vantage XAUUSD avg is ~25 pts; 70 is conservative for live execution variance.
# Pass spread_pts=0 to load_ticks for raw real-tick spreads.
SIM_SPREAD_PTS = 70
XAUUSD_POINT = 0.01


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
    tf_map = {
        "M1": mt5.TIMEFRAME_M1,
        "M5": mt5.TIMEFRAME_M5,
        "M15": mt5.TIMEFRAME_M15,
        "M30": mt5.TIMEFRAME_M30,
        "H1": mt5.TIMEFRAME_H1,
        "H4": mt5.TIMEFRAME_H4,
    }
    arr = mt5.copy_rates_range(symbol, tf_map[tf], start, end)
    if arr is None or len(arr) == 0:
        raise RuntimeError(f"No bars for {symbol} {tf}: {mt5.last_error()}")
    df = pd.DataFrame(arr)
    df["ts"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df = df[["ts", "open", "high", "low", "close"]].copy()
    return df.reset_index(drop=True)


def _apply_spread_override(df: pd.DataFrame, spread_pts: int, point: float) -> pd.DataFrame:
    """Override real bid/ask with synthetic fixed spread centered on mid."""
    if spread_pts <= 0:
        return df
    mid = (df["bid"] + df["ask"]) / 2.0
    half = spread_pts * point / 2.0
    df = df.copy()
    df["bid"] = mid - half
    df["ask"] = mid + half
    return df


def load_ticks(symbol: str, start: datetime, end: datetime,
               spread_pts: int | None = None) -> pd.DataFrame:
    """Load ticks [start, end) UTC. Caches per-month in parquet.

    spread_pts: synthetic fixed spread to apply (overrides real bid/ask).
                None (default) = use SIM_SPREAD_PTS module constant (60).
                0 = preserve real recorded spreads.
    """
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

    # Coverage check: a per-month file is "complete" only if it covers
    # through the end of its month. If the cached file was written mid-month
    # it'll have a max(ts) earlier than month-end and must be re-pulled.
    end_utc_check = end if end.tzinfo else end.replace(tzinfo=timezone.utc)
    today_utc = datetime.now(timezone.utc)

    def _month_end_utc(yy: int, mm: int) -> datetime:
        if mm == 12:
            return datetime(yy + 1, 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
        return datetime(yy, mm + 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)

    def _is_cache_stale(path: Path, yy: int, mm: int) -> bool:
        try:
            df = pd.read_parquet(path, columns=["ts"])
        except Exception:
            return True
        if df.empty:
            return True
        cached_max = pd.Timestamp(df["ts"].max())
        if cached_max.tz is None:
            cached_max = cached_max.tz_localize("UTC")
        # For a fully-past month we expect cache through month-end (or close).
        # For the current month we expect cache through "yesterday" at minimum.
        month_end = _month_end_utc(yy, mm)
        target = min(month_end, today_utc - timedelta(days=1))
        # Allow 2-day grace for weekends/holidays where forex was closed
        return cached_max < target - timedelta(days=2)

    parts = []
    need_mt5 = False
    stale_months: list[tuple[int, int]] = []
    for y, m in sorted(months):
        p = _ticks_cache_path(symbol, y, m)
        if not p.exists():
            need_mt5 = True
            break
        # Only check staleness for the requested-window's last month
        # (older months are assumed complete once cached)
        is_last_month = (y == end_utc_check.year and m == end_utc_check.month) or \
                        (y == today_utc.year and m == today_utc.month)
        if is_last_month and _is_cache_stale(p, y, m):
            print(f"  [tick cache] {symbol} {y}-{m:02d} is stale -- re-pulling.")
            stale_months.append((y, m))
            need_mt5 = True
            break
        parts.append(pd.read_parquet(p))

    if need_mt5:
        # Delete any stale files so the re-pull can replace them cleanly
        for y, m in stale_months:
            p = _ticks_cache_path(symbol, y, m)
            try: p.unlink()
            except Exception: pass
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

    # Apply standard sim spread override
    eff_spread = SIM_SPREAD_PTS if spread_pts is None else spread_pts
    if eff_spread > 0 and symbol == "XAUUSD":
        ticks = _apply_spread_override(ticks, eff_spread, XAUUSD_POINT)
    return ticks


def load_bars(symbol: str, tf: str, start: datetime, end: datetime) -> pd.DataFrame:
    """Load OHLC bars [start, end] UTC. Cached one parquet per (symbol, tf).

    Supported TFs: M1, M5, M15, M30, H1, H4.
    """
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
    """Fetch relevant symbol metadata. Caches to JSON for offline use.
    Falls back to known XAUUSD/Vantage defaults when MT5 unavailable.
    """
    import json
    cache_path = CACHE_DIR / f"meta_{symbol}.json"
    if cache_path.exists():
        return json.loads(cache_path.read_text())

    # XAUUSD on Vantage — known stable defaults (matches mt5.symbol_info() output)
    XAUUSD_FALLBACK = {
        "point": 0.01, "digits": 2,
        "tick_size": 0.01, "tick_value": 1.0,
        "stops_level": 0,
        "volume_min": 0.01, "volume_max": 100.0, "volume_step": 0.01,
    }

    import MetaTrader5 as mt5
    if not mt5.initialize():
        # Fallback: use known XAUUSD defaults if MT5 unavailable
        if symbol == "XAUUSD":
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(XAUUSD_FALLBACK, indent=2))
            print(f"  [symbol_meta] MT5 unavailable, using cached XAUUSD defaults.")
            return XAUUSD_FALLBACK
        raise RuntimeError("MT5 init failed")
    try:
        mt5.symbol_select(symbol, True)
        info = mt5.symbol_info(symbol)
        if info is None:
            raise RuntimeError(f"No symbol info for {symbol}")
        meta_dict = {
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
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(meta_dict, indent=2))
        return meta_dict
    finally:
        mt5.shutdown()
        kill_mt5_terminal()  # user rule: never leave MT5 running
