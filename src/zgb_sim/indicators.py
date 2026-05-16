"""Indicators for GMMA + RSI strategy research.

EMA: pandas .ewm exponential moving average (alpha = 2 / (period+1)).
RSI: Wilder's RSI (alpha = 1/period) — matches MT5 iRSI(PRICE_CLOSE) behavior.
gmma_state: classify each bar as UP / DOWN / MIXED from a 12-EMA fan.
"""
from __future__ import annotations

from typing import Sequence

import numpy as np
import pandas as pd


# Canonical Guppy fan periods (LiteFinance spec).
GMMA_SHORT = (3, 5, 8, 10, 12, 15)
GMMA_LONG = (30, 35, 40, 45, 50, 60)


def ema(series: pd.Series, period: int) -> pd.Series:
    """Exponential moving average (span convention, matches MT5 iMA MODE_EMA)."""
    return series.ewm(span=period, adjust=False, min_periods=period).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Wilder RSI — alpha = 1/period (equivalent to MT5 iRSI on PRICE_CLOSE).

    The first `period` values are NaN. Output range [0, 100].
    """
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    alpha = 1.0 / period
    # Use ewm with alpha + adjust=False = Wilder's smoothing.
    avg_gain = gain.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    out = 100.0 - 100.0 / (1.0 + rs)
    # When avg_loss == 0 and avg_gain > 0, RSI = 100. When both 0 (flat), RSI = 50.
    out = out.where(avg_loss != 0, other=100.0)
    out = out.where(~((avg_loss == 0) & (avg_gain == 0)), other=50.0)
    return out


def gmma_fans(close: pd.Series,
              short_periods: Sequence[int] = GMMA_SHORT,
              long_periods: Sequence[int] = GMMA_LONG) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute the short and long EMA fans as DataFrames keyed by period."""
    s = pd.DataFrame({p: ema(close, p) for p in short_periods}, index=close.index)
    l = pd.DataFrame({p: ema(close, p) for p in long_periods}, index=close.index)
    return s, l


def gmma_state(close: pd.Series,
               short_periods: Sequence[int] = GMMA_SHORT,
               long_periods: Sequence[int] = GMMA_LONG) -> pd.Series:
    """Return per-bar 'UP' / 'DOWN' / 'MIXED'.

    UP: every short-EMA strictly above every long-EMA.
    DOWN: every short-EMA strictly below every long-EMA.
    MIXED: anything else (incl. warmup bars with NaN EMAs).
    """
    s, l = gmma_fans(close, short_periods, long_periods)
    s_min = s.min(axis=1)
    s_max = s.max(axis=1)
    l_min = l.min(axis=1)
    l_max = l.max(axis=1)
    up = s_min > l_max
    dn = s_max < l_min
    out = pd.Series("MIXED", index=close.index, dtype="object")
    out[up.fillna(False)] = "UP"
    out[dn.fillna(False)] = "DOWN"
    # NaN warmup -> already MIXED (default)
    return out


def in_ribbon(price: pd.Series, ribbon: pd.DataFrame) -> pd.Series:
    """True where price is between ribbon.min() and ribbon.max() (inclusive)."""
    lo = ribbon.min(axis=1)
    hi = ribbon.max(axis=1)
    return (price >= lo) & (price <= hi)
