"""SMA cross signal on M5 closes for the V3 cross-exit feature.

The JIT loop wants two flat numpy arrays:
  close_ts_ns:     int64 — close-time of each M5 bar in ns since epoch
  cross_signal:    int8  — +1 bullish, -1 bearish, 0 no-cross, at that bar's close

Accepts configurable fast and slow SMA periods (default: 3 and 5).
First `slow` bars (warmup) always 0.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

M5_NS = np.int64(5 * 60 * 1_000_000_000)


def sma_cross_on_m5_closes(
    m5_bars: pd.DataFrame, fast: int = 3, slow: int = 5,
) -> tuple[np.ndarray, np.ndarray]:
    if fast >= slow:
        raise ValueError(f"fast ({fast}) must be < slow ({slow})")
    if fast < 2:
        raise ValueError(f"fast must be >= 2, got {fast}")

    if len(m5_bars) == 0:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.int8)
    ts = m5_bars["ts"]
    if hasattr(ts.dt, "tz") and ts.dt.tz is not None:
        ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)
    open_ns = ts.values.astype("datetime64[ns]").astype(np.int64)
    close_ts = (open_ns + M5_NS).astype(np.int64)

    closes = m5_bars["close"].values.astype(np.float64)
    sma_fast = pd.Series(closes).rolling(fast, min_periods=fast).mean().to_numpy()
    sma_slow = pd.Series(closes).rolling(slow, min_periods=slow).mean().to_numpy()

    n = len(closes)
    signal = np.zeros(n, dtype=np.int8)
    # Need both SMAs defined at i AND i-1 to detect cross.
    # SMA_slow needs `slow` closes -> first defined at index (slow-1) -> first comparable at index slow.
    for i in range(slow, n):
        prev_diff = sma_fast[i - 1] - sma_slow[i - 1]
        cur_diff  = sma_fast[i]     - sma_slow[i]
        if np.isnan(prev_diff) or np.isnan(cur_diff):
            continue
        if prev_diff >= 0 and cur_diff < 0:
            signal[i] = -1  # bearish
        elif prev_diff < 0 and cur_diff >= 0:
            signal[i] = 1   # bullish
    return close_ts, signal
