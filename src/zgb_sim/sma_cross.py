"""SMA(3) x SMA(5) cross signal on M5 closes for the V3 cross-exit feature.

The JIT loop wants two flat numpy arrays:
  close_ts_ns:     int64 — close-time of each M5 bar in ns since epoch
  cross_signal:    int8  — +1 bullish, -1 bearish, 0 no-cross, at that bar's close

First 5 bars (warmup) always 0.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

M5_NS = np.int64(5 * 60 * 1_000_000_000)


def sma_cross_on_m5_closes(m5_bars: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    if len(m5_bars) == 0:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.int8)
    ts = m5_bars["ts"]
    if hasattr(ts.dt, "tz") and ts.dt.tz is not None:
        ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)
    open_ns = ts.values.astype("datetime64[ns]").astype(np.int64)
    close_ts = (open_ns + M5_NS).astype(np.int64)

    closes = m5_bars["close"].values.astype(np.float64)
    sma3 = pd.Series(closes).rolling(3, min_periods=3).mean().to_numpy()
    sma5 = pd.Series(closes).rolling(5, min_periods=5).mean().to_numpy()

    n = len(closes)
    signal = np.zeros(n, dtype=np.int8)
    # Need both SMAs defined at i AND i-1 to detect cross.
    # SMA5 needs 5 closes -> first defined at index 4 -> first comparable at index 5.
    for i in range(5, n):
        prev_diff = sma3[i - 1] - sma5[i - 1]
        cur_diff  = sma3[i]     - sma5[i]
        if np.isnan(prev_diff) or np.isnan(cur_diff):
            continue
        if prev_diff >= 0 and cur_diff < 0:
            signal[i] = -1  # bearish
        elif prev_diff < 0 and cur_diff >= 0:
            signal[i] = 1   # bullish
    return close_ts, signal
