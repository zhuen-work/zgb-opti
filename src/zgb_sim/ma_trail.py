"""SMA(7, close) on M5 bars for the post-HTP trailing stop in orb_fast.

The JIT loop wants two flat numpy arrays:
  close_ts_ns: int64 — close-time of each M5 bar in ns since epoch
  sma7:        float64 — SMA(close, 7) ending at that bar; NaN for first 6 bars

NaN-valued entries must not be read by the JIT consumer (which guards with
`isnan` check before ratcheting).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

M5_NS = np.int64(5 * 60 * 1_000_000_000)
WINDOW = 7


def sma7_on_m5_closes(m5_bars: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    if len(m5_bars) == 0:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float64)
    ts = m5_bars["ts"]
    if hasattr(ts.dt, "tz") and ts.dt.tz is not None:
        ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)
    open_ns = ts.values.astype("datetime64[ns]").astype(np.int64)
    close_ts = open_ns + M5_NS
    closes = m5_bars["close"].values.astype(np.float64)
    sma = pd.Series(closes).rolling(WINDOW, min_periods=WINDOW).mean().to_numpy()
    return close_ts.astype(np.int64), sma.astype(np.float64)
