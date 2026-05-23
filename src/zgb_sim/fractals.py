"""Bill Williams-style N-bar fractal detection.

A fractal at bar i (width w, odd, >=3) is:
  up:   high[i] strictly greater than high[i +/- 1..w//2]
  down: low[i]  strictly less    than low[i +/- 1..w//2]

Confirmation lag: w//2 bars. The fractal becomes usable at bar i + w//2,
whose ts is recorded as the fractal's timestamp (no-peek guarantee).

Returned arrays are sorted by confirm-time ascending.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def confirmed_fractals(m5_bars: pd.DataFrame, width: int = 5) -> dict[str, np.ndarray]:
    """Return {'up_ts','up_price','dn_ts','dn_price'} as int64/float64 numpy arrays.

    Each up_ts[k] is the int64 ns timestamp of the bar at which the k-th
    up-fractal becomes usable (= source_bar_index + width//2 close time).
    Same for down. up_price is the source bar's high; dn_price is its low.
    """
    if width < 3 or width % 2 == 0:
        raise ValueError(f"width must be odd >=3, got {width}")

    n = len(m5_bars)
    half = width // 2
    if n < width:
        return {
            "up_ts": np.empty(0, dtype=np.int64),
            "up_price": np.empty(0, dtype=np.float64),
            "dn_ts": np.empty(0, dtype=np.int64),
            "dn_price": np.empty(0, dtype=np.float64),
        }

    highs = m5_bars["high"].values.astype(np.float64)
    lows = m5_bars["low"].values.astype(np.float64)
    ts_series = m5_bars["ts"]
    if hasattr(ts_series.dt, "tz") and ts_series.dt.tz is not None:
        ts_ns = ts_series.dt.tz_convert("UTC").dt.tz_localize(None).values.astype("datetime64[ns]").astype(np.int64)
    else:
        ts_ns = ts_series.values.astype("datetime64[ns]").astype(np.int64)

    up_ts, up_pr, dn_ts, dn_pr = [], [], [], []
    for i in range(half, n - half):
        h = highs[i]
        is_up = True
        for k in range(1, half + 1):
            if not (h > highs[i - k] and h > highs[i + k]):
                is_up = False
                break
        if is_up:
            up_ts.append(int(ts_ns[i + half]))
            up_pr.append(float(h))

        lo = lows[i]
        is_dn = True
        for k in range(1, half + 1):
            if not (lo < lows[i - k] and lo < lows[i + k]):
                is_dn = False
                break
        if is_dn:
            dn_ts.append(int(ts_ns[i + half]))
            dn_pr.append(float(lo))

    return {
        "up_ts": np.array(up_ts, dtype=np.int64),
        "up_price": np.array(up_pr, dtype=np.float64),
        "dn_ts": np.array(dn_ts, dtype=np.int64),
        "dn_price": np.array(dn_pr, dtype=np.float64),
    }
