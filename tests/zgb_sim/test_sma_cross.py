import numpy as np
import pandas as pd
import pytest
from zgb_sim.sma_cross import sma_cross_on_m5_closes


def _bars(ts_start: str, closes: list[float]) -> pd.DataFrame:
    n = len(closes)
    ts = pd.date_range(ts_start, periods=n, freq="5min", tz="UTC")
    return pd.DataFrame({
        "ts": ts.tz_localize(None),
        "open": closes, "high": closes, "low": closes, "close": closes,
    })


def test_empty_bars_returns_empty_arrays():
    bars = pd.DataFrame({"ts": [], "open": [], "high": [], "low": [], "close": []})
    close_ts, signal = sma_cross_on_m5_closes(bars)
    assert close_ts.shape == (0,)
    assert signal.shape == (0,)


def test_close_ts_is_open_plus_5min_int64():
    bars = _bars("2026-01-01 07:00", [100.0] * 10)
    close_ts, signal = sma_cross_on_m5_closes(bars)
    expected = np.int64(pd.Timestamp("2026-01-01 07:05").value)
    assert close_ts[0] == expected
    assert close_ts.dtype == np.int64
    assert signal.dtype == np.int8


def test_first_five_bars_signal_zero():
    bars = _bars("2026-01-01 07:00", [10, 11, 12, 13, 14, 15, 14, 13, 12, 11])
    _, signal = sma_cross_on_m5_closes(bars)
    # First 4 bars: SMA5 NaN. Bar 4 (index): SMA5 defined but no prior SMA5 to
    # compare against, so signal is 0. Crosses can only fire from bar 5 onward.
    assert all(signal[:5] == 0)


def test_bullish_cross_detected():
    # Pattern: closes that take SMA3 from below SMA5 to above SMA5.
    # Falling then rising sharply.
    closes = [50, 49, 48, 47, 46, 45, 48, 52, 56, 60, 64, 68]
    bars = _bars("2026-01-01 07:00", closes)
    _, signal = sma_cross_on_m5_closes(bars)
    # Somewhere in the rising portion, SMA3 (faster) will cross above SMA5.
    assert (signal == 1).any(), f"expected at least one bullish cross, got {signal.tolist()}"


def test_bearish_cross_detected():
    closes = [50, 51, 52, 53, 54, 55, 52, 48, 44, 40, 36, 32]
    bars = _bars("2026-01-01 07:00", closes)
    _, signal = sma_cross_on_m5_closes(bars)
    assert (signal == -1).any(), f"expected at least one bearish cross, got {signal.tolist()}"


def test_steady_uptrend_no_cross():
    # Monotonic uptrend: SMA3 stays above SMA5 the whole time after warmup.
    closes = list(range(100, 130))
    bars = _bars("2026-01-01 07:00", closes)
    _, signal = sma_cross_on_m5_closes(bars)
    # After warmup (bars 5+), no cross should fire.
    assert (signal[5:] == 0).all(), \
        f"expected no crosses in monotonic uptrend, got {signal.tolist()}"
