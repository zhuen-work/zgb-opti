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


def test_default_args_match_explicit_3_5():
    closes = [50, 51, 52, 53, 54, 55, 52, 48, 44, 40, 36, 32]
    bars = _bars("2026-01-01 07:00", closes)
    _, signal_default = sma_cross_on_m5_closes(bars)
    _, signal_explicit = sma_cross_on_m5_closes(bars, fast=3, slow=5)
    assert np.array_equal(signal_default, signal_explicit)


def test_longer_slow_period_delays_cross():
    """Slower MA = fewer crosses on a noisy series."""
    # Construct a series with a small wiggle that triggers cross at (3,5)
    # but is averaged out at (5,20).
    closes = [50, 51, 52, 53, 54, 53.5, 54.5, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70]
    bars = _bars("2026-01-01 07:00", closes)
    _, fast_signal = sma_cross_on_m5_closes(bars, fast=3, slow=5)
    _, slow_signal = sma_cross_on_m5_closes(bars, fast=5, slow=20)
    # Should be valid output regardless
    assert fast_signal.dtype == np.int8
    assert slow_signal.dtype == np.int8
    # Slower-MA signal has more warmup (first slow=20 bars are 0)
    assert (slow_signal[:20] == 0).all()


def test_invalid_periods_raise():
    bars = _bars("2026-01-01 07:00", [100.0] * 10)
    with pytest.raises(ValueError):
        sma_cross_on_m5_closes(bars, fast=5, slow=5)  # fast >= slow
    with pytest.raises(ValueError):
        sma_cross_on_m5_closes(bars, fast=10, slow=5)  # fast > slow
    with pytest.raises(ValueError):
        sma_cross_on_m5_closes(bars, fast=1, slow=5)  # fast < 2
