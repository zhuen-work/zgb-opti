"""Unit tests for src/zgb_sim/fractals.py"""
import numpy as np
import pandas as pd
import pytest

from zgb_sim.fractals import confirmed_fractals


def _bars(highs, lows, start="2026-01-05 07:00", freq="5min"):
    ts = pd.date_range(start, periods=len(highs), freq=freq, tz="UTC")
    return pd.DataFrame({"ts": ts, "high": highs, "low": lows})


def test_width5_isolated_up_fractal():
    # Bar 4 is an up-fractal: h=10 beats bars 2,3,5,6 (all <10)
    highs = [5, 6, 7, 8, 10, 9, 8, 7, 6]
    lows = [1, 2, 3, 4, 5, 4, 3, 2, 1]
    bars = _bars(highs, lows)
    out = confirmed_fractals(bars, width=5)
    # up fractal at index 4, confirmed at index 6 (4 + 5//2)
    assert len(out["up_ts"]) == 1
    assert out["up_ts"][0] == bars["ts"].iloc[6].value
    assert out["up_price"][0] == 10.0


def test_width5_no_fractal_when_equal_neighbour():
    # Equal high disqualifies (strict >, not >=)
    highs = [5, 6, 7, 10, 10, 9, 8]
    lows = [1, 2, 3, 4, 4, 3, 2]
    out = confirmed_fractals(_bars(highs, lows), width=5)
    assert len(out["up_ts"]) == 0


def test_width3_more_signals_than_width5():
    # Many small swings — w=3 should find more fractals than w=5
    highs = [1, 3, 2, 4, 2, 5, 2, 6, 2, 7, 2]
    lows = [0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0]
    out3 = confirmed_fractals(_bars(highs, lows), width=3)
    out5 = confirmed_fractals(_bars(highs, lows), width=5)
    assert len(out3["up_ts"]) > len(out5["up_ts"])


def test_width5_down_fractal():
    highs = [10, 9, 8, 7, 6, 7, 8, 9, 10]
    lows = [9, 8, 7, 6, 1, 6, 7, 8, 9]  # bar 4 is down-fractal low=1
    bars = _bars(highs, lows)
    out = confirmed_fractals(bars, width=5)
    assert len(out["dn_ts"]) == 1
    assert out["dn_price"][0] == 1.0
    assert out["dn_ts"][0] == bars["ts"].iloc[6].value


def test_no_peek_timestamp_is_confirming_bar_close():
    # The fractal at bar i must surface as usable at bar i + w//2, not bar i.
    highs = [1, 2, 3, 10, 3, 2, 1]
    lows = [0, 1, 2, 3, 2, 1, 0]
    bars = _bars(highs, lows)
    out = confirmed_fractals(bars, width=5)
    assert out["up_ts"][0] == bars["ts"].iloc[3 + 5 // 2].value  # iloc[5]


def test_invalid_width_raises():
    bars = _bars([1, 2, 3, 4, 5], [0, 1, 2, 3, 4])
    with pytest.raises(ValueError):
        confirmed_fractals(bars, width=4)  # only odd >=3 allowed
    with pytest.raises(ValueError):
        confirmed_fractals(bars, width=1)


def test_empty_bars_returns_empty_arrays():
    bars = _bars([], [])
    out = confirmed_fractals(bars, width=5)
    assert len(out["up_ts"]) == 0
    assert len(out["dn_ts"]) == 0
    assert out["up_ts"].dtype == np.int64
