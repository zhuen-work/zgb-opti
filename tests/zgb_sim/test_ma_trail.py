import numpy as np
import pandas as pd
import pytest
from zgb_sim.ma_trail import sma7_on_m5_closes


def _bars(ts_start: str, closes: list[float]) -> pd.DataFrame:
    n = len(closes)
    ts = pd.date_range(ts_start, periods=n, freq="5min", tz="UTC")
    return pd.DataFrame({
        "ts": ts.tz_localize(None),
        "open": closes,
        "high": closes,
        "low": closes,
        "close": closes,
    })


def test_close_ts_is_open_plus_5min():
    bars = _bars("2026-01-01 07:00", [100.0] * 8)
    close_ts, sma = sma7_on_m5_closes(bars)
    # close of first bar = open + 5min
    expected_first = np.int64(pd.Timestamp("2026-01-01 07:05").value)
    assert close_ts[0] == expected_first
    assert len(close_ts) == 8


def test_first_six_bars_nan_then_avg():
    closes = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0]
    bars = _bars("2026-01-01 07:00", closes)
    _, sma = sma7_on_m5_closes(bars)
    assert all(np.isnan(sma[:6]))
    # bar 6 (index 6): mean of closes[0..6] = (10+20+30+40+50+60+70)/7 = 40.0
    assert sma[6] == pytest.approx(40.0)
    # bar 7: mean of closes[1..7] = (20+30+40+50+60+70+80)/7 = 50.0
    assert sma[7] == pytest.approx(50.0)


def test_empty_bars_returns_empty_arrays():
    bars = pd.DataFrame({"ts": [], "open": [], "high": [], "low": [], "close": []})
    close_ts, sma = sma7_on_m5_closes(bars)
    assert close_ts.shape == (0,)
    assert sma.shape == (0,)


def test_returned_types_are_jit_compatible():
    bars = _bars("2026-01-01 07:00", [100.0] * 10)
    close_ts, sma = sma7_on_m5_closes(bars)
    assert close_ts.dtype == np.int64
    assert sma.dtype == np.float64
