from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import pytest
from zgb_sim.sweep_reclaim import _detect_sweep_setup, Setup


def test_no_sweep_returns_none():
    # bar entirely inside range
    assert _detect_sweep_setup(bar_high=2010.0, bar_low=1995.0, bar_close=2005.0,
                               range_high=2020.0, range_low=1990.0) is None


def test_sell_setup_sweep_above_close_back_inside():
    # high pierces above range_high, close back inside
    s = _detect_sweep_setup(bar_high=2025.0, bar_low=2010.0, bar_close=2015.0,
                            range_high=2020.0, range_low=1990.0)
    assert s is not None
    assert s.side == "SELL"
    assert s.sweep_high == 2025.0
    assert s.sweep_low == 2010.0


def test_buy_setup_sweep_below_close_back_inside():
    s = _detect_sweep_setup(bar_high=2000.0, bar_low=1985.0, bar_close=1995.0,
                            range_high=2020.0, range_low=1990.0)
    assert s is not None
    assert s.side == "BUY"


def test_dual_sweep_returns_none():
    # both sides swept, close inside range -> ambiguous, skip
    s = _detect_sweep_setup(bar_high=2025.0, bar_low=1985.0, bar_close=2005.0,
                            range_high=2020.0, range_low=1990.0)
    assert s is None


def test_close_outside_range_high_is_not_a_sell_setup():
    # high pierced but close stayed above range_high -> breakout, not reclaim
    assert _detect_sweep_setup(bar_high=2025.0, bar_low=2018.0, bar_close=2022.0,
                               range_high=2020.0, range_low=1990.0) is None


def test_close_outside_range_low_is_not_a_buy_setup():
    assert _detect_sweep_setup(bar_high=1992.0, bar_low=1980.0, bar_close=1985.0,
                               range_high=2020.0, range_low=1990.0) is None
