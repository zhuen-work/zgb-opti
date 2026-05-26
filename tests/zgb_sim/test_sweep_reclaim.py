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


# Task 2: Entry / SL / TP construction
from zgb_sim.sweep_reclaim import _build_entry, Entry


def test_v_stop_sell_entry_geometry():
    setup = Setup(side="SELL", sweep_high=2025.0, sweep_low=2010.0)
    e = _build_entry(setup, mode="stop", buffer_pts=0,
                     range_high=2020.0, range_low=1990.0, point=0.01)
    assert e is not None
    assert e.direction == -1
    assert e.order_kind == "SELL_STOP"
    assert e.entry_price == pytest.approx(2010.0)
    assert e.sl_price    == pytest.approx(2025.0)
    assert e.tp_price    == pytest.approx(1990.0)


def test_v_limit_sell_entry_geometry():
    setup = Setup(side="SELL", sweep_high=2025.0, sweep_low=2010.0)
    e = _build_entry(setup, mode="limit", buffer_pts=0,
                     range_high=2020.0, range_low=1990.0, point=0.01)
    assert e is not None
    assert e.order_kind == "SELL_LIMIT"
    assert e.entry_price == pytest.approx(2020.0)   # at range_high
    assert e.sl_price    == pytest.approx(2025.0)
    assert e.tp_price    == pytest.approx(1990.0)


def test_v_stop_buy_entry_geometry():
    setup = Setup(side="BUY", sweep_high=2000.0, sweep_low=1985.0)
    e = _build_entry(setup, mode="stop", buffer_pts=0,
                     range_high=2020.0, range_low=1990.0, point=0.01)
    assert e is not None
    assert e.direction == 1
    assert e.order_kind == "BUY_STOP"
    assert e.entry_price == pytest.approx(2000.0)
    assert e.sl_price    == pytest.approx(1985.0)
    assert e.tp_price    == pytest.approx(2020.0)


def test_v_stop_skipped_when_sweep_wick_below_tp():
    # SELL: sweep_bar.low <= range_low -> entry would sit at/below TP
    setup = Setup(side="SELL", sweep_high=2025.0, sweep_low=1988.0)
    e = _build_entry(setup, mode="stop", buffer_pts=0,
                     range_high=2020.0, range_low=1990.0, point=0.01)
    assert e is None


def test_v_stop_buffer_applied_to_entry_and_sl():
    setup = Setup(side="SELL", sweep_high=2025.0, sweep_low=2010.0)
    e = _build_entry(setup, mode="stop", buffer_pts=20,   # 20 pts = $0.20 with point=0.01
                     range_high=2020.0, range_low=1990.0, point=0.01)
    assert e is not None
    assert e.entry_price == pytest.approx(2010.0 - 0.20)
    assert e.sl_price    == pytest.approx(2025.0 + 0.20)


def test_v_limit_skip_not_triggered_by_sweep_depth():
    # V_limit entry is always at range edge; sweep depth never makes entry==TP
    setup = Setup(side="SELL", sweep_high=2025.0, sweep_low=1988.0)
    e = _build_entry(setup, mode="limit", buffer_pts=0,
                     range_high=2020.0, range_low=1990.0, point=0.01)
    assert e is not None
    assert e.entry_price == pytest.approx(2020.0)
