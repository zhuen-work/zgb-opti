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


# Task 3: Session enumeration
from datetime import datetime, timezone
import pandas as pd
from zgb_sim.orb import ORBConfig
from zgb_sim.sweep_reclaim import _build_sr_sessions, SRSession


def _make_m5_day(day: datetime, num_bars: int = 288, hi=2020.0, lo=1990.0):
    """Build a flat synthetic M5 bar frame for one trading day."""
    ts = pd.date_range(day, periods=num_bars, freq="5min", tz="UTC")
    rows = []
    for t in ts:
        rows.append({"ts": t, "open": (hi+lo)/2, "high": hi, "low": lo,
                     "close": (hi+lo)/2, "volume": 0})
    return pd.DataFrame(rows)


def test_sr_sessions_one_per_enabled_session_per_weekday():
    # LDN+NY enabled, range_minutes=30, pending_expire_minutes=120
    cfg = ORBConfig(range_minutes=30, pending_expire_minutes=120,
                    ldn_enabled=True, ldn_start_hour=7,
                    ny_enabled=True,  ny_start_hour=13)
    # 5 weekdays of synthetic M5
    frames = [_make_m5_day(datetime(2026, 2, 16, tzinfo=timezone.utc) +
                           pd.Timedelta(days=d)) for d in range(5)]
    m5 = pd.concat(frames, ignore_index=True)
    sessions = _build_sr_sessions(m5, cfg)
    assert len(sessions) == 10        # 5 days * 2 sessions
    s = sessions[0]
    assert s.range_start == pd.Timestamp("2026-02-16 07:00", tz="UTC")
    assert s.range_end   == pd.Timestamp("2026-02-16 07:30", tz="UTC")
    assert s.expire_ts   == pd.Timestamp("2026-02-16 09:30", tz="UTC")
    assert s.range_high  == 2020.0
    assert s.range_low   == 1990.0


def test_sr_sessions_skips_weekends():
    cfg = ORBConfig(range_minutes=30, pending_expire_minutes=60)
    # Saturday 2026-02-14
    m5 = _make_m5_day(datetime(2026, 2, 14, tzinfo=timezone.utc))
    sessions = _build_sr_sessions(m5, cfg)
    assert sessions == []


def test_sr_sessions_skips_session_with_no_m5_bars_in_range_window():
    cfg = ORBConfig(range_minutes=30, pending_expire_minutes=60,
                    ldn_enabled=True, ldn_start_hour=7, ny_enabled=False)
    # M5 frame starts at 10:00 (after LDN range window) — session skipped
    ts = pd.date_range(datetime(2026, 2, 16, 10, 0, tzinfo=timezone.utc),
                       periods=20, freq="5min", tz="UTC")
    m5 = pd.DataFrame({"ts": ts, "open": 2000, "high": 2010, "low": 1990,
                       "close": 2000, "volume": 0})
    sessions = _build_sr_sessions(m5, cfg)
    assert sessions == []
