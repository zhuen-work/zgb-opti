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


# Task 4: Single-session fill modeling — V_stop happy path
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.sweep_reclaim import _simulate_session, SessionResult, SRConfig


def _bar(ts, o, h, l, c):
    return {"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": 0}


def _meta_default():
    return SymbolMeta(point=0.01, tick_size=0.01, tick_value=1.0,
                      stops_level_pts=20, volume_min=0.01, volume_max=500.0,
                      volume_step=0.01, digits=2)


def test_v_stop_sell_fills_and_hits_tp():
    rs = pd.Timestamp("2026-02-16 07:00", tz="UTC")
    re = pd.Timestamp("2026-02-16 07:30", tz="UTC")
    ex = pd.Timestamp("2026-02-16 09:30", tz="UTC")
    session = SRSession(range_start=rs, range_end=re, expire_ts=ex,
                        range_high=2020.0, range_low=1990.0, session_tag="LDN")
    # M5 in active window:
    #   07:30 sweep+reclaim bar  high=2025 low=2010 close=2015  -> SELL setup
    #   07:35 next bar trades through 2010 (fills SELL_STOP)    -> entry at 2010
    #   07:40+ price falls to range_low (2010 -> 1990 = 200 pts) -> TP hit
    m5_active = pd.DataFrame([
        _bar(pd.Timestamp("2026-02-16 07:30", tz="UTC"), 2018, 2025, 2010, 2015),
        _bar(pd.Timestamp("2026-02-16 07:35", tz="UTC"), 2015, 2017, 2005, 2008),
        _bar(pd.Timestamp("2026-02-16 07:40", tz="UTC"), 2008, 2009, 1990, 1992),
    ])
    m1_active = pd.DataFrame([
        _bar(pd.Timestamp("2026-02-16 07:35", tz="UTC"), 2015, 2017, 2014, 2014),
        _bar(pd.Timestamp("2026-02-16 07:36", tz="UTC"), 2014, 2014, 2012, 2012),
        _bar(pd.Timestamp("2026-02-16 07:37", tz="UTC"), 2012, 2012, 2010, 2010),
        _bar(pd.Timestamp("2026-02-16 07:38", tz="UTC"), 2010, 2010, 2008, 2008),
        _bar(pd.Timestamp("2026-02-16 07:39", tz="UTC"), 2008, 2008, 2005, 2005),
        _bar(pd.Timestamp("2026-02-16 07:40", tz="UTC"), 2005, 2005, 2002, 2002),
        _bar(pd.Timestamp("2026-02-16 07:41", tz="UTC"), 2002, 2002, 1998, 1998),
        _bar(pd.Timestamp("2026-02-16 07:42", tz="UTC"), 1998, 1998, 1994, 1994),
        _bar(pd.Timestamp("2026-02-16 07:43", tz="UTC"), 1994, 1994, 1990, 1990),
        _bar(pd.Timestamp("2026-02-16 07:44", tz="UTC"), 1990, 1992, 1990, 1992),
    ])
    cfg = SRConfig(risk_pct=1.0, mode="stop", buffer_pts=0)
    meta = _meta_default()
    result = _simulate_session(session, m5_active, m1_active, cfg, meta,
                               balance=10_000.0)
    assert result.outcome == "tp"
    assert result.entry_price == pytest.approx(2010.0)
    assert result.exit_price  == pytest.approx(1990.0)
    assert result.direction == -1
    assert result.pnl > 0


# Task 5: V_limit + edge cases (TDD)
def test_v_limit_sell_fills_on_retest_and_hits_tp():
    rs = pd.Timestamp("2026-02-16 07:00", tz="UTC")
    re = pd.Timestamp("2026-02-16 07:30", tz="UTC")
    ex = pd.Timestamp("2026-02-16 09:30", tz="UTC")
    session = SRSession(range_start=rs, range_end=re, expire_ts=ex,
                        range_high=2020.0, range_low=1990.0, session_tag="LDN")
    m5_active = pd.DataFrame([
        _bar(pd.Timestamp("2026-02-16 07:30", tz="UTC"), 2018, 2025, 2010, 2015),
        _bar(pd.Timestamp("2026-02-16 07:35", tz="UTC"), 2015, 2021, 2014, 2014),
        _bar(pd.Timestamp("2026-02-16 07:40", tz="UTC"), 2014, 2014, 1990, 1992),
    ])
    m1_active = pd.DataFrame([
        _bar(pd.Timestamp("2026-02-16 07:35", tz="UTC"), 2015, 2021, 2014, 2018),
        _bar(pd.Timestamp("2026-02-16 07:36", tz="UTC"), 2018, 2019, 2016, 2016),
        _bar(pd.Timestamp("2026-02-16 07:40", tz="UTC"), 2016, 2016, 2010, 2010),
        _bar(pd.Timestamp("2026-02-16 07:41", tz="UTC"), 2010, 2010, 2000, 2000),
        _bar(pd.Timestamp("2026-02-16 07:42", tz="UTC"), 2000, 2000, 1990, 1990),
    ])
    cfg = SRConfig(risk_pct=1.0, mode="limit", buffer_pts=0)
    result = _simulate_session(session, m5_active, m1_active, cfg, _meta_default(),
                               balance=10_000.0)
    assert result.outcome == "tp"
    assert result.entry_price == pytest.approx(2020.0)
    assert result.exit_price  == pytest.approx(1990.0)


def test_v_stop_sl_hit():
    rs = pd.Timestamp("2026-02-16 07:00", tz="UTC")
    re = pd.Timestamp("2026-02-16 07:30", tz="UTC")
    ex = pd.Timestamp("2026-02-16 09:30", tz="UTC")
    session = SRSession(range_start=rs, range_end=re, expire_ts=ex,
                        range_high=2020.0, range_low=1990.0, session_tag="LDN")
    m5_active = pd.DataFrame([
        _bar(pd.Timestamp("2026-02-16 07:30", tz="UTC"), 2018, 2025, 2010, 2015),
        _bar(pd.Timestamp("2026-02-16 07:35", tz="UTC"), 2015, 2026, 2009, 2024),
    ])
    m1_active = pd.DataFrame([
        _bar(pd.Timestamp("2026-02-16 07:35", tz="UTC"), 2015, 2015, 2009, 2010),
        _bar(pd.Timestamp("2026-02-16 07:36", tz="UTC"), 2010, 2015, 2010, 2015),
        _bar(pd.Timestamp("2026-02-16 07:37", tz="UTC"), 2015, 2026, 2015, 2024),
    ])
    cfg = SRConfig(risk_pct=1.0, mode="stop", buffer_pts=0)
    result = _simulate_session(session, m5_active, m1_active, cfg, _meta_default(),
                               balance=10_000.0)
    assert result.outcome == "sl"
    assert result.exit_price == pytest.approx(2025.0)
    assert result.pnl < 0


def test_no_fill_expire_returns_expired():
    rs = pd.Timestamp("2026-02-16 07:00", tz="UTC")
    re = pd.Timestamp("2026-02-16 07:30", tz="UTC")
    ex = pd.Timestamp("2026-02-16 09:30", tz="UTC")
    session = SRSession(range_start=rs, range_end=re, expire_ts=ex,
                        range_high=2020.0, range_low=1990.0, session_tag="LDN")
    m5_active = pd.DataFrame([
        _bar(pd.Timestamp("2026-02-16 07:30", tz="UTC"), 2018, 2025, 2010, 2015),
        _bar(pd.Timestamp("2026-02-16 07:35", tz="UTC"), 2015, 2016, 2010, 2012),
    ])
    m1_active = pd.DataFrame([
        _bar(pd.Timestamp("2026-02-16 07:35", tz="UTC"), 2015, 2016, 2014, 2014),
        _bar(pd.Timestamp("2026-02-16 07:36", tz="UTC"), 2014, 2015, 2010, 2012),
    ])
    cfg = SRConfig(risk_pct=1.0, mode="limit", buffer_pts=0)
    result = _simulate_session(session, m5_active, m1_active, cfg, _meta_default(),
                               balance=10_000.0)
    assert result.outcome == "expired"
    assert result.skip_reason == "no_fill"


def test_no_sweep_returns_skipped():
    rs = pd.Timestamp("2026-02-16 07:00", tz="UTC")
    re = pd.Timestamp("2026-02-16 07:30", tz="UTC")
    ex = pd.Timestamp("2026-02-16 09:30", tz="UTC")
    session = SRSession(range_start=rs, range_end=re, expire_ts=ex,
                        range_high=2020.0, range_low=1990.0, session_tag="LDN")
    m5_active = pd.DataFrame([
        _bar(pd.Timestamp("2026-02-16 07:30", tz="UTC"), 2010, 2015, 2005, 2012),
    ])
    cfg = SRConfig(risk_pct=1.0, mode="stop", buffer_pts=0)
    result = _simulate_session(session, m5_active, pd.DataFrame(), cfg,
                               _meta_default(), balance=10_000.0)
    assert result.outcome == "skipped"
    assert result.skip_reason == "no_sweep"


def test_v_stop_no_rr_skip():
    rs = pd.Timestamp("2026-02-16 07:00", tz="UTC")
    re = pd.Timestamp("2026-02-16 07:30", tz="UTC")
    ex = pd.Timestamp("2026-02-16 09:30", tz="UTC")
    session = SRSession(range_start=rs, range_end=re, expire_ts=ex,
                        range_high=2020.0, range_low=1990.0, session_tag="LDN")
    # SELL setup: high > range_high, close < range_high, but low <= range_low (no RR room)
    # close=1985 <= range_low so buy_trig fails; sweep_low=1988 <= tp=1990 so _build_entry fails
    m5_active = pd.DataFrame([
        _bar(pd.Timestamp("2026-02-16 07:30", tz="UTC"), 2018, 2025, 1988, 1985),
    ])
    cfg = SRConfig(risk_pct=1.0, mode="stop", buffer_pts=0)
    result = _simulate_session(session, m5_active, pd.DataFrame(), cfg,
                               _meta_default(), balance=10_000.0)
    assert result.outcome == "skipped"
    assert result.skip_reason == "no_rr"


# Task 6: Top-level simulate() + diagnostic counters
from zgb_sim.sweep_reclaim import simulate, SRSimResult


def test_simulate_aggregates_sessions():
    # 2 weekdays, LDN-only, range_minutes=30, expire=60
    days = [datetime(2026, 2, 16, tzinfo=timezone.utc),
            datetime(2026, 2, 17, tzinfo=timezone.utc)]
    rows_m5, rows_m1 = [], []
    for d in days:
        for k in range(6):
            t = pd.Timestamp(d) + pd.Timedelta(minutes=5*k) + pd.Timedelta(hours=7)
            rows_m5.append(_bar(t, 2000, 2020, 1990, 2000))
        rows_m5.append(_bar(pd.Timestamp(d)+pd.Timedelta(hours=7,minutes=30),
                            2018, 2025, 2010, 2015))   # sweep+reclaim SELL
        for k, px in enumerate([2014, 2012, 2010, 2008, 2004, 2000, 1995, 1990]):
            t = pd.Timestamp(d) + pd.Timedelta(hours=7, minutes=35+k)
            rows_m1.append(_bar(t, px, px+1, px-1, px))
    m5 = pd.DataFrame(rows_m5).sort_values("ts").reset_index(drop=True)
    m1 = pd.DataFrame(rows_m1).sort_values("ts").reset_index(drop=True)
    parent = ORBConfig(range_minutes=30, pending_expire_minutes=60,
                       ldn_enabled=True, ldn_start_hour=7,
                       ny_enabled=False)
    cfg = SRConfig(risk_pct=1.0, mode="stop", buffer_pts=0)
    res = simulate(m5, m1, parent, cfg, _meta_default(), initial_balance=10_000.0)
    assert isinstance(res, SRSimResult)
    assert res.sessions_total == 2
    assert res.tp_count == 2
    assert res.sl_count == 0
    assert res.skip_no_sweep == 0
    assert res.skip_no_rr == 0
    assert res.expire_no_fill == 0
    assert len(res.deals) == 2
    assert all(d.pnl > 0 for d in res.deals)
