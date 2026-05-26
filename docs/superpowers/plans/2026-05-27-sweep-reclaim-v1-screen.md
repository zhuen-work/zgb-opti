# SR_v1 Sweep-and-Reclaim Stage-1 Screen — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone simulator + screen driver that runs three configs (`baseline`, `SR_stop`, `SR_limit`) per the design spec [2026-05-27-sweep-reclaim-v1-design.md](../specs/2026-05-27-sweep-reclaim-v1-design.md) and emits a portfolio-haircut decision (PASS / PARTIAL / REJECT) plus per-pair SR↔ORB weekly-NP correlation.

**Architecture:** New self-contained module `src/zgb_sim/sweep_reclaim.py` with its own pure-NumPy simulator (no numba). Zero changes to the ORB engine — if Stage 1 is rejected, the module is deleted in one commit. The screen driver `scripts/sim_orb_sweep_reclaim_screen.py` mirrors `scripts/sim_orb_fractal_screen.py`: same loading, aggregation, output structure. Baseline config uses the existing `orb_simulate`; SR configs use the new module.

**Tech Stack:** Python 3.11, NumPy, pandas, `pytest`. Reuses `zgb_sim.scalper_v1.SymbolMeta / _calc_lots / _norm_price / Deal`, `zgb_sim.tick_loader.{symbol_meta, load_bars, kill_mt5_terminal}`, and `STREAM_CFGS / make_stream_cfg` from `scripts/sim_wfo_hedge_retry.py`.

---

## File Structure

**Create:**
- `src/zgb_sim/sweep_reclaim.py` — new simulator. Contains:
  - `SRConfig` dataclass (per-stream params + mode + buffer)
  - `_detect_sweep_setup(bar_high, bar_low, bar_close, range_high, range_low) -> Optional[Setup]`
  - `_build_sr_sessions(m5_bars, parent_cfg, mode) -> list[SRSession]`
  - `_simulate_session(session, m5_window, m1_window, cfg, meta, balance) -> SessionResult`
  - `simulate(m5_bars, m1_bars, parent_cfg, mode, meta, initial_balance) -> SRSimResult`
- `tests/zgb_sim/test_sweep_reclaim.py` — unit tests for detector, session builder, single-session sim (V_stop / V_limit), edge cases.
- `scripts/sim_orb_sweep_reclaim_screen.py` — screen driver. Loops 3 configs, deal-merges, applies haircut, computes pair correlation, writes CSVs + summary.md, prints decision.

**Modify:** None.

**Out scope:** ORB engine, EA, setfiles, `D:\` mirroring.

---

## Task 1: Module scaffold + setup detector (TDD)

**Files:**
- Create: `src/zgb_sim/sweep_reclaim.py`
- Test: `tests/zgb_sim/test_sweep_reclaim.py`

- [ ] **Step 1: Write the failing tests for `_detect_sweep_setup`**

```python
# tests/zgb_sim/test_sweep_reclaim.py
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/zgb_sim/test_sweep_reclaim.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'zgb_sim.sweep_reclaim'`.

- [ ] **Step 3: Create the module with `Setup` and `_detect_sweep_setup`**

```python
# src/zgb_sim/sweep_reclaim.py
"""Sweep-and-Reclaim counter-stream simulator (SR_v1).

Spec: docs/superpowers/specs/2026-05-27-sweep-reclaim-v1-design.md
Stage-1 feasibility gate. Standalone — no ORB co-running.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import List, Optional

import numpy as np
import pandas as pd

from .scalper_v1 import Deal, SymbolMeta, _calc_lots, _norm_price


@dataclass(frozen=True)
class Setup:
    """One sweep-and-reclaim setup detected on a single M5 bar."""
    side: str            # "SELL" or "BUY"
    sweep_high: float    # bar.high (the swept wick on SELL side)
    sweep_low: float     # bar.low  (the swept wick on BUY side)


def _detect_sweep_setup(
    bar_high: float, bar_low: float, bar_close: float,
    range_high: float, range_low: float,
) -> Optional[Setup]:
    """Return a Setup if the bar meets the sweep+reclaim condition, else None.

    SELL: bar.high > range_high AND bar.close < range_high
    BUY:  bar.low  < range_low  AND bar.close > range_low
    Dual-sweep (both sides triggered with close inside range) -> None.
    """
    sell_trig = bar_high > range_high and bar_close < range_high
    buy_trig  = bar_low  < range_low  and bar_close > range_low
    if sell_trig and buy_trig:
        return None
    if sell_trig:
        return Setup(side="SELL", sweep_high=bar_high, sweep_low=bar_low)
    if buy_trig:
        return Setup(side="BUY",  sweep_high=bar_high, sweep_low=bar_low)
    return None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/zgb_sim/test_sweep_reclaim.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/zgb_sim/sweep_reclaim.py tests/zgb_sim/test_sweep_reclaim.py
git commit -m "feat(sim): SR_v1 module scaffold + sweep-reclaim setup detector"
```

---

## Task 2: Entry / SL / TP construction (TDD)

**Files:**
- Modify: `src/zgb_sim/sweep_reclaim.py`
- Modify: `tests/zgb_sim/test_sweep_reclaim.py`

- [ ] **Step 1: Add failing tests for `_build_entry`**

Append to `tests/zgb_sim/test_sweep_reclaim.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/zgb_sim/test_sweep_reclaim.py -v -k entry`
Expected: FAIL with `ImportError: cannot import name '_build_entry'`.

- [ ] **Step 3: Implement `_build_entry` and `Entry`**

Append to `src/zgb_sim/sweep_reclaim.py`:

```python
@dataclass(frozen=True)
class Entry:
    direction: int          # +1 BUY, -1 SELL
    order_kind: str         # "SELL_STOP" | "BUY_STOP" | "SELL_LIMIT" | "BUY_LIMIT"
    entry_price: float
    sl_price: float
    tp_price: float


def _build_entry(
    setup: Setup, mode: str, buffer_pts: int,
    range_high: float, range_low: float, point: float,
) -> Optional[Entry]:
    """Construct the entry/SL/TP triple for a setup. Returns None on skip.

    mode: "stop" or "limit".
    """
    buf = buffer_pts * point
    if setup.side == "SELL":
        sl = setup.sweep_high + buf
        tp = range_low
        if mode == "stop":
            entry = setup.sweep_low - buf
            if entry <= tp:           # no profit room
                return None
            order_kind = "SELL_STOP"
        else:                          # limit
            entry = range_high         # at range edge by construction > range_low
            order_kind = "SELL_LIMIT"
        return Entry(direction=-1, order_kind=order_kind,
                     entry_price=entry, sl_price=sl, tp_price=tp)
    else:  # BUY
        sl = setup.sweep_low - buf
        tp = range_high
        if mode == "stop":
            entry = setup.sweep_high + buf
            if entry >= tp:
                return None
            order_kind = "BUY_STOP"
        else:
            entry = range_low
            order_kind = "BUY_LIMIT"
        return Entry(direction=+1, order_kind=order_kind,
                     entry_price=entry, sl_price=sl, tp_price=tp)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/zgb_sim/test_sweep_reclaim.py -v`
Expected: all (12 total) passed.

- [ ] **Step 5: Commit**

```bash
git add src/zgb_sim/sweep_reclaim.py tests/zgb_sim/test_sweep_reclaim.py
git commit -m "feat(sim): SR_v1 _build_entry — V_stop + V_limit geometry"
```

---

## Task 3: Session enumeration (TDD)

**Files:**
- Modify: `src/zgb_sim/sweep_reclaim.py`
- Modify: `tests/zgb_sim/test_sweep_reclaim.py`

- [ ] **Step 1: Add failing tests for `_build_sr_sessions`**

Append to test file:

```python
from datetime import datetime, timezone
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
```

- [ ] **Step 2: Run tests, verify they fail**

Run: `pytest tests/zgb_sim/test_sweep_reclaim.py -v -k sessions`
Expected: ImportError on `_build_sr_sessions`.

- [ ] **Step 3: Implement `_build_sr_sessions` + `SRSession`**

Append to `src/zgb_sim/sweep_reclaim.py`:

```python
@dataclass(frozen=True)
class SRSession:
    range_start: pd.Timestamp
    range_end:   pd.Timestamp
    expire_ts:   pd.Timestamp
    range_high:  float
    range_low:   float
    session_tag: str          # "LDN" or "NY" (diagnostic)


def _build_sr_sessions(m5_bars: pd.DataFrame, cfg) -> List[SRSession]:
    """Enumerate (LDN, NY) sessions across the M5 frame's date span.

    Mon-Fri only. Skips sessions where no M5 bars cover [range_start, range_end).
    Uses bar high/low extremes for range_high/range_low (entry_mode='wick' assumed —
    SR_v1 spec doesn't expose entry_mode at Stage 1).
    """
    if m5_bars.empty:
        return []
    ts = pd.to_datetime(m5_bars["ts"], utc=True)
    days = pd.unique(ts.dt.date)
    sessions: List[SRSession] = []
    enabled = []
    if cfg.ldn_enabled: enabled.append(("LDN", cfg.ldn_start_hour))
    if cfg.ny_enabled:  enabled.append(("NY",  cfg.ny_start_hour))
    for d in days:
        if pd.Timestamp(d).weekday() >= 5:    # Sat/Sun
            continue
        for tag, hr in enabled:
            rs = pd.Timestamp(datetime.combine(d, time(hr, 0)), tz="UTC")
            re = rs + pd.Timedelta(minutes=cfg.range_minutes)
            ex = re + pd.Timedelta(minutes=cfg.pending_expire_minutes)
            mask = (ts >= rs) & (ts < re)
            if not mask.any():
                continue
            window = m5_bars.loc[mask]
            rh = float(window["high"].max())
            rl = float(window["low"].min())
            sessions.append(SRSession(range_start=rs, range_end=re,
                                       expire_ts=ex, range_high=rh,
                                       range_low=rl, session_tag=tag))
    return sessions
```

- [ ] **Step 4: Run tests, verify they pass**

Run: `pytest tests/zgb_sim/test_sweep_reclaim.py -v`
Expected: 15 passed.

- [ ] **Step 5: Commit**

```bash
git add src/zgb_sim/sweep_reclaim.py tests/zgb_sim/test_sweep_reclaim.py
git commit -m "feat(sim): SR_v1 session enumeration with LDN/NY + weekday filter"
```

---

## Task 4: Single-session fill modeling — V_stop happy path (TDD)

**Files:**
- Modify: `src/zgb_sim/sweep_reclaim.py`
- Modify: `tests/zgb_sim/test_sweep_reclaim.py`

- [ ] **Step 1: Add failing test for a V_stop SELL that fills and hits TP**

Append:

```python
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.sweep_reclaim import _simulate_session, SessionResult, SRConfig


def _bar(ts, o, h, l, c):
    return {"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": 0}


def _meta_default():
    return SymbolMeta(point=0.01, tick_size=0.01, tick_value=1.0,
                      stops_level_pts=20, volume_min=0.01, volume_max=500.0,
                      volume_step=0.01, digits=2)


def test_v_stop_sell_fills_and_hits_tp():
    # Session: range_high=2020, range_low=1990, window 07:30-09:30
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
    # M1 frame: enrich M5 bar [07:35, 07:40) with 5 M1 bars stepping 2015 -> 2005
    m1_active = pd.DataFrame([
        _bar(pd.Timestamp("2026-02-16 07:35", tz="UTC"), 2015, 2017, 2014, 2014),
        _bar(pd.Timestamp("2026-02-16 07:36", tz="UTC"), 2014, 2014, 2012, 2012),
        _bar(pd.Timestamp("2026-02-16 07:37", tz="UTC"), 2012, 2012, 2010, 2010),
        _bar(pd.Timestamp("2026-02-16 07:38", tz="UTC"), 2010, 2010, 2008, 2008),
        _bar(pd.Timestamp("2026-02-16 07:39", tz="UTC"), 2008, 2008, 2005, 2005),
        # [07:40, 07:45) M1 bars step 2005 -> 1990 monotonically
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
    # SL distance = 2025 - 2010 = 15.00 = 1500 pts at point=0.01.
    # risk_money = 10_000 * 1% = $100.
    # sl_money per lot = 1500 * 0.01 / 0.01 * 1.0 = $15.
    # lots = 100/15 = 6.67 -> rounded to 6.67 then volume_step round.
    # P&L magnitude = 2000 pts * lots * tick_value/tick_size = 2000 * 0.01 * lots * 1/0.01 = 2000 * lots
    # Acceptance: pnl > 0 and outcome tp (numeric exactness deferred to int. tests).
```

- [ ] **Step 2: Run test, verify it fails**

Run: `pytest tests/zgb_sim/test_sweep_reclaim.py::test_v_stop_sell_fills_and_hits_tp -v`
Expected: ImportError on `_simulate_session` / `SessionResult` / `SRConfig`.

- [ ] **Step 3: Implement `SRConfig`, `SessionResult`, `_simulate_session` (V_stop path only — V_limit comes in Task 5)**

Append to `src/zgb_sim/sweep_reclaim.py`:

```python
@dataclass(frozen=True)
class SRConfig:
    risk_pct: float = 1.0
    mode: str = "stop"               # "stop" | "limit"
    buffer_pts: int = 0


@dataclass
class SessionResult:
    """Outcome of one SR session. outcome in: 'tp','sl','expired','skipped'."""
    outcome: str
    entry_ts: Optional[pd.Timestamp] = None
    entry_price: float = 0.0
    exit_ts: Optional[pd.Timestamp] = None
    exit_price: float = 0.0
    direction: int = 0
    lots: float = 0.0
    pnl: float = 0.0
    skip_reason: str = ""


def _simulate_session(
    session: SRSession,
    m5_window: pd.DataFrame,    # M5 bars with ts in [range_end, expire_ts]
    m1_window: pd.DataFrame,    # M1 bars with ts in [range_end, expire_ts]
    cfg: SRConfig,
    meta: SymbolMeta,
    balance: float,
) -> SessionResult:
    """Simulate one session: scan M5 for first sweep+reclaim, arm a pending,
    then walk M1 bars to detect fill -> SL/TP/expire.
    """
    # 1) Find first qualifying M5 bar
    active5 = m5_window[(m5_window["ts"] >= session.range_end) &
                        (m5_window["ts"] <  session.expire_ts)]
    setup: Optional[Setup] = None
    sweep_bar_ts: Optional[pd.Timestamp] = None
    for _, b in active5.iterrows():
        s = _detect_sweep_setup(float(b["high"]), float(b["low"]),
                                float(b["close"]),
                                session.range_high, session.range_low)
        if s is not None:
            setup = s
            sweep_bar_ts = b["ts"]
            break
    if setup is None:
        return SessionResult(outcome="skipped", skip_reason="no_sweep")
    if sweep_bar_ts is None:
        return SessionResult(outcome="skipped", skip_reason="no_sweep")

    # 2) Build entry triple
    entry = _build_entry(setup, mode=cfg.mode, buffer_pts=cfg.buffer_pts,
                         range_high=session.range_high,
                         range_low=session.range_low, point=meta.point)
    if entry is None:
        return SessionResult(outcome="skipped", skip_reason="no_rr")
    entry_px = _norm_price(entry.entry_price, meta)
    sl_px    = _norm_price(entry.sl_price,    meta)
    tp_px    = _norm_price(entry.tp_price,    meta)

    # 3) Lot sizing — SL distance in pts
    sl_pts = int(round(abs(sl_px - entry_px) / meta.point))
    lots = _calc_lots(balance, cfg.risk_pct, sl_pts, meta)
    if lots <= 0:
        return SessionResult(outcome="skipped", skip_reason="zero_lots")

    # 4) Walk M1 bars from bar AFTER sweep_bar_ts up to expire_ts
    one_m5 = pd.Timedelta(minutes=5)
    arm_after = sweep_bar_ts + one_m5
    walk = m1_window[(m1_window["ts"] >= arm_after) &
                     (m1_window["ts"] <  session.expire_ts)]
    filled = False
    fill_ts: Optional[pd.Timestamp] = None
    for _, b in walk.iterrows():
        hi, lo, ts = float(b["high"]), float(b["low"]), b["ts"]
        if not filled:
            # Detect fill
            if entry.order_kind == "SELL_STOP" and lo <= entry_px:
                filled = True; fill_ts = ts
            elif entry.order_kind == "BUY_STOP" and hi >= entry_px:
                filled = True; fill_ts = ts
            elif entry.order_kind == "SELL_LIMIT" and hi >= entry_px:
                filled = True; fill_ts = ts
            elif entry.order_kind == "BUY_LIMIT" and lo <= entry_px:
                filled = True; fill_ts = ts
            if not filled:
                continue
            # After detecting fill in this same bar, fall through to TP/SL check
        # SL / TP detection (post-fill). Worst case: assume SL before TP if both touched.
        if entry.direction == -1:   # SELL
            if hi >= sl_px:
                exit_px = sl_px
                pnl = (entry_px - exit_px) * lots * meta.tick_value / meta.tick_size
                return SessionResult(outcome="sl", entry_ts=fill_ts,
                                     entry_price=entry_px, exit_ts=ts,
                                     exit_price=exit_px, direction=-1,
                                     lots=lots, pnl=pnl)
            if lo <= tp_px:
                exit_px = tp_px
                pnl = (entry_px - exit_px) * lots * meta.tick_value / meta.tick_size
                return SessionResult(outcome="tp", entry_ts=fill_ts,
                                     entry_price=entry_px, exit_ts=ts,
                                     exit_price=exit_px, direction=-1,
                                     lots=lots, pnl=pnl)
        else:                        # BUY
            if lo <= sl_px:
                exit_px = sl_px
                pnl = (exit_px - entry_px) * lots * meta.tick_value / meta.tick_size
                return SessionResult(outcome="sl", entry_ts=fill_ts,
                                     entry_price=entry_px, exit_ts=ts,
                                     exit_price=exit_px, direction=+1,
                                     lots=lots, pnl=pnl)
            if hi >= tp_px:
                exit_px = tp_px
                pnl = (exit_px - entry_px) * lots * meta.tick_value / meta.tick_size
                return SessionResult(outcome="tp", entry_ts=fill_ts,
                                     entry_price=entry_px, exit_ts=ts,
                                     exit_price=exit_px, direction=+1,
                                     lots=lots, pnl=pnl)

    # 5) Expire branch
    if filled:
        # Filled but neither SL nor TP hit by expire_ts.
        # Close at the last walked bar's close.
        last = walk.iloc[-1]
        exit_px = _norm_price(float(last["close"]), meta)
        if entry.direction == -1:
            pnl = (entry_px - exit_px) * lots * meta.tick_value / meta.tick_size
        else:
            pnl = (exit_px - entry_px) * lots * meta.tick_value / meta.tick_size
        return SessionResult(outcome="expired_inflight",
                             entry_ts=fill_ts, entry_price=entry_px,
                             exit_ts=last["ts"], exit_price=exit_px,
                             direction=entry.direction, lots=lots, pnl=pnl)
    return SessionResult(outcome="expired", skip_reason="no_fill")
```

- [ ] **Step 4: Run test, verify it passes**

Run: `pytest tests/zgb_sim/test_sweep_reclaim.py::test_v_stop_sell_fills_and_hits_tp -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/zgb_sim/sweep_reclaim.py tests/zgb_sim/test_sweep_reclaim.py
git commit -m "feat(sim): SR_v1 _simulate_session V_stop happy path (sweep -> fill -> TP)"
```

---

## Task 5: V_limit + edge cases (TDD)

**Files:**
- Modify: `tests/zgb_sim/test_sweep_reclaim.py`

- [ ] **Step 1: Add failing tests for V_limit fill, SL hit, no-fill expire, dual-sweep skip, no-RR skip**

Append:

```python
def test_v_limit_sell_fills_on_retest_and_hits_tp():
    rs = pd.Timestamp("2026-02-16 07:00", tz="UTC")
    re = pd.Timestamp("2026-02-16 07:30", tz="UTC")
    ex = pd.Timestamp("2026-02-16 09:30", tz="UTC")
    session = SRSession(range_start=rs, range_end=re, expire_ts=ex,
                        range_high=2020.0, range_low=1990.0, session_tag="LDN")
    # M5 active: sweep at 07:30 (high 2025 close 2015); next M5 retests 2020
    m5_active = pd.DataFrame([
        _bar(pd.Timestamp("2026-02-16 07:30", tz="UTC"), 2018, 2025, 2010, 2015),
        _bar(pd.Timestamp("2026-02-16 07:35", tz="UTC"), 2015, 2021, 2014, 2014),
        _bar(pd.Timestamp("2026-02-16 07:40", tz="UTC"), 2014, 2014, 1990, 1992),
    ])
    # M1 during 07:35: retest 2020 then drop; 07:40+: drop monotonic to 1990
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
    # M1 [07:35, 07:40): fill at 2010 then ramp to 2026
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
    # V_limit at 2020; price never re-tests
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
    # sweep_bar.low = 1988 (already below range_low) -> no_rr for V_stop SELL
    m5_active = pd.DataFrame([
        _bar(pd.Timestamp("2026-02-16 07:30", tz="UTC"), 2018, 2025, 1988, 2015),
    ])
    cfg = SRConfig(risk_pct=1.0, mode="stop", buffer_pts=0)
    result = _simulate_session(session, m5_active, pd.DataFrame(), cfg,
                               _meta_default(), balance=10_000.0)
    assert result.outcome == "skipped"
    assert result.skip_reason == "no_rr"
```

- [ ] **Step 2: Run tests, verify the 4 V_limit/SL/expire/no_rr tests pass and one (no_sweep) too**

Run: `pytest tests/zgb_sim/test_sweep_reclaim.py -v`
Expected: All 20 tests pass (V_limit handling is already implemented in `_simulate_session` from Task 4 because the fill check covers all four order_kinds).

- [ ] **Step 3: If any V_limit / edge-case test fails, fix `_simulate_session` directly. The most likely fixup is M1-bar-of-sweep handling. Re-run.**

- [ ] **Step 4: Commit**

```bash
git add tests/zgb_sim/test_sweep_reclaim.py src/zgb_sim/sweep_reclaim.py
git commit -m "test(sim): SR_v1 V_limit fill, SL hit, expire, dual-sweep, no-RR coverage"
```

---

## Task 6: Top-level `simulate()` + diagnostic counters

**Files:**
- Modify: `src/zgb_sim/sweep_reclaim.py`
- Modify: `tests/zgb_sim/test_sweep_reclaim.py`

- [ ] **Step 1: Add failing test for `simulate()` returning per-session deals + skip/expire counters**

Append:

```python
from zgb_sim.sweep_reclaim import simulate, SRSimResult


def test_simulate_aggregates_sessions():
    # 2 weekdays, LDN-only, range_minutes=30, expire=60
    days = [datetime(2026, 2, 16, tzinfo=timezone.utc),
            datetime(2026, 2, 17, tzinfo=timezone.utc)]
    rows_m5, rows_m1 = [], []
    for d in days:
        # range window 07:00-07:30 produces range_high=2020 range_low=1990
        for k in range(6):
            t = pd.Timestamp(d) + pd.Timedelta(minutes=5*k) + pd.Timedelta(hours=7)
            rows_m5.append(_bar(t, 2000, 2020, 1990, 2000))
        # active window 07:30+
        rows_m5.append(_bar(pd.Timestamp(d)+pd.Timedelta(hours=7,minutes=30),
                            2018, 2025, 2010, 2015))   # sweep+reclaim SELL
        # M1 walks after 07:35 fill SELL_STOP @ 2010 then go to TP 1990
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
    # Two TP deals -> two non-zero pnl entries
    assert len(res.deals) == 2
    assert all(d.pnl > 0 for d in res.deals)
```

- [ ] **Step 2: Run test, verify it fails (ImportError)**

Run: `pytest tests/zgb_sim/test_sweep_reclaim.py::test_simulate_aggregates_sessions -v`
Expected: FAIL.

- [ ] **Step 3: Implement `simulate()` + `SRSimResult`**

Append to `src/zgb_sim/sweep_reclaim.py`:

```python
@dataclass
class SRSimResult:
    deals: List[Deal]
    sessions_total: int
    tp_count: int
    sl_count: int
    expire_inflight_count: int      # filled but neither TP/SL by expire
    expire_no_fill: int
    skip_no_sweep: int
    skip_no_rr: int
    skip_zero_lots: int
    initial_balance: float
    final_balance: float


def simulate(
    m5_bars: pd.DataFrame,
    m1_bars: pd.DataFrame,
    parent_cfg,                       # ORBConfig — for session enumeration
    sr_cfg: SRConfig,
    meta: SymbolMeta,
    initial_balance: float,
) -> SRSimResult:
    sessions = _build_sr_sessions(m5_bars, parent_cfg)
    deals: List[Deal] = []
    balance = initial_balance
    tp = sl = exi = enf = sns = snr = szl = 0
    for sess in sessions:
        # Slice once per session for cheap loops (sim is non-numba)
        m5w = m5_bars[(m5_bars["ts"] >= sess.range_end) &
                      (m5_bars["ts"] <  sess.expire_ts)]
        m1w = m1_bars[(m1_bars["ts"] >= sess.range_end) &
                      (m1_bars["ts"] <  sess.expire_ts)]
        r = _simulate_session(sess, m5w, m1w, sr_cfg, meta, balance)
        if r.outcome == "tp":
            tp += 1
            deals.append(Deal(ts=r.exit_ts, kind="tp", direction=r.direction,
                              lots=r.lots, price=r.exit_price, pnl=r.pnl))
            balance += r.pnl
        elif r.outcome == "sl":
            sl += 1
            deals.append(Deal(ts=r.exit_ts, kind="sl", direction=r.direction,
                              lots=r.lots, price=r.exit_price, pnl=r.pnl))
            balance += r.pnl
        elif r.outcome == "expired_inflight":
            exi += 1
            deals.append(Deal(ts=r.exit_ts, kind="other", direction=r.direction,
                              lots=r.lots, price=r.exit_price, pnl=r.pnl))
            balance += r.pnl
        elif r.outcome == "expired":
            enf += 1
        elif r.outcome == "skipped":
            if r.skip_reason == "no_sweep": sns += 1
            elif r.skip_reason == "no_rr":   snr += 1
            elif r.skip_reason == "zero_lots": szl += 1
    return SRSimResult(
        deals=deals, sessions_total=len(sessions),
        tp_count=tp, sl_count=sl,
        expire_inflight_count=exi, expire_no_fill=enf,
        skip_no_sweep=sns, skip_no_rr=snr, skip_zero_lots=szl,
        initial_balance=initial_balance, final_balance=balance,
    )
```

- [ ] **Step 4: Run all tests, verify they pass**

Run: `pytest tests/zgb_sim/test_sweep_reclaim.py -v`
Expected: 21 passed.

- [ ] **Step 5: Commit**

```bash
git add src/zgb_sim/sweep_reclaim.py tests/zgb_sim/test_sweep_reclaim.py
git commit -m "feat(sim): SR_v1 top-level simulate() with diagnostic counters"
```

---

## Task 7: Screen driver script

**Files:**
- Create: `scripts/sim_orb_sweep_reclaim_screen.py`

- [ ] **Step 1: Write the driver script**

```python
# scripts/sim_orb_sweep_reclaim_screen.py
"""SR_v1 sweep-and-reclaim Stage-1 screen (spec: docs/superpowers/specs/2026-05-27-sweep-reclaim-v1-design.md).

Runs 3 configs across the 6-stream architecture:
  - baseline : current ORB portfolio (orb_simulate)
  - SR_stop  : 6 SR streams, V_stop entry,  ORB OFF
  - SR_limit : 6 SR streams, V_limit entry, ORB OFF

Emits per-stream + portfolio-haircut metrics and a PASS/PARTIAL/REJECT decision.

Usage:
  python scripts/sim_orb_sweep_reclaim_screen.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.sweep_reclaim import simulate as sr_simulate, SRConfig

from sim_wfo_hedge_retry import STREAM_CFGS, make_stream_cfg

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
START = datetime(2026, 2, 14, tzinfo=timezone.utc)
END = datetime(2026, 4, 25, tzinfo=timezone.utc)
SPREAD = 30
PER_STREAM_RISK = 1.0

HAIRCUT_NP = 0.94
HAIRCUT_PF = 0.25

# Stage-1 thresholds (spec)
MIN_HAIRCUT_NDD = 0.50
MAX_PAIR_CORR   = 0.30

OUT_DIR = ROOT / "output" / "sweep_reclaim_screen_2026_05_27"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def aggregate(deals):
    """Same as fractal_screen.aggregate — deals is list[(ts, stream, pnl)]."""
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gains = 0.0; losses = 0.0; wins = 0; trades = 0
    for _, _s, p in sorted(deals, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p >= 0: gains += p; wins += 1
        else:      losses += -p
        trades += 1
    np_ = bal - DEPOSIT
    ndd = np_ / dd_abs if dd_abs > 0 else 0
    pf = gains / losses if losses > 0 else float("inf")
    wr = wins / trades * 100 if trades > 0 else 0
    return dict(np=np_, dd_abs=dd_abs, ndd=ndd, pf=pf, trades=trades, wr=wr)


def weekly_np(deals):
    """deals = list[(ts, stream, pnl)] -> {week_start_date: np}."""
    if not deals:
        return {}
    df = pd.DataFrame(deals, columns=["ts", "stream", "pnl"])
    df["week"] = pd.to_datetime(df["ts"]).dt.to_period("W").dt.start_time
    return df.groupby("week")["pnl"].sum().to_dict()


def pair_correlation(sr_weekly_by_stream, orb_weekly_by_stream):
    """Returns (per_pair_df, mean_corr) for matching SR_Sn vs ORB_Sn weekly NP."""
    rows = []
    for s in ("S1","S2","S3","S4","S5","S6"):
        sr  = sr_weekly_by_stream.get(s, {})
        orb = orb_weekly_by_stream.get(s, {})
        weeks = sorted(set(sr.keys()) | set(orb.keys()))
        a = np.array([sr.get(w, 0.0)  for w in weeks])
        b = np.array([orb.get(w, 0.0) for w in weeks])
        if len(weeks) < 2 or a.std() == 0 or b.std() == 0:
            corr = float("nan")
        else:
            corr = float(np.corrcoef(a, b)[0, 1])
        rows.append({"stream": s, "weeks": len(weeks), "corr": corr})
    df = pd.DataFrame(rows)
    mean_c = float(df["corr"].dropna().mean()) if df["corr"].notna().any() else float("nan")
    return df, mean_c


def run_baseline(ticks, m5, m1, meta):
    """Existing 6-stream ORB portfolio. Returns per_stream_rows, port_dict, weekly_by_stream."""
    merged = []
    per = []
    weekly_by_stream = {}
    for s in ("S1","S2","S3","S4","S5","S6"):
        cfg = make_stream_cfg(s, PER_STREAM_RISK)
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        deals = [(d.ts, s, d.pnl) for d in r.deals if d.kind != "entry"]
        agg = aggregate(deals)
        per.append({"config":"baseline","stream":s,
                    "np":agg["np"],"dd":agg["dd_abs"],"pf":agg["pf"],
                    "ndd":agg["ndd"],"trades":agg["trades"],"wr":agg["wr"],
                    "skip_no_sweep":0,"expire_no_fill":0})
        merged.extend(deals)
        weekly_by_stream[s] = weekly_np(deals)
    return per, aggregate(merged), weekly_by_stream


def run_sr(m5, m1, meta, mode):
    """6 SR streams (one per ORB stream's range geometry). mode in 'stop'|'limit'."""
    merged = []
    per = []
    weekly_by_stream = {}
    for s in ("S1","S2","S3","S4","S5","S6"):
        parent_cfg = make_stream_cfg(s, PER_STREAM_RISK)
        sr_cfg = SRConfig(risk_pct=PER_STREAM_RISK, mode=mode, buffer_pts=0)
        r = sr_simulate(m5, m1, parent_cfg, sr_cfg, meta, initial_balance=DEPOSIT)
        deals = [(d.ts, s, d.pnl) for d in r.deals]
        agg = aggregate(deals)
        per.append({"config":f"SR_{mode}","stream":s,
                    "np":agg["np"],"dd":agg["dd_abs"],"pf":agg["pf"],
                    "ndd":agg["ndd"],"trades":agg["trades"],"wr":agg["wr"],
                    "skip_no_sweep":r.skip_no_sweep,
                    "expire_no_fill":r.expire_no_fill})
        merged.extend(deals)
        weekly_by_stream[s] = weekly_np(deals)
    return per, aggregate(merged), weekly_by_stream


def decision(port_hc_ndd, mean_corr):
    if port_hc_ndd <= 0 or port_hc_ndd < MIN_HAIRCUT_NDD:
        return "REJECT"
    if not np.isnan(mean_corr) and mean_corr > MAX_PAIR_CORR:
        return "PARTIAL"
    return "PASS"


def main() -> int:
    print(f"=== SR_v1 Screen | {START.date()} -> {END.date()} | {SPREAD}pt | $10k | {PER_STREAM_RISK}%/stream ===")
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        ticks = load_ticks(SYMBOL, START, END, spread_pts=SPREAD)
        m1 = load_bars(SYMBOL, "M1", START, END)
        m5 = load_bars(SYMBOL, "M5", START, END)

        # Baseline ORB run (for reference + correlation)
        baseline_per, baseline_port, baseline_weekly = run_baseline(ticks, m5, m1, meta)
        # SR runs
        sr_stop_per,  sr_stop_port,  sr_stop_weekly  = run_sr(m5, m1, meta, "stop")
        sr_limit_per, sr_limit_port, sr_limit_weekly = run_sr(m5, m1, meta, "limit")

        # Per-stream CSV
        per_df = pd.DataFrame(baseline_per + sr_stop_per + sr_limit_per)
        per_df.to_csv(OUT_DIR / "per_stream.csv", index=False)

        # Portfolio summary + haircut + correlation + decision
        port_rows = []
        for name, port, weekly in [
            ("baseline", baseline_port, None),
            ("SR_stop",  sr_stop_port,  sr_stop_weekly),
            ("SR_limit", sr_limit_port, sr_limit_weekly),
        ]:
            np_hc  = port["np"] * HAIRCUT_NP
            pf_hc  = max(port["pf"] - HAIRCUT_PF, 0.0)
            ndd_hc = np_hc / port["dd_abs"] if port["dd_abs"] > 0 else 0
            mean_corr = float("nan")
            if weekly is not None:
                _, mean_corr = pair_correlation(weekly, baseline_weekly)
            dec = ("baseline" if name == "baseline"
                   else decision(ndd_hc, mean_corr))
            port_rows.append({
                "config": name, "np": port["np"], "np_hc": np_hc,
                "dd_abs": port["dd_abs"], "pf": port["pf"], "pf_hc": pf_hc,
                "ndd": port["ndd"], "ndd_hc": ndd_hc,
                "mean_pair_corr": mean_corr,
                "trades": port["trades"], "wr": port["wr"],
                "decision": dec,
            })
            print(f"  {name:<10} NP=${port['np']:>+8,.0f} (hc ${np_hc:>+8,.0f})  "
                  f"DD=${port['dd_abs']:>7,.0f}  NP/DD$_hc={ndd_hc:>5.2f}  "
                  f"corr={mean_corr:>+.2f}  trades={port['trades']}  -> {dec}")
        port_df = pd.DataFrame(port_rows)
        port_df.to_csv(OUT_DIR / "portfolio.csv", index=False)

        # Per-pair correlation CSV (SR_stop and SR_limit vs baseline)
        for name, weekly in [("SR_stop", sr_stop_weekly),
                              ("SR_limit", sr_limit_weekly)]:
            df, _ = pair_correlation(weekly, baseline_weekly)
            df.insert(0, "config", name)
            (df.to_csv(OUT_DIR / f"pair_corr_{name}.csv", index=False))

        # Summary markdown
        lines = [
            f"# SR_v1 Sweep-and-Reclaim Screen -- {START.date()} to {END.date()}",
            "",
            f"**Window:** {START.date()} -> {END.date()} ({(END-START).days}d)  ",
            f"**Spread:** {SPREAD}pt  |  **Deposit:** $10k  |  **Per-stream risk:** {PER_STREAM_RISK}%",
            f"**Haircut:** NP x {HAIRCUT_NP}, PF - {HAIRCUT_PF}  ",
            f"**Stage-1 thresholds:** min haircut-NP/DD$ >= {MIN_HAIRCUT_NDD}, mean pair corr <= {MAX_PAIR_CORR}",
            "",
            "## Portfolio results (deal-merged, haircut applied)", "",
            "| Config | NP | NP_hc | DD$ | NP/DD$_hc | PF_hc | MeanCorr | Trades | Decision |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for _, r in port_df.iterrows():
            corr_str = f"{r['mean_pair_corr']:+.2f}" if not np.isnan(r['mean_pair_corr']) else "n/a"
            lines.append(f"| {r['config']} | ${r['np']:+,.0f} | ${r['np_hc']:+,.0f} | "
                         f"${r['dd_abs']:,.0f} | {r['ndd_hc']:.2f} | {r['pf_hc']:.2f} | "
                         f"{corr_str} | {int(r['trades'])} | {r['decision']} |")
        (OUT_DIR / "summary.md").write_text("\n".join(lines), encoding="utf-8")
        print(f"\nWrote: {OUT_DIR/'per_stream.csv'}, {OUT_DIR/'portfolio.csv'}, "
              f"{OUT_DIR/'summary.md'}, pair_corr_*.csv")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Smoke-run on a 1-week window first**

Edit START/END locally to a 1-week window (e.g. 2026-04-13 → 2026-04-20) and run:

```bash
python scripts/sim_orb_sweep_reclaim_screen.py
```

Expected: all 3 configs print one line each with non-NaN NP/DD$; CSVs and `summary.md` appear under `output/sweep_reclaim_screen_2026_05_27/`. SR configs should have non-zero `trades` (the screen is broken if they report 0).

- [ ] **Step 3: Revert to the full window (2026-02-14 → 2026-04-25) and run for real**

```bash
python scripts/sim_orb_sweep_reclaim_screen.py
```

Cap parallel workers per `feedback_max_6_workers.md` — n/a here (serial), but expected wall-time is several minutes from M1 walks on ~70 days × 6 streams × 2 configs ≈ 840 sessions; sims are pure-Python so 1-5 min total. If runtime > 10 min, add a heartbeat per `feedback_heartbeat_10min.md`.

- [ ] **Step 4: Commit**

```bash
git add scripts/sim_orb_sweep_reclaim_screen.py output/sweep_reclaim_screen_2026_05_27/
git commit -m "feat(sim): SR_v1 Stage-1 screen driver + first-run output"
```

---

## Task 8: Interpret results and write follow-up memory

**Files:**
- Create: one of `memory/project_sweep_reclaim_v1_<outcome>.md` (where outcome ∈ `passed`, `partial`, `rejected`)
- Modify: `memory/MEMORY.md` (add one index line)

- [ ] **Step 1: Read `output/sweep_reclaim_screen_2026_05_27/summary.md` + `pair_corr_*.csv`**

- [ ] **Step 2: Apply decision rule from spec**

| `port_df.decision` | Action |
|---|---|
| `REJECT` (both SR configs) | Write `project_sweep_reclaim_v1_rejected.md`. Delete `src/zgb_sim/sweep_reclaim.py`, `tests/zgb_sim/test_sweep_reclaim.py`, `scripts/sim_orb_sweep_reclaim_screen.py`. |
| `PARTIAL` (any config positive NDD but corr > 0.30) | Write `project_sweep_reclaim_v1_partial.md`. Keep code (may inform future variants); no Stage 2. |
| `PASS` (≥1 config positive NDD AND corr ≤ 0.30) | Write `project_sweep_reclaim_v1_passed.md` naming the winning config. Stage-2 design is a separate spec, NOT in this plan. |

- [ ] **Step 3: Write the memory file**

Template (replace `<outcome>` and metric values):

```markdown
---
name: project-sweep-reclaim-v1-<outcome>
description: SR_v1 Stage-1 screen <outcome>. <one-line headline>.
metadata:
  type: project
---

**Date:** 2026-05-?? (fill at write time)
**Spec:** docs/superpowers/specs/2026-05-27-sweep-reclaim-v1-design.md
**Plan:** docs/superpowers/plans/2026-05-27-sweep-reclaim-v1-screen.md
**Output:** output/sweep_reclaim_screen_2026_05_27/

## Result

| Config   | NP_hc | DD$ | NP/DD$_hc | Mean pair corr | Decision |
|---|---|---|---|---|---|
| baseline | $... | $... | ... | n/a | reference |
| SR_stop  | $... | $... | ... | ... | ... |
| SR_limit | $... | $... | ... | ... | ... |

## Headline

<2-3 sentences on what happened. Was the sweep-and-reclaim thesis structurally
viable on this instrument/regime? Was correlation low enough to bother with
Stage 2? Any per-stream surprises?>

## Why

<Tie back to the trigger geometry: did sweeps happen often enough? Were V_stop
fills frequent or rare? Did the TP at opposite-range-edge prove reachable or
did most fills SL first?>

## How to apply

<For REJECT: "Do not propose SR-style structural fades again without a new
regime signal or alternative sweep level (per [[project_mean_reversion_dead_on_gold]],
[[project_msb50_rejected]]).">

<For PARTIAL/PASS: explicit next step (Stage 2 design open question list).>
```

- [ ] **Step 4: Add one line to `memory/MEMORY.md`**

Index entry (one of):

```
- [SR_v1 sweep-and-reclaim REJECTED 2026-05-??](project_sweep_reclaim_v1_rejected.md) — Stage-1 screen failed haircut-NP/DD$ >= 0.5. Counter-trend on this instrument needs a new angle.
- [SR_v1 sweep-and-reclaim PARTIAL 2026-05-??](project_sweep_reclaim_v1_partial.md) — positive NDD but pair corr > +0.30. Insufficient diversification value for Stage 2.
- [SR_v1 sweep-and-reclaim PASS 2026-05-??](project_sweep_reclaim_v1_passed.md) — <winner config> cleared thresholds. Stage 2 (integration) next.
```

- [ ] **Step 5: Commit**

```bash
git add memory/project_sweep_reclaim_v1_*.md memory/MEMORY.md
git commit -m "memory: SR_v1 Stage-1 screen outcome (<outcome>)"
```

- [ ] **Step 6 (REJECT only): Delete the SR module + tests + screen driver**

```bash
git rm src/zgb_sim/sweep_reclaim.py tests/zgb_sim/test_sweep_reclaim.py scripts/sim_orb_sweep_reclaim_screen.py
git commit -m "chore(sim): remove SR_v1 module after Stage-1 rejection"
```

(Keep `output/sweep_reclaim_screen_2026_05_27/` for the historical record.)

---

## Self-review notes

- **Spec coverage:** Setup detection (Task 1), entry/SL/TP geometry incl. V_stop/V_limit + skip rules (Task 2), session enumeration LDN/NY weekday-only (Task 3), fill modeling via M1 (Tasks 4-5), top-level simulate + diagnostic counters (Task 6), 3-config screen + haircut + pair correlation + decision rule (Task 7), outcome memory (Task 8). All present.
- **Out-of-scope check:** Buffer/min-sweep-depth sweeps, Stage-2 integration design, EA/setfile/live changes — confirmed not in any task.
- **Type consistency:** `Setup`, `Entry`, `SRSession`, `SRConfig`, `SessionResult`, `SRSimResult` defined exactly once and referenced consistently across tasks. `_simulate_session` returns `SessionResult` (single session); `simulate` returns `SRSimResult` (aggregate). `mode` field is `"stop"` / `"limit"` throughout — never `"v_stop"` / `"v_limit"`. The screen driver uses `"SR_stop"` / `"SR_limit"` for config labels but passes `mode="stop" | "limit"` to `SRConfig`.
- **No placeholders:** All steps have concrete code or commands; no TBDs.
