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


# Task 2: Entry / SL / TP construction

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


# Task 3: Session enumeration

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


# Task 4: Single-session fill modeling — V_stop happy path

@dataclass(frozen=True)
class SRConfig:
    risk_pct: float = 1.0
    mode: str = "stop"               # "stop" | "limit"
    buffer_pts: int = 0


@dataclass
class SessionResult:
    """Outcome of one SR session. outcome in: 'tp','sl','expired','skipped','expired_inflight'."""
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

    entry = _build_entry(setup, mode=cfg.mode, buffer_pts=cfg.buffer_pts,
                         range_high=session.range_high,
                         range_low=session.range_low, point=meta.point)
    if entry is None:
        return SessionResult(outcome="skipped", skip_reason="no_rr")

    # Use the underlying meta for price norm; scalper_v1._norm_price signature is (price, meta)
    entry_px = _norm_price(entry.entry_price, meta)
    sl_px    = _norm_price(entry.sl_price,    meta)
    tp_px    = _norm_price(entry.tp_price,    meta)

    sl_pts = int(round(abs(sl_px - entry_px) / meta.point))
    lots = _calc_lots(balance, cfg.risk_pct, sl_pts, meta)
    if lots <= 0:
        return SessionResult(outcome="skipped", skip_reason="zero_lots")

    one_m5 = pd.Timedelta(minutes=5)
    arm_after = sweep_bar_ts + one_m5
    walk = m1_window[(m1_window["ts"] >= arm_after) &
                     (m1_window["ts"] <  session.expire_ts)]
    filled = False
    fill_ts: Optional[pd.Timestamp] = None
    for _, b in walk.iterrows():
        hi, lo, ts = float(b["high"]), float(b["low"]), b["ts"]
        if not filled:
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

    if filled:
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
