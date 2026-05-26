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
