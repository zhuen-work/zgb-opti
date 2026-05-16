"""GMMA + RSI pullback simulator.

Multi-TF design:
  - HTF (default H4): GMMA fans set trend bias UP / DOWN / MIXED per closed HTF bar.
  - LTF (default H1): GMMA fans + RSI(14) per closed LTF bar.

Signal evaluated on LTF bar CLOSE. Fill at next LTF bar's open (taken from the
tick stream — first tick whose ts >= next_lt_open_ts). SL/TP checked tick-by-tick
between fills until close.

Three entry modes (cfg.entry_mode):
  - "rsi_cross_50": pullback bar in last lookback_bars (price tagged the LTF
    short-fan ribbon) AND RSI dipped <50 (uptrend) / >50 (downtrend),
    then current bar closes with RSI back through 50 in the trade direction.
  - "tag_plus_side": current LTF bar closed inside the short-fan ribbon AND
    RSI on trend side (>50 long, <50 short). HTF trend must match.
  - "rsi_os_reversal": RSI crossed back above 30 (long) / below 70 (short)
    while LTF short-fan still aligned with HTF trend. HTF trend must match
    unless cfg.h4_trend_required = False (then only LTF fan alignment matters).

Stops: fixed pts grid-swept. TP = RR × SL. No trailing (Phase-2 work).
Daily caps + risk-% sizing carried over from scalper_v1 helpers.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List, Optional

import numpy as np
import pandas as pd

from .indicators import GMMA_SHORT, GMMA_LONG, ema, rsi as rsi_calc, gmma_state, in_ribbon
from .scalper_v1 import (
    Deal, Pending, Position, SimResult, SymbolMeta,
    _norm_price, _calc_lots, _pnl,
)


ENTRY_MODE_RSI_CROSS = "rsi_cross_50"
ENTRY_MODE_TAG_SIDE = "tag_plus_side"
ENTRY_MODE_RSI_OS = "rsi_os_reversal"


@dataclass
class GMMAConfig:
    risk_pct: float = 1.0
    entry_mode: str = ENTRY_MODE_TAG_SIDE
    sl_pts: int = 600
    rr_ratio: float = 2.0
    rsi_period: int = 14
    lookback_bars: int = 6
    h4_trend_required: bool = True
    rsi_os_level: float = 30.0   # for mode rsi_os_reversal (long); short uses 100-this
    daily_target_pct: float = 0.0
    daily_loss_pct: float = 0.0
    one_position_at_a_time: bool = True
    comment: str = "GMMA"


def _to_naive_ns(s: pd.Series) -> np.ndarray:
    if hasattr(s.dt, "tz") and s.dt.tz is not None:
        return s.dt.tz_convert("UTC").dt.tz_localize(None).values.astype("datetime64[ns]")
    return s.values.astype("datetime64[ns]")


def _build_h1_signals(h1: pd.DataFrame, h4: pd.DataFrame, cfg: GMMAConfig):
    """Pre-compute per-H1-bar signal state. Vectorised.

    Returns dict of numpy arrays aligned to h1 length:
      h1_close, h1_ts (ns), short_lo/hi, long_lo/hi, ltf_state,
      htf_state_at_bar (forward-filled from H4 close), rsi, rsi_prev,
      tagged_recent (any bar in last lookback closed inside short ribbon),
      rsi_dipped_long/short (any bar in last lookback had RSI on opposite side of 50),
      rsi_crossed_oversold_long/short.
    """
    close = h1["close"].astype(np.float64).reset_index(drop=True)
    high = h1["high"].astype(np.float64).reset_index(drop=True)
    low = h1["low"].astype(np.float64).reset_index(drop=True)
    ts_ns = _to_naive_ns(h1["ts"]).astype(np.int64)

    # LTF fans
    s_fans = pd.DataFrame({p: ema(close, p) for p in GMMA_SHORT})
    l_fans = pd.DataFrame({p: ema(close, p) for p in GMMA_LONG})
    short_lo = s_fans.min(axis=1).to_numpy(dtype=np.float64)
    short_hi = s_fans.max(axis=1).to_numpy(dtype=np.float64)
    long_lo = l_fans.min(axis=1).to_numpy(dtype=np.float64)
    long_hi = l_fans.max(axis=1).to_numpy(dtype=np.float64)
    ltf_up = short_lo > long_hi
    ltf_dn = short_hi < long_lo
    ltf_state = np.where(ltf_up, 1, np.where(ltf_dn, -1, 0))  # 1=UP, -1=DOWN, 0=MIXED

    # RSI on H1 close
    rsi_h1 = rsi_calc(close, cfg.rsi_period).to_numpy(dtype=np.float64)

    # HTF state forward-filled to each H1 bar: use most recent CLOSED H4 bar
    # (strictly before this H1 bar's CLOSE time)
    h4_close = h4["close"].astype(np.float64).reset_index(drop=True)
    h4_state_arr = gmma_state(h4_close).to_numpy()
    h4_close_ts_ns = (_to_naive_ns(h4["ts"]) + np.timedelta64(4, "h").astype("timedelta64[ns]")).astype(np.int64)
    # h1 bar close ts = bar open ts + 1h
    h1_close_ts_ns = (ts_ns + np.timedelta64(1, "h").astype("timedelta64[ns]")).astype(np.int64)
    # For each h1 bar, find last h4 bar whose close ts <= h1 bar close ts
    idx = np.searchsorted(h4_close_ts_ns, h1_close_ts_ns, side="right") - 1
    htf_state_at_bar = np.full(len(close), 0, dtype=np.int64)
    valid = idx >= 0
    mapped = np.where(h4_state_arr[np.clip(idx, 0, len(h4_state_arr) - 1)] == "UP", 1,
              np.where(h4_state_arr[np.clip(idx, 0, len(h4_state_arr) - 1)] == "DOWN", -1, 0))
    htf_state_at_bar = np.where(valid, mapped, 0)

    # "Tagged ribbon" per H1 bar: bar low <= short_hi AND bar high >= short_lo
    tagged = (low.to_numpy() <= short_hi) & (high.to_numpy() >= short_lo)
    tagged = np.nan_to_num(tagged, nan=False).astype(bool)

    # Rolling lookback helpers
    lb = max(1, int(cfg.lookback_bars))
    tagged_recent = np.zeros(len(close), dtype=bool)
    rsi_dipped_long = np.zeros(len(close), dtype=bool)   # any bar in window had rsi < 50
    rsi_dipped_short = np.zeros(len(close), dtype=bool)  # any bar in window had rsi > 50
    # rolling OR — cheap manual loop (len ~1500 typical, fine)
    for i in range(len(close)):
        lo_idx = max(0, i - lb)
        # exclude current bar from the "recent" window (signal eval is on current bar)
        if lo_idx == i:
            continue
        win_tag = tagged[lo_idx:i]
        if win_tag.size:
            tagged_recent[i] = win_tag.any()
        win_rsi = rsi_h1[lo_idx:i]
        if win_rsi.size:
            valid = win_rsi[~np.isnan(win_rsi)]
            if valid.size:
                rsi_dipped_long[i] = bool(valid.min() < 50.0)
                rsi_dipped_short[i] = bool(valid.max() > 50.0)

    # Previous bar RSI for cross logic
    rsi_prev = np.concatenate([[np.nan], rsi_h1[:-1]])

    return {
        "close": close.to_numpy(),
        "ts_open_ns": ts_ns,
        "ts_close_ns": h1_close_ts_ns,
        "short_lo": short_lo, "short_hi": short_hi,
        "long_lo": long_lo, "long_hi": long_hi,
        "ltf_state": ltf_state,
        "htf_state": htf_state_at_bar,
        "rsi": rsi_h1, "rsi_prev": rsi_prev,
        "tagged_recent": tagged_recent,
        "rsi_dipped_long": rsi_dipped_long,
        "rsi_dipped_short": rsi_dipped_short,
    }


def _signal_at(i: int, sig: dict, cfg: GMMAConfig) -> int:
    """Return +1 (long), -1 (short), 0 (no signal) for H1 bar i."""
    htf = int(sig["htf_state"][i])
    ltf = int(sig["ltf_state"][i])
    close = sig["close"][i]
    r = sig["rsi"][i]
    r_prev = sig["rsi_prev"][i]
    if np.isnan(r) or np.isnan(r_prev):
        return 0
    short_lo = sig["short_lo"][i]
    short_hi = sig["short_hi"][i]
    if np.isnan(short_lo) or np.isnan(short_hi):
        return 0

    trend_required = cfg.h4_trend_required
    mode = cfg.entry_mode

    # Direction candidate from HTF (or LTF if HTF disabled)
    bias = htf if trend_required else ltf
    if bias == 0:
        return 0

    if mode == ENTRY_MODE_RSI_CROSS:
        if not sig["tagged_recent"][i]:
            return 0
        if bias > 0:
            if sig["rsi_dipped_long"][i] and r_prev < 50.0 <= r:
                return +1
        else:
            if sig["rsi_dipped_short"][i] and r_prev > 50.0 >= r:
                return -1
        return 0

    if mode == ENTRY_MODE_TAG_SIDE:
        if not (short_lo <= close <= short_hi):
            return 0
        # LTF fan must also align with bias
        if bias > 0 and ltf > 0 and r > 50.0:
            return +1
        if bias < 0 and ltf < 0 and r < 50.0:
            return -1
        return 0

    if mode == ENTRY_MODE_RSI_OS:
        os_lo = cfg.rsi_os_level
        os_hi = 100.0 - os_lo
        if bias > 0 and ltf > 0 and r_prev < os_lo <= r:
            return +1
        if bias < 0 and ltf < 0 and r_prev > os_hi >= r:
            return -1
        return 0

    return 0


def simulate(
    ticks: pd.DataFrame,
    h1_bars: pd.DataFrame,
    h4_bars: pd.DataFrame,
    cfg: GMMAConfig,
    meta: SymbolMeta,
    initial_balance: float = 10_000.0,
) -> SimResult:
    """Run GMMA+RSI sim. Single-position-at-a-time (default).

    Bar-level signal eval at each LTF bar CLOSE. Fill at next LTF bar's OPEN
    (first tick whose ts >= next bar open ts). SL/TP checked tick-by-tick.
    """
    h1 = h1_bars.sort_values("ts").reset_index(drop=True)
    h4 = h4_bars.sort_values("ts").reset_index(drop=True)
    ticks = ticks.sort_values("ts").reset_index(drop=True)

    sig = _build_h1_signals(h1, h4, cfg)
    n_bars = len(sig["close"])

    t_ts = _to_naive_ns(ticks["ts"]).astype(np.int64)
    t_bid = ticks["bid"].to_numpy(dtype=np.float64)
    t_ask = ticks["ask"].to_numpy(dtype=np.float64)
    if len(t_ts) == 0:
        return SimResult(initial_balance, initial_balance, 0.0, 0, 0, 0, 0, 0.0, 0.0,
                          0.0, pd.DataFrame(), [])

    balance = initial_balance
    balance_max = initial_balance
    dd_abs = 0.0
    deals: List[Deal] = []
    open_pos: Optional[Position] = None
    open_entry_ts: Optional[pd.Timestamp] = None

    # Pending entry waiting for fill at next bar open (we'll just market-buy/sell
    # at the first tick at-or-after the next bar open ts, using current ask/bid).
    pending_dir = 0           # +1 buy market, -1 sell market
    pending_open_ts_ns = 0    # ns when the order becomes active
    pending_sl_dist_pts = 0
    pending_tp_dist_pts = 0

    # Daily cap state
    session_day: Optional[date] = None
    bal_day_start = initial_balance
    realized_today = 0.0
    daily_lock = False

    def _close_open_at(close_px: float, ts: pd.Timestamp, kind: str):
        nonlocal balance, balance_max, dd_abs, open_pos, realized_today
        if open_pos is None:
            return
        pnl = _pnl(open_pos, close_px, meta)
        balance += pnl
        realized_today += pnl
        deals.append(Deal(ts, kind, open_pos.direction, open_pos.lots, close_px, pnl))
        if balance > balance_max:
            balance_max = balance
        cur = balance_max - balance
        if cur > dd_abs:
            dd_abs = cur
        open_pos = None

    # Pre-compute the signal direction at each bar (so we don't reconstruct it inside the loop).
    bar_signal = np.zeros(n_bars, dtype=np.int8)
    for i in range(n_bars):
        bar_signal[i] = _signal_at(i, sig, cfg)

    bar_close_ts = sig["ts_close_ns"]
    # The "fill at next open" target = next bar's open ts = this bar's close ts.
    # That's the same value as bar_close_ts[i] = ts_open_ns[i] + 1h.

    next_bar_idx = 0  # next bar whose close we haven't processed yet
    diag_signals = 0
    diag_fills = 0

    for k in range(len(t_ts)):
        ts_ns = int(t_ts[k])
        bid = t_bid[k]
        ask = t_ask[k]
        ts = pd.Timestamp(ts_ns)

        # Daily rollover
        day = ts.date()
        if day != session_day:
            session_day = day
            bal_day_start = balance
            realized_today = 0.0
            daily_lock = False

        # Process bar closes up to current tick
        while next_bar_idx < n_bars and bar_close_ts[next_bar_idx] <= ts_ns:
            i = next_bar_idx
            next_bar_idx += 1
            if daily_lock:
                continue
            if pending_dir != 0:
                # An order is already queued — skip new signals till it fills
                continue
            if cfg.one_position_at_a_time and open_pos is not None:
                continue
            d = int(bar_signal[i])
            if d == 0:
                continue
            diag_signals += 1
            # Queue market fill at next tick (current tick should already be >= bar_close)
            pending_dir = d
            pending_open_ts_ns = int(bar_close_ts[i])
            pending_sl_dist_pts = int(cfg.sl_pts)
            pending_tp_dist_pts = int(round(cfg.sl_pts * cfg.rr_ratio))

        # Daily cap check before any fills/SLTP
        if not daily_lock:
            unrealized = 0.0
            if open_pos is not None:
                close_px = bid if open_pos.direction == 1 else ask
                unrealized = _pnl(open_pos, close_px, meta)
            today_pnl = realized_today + unrealized
            hit = False
            if cfg.daily_target_pct > 0 and today_pnl >= bal_day_start * cfg.daily_target_pct / 100.0:
                hit = True
            elif cfg.daily_loss_pct > 0 and today_pnl <= -bal_day_start * cfg.daily_loss_pct / 100.0:
                hit = True
            if hit:
                if open_pos is not None:
                    close_px = bid if open_pos.direction == 1 else ask
                    _close_open_at(close_px, ts, "other")
                pending_dir = 0
                daily_lock = True
                continue

        # Pending fill
        if pending_dir != 0 and ts_ns >= pending_open_ts_ns and open_pos is None:
            if pending_dir > 0:
                fill = ask
                sl = _norm_price(fill - pending_sl_dist_pts * meta.point, meta)
                tp = _norm_price(fill + pending_tp_dist_pts * meta.point, meta)
            else:
                fill = bid
                sl = _norm_price(fill + pending_sl_dist_pts * meta.point, meta)
                tp = _norm_price(fill - pending_tp_dist_pts * meta.point, meta)
            lots = _calc_lots(balance, cfg.risk_pct, pending_sl_dist_pts, meta)
            if lots > 0:
                open_pos = Position(pending_dir, fill, sl, tp, lots)
                deals.append(Deal(ts, "entry", pending_dir, lots, fill, 0.0))
                diag_fills += 1
                open_entry_ts = ts
            pending_dir = 0

        # SL/TP on open position
        if open_pos is not None:
            hit_sl = hit_tp = False
            if open_pos.direction == 1:
                if bid <= open_pos.sl: hit_sl = True
                elif bid >= open_pos.tp: hit_tp = True
            else:
                if ask >= open_pos.sl: hit_sl = True
                elif ask <= open_pos.tp: hit_tp = True
            if hit_sl:
                _close_open_at(open_pos.sl, ts, "sl")
            elif hit_tp:
                _close_open_at(open_pos.tp, ts, "tp")

    # End of stream: close any open position at last tick mid
    if open_pos is not None:
        last_bid = t_bid[-1]
        last_ask = t_ask[-1]
        close_px = last_bid if open_pos.direction == 1 else last_ask
        _close_open_at(close_px, pd.Timestamp(int(t_ts[-1])), "other")

    print(f"  [diag] H1 bars={n_bars}  signals_fired={diag_signals}  fills={diag_fills}")

    tp_count = sum(1 for d in deals if d.kind == "tp")
    sl_count = sum(1 for d in deals if d.kind == "sl")
    other_count = sum(1 for d in deals if d.kind == "other")
    trades = tp_count + sl_count + other_count

    wins = [d.pnl for d in deals if d.kind != "entry" and d.pnl > 0]
    losses = [d.pnl for d in deals if d.kind != "entry" and d.pnl < 0]
    pf = sum(wins) / abs(sum(losses)) if losses else float("inf")
    net = balance - initial_balance
    dd_pct = (dd_abs / balance_max * 100.0) if balance_max > 0 else 0.0

    bc = pd.DataFrame([{"ts": d.ts, "pnl": d.pnl} for d in deals if d.kind != "entry"])
    if not bc.empty:
        bc["balance"] = initial_balance + bc["pnl"].cumsum()

    return SimResult(
        initial_balance=initial_balance,
        final_balance=balance,
        net_profit=net,
        trades=trades,
        tp_count=tp_count,
        sl_count=sl_count,
        other_count=other_count,
        max_drawdown=dd_abs,
        max_drawdown_pct=dd_pct,
        profit_factor=pf,
        balance_curve=bc,
        deals=deals,
    )
