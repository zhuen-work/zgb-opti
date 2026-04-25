"""Numba-compiled version of the Scalper_v1 simulator.

Same semantics as scalper_v1.simulate(), ~10x faster on the tick loop.
Uses parallel numpy arrays for positions/pending/deals (preallocated, no append).

Validation: should match reference (+$100.72, 372 trades) from scalper_v1.py.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import numpy as np
import pandas as pd
from numba import njit

from .scalper_v1 import S1Config, SymbolMeta, SimResult, Deal


# Constants for order kinds (int8-packed)
K_BUY_STOP = np.int8(0)
K_SELL_STOP = np.int8(1)
K_BUY_LIMIT = np.int8(2)
K_SELL_LIMIT = np.int8(3)

# Deal kind encoding
D_ENTRY = np.int8(0)
D_TP = np.int8(1)
D_SL = np.int8(2)
D_OTHER = np.int8(3)

# Preallocated slot counts
MAX_PENDING = 16   # 4 at placement × up to ~4 overlapping = generous
MAX_POSITIONS = 16
MAX_DEALS = 200_000  # 6 weeks × 1000 deals/day + margin


@njit(cache=True, fastmath=False)
def _norm_price(price: float, tick_size: float, digits: int) -> float:
    # Round to nearest tick, then round to digits to avoid float drift
    return round(round(price / tick_size) * tick_size, digits)


@njit(cache=True, fastmath=False)
def _calc_lots(balance: float, risk_pct: float, sl_pts: int,
               point: float, tick_size: float, tick_value: float,
               volume_min: float, volume_max: float, volume_step: float) -> float:
    risk_money = balance * risk_pct / 100.0
    sl_money = (sl_pts * point / tick_size) * tick_value
    if sl_money <= 0:
        return 0.0
    lots = risk_money / sl_money
    if lots < volume_min:
        lots = volume_min
    elif lots > volume_max:
        lots = volume_max
    lots = round(lots / volume_step) * volume_step
    return round(lots, 2)


@njit(cache=True, fastmath=False)
def _pnl(direction: int, entry: float, close_price: float, lots: float,
         tick_value: float, tick_size: float) -> float:
    diff = (close_price - entry) * direction
    return diff * lots * tick_value / tick_size


@njit(cache=True, fastmath=False)
def _add_pending(
    pend_kind, pend_price, pend_sl, pend_tp, pend_lots, pend_expire, pend_active,
    kind_val, price, sl, tp, lots, expire_ns,
) -> bool:
    """Return True on success, False if pool full."""
    for i in range(len(pend_active)):
        if not pend_active[i]:
            pend_kind[i] = kind_val
            pend_price[i] = price
            pend_sl[i] = sl
            pend_tp[i] = tp
            pend_lots[i] = lots
            pend_expire[i] = expire_ns
            pend_active[i] = True
            return True
    return False


@njit(cache=True, fastmath=False)
def _add_position(
    pos_dir, pos_entry, pos_sl, pos_tp, pos_lots, pos_active,
    direction, entry, sl, tp, lots,
) -> bool:
    for i in range(len(pos_active)):
        if not pos_active[i]:
            pos_dir[i] = direction
            pos_entry[i] = entry
            pos_sl[i] = sl
            pos_tp[i] = tp
            pos_lots[i] = lots
            pos_active[i] = True
            return True
    return False


@njit(cache=True, fastmath=False)
def _run_sim(
    # Tick streams
    tick_ts,         # int64[N] ns
    tick_bid,        # float64[N]
    tick_ask,        # float64[N]
    tick_hour,       # int8[N]
    tick_weekday,    # int8[N]
    tick_day_idx,    # int64[N] days since epoch
    # Bars
    m1_ts,           # int64[M1]
    m5_ts,           # int64[M5]
    donch_high,      # float64[M5]
    donch_low,       # float64[M5]
    # Strategy params
    donch_bars,
    tp_pts,
    sl_pts,
    htp_ratio,
    peb_bars,
    start_hour,
    end_hour,
    risk_pct,
    daily_target_pct,
    daily_loss_pct,
    hedge_mode,
    initial_balance,
    # Meta
    point,
    tick_size,
    tick_value,
    stops_level_pts,
    volume_min,
    volume_max,
    volume_step,
    digits,
    # Output buffers
    deal_ts,
    deal_kind,
    deal_dir,
    deal_lots,
    deal_price,
    deal_pnl,
):
    """JIT-compiled sim loop. Returns (deal_count, final_balance, max_drawdown_abs, balance_max)."""

    # Preallocated state
    pend_kind = np.zeros(MAX_PENDING, dtype=np.int8)
    pend_price = np.zeros(MAX_PENDING, dtype=np.float64)
    pend_sl = np.zeros(MAX_PENDING, dtype=np.float64)
    pend_tp = np.zeros(MAX_PENDING, dtype=np.float64)
    pend_lots = np.zeros(MAX_PENDING, dtype=np.float64)
    pend_expire = np.zeros(MAX_PENDING, dtype=np.int64)
    pend_active = np.zeros(MAX_PENDING, dtype=np.bool_)

    pos_dir = np.zeros(MAX_POSITIONS, dtype=np.int8)
    pos_entry = np.zeros(MAX_POSITIONS, dtype=np.float64)
    pos_sl = np.zeros(MAX_POSITIONS, dtype=np.float64)
    pos_tp = np.zeros(MAX_POSITIONS, dtype=np.float64)
    pos_lots = np.zeros(MAX_POSITIONS, dtype=np.float64)
    pos_active = np.zeros(MAX_POSITIONS, dtype=np.bool_)

    balance = initial_balance
    balance_max = initial_balance
    dd_abs = 0.0
    session_day = -1
    balance_at_day_start = initial_balance
    realized_today = 0.0

    deal_count = 0
    m1_idx = 0
    n_ticks = tick_ts.shape[0]
    n_m1 = m1_ts.shape[0]
    n_m5 = m5_ts.shape[0]

    stops_pad = stops_level_pts * point
    ns_5min = np.int64(5 * 60 * 1_000_000_000)

    for k in range(n_ticks):
        ts_ns = tick_ts[k]
        bid = tick_bid[k]
        ask = tick_ask[k]
        hour = tick_hour[k]
        weekday = tick_weekday[k]
        day = tick_day_idx[k]

        # Session rollover
        if day != session_day:
            session_day = day
            balance_at_day_start = balance
            realized_today = 0.0

        # Compute unrealized for daily target check
        unrealized = 0.0
        for i in range(MAX_POSITIONS):
            if pos_active[i]:
                close_px = bid if pos_dir[i] == 1 else ask
                unrealized += _pnl(pos_dir[i], pos_entry[i], close_px,
                                   pos_lots[i], tick_value, tick_size)

        today_pnl = realized_today + unrealized
        target_locked = False
        if daily_target_pct > 0 and today_pnl >= balance_at_day_start * daily_target_pct / 100.0:
            target_locked = True
        elif daily_loss_pct > 0 and today_pnl <= -balance_at_day_start * daily_loss_pct / 100.0:
            target_locked = True

        if target_locked:
            # Close all positions at current bid/ask, mark as 'other' deals
            for i in range(MAX_POSITIONS):
                if pos_active[i]:
                    close_px = bid if pos_dir[i] == 1 else ask
                    pnl = _pnl(pos_dir[i], pos_entry[i], close_px,
                               pos_lots[i], tick_value, tick_size)
                    balance += pnl
                    realized_today += pnl
                    if deal_count < deal_ts.shape[0]:
                        deal_ts[deal_count] = ts_ns
                        deal_kind[deal_count] = D_OTHER
                        deal_dir[deal_count] = pos_dir[i]
                        deal_lots[deal_count] = pos_lots[i]
                        deal_price[deal_count] = close_px
                        deal_pnl[deal_count] = pnl
                        deal_count += 1
                    if balance > balance_max:
                        balance_max = balance
                    cur_dd = balance_max - balance
                    if cur_dd > dd_abs:
                        dd_abs = cur_dd
                    pos_active[i] = False
            # Clear pending
            for i in range(MAX_PENDING):
                pend_active[i] = False
            # Advance m1_idx past any bars <= this tick
            while m1_idx < n_m1 and m1_ts[m1_idx] <= ts_ns:
                m1_idx += 1
            continue

        # Check trading hours
        in_hrs = True
        if weekday == 5 or weekday == 6:
            in_hrs = False
        elif hour < start_hour or hour >= end_hour:
            in_hrs = False

        # Out of hours: clear pending
        if not in_hrs:
            for i in range(MAX_PENDING):
                pend_active[i] = False

        # Expire pending past expire_ts (strict <)
        for i in range(MAX_PENDING):
            if pend_active[i] and ts_ns >= pend_expire[i]:
                pend_active[i] = False

        # Check pending triggers (fills become new_positions added AFTER SL/TP check)
        new_pos_dir = np.zeros(MAX_PENDING, dtype=np.int8)
        new_pos_entry = np.zeros(MAX_PENDING, dtype=np.float64)
        new_pos_sl = np.zeros(MAX_PENDING, dtype=np.float64)
        new_pos_tp = np.zeros(MAX_PENDING, dtype=np.float64)
        new_pos_lots = np.zeros(MAX_PENDING, dtype=np.float64)
        new_pos_count = 0

        for i in range(MAX_PENDING):
            if not pend_active[i]:
                continue
            triggered = False
            direction = 0
            fill = 0.0
            k_val = pend_kind[i]
            p_val = pend_price[i]
            if k_val == K_BUY_STOP and ask >= p_val:
                triggered = True
                direction = 1
                fill = p_val
            elif k_val == K_SELL_STOP and bid <= p_val:
                triggered = True
                direction = -1
                fill = p_val
            elif k_val == K_SELL_LIMIT and bid >= p_val:
                triggered = True
                direction = -1
                fill = p_val
            elif k_val == K_BUY_LIMIT and ask <= p_val:
                triggered = True
                direction = 1
                fill = p_val

            if triggered:
                new_pos_dir[new_pos_count] = direction
                new_pos_entry[new_pos_count] = fill
                new_pos_sl[new_pos_count] = pend_sl[i]
                new_pos_tp[new_pos_count] = pend_tp[i]
                new_pos_lots[new_pos_count] = pend_lots[i]
                new_pos_count += 1
                if deal_count < deal_ts.shape[0]:
                    deal_ts[deal_count] = ts_ns
                    deal_kind[deal_count] = D_ENTRY
                    deal_dir[deal_count] = direction
                    deal_lots[deal_count] = pend_lots[i]
                    deal_price[deal_count] = fill
                    deal_pnl[deal_count] = 0.0
                    deal_count += 1
                pend_active[i] = False

        # Check SL/TP on EXISTING positions only (not new_positions this tick)
        for i in range(MAX_POSITIONS):
            if not pos_active[i]:
                continue
            hit_sl = False
            hit_tp = False
            if pos_dir[i] == 1:
                if bid <= pos_sl[i]:
                    hit_sl = True
                elif bid >= pos_tp[i]:
                    hit_tp = True
            else:
                if ask >= pos_sl[i]:
                    hit_sl = True
                elif ask <= pos_tp[i]:
                    hit_tp = True

            if hit_sl:
                pnl = _pnl(pos_dir[i], pos_entry[i], pos_sl[i],
                           pos_lots[i], tick_value, tick_size)
                balance += pnl
                realized_today += pnl
                if deal_count < deal_ts.shape[0]:
                    deal_ts[deal_count] = ts_ns
                    deal_kind[deal_count] = D_SL
                    deal_dir[deal_count] = pos_dir[i]
                    deal_lots[deal_count] = pos_lots[i]
                    deal_price[deal_count] = pos_sl[i]
                    deal_pnl[deal_count] = pnl
                    deal_count += 1
                if balance > balance_max:
                    balance_max = balance
                cur_dd = balance_max - balance
                if cur_dd > dd_abs:
                    dd_abs = cur_dd
                pos_active[i] = False
            elif hit_tp:
                pnl = _pnl(pos_dir[i], pos_entry[i], pos_tp[i],
                           pos_lots[i], tick_value, tick_size)
                balance += pnl
                realized_today += pnl
                if deal_count < deal_ts.shape[0]:
                    deal_ts[deal_count] = ts_ns
                    deal_kind[deal_count] = D_TP
                    deal_dir[deal_count] = pos_dir[i]
                    deal_lots[deal_count] = pos_lots[i]
                    deal_price[deal_count] = pos_tp[i]
                    deal_pnl[deal_count] = pnl
                    deal_count += 1
                if balance > balance_max:
                    balance_max = balance
                cur_dd = balance_max - balance
                if cur_dd > dd_abs:
                    dd_abs = cur_dd
                pos_active[i] = False

        # Now add new_positions from this tick's fills
        for j in range(new_pos_count):
            _add_position(pos_dir, pos_entry, pos_sl, pos_tp, pos_lots, pos_active,
                          new_pos_dir[j], new_pos_entry[j], new_pos_sl[j],
                          new_pos_tp[j], new_pos_lots[j])

        # Process new M1 bars
        while m1_idx < n_m1 and m1_ts[m1_idx] <= ts_ns:
            bar_ts_ns = m1_ts[m1_idx]
            m1_idx += 1

            # Need weekday/hour of bar_ts — use current tick's (close enough, same M1)
            # But to be correct at bar boundary, compute from bar_ts
            # Simple: same weekday/hour if in same tick-bar alignment; use tick's since they match within 1 minute
            # More robust: compute bar's weekday/hour directly
            bar_seconds = bar_ts_ns // 1_000_000_000
            bar_days = bar_seconds // 86400
            # 1970-01-01 = Thursday (Python weekday 3 where Mon=0). Formula: (days+3)%7.
            bar_weekday = (bar_days + 3) % 7
            bar_hour = (bar_seconds // 3600) % 24

            bar_in_hrs = True
            if bar_weekday == 5 or bar_weekday == 6:
                bar_in_hrs = False
            elif bar_hour < start_hour or bar_hour >= end_hour:
                bar_in_hrs = False
            if not bar_in_hrs:
                continue

            # Skip if pending exists
            any_pending = False
            for i in range(MAX_PENDING):
                if pend_active[i]:
                    any_pending = True
                    break
            if any_pending:
                continue

            # Find last M5 bar with ts <= bar_ts (matches slow sim's
            # searchsorted(side='right') - 1 semantics).
            lo = 0
            hi = n_m5
            while lo < hi:
                mid = (lo + hi) // 2
                if m5_ts[mid] <= bar_ts_ns:
                    lo = mid + 1
                else:
                    hi = mid
            last_m5 = lo - 1
            if last_m5 < donch_bars:
                continue
            dh = donch_high[last_m5]
            dl = donch_low[last_m5]
            if np.isnan(dh) or np.isnan(dl):
                continue

            expire_ns = bar_ts_ns + peb_bars * ns_5min

            total_lots = _calc_lots(balance, risk_pct, sl_pts,
                                    point, tick_size, tick_value,
                                    volume_min, volume_max, volume_step)
            if total_lots <= 0:
                continue

            half_lots = total_lots
            if htp_ratio > 0:
                half_lots = round(total_lots / 2.0 / volume_step) * volume_step
                if half_lots < volume_min:
                    half_lots = volume_min
                half_lots = round(half_lots, 2)

            # BuyStop at Donchian high
            high_brk = _norm_price(dh, tick_size, digits)
            min_buy = _norm_price(ask + stops_pad, tick_size, digits)
            if high_brk < min_buy:
                high_brk = min_buy
            if high_brk > ask:
                sl = _norm_price(high_brk - sl_pts * point, tick_size, digits)
                tp = _norm_price(high_brk + tp_pts * point, tick_size, digits)
                if htp_ratio > 0:
                    tp_half = _norm_price(high_brk + tp_pts * htp_ratio * point, tick_size, digits)
                    _add_pending(pend_kind, pend_price, pend_sl, pend_tp, pend_lots, pend_expire, pend_active,
                                 K_BUY_STOP, high_brk, sl, tp_half, half_lots, expire_ns)
                    _add_pending(pend_kind, pend_price, pend_sl, pend_tp, pend_lots, pend_expire, pend_active,
                                 K_BUY_STOP, high_brk, sl, tp, half_lots, expire_ns)
                else:
                    _add_pending(pend_kind, pend_price, pend_sl, pend_tp, pend_lots, pend_expire, pend_active,
                                 K_BUY_STOP, high_brk, sl, tp, total_lots, expire_ns)

                if hedge_mode:
                    min_sell_lim = _norm_price(bid + stops_pad, tick_size, digits)
                    if high_brk >= min_sell_lim:
                        slH = _norm_price(high_brk + sl_pts * point, tick_size, digits)
                        tpH = _norm_price(high_brk - tp_pts * point, tick_size, digits)
                        if htp_ratio > 0:
                            tp_halfH = _norm_price(high_brk - tp_pts * htp_ratio * point, tick_size, digits)
                            _add_pending(pend_kind, pend_price, pend_sl, pend_tp, pend_lots, pend_expire, pend_active,
                                         K_SELL_LIMIT, high_brk, slH, tp_halfH, half_lots, expire_ns)
                            _add_pending(pend_kind, pend_price, pend_sl, pend_tp, pend_lots, pend_expire, pend_active,
                                         K_SELL_LIMIT, high_brk, slH, tpH, half_lots, expire_ns)
                        else:
                            _add_pending(pend_kind, pend_price, pend_sl, pend_tp, pend_lots, pend_expire, pend_active,
                                         K_SELL_LIMIT, high_brk, slH, tpH, total_lots, expire_ns)

            # SellStop at Donchian low
            low_brk = _norm_price(dl, tick_size, digits)
            max_sell = _norm_price(bid - stops_pad, tick_size, digits)
            if low_brk > max_sell:
                low_brk = max_sell
            if low_brk < bid:
                sl = _norm_price(low_brk + sl_pts * point, tick_size, digits)
                tp = _norm_price(low_brk - tp_pts * point, tick_size, digits)
                if htp_ratio > 0:
                    tp_half = _norm_price(low_brk - tp_pts * htp_ratio * point, tick_size, digits)
                    _add_pending(pend_kind, pend_price, pend_sl, pend_tp, pend_lots, pend_expire, pend_active,
                                 K_SELL_STOP, low_brk, sl, tp_half, half_lots, expire_ns)
                    _add_pending(pend_kind, pend_price, pend_sl, pend_tp, pend_lots, pend_expire, pend_active,
                                 K_SELL_STOP, low_brk, sl, tp, half_lots, expire_ns)
                else:
                    _add_pending(pend_kind, pend_price, pend_sl, pend_tp, pend_lots, pend_expire, pend_active,
                                 K_SELL_STOP, low_brk, sl, tp, total_lots, expire_ns)

                if hedge_mode:
                    max_buy_lim = _norm_price(ask - stops_pad, tick_size, digits)
                    if low_brk <= max_buy_lim:
                        slH = _norm_price(low_brk - sl_pts * point, tick_size, digits)
                        tpH = _norm_price(low_brk + tp_pts * point, tick_size, digits)
                        if htp_ratio > 0:
                            tp_halfH = _norm_price(low_brk + tp_pts * htp_ratio * point, tick_size, digits)
                            _add_pending(pend_kind, pend_price, pend_sl, pend_tp, pend_lots, pend_expire, pend_active,
                                         K_BUY_LIMIT, low_brk, slH, tp_halfH, half_lots, expire_ns)
                            _add_pending(pend_kind, pend_price, pend_sl, pend_tp, pend_lots, pend_expire, pend_active,
                                         K_BUY_LIMIT, low_brk, slH, tpH, half_lots, expire_ns)
                        else:
                            _add_pending(pend_kind, pend_price, pend_sl, pend_tp, pend_lots, pend_expire, pend_active,
                                         K_BUY_LIMIT, low_brk, slH, tpH, total_lots, expire_ns)

    return deal_count, balance, dd_abs, balance_max


# ----- Python wrapper -----

def _ts_to_ns(series: pd.Series) -> np.ndarray:
    """Convert pandas datetime series (possibly tz-aware) to int64 ns (naive UTC)."""
    if hasattr(series.dt, "tz") and series.dt.tz is not None:
        series = series.dt.tz_convert("UTC").dt.tz_localize(None)
    return series.values.astype("datetime64[ns]").astype(np.int64)


def _compute_donchian(m5_highs: np.ndarray, m5_lows: np.ndarray, n: int):
    """Donchian over last N completed M5 bars ending at bar i (inclusive)."""
    length = len(m5_highs)
    dh = np.full(length, np.nan)
    dl = np.full(length, np.nan)
    for i in range(n, length):
        dh[i] = m5_highs[i - n:i].max()
        dl[i] = m5_lows[i - n:i].min()
    return dh, dl


def simulate_fast(
    ticks: pd.DataFrame,
    m5_bars: pd.DataFrame,
    m1_bars: pd.DataFrame,
    cfg: S1Config,
    meta: SymbolMeta,
    initial_balance: float = 100.0,
) -> SimResult:
    """Numba-accelerated drop-in for scalper_v1.simulate()."""
    # Pre-process to numpy
    tick_ts_ns = _ts_to_ns(ticks["ts"])
    tick_bid = ticks["bid"].values.astype(np.float64)
    tick_ask = ticks["ask"].values.astype(np.float64)

    # hour, weekday, day_idx
    tick_seconds = tick_ts_ns // 1_000_000_000
    tick_day_idx = (tick_seconds // 86400).astype(np.int64)
    # weekday: 1970-01-01 was Thursday (weekday=3 Mon=0 Sun=6). Let me verify:
    # weekday formula: (days + 4) % 7 → Mon=0 when day 0 is Thursday... actually 1970-01-01 = Thursday = weekday 3 (Mon=0)
    # So (day_idx + 3) % 7 gives Mon=0.
    # Python's weekday(): Mon=0, Sun=6. 1970-01-01 weekday = 3 (Thursday).
    # So tick_weekday = (day_idx + 3) % 7
    tick_weekday = ((tick_day_idx + 3) % 7).astype(np.int8)
    tick_hour = ((tick_seconds // 3600) % 24).astype(np.int8)

    m1_ts_ns = _ts_to_ns(m1_bars["ts"])
    m5_ts_ns = _ts_to_ns(m5_bars["ts"])
    m5_highs = m5_bars["high"].values.astype(np.float64)
    m5_lows = m5_bars["low"].values.astype(np.float64)

    donch_high, donch_low = _compute_donchian(m5_highs, m5_lows, cfg.donchian_bars)

    # Output buffers
    deal_ts = np.zeros(MAX_DEALS, dtype=np.int64)
    deal_kind = np.zeros(MAX_DEALS, dtype=np.int8)
    deal_dir = np.zeros(MAX_DEALS, dtype=np.int8)
    deal_lots = np.zeros(MAX_DEALS, dtype=np.float64)
    deal_price = np.zeros(MAX_DEALS, dtype=np.float64)
    deal_pnl = np.zeros(MAX_DEALS, dtype=np.float64)

    deal_count, final_balance, dd_abs, balance_max = _run_sim(
        tick_ts_ns, tick_bid, tick_ask, tick_hour, tick_weekday, tick_day_idx,
        m1_ts_ns,
        m5_ts_ns, donch_high, donch_low,
        int(cfg.donchian_bars),
        int(cfg.take_profit_pts),
        int(cfg.stop_loss_pts),
        float(cfg.half_tp_ratio),
        int(cfg.pending_expire_bars),
        int(cfg.start_hour),
        int(cfg.end_hour),
        float(cfg.risk_pct),
        float(cfg.daily_target_pct),
        float(cfg.daily_loss_pct),
        bool(cfg.hedge_mode),
        float(initial_balance),
        float(meta.point),
        float(meta.tick_size),
        float(meta.tick_value),
        int(meta.stops_level_pts),
        float(meta.volume_min),
        float(meta.volume_max),
        float(meta.volume_step),
        int(meta.digits),
        deal_ts, deal_kind, deal_dir, deal_lots, deal_price, deal_pnl,
    )

    # Build deals list + summary
    tp_count = 0
    sl_count = 0
    other_count = 0
    deals: List[Deal] = []
    for i in range(deal_count):
        kind_int = int(deal_kind[i])
        if kind_int == int(D_ENTRY):
            kind_str = "entry"
        elif kind_int == int(D_TP):
            kind_str = "tp"; tp_count += 1
        elif kind_int == int(D_SL):
            kind_str = "sl"; sl_count += 1
        else:
            kind_str = "other"; other_count += 1
        deals.append(Deal(
            ts=pd.Timestamp(int(deal_ts[i])),
            kind=kind_str,
            direction=int(deal_dir[i]),
            lots=float(deal_lots[i]),
            price=float(deal_price[i]),
            pnl=float(deal_pnl[i]),
        ))

    trades = tp_count + sl_count + other_count
    wins_sum = sum(d.pnl for d in deals if d.kind != "entry" and d.pnl > 0)
    losses_sum = sum(d.pnl for d in deals if d.kind != "entry" and d.pnl < 0)
    pf = wins_sum / abs(losses_sum) if losses_sum != 0 else (float("inf") if wins_sum > 0 else 0.0)
    net = final_balance - initial_balance
    dd_pct = (dd_abs / balance_max * 100.0) if balance_max > 0 else 0.0

    bc = pd.DataFrame([{"ts": d.ts, "pnl": d.pnl} for d in deals if d.kind != "entry"])
    if not bc.empty:
        bc["balance"] = initial_balance + bc["pnl"].cumsum()

    return SimResult(
        initial_balance=initial_balance,
        final_balance=final_balance,
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
