"""Numba-compiled Sweep Reversal simulator. Same semantics as sweep_rev.simulate().

Sweep events pre-detected in Python (one-shot per call), JIT loop processes
ticks for fills + SL/TP.
"""
from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd
from numba import njit

from .sweep_rev import SweepRevConfig
from .scalper_v1 import SymbolMeta, SimResult, Deal


D_ENTRY = np.int8(0)
D_TP = np.int8(1)
D_SL = np.int8(2)
D_OTHER = np.int8(3)

MAX_POSITIONS = 4    # at most 2 (HalfTP split)
MAX_DEALS = 50_000


@njit(cache=True, fastmath=False)
def _norm_price(price, tick_size, digits):
    return round(round(price / tick_size) * tick_size, digits)


@njit(cache=True, fastmath=False)
def _calc_lots(balance, risk_pct, sl_pts, point, tick_size, tick_value,
               volume_min, volume_max, volume_step):
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
def _pnl(direction, entry, close_price, lots, tick_value, tick_size):
    diff = (close_price - entry) * direction
    return diff * lots * tick_value / tick_size


@njit(cache=True, fastmath=False)
def _detect_sweeps_jit(highs, lows, closes, lookback, sweep_min, point, confirm_bars):
    """Returns parallel arrays: trigger_bar_idx[], direction[], sweep_extreme[], swing_level[]."""
    n = len(highs)
    max_events = n  # upper bound
    out_trigger = np.zeros(max_events, dtype=np.int64)
    out_dir = np.zeros(max_events, dtype=np.int8)
    out_extreme = np.zeros(max_events, dtype=np.float64)
    out_swing = np.zeros(max_events, dtype=np.float64)
    count = 0

    for j in range(lookback + 1, n - confirm_bars - 1):
        # swing prior to bar j
        swing_hi = highs[j - lookback]
        swing_lo = lows[j - lookback]
        for k in range(j - lookback + 1, j):
            if highs[k] > swing_hi:
                swing_hi = highs[k]
            if lows[k] < swing_lo:
                swing_lo = lows[k]

        c = closes[j]

        # Bullish sweep (high taken) → SELL setup
        if highs[j] >= swing_hi + sweep_min * point and c < swing_hi:
            confirm_idx = j + confirm_bars
            invalidated = False
            for k in range(j + 1, confirm_idx + 1):
                if highs[k] > highs[j]:
                    invalidated = True
                    break
            if not invalidated and closes[confirm_idx] < swing_hi:
                # Trigger at the BAR AFTER confirm_idx (next M5 open)
                next_idx = confirm_idx + 1
                if next_idx < n:
                    out_trigger[count] = next_idx
                    out_dir[count] = -1
                    out_extreme[count] = highs[j]
                    out_swing[count] = swing_hi
                    count += 1

        # Bearish sweep (low taken) → BUY setup
        if lows[j] <= swing_lo - sweep_min * point and c > swing_lo:
            confirm_idx = j + confirm_bars
            invalidated = False
            for k in range(j + 1, confirm_idx + 1):
                if lows[k] < lows[j]:
                    invalidated = True
                    break
            if not invalidated and closes[confirm_idx] > swing_lo:
                next_idx = confirm_idx + 1
                if next_idx < n:
                    out_trigger[count] = next_idx
                    out_dir[count] = 1
                    out_extreme[count] = lows[j]
                    out_swing[count] = swing_lo
                    count += 1

    return out_trigger[:count], out_dir[:count], out_extreme[:count], out_swing[:count]


@njit(cache=True, fastmath=False)
def _run_sim(
    tick_ts, tick_bid, tick_ask, tick_day_idx,
    sig_trigger_ts_ns, sig_dir, sig_extreme,
    sl_buffer_pts, rr_ratio, htp_ratio,
    daily_target_pct, daily_loss_pct,
    risk_pct, initial_balance,
    point, tick_size, tick_value, stops_level_pts,
    volume_min, volume_max, volume_step, digits,
    deal_ts, deal_kind, deal_dir, deal_lots, deal_price, deal_pnl,
):
    pos_dir = np.zeros(MAX_POSITIONS, dtype=np.int8)
    pos_entry = np.zeros(MAX_POSITIONS, dtype=np.float64)
    pos_sl = np.zeros(MAX_POSITIONS, dtype=np.float64)
    pos_tp = np.zeros(MAX_POSITIONS, dtype=np.float64)
    pos_lots = np.zeros(MAX_POSITIONS, dtype=np.float64)
    pos_active = np.zeros(MAX_POSITIONS, dtype=np.bool_)

    balance = initial_balance
    balance_max = initial_balance
    dd_abs = 0.0

    session_day = np.int64(-1)
    balance_day_start = initial_balance
    realized_today = 0.0
    daily_lock = False

    deal_count = 0
    next_sig = 0
    n_ticks = tick_ts.shape[0]
    n_sig = sig_trigger_ts_ns.shape[0]

    for k in range(n_ticks):
        ts_ns = tick_ts[k]
        bid = tick_bid[k]
        ask = tick_ask[k]
        day = tick_day_idx[k]

        # Daily rollover
        if day != session_day:
            session_day = day
            balance_day_start = balance
            realized_today = 0.0
            daily_lock = False

        # Daily cap check
        if not daily_lock:
            unrealized = 0.0
            for i in range(MAX_POSITIONS):
                if pos_active[i]:
                    close_px = bid if pos_dir[i] == 1 else ask
                    unrealized += _pnl(pos_dir[i], pos_entry[i], close_px,
                                        pos_lots[i], tick_value, tick_size)
            today_pnl = realized_today + unrealized
            target_locked = False
            if daily_target_pct > 0 and today_pnl >= balance_day_start * daily_target_pct / 100.0:
                target_locked = True
            elif daily_loss_pct > 0 and today_pnl <= -balance_day_start * daily_loss_pct / 100.0:
                target_locked = True
            if target_locked:
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
                daily_lock = True

        if daily_lock:
            continue

        # Process triggered signals
        while next_sig < n_sig and sig_trigger_ts_ns[next_sig] <= ts_ns:
            sid = next_sig
            next_sig += 1
            # Skip if any open position
            any_pos = False
            for i in range(MAX_POSITIONS):
                if pos_active[i]:
                    any_pos = True
                    break
            if any_pos:
                continue

            direction = sig_dir[sid]
            sweep_extreme = sig_extreme[sid]

            if direction == 1:
                entry_price = ask
                sl_price = _norm_price(sweep_extreme - sl_buffer_pts * point, tick_size, digits)
                sl_dist = entry_price - sl_price
                if sl_dist <= 0:
                    continue
                tp_price = _norm_price(entry_price + sl_dist * rr_ratio, tick_size, digits)
            else:
                entry_price = bid
                sl_price = _norm_price(sweep_extreme + sl_buffer_pts * point, tick_size, digits)
                sl_dist = sl_price - entry_price
                if sl_dist <= 0:
                    continue
                tp_price = _norm_price(entry_price - sl_dist * rr_ratio, tick_size, digits)

            sl_pts = int(sl_dist / point)
            total_lots = _calc_lots(balance, risk_pct, sl_pts,
                                    point, tick_size, tick_value,
                                    volume_min, volume_max, volume_step)
            if total_lots <= 0:
                continue

            if htp_ratio > 0:
                half_lots = round(total_lots / 2.0 / volume_step) * volume_step
                if half_lots < volume_min:
                    half_lots = volume_min
                half_lots = round(half_lots, 2)
                if direction == 1:
                    tp_half = _norm_price(entry_price + sl_dist * rr_ratio * htp_ratio, tick_size, digits)
                else:
                    tp_half = _norm_price(entry_price - sl_dist * rr_ratio * htp_ratio, tick_size, digits)

                # Add 2 positions
                for _ in range(2):
                    for i in range(MAX_POSITIONS):
                        if not pos_active[i]:
                            pos_dir[i] = direction
                            pos_entry[i] = entry_price
                            pos_sl[i] = sl_price
                            pos_lots[i] = half_lots
                            pos_active[i] = True
                            break
                # Set TPs (first half_tp, then full)
                # Iterate active positions and set the first 2 newly-added TPs
                # Simpler approach: track via 2 explicit slots
                # (we just set pos_tp on the slots we just activated)
                # Re-find them: highest 2 indices with active=True at this entry_price
                slot1 = -1
                slot2 = -1
                for i in range(MAX_POSITIONS):
                    if pos_active[i] and pos_entry[i] == entry_price and pos_lots[i] == half_lots:
                        if slot1 == -1:
                            slot1 = i
                        elif slot2 == -1:
                            slot2 = i
                            break
                if slot1 != -1:
                    pos_tp[slot1] = tp_half
                if slot2 != -1:
                    pos_tp[slot2] = tp_price

                if deal_count < deal_ts.shape[0]:
                    deal_ts[deal_count] = ts_ns
                    deal_kind[deal_count] = D_ENTRY
                    deal_dir[deal_count] = direction
                    deal_lots[deal_count] = half_lots
                    deal_price[deal_count] = entry_price
                    deal_pnl[deal_count] = 0.0
                    deal_count += 1
                if deal_count < deal_ts.shape[0]:
                    deal_ts[deal_count] = ts_ns
                    deal_kind[deal_count] = D_ENTRY
                    deal_dir[deal_count] = direction
                    deal_lots[deal_count] = half_lots
                    deal_price[deal_count] = entry_price
                    deal_pnl[deal_count] = 0.0
                    deal_count += 1
            else:
                for i in range(MAX_POSITIONS):
                    if not pos_active[i]:
                        pos_dir[i] = direction
                        pos_entry[i] = entry_price
                        pos_sl[i] = sl_price
                        pos_tp[i] = tp_price
                        pos_lots[i] = total_lots
                        pos_active[i] = True
                        break
                if deal_count < deal_ts.shape[0]:
                    deal_ts[deal_count] = ts_ns
                    deal_kind[deal_count] = D_ENTRY
                    deal_dir[deal_count] = direction
                    deal_lots[deal_count] = total_lots
                    deal_price[deal_count] = entry_price
                    deal_pnl[deal_count] = 0.0
                    deal_count += 1

        # SL/TP on positions
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

    return deal_count, balance, dd_abs, balance_max


def _ts_to_ns(series):
    if hasattr(series.dt, "tz") and series.dt.tz is not None:
        series = series.dt.tz_convert("UTC").dt.tz_localize(None)
    return series.values.astype("datetime64[ns]").astype(np.int64)


def simulate_fast(
    ticks: pd.DataFrame,
    m5_bars: pd.DataFrame,
    m1_bars: pd.DataFrame,
    cfg: SweepRevConfig,
    meta: SymbolMeta,
    initial_balance: float = 10_000.0,
) -> SimResult:
    tick_ts_ns = _ts_to_ns(ticks["ts"])
    tick_bid = ticks["bid"].values.astype(np.float64)
    tick_ask = ticks["ask"].values.astype(np.float64)
    tick_day_idx = (tick_ts_ns // 1_000_000_000 // 86400).astype(np.int64)

    m5_ts_ns = _ts_to_ns(m5_bars["ts"])
    m5_highs = m5_bars["high"].values.astype(np.float64)
    m5_lows = m5_bars["low"].values.astype(np.float64)
    m5_closes = m5_bars["close"].values.astype(np.float64)

    # Pre-detect sweep events
    trigger_bars, sig_dir, sig_extreme, sig_swing = _detect_sweeps_jit(
        m5_highs, m5_lows, m5_closes,
        cfg.swing_lookback, cfg.sweep_min_pts, meta.point, cfg.confirm_bars,
    )
    # Convert bar indices to trigger timestamps (M5 bar open of trigger_bar)
    sig_trigger_ts_ns = m5_ts_ns[trigger_bars]

    deal_ts = np.zeros(MAX_DEALS, dtype=np.int64)
    deal_kind = np.zeros(MAX_DEALS, dtype=np.int8)
    deal_dir = np.zeros(MAX_DEALS, dtype=np.int8)
    deal_lots = np.zeros(MAX_DEALS, dtype=np.float64)
    deal_price = np.zeros(MAX_DEALS, dtype=np.float64)
    deal_pnl = np.zeros(MAX_DEALS, dtype=np.float64)

    deal_count, final_balance, dd_abs, balance_max = _run_sim(
        tick_ts_ns, tick_bid, tick_ask, tick_day_idx,
        sig_trigger_ts_ns, sig_dir, sig_extreme,
        int(cfg.sl_buffer_pts), float(cfg.rr_ratio), float(cfg.half_tp_ratio),
        float(cfg.daily_target_pct), float(cfg.daily_loss_pct),
        float(cfg.risk_pct), float(initial_balance),
        float(meta.point), float(meta.tick_size), float(meta.tick_value),
        int(meta.stops_level_pts), float(meta.volume_min),
        float(meta.volume_max), float(meta.volume_step), int(meta.digits),
        deal_ts, deal_kind, deal_dir, deal_lots, deal_price, deal_pnl,
    )

    tp_count = sl_count = other_count = 0
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
        deals.append(Deal(ts=pd.Timestamp(int(deal_ts[i])), kind=kind_str,
                          direction=int(deal_dir[i]), lots=float(deal_lots[i]),
                          price=float(deal_price[i]), pnl=float(deal_pnl[i])))

    trades = tp_count + sl_count + other_count
    wins = sum(d.pnl for d in deals if d.kind != "entry" and d.pnl > 0)
    losses = sum(d.pnl for d in deals if d.kind != "entry" and d.pnl < 0)
    pf = wins / abs(losses) if losses != 0 else (float("inf") if wins > 0 else 0.0)
    net = final_balance - initial_balance
    dd_pct = (dd_abs / balance_max * 100.0) if balance_max > 0 else 0.0

    bc = pd.DataFrame([{"ts": d.ts, "pnl": d.pnl} for d in deals if d.kind != "entry"])
    if not bc.empty:
        bc["balance"] = initial_balance + bc["pnl"].cumsum()

    return SimResult(
        initial_balance=initial_balance, final_balance=final_balance,
        net_profit=net, trades=trades, tp_count=tp_count,
        sl_count=sl_count, other_count=other_count,
        max_drawdown=dd_abs, max_drawdown_pct=dd_pct,
        profit_factor=pf, balance_curve=bc, deals=deals,
    )
