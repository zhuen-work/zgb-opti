"""Numba-compiled ORB simulator. Same semantics as orb.simulate(), ~25-30x faster.

Pre-builds session range bounds in Python (one-shot per call), then JIT loop
processes ticks against precomputed sessions.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import List

import numpy as np
import pandas as pd
from numba import njit

from .orb import ORBConfig
from .scalper_v1 import SymbolMeta, SimResult, Deal


K_BUY_STOP = np.int8(0)
K_SELL_STOP = np.int8(1)

D_ENTRY = np.int8(0)
D_TP = np.int8(1)
D_SL = np.int8(2)
D_OTHER = np.int8(3)

# Continuous mode can have many sessions overlapping. Refire=60 min,
# expire=240 min → up to 4 sessions × 4 pendings = 16 pendings concurrently.
# Allow generous headroom for tighter refire intervals.
MAX_PENDING = 64
MAX_POSITIONS = 16
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
def _add_pending(pend_kind, pend_price, pend_sl, pend_tp, pend_lots,
                 pend_expire, pend_active, pend_session,
                 kind_val, price, sl, tp, lots, expire_ns, session_id):
    for i in range(len(pend_active)):
        if not pend_active[i]:
            pend_kind[i] = kind_val
            pend_price[i] = price
            pend_sl[i] = sl
            pend_tp[i] = tp
            pend_lots[i] = lots
            pend_expire[i] = expire_ns
            pend_session[i] = session_id
            pend_active[i] = True
            return True
    return False


@njit(cache=True, fastmath=False)
def _add_position(pos_dir, pos_entry, pos_sl, pos_tp, pos_lots, pos_active,
                  pos_session, direction, entry, sl, tp, lots, session_id):
    for i in range(len(pos_active)):
        if not pos_active[i]:
            pos_dir[i] = direction
            pos_entry[i] = entry
            pos_sl[i] = sl
            pos_tp[i] = tp
            pos_lots[i] = lots
            pos_session[i] = session_id
            pos_active[i] = True
            return True
    return False


@njit(cache=True, fastmath=False)
def _run_sim(
    tick_ts, tick_bid, tick_ask, tick_day_idx,
    sess_range_end_ns, sess_expire_ns, sess_range_high, sess_range_low,
    range_minutes,
    buffer_pts, min_range_pts, max_range_pts, fixed_sl_pts,
    rr_ratio, htp_ratio,
    daily_target_pct, daily_loss_pct,
    risk_pct, initial_balance,
    point, tick_size, tick_value, stops_level_pts,
    volume_min, volume_max, volume_step, digits,
    be_trigger_r, be_buffer_pts,
    deal_ts, deal_kind, deal_dir, deal_lots, deal_price, deal_pnl,
):
    """JIT ORB sim. sess_* arrays pre-built; iterate ticks, fire on session end."""
    pend_kind = np.zeros(MAX_PENDING, dtype=np.int8)
    pend_price = np.zeros(MAX_PENDING, dtype=np.float64)
    pend_sl = np.zeros(MAX_PENDING, dtype=np.float64)
    pend_tp = np.zeros(MAX_PENDING, dtype=np.float64)
    pend_lots = np.zeros(MAX_PENDING, dtype=np.float64)
    pend_expire = np.zeros(MAX_PENDING, dtype=np.int64)
    pend_session = np.full(MAX_PENDING, -1, dtype=np.int32)
    pend_active = np.zeros(MAX_PENDING, dtype=np.bool_)

    pos_dir = np.zeros(MAX_POSITIONS, dtype=np.int8)
    pos_entry = np.zeros(MAX_POSITIONS, dtype=np.float64)
    pos_sl = np.zeros(MAX_POSITIONS, dtype=np.float64)
    pos_tp = np.zeros(MAX_POSITIONS, dtype=np.float64)
    pos_lots = np.zeros(MAX_POSITIONS, dtype=np.float64)
    pos_session = np.full(MAX_POSITIONS, -1, dtype=np.int32)
    pos_active = np.zeros(MAX_POSITIONS, dtype=np.bool_)
    pos_be_done = np.zeros(MAX_POSITIONS, dtype=np.bool_)
    pos_orig_sl_dist = np.zeros(MAX_POSITIONS, dtype=np.float64)  # for BE trigger price calc

    n_sess = sess_range_end_ns.shape[0]
    sess_fired = np.zeros(n_sess, dtype=np.bool_)

    balance = initial_balance
    balance_max = initial_balance
    dd_abs = 0.0

    # Daily cap state
    session_day = np.int64(-1)
    balance_day_start = initial_balance
    realized_today = 0.0
    daily_lock = False

    deal_count = 0
    next_sess = 0
    n_ticks = tick_ts.shape[0]
    stops_pad = stops_level_pts * point

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
                # Close all positions at current bid/ask
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
                # Cancel all pending
                for i in range(MAX_PENDING):
                    pend_active[i] = False
                daily_lock = True

        if daily_lock:
            continue

        # Fire any sessions whose range_end <= now and not yet fired
        while next_sess < n_sess and sess_range_end_ns[next_sess] <= ts_ns:
            sid = next_sess
            next_sess += 1
            if sess_fired[sid]:
                continue
            sess_fired[sid] = True

            rh = sess_range_high[sid]
            rl = sess_range_low[sid]
            if rh <= 0 or rl <= 0:
                continue
            range_pts = (rh - rl) / point
            if range_pts < min_range_pts or range_pts > max_range_pts:
                continue

            sl_dist_pts = fixed_sl_pts if fixed_sl_pts > 0 else int(range_pts)
            tp_dist_pts = sl_dist_pts * rr_ratio

            total_lots = _calc_lots(balance, risk_pct, sl_dist_pts,
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

            expire_ns = sess_expire_ns[sid]

            # BuyStop above range_high
            buy_entry = _norm_price(rh + buffer_pts * point, tick_size, digits)
            min_buy = _norm_price(ask + stops_pad, tick_size, digits)
            if buy_entry < min_buy:
                buy_entry = min_buy
            if buy_entry > ask:
                sl = _norm_price(buy_entry - sl_dist_pts * point, tick_size, digits)
                tp = _norm_price(buy_entry + tp_dist_pts * point, tick_size, digits)
                if htp_ratio > 0:
                    tp_half = _norm_price(buy_entry + tp_dist_pts * htp_ratio * point, tick_size, digits)
                    _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                 pend_lots, pend_expire, pend_active, pend_session,
                                 K_BUY_STOP, buy_entry, sl, tp_half, half_lots, expire_ns, sid)
                    _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                 pend_lots, pend_expire, pend_active, pend_session,
                                 K_BUY_STOP, buy_entry, sl, tp, half_lots, expire_ns, sid)
                else:
                    _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                 pend_lots, pend_expire, pend_active, pend_session,
                                 K_BUY_STOP, buy_entry, sl, tp, total_lots, expire_ns, sid)

            # SellStop below range_low
            sell_entry = _norm_price(rl - buffer_pts * point, tick_size, digits)
            max_sell = _norm_price(bid - stops_pad, tick_size, digits)
            if sell_entry > max_sell:
                sell_entry = max_sell
            if sell_entry < bid:
                sl = _norm_price(sell_entry + sl_dist_pts * point, tick_size, digits)
                tp = _norm_price(sell_entry - tp_dist_pts * point, tick_size, digits)
                if htp_ratio > 0:
                    tp_half = _norm_price(sell_entry - tp_dist_pts * htp_ratio * point, tick_size, digits)
                    _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                 pend_lots, pend_expire, pend_active, pend_session,
                                 K_SELL_STOP, sell_entry, sl, tp_half, half_lots, expire_ns, sid)
                    _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                 pend_lots, pend_expire, pend_active, pend_session,
                                 K_SELL_STOP, sell_entry, sl, tp, half_lots, expire_ns, sid)
                else:
                    _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                 pend_lots, pend_expire, pend_active, pend_session,
                                 K_SELL_STOP, sell_entry, sl, tp, total_lots, expire_ns, sid)

        # Expire pending past expire
        for i in range(MAX_PENDING):
            if pend_active[i] and ts_ns >= pend_expire[i]:
                pend_active[i] = False

        # Pending fills
        new_pos_dir = np.zeros(MAX_PENDING, dtype=np.int8)
        new_pos_entry = np.zeros(MAX_PENDING, dtype=np.float64)
        new_pos_sl = np.zeros(MAX_PENDING, dtype=np.float64)
        new_pos_tp = np.zeros(MAX_PENDING, dtype=np.float64)
        new_pos_lots = np.zeros(MAX_PENDING, dtype=np.float64)
        new_pos_session = np.full(MAX_PENDING, -1, dtype=np.int32)
        new_pos_count = 0

        filled_session_set = np.zeros(n_sess, dtype=np.bool_)

        for i in range(MAX_PENDING):
            if not pend_active[i]:
                continue
            triggered = False
            direction = 0
            fill = 0.0
            k_val = pend_kind[i]
            p_val = pend_price[i]
            if k_val == K_BUY_STOP and ask >= p_val:
                triggered = True; direction = 1; fill = p_val
            elif k_val == K_SELL_STOP and bid <= p_val:
                triggered = True; direction = -1; fill = p_val
            if triggered:
                new_pos_dir[new_pos_count] = direction
                new_pos_entry[new_pos_count] = fill
                new_pos_sl[new_pos_count] = pend_sl[i]
                new_pos_tp[new_pos_count] = pend_tp[i]
                new_pos_lots[new_pos_count] = pend_lots[i]
                new_pos_session[new_pos_count] = pend_session[i]
                new_pos_count += 1
                if deal_count < deal_ts.shape[0]:
                    deal_ts[deal_count] = ts_ns
                    deal_kind[deal_count] = D_ENTRY
                    deal_dir[deal_count] = direction
                    deal_lots[deal_count] = pend_lots[i]
                    deal_price[deal_count] = fill
                    deal_pnl[deal_count] = 0.0
                    deal_count += 1
                filled_session_set[pend_session[i]] = True
                pend_active[i] = False

        # (OCO removed; both directions allowed to fire same session.)

        # Break-even trigger: if price moved be_trigger_r × orig_sl_dist favorably,
        # move SL to entry + be_buffer_pts*direction (in entry direction).
        if be_trigger_r > 0:
            for i in range(MAX_POSITIONS):
                if not pos_active[i] or pos_be_done[i]:
                    continue
                trig_dist = be_trigger_r * pos_orig_sl_dist[i]
                if pos_dir[i] == 1:
                    if bid >= pos_entry[i] + trig_dist:
                        pos_sl[i] = _norm_price(pos_entry[i] + be_buffer_pts * point,
                                                tick_size, digits)
                        pos_be_done[i] = True
                else:
                    if ask <= pos_entry[i] - trig_dist:
                        pos_sl[i] = _norm_price(pos_entry[i] - be_buffer_pts * point,
                                                tick_size, digits)
                        pos_be_done[i] = True

        # SL/TP on EXISTING positions
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

        # Add new positions (capture orig SL dist for BE trigger calc)
        for j in range(new_pos_count):
            for slot in range(MAX_POSITIONS):
                if not pos_active[slot]:
                    pos_dir[slot] = new_pos_dir[j]
                    pos_entry[slot] = new_pos_entry[j]
                    pos_sl[slot] = new_pos_sl[j]
                    pos_tp[slot] = new_pos_tp[j]
                    pos_lots[slot] = new_pos_lots[j]
                    pos_session[slot] = new_pos_session[j]
                    pos_active[slot] = True
                    pos_be_done[slot] = False
                    if new_pos_dir[j] == 1:
                        pos_orig_sl_dist[slot] = new_pos_entry[j] - new_pos_sl[j]
                    else:
                        pos_orig_sl_dist[slot] = new_pos_sl[j] - new_pos_entry[j]
                    break

    return deal_count, balance, dd_abs, balance_max


def _ts_to_ns(series: pd.Series) -> np.ndarray:
    if hasattr(series.dt, "tz") and series.dt.tz is not None:
        series = series.dt.tz_convert("UTC").dt.tz_localize(None)
    return series.values.astype("datetime64[ns]").astype(np.int64)


def _build_sessions_arrays(
    days_range: tuple[date, date], cfg: ORBConfig,
    m5_ts_ns: np.ndarray, m5_highs: np.ndarray, m5_lows: np.ndarray,
):
    """Pre-build session arrays + compute range_high/low from M5 bars."""
    sess = []
    d = days_range[0]
    while d <= days_range[1]:
        if d.weekday() >= 5:
            d += timedelta(days=1)
            continue

        if cfg.continuous_mode:
            firing = datetime.combine(d, time(cfg.cont_start_hour, 0, tzinfo=timezone.utc))
            day_end = datetime.combine(d, time(cfg.cont_end_hour, 0, tzinfo=timezone.utc))
            while firing <= day_end:
                rs = firing - timedelta(minutes=cfg.range_minutes)
                re_ = firing
                sess.append((rs, re_, re_ + timedelta(minutes=cfg.pending_expire_minutes)))
                firing += timedelta(minutes=cfg.refire_minutes)
        else:
            if cfg.ldn_enabled:
                rs = datetime.combine(d, time(cfg.ldn_start_hour, 0, tzinfo=timezone.utc))
                re_ = rs + timedelta(minutes=cfg.range_minutes)
                sess.append((rs, re_, re_ + timedelta(minutes=cfg.pending_expire_minutes)))
            if cfg.ny_enabled:
                rs = datetime.combine(d, time(cfg.ny_start_hour, 0, tzinfo=timezone.utc))
                re_ = rs + timedelta(minutes=cfg.range_minutes)
                sess.append((rs, re_, re_ + timedelta(minutes=cfg.pending_expire_minutes)))
        d += timedelta(days=1)

    # Sort by range_end then convert to int64 ns + compute ranges
    sess.sort(key=lambda x: x[1])
    n = len(sess)
    range_start_ns = np.zeros(n, dtype=np.int64)
    range_end_ns = np.zeros(n, dtype=np.int64)
    expire_ns = np.zeros(n, dtype=np.int64)
    range_high = np.zeros(n, dtype=np.float64)
    range_low = np.zeros(n, dtype=np.float64)
    for i, (rs, re_, ex) in enumerate(sess):
        range_start_ns[i] = pd.Timestamp(rs).tz_localize(None).value
        range_end_ns[i] = pd.Timestamp(re_).tz_localize(None).value
        expire_ns[i] = pd.Timestamp(ex).tz_localize(None).value
        # Find M5 bars whose start time is in [range_start, range_end)
        lo = np.searchsorted(m5_ts_ns, range_start_ns[i], side='left')
        hi = np.searchsorted(m5_ts_ns, range_end_ns[i], side='left')
        if hi > lo:
            range_high[i] = m5_highs[lo:hi].max()
            range_low[i] = m5_lows[lo:hi].min()
    return range_start_ns, range_end_ns, expire_ns, range_high, range_low


def simulate_fast(
    ticks: pd.DataFrame,
    m5_bars: pd.DataFrame,
    m1_bars: pd.DataFrame,         # unused
    cfg: ORBConfig,
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

    if len(tick_ts_ns) == 0:
        first_day = last_day = date.today()
    else:
        first_day = pd.Timestamp(int(tick_ts_ns[0])).date()
        last_day = pd.Timestamp(int(tick_ts_ns[-1])).date()

    rs_ns, re_ns, ex_ns, rh_arr, rl_arr = _build_sessions_arrays(
        (first_day, last_day), cfg, m5_ts_ns, m5_highs, m5_lows,
    )

    deal_ts = np.zeros(MAX_DEALS, dtype=np.int64)
    deal_kind = np.zeros(MAX_DEALS, dtype=np.int8)
    deal_dir = np.zeros(MAX_DEALS, dtype=np.int8)
    deal_lots = np.zeros(MAX_DEALS, dtype=np.float64)
    deal_price = np.zeros(MAX_DEALS, dtype=np.float64)
    deal_pnl = np.zeros(MAX_DEALS, dtype=np.float64)

    deal_count, final_balance, dd_abs, balance_max = _run_sim(
        tick_ts_ns, tick_bid, tick_ask, tick_day_idx,
        re_ns, ex_ns, rh_arr, rl_arr,
        int(cfg.range_minutes),
        int(cfg.buffer_pts), int(cfg.min_range_pts), int(cfg.max_range_pts),
        int(cfg.fixed_sl_pts),
        float(cfg.rr_ratio), float(cfg.half_tp_ratio),
        float(cfg.daily_target_pct), float(cfg.daily_loss_pct),
        float(cfg.risk_pct), float(initial_balance),
        float(meta.point), float(meta.tick_size), float(meta.tick_value),
        int(meta.stops_level_pts), float(meta.volume_min),
        float(meta.volume_max), float(meta.volume_step), int(meta.digits),
        float(cfg.be_trigger_r), int(cfg.be_buffer_pts),
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
