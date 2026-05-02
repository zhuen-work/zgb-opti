"""Numba-compiled EMA pullback simulator. Same semantics as ema_pullback.simulate(), ~25-30x faster.

Pre-builds signal arrays (entry/sl/tp/bar_close_ns/expire_ns) in Python from
the signal-TF bars + EMA, then JIT loop processes ticks against them.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from numba import njit

from .ema_pullback import EMAPullbackConfig
from .scalper_v1 import SymbolMeta, SimResult, Deal


K_BUY_STOP = np.int8(0)
K_SELL_STOP = np.int8(1)

D_ENTRY = np.int8(0)
D_TP = np.int8(1)
D_SL = np.int8(2)
D_OTHER = np.int8(3)

# Single-position-at-a-time per stream → small caps suffice.
MAX_PENDING = 8        # supports HTP split (2 pendings/signal) with margin
MAX_POSITIONS = 4
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
                 pend_expire, pend_active,
                 kind_val, price, sl, tp, lots, expire_ns):
    for i in range(pend_active.shape[0]):
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
def _any_active(arr):
    for i in range(arr.shape[0]):
        if arr[i]:
            return True
    return False


@njit(cache=True, fastmath=False)
def _run_sim(
    tick_ts, tick_bid, tick_ask, tick_day_idx,
    sig_bar_close_ns, sig_kind, sig_entry, sig_sl, sig_tp, sig_expire_ns,
    rr_ratio, htp_ratio,
    daily_target_pct, daily_loss_pct,
    risk_pct, initial_balance,
    point, tick_size, tick_value, stops_level_pts,
    volume_min, volume_max, volume_step, digits,
    deal_ts, deal_kind, deal_dir, deal_lots, deal_price, deal_pnl,
):
    """JIT EMA-pullback sim. sig_* arrays pre-built; iterate ticks."""
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

    n_sig = sig_bar_close_ns.shape[0]
    sig_consumed = np.zeros(n_sig, dtype=np.bool_)

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
                for i in range(MAX_PENDING):
                    pend_active[i] = False
                daily_lock = True

        if daily_lock:
            continue

        # Place pending from any signals whose bar has just closed
        # Single-position-at-a-time: skip if any pending or any position active
        while next_sig < n_sig and sig_bar_close_ns[next_sig] <= ts_ns:
            sid = next_sig
            next_sig += 1
            if sig_consumed[sid]:
                continue
            sig_consumed[sid] = True

            if _any_active(pend_active) or _any_active(pos_active):
                continue

            entry = sig_entry[sid]
            sl = sig_sl[sid]
            tp = sig_tp[sid]
            kind = sig_kind[sid]   # +1 long, -1 short
            expire_ns = sig_expire_ns[sid]

            sl_dist_pts_f = abs(entry - sl) / point
            sl_dist_pts = int(sl_dist_pts_f) if sl_dist_pts_f > 0 else 0
            if sl_dist_pts <= 0:
                continue

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

            if kind == 1:
                # BuyStop above prev bar high; needs entry > ask + stops_pad
                if entry > ask + stops_pad:
                    if htp_ratio > 0:
                        sl_dist = entry - sl
                        tp_half = _norm_price(entry + sl_dist * rr_ratio * htp_ratio,
                                              tick_size, digits)
                        _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                     pend_lots, pend_expire, pend_active,
                                     K_BUY_STOP, entry, sl, tp_half, half_lots, expire_ns)
                        _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                     pend_lots, pend_expire, pend_active,
                                     K_BUY_STOP, entry, sl, tp, half_lots, expire_ns)
                    else:
                        _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                     pend_lots, pend_expire, pend_active,
                                     K_BUY_STOP, entry, sl, tp, total_lots, expire_ns)
            else:
                # SellStop below prev bar low; needs entry < bid - stops_pad
                if entry < bid - stops_pad:
                    if htp_ratio > 0:
                        sl_dist = sl - entry
                        tp_half = _norm_price(entry - sl_dist * rr_ratio * htp_ratio,
                                              tick_size, digits)
                        _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                     pend_lots, pend_expire, pend_active,
                                     K_SELL_STOP, entry, sl, tp_half, half_lots, expire_ns)
                        _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                     pend_lots, pend_expire, pend_active,
                                     K_SELL_STOP, entry, sl, tp, half_lots, expire_ns)
                    else:
                        _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                     pend_lots, pend_expire, pend_active,
                                     K_SELL_STOP, entry, sl, tp, total_lots, expire_ns)

        # Expire pending past expire_ns
        for i in range(MAX_PENDING):
            if pend_active[i] and ts_ns >= pend_expire[i]:
                pend_active[i] = False

        # Pending fills
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
                triggered = True; direction = 1; fill = p_val
            elif k_val == K_SELL_STOP and bid <= p_val:
                triggered = True; direction = -1; fill = p_val
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

        # SL/TP on existing positions
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

        # Add new positions
        for j in range(new_pos_count):
            for slot in range(MAX_POSITIONS):
                if not pos_active[slot]:
                    pos_dir[slot] = new_pos_dir[j]
                    pos_entry[slot] = new_pos_entry[j]
                    pos_sl[slot] = new_pos_sl[j]
                    pos_tp[slot] = new_pos_tp[j]
                    pos_lots[slot] = new_pos_lots[j]
                    pos_active[slot] = True
                    break

    return deal_count, balance, dd_abs, balance_max


def _ts_to_ns(series: pd.Series) -> np.ndarray:
    if hasattr(series.dt, "tz") and series.dt.tz is not None:
        series = series.dt.tz_convert("UTC").dt.tz_localize(None)
    return series.values.astype("datetime64[ns]").astype(np.int64)


def _compute_ema(closes: np.ndarray, period: int) -> np.ndarray:
    n = len(closes)
    ema = np.full(n, np.nan)
    if n < period or period <= 0:
        return ema
    alpha = 2.0 / (period + 1)
    ema[period - 1] = closes[:period].mean()
    for i in range(period, n):
        ema[i] = alpha * closes[i] + (1 - alpha) * ema[i - 1]
    return ema


def _build_signals(
    s_ts_ns: np.ndarray, s_open: np.ndarray, s_high: np.ndarray,
    s_low: np.ndarray, s_close: np.ndarray, ema: np.ndarray,
    cfg: EMAPullbackConfig, point: float, tick_size: float, digits: int,
):
    """Pre-build EMA-pullback signal arrays in Python (vectorizable later)."""
    n = len(s_ts_ns)
    bar_period_ns = int(cfg.signal_tf_minutes) * 60 * 1_000_000_000
    look = cfg.lookback_bars
    band_pts = cfg.pullback_band_pts * point
    sl_buf = cfg.sl_buffer_pts * point
    entry_buf = cfg.entry_buffer_pts * point

    bar_close_list = []
    kind_list = []
    entry_list = []
    sl_list = []
    tp_list = []
    expire_list = []

    start_i = max(cfg.ema_period + 1, look + 2)
    for i in range(start_i, n):
        e1 = ema[i - 1]
        if not np.isfinite(e1):
            continue
        c1 = s_close[i - 1]
        o1 = s_open[i - 1]
        h1 = s_high[i - 1]
        l1 = s_low[i - 1]
        win_lo = float(np.min(s_low[i - look:i]))
        win_hi = float(np.max(s_high[i - look:i]))
        bar_close_ns = int(s_ts_ns[i - 1]) + bar_period_ns
        expire_ns = bar_close_ns + cfg.pending_expire_bars * bar_period_ns

        # LONG
        if c1 > e1 and c1 > o1 and win_lo <= e1 + band_pts:
            entry = h1 + entry_buf
            sl = win_lo - sl_buf
            if sl < entry:
                sl_dist = entry - sl
                tp = entry + sl_dist * cfg.rr_ratio
                bar_close_list.append(bar_close_ns)
                kind_list.append(1)
                entry_list.append(_norm_price_py(entry, tick_size, digits))
                sl_list.append(_norm_price_py(sl, tick_size, digits))
                tp_list.append(_norm_price_py(tp, tick_size, digits))
                expire_list.append(expire_ns)
        # SHORT
        if c1 < e1 and c1 < o1 and win_hi >= e1 - band_pts:
            entry = l1 - entry_buf
            sl = win_hi + sl_buf
            if sl > entry:
                sl_dist = sl - entry
                tp = entry - sl_dist * cfg.rr_ratio
                bar_close_list.append(bar_close_ns)
                kind_list.append(-1)
                entry_list.append(_norm_price_py(entry, tick_size, digits))
                sl_list.append(_norm_price_py(sl, tick_size, digits))
                tp_list.append(_norm_price_py(tp, tick_size, digits))
                expire_list.append(expire_ns)

    if not bar_close_list:
        empty_i64 = np.zeros(0, dtype=np.int64)
        empty_i8 = np.zeros(0, dtype=np.int8)
        empty_f = np.zeros(0, dtype=np.float64)
        return empty_i64, empty_i8, empty_f, empty_f, empty_f, empty_i64

    # Sort by bar_close_ns (already ordered since iterating sequentially)
    bar_close_arr = np.array(bar_close_list, dtype=np.int64)
    order = np.argsort(bar_close_arr, kind='stable')
    return (
        bar_close_arr[order],
        np.array(kind_list, dtype=np.int8)[order],
        np.array(entry_list, dtype=np.float64)[order],
        np.array(sl_list, dtype=np.float64)[order],
        np.array(tp_list, dtype=np.float64)[order],
        np.array(expire_list, dtype=np.int64)[order],
    )


def _norm_price_py(price, tick_size, digits):
    return round(round(price / tick_size) * tick_size, digits)


def simulate_fast(
    ticks: pd.DataFrame,
    signal_bars: pd.DataFrame,
    m1_bars: pd.DataFrame,         # unused; kept for sim-signature compat
    cfg: EMAPullbackConfig,
    meta: SymbolMeta,
    initial_balance: float = 10_000.0,
) -> SimResult:
    tick_ts_ns = _ts_to_ns(ticks["ts"])
    tick_bid = ticks["bid"].values.astype(np.float64)
    tick_ask = ticks["ask"].values.astype(np.float64)
    tick_day_idx = (tick_ts_ns // 1_000_000_000 // 86400).astype(np.int64)

    s_ts_ns = _ts_to_ns(signal_bars["ts"])
    s_open = signal_bars["open"].values.astype(np.float64)
    s_high = signal_bars["high"].values.astype(np.float64)
    s_low = signal_bars["low"].values.astype(np.float64)
    s_close = signal_bars["close"].values.astype(np.float64)

    ema = _compute_ema(s_close, cfg.ema_period)

    sig_bar_close, sig_kind, sig_entry, sig_sl, sig_tp, sig_expire = _build_signals(
        s_ts_ns, s_open, s_high, s_low, s_close, ema, cfg,
        meta.point, meta.tick_size, meta.digits,
    )

    deal_ts = np.zeros(MAX_DEALS, dtype=np.int64)
    deal_kind = np.zeros(MAX_DEALS, dtype=np.int8)
    deal_dir = np.zeros(MAX_DEALS, dtype=np.int8)
    deal_lots = np.zeros(MAX_DEALS, dtype=np.float64)
    deal_price = np.zeros(MAX_DEALS, dtype=np.float64)
    deal_pnl = np.zeros(MAX_DEALS, dtype=np.float64)

    deal_count, final_balance, dd_abs, balance_max = _run_sim(
        tick_ts_ns, tick_bid, tick_ask, tick_day_idx,
        sig_bar_close, sig_kind, sig_entry, sig_sl, sig_tp, sig_expire,
        float(cfg.rr_ratio), float(cfg.half_tp_ratio),
        float(cfg.daily_target_pct), float(cfg.daily_loss_pct),
        float(cfg.risk_pct), float(initial_balance),
        meta.point, meta.tick_size, meta.tick_value, meta.stops_level_pts,
        meta.volume_min, meta.volume_max, meta.volume_step, meta.digits,
        deal_ts, deal_kind, deal_dir, deal_lots, deal_price, deal_pnl,
    )

    deals = []
    tp_count = 0
    sl_count = 0
    other_count = 0
    for i in range(deal_count):
        kind_val = deal_kind[i]
        if kind_val == D_ENTRY:
            kind_str = "entry"
        elif kind_val == D_TP:
            kind_str = "tp"; tp_count += 1
        elif kind_val == D_SL:
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
    wins = sum(d.pnl for d in deals if d.kind != "entry" and d.pnl > 0)
    losses_abs = abs(sum(d.pnl for d in deals if d.kind != "entry" and d.pnl < 0))
    pf = wins / losses_abs if losses_abs > 0 else float("inf")
    net = final_balance - initial_balance
    dd_pct = (dd_abs / balance_max * 100.0) if balance_max > 0 else 0.0

    bc_rows = [{"ts": d.ts, "pnl": d.pnl} for d in deals if d.kind != "entry"]
    bc = pd.DataFrame(bc_rows)
    if not bc.empty:
        bc["balance"] = initial_balance + bc["pnl"].cumsum()

    print(f"  [diag-fast] signals={sig_bar_close.shape[0]}  filled={tp_count+sl_count+other_count}  "
          f"tp={tp_count}  sl={sl_count}  other={other_count}")

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
