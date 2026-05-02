"""Numba-compiled LSFVG simulator. Same semantics as lsfvg.simulate(), ~25-30x faster.

Pre-computes signal-TF SMA in Python; JIT loop iterates ticks, advances signal-TF
bar index on tick timestamp crossing, detects setups on bar close.
"""
from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd
from numba import njit

from .lsfvg import LSFVGConfig
from .scalper_v1 import SymbolMeta, SimResult, Deal


K_BUY_LIMIT = np.int8(2)
K_SELL_LIMIT = np.int8(3)

D_ENTRY = np.int8(0)
D_TP = np.int8(1)
D_SL = np.int8(2)
D_OTHER = np.int8(3)

MAX_PENDING = 4       # 2 (half-TP split) per direction; one direction per bar
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
def _add_position(pos_dir, pos_entry, pos_sl, pos_tp, pos_lots, pos_active,
                  direction, entry, sl, tp, lots):
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
    tick_ts, tick_bid, tick_ask, tick_day_idx,
    sig_ts, sig_h, sig_l, sig_c, sma,
    lookback_bars, min_fvg_pts, max_fvg_pts,
    sweep_buffer_pts, rr_ratio, htp_ratio,
    pending_expire_bars, signal_tf_minutes,
    sweep_window_bars,
    use_ema_filter,
    daily_target_pct, daily_loss_pct,
    risk_pct, initial_balance,
    point, tick_size, tick_value, stops_level_pts,
    volume_min, volume_max, volume_step, digits,
    deal_ts, deal_kind, deal_dir, deal_lots, deal_price, deal_pnl,
):
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

    session_day = np.int64(-1)
    balance_day_start = initial_balance
    realized_today = 0.0
    daily_lock = False

    deal_count = 0
    n_ticks = tick_ts.shape[0]
    n_sig = sig_ts.shape[0]
    stops_pad = stops_level_pts * point
    sig_tf_ns = np.int64(signal_tf_minutes * 60 * 1_000_000_000)

    last_sig_idx = -1  # last signal-TF bar idx whose CLOSE we've already processed

    diag_setups = 0
    diag_filled = 0

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

        # Daily caps
        if not daily_lock and (daily_target_pct > 0 or daily_loss_pct > 0):
            unreal = 0.0
            for i in range(MAX_POSITIONS):
                if pos_active[i]:
                    close_px = bid if pos_dir[i] == 1 else ask
                    unreal += _pnl(pos_dir[i], pos_entry[i], close_px,
                                    pos_lots[i], tick_value, tick_size)
            today_pnl = realized_today + unreal
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

        # Expire pending
        for i in range(MAX_PENDING):
            if pend_active[i] and ts_ns >= pend_expire[i]:
                pend_active[i] = False

        # Pending fills (LIMIT semantics)
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
            if k_val == K_BUY_LIMIT and ask <= p_val:
                triggered = True; direction = 1; fill = p_val
            elif k_val == K_SELL_LIMIT and bid >= p_val:
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
                diag_filled += 1
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
            _add_position(pos_dir, pos_entry, pos_sl, pos_tp, pos_lots, pos_active,
                          new_pos_dir[j], new_pos_entry[j], new_pos_sl[j],
                          new_pos_tp[j], new_pos_lots[j])

        # Advance signal-TF bar idx & detect setup. A bar is "closed" once next bar's
        # open ts has been crossed by tick. Maintain last_sig_idx as last fully-closed.
        while last_sig_idx + 1 < n_sig and (
            (last_sig_idx + 2 < n_sig and ts_ns >= sig_ts[last_sig_idx + 2])
        ):
            last_sig_idx += 1

            # Stream busy?
            any_pending = False
            for i in range(MAX_PENDING):
                if pend_active[i]:
                    any_pending = True; break
            if any_pending:
                continue
            any_pos = False
            for i in range(MAX_POSITIONS):
                if pos_active[i]:
                    any_pos = True; break
            if any_pos:
                continue

            # Strict canonical pattern: sweep bar IS the FVG middle bar.
            # Bar 1 = displacement (sets one FVG edge), bar 2 = sweep wick + FVG middle,
            # bar 3 = pre-sweep (sets other FVG edge).
            if last_sig_idx < 3 + lookback_bars:
                continue

            i1 = last_sig_idx
            i2 = last_sig_idx - 1
            i3 = last_sig_idx - 2
            pool_lo = last_sig_idx - 2 - lookback_bars
            pool_hi = last_sig_idx - 2
            if pool_lo < 0:
                continue

            pool_high = sig_h[pool_lo]
            pool_low = sig_l[pool_lo]
            for j in range(pool_lo + 1, pool_hi):
                if sig_h[j] > pool_high:
                    pool_high = sig_h[j]
                if sig_l[j] < pool_low:
                    pool_low = sig_l[j]

            h1 = sig_h[i1]; l1 = sig_l[i1]; c1 = sig_c[i1]
            h2 = sig_h[i2]; l2 = sig_l[i2]
            h3 = sig_h[i3]; l3 = sig_l[i3]
            sweep_high = h1 if h1 > h2 else h2
            sweep_low = l1 if l1 < l2 else l2

            ema = sma[i1] if use_ema_filter else 0.0
            if use_ema_filter:
                if np.isnan(ema) or ema <= 0:
                    continue

            forming_open = sig_ts[i1] + sig_tf_ns
            expire_ns = forming_open + np.int64(pending_expire_bars) * sig_tf_ns

            # ===== BEARISH =====
            bearish = (sweep_high > pool_high) and (c1 < pool_high) and (l3 > h1)
            if use_ema_filter:
                bearish = bearish and (c1 < ema)
            if bearish:
                fvg_pts = (l3 - h1) / point
                if fvg_pts >= min_fvg_pts and fvg_pts <= max_fvg_pts:
                    entry = _norm_price(l3, tick_size, digits)
                    sl = _norm_price(sweep_high + sweep_buffer_pts * point,
                                     tick_size, digits)
                    sl_dist = sl - entry
                    if sl_dist > 0 and entry >= bid + stops_pad:
                        tp = _norm_price(entry - sl_dist * rr_ratio, tick_size, digits)
                        sl_pts = sl_dist / point
                        total_lots = _calc_lots(balance, risk_pct, sl_pts,
                                                point, tick_size, tick_value,
                                                volume_min, volume_max, volume_step)
                        if total_lots > 0:
                            half_lots = total_lots
                            if htp_ratio > 0:
                                half_lots = round(total_lots / 2.0 / volume_step) * volume_step
                                if half_lots < volume_min:
                                    half_lots = volume_min
                                half_lots = round(half_lots, 2)
                                tp_half = _norm_price(entry - sl_dist * rr_ratio * htp_ratio,
                                                      tick_size, digits)
                                _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                             pend_lots, pend_expire, pend_active,
                                             K_SELL_LIMIT, entry, sl, tp_half, half_lots, expire_ns)
                                _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                             pend_lots, pend_expire, pend_active,
                                             K_SELL_LIMIT, entry, sl, tp, half_lots, expire_ns)
                            else:
                                _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                             pend_lots, pend_expire, pend_active,
                                             K_SELL_LIMIT, entry, sl, tp, total_lots, expire_ns)
                            diag_setups += 1
                            continue

            # ===== BULLISH =====
            bullish = (sweep_low < pool_low) and (c1 > pool_low) and (h3 < l1)
            if use_ema_filter:
                bullish = bullish and (c1 > ema)
            if bullish:
                fvg_pts = (l1 - h3) / point
                if fvg_pts >= min_fvg_pts and fvg_pts <= max_fvg_pts:
                    entry = _norm_price(h3, tick_size, digits)
                    sl = _norm_price(sweep_low - sweep_buffer_pts * point,
                                     tick_size, digits)
                    sl_dist = entry - sl
                    if sl_dist > 0 and entry <= ask - stops_pad:
                        tp = _norm_price(entry + sl_dist * rr_ratio, tick_size, digits)
                        sl_pts = sl_dist / point
                        total_lots = _calc_lots(balance, risk_pct, sl_pts,
                                                point, tick_size, tick_value,
                                                volume_min, volume_max, volume_step)
                        if total_lots > 0:
                            half_lots = total_lots
                            if htp_ratio > 0:
                                half_lots = round(total_lots / 2.0 / volume_step) * volume_step
                                if half_lots < volume_min:
                                    half_lots = volume_min
                                half_lots = round(half_lots, 2)
                                tp_half = _norm_price(entry + sl_dist * rr_ratio * htp_ratio,
                                                      tick_size, digits)
                                _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                             pend_lots, pend_expire, pend_active,
                                             K_BUY_LIMIT, entry, sl, tp_half, half_lots, expire_ns)
                                _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                             pend_lots, pend_expire, pend_active,
                                             K_BUY_LIMIT, entry, sl, tp, half_lots, expire_ns)
                            else:
                                _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                             pend_lots, pend_expire, pend_active,
                                             K_BUY_LIMIT, entry, sl, tp, total_lots, expire_ns)
                            diag_setups += 1
            # NB: sweep_window_bars param is reserved for future use (currently unused).

    return deal_count, balance, dd_abs, balance_max, diag_setups, diag_filled


def _ts_to_ns(series: pd.Series) -> np.ndarray:
    if hasattr(series.dt, "tz") and series.dt.tz is not None:
        series = series.dt.tz_convert("UTC").dt.tz_localize(None)
    return series.values.astype("datetime64[ns]").astype(np.int64)


def _compute_sma(closes: np.ndarray, period: int) -> np.ndarray:
    n = len(closes)
    sma = np.full(n, np.nan)
    if n < period or period <= 0:
        return sma
    csum = np.cumsum(closes, dtype=np.float64)
    sma[period - 1] = csum[period - 1] / period
    for i in range(period, n):
        sma[i] = (csum[i] - csum[i - period]) / period
    return sma


def simulate_fast(
    ticks: pd.DataFrame,
    sig_bars: pd.DataFrame,
    m1_bars: pd.DataFrame,         # unused; kept for sig parity
    cfg: LSFVGConfig,
    meta: SymbolMeta,
    initial_balance: float = 10_000.0,
) -> SimResult:
    tick_ts_ns = _ts_to_ns(ticks["ts"])
    tick_bid = ticks["bid"].values.astype(np.float64)
    tick_ask = ticks["ask"].values.astype(np.float64)
    tick_day_idx = (tick_ts_ns // 1_000_000_000 // 86400).astype(np.int64)

    sig_ts_ns = _ts_to_ns(sig_bars["ts"])
    sig_h = sig_bars["high"].values.astype(np.float64)
    sig_l = sig_bars["low"].values.astype(np.float64)
    sig_c = sig_bars["close"].values.astype(np.float64)

    sma = _compute_sma(sig_c, cfg.ema_period) if cfg.use_ema_filter else np.zeros(len(sig_c))

    deal_ts = np.zeros(MAX_DEALS, dtype=np.int64)
    deal_kind = np.zeros(MAX_DEALS, dtype=np.int8)
    deal_dir = np.zeros(MAX_DEALS, dtype=np.int8)
    deal_lots = np.zeros(MAX_DEALS, dtype=np.float64)
    deal_price = np.zeros(MAX_DEALS, dtype=np.float64)
    deal_pnl = np.zeros(MAX_DEALS, dtype=np.float64)

    deal_count, final_balance, dd_abs, balance_max, n_setups, n_filled = _run_sim(
        tick_ts_ns, tick_bid, tick_ask, tick_day_idx,
        sig_ts_ns, sig_h, sig_l, sig_c, sma,
        int(cfg.lookback_bars), int(cfg.min_fvg_pts), int(cfg.max_fvg_pts),
        int(cfg.sweep_buffer_pts), float(cfg.rr_ratio), float(cfg.half_tp_ratio),
        int(cfg.pending_expire_bars), int(cfg.signal_tf_minutes),
        int(cfg.sweep_window_bars),
        bool(cfg.use_ema_filter),
        float(cfg.daily_target_pct), float(cfg.daily_loss_pct),
        float(cfg.risk_pct), float(initial_balance),
        float(meta.point), float(meta.tick_size), float(meta.tick_value),
        int(meta.stops_level_pts), float(meta.volume_min),
        float(meta.volume_max), float(meta.volume_step), int(meta.digits),
        deal_ts, deal_kind, deal_dir, deal_lots, deal_price, deal_pnl,
    )

    print(f"  [diag] sig_bars={len(sig_ts_ns):,}  setups={n_setups}  filled={n_filled}")

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
