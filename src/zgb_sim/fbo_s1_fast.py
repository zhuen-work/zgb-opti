"""Numba-compiled version of FBO Stream 1 simulator.

Same semantics as fbo_s1.simulate(). Targets ~25-30x speedup matching the
scalper's Numba port.

Validation: must match smoke test reference (NP=+$2,150 / 36 trades / 14 TP /
22 SL / 11.6% DD over Mar 14 -> Apr 25 with reference setfile params).
"""
from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd
from numba import njit

from .fbo_s1 import FBOS1Config
from .scalper_v1 import SymbolMeta, SimResult, Deal


# Order kind codes (FBO Stream 1 only uses stop orders, no limits)
K_BUY_STOP = np.int8(0)
K_SELL_STOP = np.int8(1)

# Deal kind codes
D_ENTRY = np.int8(0)
D_TP = np.int8(1)
D_SL = np.int8(2)
D_OTHER = np.int8(3)

# Preallocated slot counts
MAX_PENDING = 4       # FBO places at most 2 stops per signal (half-TP split)
MAX_POSITIONS = 4
MAX_DEALS = 50_000    # 6 weeks × ~few hundred deals max


@njit(cache=True, fastmath=False)
def _norm_price(price: float, tick_size: float, digits: int) -> float:
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
    # Bars
    m1_ts,           # int64[M1]
    m30_ts,          # int64[M30]
    frac_high,       # float64[M30]
    frac_low,        # float64[M30]
    sma,             # float64[M30]  (NaN where < period-1)
    # Strategy params
    fractal_bars,
    sma_period,
    tp_pts,
    sl_pts,
    htp_ratio,
    peb_bars,
    signal_tf_minutes,
    risk_pct,
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
    """JIT-compiled FBO Stream 1 sim loop.
    Returns (deal_count, final_balance, max_drawdown_abs, balance_max).
    """
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

    deal_count = 0
    m1_idx = 0
    n_ticks = tick_ts.shape[0]
    n_m1 = m1_ts.shape[0]
    n_m30 = m30_ts.shape[0]

    stops_pad = stops_level_pts * point
    ns_signal_tf = np.int64(signal_tf_minutes * 60 * 1_000_000_000)

    for k in range(n_ticks):
        ts_ns = tick_ts[k]
        bid = tick_bid[k]
        ask = tick_ask[k]

        # Expire pending (strict <=)
        for i in range(MAX_PENDING):
            if pend_active[i] and ts_ns >= pend_expire[i]:
                pend_active[i] = False

        # Pending triggers
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

        # Check SL/TP on EXISTING positions (not new fills)
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

        # Add new positions from this tick's fills
        for j in range(new_pos_count):
            _add_position(pos_dir, pos_entry, pos_sl, pos_tp, pos_lots, pos_active,
                          new_pos_dir[j], new_pos_entry[j], new_pos_sl[j],
                          new_pos_tp[j], new_pos_lots[j])

        # Process new M1 bars
        while m1_idx < n_m1 and m1_ts[m1_idx] <= ts_ns:
            bar_ts_ns = m1_ts[m1_idx]
            m1_idx += 1

            # Skip if any pending OR any position (FBO idle-stream rule)
            any_pending = False
            for i in range(MAX_PENDING):
                if pend_active[i]:
                    any_pending = True
                    break
            if any_pending:
                continue
            any_position = False
            for i in range(MAX_POSITIONS):
                if pos_active[i]:
                    any_position = True
                    break
            if any_position:
                continue

            # Find forming M30 bar (bar containing bar_ts_ns) via binary search
            lo = 0
            hi = n_m30
            while lo < hi:
                mid = (lo + hi) // 2
                if m30_ts[mid] <= bar_ts_ns:
                    lo = mid + 1
                else:
                    hi = mid
            forming_m30 = lo - 1
            last_completed = forming_m30 - 1
            if last_completed < sma_period - 1:
                continue

            sma_val = sma[last_completed]
            if np.isnan(sma_val) or sma_val <= 0:
                continue

            want_buy = bid > sma_val
            want_sell = bid < sma_val
            if not (want_buy or want_sell):
                continue

            # Pending expiry = forming signal-TF bar open + N × signal_tf_minutes
            forming_open = m30_ts[forming_m30] if forming_m30 >= 0 else bar_ts_ns
            expire_ns = forming_open + np.int64(peb_bars) * ns_signal_tf

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

            if want_buy:
                fh = frac_high[last_completed]
                if fh > 0:
                    entry = _norm_price(fh, tick_size, digits)
                    min_e = _norm_price(ask + stops_pad, tick_size, digits)
                    if entry < min_e:
                        entry = min_e
                    if entry > ask:
                        sl = _norm_price(entry - sl_pts * point, tick_size, digits)
                        tp_full = _norm_price(entry + tp_pts * point, tick_size, digits)
                        if htp_ratio > 0:
                            tp_half = _norm_price(
                                entry + tp_pts * htp_ratio * point, tick_size, digits
                            )
                            _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                         pend_lots, pend_expire, pend_active,
                                         K_BUY_STOP, entry, sl, tp_half, half_lots, expire_ns)
                            _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                         pend_lots, pend_expire, pend_active,
                                         K_BUY_STOP, entry, sl, tp_full, half_lots, expire_ns)
                        else:
                            _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                         pend_lots, pend_expire, pend_active,
                                         K_BUY_STOP, entry, sl, tp_full, total_lots, expire_ns)

            if want_sell:
                fl = frac_low[last_completed]
                if fl > 0:
                    entry = _norm_price(fl, tick_size, digits)
                    max_e = _norm_price(bid - stops_pad, tick_size, digits)
                    if entry > max_e:
                        entry = max_e
                    if entry < bid:
                        sl = _norm_price(entry + sl_pts * point, tick_size, digits)
                        tp_full = _norm_price(entry - tp_pts * point, tick_size, digits)
                        if htp_ratio > 0:
                            tp_half = _norm_price(
                                entry - tp_pts * htp_ratio * point, tick_size, digits
                            )
                            _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                         pend_lots, pend_expire, pend_active,
                                         K_SELL_STOP, entry, sl, tp_half, half_lots, expire_ns)
                            _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                         pend_lots, pend_expire, pend_active,
                                         K_SELL_STOP, entry, sl, tp_full, half_lots, expire_ns)
                        else:
                            _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                         pend_lots, pend_expire, pend_active,
                                         K_SELL_STOP, entry, sl, tp_full, total_lots, expire_ns)

    return deal_count, balance, dd_abs, balance_max


# ----- Python wrapper -----

def _ts_to_ns(series: pd.Series) -> np.ndarray:
    if hasattr(series.dt, "tz") and series.dt.tz is not None:
        series = series.dt.tz_convert("UTC").dt.tz_localize(None)
    return series.values.astype("datetime64[ns]").astype(np.int64)


def _compute_fractal_levels(
    highs: np.ndarray, lows: np.ndarray, period: int
) -> tuple[np.ndarray, np.ndarray]:
    n = len(highs)
    is_fh = np.zeros(n, dtype=bool)
    is_fl = np.zeros(n, dtype=bool)
    for j in range(period, n - period):
        h = highs[j]
        if h > highs[j-period:j].max() and h > highs[j+1:j+period+1].max():
            is_fh[j] = True
        l = lows[j]
        if l < lows[j-period:j].min() and l < lows[j+1:j+period+1].min():
            is_fl[j] = True

    frac_h = np.zeros(n, dtype=np.float64)
    frac_l = np.zeros(n, dtype=np.float64)
    last_fh = 0.0
    last_fl = 0.0
    for i in range(n):
        confirm_idx = i - period
        if confirm_idx >= 0:
            if is_fh[confirm_idx]:
                last_fh = highs[confirm_idx]
            if is_fl[confirm_idx]:
                last_fl = lows[confirm_idx]
        frac_h[i] = last_fh
        frac_l[i] = last_fl
    return frac_h, frac_l


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
    m30_bars: pd.DataFrame,
    m1_bars: pd.DataFrame,
    cfg: FBOS1Config,
    meta: SymbolMeta,
    initial_balance: float = 10_000.0,
) -> SimResult:
    """Numba-accelerated drop-in for fbo_s1.simulate()."""
    tick_ts_ns = _ts_to_ns(ticks["ts"])
    tick_bid = ticks["bid"].values.astype(np.float64)
    tick_ask = ticks["ask"].values.astype(np.float64)

    m1_ts_ns = _ts_to_ns(m1_bars["ts"])
    m30_ts_ns = _ts_to_ns(m30_bars["ts"])
    m30_highs = m30_bars["high"].values.astype(np.float64)
    m30_lows = m30_bars["low"].values.astype(np.float64)
    m30_closes = m30_bars["close"].values.astype(np.float64)

    frac_h, frac_l = _compute_fractal_levels(m30_highs, m30_lows, cfg.fractal_bars)
    sma = _compute_sma(m30_closes, cfg.sma_period)

    deal_ts = np.zeros(MAX_DEALS, dtype=np.int64)
    deal_kind = np.zeros(MAX_DEALS, dtype=np.int8)
    deal_dir = np.zeros(MAX_DEALS, dtype=np.int8)
    deal_lots = np.zeros(MAX_DEALS, dtype=np.float64)
    deal_price = np.zeros(MAX_DEALS, dtype=np.float64)
    deal_pnl = np.zeros(MAX_DEALS, dtype=np.float64)

    deal_count, final_balance, dd_abs, balance_max = _run_sim(
        tick_ts_ns, tick_bid, tick_ask,
        m1_ts_ns, m30_ts_ns, frac_h, frac_l, sma,
        int(cfg.fractal_bars),
        int(cfg.sma_period),
        int(cfg.take_profit_pts),
        int(cfg.stop_loss_pts),
        float(cfg.half_tp_ratio),
        int(cfg.pending_expire_bars),
        int(cfg.signal_tf_minutes),
        float(cfg.risk_pct),
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
