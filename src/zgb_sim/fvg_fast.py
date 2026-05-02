"""Numba-compiled FVG simulator. Same semantics as fvg.simulate(), ~25-30x faster.

Validation: should match pure-Python fvg.py reference outputs.
"""
from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd
from numba import njit

from .fvg import FVGConfig
from .scalper_v1 import SymbolMeta, SimResult, Deal


K_BUY_LIMIT = np.int8(2)
K_SELL_LIMIT = np.int8(3)

D_ENTRY = np.int8(0)
D_TP = np.int8(1)
D_SL = np.int8(2)
D_OTHER = np.int8(3)

# FVG can place up to MaxZones × 2 pendings (HalfTP split). MaxZones in our
# grid is up to 5. Leave headroom: 16 pending slots.
MAX_PENDING = 16
MAX_POSITIONS = 8        # up to MaxZones positions if all fill
MAX_DEALS = 50_000


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
def _scan_zones(
    sig_highs, sig_lows, forming_idx, point,
    min_size_pts, max_age, max_zones,
    out_top, out_bottom, out_is_bull,
):
    """Fill pre-allocated arrays out_top[max_zones], out_bottom, out_is_bull
    with up to max_zones FVG zones. Returns number of zones found."""
    count = 0
    upper_i = max_age - 1
    if upper_i > forming_idx:
        upper_i = forming_idx
    for i in range(1, upper_i):
        if count >= max_zones:
            break
        idx_i = forming_idx - i
        idx_i2 = forming_idx - (i + 2)
        if idx_i2 < 0:
            break
        low_i = sig_lows[idx_i]
        high_i = sig_highs[idx_i]
        high_i2 = sig_highs[idx_i2]
        low_i2 = sig_lows[idx_i2]

        # Bullish FVG: low[i] > high[i+2]
        if low_i > high_i2:
            gap_pts = (low_i - high_i2) / point
            if gap_pts >= min_size_pts:
                out_top[count] = low_i
                out_bottom[count] = high_i2
                out_is_bull[count] = True
                count += 1
                continue

        # Bearish FVG: high[i] < low[i+2]
        if high_i < low_i2:
            gap_pts = (low_i2 - high_i) / point
            if gap_pts >= min_size_pts:
                out_top[count] = low_i2
                out_bottom[count] = high_i
                out_is_bull[count] = False
                count += 1
    return count


@njit(cache=True, fastmath=False)
def _run_sim(
    tick_ts, tick_bid, tick_ask,
    m1_ts,
    sig_ts, sig_highs, sig_lows,
    min_size_pts, max_age_bars, max_zones, rr_ratio, sl_buffer_pts,
    peb_bars, signal_tf_minutes, htp_ratio, risk_pct,
    initial_balance,
    point, tick_size, tick_value, stops_level_pts,
    volume_min, volume_max, volume_step, digits,
    deal_ts, deal_kind, deal_dir, deal_lots, deal_price, deal_pnl,
):
    """JIT FVG simulation. Returns (deal_count, final_balance, dd_abs, balance_max)."""
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

    # Reusable zone-scan output buffers
    zone_top = np.zeros(max_zones, dtype=np.float64)
    zone_bottom = np.zeros(max_zones, dtype=np.float64)
    zone_is_bull = np.zeros(max_zones, dtype=np.bool_)

    balance = initial_balance
    balance_max = initial_balance
    dd_abs = 0.0

    deal_count = 0
    m1_idx = 0
    n_ticks = tick_ts.shape[0]
    n_m1 = m1_ts.shape[0]
    n_sig = sig_ts.shape[0]

    stops_pad = stops_level_pts * point
    ns_signal_tf = np.int64(signal_tf_minutes * 60 * 1_000_000_000)

    for k in range(n_ticks):
        ts_ns = tick_ts[k]
        bid = tick_bid[k]
        ask = tick_ask[k]

        # Expire pending
        for i in range(MAX_PENDING):
            if pend_active[i] and ts_ns >= pend_expire[i]:
                pend_active[i] = False

        # Pending fills (limit orders)
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
                triggered = True
                direction = 1
                fill = p_val
            elif k_val == K_SELL_LIMIT and bid >= p_val:
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

            # Delete all pending (re-place semantics)
            for i in range(MAX_PENDING):
                pend_active[i] = False

            # Skip if any open position
            any_position = False
            for i in range(MAX_POSITIONS):
                if pos_active[i]:
                    any_position = True
                    break
            if any_position:
                continue

            # Locate forming signal-TF bar (binary search)
            lo = 0
            hi = n_sig
            while lo < hi:
                mid = (lo + hi) // 2
                if sig_ts[mid] <= bar_ts_ns:
                    lo = mid + 1
                else:
                    hi = mid
            forming = lo - 1
            if forming < 2:
                continue

            n_zones = _scan_zones(
                sig_highs, sig_lows, forming, point,
                min_size_pts, max_age_bars, max_zones,
                zone_top, zone_bottom, zone_is_bull,
            )
            if n_zones == 0:
                continue

            # Pending expiry = forming bar open + PEB × signal_tf
            forming_open = sig_ts[forming]
            expire_ns = forming_open + np.int64(peb_bars) * ns_signal_tf

            for z in range(n_zones):
                top = zone_top[z]
                bottom = zone_bottom[z]
                is_bull = zone_is_bull[z]
                if is_bull:
                    entry = _norm_price(top, tick_size, digits)
                    sl_px = _norm_price(bottom - sl_buffer_pts * point, tick_size, digits)
                    risk = entry - sl_px
                    tp = _norm_price(entry + risk * rr_ratio, tick_size, digits)
                    if entry >= ask:
                        continue
                    if (ask - entry) < stops_pad:
                        continue
                    risk_pts = int(risk / point)
                    total_lots = _calc_lots(balance, risk_pct, risk_pts,
                                            point, tick_size, tick_value,
                                            volume_min, volume_max, volume_step)
                    if total_lots <= 0:
                        continue
                    if htp_ratio > 0:
                        half_lots = round(total_lots / 2.0 / volume_step) * volume_step
                        if half_lots < volume_min:
                            half_lots = volume_min
                        half_lots = round(half_lots, 2)
                        tp_half = _norm_price(entry + risk * rr_ratio * htp_ratio, tick_size, digits)
                        _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                     pend_lots, pend_expire, pend_active,
                                     K_BUY_LIMIT, entry, sl_px, tp_half, half_lots, expire_ns)
                        _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                     pend_lots, pend_expire, pend_active,
                                     K_BUY_LIMIT, entry, sl_px, tp, half_lots, expire_ns)
                    else:
                        _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                     pend_lots, pend_expire, pend_active,
                                     K_BUY_LIMIT, entry, sl_px, tp, total_lots, expire_ns)
                else:
                    entry = _norm_price(bottom, tick_size, digits)
                    sl_px = _norm_price(top + sl_buffer_pts * point, tick_size, digits)
                    risk = sl_px - entry
                    tp = _norm_price(entry - risk * rr_ratio, tick_size, digits)
                    if entry <= bid:
                        continue
                    if (entry - bid) < stops_pad:
                        continue
                    risk_pts = int(risk / point)
                    total_lots = _calc_lots(balance, risk_pct, risk_pts,
                                            point, tick_size, tick_value,
                                            volume_min, volume_max, volume_step)
                    if total_lots <= 0:
                        continue
                    if htp_ratio > 0:
                        half_lots = round(total_lots / 2.0 / volume_step) * volume_step
                        if half_lots < volume_min:
                            half_lots = volume_min
                        half_lots = round(half_lots, 2)
                        tp_half = _norm_price(entry - risk * rr_ratio * htp_ratio, tick_size, digits)
                        _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                     pend_lots, pend_expire, pend_active,
                                     K_SELL_LIMIT, entry, sl_px, tp_half, half_lots, expire_ns)
                        _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                     pend_lots, pend_expire, pend_active,
                                     K_SELL_LIMIT, entry, sl_px, tp, half_lots, expire_ns)
                    else:
                        _add_pending(pend_kind, pend_price, pend_sl, pend_tp,
                                     pend_lots, pend_expire, pend_active,
                                     K_SELL_LIMIT, entry, sl_px, tp, total_lots, expire_ns)

    return deal_count, balance, dd_abs, balance_max


def _ts_to_ns(series: pd.Series) -> np.ndarray:
    if hasattr(series.dt, "tz") and series.dt.tz is not None:
        series = series.dt.tz_convert("UTC").dt.tz_localize(None)
    return series.values.astype("datetime64[ns]").astype(np.int64)


def simulate_fast(
    ticks: pd.DataFrame,
    signal_bars: pd.DataFrame,
    m1_bars: pd.DataFrame,
    cfg: FVGConfig,
    meta: SymbolMeta,
    initial_balance: float = 10_000.0,
) -> SimResult:
    """Numba-accelerated drop-in for fvg.simulate()."""
    tick_ts_ns = _ts_to_ns(ticks["ts"])
    tick_bid = ticks["bid"].values.astype(np.float64)
    tick_ask = ticks["ask"].values.astype(np.float64)

    m1_ts_ns = _ts_to_ns(m1_bars["ts"])
    sig_ts_ns = _ts_to_ns(signal_bars["ts"])
    sig_highs = signal_bars["high"].values.astype(np.float64)
    sig_lows = signal_bars["low"].values.astype(np.float64)

    deal_ts = np.zeros(MAX_DEALS, dtype=np.int64)
    deal_kind = np.zeros(MAX_DEALS, dtype=np.int8)
    deal_dir = np.zeros(MAX_DEALS, dtype=np.int8)
    deal_lots = np.zeros(MAX_DEALS, dtype=np.float64)
    deal_price = np.zeros(MAX_DEALS, dtype=np.float64)
    deal_pnl = np.zeros(MAX_DEALS, dtype=np.float64)

    deal_count, final_balance, dd_abs, balance_max = _run_sim(
        tick_ts_ns, tick_bid, tick_ask,
        m1_ts_ns,
        sig_ts_ns, sig_highs, sig_lows,
        int(cfg.min_size_pts),
        int(cfg.max_age_bars),
        int(cfg.max_zones),
        float(cfg.rr_ratio),
        int(cfg.sl_buffer_pts),
        int(cfg.pending_expire_bars),
        int(cfg.signal_tf_minutes),
        float(cfg.half_tp_ratio),
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
