"""HourMo (Hour-of-Day Momentum) simulator.

Strategy: trade continuation of a strong M15 bar, but ONLY at high-edge
trigger hours identified from 2026 XAUUSD study (default 02, 08, 16 UTC).

Per signal-TF bar close (default M15):
  - If bar's open hour (UTC) is in trigger_hours_utc AND |return| >= min_signal_pts:
      Bullish bar: place BuyStop at high[i] + entry_buffer_pts
        SL: low[i] - sl_buffer_pts
        TP: entry + (entry - SL) * rr_ratio
      Bearish bar: place SellStop at low[i] - entry_buffer_pts
        SL: high[i] + sl_buffer_pts
        TP: entry - (SL - entry) * rr_ratio

Single-position-at-a-time per stream. Pending TTL = pending_expire_bars.
Daily caps standard.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from .scalper_v1 import (
    Deal, Pending, Position, SimResult, SymbolMeta,
    ORDER_BUY_STOP, ORDER_SELL_STOP,
    _norm_price, _calc_lots, _pnl,
)


@dataclass
class HourMoConfig:
    risk_pct: float = 3.0
    signal_tf_minutes: int = 15
    trigger_hours_utc: Tuple[int, ...] = (2, 8, 16)
    min_signal_pts: int = 600         # filter weak signals (study p75 ~ 800; 600 = top ~30%)
    entry_buffer_pts: int = 0
    sl_buffer_pts: int = 100
    rr_ratio: float = 2.0
    half_tp_ratio: float = 0.0
    pending_expire_bars: int = 2      # 30 min pending lifetime
    daily_target_pct: float = 0.0
    daily_loss_pct: float = 6.0
    comment: str = "HourMo"


def simulate(
    ticks: pd.DataFrame,
    signal_bars: pd.DataFrame,        # M15 (or whatever signal_tf_minutes)
    m1_bars: pd.DataFrame,            # unused; kept for sim-signature compat
    cfg: HourMoConfig,
    meta: SymbolMeta,
    initial_balance: float = 10_000.0,
) -> SimResult:
    def _to_naive_ns(s: pd.Series) -> np.ndarray:
        if hasattr(s.dt, "tz") and s.dt.tz is not None:
            return s.dt.tz_convert("UTC").dt.tz_localize(None).values.astype("datetime64[ns]")
        return s.values.astype("datetime64[ns]")

    t_ts = _to_naive_ns(ticks["ts"]).astype(np.int64)
    t_bid = ticks["bid"].values.astype(np.float64)
    t_ask = ticks["ask"].values.astype(np.float64)

    s_ts = _to_naive_ns(signal_bars["ts"]).astype(np.int64)
    s_open = signal_bars["open"].values.astype(np.float64)
    s_high = signal_bars["high"].values.astype(np.float64)
    s_low = signal_bars["low"].values.astype(np.float64)
    s_close = signal_bars["close"].values.astype(np.float64)
    n_bars = len(s_ts)
    bar_period_ns = int(cfg.signal_tf_minutes) * 60 * 1_000_000_000
    point = meta.point
    trigger_set = set(cfg.trigger_hours_utc)

    # Pre-build signals
    @dataclass
    class Signal:
        kind: int
        bar_close_ns: int
        entry: float
        sl: float
        tp: float
        expire_ns: int

    signals: list[Signal] = []
    diag_setups = 0
    diag_strong = 0
    diag_at_trigger_hour = 0

    for i in range(n_bars):
        bar_ts = pd.Timestamp(int(s_ts[i]))
        if bar_ts.hour not in trigger_set:
            continue
        diag_at_trigger_hour += 1
        ret_pts = (s_close[i] - s_open[i]) / point
        if abs(ret_pts) < cfg.min_signal_pts:
            continue
        diag_strong += 1

        bar_close_ns = int(s_ts[i]) + bar_period_ns
        expire_ns = bar_close_ns + cfg.pending_expire_bars * bar_period_ns

        if ret_pts > 0:
            # Bullish trigger -> BuyStop above bar high
            entry = s_high[i] + cfg.entry_buffer_pts * point
            sl = s_low[i] - cfg.sl_buffer_pts * point
            sl_dist = entry - sl
            if sl_dist <= 0:
                continue
            tp = entry + sl_dist * cfg.rr_ratio
            signals.append(Signal(+1, bar_close_ns,
                                   _norm_price(entry, meta),
                                   _norm_price(sl, meta),
                                   _norm_price(tp, meta), expire_ns))
            diag_setups += 1
        else:
            # Bearish trigger -> SellStop below bar low
            entry = s_low[i] - cfg.entry_buffer_pts * point
            sl = s_high[i] + cfg.sl_buffer_pts * point
            sl_dist = sl - entry
            if sl_dist <= 0:
                continue
            tp = entry - sl_dist * cfg.rr_ratio
            signals.append(Signal(-1, bar_close_ns,
                                   _norm_price(entry, meta),
                                   _norm_price(sl, meta),
                                   _norm_price(tp, meta), expire_ns))
            diag_setups += 1

    signals.sort(key=lambda s: s.bar_close_ns)

    balance = initial_balance
    pending: List[Pending] = []
    positions: List[Position] = []
    deals: List[Deal] = []
    balance_max = initial_balance
    dd_abs = 0.0

    session_day: Optional[date] = None
    balance_day_start = initial_balance
    realized_today = 0.0
    daily_lock = False

    diag_placed = 0
    diag_filled = 0
    diag_expired = 0
    diag_target_hits = 0
    diag_loss_hits = 0

    next_sig_idx = 0

    def _update_dd():
        nonlocal balance_max, dd_abs
        if balance > balance_max:
            balance_max = balance
        cur = balance_max - balance
        if cur > dd_abs:
            dd_abs = cur

    for k in range(len(t_ts)):
        ts_ns = int(t_ts[k])
        ts = pd.Timestamp(ts_ns)
        bid = t_bid[k]
        ask = t_ask[k]

        day = ts.date()
        if day != session_day:
            session_day = day
            balance_day_start = balance
            realized_today = 0.0
            daily_lock = False

        if not daily_lock:
            unrealized = 0.0
            for p in positions:
                close_px = bid if p.direction == 1 else ask
                unrealized += _pnl(p, close_px, meta)
            today_pnl = realized_today + unrealized
            target_locked = False
            if cfg.daily_target_pct > 0 and today_pnl >= balance_day_start * cfg.daily_target_pct / 100.0:
                target_locked = True; diag_target_hits += 1
            elif cfg.daily_loss_pct > 0 and today_pnl <= -balance_day_start * cfg.daily_loss_pct / 100.0:
                target_locked = True; diag_loss_hits += 1
            if target_locked:
                for p in positions:
                    close_px = bid if p.direction == 1 else ask
                    pnl = _pnl(p, close_px, meta)
                    balance += pnl; realized_today += pnl
                    deals.append(Deal(ts, 'other', p.direction, p.lots, close_px, pnl))
                    _update_dd()
                positions = []
                pending = []
                daily_lock = True

        if daily_lock:
            continue

        # 1) Place pending from any signals whose bar has just closed
        while next_sig_idx < len(signals):
            sig = signals[next_sig_idx]
            if sig.bar_close_ns > ts_ns:
                break
            next_sig_idx += 1
            if pending or positions:
                continue

            sl_dist_pts = abs(sig.entry - sig.sl) / point
            total_lots = _calc_lots(balance, cfg.risk_pct, sl_dist_pts, meta)
            if total_lots <= 0:
                continue
            half_lots = total_lots
            if cfg.half_tp_ratio > 0:
                half_lots = round(total_lots / 2.0 / meta.volume_step) * meta.volume_step
                if half_lots < meta.volume_min:
                    half_lots = meta.volume_min
                half_lots = round(half_lots, 2)

            stops_pad = meta.stops_level_pts * point
            expire_ts = pd.Timestamp(sig.expire_ns).tz_localize(None)

            if sig.kind == +1:
                if sig.entry > ask + stops_pad:
                    if cfg.half_tp_ratio > 0:
                        sl_dist = sig.entry - sig.sl
                        tp_half = _norm_price(sig.entry + sl_dist * cfg.rr_ratio * cfg.half_tp_ratio, meta)
                        pending.append(Pending(ORDER_BUY_STOP, sig.entry, sig.sl, tp_half, half_lots, expire_ts, 0, ts))
                        pending.append(Pending(ORDER_BUY_STOP, sig.entry, sig.sl, sig.tp, half_lots, expire_ts, 0, ts))
                    else:
                        pending.append(Pending(ORDER_BUY_STOP, sig.entry, sig.sl, sig.tp, total_lots, expire_ts, 0, ts))
                    diag_placed += len(pending)
            else:
                if sig.entry < bid - stops_pad:
                    if cfg.half_tp_ratio > 0:
                        sl_dist = sig.sl - sig.entry
                        tp_half = _norm_price(sig.entry - sl_dist * cfg.rr_ratio * cfg.half_tp_ratio, meta)
                        pending.append(Pending(ORDER_SELL_STOP, sig.entry, sig.sl, tp_half, half_lots, expire_ts, 0, ts))
                        pending.append(Pending(ORDER_SELL_STOP, sig.entry, sig.sl, sig.tp, half_lots, expire_ts, 0, ts))
                    else:
                        pending.append(Pending(ORDER_SELL_STOP, sig.entry, sig.sl, sig.tp, total_lots, expire_ts, 0, ts))
                    diag_placed += len(pending)

        # 2) Expire pending
        if pending:
            still = []
            for p in pending:
                if ts < p.expire_ts:
                    still.append(p)
                else:
                    diag_expired += 1
            pending = still

        # 3) Pending fills
        new_positions = []
        still_pending = []
        for p in pending:
            triggered = False
            if p.kind == ORDER_BUY_STOP and ask >= p.price:
                triggered = True; fill = p.price; direction = 1
            elif p.kind == ORDER_SELL_STOP and bid <= p.price:
                triggered = True; fill = p.price; direction = -1
            if triggered:
                pos = Position(direction, fill, p.sl, p.tp, p.lots)
                new_positions.append(pos)
                deals.append(Deal(ts, 'entry', direction, p.lots, fill, 0.0))
                diag_filled += 1
            else:
                still_pending.append(p)
        pending = still_pending

        # 4) SL/TP on positions
        new_plist = []
        for pos in positions:
            hit_sl = hit_tp = False
            if pos.direction == 1:
                if bid <= pos.sl: hit_sl = True
                elif bid >= pos.tp: hit_tp = True
            else:
                if ask >= pos.sl: hit_sl = True
                elif ask <= pos.tp: hit_tp = True
            if hit_sl:
                pnl = _pnl(pos, pos.sl, meta)
                balance += pnl; realized_today += pnl
                deals.append(Deal(ts, 'sl', pos.direction, pos.lots, pos.sl, pnl)); _update_dd()
            elif hit_tp:
                pnl = _pnl(pos, pos.tp, meta)
                balance += pnl; realized_today += pnl
                deals.append(Deal(ts, 'tp', pos.direction, pos.lots, pos.tp, pnl)); _update_dd()
            else:
                new_plist.append(pos)
        positions = new_plist + new_positions

    print(f"  [diag] at_trigger_hour={diag_at_trigger_hour}  strong={diag_strong}  "
          f"setups={diag_setups}  placed={diag_placed}  filled={diag_filled}  "
          f"expired={diag_expired}  target_hits={diag_target_hits}  loss_hits={diag_loss_hits}")

    tp_count = sum(1 for d in deals if d.kind == 'tp')
    sl_count = sum(1 for d in deals if d.kind == 'sl')
    other_count = sum(1 for d in deals if d.kind == 'other')
    trades = tp_count + sl_count + other_count

    wins = [d.pnl for d in deals if d.kind != 'entry' and d.pnl > 0]
    losses = [d.pnl for d in deals if d.kind != 'entry' and d.pnl < 0]
    pf = sum(wins) / abs(sum(losses)) if losses else float('inf')
    net = balance - initial_balance
    dd_pct = (dd_abs / balance_max * 100.0) if balance_max > 0 else 0.0

    bc = pd.DataFrame([{"ts": d.ts, "pnl": d.pnl} for d in deals if d.kind != 'entry'])
    if not bc.empty:
        bc["balance"] = initial_balance + bc["pnl"].cumsum()

    return SimResult(
        initial_balance=initial_balance, final_balance=balance, net_profit=net,
        trades=trades, tp_count=tp_count, sl_count=sl_count, other_count=other_count,
        max_drawdown=dd_abs, max_drawdown_pct=dd_pct, profit_factor=pf,
        balance_curve=bc, deals=deals,
    )
