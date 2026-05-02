"""EMA Pullback Bounce simulator.

Strategy: trade pullbacks to a single EMA in the direction of the trend.
Different mechanic from FBO (level-based vs fractal-based) — same momentum archetype.

Per signal-TF bar close (default M15):
  Uptrend setup (BuyStop):
    Trend:      close[1] > EMA[1]
    Pullback:   min(low[1..lookback]) <= EMA[1] + pullback_band_pts
                AND any low in window touched within band
    Trigger:    close[1] > open[1]   (bullish reversal candle)
                AND close[1] > EMA[1]
    Entry:      BuyStop at high[1] + entry_buffer_pts
    SL:         min(low[1..lookback]) - sl_buffer_pts
    TP:         entry + (entry - SL) * rr_ratio

  Downtrend mirror.

Single-position-at-a-time per stream. Daily caps standard.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import List, Optional

import numpy as np
import pandas as pd

from .scalper_v1 import (
    Deal, Pending, Position, SimResult, SymbolMeta,
    ORDER_BUY_STOP, ORDER_SELL_STOP,
    _norm_price, _calc_lots, _pnl,
)


@dataclass
class EMAPullbackConfig:
    risk_pct: float = 3.0
    signal_tf_minutes: int = 15
    ema_period: int = 50
    lookback_bars: int = 5            # how far back to look for pullback touch + SL low
    pullback_band_pts: int = 50       # how close to EMA the pullback wick must come
    entry_buffer_pts: int = 0         # offset of BuyStop above prev bar high
    sl_buffer_pts: int = 30           # extra distance below pullback low for SL
    rr_ratio: float = 2.0
    half_tp_ratio: float = 0.0
    pending_expire_bars: int = 3
    daily_target_pct: float = 0.0     # 0 disables
    daily_loss_pct: float = 0.0       # 0 disables
    comment: str = "EMAPullback"


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


def simulate(
    ticks: pd.DataFrame,
    signal_bars: pd.DataFrame,        # M15 (or whatever signal_tf_minutes)
    m1_bars: pd.DataFrame,             # unused; kept for sim-signature compat
    cfg: EMAPullbackConfig,
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

    ema = _compute_ema(s_close, cfg.ema_period)
    bar_period_ns = int(cfg.signal_tf_minutes) * 60 * 1_000_000_000

    # Per-bar signal generation
    @dataclass
    class Signal:
        kind: int           # +1 long, -1 short
        bar_close_ns: int   # ns timestamp of the just-closed signal bar
        entry: float
        sl: float
        tp: float
        expire_ns: int

    signals: list[Signal] = []
    diag_setups = 0

    look = cfg.lookback_bars
    for i in range(max(cfg.ema_period + 1, look + 2), n_bars):
        # bar [1] = i-1; lookback window low/high over indices [i-look .. i-1]
        e1 = ema[i - 1]
        if np.isnan(e1):
            continue
        c1 = s_close[i - 1]
        o1 = s_open[i - 1]
        h1 = s_high[i - 1]
        l1 = s_low[i - 1]
        win_lo = float(np.min(s_low[i - look:i]))
        win_hi = float(np.max(s_high[i - look:i]))
        bar_close_ns = int(s_ts[i - 1]) + bar_period_ns  # close time of bar [1]
        expire_ns = bar_close_ns + cfg.pending_expire_bars * bar_period_ns

        # ===== LONG =====
        # Trend: close[1] > EMA[1]
        # Pullback: min(low[1..look]) <= EMA[1] + band
        # Trigger: close[1] > open[1]
        if c1 > e1 and c1 > o1:
            if win_lo <= e1 + cfg.pullback_band_pts * meta.point:
                entry = h1 + cfg.entry_buffer_pts * meta.point
                sl = win_lo - cfg.sl_buffer_pts * meta.point
                if sl < entry:
                    sl_dist = entry - sl
                    tp = entry + sl_dist * cfg.rr_ratio
                    signals.append(Signal(+1, bar_close_ns, _norm_price(entry, meta),
                                           _norm_price(sl, meta), _norm_price(tp, meta),
                                           expire_ns))
                    diag_setups += 1

        # ===== SHORT =====
        if c1 < e1 and c1 < o1:
            if win_hi >= e1 - cfg.pullback_band_pts * meta.point:
                entry = l1 - cfg.entry_buffer_pts * meta.point
                sl = win_hi + cfg.sl_buffer_pts * meta.point
                if sl > entry:
                    sl_dist = sl - entry
                    tp = entry - sl_dist * cfg.rr_ratio
                    signals.append(Signal(-1, bar_close_ns, _norm_price(entry, meta),
                                           _norm_price(sl, meta), _norm_price(tp, meta),
                                           expire_ns))
                    diag_setups += 1

    signals.sort(key=lambda s: s.bar_close_ns)

    # Tick-driven simulation
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

        # Daily rollover
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

            # Single-position-at-a-time: skip if already pending or open for this stream
            if pending or positions:
                continue

            sl_dist_pts = abs(sig.entry - sig.sl) / meta.point
            total_lots = _calc_lots(balance, cfg.risk_pct, sl_dist_pts, meta)
            if total_lots <= 0:
                continue
            half_lots = total_lots
            if cfg.half_tp_ratio > 0:
                half_lots = round(total_lots / 2.0 / meta.volume_step) * meta.volume_step
                if half_lots < meta.volume_min:
                    half_lots = meta.volume_min
                half_lots = round(half_lots, 2)

            stops_pad = meta.stops_level_pts * meta.point
            expire_ts = pd.Timestamp(sig.expire_ns).tz_localize(None)

            if sig.kind == +1:
                # BuyStop above prev high
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

    print(f"  [diag] setups={diag_setups}  placed={diag_placed}  filled={diag_filled}  "
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
