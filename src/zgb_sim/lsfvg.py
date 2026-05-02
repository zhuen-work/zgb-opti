"""LSFVG (Liquidity Sweep + Fair Value Gap) simulator — port of LSFVG.mq5.

SMC-style stop-hunt + imbalance entry:
  bar idx:   0    1    2    3    4 ...  3+lookback
             |    |    |    |    |
            new  disp swp  ref  pool lookback range

  Bearish setup (SellLimit):
    pool_high = max(high[3..3+lookback])
    sweep:    max(high[1], high[2]) > pool_high AND close[1] < pool_high
    FVG:      low[3] > high[1]  AND  (low[3]-high[1]) within [min_fvg_pts, max_fvg_pts]
    entry:    SellLimit at low[3] (top of FVG)
    SL:       max(high[1], high[2]) + sweep_buffer_pts
    TP:       entry - (SL-entry) * rr_ratio

  Bullish mirror.

Detection runs on every signal-TF bar close. Stream is single-position-at-a-time
(skip if any pending or any open position with this comment).

The slow Python version exists for diagnostics; production use should call
lsfvg_fast.simulate_fast() (Numba JIT, ~25-30x faster).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import List, Optional

import numpy as np
import pandas as pd

from .scalper_v1 import (
    Deal, Pending, Position, SimResult, SymbolMeta,
    ORDER_BUY_LIMIT, ORDER_SELL_LIMIT,
    _norm_price, _calc_lots, _pnl,
)


@dataclass
class LSFVGConfig:
    risk_pct: float = 3.0
    signal_tf_minutes: int = 15
    lookback_bars: int = 15        # Bars [3+sweep_window..3+sweep_window+lookback] = pool
    min_fvg_pts: int = 50          # Skip FVGs smaller than this (filter noise)
    max_fvg_pts: int = 5000        # Skip FVGs larger than this (news spikes)
    sweep_buffer_pts: int = 30     # SL = sweep_high + buffer
    rr_ratio: float = 2.0          # TP distance = SL distance × RR
    half_tp_ratio: float = 0.5     # 0 disables split TP
    pending_expire_bars: int = 4   # Pending order TTL in signal-TF bars
    # Loosened detection: sweep can be on any bar in [1..sweep_window_bars],
    # FVG (3-bar pattern) can appear anywhere strictly newer than sweep,
    # with FVG-middle-bar in [2..sweep_idx-1].
    sweep_window_bars: int = 5
    use_ema_filter: bool = False
    ema_period: int = 50
    daily_target_pct: float = 0.0  # 0 disables
    daily_loss_pct: float = 0.0    # 0 disables
    comment: str = "LSFVG"


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


def simulate(
    ticks: pd.DataFrame,
    sig_bars: pd.DataFrame,         # signal-TF bars (M15 default)
    m1_bars: pd.DataFrame,          # M1 bars for new-bar event timing
    cfg: LSFVGConfig,
    meta: SymbolMeta,
    initial_balance: float = 10_000.0,
    debug_path: Optional[str] = None,
) -> SimResult:
    """Reference Python sim. For production WFO use lsfvg_fast.simulate_fast()."""
    def _to_naive_ns(s: pd.Series) -> np.ndarray:
        if hasattr(s.dt, "tz") and s.dt.tz is not None:
            return s.dt.tz_convert("UTC").dt.tz_localize(None).values.astype("datetime64[ns]")
        return s.values.astype("datetime64[ns]")

    t_ts = _to_naive_ns(ticks["ts"]).astype(np.int64)
    t_bid = ticks["bid"].values.astype(np.float64)
    t_ask = ticks["ask"].values.astype(np.float64)

    sig_ts = _to_naive_ns(sig_bars["ts"]).astype(np.int64)
    sig_h = sig_bars["high"].values.astype(np.float64)
    sig_l = sig_bars["low"].values.astype(np.float64)
    sig_c = sig_bars["close"].values.astype(np.float64)
    n_sig = len(sig_ts)

    sma = _compute_sma(sig_c, cfg.ema_period) if cfg.use_ema_filter else np.zeros(n_sig)

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

    diag_setups = 0
    diag_filled = 0
    diag_expired = 0

    sig_tf_ns = np.int64(cfg.signal_tf_minutes * 60 * 1_000_000_000)

    # Process each tick: (1) daily caps (2) expire/fill/SL-TP (3) on signal-TF bar close, detect setup
    last_sig_idx = -1  # last index whose close we've already processed for setup

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

        # Daily cap check
        if not daily_lock and (cfg.daily_target_pct > 0 or cfg.daily_loss_pct > 0):
            unreal = 0.0
            for p in positions:
                close_px = bid if p.direction == 1 else ask
                unreal += _pnl(p, close_px, meta)
            today_pnl = realized_today + unreal
            target_locked = False
            if cfg.daily_target_pct > 0 and today_pnl >= balance_day_start * cfg.daily_target_pct / 100.0:
                target_locked = True
            elif cfg.daily_loss_pct > 0 and today_pnl <= -balance_day_start * cfg.daily_loss_pct / 100.0:
                target_locked = True
            if target_locked:
                for p in positions:
                    close_px = bid if p.direction == 1 else ask
                    pnl = _pnl(p, close_px, meta)
                    balance += pnl
                    realized_today += pnl
                    deals.append(Deal(ts, 'other', p.direction, p.lots, close_px, pnl))
                    if balance > balance_max: balance_max = balance
                    cur = balance_max - balance
                    if cur > dd_abs: dd_abs = cur
                positions = []
                pending = []
                daily_lock = True

        if daily_lock:
            continue

        # Expire pending
        if pending:
            still = []
            for p in pending:
                if ts < p.expire_ts:
                    still.append(p)
                else:
                    diag_expired += 1
            pending = still

        # Pending fills
        new_pos = []
        still_pending = []
        for p in pending:
            triggered = False
            fill = 0.0
            direction = 0
            # LIMIT orders fill on retracement TO the level
            if p.kind == ORDER_BUY_LIMIT and ask <= p.price:
                triggered = True; fill = p.price; direction = 1
            elif p.kind == ORDER_SELL_LIMIT and bid >= p.price:
                triggered = True; fill = p.price; direction = -1
            if triggered:
                pos = Position(direction, fill, p.sl, p.tp, p.lots)
                new_pos.append(pos)
                deals.append(Deal(ts, 'entry', direction, p.lots, fill, 0.0))
                diag_filled += 1
            else:
                still_pending.append(p)
        pending = still_pending

        # SL/TP on existing positions (not just-filled)
        survivors = []
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
                deals.append(Deal(ts, 'sl', pos.direction, pos.lots, pos.sl, pnl))
                if balance > balance_max: balance_max = balance
                cur = balance_max - balance
                if cur > dd_abs: dd_abs = cur
            elif hit_tp:
                pnl = _pnl(pos, pos.tp, meta)
                balance += pnl; realized_today += pnl
                deals.append(Deal(ts, 'tp', pos.direction, pos.lots, pos.tp, pnl))
                if balance > balance_max: balance_max = balance
                cur = balance_max - balance
                if cur > dd_abs: dd_abs = cur
            else:
                survivors.append(pos)
        positions = survivors + new_pos

        # On signal-TF bar close → detect setup. Only one detection per bar.
        # Find newly-completed signal-TF bar: bar i is "complete" once ts_ns >= sig_ts[i+1].
        # Iterate to advance last_sig_idx.
        while last_sig_idx + 1 < n_sig and (
            (last_sig_idx + 2 < n_sig and ts_ns >= sig_ts[last_sig_idx + 2])
        ):
            last_sig_idx += 1
            # bar (last_sig_idx) is now fully closed (next bar already started)
            if pending or positions:
                continue  # stream busy
            if last_sig_idx < 3 + cfg.lookback_bars:
                continue  # not enough history
            # Map "bar 1" semantics from the EA: just-closed bar = last_sig_idx
            # bar 1 = last_sig_idx, bar 2 = last_sig_idx-1, bar 3 = last_sig_idx-2
            # pool = bars [last_sig_idx-3-lookback .. last_sig_idx-3]
            i1 = last_sig_idx
            i2 = last_sig_idx - 1
            i3 = last_sig_idx - 2
            pool_lo = last_sig_idx - 2 - cfg.lookback_bars  # bar 3+lookback
            pool_hi = last_sig_idx - 2                       # bar 3 (exclusive end below)
            if pool_lo < 0:
                continue
            pool_high = sig_h[pool_lo:pool_hi].max()
            pool_low = sig_l[pool_lo:pool_hi].min()

            h1, l1, c1 = sig_h[i1], sig_l[i1], sig_c[i1]
            h2, l2 = sig_h[i2], sig_l[i2]
            h3, l3 = sig_h[i3], sig_l[i3]
            sweep_high = max(h1, h2)
            sweep_low = min(l1, l2)

            ema = sma[i1] if cfg.use_ema_filter else 0.0
            if cfg.use_ema_filter and (np.isnan(ema) or ema <= 0):
                continue

            stops_pad = meta.stops_level_pts * meta.point
            # Pending expiry = next bar open + N × signal_tf_minutes
            forming_open = sig_ts[i1] + sig_tf_ns
            expire_ts_ns = forming_open + np.int64(cfg.pending_expire_bars) * sig_tf_ns
            expire_ts = pd.Timestamp(expire_ts_ns)

            # ==== BEARISH ====
            bearish = (sweep_high > pool_high) and (c1 < pool_high) and (l3 > h1)
            if cfg.use_ema_filter:
                bearish = bearish and (c1 < ema)
            if bearish:
                fvg_pts = (l3 - h1) / meta.point
                if cfg.min_fvg_pts <= fvg_pts <= cfg.max_fvg_pts:
                    entry = _norm_price(l3, meta)
                    sl = _norm_price(sweep_high + cfg.sweep_buffer_pts * meta.point, meta)
                    sl_dist = sl - entry
                    if sl_dist > 0 and entry >= bid + stops_pad:
                        tp = _norm_price(entry - sl_dist * cfg.rr_ratio, meta)
                        sl_pts = sl_dist / meta.point
                        total_lots = _calc_lots(balance, cfg.risk_pct, sl_pts, meta)
                        if total_lots > 0:
                            half_lots = total_lots
                            if cfg.half_tp_ratio > 0:
                                half_lots = round(total_lots / 2.0 / meta.volume_step) * meta.volume_step
                                if half_lots < meta.volume_min:
                                    half_lots = meta.volume_min
                                half_lots = round(half_lots, 2)
                            if cfg.half_tp_ratio > 0:
                                tp_half = _norm_price(entry - sl_dist * cfg.rr_ratio * cfg.half_tp_ratio, meta)
                                pending.append(Pending(ORDER_SELL_LIMIT, entry, sl, tp_half,
                                                        half_lots, expire_ts, 0, ts))
                                pending.append(Pending(ORDER_SELL_LIMIT, entry, sl, tp,
                                                        half_lots, expire_ts, 0, ts))
                            else:
                                pending.append(Pending(ORDER_SELL_LIMIT, entry, sl, tp,
                                                        total_lots, expire_ts, 0, ts))
                            diag_setups += 1
                            continue  # one direction per bar

            # ==== BULLISH ====
            bullish = (sweep_low < pool_low) and (c1 > pool_low) and (h3 < l1)
            if cfg.use_ema_filter:
                bullish = bullish and (c1 > ema)
            if bullish:
                fvg_pts = (l1 - h3) / meta.point
                if cfg.min_fvg_pts <= fvg_pts <= cfg.max_fvg_pts:
                    entry = _norm_price(h3, meta)
                    sl = _norm_price(sweep_low - cfg.sweep_buffer_pts * meta.point, meta)
                    sl_dist = entry - sl
                    if sl_dist > 0 and entry <= ask - stops_pad:
                        tp = _norm_price(entry + sl_dist * cfg.rr_ratio, meta)
                        sl_pts = sl_dist / meta.point
                        total_lots = _calc_lots(balance, cfg.risk_pct, sl_pts, meta)
                        if total_lots > 0:
                            half_lots = total_lots
                            if cfg.half_tp_ratio > 0:
                                half_lots = round(total_lots / 2.0 / meta.volume_step) * meta.volume_step
                                if half_lots < meta.volume_min:
                                    half_lots = meta.volume_min
                                half_lots = round(half_lots, 2)
                            if cfg.half_tp_ratio > 0:
                                tp_half = _norm_price(entry + sl_dist * cfg.rr_ratio * cfg.half_tp_ratio, meta)
                                pending.append(Pending(ORDER_BUY_LIMIT, entry, sl, tp_half,
                                                        half_lots, expire_ts, 0, ts))
                                pending.append(Pending(ORDER_BUY_LIMIT, entry, sl, tp,
                                                        half_lots, expire_ts, 0, ts))
                            else:
                                pending.append(Pending(ORDER_BUY_LIMIT, entry, sl, tp,
                                                        total_lots, expire_ts, 0, ts))
                            diag_setups += 1

    print(f"  [diag] sig_bars={n_sig:,}  setups={diag_setups}  "
          f"filled={diag_filled}  expired={diag_expired}")

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
        initial_balance=initial_balance, final_balance=balance,
        net_profit=net, trades=trades, tp_count=tp_count, sl_count=sl_count,
        other_count=other_count, max_drawdown=dd_abs, max_drawdown_pct=dd_pct,
        profit_factor=pf, balance_curve=bc, deals=deals,
    )
