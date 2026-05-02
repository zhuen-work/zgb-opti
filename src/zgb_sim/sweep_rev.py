"""Liquidity Sweep Reversal simulator — port of Scalper_v2 Stream 2.

Detect bars that swept a recent swing high/low and reverted, then enter
in the OPPOSITE direction (fade the stop-hunt).

Per M5 bar j (just-completed):
  - swing_hi_prior = max(high[j-lookback..j-1])  ← bars BEFORE j
  - swing_lo_prior = min(low[j-lookback..j-1])
  - Bullish sweep: high[j] >= swing_hi_prior + sweep_min_pts AND close[j] < swing_hi_prior
    → SELL setup (sweep_dir=-1)
  - Bearish sweep: low[j] <= swing_lo_prior - sweep_min_pts AND close[j] > swing_lo_prior
    → BUY setup (sweep_dir=+1)

After detection, wait `confirm_bars` M5 bars. Confirm if:
  - SELL: close[j + confirm_bars] < swing_level (price stayed below)
  - BUY:  close[j + confirm_bars] > swing_level (price stayed above)

Invalidate if interim bar re-took the swept extreme.

On confirmation, enter market at next M1 bar's open. SL = sweep_extreme ± buffer,
TP = entry ± RR × SL_dist.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import numpy as np
import pandas as pd

from .scalper_v1 import (
    Deal, Pending, Position, SimResult, SymbolMeta,
    ORDER_BUY_STOP, ORDER_SELL_STOP,
    _norm_price, _calc_lots, _pnl,
)


@dataclass
class SweepRevConfig:
    risk_pct: float = 1.0
    swing_lookback: int = 30
    sweep_min_pts: int = 50
    confirm_bars: int = 1
    sl_buffer_pts: int = 30
    rr_ratio: float = 2.0
    half_tp_ratio: float = 0.0
    pending_expire_bars: int = 5     # after entry placed, expire if not filled (market orders fill instantly so unused)
    signal_tf_minutes: int = 5       # M5 default
    daily_target_pct: float = 6.0    # 0 disables
    daily_loss_pct: float = 8.0      # 0 disables
    comment: str = "SWEEP"


def _detect_sweeps(
    highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
    lookback: int, sweep_min: float, point: float,
    confirm_bars: int,
) -> list[tuple[int, int, float, float]]:
    """Return list of (confirm_bar_idx, direction, sweep_extreme, swing_level)
    for confirmed sweeps. direction=+1 for BUY setup (low swept), -1 for SELL.

    Confirm = close[j+confirm_bars] still on the right side of swing_level
    AND no intermediate bar re-took the sweep extreme.
    """
    n = len(highs)
    events = []
    for j in range(lookback + 1, n - confirm_bars):
        # Swing prior to bar j (excluding j itself)
        swing_hi = highs[j - lookback:j].max()
        swing_lo = lows[j - lookback:j].min()
        c = closes[j]

        bullish_sweep = (highs[j] >= swing_hi + sweep_min * point) and (c < swing_hi)
        bearish_sweep = (lows[j] <= swing_lo - sweep_min * point) and (c > swing_lo)

        # Bullish sweep (high taken) → SELL setup (sweep_dir = -1)
        if bullish_sweep:
            confirm_idx = j + confirm_bars
            invalidated = False
            for k in range(j + 1, confirm_idx + 1):
                if highs[k] > highs[j]:
                    invalidated = True
                    break
            if not invalidated and closes[confirm_idx] < swing_hi:
                events.append((confirm_idx, -1, highs[j], swing_hi))

        # Bearish sweep (low taken) → BUY setup
        if bearish_sweep:
            confirm_idx = j + confirm_bars
            invalidated = False
            for k in range(j + 1, confirm_idx + 1):
                if lows[k] < lows[j]:
                    invalidated = True
                    break
            if not invalidated and closes[confirm_idx] > swing_lo:
                events.append((confirm_idx, +1, lows[j], swing_lo))

    return events


def simulate(
    ticks: pd.DataFrame,
    m5_bars: pd.DataFrame,
    m1_bars: pd.DataFrame,
    cfg: SweepRevConfig,
    meta: SymbolMeta,
    initial_balance: float = 100.0,
    debug_path: Optional[str] = None,
) -> SimResult:
    """Run Sweep Reversal simulation."""
    def _to_naive_ns(s: pd.Series) -> np.ndarray:
        if hasattr(s.dt, "tz") and s.dt.tz is not None:
            return s.dt.tz_convert("UTC").dt.tz_localize(None).values.astype("datetime64[ns]")
        return s.values.astype("datetime64[ns]")

    t_ts = _to_naive_ns(ticks["ts"]).astype(np.int64)
    t_bid = ticks["bid"].values.astype(np.float64)
    t_ask = ticks["ask"].values.astype(np.float64)

    m5_ts = _to_naive_ns(m5_bars["ts"]).astype(np.int64)
    m5_highs = m5_bars["high"].values.astype(np.float64)
    m5_lows = m5_bars["low"].values.astype(np.float64)
    m5_closes = m5_bars["close"].values.astype(np.float64)

    # Pre-detect all sweep events (confirm_bar_idx, direction, sweep_extreme, swing_level)
    events = _detect_sweeps(
        m5_highs, m5_lows, m5_closes,
        cfg.swing_lookback, cfg.sweep_min_pts, meta.point,
        cfg.confirm_bars,
    )
    # Convert to entry-ready format: entry triggers at the open of the M5 bar AFTER confirm_bar
    entry_signals = []
    for confirm_idx, direction, sweep_extreme, swing_level in events:
        # Trigger time = m5 bar after confirm_idx (i.e., next bar starting after confirm_idx close)
        next_bar_idx = confirm_idx + 1
        if next_bar_idx >= len(m5_ts):
            continue
        trigger_ts_ns = m5_ts[next_bar_idx]
        entry_signals.append({
            "trigger_ts_ns": trigger_ts_ns,
            "direction": direction,
            "sweep_extreme": sweep_extreme,
            "swing_level": swing_level,
        })
    entry_signals.sort(key=lambda x: x["trigger_ts_ns"])

    balance = initial_balance
    positions: List[Position] = []
    deals: List[Deal] = []
    balance_max = initial_balance
    dd_abs = 0.0

    # Daily cap state
    session_day = None
    balance_day_start = initial_balance
    realized_today = 0.0
    daily_lock = False

    diag_signals = len(entry_signals)
    diag_filled = 0
    diag_skipped_open = 0
    diag_target_hits = 0
    diag_loss_hits = 0

    next_signal_idx = 0

    def _update_dd():
        nonlocal balance_max, dd_abs
        if balance > balance_max:
            balance_max = balance
        cur = balance_max - balance
        if cur > dd_abs:
            dd_abs = cur

    for k in range(len(t_ts)):
        ts_ns = int(t_ts[k])
        bid = t_bid[k]
        ask = t_ask[k]
        ts_pd = pd.Timestamp(ts_ns)

        # Daily rollover
        day = ts_pd.date()
        if day != session_day:
            session_day = day
            balance_day_start = balance
            realized_today = 0.0
            daily_lock = False

        # Daily cap check
        if not daily_lock:
            unrealized = 0.0
            for pos in positions:
                close_px = bid if pos.direction == 1 else ask
                unrealized += _pnl(pos, close_px, meta)
            today_pnl = realized_today + unrealized
            target_locked = False
            if cfg.daily_target_pct > 0 and today_pnl >= balance_day_start * cfg.daily_target_pct / 100.0:
                target_locked = True; diag_target_hits += 1
            elif cfg.daily_loss_pct > 0 and today_pnl <= -balance_day_start * cfg.daily_loss_pct / 100.0:
                target_locked = True; diag_loss_hits += 1
            if target_locked:
                for pos in positions:
                    close_px = bid if pos.direction == 1 else ask
                    pnl = _pnl(pos, close_px, meta)
                    balance += pnl
                    realized_today += pnl
                    deals.append(Deal(ts_pd, 'other', pos.direction, pos.lots, close_px, pnl))
                    _update_dd()
                positions = []
                daily_lock = True

        if daily_lock:
            continue

        # Process any signals whose trigger ts has just passed
        while next_signal_idx < len(entry_signals) and entry_signals[next_signal_idx]["trigger_ts_ns"] <= ts_ns:
            sig = entry_signals[next_signal_idx]
            next_signal_idx += 1

            # Skip if any open position (one trade at a time)
            if positions:
                diag_skipped_open += 1
                continue

            direction = sig["direction"]
            sweep_extreme = sig["sweep_extreme"]
            swing_level = sig["swing_level"]

            if direction == 1:
                entry_price = ask
                sl_price = _norm_price(sweep_extreme - cfg.sl_buffer_pts * meta.point, meta)
                sl_dist = entry_price - sl_price
                if sl_dist <= 0:
                    continue
                tp_price = _norm_price(entry_price + sl_dist * cfg.rr_ratio, meta)
            else:
                entry_price = bid
                sl_price = _norm_price(sweep_extreme + cfg.sl_buffer_pts * meta.point, meta)
                sl_dist = sl_price - entry_price
                if sl_dist <= 0:
                    continue
                tp_price = _norm_price(entry_price - sl_dist * cfg.rr_ratio, meta)

            sl_pts = int(sl_dist / meta.point)
            total_lots = _calc_lots(balance, cfg.risk_pct, sl_pts, meta)
            if total_lots <= 0:
                continue

            half_lots = total_lots
            if cfg.half_tp_ratio > 0:
                half_lots = round(total_lots / 2.0 / meta.volume_step) * meta.volume_step
                if half_lots < meta.volume_min:
                    half_lots = meta.volume_min
                half_lots = round(half_lots, 2)
                # Half at HTP × distance
                if direction == 1:
                    tp_half = _norm_price(entry_price + sl_dist * cfg.rr_ratio * cfg.half_tp_ratio, meta)
                else:
                    tp_half = _norm_price(entry_price - sl_dist * cfg.rr_ratio * cfg.half_tp_ratio, meta)
                positions.append(Position(direction, entry_price, sl_price, tp_half, half_lots))
                positions.append(Position(direction, entry_price, sl_price, tp_price, half_lots))
                deals.append(Deal(pd.Timestamp(ts_ns), 'entry', direction, half_lots, entry_price, 0.0))
                deals.append(Deal(pd.Timestamp(ts_ns), 'entry', direction, half_lots, entry_price, 0.0))
            else:
                positions.append(Position(direction, entry_price, sl_price, tp_price, total_lots))
                deals.append(Deal(pd.Timestamp(ts_ns), 'entry', direction, total_lots, entry_price, 0.0))

            diag_filled += 1

        # SL/TP on positions
        survivors = []
        for pos in positions:
            hit_sl = hit_tp = False
            if pos.direction == 1:
                if bid <= pos.sl: hit_sl = True
                elif bid >= pos.tp: hit_tp = True
            else:
                if ask >= pos.sl: hit_sl = True
                elif ask <= pos.tp: hit_tp = True
            ts = pd.Timestamp(ts_ns)
            if hit_sl:
                pnl = _pnl(pos, pos.sl, meta)
                balance += pnl
                realized_today += pnl
                deals.append(Deal(ts, 'sl', pos.direction, pos.lots, pos.sl, pnl))
                _update_dd()
            elif hit_tp:
                pnl = _pnl(pos, pos.tp, meta)
                balance += pnl
                realized_today += pnl
                deals.append(Deal(ts, 'tp', pos.direction, pos.lots, pos.tp, pnl))
                _update_dd()
            else:
                survivors.append(pos)
        positions = survivors

    print(f"  [diag] sweeps_detected={diag_signals}  filled={diag_filled}  "
          f"skipped_open_pos={diag_skipped_open}  "
          f"daily_target_hits={diag_target_hits}  daily_loss_hits={diag_loss_hits}")

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
        initial_balance=initial_balance,
        final_balance=balance,
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
