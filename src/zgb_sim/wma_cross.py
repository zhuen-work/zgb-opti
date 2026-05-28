"""WMA Golden Cross simulator (50/200 weighted moving averages).

Strategy spec (user-provided):
  - Long  on 50-WMA crossing above 200-WMA
  - Short on 50-WMA crossing below 200-WMA
  - SL = below/above 200-WMA at entry bar (with optional buffer)
  - TP = (a) exit on opposite crossover, or (b) fixed RR if rr_ratio > 0
  - Position size = balance × risk% / SL_dist (standard)

Bar-level sim (no ticks needed) — entries on bar close, SL/TP checked via bar H/L.
Pre-loads 60d of warmup bars before the test window so WMAs are valid by start.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import numpy as np
import pandas as pd

from .scalper_v1 import Deal, SimResult, SymbolMeta


@dataclass
class WMACrossConfig:
    risk_pct: float = 1.0
    fast_period: int = 50
    slow_period: int = 200
    sl_buffer_pts: int = 100       # extra distance beyond 200-WMA for SL
    rr_ratio: float = 0.0          # 0 = exit on opposite cross only; >0 = fixed RR TP
    max_spread_pts: int = 70       # not used in bar-sim but kept for API parity
    one_position_at_a_time: bool = True


def _wma(values: np.ndarray, period: int) -> np.ndarray:
    """Linear-weighted moving average. weights = 1..period, divisor = period*(period+1)/2."""
    L = len(values)
    out = np.full(L, np.nan)
    weights = np.arange(1, period + 1, dtype=np.float64)
    denom = weights.sum()
    for i in range(period - 1, L):
        out[i] = float((values[i - period + 1:i + 1] * weights).sum() / denom)
    return out


def simulate_wma_cross(
    bars: pd.DataFrame,
    cfg: WMACrossConfig,
    meta: SymbolMeta,
    initial_balance: float = 10_000.0,
    window_start_ts: Optional[pd.Timestamp] = None,
    debug: bool = False,
) -> SimResult:
    """Run WMA crossover sim. bars=[ts,open,high,low,close] tz-aware UTC.

    window_start_ts: only count trades with entry timestamp >= this. Pre-window
    bars are used for WMA warmup but not for P&L.
    """
    if len(bars) < cfg.slow_period + 1:
        return SimResult(initial_balance, initial_balance, 0.0, 0, 0, 0, 0,
                         0.0, 0.0, 0.0, pd.DataFrame(), [])

    # Strip tz for fast comparisons
    if hasattr(bars["ts"].dt, "tz") and bars["ts"].dt.tz is not None:
        bar_ts = bars["ts"].dt.tz_convert("UTC").dt.tz_localize(None).values.astype("datetime64[ns]")
    else:
        bar_ts = bars["ts"].values.astype("datetime64[ns]")
    b_open = bars["open"].values.astype(np.float64)
    b_high = bars["high"].values.astype(np.float64)
    b_low = bars["low"].values.astype(np.float64)
    b_close = bars["close"].values.astype(np.float64)

    wma_fast = _wma(b_close, cfg.fast_period)
    wma_slow = _wma(b_close, cfg.slow_period)

    # Pre-compute crossover events: +1 = bullish cross (fast crosses up over slow),
    # -1 = bearish cross, 0 = none.
    cross = np.zeros(len(bars), dtype=np.int8)
    for i in range(1, len(bars)):
        if np.isnan(wma_fast[i]) or np.isnan(wma_slow[i]) or np.isnan(wma_fast[i-1]) or np.isnan(wma_slow[i-1]):
            continue
        prev_diff = wma_fast[i-1] - wma_slow[i-1]
        curr_diff = wma_fast[i] - wma_slow[i]
        if prev_diff <= 0 and curr_diff > 0:
            cross[i] = 1
        elif prev_diff >= 0 and curr_diff < 0:
            cross[i] = -1

    window_start_ns = (window_start_ts.tz_localize(None).value if window_start_ts and window_start_ts.tzinfo
                       else (window_start_ts.value if window_start_ts else 0))

    balance = initial_balance
    balance_max = initial_balance
    dd_abs = 0.0

    pos_active = False
    pos_dir = 0
    pos_entry = 0.0
    pos_sl = 0.0
    pos_tp = 0.0
    pos_lots = 0.0
    pos_entry_ts = None

    deals: List[Deal] = []

    def _close(close_px: float, kind: str, ts: pd.Timestamp):
        nonlocal balance, balance_max, dd_abs, pos_active
        diff = (close_px - pos_entry) * pos_dir
        pnl = diff * pos_lots * meta.tick_value / meta.tick_size
        balance += pnl
        if balance > balance_max:
            balance_max = balance
        cur_dd = balance_max - balance
        if cur_dd > dd_abs:
            dd_abs = cur_dd
        deals.append(Deal(ts, kind, pos_dir, pos_lots, close_px, pnl))
        pos_active = False

    for i in range(len(bars)):
        ts = pd.Timestamp(bar_ts[i])
        # Use bar OPEN as the "current" reference for entries fired at prior bar close.
        # SL/TP intra-bar via bar H/L.
        hi = b_high[i]
        lo = b_low[i]

        # ---- Manage existing position ----
        if pos_active:
            # Check SL/TP using bar H/L (worst case order: SL before TP — pessimistic)
            hit_sl = False
            hit_tp = False
            if pos_dir == 1:
                if lo <= pos_sl:
                    hit_sl = True
                elif pos_tp > 0 and hi >= pos_tp:
                    hit_tp = True
            else:
                if hi >= pos_sl:
                    hit_sl = True
                elif pos_tp > 0 and lo <= pos_tp:
                    hit_tp = True
            if hit_sl:
                _close(pos_sl, "sl", ts)
            elif hit_tp:
                _close(pos_tp, "tp", ts)
            elif cross[i] != 0 and cross[i] == -pos_dir:
                # Opposite crossover → exit at bar close
                _close(b_close[i], "other", ts)

        # ---- Detect new entry on this bar's crossover ----
        if cross[i] != 0 and not pos_active:
            # Only count trade if entry is within reporting window
            if window_start_ns > 0 and bar_ts[i].astype(np.int64) < window_start_ns:
                continue
            direction = int(cross[i])  # +1 long, -1 short
            entry_px = b_close[i]
            # SL = beyond 200-WMA at this bar
            slow_lvl = wma_slow[i]
            if direction == 1:
                sl_px = slow_lvl - cfg.sl_buffer_pts * meta.point
                if sl_px >= entry_px:  # SL on wrong side (rare)
                    continue
                sl_dist_pts = max(1, int(round((entry_px - sl_px) / meta.point)))
                tp_px = entry_px + cfg.rr_ratio * sl_dist_pts * meta.point if cfg.rr_ratio > 0 else 0.0
            else:
                sl_px = slow_lvl + cfg.sl_buffer_pts * meta.point
                if sl_px <= entry_px:
                    continue
                sl_dist_pts = max(1, int(round((sl_px - entry_px) / meta.point)))
                tp_px = entry_px - cfg.rr_ratio * sl_dist_pts * meta.point if cfg.rr_ratio > 0 else 0.0

            # Lot sizing (matches scalper_v1._calc_lots)
            risk_money = balance * cfg.risk_pct / 100.0
            sl_money = sl_dist_pts * meta.point * meta.tick_value / meta.tick_size
            if sl_money <= 0:
                continue
            lots = risk_money / sl_money
            lots = max(meta.volume_min, min(meta.volume_max, lots))
            lots = round(lots / meta.volume_step) * meta.volume_step
            lots = round(lots, 2)
            if lots <= 0:
                continue

            pos_active = True
            pos_dir = direction
            pos_entry = entry_px
            pos_sl = sl_px
            pos_tp = tp_px
            pos_lots = lots
            pos_entry_ts = ts
            deals.append(Deal(ts, "entry", direction, lots, entry_px, 0.0))
            if debug:
                print(f"  [{ts}] {'BUY' if direction==1 else 'SELL'} @ {entry_px:.2f} "
                      f"SL={sl_px:.2f} ({sl_dist_pts}pt) lots={lots:.2f} "
                      f"WMA50={wma_fast[i]:.2f} WMA200={wma_slow[i]:.2f}")

    # Close any open position at last bar
    if pos_active:
        _close(b_close[-1], "other", pd.Timestamp(bar_ts[-1]))

    # Summary
    tp_count = sum(1 for d in deals if d.kind == "tp")
    sl_count = sum(1 for d in deals if d.kind == "sl")
    other_count = sum(1 for d in deals if d.kind == "other")
    trades = tp_count + sl_count + other_count
    wins = sum(d.pnl for d in deals if d.kind != "entry" and d.pnl > 0)
    losses = sum(d.pnl for d in deals if d.kind != "entry" and d.pnl < 0)
    pf = wins / abs(losses) if losses != 0 else (float("inf") if wins > 0 else 0.0)
    net = balance - initial_balance
    dd_pct = (dd_abs / balance_max * 100.0) if balance_max > 0 else 0.0
    bc = pd.DataFrame([{"ts": d.ts, "pnl": d.pnl} for d in deals if d.kind != "entry"])
    if not bc.empty:
        bc["balance"] = initial_balance + bc["pnl"].cumsum()

    return SimResult(
        initial_balance=initial_balance, final_balance=balance,
        net_profit=net, trades=trades, tp_count=tp_count,
        sl_count=sl_count, other_count=other_count,
        max_drawdown=dd_abs, max_drawdown_pct=dd_pct,
        profit_factor=pf, balance_curve=bc, deals=deals,
    )
