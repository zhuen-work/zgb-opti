"""FBO Stream 1 simulator — port of FBO_FVG_v2.mq5's ProcessFBO (Stream 1 only).

Strategy:
- Signal TF = M30 (configurable but defaults match EA's _time_frame=30)
- Custom N-bar fractal: bar i is fractal-high if high[i] strictly greater than
  highs of N bars on each side (same for low). Scans backward from most recent
  fully-confirmable bar.
- SMA(period) on M30 closes is the direction filter:
    * bid > SMA -> buy only
    * bid < SMA -> sell only
- Single pending pair per stream, only placed if no pending and no open position.
- HalfTP_Ratio > 0 splits order into two halves with split TP.
- Pending expiry = pending_expire_bars * 30 minutes from current M30 bar open.
- No daily target/loss caps. No hours filter. Trades any time M1 bars exist.

Differences vs EA (intentional simplifications):
- Pysim uses COMPLETED M30 bars only (forming-bar high/low not running; fractal
  detection lags by 1 M30 bar vs MT5 live). Reduces complexity at small accuracy
  cost. SMA also uses last completed M30 bar's value (matches EA's GetEMA shift=1).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List, Optional

import numpy as np
import pandas as pd

from .scalper_v1 import (
    Deal, Pending, Position, SimResult, SymbolMeta,
    ORDER_BUY_STOP, ORDER_SELL_STOP,
    _norm_price, _calc_lots, _pnl,
)


@dataclass
class FBOS1Config:
    risk_pct: float = 3.0
    fractal_bars: int = 4              # _Bars (fractal lookback period each side)
    take_profit_pts: int = 15000       # _take_profit
    stop_loss_pts: int = 5000          # _stop_loss
    half_tp_ratio: float = 0.6         # _HalfTP1
    sma_period: int = 5                # _EMA_Period1 (SMA on signal-TF closes)
    pending_expire_bars: int = 2       # _PendingExpireBars (in signal-TF bars)
    signal_tf_minutes: int = 30        # M30 (S1) = 30, H4 (S2) = 240
    comment: str = "FBO_A"


def _compute_fractal_levels(
    highs: np.ndarray, lows: np.ndarray, period: int
) -> tuple[np.ndarray, np.ndarray]:
    """For each M30 bar index i, compute (frac_high[i], frac_low[i]) = the most
    recent confirmed fractal level usable when processing bar i+1.

    A bar j is a fractal-high if high[j] is strictly greater than each of
    high[j-period..j-1] and high[j+1..j+period]. Symmetric for low.

    Returns arrays indexed by i. Value is 0.0 if no fractal yet."""
    n = len(highs)
    frac_h = np.zeros(n, dtype=np.float64)
    frac_l = np.zeros(n, dtype=np.float64)

    # Mark which bars are valid fractals (require period bars on each side)
    is_fh = np.zeros(n, dtype=bool)
    is_fl = np.zeros(n, dtype=bool)
    for j in range(period, n - period):
        h = highs[j]
        if h > highs[j-period:j].max() and h > highs[j+1:j+period+1].max():
            is_fh[j] = True
        l = lows[j]
        if l < lows[j-period:j].min() and l < lows[j+1:j+period+1].min():
            is_fl[j] = True

    # Forward sweep: at bar i, the most recent confirmed fractal is at j <= i - period
    # (since we needed period bars to the right of j to confirm it)
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
    """SMA of `closes` with period; sma[i] = mean(closes[i-period+1..i]).

    Returns array of same length as closes; sma[i] = NaN until i >= period-1."""
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
    m30_bars: pd.DataFrame,
    m1_bars: pd.DataFrame,
    cfg: FBOS1Config,
    meta: SymbolMeta,
    initial_balance: float = 10_000.0,
    debug_path: Optional[str] = None,
) -> SimResult:
    """Run the FBO Stream 1 simulation.

    ticks: [ts, bid, ask] tz-aware UTC
    m30_bars / m1_bars: [ts, open, high, low, close] tz-aware UTC
    """
    def _to_naive_ns(s: pd.Series) -> np.ndarray:
        if hasattr(s.dt, "tz") and s.dt.tz is not None:
            return s.dt.tz_convert("UTC").dt.tz_localize(None).values.astype("datetime64[ns]")
        return s.values.astype("datetime64[ns]")

    # Pre-compute fractal levels and SMA on M30 closes
    m30_highs = m30_bars["high"].values.astype(np.float64)
    m30_lows = m30_bars["low"].values.astype(np.float64)
    m30_closes = m30_bars["close"].values.astype(np.float64)
    m30_times = _to_naive_ns(m30_bars["ts"])
    frac_h, frac_l = _compute_fractal_levels(m30_highs, m30_lows, cfg.fractal_bars)
    sma = _compute_sma(m30_closes, cfg.sma_period)

    # M1 bar timestamps for entry-trigger loop
    m1_times = _to_naive_ns(m1_bars["ts"])

    t_ts = _to_naive_ns(ticks["ts"])
    t_bid = ticks["bid"].values.astype(np.float64)
    t_ask = ticks["ask"].values.astype(np.float64)

    balance = initial_balance
    pending: List[Pending] = []
    positions: List[Position] = []
    deals: List[Deal] = []
    balance_max = initial_balance
    dd_abs = 0.0

    # Diagnostics
    m1_bars_seen = 0
    m1_bars_skipped_pending_or_pos = 0
    m1_bars_placed_orders = 0
    pending_placed_count = 0
    pending_expired_count = 0
    pending_filled_count = 0

    debug_enabled = debug_path is not None
    placements_log = []
    order_events = []
    _order_seq = [0]

    def _next_oid():
        _order_seq[0] += 1
        return _order_seq[0]

    def _update_dd():
        nonlocal balance_max, dd_abs
        if balance > balance_max:
            balance_max = balance
        cur = balance_max - balance
        if cur > dd_abs:
            dd_abs = cur

    m1_idx = 0
    m1_ts_np = m1_times
    m30_ts_np = m30_times

    for k in range(len(t_ts)):
        ts = pd.Timestamp(t_ts[k])
        bid = t_bid[k]
        ask = t_ask[k]

        # Expire pending past expiry timestamp
        if pending:
            still = []
            before = len(pending)
            for p in pending:
                if ts < p.expire_ts:
                    still.append(p)
                elif debug_enabled:
                    order_events.append({
                        "ts": ts, "oid": p.oid, "event": "expired",
                        "kind": p.kind, "price": p.price,
                    })
            pending = still
            pending_expired_count += (before - len(pending))

        # Check pending triggers (stop orders only — no limit orders for FBO Stream 1)
        new_positions = []
        still_pending = []
        for p in pending:
            triggered = False
            if p.kind == ORDER_BUY_STOP and ask >= p.price:
                triggered = True
                fill = p.price
                direction = 1
            elif p.kind == ORDER_SELL_STOP and bid <= p.price:
                triggered = True
                fill = p.price
                direction = -1
            if triggered:
                new_positions.append(Position(direction, fill, p.sl, p.tp, p.lots))
                deals.append(Deal(ts, 'entry', direction, p.lots, fill, 0.0))
                pending_filled_count += 1
                if debug_enabled:
                    order_events.append({
                        "ts": ts, "oid": p.oid, "event": "filled",
                        "kind": p.kind, "price": p.price, "fill_price": fill,
                        "bid": bid, "ask": ask,
                    })
            else:
                still_pending.append(p)
        pending = still_pending

        # Check SL/TP on existing positions (not just-filled — same broker semantics as scalper)
        survivors = []
        for pos in positions:
            hit_sl = hit_tp = False
            if pos.direction == 1:
                if bid <= pos.sl:
                    hit_sl = True
                elif bid >= pos.tp:
                    hit_tp = True
            else:
                if ask >= pos.sl:
                    hit_sl = True
                elif ask <= pos.tp:
                    hit_tp = True
            if hit_sl:
                pnl = _pnl(pos, pos.sl, meta)
                balance += pnl
                deals.append(Deal(ts, 'sl', pos.direction, pos.lots, pos.sl, pnl))
                _update_dd()
            elif hit_tp:
                pnl = _pnl(pos, pos.tp, meta)
                balance += pnl
                deals.append(Deal(ts, 'tp', pos.direction, pos.lots, pos.tp, pnl))
                _update_dd()
            else:
                survivors.append(pos)
        positions = survivors + new_positions

        # New M1 bar? Process entry logic
        ts_ns = t_ts[k]
        while m1_idx < len(m1_ts_np) and m1_ts_np[m1_idx] <= ts_ns:
            bar_ts = pd.Timestamp(m1_ts_np[m1_idx])
            m1_idx += 1
            m1_bars_seen += 1

            # FBO: only place if stream is idle (no pending and no open position)
            if pending or positions:
                m1_bars_skipped_pending_or_pos += 1
                continue

            # Locate M30 bar containing this M1 bar (current/forming bar in EA's view).
            # In pysim we use the last COMPLETED M30 bar (one before forming) for fractal/SMA.
            forming_m30 = np.searchsorted(
                m30_ts_np, np.datetime64(bar_ts.to_datetime64()), side='right'
            ) - 1
            last_completed = forming_m30 - 1   # most recent fully-closed M30
            if last_completed < cfg.sma_period - 1:
                continue

            sma_val = sma[last_completed]
            if np.isnan(sma_val) or sma_val <= 0:
                continue

            # Direction filter
            want_buy = bid > sma_val
            want_sell = bid < sma_val
            if not (want_buy or want_sell):
                continue

            m1_bars_placed_orders += 1

            # Pending expiry = forming M30 open + N * 30 min
            if forming_m30 >= 0:
                forming_open = pd.Timestamp(m30_ts_np[forming_m30])
            else:
                forming_open = bar_ts
            expire_ts = forming_open + pd.Timedelta(
                minutes=cfg.signal_tf_minutes * cfg.pending_expire_bars
            )

            stops_pad = meta.stops_level_pts * meta.point

            total_lots = _calc_lots(balance, cfg.risk_pct, cfg.stop_loss_pts, meta)
            if total_lots <= 0:
                continue
            half_lots = total_lots
            if cfg.half_tp_ratio > 0:
                half_lots = round(total_lots / 2.0 / meta.volume_step) * meta.volume_step
                if half_lots < meta.volume_min:
                    half_lots = meta.volume_min
                half_lots = round(half_lots, 2)

            placed_this_bar = 0

            def _add_pending(kind, price, sl_px, tp_px, lots):
                nonlocal placed_this_bar
                oid = _next_oid() if debug_enabled else 0
                pending.append(Pending(kind, price, sl_px, tp_px, lots, expire_ts, oid, bar_ts))
                placed_this_bar += 1
                if debug_enabled:
                    order_events.append({
                        "ts": bar_ts, "oid": oid, "event": "placed",
                        "kind": kind, "price": price, "sl": sl_px, "tp": tp_px,
                        "lots": lots, "expire_ts": expire_ts,
                    })

            placement_row = {
                "bar_ts": bar_ts,
                "forming_m30_ts": pd.Timestamp(m30_ts_np[forming_m30]) if forming_m30 >= 0 else None,
                "last_completed_m30_ts": pd.Timestamp(m30_ts_np[last_completed]),
                "frac_high": frac_h[last_completed],
                "frac_low": frac_l[last_completed],
                "sma": sma_val,
                "ask": ask, "bid": bid, "balance": balance,
                "total_lots": total_lots, "half_lots": half_lots,
                "want_buy": want_buy, "want_sell": want_sell,
            }

            if want_buy:
                fh = frac_h[last_completed]
                if fh > 0:
                    entry = _norm_price(fh, meta)
                    min_e = _norm_price(ask + stops_pad, meta)
                    if entry < min_e:
                        entry = min_e
                    if entry > ask:
                        sl_px = _norm_price(entry - cfg.stop_loss_pts * meta.point, meta)
                        tp_full = _norm_price(entry + cfg.take_profit_pts * meta.point, meta)
                        if cfg.half_tp_ratio > 0:
                            tp_half = _norm_price(
                                entry + cfg.take_profit_pts * cfg.half_tp_ratio * meta.point, meta
                            )
                            _add_pending(ORDER_BUY_STOP, entry, sl_px, tp_half, half_lots)
                            _add_pending(ORDER_BUY_STOP, entry, sl_px, tp_full, half_lots)
                        else:
                            _add_pending(ORDER_BUY_STOP, entry, sl_px, tp_full, total_lots)
                        placement_row["buy_entry"] = entry
                        placement_row["buy_sl"] = sl_px
                        placement_row["buy_tp"] = tp_full

            if want_sell:
                fl = frac_l[last_completed]
                if fl > 0:
                    entry = _norm_price(fl, meta)
                    max_e = _norm_price(bid - stops_pad, meta)
                    if entry > max_e:
                        entry = max_e
                    if entry < bid:
                        sl_px = _norm_price(entry + cfg.stop_loss_pts * meta.point, meta)
                        tp_full = _norm_price(entry - cfg.take_profit_pts * meta.point, meta)
                        if cfg.half_tp_ratio > 0:
                            tp_half = _norm_price(
                                entry - cfg.take_profit_pts * cfg.half_tp_ratio * meta.point, meta
                            )
                            _add_pending(ORDER_SELL_STOP, entry, sl_px, tp_half, half_lots)
                            _add_pending(ORDER_SELL_STOP, entry, sl_px, tp_full, half_lots)
                        else:
                            _add_pending(ORDER_SELL_STOP, entry, sl_px, tp_full, total_lots)
                        placement_row["sell_entry"] = entry
                        placement_row["sell_sl"] = sl_px
                        placement_row["sell_tp"] = tp_full

            if debug_enabled and placed_this_bar > 0:
                placements_log.append(placement_row)
            pending_placed_count += placed_this_bar

    # End: compute metrics (mirrors scalper_v1 sim)
    print(f"  [diag] M1 bars seen={m1_bars_seen:,}  "
          f"skipped_pending_or_pos={m1_bars_skipped_pending_or_pos:,}  "
          f"placed_orders={m1_bars_placed_orders:,}")
    print(f"  [diag] pending placed={pending_placed_count:,}  "
          f"filled={pending_filled_count:,}  expired={pending_expired_count:,}")

    if debug_enabled:
        from pathlib import Path as _P
        base = _P(debug_path)
        base.parent.mkdir(parents=True, exist_ok=True)
        if placements_log:
            pd.DataFrame(placements_log).to_csv(base.with_suffix(".placements.csv"), index=False)
        if order_events:
            pd.DataFrame(order_events).to_csv(base.with_suffix(".orders.csv"), index=False)
        deals_df = pd.DataFrame([
            {"ts": d.ts, "kind": d.kind, "direction": d.direction,
             "lots": d.lots, "price": d.price, "pnl": d.pnl}
            for d in deals
        ])
        if not deals_df.empty:
            deals_df.to_csv(base.with_suffix(".deals.csv"), index=False)
        print(f"  [debug] placements={len(placements_log)}  "
              f"order_events={len(order_events)}  deals={len(deals)}")

    tp_count = sum(1 for d in deals if d.kind == 'tp')
    sl_count = sum(1 for d in deals if d.kind == 'sl')
    other_count = sum(1 for d in deals if d.kind == 'other')
    trades = tp_count + sl_count + other_count

    wins = [d.pnl for d in deals if d.kind != 'entry' and d.pnl > 0]
    losses = [d.pnl for d in deals if d.kind != 'entry' and d.pnl < 0]
    pf = sum(wins) / abs(sum(losses)) if losses else float('inf')
    net = balance - initial_balance
    dd_pct = (dd_abs / balance_max * 100.0) if balance_max > 0 else 0.0

    bc = pd.DataFrame([
        {"ts": d.ts, "pnl": d.pnl}
        for d in deals if d.kind != 'entry'
    ])
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
