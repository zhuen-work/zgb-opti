"""Combined FBO Stream 1 + Stream 2 simulator (shared balance + DD).

Mirrors the EA: both streams run on every M1 bar, place orders independently
(distinct comments), but share the account balance. Lot sizing on each new
order uses the current balance (post both streams' realized PnL).

This is NOT just two independent sims summed — wins/losses on either stream
re-scale lot sizes for subsequent orders on the OTHER stream.

Architecture:
  - Per-stream state: pending list, position list, comment, signal-TF bars,
    fractal_high/low arrays, sma array, signal_tf_minutes
  - Shared state: balance, balance_max, dd_abs
  - Per tick: process expirations + fills + SL/TP for BOTH streams together
  - Per M1 bar: process entry-placement logic for BOTH streams
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

import numpy as np
import pandas as pd

from .fbo_s1 import FBOS1Config, _compute_fractal_levels, _compute_sma
from .scalper_v1 import (
    Deal, Pending, Position, SimResult, SymbolMeta,
    ORDER_BUY_STOP, ORDER_SELL_STOP,
    _norm_price, _calc_lots, _pnl,
)


@dataclass
class CombinedResult:
    summary: SimResult
    s1_result: SimResult
    s2_result: SimResult
    total_np: float
    s1_np: float
    s2_np: float
    combined_dd_pct: float


def _process_stream_entry(
    bar_ts: pd.Timestamp,
    bid: float, ask: float, balance: float,
    pending: List[Pending], positions: List[Position],
    cfg: FBOS1Config, meta: SymbolMeta,
    signal_ts_np: np.ndarray, frac_h: np.ndarray, frac_l: np.ndarray, sma: np.ndarray,
) -> int:
    """Place stream's entry orders on this M1 bar if criteria met.
    Returns number of orders placed."""
    if pending or positions:
        return 0  # idle-stream rule: skip if any pending or position

    forming = np.searchsorted(
        signal_ts_np, np.datetime64(bar_ts.to_datetime64()), side='right'
    ) - 1
    last_completed = forming - 1
    if last_completed < cfg.sma_period - 1:
        return 0

    sma_val = sma[last_completed]
    if np.isnan(sma_val) or sma_val <= 0:
        return 0
    want_buy = bid > sma_val
    want_sell = bid < sma_val
    if not (want_buy or want_sell):
        return 0

    forming_open = pd.Timestamp(signal_ts_np[forming]) if forming >= 0 else bar_ts
    expire_ts = forming_open + pd.Timedelta(
        minutes=cfg.signal_tf_minutes * cfg.pending_expire_bars
    )
    stops_pad = meta.stops_level_pts * meta.point

    total_lots = _calc_lots(balance, cfg.risk_pct, cfg.stop_loss_pts, meta)
    if total_lots <= 0:
        return 0
    half_lots = total_lots
    if cfg.half_tp_ratio > 0:
        half_lots = round(total_lots / 2.0 / meta.volume_step) * meta.volume_step
        if half_lots < meta.volume_min:
            half_lots = meta.volume_min
        half_lots = round(half_lots, 2)

    placed = 0

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
                    pending.append(Pending(ORDER_BUY_STOP, entry, sl_px, tp_half,
                                            half_lots, expire_ts, 0, bar_ts))
                    pending.append(Pending(ORDER_BUY_STOP, entry, sl_px, tp_full,
                                            half_lots, expire_ts, 0, bar_ts))
                    placed = 2
                else:
                    pending.append(Pending(ORDER_BUY_STOP, entry, sl_px, tp_full,
                                            total_lots, expire_ts, 0, bar_ts))
                    placed = 1

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
                    pending.append(Pending(ORDER_SELL_STOP, entry, sl_px, tp_half,
                                            half_lots, expire_ts, 0, bar_ts))
                    pending.append(Pending(ORDER_SELL_STOP, entry, sl_px, tp_full,
                                            half_lots, expire_ts, 0, bar_ts))
                    placed = 2
                else:
                    pending.append(Pending(ORDER_SELL_STOP, entry, sl_px, tp_full,
                                            total_lots, expire_ts, 0, bar_ts))
                    placed = 1

    return placed


def simulate_combined(
    ticks: pd.DataFrame,
    s1_bars: pd.DataFrame,           # M30 bars for S1
    s2_bars: pd.DataFrame,           # H4 bars for S2
    m1_bars: pd.DataFrame,
    cfg_s1: FBOS1Config,
    cfg_s2: FBOS1Config,
    meta: SymbolMeta,
    initial_balance: float = 10_000.0,
) -> CombinedResult:
    """Run S1 + S2 together on shared balance.

    Returns CombinedResult with:
      - summary: total SimResult (combined deals + DD)
      - s1_result, s2_result: per-stream SimResults (sub-deals only)
      - total_np / s1_np / s2_np
    """
    def _to_naive_ns(s: pd.Series) -> np.ndarray:
        if hasattr(s.dt, "tz") and s.dt.tz is not None:
            return s.dt.tz_convert("UTC").dt.tz_localize(None).values.astype("datetime64[ns]")
        return s.values.astype("datetime64[ns]")

    s1_ts = _to_naive_ns(s1_bars["ts"])
    s2_ts = _to_naive_ns(s2_bars["ts"])
    s1_frac_h, s1_frac_l = _compute_fractal_levels(
        s1_bars["high"].values.astype(np.float64),
        s1_bars["low"].values.astype(np.float64),
        cfg_s1.fractal_bars,
    )
    s1_sma = _compute_sma(s1_bars["close"].values.astype(np.float64), cfg_s1.sma_period)
    s2_frac_h, s2_frac_l = _compute_fractal_levels(
        s2_bars["high"].values.astype(np.float64),
        s2_bars["low"].values.astype(np.float64),
        cfg_s2.fractal_bars,
    )
    s2_sma = _compute_sma(s2_bars["close"].values.astype(np.float64), cfg_s2.sma_period)

    m1_times = _to_naive_ns(m1_bars["ts"])
    t_ts = _to_naive_ns(ticks["ts"])
    t_bid = ticks["bid"].values.astype(np.float64)
    t_ask = ticks["ask"].values.astype(np.float64)

    balance = initial_balance
    balance_max = initial_balance
    dd_abs = 0.0

    # Per-stream state
    s1_pending: List[Pending] = []
    s1_positions: List[Position] = []
    s1_deals: List[Deal] = []
    s2_pending: List[Pending] = []
    s2_positions: List[Position] = []
    s2_deals: List[Deal] = []

    def _update_dd(after_balance: float):
        nonlocal balance_max, dd_abs
        if after_balance > balance_max:
            balance_max = after_balance
        cur = balance_max - after_balance
        if cur > dd_abs:
            dd_abs = cur

    def _process_tick_stream(
        ts: pd.Timestamp, bid: float, ask: float,
        pending: List[Pending], positions: List[Position], deals: List[Deal],
    ) -> tuple[List[Pending], List[Position]]:
        nonlocal balance
        # Expire pending
        if pending:
            still = []
            for p in pending:
                if ts < p.expire_ts:
                    still.append(p)
            pending = still

        # Pending fills
        new_positions = []
        still_pending = []
        for p in pending:
            triggered = False
            if p.kind == ORDER_BUY_STOP and ask >= p.price:
                triggered = True; fill = p.price; direction = 1
            elif p.kind == ORDER_SELL_STOP and bid <= p.price:
                triggered = True; fill = p.price; direction = -1
            if triggered:
                new_positions.append(Position(direction, fill, p.sl, p.tp, p.lots))
                deals.append(Deal(ts, 'entry', direction, p.lots, fill, 0.0))
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
                balance += pnl
                deals.append(Deal(ts, 'sl', pos.direction, pos.lots, pos.sl, pnl))
                _update_dd(balance)
            elif hit_tp:
                pnl = _pnl(pos, pos.tp, meta)
                balance += pnl
                deals.append(Deal(ts, 'tp', pos.direction, pos.lots, pos.tp, pnl))
                _update_dd(balance)
            else:
                survivors.append(pos)
        positions = survivors + new_positions
        return pending, positions

    m1_idx = 0
    n_m1 = len(m1_times)

    for k in range(len(t_ts)):
        ts = pd.Timestamp(t_ts[k])
        bid = t_bid[k]
        ask = t_ask[k]

        # Process tick for both streams (S1 first, S2 second — order matters for
        # tied-tick fills but is consistent)
        s1_pending, s1_positions = _process_tick_stream(
            ts, bid, ask, s1_pending, s1_positions, s1_deals
        )
        s2_pending, s2_positions = _process_tick_stream(
            ts, bid, ask, s2_pending, s2_positions, s2_deals
        )

        # Process new M1 bars — entry logic for both streams
        ts_ns = t_ts[k]
        while m1_idx < n_m1 and m1_times[m1_idx] <= ts_ns:
            bar_ts = pd.Timestamp(m1_times[m1_idx])
            m1_idx += 1
            # S1 entry
            _process_stream_entry(
                bar_ts, bid, ask, balance,
                s1_pending, s1_positions,
                cfg_s1, meta, s1_ts, s1_frac_h, s1_frac_l, s1_sma,
            )
            # S2 entry
            _process_stream_entry(
                bar_ts, bid, ask, balance,
                s2_pending, s2_positions,
                cfg_s2, meta, s2_ts, s2_frac_h, s2_frac_l, s2_sma,
            )

    # Build per-stream and combined results
    def _summarise(deals: List[Deal], stream_label: str) -> SimResult:
        tp_count = sum(1 for d in deals if d.kind == 'tp')
        sl_count = sum(1 for d in deals if d.kind == 'sl')
        other_count = sum(1 for d in deals if d.kind == 'other')
        trades = tp_count + sl_count + other_count
        wins = [d.pnl for d in deals if d.kind != 'entry' and d.pnl > 0]
        losses = [d.pnl for d in deals if d.kind != 'entry' and d.pnl < 0]
        pf = sum(wins) / abs(sum(losses)) if losses else float('inf')
        net = sum(d.pnl for d in deals if d.kind != 'entry')
        # Stream-level DD requires its own running balance — use combined for now
        bc = pd.DataFrame([{"ts": d.ts, "pnl": d.pnl} for d in deals if d.kind != 'entry'])
        if not bc.empty:
            bc["balance"] = initial_balance + bc["pnl"].cumsum()
        return SimResult(
            initial_balance=initial_balance,
            final_balance=initial_balance + net,
            net_profit=net,
            trades=trades,
            tp_count=tp_count,
            sl_count=sl_count,
            other_count=other_count,
            max_drawdown=0.0,    # not meaningful per-stream
            max_drawdown_pct=0.0,
            profit_factor=pf,
            balance_curve=bc,
            deals=deals,
        )

    s1_res = _summarise(s1_deals, "S1")
    s2_res = _summarise(s2_deals, "S2")

    # Combined: merge deals chronologically
    all_deals = sorted(s1_deals + s2_deals, key=lambda d: d.ts)
    combined_tp = sum(1 for d in all_deals if d.kind == 'tp')
    combined_sl = sum(1 for d in all_deals if d.kind == 'sl')
    combined_other = sum(1 for d in all_deals if d.kind == 'other')
    combined_trades = combined_tp + combined_sl + combined_other
    wins = [d.pnl for d in all_deals if d.kind != 'entry' and d.pnl > 0]
    losses = [d.pnl for d in all_deals if d.kind != 'entry' and d.pnl < 0]
    combined_pf = sum(wins) / abs(sum(losses)) if losses else float('inf')
    combined_net = balance - initial_balance
    combined_dd_pct = (dd_abs / balance_max * 100.0) if balance_max > 0 else 0.0

    bc = pd.DataFrame([{"ts": d.ts, "pnl": d.pnl} for d in all_deals if d.kind != 'entry'])
    if not bc.empty:
        bc["balance"] = initial_balance + bc["pnl"].cumsum()

    summary = SimResult(
        initial_balance=initial_balance,
        final_balance=balance,
        net_profit=combined_net,
        trades=combined_trades,
        tp_count=combined_tp,
        sl_count=combined_sl,
        other_count=combined_other,
        max_drawdown=dd_abs,
        max_drawdown_pct=combined_dd_pct,
        profit_factor=combined_pf,
        balance_curve=bc,
        deals=all_deals,
    )

    return CombinedResult(
        summary=summary,
        s1_result=s1_res,
        s2_result=s2_res,
        total_np=combined_net,
        s1_np=s1_res.net_profit,
        s2_np=s2_res.net_profit,
        combined_dd_pct=combined_dd_pct,
    )
