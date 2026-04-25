"""Multi-stream sim — runs N S1Configs concurrently on a shared account balance.

Each stream has its own pending/positions/realized_today (so each daily target
fires only on its own deals), but they share the same balance for lot sizing
and equity-drawdown tracking.

Slow Python path (correctness over speed; used for one-shot combined ET).
"""
from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd

from .scalper_v1 import (
    S1Config, SymbolMeta, SimResult, Deal, Position, Pending,
    ORDER_BUY_STOP, ORDER_SELL_STOP, ORDER_BUY_LIMIT, ORDER_SELL_LIMIT,
    _norm_price, _calc_lots, _pnl, _in_hours,
)


def simulate_combined(
    ticks: pd.DataFrame,
    m5_bars: pd.DataFrame,
    m1_bars: pd.DataFrame,
    cfgs: List[S1Config],
    meta: SymbolMeta,
    initial_balance: float = 100.0,
    stream_labels: List[str] | None = None,
) -> SimResult:
    """Run multiple streams sharing one account balance."""
    n_streams = len(cfgs)
    if stream_labels is None:
        stream_labels = [f"S{i+1}" for i in range(n_streams)]

    # Strip tz for fast comparison
    def _to_naive_ns(s):
        if hasattr(s.dt, "tz") and s.dt.tz is not None:
            return s.dt.tz_convert("UTC").dt.tz_localize(None).values.astype("datetime64[ns]")
        return s.values.astype("datetime64[ns]")

    m5_highs = m5_bars["high"].values.astype(np.float64)
    m5_lows  = m5_bars["low"].values.astype(np.float64)
    m5_times = _to_naive_ns(m5_bars["ts"])

    # Per-stream pre-computed Donchian (each stream may use different donchian_bars)
    streams = []
    for i, cfg in enumerate(cfgs):
        n = cfg.donchian_bars
        dh = np.full(len(m5_bars), np.nan)
        dl = np.full(len(m5_bars), np.nan)
        for j in range(n, len(m5_bars)):
            dh[j] = m5_highs[j-n:j].max()
            dl[j] = m5_lows[j-n:j].min()
        streams.append({
            "cfg": cfg,
            "label": stream_labels[i],
            "donch_high": dh,
            "donch_low": dl,
            "pending": [],
            "positions": [],
            "realized_today": 0.0,
            "m1_idx": 0,
        })

    m1_times = _to_naive_ns(m1_bars["ts"])
    t_ts  = _to_naive_ns(ticks["ts"])
    t_bid = ticks["bid"].values.astype(np.float64)
    t_ask = ticks["ask"].values.astype(np.float64)

    # Shared balance state
    balance = initial_balance
    balance_max = initial_balance
    dd_abs = 0.0
    session_day = None
    balance_at_day_start = initial_balance

    deals: List[Deal] = []
    # Aggregate counts (across all streams)
    tp_count = sl_count = other_count = 0
    # Per-stream counts (for reporting)
    per_stream_counts = {s["label"]: {"tp": 0, "sl": 0, "other": 0, "trades": 0, "pnl": 0.0}
                        for s in streams}

    def _update_dd():
        nonlocal balance_max, dd_abs
        if balance > balance_max: balance_max = balance
        cur = balance_max - balance
        if cur > dd_abs: dd_abs = cur

    def _close_stream_all(s, k):
        nonlocal balance
        if s["positions"]:
            cur_bid = t_bid[k]; cur_ask = t_ask[k]
            ts_now = pd.Timestamp(t_ts[k])
            for p in s["positions"]:
                close_px = cur_bid if p.direction == 1 else cur_ask
                pnl = _pnl(p, close_px, meta)
                balance += pnl
                s["realized_today"] += pnl
                deals.append(Deal(ts_now, "other", p.direction, p.lots, close_px, pnl))
                per_stream_counts[s["label"]]["other"] += 1
                per_stream_counts[s["label"]]["trades"] += 1
                per_stream_counts[s["label"]]["pnl"] += pnl
                _update_dd()
            s["positions"].clear()
        s["pending"].clear()

    n_ticks = len(t_ts)
    for k in range(n_ticks):
        ts = pd.Timestamp(t_ts[k])
        bid = t_bid[k]; ask = t_ask[k]

        day = ts.date()
        if day != session_day:
            session_day = day
            balance_at_day_start = balance
            for s in streams:
                s["realized_today"] = 0.0

        # Per-stream daily target/loss check + lockout
        any_locked = []
        for i, s in enumerate(streams):
            cfg = s["cfg"]
            unrealized = sum(_pnl(p, bid if p.direction == 1 else ask, meta)
                             for p in s["positions"])
            today_pnl = s["realized_today"] + unrealized
            tgt = balance_at_day_start * cfg.daily_target_pct / 100.0
            loss = balance_at_day_start * cfg.daily_loss_pct / 100.0
            locked = ((cfg.daily_target_pct > 0 and today_pnl >= tgt) or
                      (cfg.daily_loss_pct > 0 and today_pnl <= -loss))
            if locked:
                _close_stream_all(s, k)
                # Advance this stream's m1_idx past current ts
                ts_ns = t_ts[k]
                while s["m1_idx"] < len(m1_times) and m1_times[s["m1_idx"]] <= ts_ns:
                    s["m1_idx"] += 1
                any_locked.append(i)

        # For non-locked streams: pending expire / triggers / SL/TP / M1 processing
        for i, s in enumerate(streams):
            if i in any_locked:
                continue
            cfg = s["cfg"]
            ts_ns = t_ts[k]

            # Out of hours: clear pending
            in_hrs = _in_hours(ts, cfg.start_hour, cfg.end_hour, cfg.block_fri_pm)
            if not in_hrs and s["pending"]:
                s["pending"].clear()

            # Expire pending
            if s["pending"]:
                s["pending"] = [p for p in s["pending"] if ts < p.expire_ts]

            # Pending triggers — collect new positions
            new_positions = []
            still_pending = []
            for p in s["pending"]:
                triggered = False; direction = 0; fill = 0.0
                if p.kind == ORDER_BUY_STOP and ask >= p.price:
                    triggered = True; direction = 1; fill = p.price
                elif p.kind == ORDER_SELL_STOP and bid <= p.price:
                    triggered = True; direction = -1; fill = p.price
                elif p.kind == ORDER_SELL_LIMIT and bid >= p.price:
                    triggered = True; direction = -1; fill = p.price
                elif p.kind == ORDER_BUY_LIMIT and ask <= p.price:
                    triggered = True; direction = 1; fill = p.price
                if triggered:
                    new_positions.append(Position(direction, fill, p.sl, p.tp, p.lots))
                    deals.append(Deal(ts, "entry", direction, p.lots, fill, 0.0))
                else:
                    still_pending.append(p)
            s["pending"] = still_pending

            # SL/TP on existing positions (not new ones — defer to next tick)
            survivors = []
            for pos in s["positions"]:
                hit_sl = hit_tp = False
                if pos.direction == 1:
                    if bid <= pos.sl: hit_sl = True
                    elif bid >= pos.tp: hit_tp = True
                else:
                    if ask >= pos.sl: hit_sl = True
                    elif ask <= pos.tp: hit_tp = True

                if hit_sl:
                    pnl = _pnl(pos, pos.sl, meta)
                    balance += pnl; s["realized_today"] += pnl
                    deals.append(Deal(ts, "sl", pos.direction, pos.lots, pos.sl, pnl))
                    per_stream_counts[s["label"]]["sl"] += 1
                    per_stream_counts[s["label"]]["trades"] += 1
                    per_stream_counts[s["label"]]["pnl"] += pnl
                    _update_dd()
                elif hit_tp:
                    pnl = _pnl(pos, pos.tp, meta)
                    balance += pnl; s["realized_today"] += pnl
                    deals.append(Deal(ts, "tp", pos.direction, pos.lots, pos.tp, pnl))
                    per_stream_counts[s["label"]]["tp"] += 1
                    per_stream_counts[s["label"]]["trades"] += 1
                    per_stream_counts[s["label"]]["pnl"] += pnl
                    _update_dd()
                else:
                    survivors.append(pos)
            s["positions"] = survivors + new_positions

            # New M1 bars
            while s["m1_idx"] < len(m1_times) and m1_times[s["m1_idx"]] <= ts_ns:
                bar_ts = pd.Timestamp(m1_times[s["m1_idx"]])
                s["m1_idx"] += 1
                if not _in_hours(bar_ts, cfg.start_hour, cfg.end_hour, cfg.block_fri_pm):
                    continue
                if s["pending"]:
                    continue

                last_m5 = np.searchsorted(m5_times, np.datetime64(bar_ts.to_datetime64()), side="right") - 1
                if last_m5 < cfg.donchian_bars:
                    continue
                dh = s["donch_high"][last_m5]
                dl = s["donch_low"][last_m5]
                if np.isnan(dh) or np.isnan(dl):
                    continue

                expire_ts = bar_ts + pd.Timedelta(minutes=5 * cfg.pending_expire_bars)
                stops_pad = meta.stops_level_pts * meta.point

                # Lot size based on CURRENT shared balance
                total_lots = _calc_lots(balance, cfg.risk_pct, cfg.stop_loss_pts, meta)
                if total_lots <= 0: continue
                half_lots = total_lots
                if cfg.half_tp_ratio > 0:
                    half_lots = round(total_lots / 2.0 / meta.volume_step) * meta.volume_step
                    if half_lots < meta.volume_min: half_lots = meta.volume_min
                    half_lots = round(half_lots, 2)

                # BuyStop at Donchian high
                high_brk = _norm_price(dh, meta)
                min_buy = _norm_price(ask + stops_pad, meta)
                if high_brk < min_buy: high_brk = min_buy
                if high_brk > ask:
                    sl = _norm_price(high_brk - cfg.stop_loss_pts * meta.point, meta)
                    tp = _norm_price(high_brk + cfg.take_profit_pts * meta.point, meta)
                    if cfg.half_tp_ratio > 0:
                        tp_half = _norm_price(high_brk + cfg.take_profit_pts * cfg.half_tp_ratio * meta.point, meta)
                        s["pending"].append(Pending(ORDER_BUY_STOP, high_brk, sl, tp_half, half_lots, expire_ts))
                        s["pending"].append(Pending(ORDER_BUY_STOP, high_brk, sl, tp, half_lots, expire_ts))
                    else:
                        s["pending"].append(Pending(ORDER_BUY_STOP, high_brk, sl, tp, total_lots, expire_ts))

                # SellStop at Donchian low
                low_brk = _norm_price(dl, meta)
                max_sell = _norm_price(bid - stops_pad, meta)
                if low_brk > max_sell: low_brk = max_sell
                if low_brk < bid:
                    sl = _norm_price(low_brk + cfg.stop_loss_pts * meta.point, meta)
                    tp = _norm_price(low_brk - cfg.take_profit_pts * meta.point, meta)
                    if cfg.half_tp_ratio > 0:
                        tp_half = _norm_price(low_brk - cfg.take_profit_pts * cfg.half_tp_ratio * meta.point, meta)
                        s["pending"].append(Pending(ORDER_SELL_STOP, low_brk, sl, tp_half, half_lots, expire_ts))
                        s["pending"].append(Pending(ORDER_SELL_STOP, low_brk, sl, tp, half_lots, expire_ts))
                    else:
                        s["pending"].append(Pending(ORDER_SELL_STOP, low_brk, sl, tp, total_lots, expire_ts))

    # Aggregate exit counts
    for s_lbl, c in per_stream_counts.items():
        tp_count += c["tp"]; sl_count += c["sl"]; other_count += c["other"]

    trades = tp_count + sl_count + other_count
    wins = sum(d.pnl for d in deals if d.kind != "entry" and d.pnl > 0)
    losses = sum(d.pnl for d in deals if d.kind != "entry" and d.pnl < 0)
    pf = wins / abs(losses) if losses != 0 else (float("inf") if wins > 0 else 0.0)
    net = balance - initial_balance
    dd_pct = (dd_abs / balance_max * 100.0) if balance_max > 0 else 0.0

    bc = pd.DataFrame([{"ts": d.ts, "pnl": d.pnl} for d in deals if d.kind != "entry"])
    if not bc.empty:
        bc["balance"] = initial_balance + bc["pnl"].cumsum()

    print(f"  [combined] streams: {len(streams)}")
    for s in streams:
        c = per_stream_counts[s["label"]]
        print(f"    {s['label']}: trades={c['trades']}  TP={c['tp']}  SL={c['sl']}  Other={c['other']}  PnL=${c['pnl']:+,.2f}")

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
