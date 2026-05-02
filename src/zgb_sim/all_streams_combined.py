"""All-streams combined simulator: FBO S1 + FBO S2 + FVG S1 + FVG S2 on shared balance.

Each stream independent except they share account balance and DD tracking.
Lot sizing on each new order uses current_balance (post all streams' realized PnL).

Per stream:
  - FBO streams: stop orders at fractal extremes, SMA filter, idle-only re-entry
  - FVG streams: limit orders at gap zones, multi-zone, delete-and-replace per M1 bar
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import numpy as np
import pandas as pd

from .fbo_s1 import FBOS1Config, _compute_fractal_levels, _compute_sma
from .fvg import FVGConfig, _scan_fvg_zones
from .scalper_v1 import (
    Deal, Pending, Position, SimResult, SymbolMeta,
    ORDER_BUY_STOP, ORDER_SELL_STOP, ORDER_BUY_LIMIT, ORDER_SELL_LIMIT,
    _norm_price, _calc_lots, _pnl,
)


@dataclass
class AllStreamsResult:
    summary: SimResult
    per_stream: dict        # name -> dict(np, trades, tp, sl)
    combined_dd_pct: float


def simulate_all_streams(
    ticks: pd.DataFrame,
    fbo_s1_bars: pd.DataFrame,    # M30 by default
    fbo_s2_bars: pd.DataFrame,    # M15 (or whichever)
    fvg_s1_bars: pd.DataFrame,    # H1
    fvg_s2_bars: pd.DataFrame,    # H4
    m1_bars: pd.DataFrame,
    fbo_s1_cfg: Optional[FBOS1Config],
    fbo_s2_cfg: Optional[FBOS1Config],
    fvg_s1_cfg: Optional[FVGConfig],
    fvg_s2_cfg: Optional[FVGConfig],
    meta: SymbolMeta,
    initial_balance: float = 10_000.0,
) -> AllStreamsResult:
    """Run all enabled streams together on a shared balance.

    Pass None for any cfg to disable that stream.
    """
    def _to_naive_ns(s: pd.Series) -> np.ndarray:
        if hasattr(s.dt, "tz") and s.dt.tz is not None:
            return s.dt.tz_convert("UTC").dt.tz_localize(None).values.astype("datetime64[ns]")
        return s.values.astype("datetime64[ns]")

    # FBO precomputes
    streams = {}
    if fbo_s1_cfg is not None:
        bars = fbo_s1_bars
        streams["FBO S1"] = {
            "type": "fbo", "cfg": fbo_s1_cfg,
            "ts": _to_naive_ns(bars["ts"]),
            "highs": bars["high"].values.astype(np.float64),
            "lows": bars["low"].values.astype(np.float64),
            "frac_h": None, "frac_l": None, "sma": None,
            "pending": [], "positions": [], "deals": [],
        }
        s = streams["FBO S1"]
        s["frac_h"], s["frac_l"] = _compute_fractal_levels(s["highs"], s["lows"], fbo_s1_cfg.fractal_bars)
        s["sma"] = _compute_sma(bars["close"].values.astype(np.float64), fbo_s1_cfg.sma_period)
    if fbo_s2_cfg is not None:
        bars = fbo_s2_bars
        streams["FBO S2"] = {
            "type": "fbo", "cfg": fbo_s2_cfg,
            "ts": _to_naive_ns(bars["ts"]),
            "highs": bars["high"].values.astype(np.float64),
            "lows": bars["low"].values.astype(np.float64),
            "frac_h": None, "frac_l": None, "sma": None,
            "pending": [], "positions": [], "deals": [],
        }
        s = streams["FBO S2"]
        s["frac_h"], s["frac_l"] = _compute_fractal_levels(s["highs"], s["lows"], fbo_s2_cfg.fractal_bars)
        s["sma"] = _compute_sma(bars["close"].values.astype(np.float64), fbo_s2_cfg.sma_period)
    if fvg_s1_cfg is not None:
        bars = fvg_s1_bars
        streams["FVG S1"] = {
            "type": "fvg", "cfg": fvg_s1_cfg,
            "ts": _to_naive_ns(bars["ts"]),
            "highs": bars["high"].values.astype(np.float64),
            "lows": bars["low"].values.astype(np.float64),
            "pending": [], "positions": [], "deals": [],
        }
    if fvg_s2_cfg is not None:
        bars = fvg_s2_bars
        streams["FVG S2"] = {
            "type": "fvg", "cfg": fvg_s2_cfg,
            "ts": _to_naive_ns(bars["ts"]),
            "highs": bars["high"].values.astype(np.float64),
            "lows": bars["low"].values.astype(np.float64),
            "pending": [], "positions": [], "deals": [],
        }

    m1_times = _to_naive_ns(m1_bars["ts"])
    t_ts = _to_naive_ns(ticks["ts"])
    t_bid = ticks["bid"].values.astype(np.float64)
    t_ask = ticks["ask"].values.astype(np.float64)

    balance = initial_balance
    balance_max = initial_balance
    dd_abs = 0.0

    def _update_dd():
        nonlocal balance_max, dd_abs
        if balance > balance_max:
            balance_max = balance
        cur = balance_max - balance
        if cur > dd_abs:
            dd_abs = cur

    def _process_tick(stream, ts, bid, ask):
        """Expire / fill / SL-TP for one stream. Mutates stream state + balance."""
        nonlocal balance
        # Expire pending
        if stream["pending"]:
            stream["pending"] = [p for p in stream["pending"] if ts < p.expire_ts]

        # Pending fills
        new_positions = []
        still_pending = []
        for p in stream["pending"]:
            triggered = False
            direction = 0
            fill = 0.0
            if p.kind == ORDER_BUY_STOP and ask >= p.price:
                triggered = True; direction = 1; fill = p.price
            elif p.kind == ORDER_SELL_STOP and bid <= p.price:
                triggered = True; direction = -1; fill = p.price
            elif p.kind == ORDER_BUY_LIMIT and ask <= p.price:
                triggered = True; direction = 1; fill = p.price
            elif p.kind == ORDER_SELL_LIMIT and bid >= p.price:
                triggered = True; direction = -1; fill = p.price
            if triggered:
                new_positions.append(Position(direction, fill, p.sl, p.tp, p.lots))
                stream["deals"].append(Deal(ts, 'entry', direction, p.lots, fill, 0.0))
            else:
                still_pending.append(p)
        stream["pending"] = still_pending

        # SL/TP on existing positions (not just-filled)
        survivors = []
        for pos in stream["positions"]:
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
                stream["deals"].append(Deal(ts, 'sl', pos.direction, pos.lots, pos.sl, pnl))
                _update_dd()
            elif hit_tp:
                pnl = _pnl(pos, pos.tp, meta)
                balance += pnl
                stream["deals"].append(Deal(ts, 'tp', pos.direction, pos.lots, pos.tp, pnl))
                _update_dd()
            else:
                survivors.append(pos)
        stream["positions"] = survivors + new_positions

    def _fbo_entry(stream, bar_ts, bid, ask):
        """FBO entry on M1 bar — idle-stream rule, fractal+SMA, stop orders."""
        nonlocal balance
        if stream["pending"] or stream["positions"]:
            return
        cfg = stream["cfg"]
        forming = np.searchsorted(stream["ts"], np.datetime64(bar_ts.to_datetime64()), side='right') - 1
        last = forming - 1
        if last < cfg.sma_period - 1:
            return
        sma_val = stream["sma"][last]
        if np.isnan(sma_val) or sma_val <= 0:
            return
        want_buy = bid > sma_val
        want_sell = bid < sma_val
        if not (want_buy or want_sell):
            return
        forming_open = pd.Timestamp(stream["ts"][forming]) if forming >= 0 else bar_ts
        expire_ts = forming_open + pd.Timedelta(minutes=cfg.signal_tf_minutes * cfg.pending_expire_bars)
        stops_pad = meta.stops_level_pts * meta.point
        total_lots = _calc_lots(balance, cfg.risk_pct, cfg.stop_loss_pts, meta)
        if total_lots <= 0:
            return
        half_lots = total_lots
        if cfg.half_tp_ratio > 0:
            half_lots = round(total_lots / 2.0 / meta.volume_step) * meta.volume_step
            if half_lots < meta.volume_min:
                half_lots = meta.volume_min
            half_lots = round(half_lots, 2)
        if want_buy:
            fh = stream["frac_h"][last]
            if fh > 0:
                entry = _norm_price(fh, meta)
                min_e = _norm_price(ask + stops_pad, meta)
                if entry < min_e: entry = min_e
                if entry > ask:
                    sl_px = _norm_price(entry - cfg.stop_loss_pts * meta.point, meta)
                    tp_full = _norm_price(entry + cfg.take_profit_pts * meta.point, meta)
                    if cfg.half_tp_ratio > 0:
                        tp_half = _norm_price(entry + cfg.take_profit_pts * cfg.half_tp_ratio * meta.point, meta)
                        stream["pending"].append(Pending(ORDER_BUY_STOP, entry, sl_px, tp_half, half_lots, expire_ts, 0, bar_ts))
                        stream["pending"].append(Pending(ORDER_BUY_STOP, entry, sl_px, tp_full, half_lots, expire_ts, 0, bar_ts))
                    else:
                        stream["pending"].append(Pending(ORDER_BUY_STOP, entry, sl_px, tp_full, total_lots, expire_ts, 0, bar_ts))
        if want_sell:
            fl = stream["frac_l"][last]
            if fl > 0:
                entry = _norm_price(fl, meta)
                max_e = _norm_price(bid - stops_pad, meta)
                if entry > max_e: entry = max_e
                if entry < bid:
                    sl_px = _norm_price(entry + cfg.stop_loss_pts * meta.point, meta)
                    tp_full = _norm_price(entry - cfg.take_profit_pts * meta.point, meta)
                    if cfg.half_tp_ratio > 0:
                        tp_half = _norm_price(entry - cfg.take_profit_pts * cfg.half_tp_ratio * meta.point, meta)
                        stream["pending"].append(Pending(ORDER_SELL_STOP, entry, sl_px, tp_half, half_lots, expire_ts, 0, bar_ts))
                        stream["pending"].append(Pending(ORDER_SELL_STOP, entry, sl_px, tp_full, half_lots, expire_ts, 0, bar_ts))
                    else:
                        stream["pending"].append(Pending(ORDER_SELL_STOP, entry, sl_px, tp_full, total_lots, expire_ts, 0, bar_ts))

    def _fvg_entry(stream, bar_ts, bid, ask):
        """FVG entry on M1 bar — delete pending + scan zones + place limits."""
        nonlocal balance
        cfg = stream["cfg"]
        # Delete all pending each M1 bar
        stream["pending"].clear()
        # Skip if any position
        if stream["positions"]:
            return
        forming = np.searchsorted(stream["ts"], np.datetime64(bar_ts.to_datetime64()), side='right') - 1
        if forming < 2:
            return
        zones = _scan_fvg_zones(
            stream["highs"], stream["lows"], forming, meta.point,
            cfg.min_size_pts, cfg.max_age_bars, cfg.max_zones,
        )
        if not zones:
            return
        forming_open = pd.Timestamp(stream["ts"][forming])
        expire_ts = forming_open + pd.Timedelta(minutes=cfg.signal_tf_minutes * cfg.pending_expire_bars)
        stops_pad = meta.stops_level_pts * meta.point

        for top, bottom, is_bull, _idx in zones:
            if is_bull:
                entry = _norm_price(top, meta)
                sl_px = _norm_price(bottom - cfg.sl_buffer_pts * meta.point, meta)
                risk = entry - sl_px
                tp = _norm_price(entry + risk * cfg.rr_ratio, meta)
                if entry >= ask: continue
                if (ask - entry) < stops_pad: continue
                risk_pts = int(risk / meta.point)
                total_lots = _calc_lots(balance, cfg.risk_pct, risk_pts, meta)
                if total_lots <= 0: continue
                if cfg.half_tp_ratio > 0:
                    half_lots = round(total_lots / 2.0 / meta.volume_step) * meta.volume_step
                    if half_lots < meta.volume_min:
                        half_lots = meta.volume_min
                    half_lots = round(half_lots, 2)
                    tp_half = _norm_price(entry + risk * cfg.rr_ratio * cfg.half_tp_ratio, meta)
                    stream["pending"].append(Pending(ORDER_BUY_LIMIT, entry, sl_px, tp_half, half_lots, expire_ts, 0, bar_ts))
                    stream["pending"].append(Pending(ORDER_BUY_LIMIT, entry, sl_px, tp, half_lots, expire_ts, 0, bar_ts))
                else:
                    stream["pending"].append(Pending(ORDER_BUY_LIMIT, entry, sl_px, tp, total_lots, expire_ts, 0, bar_ts))
            else:
                entry = _norm_price(bottom, meta)
                sl_px = _norm_price(top + cfg.sl_buffer_pts * meta.point, meta)
                risk = sl_px - entry
                tp = _norm_price(entry - risk * cfg.rr_ratio, meta)
                if entry <= bid: continue
                if (entry - bid) < stops_pad: continue
                risk_pts = int(risk / meta.point)
                total_lots = _calc_lots(balance, cfg.risk_pct, risk_pts, meta)
                if total_lots <= 0: continue
                if cfg.half_tp_ratio > 0:
                    half_lots = round(total_lots / 2.0 / meta.volume_step) * meta.volume_step
                    if half_lots < meta.volume_min:
                        half_lots = meta.volume_min
                    half_lots = round(half_lots, 2)
                    tp_half = _norm_price(entry - risk * cfg.rr_ratio * cfg.half_tp_ratio, meta)
                    stream["pending"].append(Pending(ORDER_SELL_LIMIT, entry, sl_px, tp_half, half_lots, expire_ts, 0, bar_ts))
                    stream["pending"].append(Pending(ORDER_SELL_LIMIT, entry, sl_px, tp, half_lots, expire_ts, 0, bar_ts))
                else:
                    stream["pending"].append(Pending(ORDER_SELL_LIMIT, entry, sl_px, tp, total_lots, expire_ts, 0, bar_ts))

    m1_idx = 0
    n_m1 = len(m1_times)

    for k in range(len(t_ts)):
        ts = pd.Timestamp(t_ts[k])
        bid = t_bid[k]
        ask = t_ask[k]

        # Per-tick processing for all streams
        for stream in streams.values():
            _process_tick(stream, ts, bid, ask)

        # M1 bars — entry logic for all streams
        ts_ns = t_ts[k]
        while m1_idx < n_m1 and m1_times[m1_idx] <= ts_ns:
            bar_ts = pd.Timestamp(m1_times[m1_idx])
            m1_idx += 1
            for name, stream in streams.items():
                if stream["type"] == "fbo":
                    _fbo_entry(stream, bar_ts, bid, ask)
                else:
                    _fvg_entry(stream, bar_ts, bid, ask)

    # Per-stream summaries
    per_stream = {}
    all_deals = []
    for name, stream in streams.items():
        deals = stream["deals"]
        all_deals.extend(deals)
        tp = sum(1 for d in deals if d.kind == 'tp')
        sl = sum(1 for d in deals if d.kind == 'sl')
        other = sum(1 for d in deals if d.kind == 'other')
        net = sum(d.pnl for d in deals if d.kind != 'entry')
        per_stream[name] = {
            "np": net, "trades": tp + sl + other, "tp": tp, "sl": sl, "other": other,
        }

    all_deals.sort(key=lambda d: d.ts)
    combined_tp = sum(1 for d in all_deals if d.kind == 'tp')
    combined_sl = sum(1 for d in all_deals if d.kind == 'sl')
    combined_other = sum(1 for d in all_deals if d.kind == 'other')
    combined_trades = combined_tp + combined_sl + combined_other
    wins = [d.pnl for d in all_deals if d.kind != 'entry' and d.pnl > 0]
    losses = [d.pnl for d in all_deals if d.kind != 'entry' and d.pnl < 0]
    pf = sum(wins) / abs(sum(losses)) if losses else float('inf')
    net = balance - initial_balance
    dd_pct = (dd_abs / balance_max * 100.0) if balance_max > 0 else 0.0

    bc = pd.DataFrame([{"ts": d.ts, "pnl": d.pnl} for d in all_deals if d.kind != 'entry'])
    if not bc.empty:
        bc["balance"] = initial_balance + bc["pnl"].cumsum()

    summary = SimResult(
        initial_balance=initial_balance,
        final_balance=balance,
        net_profit=net,
        trades=combined_trades,
        tp_count=combined_tp,
        sl_count=combined_sl,
        other_count=combined_other,
        max_drawdown=dd_abs,
        max_drawdown_pct=dd_pct,
        profit_factor=pf,
        balance_curve=bc,
        deals=all_deals,
    )

    return AllStreamsResult(
        summary=summary,
        per_stream=per_stream,
        combined_dd_pct=dd_pct,
    )
