"""FVG (Fair Value Gap) stream simulator — port of FBO_FVG_v2.mq5 ProcessFVG.

Mechanics (different from FBO):
- Scans signal-TF bars BACKWARD from most recent for unfilled gaps:
    * Bullish FVG: low[i] > high[i+2]  (gap UP — price moved up too fast,
      expected to retrace down to fill the gap)
    * Bearish FVG: high[i] < low[i+2]  (gap DOWN)
  Filtered by min gap size (in points) and max bar age.
- Up to MaxZones simultaneous zones held; each becomes a pending LIMIT order:
    * Bullish: BuyLimit at top of gap (low[i]); SL = bottom - SL_Buffer;
      TP = entry + risk × RR
    * Bearish: SellLimit at bottom of gap (high[i]); SL = top + SL_Buffer;
      TP = entry - risk × RR
- Every M1 bar:
    1. DELETE all pending FVG orders (re-place semantics — lot updates with balance)
    2. If any open FVG position → skip placement
    3. Re-scan zones, place fresh limits up to MaxZones
- Pending expiry: bar_open + PEB × signal_tf seconds
- HalfTP: optional split into two limits (one at HTP × full_TP, one at full TP)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import numpy as np
import pandas as pd

from .scalper_v1 import (
    Deal, Pending, Position, SimResult, SymbolMeta,
    ORDER_BUY_LIMIT, ORDER_SELL_LIMIT,
    _norm_price, _calc_lots, _pnl,
)


@dataclass
class FVGConfig:
    risk_pct: float = 3.0
    min_size_pts: int = 1600           # _FVG_MinSize
    max_age_bars: int = 150            # _FVG_MaxAge
    max_zones: int = 3                 # _MaxZones
    rr_ratio: float = 5.0              # _RR_Ratio
    sl_buffer_pts: int = 40            # _SL_Buffer
    pending_expire_bars: int = 3       # _PendingExpireBars_F1
    half_tp_ratio: float = 0.0         # _HalfTP_F1
    signal_tf_minutes: int = 60        # H1 default; 240 for H4
    comment: str = "FVG_A"


def _scan_fvg_zones(
    highs: np.ndarray, lows: np.ndarray,
    forming_idx: int, point: float,
    min_size_pts: int, max_age: int, max_zones: int,
) -> list[tuple[float, float, bool, int]]:
    """Scan signal-TF history for unfilled FVG zones.

    Returns list of (top, bottom, is_bullish, bar_idx) tuples, ordered by recency.
    Mirrors EA's ScanFVGZones: scans i from 1 to max_age-2.
    "i bars ago" in EA = forming_idx - i in our index space.
    """
    zones = []
    # i: number of bars ago (i=1 is most recent completed; i=0 is forming)
    for i in range(1, min(max_age - 1, forming_idx)):
        if len(zones) >= max_zones:
            break
        idx_i = forming_idx - i
        idx_i2 = forming_idx - (i + 2)
        if idx_i2 < 0:
            break
        low_i = lows[idx_i]
        high_i = highs[idx_i]
        high_i2 = highs[idx_i2]
        low_i2 = lows[idx_i2]

        # Bullish FVG: low[i] > high[i+2]
        if low_i > high_i2:
            gap_pts = (low_i - high_i2) / point
            if gap_pts >= min_size_pts:
                zones.append((low_i, high_i2, True, idx_i))
                continue   # don't double-count if both directions match (shouldn't happen)

        # Bearish FVG: high[i] < low[i+2]
        if high_i < low_i2:
            gap_pts = (low_i2 - high_i) / point
            if gap_pts >= min_size_pts:
                zones.append((low_i2, high_i, False, idx_i))

    return zones


def simulate(
    ticks: pd.DataFrame,
    signal_bars: pd.DataFrame,
    m1_bars: pd.DataFrame,
    cfg: FVGConfig,
    meta: SymbolMeta,
    initial_balance: float = 10_000.0,
    debug_path: Optional[str] = None,
) -> SimResult:
    """Run FVG stream simulation.

    ticks: [ts, bid, ask] tz-aware UTC
    signal_bars: signal-TF OHLC bars (H1 for FVG S1, H4 for FVG S2)
    m1_bars: M1 OHLC for entry-trigger loop
    """
    def _to_naive_ns(s: pd.Series) -> np.ndarray:
        if hasattr(s.dt, "tz") and s.dt.tz is not None:
            return s.dt.tz_convert("UTC").dt.tz_localize(None).values.astype("datetime64[ns]")
        return s.values.astype("datetime64[ns]")

    sig_highs = signal_bars["high"].values.astype(np.float64)
    sig_lows = signal_bars["low"].values.astype(np.float64)
    sig_times = _to_naive_ns(signal_bars["ts"])
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
    m1_bars_with_position = 0
    m1_bars_placed = 0
    pending_placed_count = 0
    pending_expired_count = 0
    pending_filled_count = 0
    zones_scanned_total = 0

    debug_enabled = debug_path is not None
    placements_log = []
    order_events = []

    def _update_dd():
        nonlocal balance_max, dd_abs
        if balance > balance_max:
            balance_max = balance
        cur = balance_max - balance
        if cur > dd_abs:
            dd_abs = cur

    m1_idx = 0
    n_m1 = len(m1_times)
    n_sig = len(sig_times)

    for k in range(len(t_ts)):
        ts = pd.Timestamp(t_ts[k])
        bid = t_bid[k]
        ask = t_ask[k]

        # Expire pending
        if pending:
            still = []
            before = len(pending)
            for p in pending:
                if ts < p.expire_ts:
                    still.append(p)
                elif debug_enabled:
                    order_events.append({
                        "ts": ts, "event": "expired",
                        "kind": p.kind, "price": p.price,
                    })
            pending = still
            pending_expired_count += (before - len(pending))

        # Pending fills (limit orders): BuyLimit fires when ask <= price; SellLimit when bid >= price
        new_positions = []
        still_pending = []
        for p in pending:
            triggered = False
            if p.kind == ORDER_BUY_LIMIT and ask <= p.price:
                triggered = True
                fill = p.price
                direction = 1
            elif p.kind == ORDER_SELL_LIMIT and bid >= p.price:
                triggered = True
                fill = p.price
                direction = -1
            if triggered:
                new_positions.append(Position(direction, fill, p.sl, p.tp, p.lots))
                deals.append(Deal(ts, 'entry', direction, p.lots, fill, 0.0))
                pending_filled_count += 1
                if debug_enabled:
                    order_events.append({
                        "ts": ts, "event": "filled",
                        "kind": p.kind, "fill_price": fill,
                    })
            else:
                still_pending.append(p)
        pending = still_pending

        # SL/TP on existing positions (not just-filled — same broker semantics)
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

        # New M1 bar?
        ts_ns = t_ts[k]
        while m1_idx < n_m1 and m1_times[m1_idx] <= ts_ns:
            bar_ts = pd.Timestamp(m1_times[m1_idx])
            m1_idx += 1
            m1_bars_seen += 1

            # Delete all pending (re-place semantics)
            if pending:
                if debug_enabled:
                    for p in pending:
                        order_events.append({
                            "ts": bar_ts, "event": "deleted",
                            "kind": p.kind, "price": p.price,
                        })
                pending.clear()

            # Skip placement if any open position
            if positions:
                m1_bars_with_position += 1
                continue

            # Locate forming signal-TF bar
            forming = np.searchsorted(
                sig_times, np.datetime64(bar_ts.to_datetime64()), side='right'
            ) - 1
            if forming < 2:
                continue

            zones = _scan_fvg_zones(
                sig_highs, sig_lows, forming, meta.point,
                cfg.min_size_pts, cfg.max_age_bars, cfg.max_zones,
            )
            zones_scanned_total += len(zones)
            if not zones:
                continue

            # Pending expiry = forming bar open + PEB × signal_tf
            forming_open = pd.Timestamp(sig_times[forming])
            expire_ts = forming_open + pd.Timedelta(
                minutes=cfg.signal_tf_minutes * cfg.pending_expire_bars
            )
            stops_pad = meta.stops_level_pts * meta.point

            placed_this_bar = 0
            for top, bottom, is_bullish, _bar_idx in zones:
                if is_bullish:
                    entry = _norm_price(top, meta)
                    sl_px = _norm_price(bottom - cfg.sl_buffer_pts * meta.point, meta)
                    risk = entry - sl_px
                    tp = _norm_price(entry + risk * cfg.rr_ratio, meta)
                    # EA semantics: skip if entry >= ask (gap above current price)
                    if entry >= ask:
                        continue
                    if (ask - entry) < stops_pad:
                        continue
                    risk_pts = int(risk / meta.point)
                    total_lots = _calc_lots(balance, cfg.risk_pct, risk_pts, meta)
                    if total_lots <= 0:
                        continue
                    half_lots = total_lots
                    if cfg.half_tp_ratio > 0:
                        half_lots = round(total_lots / 2.0 / meta.volume_step) * meta.volume_step
                        if half_lots < meta.volume_min:
                            half_lots = meta.volume_min
                        half_lots = round(half_lots, 2)
                        tp_half = _norm_price(entry + risk * cfg.rr_ratio * cfg.half_tp_ratio, meta)
                        pending.append(Pending(ORDER_BUY_LIMIT, entry, sl_px, tp_half,
                                               half_lots, expire_ts, 0, bar_ts))
                        pending.append(Pending(ORDER_BUY_LIMIT, entry, sl_px, tp,
                                               half_lots, expire_ts, 0, bar_ts))
                        placed_this_bar += 2
                    else:
                        pending.append(Pending(ORDER_BUY_LIMIT, entry, sl_px, tp,
                                               total_lots, expire_ts, 0, bar_ts))
                        placed_this_bar += 1
                else:
                    entry = _norm_price(bottom, meta)
                    sl_px = _norm_price(top + cfg.sl_buffer_pts * meta.point, meta)
                    risk = sl_px - entry
                    tp = _norm_price(entry - risk * cfg.rr_ratio, meta)
                    if entry <= bid:
                        continue
                    if (entry - bid) < stops_pad:
                        continue
                    risk_pts = int(risk / meta.point)
                    total_lots = _calc_lots(balance, cfg.risk_pct, risk_pts, meta)
                    if total_lots <= 0:
                        continue
                    half_lots = total_lots
                    if cfg.half_tp_ratio > 0:
                        half_lots = round(total_lots / 2.0 / meta.volume_step) * meta.volume_step
                        if half_lots < meta.volume_min:
                            half_lots = meta.volume_min
                        half_lots = round(half_lots, 2)
                        tp_half = _norm_price(entry - risk * cfg.rr_ratio * cfg.half_tp_ratio, meta)
                        pending.append(Pending(ORDER_SELL_LIMIT, entry, sl_px, tp_half,
                                               half_lots, expire_ts, 0, bar_ts))
                        pending.append(Pending(ORDER_SELL_LIMIT, entry, sl_px, tp,
                                               half_lots, expire_ts, 0, bar_ts))
                        placed_this_bar += 2
                    else:
                        pending.append(Pending(ORDER_SELL_LIMIT, entry, sl_px, tp,
                                               total_lots, expire_ts, 0, bar_ts))
                        placed_this_bar += 1

            if placed_this_bar > 0:
                m1_bars_placed += 1
                pending_placed_count += placed_this_bar

    # End: metrics
    print(f"  [diag] M1 bars seen={m1_bars_seen:,}  "
          f"with_position={m1_bars_with_position:,}  placed={m1_bars_placed:,}")
    print(f"  [diag] pending placed={pending_placed_count:,}  "
          f"filled={pending_filled_count:,}  expired={pending_expired_count:,}  "
          f"zones_scanned={zones_scanned_total:,}")

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
