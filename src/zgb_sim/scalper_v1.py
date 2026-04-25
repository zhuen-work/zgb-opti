"""Scalper_v1 simulator — matches EA logic 1:1 (S1 stream only, S2 removed).

Semantics (verified against EA source):
- Processes entries on each new M1 bar when in trading hours.
- Places BuyStop at Donchian high + SellStop at Donchian low (last N M5 bars excluding current).
- HalfTP_Ratio > 0 splits order into two halves, half with partial TP, half with full TP.
- Pending orders expire after _PendingExpireBars * M5 bars.
- SL/TP checked tick-by-tick once a position opens.
- Daily target/loss checked on each tick — if hit, programmatic close of all positions + pending orders
  (these close events have no 'sl'/'tp' label, carry the stream comment).
- Balance updates only on closed trades. Lot size = balance × risk% / (SL points × tick_value / tick_size).
- Session day = UTC calendar day (matches EA's DayStart).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import List, Optional

import numpy as np
import pandas as pd


# ---------- Config ----------

@dataclass
class S1Config:
    risk_pct: float = 1.0
    donchian_bars: int = 20
    take_profit_pts: int = 200
    stop_loss_pts: int = 50
    half_tp_ratio: float = 0.6
    pending_expire_bars: int = 2
    start_hour: int = 14
    end_hour: int = 22
    daily_target_pct: float = 3.0
    daily_loss_pct: float = 6.0
    block_fri_pm: bool = True
    hedge_mode: bool = False
    comment: str = "DT818_S1"


@dataclass
class SymbolMeta:
    point: float = 0.01
    tick_size: float = 0.01
    tick_value: float = 1.0
    stops_level_pts: int = 20
    volume_min: float = 0.01
    volume_max: float = 500.0
    volume_step: float = 0.01
    digits: int = 2


# ---------- Orders / Positions ----------

ORDER_BUY_STOP = "buy_stop"
ORDER_SELL_STOP = "sell_stop"
ORDER_BUY_LIMIT = "buy_limit"
ORDER_SELL_LIMIT = "sell_limit"


@dataclass
class Pending:
    kind: str            # buy_stop / sell_stop / buy_limit / sell_limit
    price: float
    sl: float
    tp: float
    lots: float
    expire_ts: pd.Timestamp
    oid: int = 0            # sim-side order id for debug matching
    placed_ts: pd.Timestamp = None


@dataclass
class Position:
    direction: int       # +1 long, -1 short
    entry_price: float
    sl: float
    tp: float
    lots: float


@dataclass
class Deal:
    ts: pd.Timestamp
    kind: str            # 'entry', 'tp', 'sl', 'other'
    direction: int
    lots: float
    price: float
    pnl: float


# ---------- Simulator ----------

def _norm_price(price: float, meta: SymbolMeta) -> float:
    return round(round(price / meta.tick_size) * meta.tick_size, meta.digits)


def _calc_lots(balance: float, risk_pct: float, sl_pts: int, meta: SymbolMeta) -> float:
    risk_money = balance * risk_pct / 100.0
    sl_money = (sl_pts * meta.point / meta.tick_size) * meta.tick_value   # per 1 lot
    if sl_money <= 0: return 0.0
    lots = risk_money / sl_money
    lots = max(meta.volume_min, min(meta.volume_max, lots))
    lots = round(lots / meta.volume_step) * meta.volume_step
    return round(lots, 2)


def _pnl(pos: Position, close_price: float, meta: SymbolMeta) -> float:
    # P&L = direction × (close - entry) × lots × tick_value / tick_size
    diff = (close_price - pos.entry_price) * pos.direction
    return diff * pos.lots * meta.tick_value / meta.tick_size


def _in_hours(ts: pd.Timestamp, start_h: int, end_h: int, block_fri: bool) -> bool:
    # block_fri parameter kept for API compat but no-op (dead code in original EA).
    if ts.weekday() in (5, 6): return False
    return start_h <= ts.hour < end_h


@dataclass
class SimResult:
    initial_balance: float
    final_balance: float
    net_profit: float
    trades: int
    tp_count: int
    sl_count: int
    other_count: int
    max_drawdown: float
    max_drawdown_pct: float
    profit_factor: float
    balance_curve: pd.DataFrame
    deals: List[Deal]

    def summary(self) -> str:
        roi = (self.net_profit / self.initial_balance * 100.0
               if self.initial_balance > 0 else 0.0)
        return (
            f"NP={self.net_profit:+,.2f}  ROI={roi:+.1f}%  "
            f"PF={self.profit_factor:.2f}  "
            f"DD={self.max_drawdown_pct:.1f}%  Trades={self.trades}  "
            f"TP/SL/Other={self.tp_count}/{self.sl_count}/{self.other_count}"
        )


def simulate(
    ticks: pd.DataFrame,
    m5_bars: pd.DataFrame,
    m1_bars: pd.DataFrame,
    cfg: S1Config,
    meta: SymbolMeta,
    initial_balance: float = 100.0,
    debug_path: Optional[str] = None,
) -> SimResult:
    """Run the simulation.

    ticks: columns [ts, bid, ask] — tz-aware UTC
    m5_bars / m1_bars: [ts, open, high, low, close] — bar open time; tz-aware UTC
    """
    # Strip tz — we work in naive UTC internally for fast np comparisons
    def _to_naive_ns(s: pd.Series) -> np.ndarray:
        if hasattr(s.dt, "tz") and s.dt.tz is not None:
            return s.dt.tz_convert("UTC").dt.tz_localize(None).values.astype("datetime64[ns]")
        return s.values.astype("datetime64[ns]")

    # Pre-compute Donchian high/low from M5 bars: for each bar i, look back N bars [i-N .. i-1]
    n = cfg.donchian_bars
    m5_highs = m5_bars["high"].values.astype(np.float64)
    m5_lows  = m5_bars["low"].values.astype(np.float64)
    m5_times = _to_naive_ns(m5_bars["ts"])

    # Donchian per M5-bar: high = max(h[i-n:i]), low = min(l[i-n:i]) — uses COMPLETED bars only
    donch_high = np.full(len(m5_bars), np.nan)
    donch_low  = np.full(len(m5_bars), np.nan)
    for i in range(n, len(m5_bars)):
        donch_high[i] = m5_highs[i-n:i].max()
        donch_low[i]  = m5_lows[i-n:i].min()

    # M1 bar timestamps (for new-bar processing) — naive UTC
    m1_times = _to_naive_ns(m1_bars["ts"])

    # Ticks array (naive UTC)
    t_ts  = _to_naive_ns(ticks["ts"])
    t_bid = ticks["bid"].values.astype(np.float64)
    t_ask = ticks["ask"].values.astype(np.float64)

    # State
    balance = initial_balance
    session_day: Optional[date] = None
    balance_at_day_start = initial_balance
    realized_today = 0.0
    pending: List[Pending] = []
    positions: List[Position] = []
    deals: List[Deal] = []
    balance_pts = []           # (ts, balance) — after each close
    balance_max = initial_balance
    dd_abs = 0.0

    # Diagnostics
    m1_bars_seen = 0
    m1_bars_in_hours = 0
    m1_bars_skipped_pending = 0
    m1_bars_placed_orders = 0
    pending_placed_count = 0
    pending_expired_count = 0
    pending_filled_count = 0

    # Debug logs (optional)
    debug_enabled = debug_path is not None
    placements_log = []  # per M1 bar placement: ts, donch_high/low, ask/bid, break_buy, break_sell
    order_events = []    # per pending event: placed/filled/expired/cancelled w/ details
    _order_seq = [0]
    def _next_oid():
        _order_seq[0] += 1
        return _order_seq[0]

    # Indexes
    m1_idx = 0       # next M1 bar to process
    # All arrays already naive UTC datetime64[ns]
    m1_ts_np = m1_times
    m5_ts_np = m5_times

    def _close_all():
        nonlocal balance, realized_today, balance_max, dd_abs
        # Close open positions at current bid/ask (last tick)
        if positions:
            cur_bid = t_bid[k]
            cur_ask = t_ask[k]
            ts_now = pd.Timestamp(t_ts[k])
            for p in positions:
                close_px = cur_bid if p.direction == 1 else cur_ask
                pnl = _pnl(p, close_px, meta)
                balance += pnl
                realized_today += pnl
                deals.append(Deal(ts_now, 'other', p.direction, p.lots, close_px, pnl))
                _update_dd()
            positions.clear()
        pending.clear()

    def _update_dd():
        nonlocal balance_max, dd_abs
        if balance > balance_max: balance_max = balance
        curr_dd = balance_max - balance
        if curr_dd > dd_abs: dd_abs = curr_dd

    # Main tick loop
    for k in range(len(t_ts)):
        ts = pd.Timestamp(t_ts[k])   # naive UTC
        bid = t_bid[k]
        ask = t_ask[k]

        # Session rollover (UTC day)
        day = ts.date()
        if day != session_day:
            session_day = day
            balance_at_day_start = balance
            realized_today = 0.0

        # Daily target/loss check (on every tick).
        # IMPORTANT: when target/loss fires we still need to advance m1_idx through
        # any M1 bars that are <= current tick, otherwise they queue up and get
        # processed at next day's first tick (after realized_today resets),
        # producing phantom placements with stale bar_ts but new-day data.
        ts_ns = t_ts[k]
        unrealized = sum(_pnl(p, bid if p.direction == 1 else ask, meta) for p in positions)
        today_pnl = realized_today + unrealized
        target_locked = False
        if cfg.daily_target_pct > 0 and today_pnl >= balance_at_day_start * cfg.daily_target_pct / 100:
            _close_all()
            target_locked = True
        elif cfg.daily_loss_pct > 0 and today_pnl <= -balance_at_day_start * cfg.daily_loss_pct / 100:
            _close_all()
            target_locked = True
        if target_locked:
            while m1_idx < len(m1_ts_np) and m1_ts_np[m1_idx] <= ts_ns:
                m1_idx += 1
                m1_bars_seen += 1
            continue

        # Out of trading hours → delete pending (but keep positions to SL/TP)
        in_hrs = _in_hours(ts, cfg.start_hour, cfg.end_hour, cfg.block_fri_pm)
        if not in_hrs and pending:
            pending.clear()

        # Expire pending orders past expire_ts
        if pending:
            before = len(pending)
            still_after_expire = []
            for p in pending:
                if ts < p.expire_ts:
                    still_after_expire.append(p)
                else:
                    if debug_enabled:
                        order_events.append({
                            "ts": ts, "oid": p.oid, "event": "expired",
                            "kind": p.kind, "price": p.price,
                        })
            pending = still_after_expire
            pending_expired_count += (before - len(pending))

        # Check pending triggers (on this tick's bid/ask)
        new_positions = []
        still_pending = []
        for p in pending:
            triggered = False
            if p.kind == ORDER_BUY_STOP and ask >= p.price:
                triggered = True; fill = p.price; direction = 1
            elif p.kind == ORDER_SELL_STOP and bid <= p.price:
                triggered = True; fill = p.price; direction = -1
            elif p.kind == ORDER_SELL_LIMIT and bid >= p.price:
                triggered = True; fill = p.price; direction = -1
            elif p.kind == ORDER_BUY_LIMIT and ask <= p.price:
                triggered = True; fill = p.price; direction = 1
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

        # Check SL/TP only on positions that existed BEFORE this tick's fills.
        # Real broker semantics: SL/TP protective orders are server-side and trigger
        # on subsequent ticks — not on the same tick the entry filled. Including a
        # just-opened position in same-tick SL/TP can produce spurious losses during
        # wide-spread ticks (e.g. bid-ask spread > SL distance).
        survivors = []
        for pos in positions:
            hit_sl = hit_tp = False
            if pos.direction == 1:
                # Long: SL if bid <= sl, TP if bid >= tp
                if bid <= pos.sl: hit_sl = True
                elif bid >= pos.tp: hit_tp = True
            else:
                if ask >= pos.sl: hit_sl = True
                elif ask <= pos.tp: hit_tp = True

            if hit_sl:
                pnl = _pnl(pos, pos.sl, meta)
                balance += pnl; realized_today += pnl
                deals.append(Deal(ts, 'sl', pos.direction, pos.lots, pos.sl, pnl))
                _update_dd()
            elif hit_tp:
                pnl = _pnl(pos, pos.tp, meta)
                balance += pnl; realized_today += pnl
                deals.append(Deal(ts, 'tp', pos.direction, pos.lots, pos.tp, pnl))
                _update_dd()
            else:
                survivors.append(pos)
        # Add newly-filled positions after SL/TP check — they become active next tick
        positions = survivors + new_positions

        # New M1 bar? Process entry logic
        ts_ns = t_ts[k]
        while m1_idx < len(m1_ts_np) and m1_ts_np[m1_idx] <= ts_ns:
            bar_ts = pd.Timestamp(m1_ts_np[m1_idx])   # naive UTC
            m1_idx += 1
            m1_bars_seen += 1
            # Trading hours?
            if not _in_hours(bar_ts, cfg.start_hour, cfg.end_hour, cfg.block_fri_pm):
                continue
            m1_bars_in_hours += 1
            # Skip if pending exists (EA rule: no re-entry while pending)
            if pending:
                m1_bars_skipped_pending += 1
                continue
            m1_bars_placed_orders += 1

            # Find the last COMPLETED M5 bar before this M1 (bar opened strictly before bar_ts)
            last_m5 = np.searchsorted(m5_ts_np, np.datetime64(bar_ts.to_datetime64()), side='right') - 1
            if last_m5 < n:
                continue
            dh = donch_high[last_m5]
            dl = donch_low[last_m5]
            if np.isnan(dh) or np.isnan(dl):
                continue

            # Use the entry-time ask/bid (current tick)
            # Place pending orders
            expire_ts = bar_ts + pd.Timedelta(minutes=5 * cfg.pending_expire_bars)
            stops_pad = meta.stops_level_pts * meta.point

            # Lot sizing based on current balance
            total_lots = _calc_lots(balance, cfg.risk_pct, cfg.stop_loss_pts, meta)
            if total_lots <= 0: continue
            half_lots = total_lots
            if cfg.half_tp_ratio > 0:
                half_lots = round(total_lots / 2.0 / meta.volume_step) * meta.volume_step
                if half_lots < meta.volume_min: half_lots = meta.volume_min
                half_lots = round(half_lots, 2)

            placed_this_bar = 0

            def _add_pending(kind, price, sl_px, tp_px, lots):
                oid = _next_oid() if debug_enabled else 0
                pending.append(Pending(kind, price, sl_px, tp_px, lots, expire_ts, oid, bar_ts))
                if debug_enabled:
                    order_events.append({
                        "ts": bar_ts, "oid": oid, "event": "placed",
                        "kind": kind, "price": price, "sl": sl_px, "tp": tp_px, "lots": lots,
                        "expire_ts": expire_ts,
                    })

            # BuyStop at Donchian high
            high_brk = _norm_price(dh, meta)
            min_buy = _norm_price(ask + stops_pad, meta)
            if high_brk < min_buy: high_brk = min_buy
            bar_placement = {
                "bar_ts": bar_ts, "last_m5_ts": pd.Timestamp(m5_ts_np[last_m5]),
                "donch_high": dh, "donch_low": dl, "ask": ask, "bid": bid,
                "balance": balance, "total_lots": total_lots, "half_lots": half_lots,
            }
            if high_brk > ask:
                sl = _norm_price(high_brk - cfg.stop_loss_pts * meta.point, meta)
                tp = _norm_price(high_brk + cfg.take_profit_pts * meta.point, meta)
                if cfg.half_tp_ratio > 0:
                    tp_half = _norm_price(high_brk + cfg.take_profit_pts * cfg.half_tp_ratio * meta.point, meta)
                    _add_pending(ORDER_BUY_STOP, high_brk, sl, tp_half, half_lots)
                    _add_pending(ORDER_BUY_STOP, high_brk, sl, tp, half_lots)
                    placed_this_bar += 2
                else:
                    _add_pending(ORDER_BUY_STOP, high_brk, sl, tp, total_lots)
                    placed_this_bar += 1

                if cfg.hedge_mode:
                    min_sell_lim = _norm_price(bid + stops_pad, meta)
                    if high_brk >= min_sell_lim:
                        slH = _norm_price(high_brk + cfg.stop_loss_pts * meta.point, meta)
                        tpH = _norm_price(high_brk - cfg.take_profit_pts * meta.point, meta)
                        if cfg.half_tp_ratio > 0:
                            tp_halfH = _norm_price(high_brk - cfg.take_profit_pts * cfg.half_tp_ratio * meta.point, meta)
                            _add_pending(ORDER_SELL_LIMIT, high_brk, slH, tp_halfH, half_lots)
                            _add_pending(ORDER_SELL_LIMIT, high_brk, slH, tpH, half_lots)
                        else:
                            _add_pending(ORDER_SELL_LIMIT, high_brk, slH, tpH, total_lots)

            # SellStop at Donchian low
            low_brk = _norm_price(dl, meta)
            max_sell = _norm_price(bid - stops_pad, meta)
            if low_brk > max_sell: low_brk = max_sell
            if low_brk < bid:
                sl = _norm_price(low_brk + cfg.stop_loss_pts * meta.point, meta)
                tp = _norm_price(low_brk - cfg.take_profit_pts * meta.point, meta)
                if cfg.half_tp_ratio > 0:
                    tp_half = _norm_price(low_brk - cfg.take_profit_pts * cfg.half_tp_ratio * meta.point, meta)
                    _add_pending(ORDER_SELL_STOP, low_brk, sl, tp_half, half_lots)
                    _add_pending(ORDER_SELL_STOP, low_brk, sl, tp, half_lots)
                    placed_this_bar += 2
                else:
                    _add_pending(ORDER_SELL_STOP, low_brk, sl, tp, total_lots)
                    placed_this_bar += 1

                if cfg.hedge_mode:
                    max_buy_lim = _norm_price(ask - stops_pad, meta)
                    if low_brk <= max_buy_lim:
                        slH = _norm_price(low_brk - cfg.stop_loss_pts * meta.point, meta)
                        tpH = _norm_price(low_brk + cfg.take_profit_pts * meta.point, meta)
                        if cfg.half_tp_ratio > 0:
                            tp_halfH = _norm_price(low_brk + cfg.take_profit_pts * cfg.half_tp_ratio * meta.point, meta)
                            _add_pending(ORDER_BUY_LIMIT, low_brk, slH, tp_halfH, half_lots)
                            _add_pending(ORDER_BUY_LIMIT, low_brk, slH, tpH, half_lots)
                        else:
                            _add_pending(ORDER_BUY_LIMIT, low_brk, slH, tpH, total_lots)

            if debug_enabled:
                bar_placement["break_buy"] = high_brk if high_brk > ask else None
                bar_placement["break_sell"] = low_brk if low_brk < bid else None
                placements_log.append(bar_placement)
            pending_placed_count += placed_this_bar

    # End: compute metrics
    print(f"  [diag] M1 bars seen={m1_bars_seen:,}  in_hours={m1_bars_in_hours:,}  "
          f"skipped_pending={m1_bars_skipped_pending:,}  placed_orders={m1_bars_placed_orders:,}")
    print(f"  [diag] pending placed={pending_placed_count:,}  filled={pending_filled_count:,}  "
          f"expired={pending_expired_count:,}")

    if debug_enabled:
        from pathlib import Path as _P
        base = _P(debug_path)
        base.parent.mkdir(parents=True, exist_ok=True)
        if placements_log:
            pd.DataFrame(placements_log).to_csv(base.with_suffix(".placements.csv"), index=False)
        if order_events:
            pd.DataFrame(order_events).to_csv(base.with_suffix(".orders.csv"), index=False)
        # Deals CSV
        deals_df = pd.DataFrame([
            {"ts": d.ts, "kind": d.kind, "direction": d.direction,
             "lots": d.lots, "price": d.price, "pnl": d.pnl}
            for d in deals
        ])
        if not deals_df.empty:
            deals_df.to_csv(base.with_suffix(".deals.csv"), index=False)
        print(f"  [debug] placements={len(placements_log)}  order_events={len(order_events)}  deals={len(deals)}")
        print(f"  [debug] wrote {base.with_suffix('.placements.csv').name}, "
              f"{base.with_suffix('.orders.csv').name}, {base.with_suffix('.deals.csv').name}")
    tp_count = sum(1 for d in deals if d.kind == 'tp')
    sl_count = sum(1 for d in deals if d.kind == 'sl')
    other_count = sum(1 for d in deals if d.kind == 'other')
    trades = tp_count + sl_count + other_count

    wins = [d.pnl for d in deals if d.kind != 'entry' and d.pnl > 0]
    losses = [d.pnl for d in deals if d.kind != 'entry' and d.pnl < 0]
    pf = sum(wins) / abs(sum(losses)) if losses else float('inf')
    net = balance - initial_balance
    dd_pct = (dd_abs / balance_max * 100.0) if balance_max > 0 else 0.0

    # Balance curve
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
