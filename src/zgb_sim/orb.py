"""ORB (Opening Range Breakout) simulator — port of Scalper_v2 Stream 1.

Per session (London 07:00 UTC, NY 13:00 UTC):
  1. Range = [start, start + range_minutes), capture high/low.
  2. Place BuyStop at range_high + buffer, SellStop at range_low - buffer.
  3. SL = range_size (or fixed_sl if >0); TP = RR_ratio × SL.
  4. OCO: if one_trade_per_session and a position fills, cancel siblings.
  5. Pending expire = range_end + pending_expire_minutes.

Multiple sessions/day stay independent (each has its own pending pair + state).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import List, Optional

import numpy as np
import pandas as pd

from .scalper_v1 import (
    Deal, Pending, Position, SimResult, SymbolMeta,
    ORDER_BUY_STOP, ORDER_SELL_STOP,
    _norm_price, _calc_lots, _pnl,
)


@dataclass
class ORBConfig:
    risk_pct: float = 1.0
    range_minutes: int = 30
    buffer_pts: int = 0  # 2026-05-19: was 30; aligned to EA (parent buffer removed). Sim retains the parameter so future WFO sweeps can re-test buffer>0, but the default matches current EA behavior (no buffer).
    min_range_pts: int = 200
    max_range_pts: int = 5000
    fixed_sl_pts: int = 0       # 0 = SL = range_size
    rr_ratio: float = 2.0
    half_tp_ratio: float = 0.0
    pending_expire_minutes: int = 120
    daily_target_pct: float = 6.0       # 0 disables; >0 closes all + locks day on hit
    daily_loss_pct: float = 8.0         # 0 disables
    # Session mode: only LDN/NY (default) — uses ldn_*/ny_* fields
    ldn_enabled: bool = True
    ldn_start_hour: int = 7
    ny_enabled: bool = True
    ny_start_hour: int = 13
    # Continuous mode: re-fire every refire_minutes across the trading day,
    # ignoring LDN/NY restriction. Each "session" computes its range from the
    # preceding range_minutes of M5 bars.
    continuous_mode: bool = False
    refire_minutes: int = 60
    cont_start_hour: int = 0   # First firing each day (UTC)
    cont_end_hour: int = 23    # Last firing each day (UTC)
    # Break-even SL: when price moves be_trigger_r × SL_dist in favor of entry,
    # move SL to entry + be_buffer_pts (in entry direction). 0 = disabled.
    be_trigger_r: float = 0.0
    be_buffer_pts: int = 0
    # Entry anchor mode: "wick" uses max(high)/min(low) across the range bars
    # (current EA behavior — BUY_STOP at the tip of the highest wick, SELL_STOP
    # at the tip of the lowest wick). "body" uses max(max(open,close)) /
    # min(min(open,close)) — entries at the body extremes, ignoring wicks.
    # Body mode triggers SOONER because body_high <= wick_high (always).
    entry_mode: str = "wick"
    # ----- Fractal experiments (default OFF; spec 2026-05-23) -----
    # All three default off so existing call sites are unchanged. Each flag
    # gates an independent mechanism in simulate() (and orb_fast.simulate_fast).
    fractal_trail: bool = False        # V1: trail SL to most recent opposite-side fractal
    fractal_confirm: bool = False      # V2: arm pending only after same-side fractal
    fractal_range: bool = False        # V3: range H/L from fractals not bar extremes
    fractal_width: int = 5             # bars each side; must be odd >=3 (3 or 5)
    # ----- MA7 post-HTP trail (default OFF) -----
    # Once HTP partial close fires on a stream's half-lot, the surviving "runner"
    # position switches its SL from the static value to SMA(7, close) on M5.
    # SL ratchets only in profit direction. No effect when half_tp_ratio == 0.
    ma_trail: bool = False
    # Retrace-from-HWM gate for the MA7 trail.
    # 0.0 = arm immediately on HTP fire (V1 behavior).
    # >0 = arm only after current unrealized PnL has retraced by at least this
    # fraction of HWM since HTP fired (e.g. 0.25 = wait for 25% giveback).
    ma_trail_retrace_pct: float = 0.0
    # SMA(3) x SMA(5) cross exit on M5 closes (V3 follow-up to ma_trail).
    # When True, post-HTP runners close on opposite-direction cross while in profit.
    # Mutually exclusive with ma_trail (simulate_fast raises if both True).
    sma_cross_exit: bool = False
    comment: str = "ORB"


def _build_sessions(
    days_range: tuple[date, date], cfg: ORBConfig
) -> list[dict]:
    """Pre-build session records (range_start, range_end, expire) for all
    enabled sessions across the date range. Mon-Fri only.

    Two modes:
    - Session mode (default): just LDN + NY at fixed hours.
    - Continuous mode: re-fire every cfg.refire_minutes from cont_start_hour
      to cont_end_hour each day. Each "session" defines its range as the
      preceding cfg.range_minutes window (i.e., range_start = firing_time -
      range_minutes; range_end = firing_time). Pending lives until firing_time
      + pending_expire_minutes.
    """
    sessions = []
    d = days_range[0]
    end = days_range[1]
    while d <= end:
        if d.weekday() >= 5:
            d += timedelta(days=1)
            continue

        if cfg.continuous_mode:
            # Continuous mode: range_end = firing minute T; range = [T-range_min, T)
            t = time(cfg.cont_start_hour, 0, tzinfo=timezone.utc)
            firing = datetime.combine(d, t)
            day_end = datetime.combine(d, time(cfg.cont_end_hour, 0, tzinfo=timezone.utc))
            while firing <= day_end:
                rs = firing - timedelta(minutes=cfg.range_minutes)
                re_ = firing
                sessions.append({
                    "name": "CONT", "session_id": len(sessions),
                    "range_start": rs, "range_end": re_,
                    "expire": re_ + timedelta(minutes=cfg.pending_expire_minutes),
                })
                firing += timedelta(minutes=cfg.refire_minutes)
        else:
            if cfg.ldn_enabled:
                rs = datetime.combine(d, time(cfg.ldn_start_hour, 0, tzinfo=timezone.utc))
                re_ = rs + timedelta(minutes=cfg.range_minutes)
                sessions.append({
                    "name": "LDN", "session_id": len(sessions),
                    "range_start": rs, "range_end": re_,
                    "expire": re_ + timedelta(minutes=cfg.pending_expire_minutes),
                })
            if cfg.ny_enabled:
                rs = datetime.combine(d, time(cfg.ny_start_hour, 0, tzinfo=timezone.utc))
                re_ = rs + timedelta(minutes=cfg.range_minutes)
                sessions.append({
                    "name": "NY", "session_id": len(sessions),
                    "range_start": rs, "range_end": re_,
                    "expire": re_ + timedelta(minutes=cfg.pending_expire_minutes),
                })
        d += timedelta(days=1)
    return sessions


def _compute_range(m5_ts: np.ndarray, m5_highs: np.ndarray, m5_lows: np.ndarray,
                   range_start_ns: int, range_end_ns: int) -> tuple[float, float]:
    """Find high/low over M5 bars whose start time is in [range_start, range_end)."""
    # Bars that started within the range window
    mask_lo = np.searchsorted(m5_ts, range_start_ns, side='left')
    mask_hi = np.searchsorted(m5_ts, range_end_ns, side='left')
    if mask_hi <= mask_lo:
        return 0.0, 0.0
    return float(m5_highs[mask_lo:mask_hi].max()), float(m5_lows[mask_lo:mask_hi].min())


def simulate(
    ticks: pd.DataFrame,
    m5_bars: pd.DataFrame,
    m1_bars: pd.DataFrame,         # unused; kept for sim signature compat
    cfg: ORBConfig,
    meta: SymbolMeta,
    initial_balance: float = 100.0,
    debug_path: Optional[str] = None,
) -> SimResult:
    """Run ORB simulation."""
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

    # Pre-build sessions over the date range
    if len(t_ts) == 0:
        first_day = last_day = date.today()
    else:
        first_day = pd.Timestamp(t_ts[0]).date()
        last_day = pd.Timestamp(t_ts[-1]).date()
    sessions = _build_sessions((first_day, last_day), cfg)

    balance = initial_balance
    pending: List[Pending] = []
    positions: List[Position] = []
    deals: List[Deal] = []
    balance_max = initial_balance
    dd_abs = 0.0

    # Per-session pending/position tracking (still useful for diagnostics, no OCO)
    pending_session: dict[int, list[Pending]] = {}
    position_session: dict[int, list[Position]] = {}

    # Daily cap state (account-level)
    session_day: Optional[date] = None
    balance_day_start = initial_balance
    realized_today = 0.0
    daily_lock = False

    diag_orders_placed = 0
    diag_orders_filled = 0
    diag_orders_expired = 0
    diag_daily_target_hit = 0
    diag_daily_loss_hit = 0

    def _update_dd():
        nonlocal balance_max, dd_abs
        if balance > balance_max:
            balance_max = balance
        cur = balance_max - balance
        if cur > dd_abs:
            dd_abs = cur

    # Sort sessions by range_end so we can iterate forward
    next_session_idx = 0
    sessions.sort(key=lambda s: s["range_end"])

    # Fractal precompute (used by V1/V2/V3; harmless if all flags off)
    fractal_cache = None
    if cfg.fractal_trail or cfg.fractal_confirm or cfg.fractal_range:
        from .fractals import confirmed_fractals
        fractal_cache = confirmed_fractals(m5_bars, width=cfg.fractal_width)

    for k in range(len(t_ts)):
        ts_ns = int(t_ts[k])
        ts = pd.Timestamp(ts_ns)
        bid = t_bid[k]
        ask = t_ask[k]

        # ----- Daily cap rollover + check -----
        day = ts.date()
        if day != session_day:
            session_day = day
            balance_day_start = balance
            realized_today = 0.0
            daily_lock = False

        if not daily_lock:
            unrealized = 0.0
            for plist in position_session.values():
                for p in plist:
                    close_px = bid if p.direction == 1 else ask
                    unrealized += _pnl(p, close_px, meta)
            today_pnl = realized_today + unrealized
            target_locked = False
            if cfg.daily_target_pct > 0 and today_pnl >= balance_day_start * cfg.daily_target_pct / 100.0:
                target_locked = True
                diag_daily_target_hit += 1
            elif cfg.daily_loss_pct > 0 and today_pnl <= -balance_day_start * cfg.daily_loss_pct / 100.0:
                target_locked = True
                diag_daily_loss_hit += 1
            if target_locked:
                # Close all open positions at current bid/ask
                for sid, plist in list(position_session.items()):
                    for p in plist:
                        close_px = bid if p.direction == 1 else ask
                        pnl = _pnl(p, close_px, meta)
                        balance += pnl
                        realized_today += pnl
                        deals.append(Deal(ts, 'other', p.direction, p.lots, close_px, pnl))
                        if balance > balance_max: balance_max = balance
                        cur = balance_max - balance
                        if cur > dd_abs: dd_abs = cur
                    position_session[sid] = []
                # Cancel all pending
                pending = []
                pending_session.clear()
                daily_lock = True

        if daily_lock:
            # Skip session firing + fills until next day
            # (pending list is empty, positions cleared, just iterate ticks)
            continue

        # 1) Fire any sessions whose range has just completed
        while next_session_idx < len(sessions):
            s = sessions[next_session_idx]
            range_end_ns = pd.Timestamp(s["range_end"]).value
            if range_end_ns > ts_ns:
                break
            next_session_idx += 1

            # Compute range bounds — V3 uses fractals, default uses bar extremes
            rs_ns = pd.Timestamp(s["range_start"]).value
            re_ns = range_end_ns
            if cfg.fractal_range and fractal_cache is not None:
                # Fractals confirmed by range_end (no-peek) inside the range window
                up_mask = (fractal_cache["up_ts"] >= rs_ns) & (fractal_cache["up_ts"] < re_ns)
                dn_mask = (fractal_cache["dn_ts"] >= rs_ns) & (fractal_cache["dn_ts"] < re_ns)
                ups = fractal_cache["up_price"][up_mask]
                dns = fractal_cache["dn_price"][dn_mask]
                if len(ups) == 0 or len(dns) == 0:
                    continue  # skip session — no qualifying fractal
                rh = float(ups.max())
                rl = float(dns.min())
            else:
                rh, rl = _compute_range(m5_ts, m5_highs, m5_lows, rs_ns, re_ns)
            if rh <= 0 or rl <= 0:
                continue
            range_pts = (rh - rl) / meta.point
            if range_pts < cfg.min_range_pts or range_pts > cfg.max_range_pts:
                continue

            sl_dist_pts = cfg.fixed_sl_pts if cfg.fixed_sl_pts > 0 else int(range_pts)
            tp_dist_pts = sl_dist_pts * cfg.rr_ratio

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
            # Strip tz for comparison with naive ts in tick loop
            expire_ts = pd.Timestamp(s["expire"]).tz_localize(None)
            sid = s["session_id"]
            pend_list: list[Pending] = []

            # BuyStop above range_high
            buy_entry = _norm_price(rh + cfg.buffer_pts * meta.point, meta)
            min_buy = _norm_price(ask + stops_pad, meta)
            if buy_entry < min_buy:
                buy_entry = min_buy
            if buy_entry > ask:
                sl = _norm_price(buy_entry - sl_dist_pts * meta.point, meta)
                tp = _norm_price(buy_entry + tp_dist_pts * meta.point, meta)
                if cfg.half_tp_ratio > 0:
                    tp_half = _norm_price(buy_entry + tp_dist_pts * cfg.half_tp_ratio * meta.point, meta)
                    pend_list.append(Pending(ORDER_BUY_STOP, buy_entry, sl, tp_half,
                                              half_lots, expire_ts, sid, ts))
                    pend_list.append(Pending(ORDER_BUY_STOP, buy_entry, sl, tp,
                                              half_lots, expire_ts, sid, ts))
                else:
                    pend_list.append(Pending(ORDER_BUY_STOP, buy_entry, sl, tp,
                                              total_lots, expire_ts, sid, ts))

            # SellStop below range_low
            sell_entry = _norm_price(rl - cfg.buffer_pts * meta.point, meta)
            max_sell = _norm_price(bid - stops_pad, meta)
            if sell_entry > max_sell:
                sell_entry = max_sell
            if sell_entry < bid:
                sl = _norm_price(sell_entry + sl_dist_pts * meta.point, meta)
                tp = _norm_price(sell_entry - tp_dist_pts * meta.point, meta)
                if cfg.half_tp_ratio > 0:
                    tp_half = _norm_price(sell_entry - tp_dist_pts * cfg.half_tp_ratio * meta.point, meta)
                    pend_list.append(Pending(ORDER_SELL_STOP, sell_entry, sl, tp_half,
                                              half_lots, expire_ts, sid, ts))
                    pend_list.append(Pending(ORDER_SELL_STOP, sell_entry, sl, tp,
                                              half_lots, expire_ts, sid, ts))
                else:
                    pend_list.append(Pending(ORDER_SELL_STOP, sell_entry, sl, tp,
                                              total_lots, expire_ts, sid, ts))

            if pend_list:
                pending.extend(pend_list)
                pending_session[sid] = pend_list
                position_session.setdefault(sid, [])
                diag_orders_placed += len(pend_list)

        # 2) Expire pending past expire_ts
        if pending:
            still = []
            for p in pending:
                if ts < p.expire_ts:
                    still.append(p)
                else:
                    diag_orders_expired += 1
            removed = len(pending) - len(still)
            if removed:
                # Rebuild per-session pending too
                pending = still
                for sid in list(pending_session.keys()):
                    pending_session[sid] = [p for p in pending_session[sid] if p in still]
                    if not pending_session[sid]:
                        pending_session.pop(sid)

        # 3) Pending fills (with optional V2 fractal-confirm gate)
        new_positions = []
        still_pending = []
        filled_session_ids = set()
        for p in pending:
            # V2 gate: pending arms only after a same-side fractal confirms past entry.
            # Once armed, skip the scan (fractal confirmation is monotonic — never un-confirms).
            if cfg.fractal_confirm and fractal_cache is not None and not p.fractal_armed:
                placed_ns = p.placed_ts.value
                if p.kind == ORDER_BUY_STOP:
                    up_mask = ((fractal_cache["up_ts"] > placed_ns) &
                               (fractal_cache["up_ts"] <= ts_ns) &
                               (fractal_cache["up_price"] > p.price))
                    if not up_mask.any():
                        still_pending.append(p)
                        continue
                else:  # SELL_STOP
                    dn_mask = ((fractal_cache["dn_ts"] > placed_ns) &
                               (fractal_cache["dn_ts"] <= ts_ns) &
                               (fractal_cache["dn_price"] < p.price))
                    if not dn_mask.any():
                        still_pending.append(p)
                        continue
                p.fractal_armed = True   # gate satisfied — never re-scan
            triggered = False
            if p.kind == ORDER_BUY_STOP and ask >= p.price:
                triggered = True; fill = p.price; direction = 1
            elif p.kind == ORDER_SELL_STOP and bid <= p.price:
                triggered = True; fill = p.price; direction = -1
            if triggered:
                pos = Position(direction, fill, p.sl, p.tp, p.lots)
                pos.entry_ts_ns = ts_ns
                # V1: skip past all fractals confirmed at or before entry — they
                # belong to the pre-entry context and must not influence trail SL.
                if cfg.fractal_trail and fractal_cache is not None:
                    pos.sl_trail_idx_dn = int(np.searchsorted(fractal_cache["dn_ts"], ts_ns, side='right'))
                    pos.sl_trail_idx_up = int(np.searchsorted(fractal_cache["up_ts"], ts_ns, side='right'))
                new_positions.append((p.oid, pos))
                deals.append(Deal(ts, 'entry', direction, p.lots, fill, 0.0))
                diag_orders_filled += 1
                filled_session_ids.add(p.oid)
            else:
                still_pending.append(p)
        pending = still_pending

        # (OCO removed — both BuyStop and SellStop allowed to fire on same session.)

        # V1 trail: ratchet SL to most recent confirmed opposite-side fractal.
        # Per-Position index pointer advances through the sorted fractal arrays
        # so each tick only scans newly-confirmed fractals (O(new) vs O(all)).
        if cfg.fractal_trail and fractal_cache is not None:
            dn_ts_arr = fractal_cache["dn_ts"]
            dn_pr_arr = fractal_cache["dn_price"]
            up_ts_arr = fractal_cache["up_ts"]
            up_pr_arr = fractal_cache["up_price"]
            for sid, plist in position_session.items():
                for pos in plist:
                    if pos.direction == 1:
                        # BUY: trail to highest down-fractal confirmed by now
                        # Find upper bound: how many dn fractals have ts <= ts_ns
                        upper = int(np.searchsorted(dn_ts_arr, ts_ns, side='right'))
                        if upper > pos.sl_trail_idx_dn:
                            # New fractals confirmed since last tick
                            new_slice = dn_pr_arr[pos.sl_trail_idx_dn:upper]
                            best = float(new_slice.max())
                            if best > pos.sl_trail_hwm:
                                pos.sl_trail_hwm = best
                                if best > pos.sl:
                                    pos.sl = _norm_price(best, meta)
                            pos.sl_trail_idx_dn = upper
                    else:
                        # SELL: trail to lowest up-fractal confirmed by now
                        upper = int(np.searchsorted(up_ts_arr, ts_ns, side='right'))
                        if upper > pos.sl_trail_idx_up:
                            new_slice = up_pr_arr[pos.sl_trail_idx_up:upper]
                            best = float(new_slice.min())
                            # Lazy init: 0.0 sentinel means "uninitialized"
                            if pos.sl_trail_hwm == 0.0 or best < pos.sl_trail_hwm:
                                pos.sl_trail_hwm = best
                                if best < pos.sl:
                                    pos.sl = _norm_price(best, meta)
                            pos.sl_trail_idx_up = upper

        # 4) SL/TP on existing positions (not just-filled)
        survivors = []
        for sid_list_pair in list(position_session.items()):
            sid, plist = sid_list_pair
            new_plist = []
            for pos in plist:
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
                    new_plist.append(pos)
            position_session[sid] = new_plist

        # Add newly-filled positions
        for sid, pos in new_positions:
            position_session.setdefault(sid, []).append(pos)

    print(f"  [diag] sessions={len(sessions):,}  placed={diag_orders_placed}  "
          f"filled={diag_orders_filled}  expired_or_cancelled={diag_orders_expired}  "
          f"daily_target_hits={diag_daily_target_hit}  daily_loss_hits={diag_daily_loss_hit}")

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
