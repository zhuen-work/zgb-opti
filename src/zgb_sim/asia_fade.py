"""Asia-Session Range Fade simulator.

Strategy archetype: mean-reversion / range-fade.
Diversifies the DT818_pro stack which is otherwise all momentum/breakout.

Per trading day:
  1. Build the Asia range from M5 highs/lows in [range_start_hour, range_end_hour) UTC.
  2. After range completes, place SellLimit at range_high + buffer_pts (fade rally),
     and BuyLimit at range_low - buffer_pts (fade dump).
  3. SL = limit_price ± sl_pts (overshoot stop, configurable).
  4. TP = limit_price ∓ tp_dist where tp_dist = sl_pts × rr_ratio.
  5. Pending expires at fade_close_hour UTC (default 13:00 = NY open) so we
     don't compete with ORB.

Default range: 00:00-06:00 UTC (Tokyo session); fade window 06:00-13:00 UTC
(post-Asia / pre-NY).  Daily caps standard.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import List, Optional

import numpy as np
import pandas as pd

from .scalper_v1 import (
    Deal, Pending, Position, SimResult, SymbolMeta,
    ORDER_BUY_LIMIT, ORDER_SELL_LIMIT,
    _norm_price, _calc_lots, _pnl,
)


@dataclass
class AsiaFadeConfig:
    risk_pct: float = 3.0
    range_start_hour: int = 0          # UTC; Tokyo open ~00:00 UTC
    range_end_hour: int = 6            # UTC; range ends at 06:00 UTC
    fade_close_hour: int = 13          # UTC; cancel pending + close positions before NY open
    buffer_pts: int = 30               # offset of limit price beyond range edge
    sl_pts: int = 200                  # overshoot stop distance from limit price
    rr_ratio: float = 1.5              # TP = sl_pts × RR
    half_tp_ratio: float = 0.0         # 0 = single TP; >0 = split half lots at sl_pts × RR × half_tp_ratio
    min_range_pts: int = 200           # skip days where Asia range is too tight (no liquidity)
    max_range_pts: int = 3000          # skip days where Asia range is too wide (volatile / news)
    daily_target_pct: float = 6.0      # 0 disables
    daily_loss_pct: float = 4.0        # 0 disables
    comment: str = "AsiaFade"


def _build_sessions(days_range: tuple[date, date], cfg: AsiaFadeConfig) -> list[dict]:
    """Pre-build session records (one per weekday)."""
    sessions = []
    d = days_range[0]
    end = days_range[1]
    while d <= end:
        if d.weekday() >= 5:
            d += timedelta(days=1)
            continue
        rs = datetime.combine(d, time(cfg.range_start_hour, 0, tzinfo=timezone.utc))
        re_ = datetime.combine(d, time(cfg.range_end_hour, 0, tzinfo=timezone.utc))
        ex = datetime.combine(d, time(cfg.fade_close_hour, 0, tzinfo=timezone.utc))
        sessions.append({
            "name": "ASIA", "session_id": len(sessions),
            "range_start": rs, "range_end": re_, "expire": ex,
        })
        d += timedelta(days=1)
    return sessions


def _compute_range(m5_ts: np.ndarray, m5_highs: np.ndarray, m5_lows: np.ndarray,
                   range_start_ns: int, range_end_ns: int) -> tuple[float, float]:
    mask_lo = np.searchsorted(m5_ts, range_start_ns, side='left')
    mask_hi = np.searchsorted(m5_ts, range_end_ns, side='left')
    if mask_hi <= mask_lo:
        return 0.0, 0.0
    return float(m5_highs[mask_lo:mask_hi].max()), float(m5_lows[mask_lo:mask_hi].min())


def simulate(
    ticks: pd.DataFrame,
    m5_bars: pd.DataFrame,
    m1_bars: pd.DataFrame,         # unused; kept for sim-signature compat
    cfg: AsiaFadeConfig,
    meta: SymbolMeta,
    initial_balance: float = 10_000.0,
) -> SimResult:
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

    if len(t_ts) == 0:
        first_day = last_day = date.today()
    else:
        first_day = pd.Timestamp(t_ts[0]).date()
        last_day = pd.Timestamp(t_ts[-1]).date()
    sessions = _build_sessions((first_day, last_day), cfg)
    sessions.sort(key=lambda s: s["range_end"])

    balance = initial_balance
    pending: List[Pending] = []
    deals: List[Deal] = []
    balance_max = initial_balance
    dd_abs = 0.0
    pending_session: dict[int, list[Pending]] = {}
    position_session: dict[int, list[Position]] = {}

    session_day: Optional[date] = None
    balance_day_start = initial_balance
    realized_today = 0.0
    daily_lock = False

    diag_placed = 0
    diag_filled = 0
    diag_expired = 0
    diag_skipped_range = 0
    diag_target_hits = 0
    diag_loss_hits = 0

    next_session_idx = 0

    def _update_dd():
        nonlocal balance_max, dd_abs
        if balance > balance_max:
            balance_max = balance
        cur = balance_max - balance
        if cur > dd_abs:
            dd_abs = cur

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

        if not daily_lock:
            unrealized = 0.0
            for plist in position_session.values():
                for p in plist:
                    close_px = bid if p.direction == 1 else ask
                    unrealized += _pnl(p, close_px, meta)
            today_pnl = realized_today + unrealized
            target_locked = False
            if cfg.daily_target_pct > 0 and today_pnl >= balance_day_start * cfg.daily_target_pct / 100.0:
                target_locked = True; diag_target_hits += 1
            elif cfg.daily_loss_pct > 0 and today_pnl <= -balance_day_start * cfg.daily_loss_pct / 100.0:
                target_locked = True; diag_loss_hits += 1
            if target_locked:
                for sid, plist in list(position_session.items()):
                    for p in plist:
                        close_px = bid if p.direction == 1 else ask
                        pnl = _pnl(p, close_px, meta)
                        balance += pnl
                        realized_today += pnl
                        deals.append(Deal(ts, 'other', p.direction, p.lots, close_px, pnl))
                        _update_dd()
                    position_session[sid] = []
                pending = []
                pending_session.clear()
                daily_lock = True

        if daily_lock:
            continue

        # 1) Fire sessions whose range completed
        while next_session_idx < len(sessions):
            s = sessions[next_session_idx]
            range_end_ns = pd.Timestamp(s["range_end"]).value
            if range_end_ns > ts_ns:
                break
            next_session_idx += 1

            rs_ns = pd.Timestamp(s["range_start"]).value
            re_ns = range_end_ns
            rh, rl = _compute_range(m5_ts, m5_highs, m5_lows, rs_ns, re_ns)
            if rh <= 0 or rl <= 0:
                continue
            range_pts = (rh - rl) / meta.point
            if range_pts < cfg.min_range_pts or range_pts > cfg.max_range_pts:
                diag_skipped_range += 1
                continue

            sl_dist_pts = cfg.sl_pts
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
            expire_ts = pd.Timestamp(s["expire"]).tz_localize(None)
            sid = s["session_id"]
            pend_list: list[Pending] = []

            # SellLimit at range_high + buffer  (fade rally)
            sell_entry = _norm_price(rh + cfg.buffer_pts * meta.point, meta)
            if sell_entry > bid + stops_pad:  # MT5 requires limit above current bid + stops
                sl = _norm_price(sell_entry + sl_dist_pts * meta.point, meta)
                tp = _norm_price(sell_entry - tp_dist_pts * meta.point, meta)
                if cfg.half_tp_ratio > 0:
                    tp_half = _norm_price(sell_entry - tp_dist_pts * cfg.half_tp_ratio * meta.point, meta)
                    pend_list.append(Pending(ORDER_SELL_LIMIT, sell_entry, sl, tp_half, half_lots, expire_ts, sid, ts))
                    pend_list.append(Pending(ORDER_SELL_LIMIT, sell_entry, sl, tp, half_lots, expire_ts, sid, ts))
                else:
                    pend_list.append(Pending(ORDER_SELL_LIMIT, sell_entry, sl, tp, total_lots, expire_ts, sid, ts))

            # BuyLimit at range_low - buffer  (fade dump)
            buy_entry = _norm_price(rl - cfg.buffer_pts * meta.point, meta)
            if buy_entry < ask - stops_pad:
                sl = _norm_price(buy_entry - sl_dist_pts * meta.point, meta)
                tp = _norm_price(buy_entry + tp_dist_pts * meta.point, meta)
                if cfg.half_tp_ratio > 0:
                    tp_half = _norm_price(buy_entry + tp_dist_pts * cfg.half_tp_ratio * meta.point, meta)
                    pend_list.append(Pending(ORDER_BUY_LIMIT, buy_entry, sl, tp_half, half_lots, expire_ts, sid, ts))
                    pend_list.append(Pending(ORDER_BUY_LIMIT, buy_entry, sl, tp, half_lots, expire_ts, sid, ts))
                else:
                    pend_list.append(Pending(ORDER_BUY_LIMIT, buy_entry, sl, tp, total_lots, expire_ts, sid, ts))

            if pend_list:
                pending.extend(pend_list)
                pending_session[sid] = pend_list
                position_session.setdefault(sid, [])
                diag_placed += len(pend_list)

        # 2) Expire pendings past expire_ts
        if pending:
            still = []
            for p in pending:
                if ts < p.expire_ts:
                    still.append(p)
                else:
                    diag_expired += 1
            if len(still) != len(pending):
                pending = still
                for sid in list(pending_session.keys()):
                    pending_session[sid] = [p for p in pending_session[sid] if p in still]
                    if not pending_session[sid]:
                        pending_session.pop(sid)

        # 3) Pending fills (limits)
        new_positions = []
        still_pending = []
        for p in pending:
            triggered = False
            if p.kind == ORDER_SELL_LIMIT and bid >= p.price:
                triggered = True; fill = p.price; direction = -1
            elif p.kind == ORDER_BUY_LIMIT and ask <= p.price:
                triggered = True; fill = p.price; direction = 1
            if triggered:
                pos = Position(direction, fill, p.sl, p.tp, p.lots)
                new_positions.append((p.oid, pos))
                deals.append(Deal(ts, 'entry', direction, p.lots, fill, 0.0))
                diag_filled += 1
            else:
                still_pending.append(p)
        pending = still_pending

        # 4) SL/TP on positions
        for sid, plist in list(position_session.items()):
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
                    balance += pnl; realized_today += pnl
                    deals.append(Deal(ts, 'sl', pos.direction, pos.lots, pos.sl, pnl)); _update_dd()
                elif hit_tp:
                    pnl = _pnl(pos, pos.tp, meta)
                    balance += pnl; realized_today += pnl
                    deals.append(Deal(ts, 'tp', pos.direction, pos.lots, pos.tp, pnl)); _update_dd()
                else:
                    new_plist.append(pos)
            position_session[sid] = new_plist

        for sid, pos in new_positions:
            position_session.setdefault(sid, []).append(pos)

    print(f"  [diag] sessions={len(sessions):,}  placed={diag_placed}  filled={diag_filled}  "
          f"expired={diag_expired}  skipped_range={diag_skipped_range}  "
          f"target_hits={diag_target_hits}  loss_hits={diag_loss_hits}")

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
        initial_balance=initial_balance, final_balance=balance, net_profit=net,
        trades=trades, tp_count=tp_count, sl_count=sl_count, other_count=other_count,
        max_drawdown=dd_abs, max_drawdown_pct=dd_pct, profit_factor=pf,
        balance_curve=bc, deals=deals,
    )
