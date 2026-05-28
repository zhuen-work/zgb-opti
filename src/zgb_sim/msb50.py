"""MSB50_v1 — 50% Retracement + Market Structure Break simulator.

Strategy (mechanical spec, see plans/msb50_v1):
  1. Detect swing pivots on M5 via N-bar fractal.
  2. Trend state from last 4 pivots: BULL=HH+HL, BEAR=LH+LL, else NEUTRAL.
  3. Range = either (a) last completed impulse leg between last opposite pivots,
     or (b) ORB-style session window [start, start+range_minutes).
  4. Once price tags the 50% midpoint of that range (within tol_pts), arm MSB.
  5. MSB trigger = either (a) M5 close beyond last minor counter-trend pivot,
     or (b) M5 close beyond Donchian(N) high/low in trend direction.
  6. On MSB fire: market order, SL behind 50% level + buffer, TP at RR*SL.

Tick-loop architecture mirrors orb.simulate(): pure Python (no numba) for
the first pass, easy to debug + step through. Optimize later if WFO is slow.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from .scalper_v1 import (
    Deal, SimResult, SymbolMeta,
    _norm_price, _calc_lots, _pnl,
)


RANGE_IMPULSE = "impulse"
RANGE_SESSION = "session"

MSB_MINOR_SWING = "minor_swing"
MSB_DONCHIAN = "donchian"


@dataclass
class MSB50Config:
    risk_pct: float = 1.0

    # Pivot detection
    pivot_n: int = 2                 # N-bar fractal each side (N=2 → 5-bar pivot)

    # Range definition
    range_mode: str = RANGE_IMPULSE  # impulse | session
    session_minutes: int = 60        # only used when range_mode=session
    ldn_enabled: bool = True
    ldn_start_hour: int = 7          # broker-time hour (matches orb.py convention)
    ny_enabled: bool = True
    ny_start_hour: int = 13

    # 50% tag tolerance
    tol_pts: int = 10                # price within mid ± tol_pts counts as "tagged"

    # MSB definition
    msb_mode: str = MSB_MINOR_SWING  # minor_swing | donchian
    n_donch: int = 20                # only used when msb_mode=donchian

    # Execution
    sl_buffer_pts: int = 50          # extra buffer behind 50% / pivot for SL
    min_sl_pts: int = 50             # reject trade if structural SL distance < this (sim integrity)
    rr_ratio: float = 2.0
    max_spread_pts: int = 50

    # Risk caps
    daily_target_pct: float = 0.0    # 0 = disabled
    daily_loss_pct: float = 0.0      # 0 = disabled

    # Arm timeout — if MSB doesn't print within this many M5 bars after 50% tag, cancel
    arm_timeout_bars: int = 24       # 24 × 5min = 2 hours

    # Overshoot cancel — if price goes past 0.5*range beyond mid on the wrong side, cancel
    overshoot_factor: float = 0.5

    comment: str = "MSB50"


# ---------- Pivot detection ----------

def detect_pivots(highs: np.ndarray, lows: np.ndarray, n: int
                 ) -> Tuple[np.ndarray, np.ndarray]:
    """N-bar fractal. Returns (pivot_idx[], pivot_kind[]) where kind +1=high, -1=low.
    Pivot at bar i confirmed only after bar i+n closes (needs n bars on each side).
    """
    L = len(highs)
    idxs: List[int] = []
    kinds: List[int] = []
    for i in range(n, L - n):
        h_i = highs[i]
        l_i = lows[i]
        is_high = True
        is_low = True
        for k in range(1, n + 1):
            if highs[i - k] >= h_i or highs[i + k] >= h_i:
                is_high = False
            if lows[i - k] <= l_i or lows[i + k] <= l_i:
                is_low = False
            if not is_high and not is_low:
                break
        if is_high:
            idxs.append(i); kinds.append(1)
        elif is_low:
            idxs.append(i); kinds.append(-1)
    return np.array(idxs, dtype=np.int64), np.array(kinds, dtype=np.int8)


# ---------- Trend state ----------

def trend_state(pivot_kinds: np.ndarray, pivot_prices: np.ndarray) -> int:
    """Given the chronological pivot stream up to NOW, return BULL=+1, BEAR=-1, NEUTRAL=0.

    BULL needs last 2 highs strictly higher than prior 2 highs AND last 2 lows
    strictly higher than prior 2 lows. We require:
      - at least 2 highs and 2 lows in the stream
      - the last high > previous high
      - the last low > previous low
    Symmetric for BEAR.
    """
    highs_p = pivot_prices[pivot_kinds == 1]
    lows_p = pivot_prices[pivot_kinds == -1]
    if len(highs_p) < 2 or len(lows_p) < 2:
        return 0
    h_curr, h_prev = highs_p[-1], highs_p[-2]
    l_curr, l_prev = lows_p[-1], lows_p[-2]
    if h_curr > h_prev and l_curr > l_prev:
        return 1
    if h_curr < h_prev and l_curr < l_prev:
        return -1
    return 0


# ---------- Session builder (for range_mode=session) ----------

def _build_sessions(days_range: Tuple[date, date], cfg: MSB50Config) -> List[dict]:
    sessions = []
    d = days_range[0]
    end = days_range[1]
    while d <= end:
        if d.weekday() >= 5:
            d += timedelta(days=1)
            continue
        if cfg.ldn_enabled:
            rs = datetime.combine(d, time(cfg.ldn_start_hour, 0, tzinfo=timezone.utc))
            re_ = rs + timedelta(minutes=cfg.session_minutes)
            sessions.append({"start": rs, "end": re_})
        if cfg.ny_enabled:
            rs = datetime.combine(d, time(cfg.ny_start_hour, 0, tzinfo=timezone.utc))
            re_ = rs + timedelta(minutes=cfg.session_minutes)
            sessions.append({"start": rs, "end": re_})
        d += timedelta(days=1)
    return sessions


# ---------- Main simulator ----------

def simulate_msb50(
    ticks: pd.DataFrame,
    m5_bars: pd.DataFrame,
    cfg: MSB50Config,
    meta: SymbolMeta,
    initial_balance: float = 10_000.0,
    debug: bool = False,
) -> SimResult:
    """Run MSB50 sim. ticks=[ts,bid,ask], m5_bars=[ts,open,high,low,close], all UTC."""

    def _to_ns(s: pd.Series) -> np.ndarray:
        if hasattr(s.dt, "tz") and s.dt.tz is not None:
            s = s.dt.tz_convert("UTC").dt.tz_localize(None)
        return s.values.astype("datetime64[ns]").astype(np.int64)

    t_ts = _to_ns(ticks["ts"])
    t_bid = ticks["bid"].values.astype(np.float64)
    t_ask = ticks["ask"].values.astype(np.float64)

    b_ts = _to_ns(m5_bars["ts"])
    b_open = m5_bars["open"].values.astype(np.float64)
    b_high = m5_bars["high"].values.astype(np.float64)
    b_low = m5_bars["low"].values.astype(np.float64)
    b_close = m5_bars["close"].values.astype(np.float64)
    n_bars = len(b_ts)

    if n_bars == 0 or len(t_ts) == 0:
        return SimResult(initial_balance, initial_balance, 0.0, 0, 0, 0, 0,
                         0.0, 0.0, 0.0, pd.DataFrame(), [])

    # Pre-detect ALL pivots upfront, then we just iterate by index.
    all_piv_idx, all_piv_kind = detect_pivots(b_high, b_low, cfg.pivot_n)
    # Per-bar: index of the latest pivot whose confirmation bar is <= this bar.
    # A pivot at idx p is confirmed at bar p + pivot_n (needs N bars right side).
    confirm_bar = all_piv_idx + cfg.pivot_n
    # For each bar i, find how many pivots are confirmed (binary search later).

    # Sessions (only used if range_mode=session)
    first_day = pd.Timestamp(int(t_ts[0])).date()
    last_day = pd.Timestamp(int(t_ts[-1])).date()
    sessions = _build_sessions((first_day, last_day), cfg) if cfg.range_mode == RANGE_SESSION else []
    sess_starts = np.array([pd.Timestamp(s["start"]).tz_localize(None).value for s in sessions], dtype=np.int64)
    sess_ends = np.array([pd.Timestamp(s["end"]).tz_localize(None).value for s in sessions], dtype=np.int64)

    # State
    balance = initial_balance
    balance_max = initial_balance
    dd_abs = 0.0
    session_day = -1
    bal_day_start = initial_balance
    realized_today = 0.0
    daily_lock = False

    # Position state (single position at a time for MVP)
    pos_active = False
    pos_dir = 0
    pos_entry = 0.0
    pos_sl = 0.0
    pos_tp = 0.0
    pos_lots = 0.0

    # Arm state
    armed = False
    armed_dir = 0           # +1 long, -1 short
    armed_mid = 0.0
    armed_range_high = 0.0
    armed_range_low = 0.0
    armed_at_bar = -1       # M5 bar index when armed
    armed_minor_pivot = 0.0 # for minor_swing MSB

    deals: List[Deal] = []
    last_bar_processed = -1

    def _record_close(ts_ns: int, kind: str):
        nonlocal balance, balance_max, dd_abs, realized_today, pos_active
        close_px = pos_sl if kind == "sl" else pos_tp
        pnl = _pnl_inline(pos_dir, pos_entry, close_px, pos_lots, meta)
        balance += pnl
        realized_today += pnl
        if balance > balance_max:
            balance_max = balance
        cur_dd = balance_max - balance
        if cur_dd > dd_abs:
            dd_abs = cur_dd
        deals.append(Deal(pd.Timestamp(ts_ns), kind, pos_dir, pos_lots, close_px, pnl))
        pos_active = False

    for k in range(len(t_ts)):
        ts_ns = t_ts[k]
        bid = t_bid[k]
        ask = t_ask[k]
        ts = pd.Timestamp(ts_ns)
        day = ts.toordinal()

        # Daily rollover
        if day != session_day:
            session_day = day
            bal_day_start = balance
            realized_today = 0.0
            daily_lock = False

        # Daily caps
        if not daily_lock and pos_active:
            unreal_px = bid if pos_dir == 1 else ask
            unreal_pnl = _pnl_inline(pos_dir, pos_entry, unreal_px, pos_lots, meta)
        else:
            unreal_pnl = 0.0
        if not daily_lock:
            today_pnl = realized_today + unreal_pnl
            locked = False
            if cfg.daily_target_pct > 0 and today_pnl >= bal_day_start * cfg.daily_target_pct / 100.0:
                locked = True
            elif cfg.daily_loss_pct > 0 and today_pnl <= -bal_day_start * cfg.daily_loss_pct / 100.0:
                locked = True
            if locked:
                if pos_active:
                    close_px = bid if pos_dir == 1 else ask
                    pnl = _pnl_inline(pos_dir, pos_entry, close_px, pos_lots, meta)
                    balance += pnl
                    realized_today += pnl
                    deals.append(Deal(ts, "other", pos_dir, pos_lots, close_px, pnl))
                    pos_active = False
                    if balance > balance_max:
                        balance_max = balance
                    cur_dd = balance_max - balance
                    if cur_dd > dd_abs:
                        dd_abs = cur_dd
                armed = False
                daily_lock = True

        if daily_lock:
            continue

        # Position management: SL/TP check
        if pos_active:
            if pos_dir == 1:
                if bid <= pos_sl:
                    _record_close(ts_ns, "sl")
                elif bid >= pos_tp:
                    _record_close(ts_ns, "tp")
            else:
                if ask >= pos_sl:
                    _record_close(ts_ns, "sl")
                elif ask <= pos_tp:
                    _record_close(ts_ns, "tp")

        # Advance M5 bar state — process every bar that has CLOSED by this tick.
        # Bar at index i closes at b_ts[i] + 5min.
        # Convert: bar i closed if ts_ns >= b_ts[i] + 5*60*1e9.
        bar_close_ns = 5 * 60 * 1_000_000_000
        while last_bar_processed + 1 < n_bars and b_ts[last_bar_processed + 1] + bar_close_ns <= ts_ns:
            bi = last_bar_processed + 1
            last_bar_processed = bi

            # How many pivots are confirmed as of this bar?
            n_conf = int(np.searchsorted(confirm_bar, bi, side="right"))
            if n_conf == 0:
                continue
            piv_idx_now = all_piv_idx[:n_conf]
            piv_kind_now = all_piv_kind[:n_conf]
            piv_price_now = np.where(piv_kind_now == 1,
                                      b_high[piv_idx_now], b_low[piv_idx_now])

            trend = trend_state(piv_kind_now, piv_price_now)

            # ---- Arm logic ----
            if not armed and trend != 0 and not pos_active:
                # Compute range
                if cfg.range_mode == RANGE_IMPULSE:
                    # Find last 2 pivots — they form the impulse leg
                    if len(piv_idx_now) < 2:
                        continue
                    last_kind = piv_kind_now[-1]
                    prev_kind = piv_kind_now[-2]
                    if last_kind == prev_kind:
                        continue  # need alternating H/L
                    p_last = piv_price_now[-1]
                    p_prev = piv_price_now[-2]
                    rh = max(p_last, p_prev)
                    rl = min(p_last, p_prev)
                else:
                    # session mode — find current session bracketing bar bi
                    bar_ns = b_ts[bi]
                    si = int(np.searchsorted(sess_ends, bar_ns, side="left"))
                    if si >= len(sess_ends) or bar_ns < sess_starts[si]:
                        continue
                    # range = bars whose start in [session_start, session_end)
                    lo = int(np.searchsorted(b_ts, sess_starts[si], side="left"))
                    hi = int(np.searchsorted(b_ts, sess_ends[si], side="left"))
                    if hi <= lo:
                        continue
                    rh = float(b_high[lo:hi].max())
                    rl = float(b_low[lo:hi].min())

                mid = (rh + rl) / 2.0
                range_pts = (rh - rl) / meta.point
                if range_pts < 100:  # too tight to be meaningful — skip
                    continue

                # Did THIS bar tag the 50% level within tolerance?
                tol_px = cfg.tol_pts * meta.point
                tagged = (b_low[bi] <= mid + tol_px) and (b_high[bi] >= mid - tol_px)
                if not tagged:
                    continue

                # Set up armed state
                armed = True
                armed_dir = trend
                armed_mid = mid
                armed_range_high = rh
                armed_range_low = rl
                armed_at_bar = bi
                # Minor pivot for MSB:
                #   BULL → most recent counter-trend pivot is the latest swing HIGH
                #          (we expect close above it to confirm trend resume).
                #   BEAR → most recent swing LOW.
                wanted_kind = 1 if armed_dir == 1 else -1
                armed_minor_pivot = 0.0
                for j in range(len(piv_kind_now) - 1, -1, -1):
                    if piv_kind_now[j] == wanted_kind:
                        armed_minor_pivot = float(piv_price_now[j])
                        break
                if debug:
                    print(f"[arm] bar={bi} ts={pd.Timestamp(b_ts[bi])} dir={armed_dir} "
                          f"mid={mid:.2f} rh={rh:.2f} rl={rl:.2f} minor_piv={armed_minor_pivot:.2f}")
                continue  # don't also fire MSB on the same bar

            # ---- Armed: check MSB fire / cancellation ----
            if armed and not pos_active:
                # Cancellation: trend flipped
                if trend != 0 and trend != armed_dir:
                    armed = False
                    if debug: print(f"[cancel-flip] bar={bi}")
                    continue
                # Cancellation: timeout
                if bi - armed_at_bar > cfg.arm_timeout_bars:
                    armed = False
                    if debug: print(f"[cancel-timeout] bar={bi}")
                    continue
                # Cancellation: overshoot past extreme
                range_size = armed_range_high - armed_range_low
                if armed_dir == 1:
                    overshoot_floor = armed_range_low - cfg.overshoot_factor * range_size
                    if b_low[bi] <= overshoot_floor:
                        armed = False
                        if debug: print(f"[cancel-overshoot-bull] bar={bi}")
                        continue
                else:
                    overshoot_ceil = armed_range_high + cfg.overshoot_factor * range_size
                    if b_high[bi] >= overshoot_ceil:
                        armed = False
                        if debug: print(f"[cancel-overshoot-bear] bar={bi}")
                        continue

                # MSB check on this bar's CLOSE
                close_px = b_close[bi]
                fired = False
                if cfg.msb_mode == MSB_MINOR_SWING:
                    if armed_minor_pivot > 0:
                        if armed_dir == 1 and close_px > armed_minor_pivot:
                            fired = True
                        elif armed_dir == -1 and close_px < armed_minor_pivot:
                            fired = True
                else:  # donchian
                    lo_d = max(0, bi - cfg.n_donch)
                    if bi - lo_d >= cfg.n_donch:
                        donch_hi = float(b_high[lo_d:bi].max())
                        donch_lo = float(b_low[lo_d:bi].min())
                        if armed_dir == 1 and close_px > donch_hi:
                            fired = True
                        elif armed_dir == -1 and close_px < donch_lo:
                            fired = True

                if fired:
                    # Spread check before entry
                    spread_pts = (ask - bid) / meta.point
                    if spread_pts > cfg.max_spread_pts:
                        armed = False
                        if debug: print(f"[skip-spread] bar={bi} spread={spread_pts:.1f}")
                        continue
                    # Place market order at next tick (ask for long, bid for short)
                    entry_px = ask if armed_dir == 1 else bid
                    if armed_dir == 1:
                        sl_anchor = min(armed_mid, armed_minor_pivot if armed_minor_pivot > 0 else armed_mid)
                        sl_px = _norm_price_inline(sl_anchor - cfg.sl_buffer_pts * meta.point, meta)
                        # Reject trade if structural SL ends up on wrong side of entry
                        # (happens when donchian MSB fires far from impulse-leg structure).
                        if sl_px >= entry_px:
                            armed = False
                            if debug: print(f"[skip-bad-sl-bull] bar={bi} entry={entry_px:.2f} sl={sl_px:.2f}")
                            continue
                        sl_dist_pts = int(round((entry_px - sl_px) / meta.point))
                        if sl_dist_pts < cfg.min_sl_pts:
                            armed = False
                            if debug: print(f"[skip-tight-sl-bull] bar={bi} sl_dist={sl_dist_pts}pt")
                            continue
                        tp_px = _norm_price_inline(entry_px + cfg.rr_ratio * sl_dist_pts * meta.point, meta)
                    else:
                        sl_anchor = max(armed_mid, armed_minor_pivot if armed_minor_pivot > 0 else armed_mid)
                        sl_px = _norm_price_inline(sl_anchor + cfg.sl_buffer_pts * meta.point, meta)
                        if sl_px <= entry_px:
                            armed = False
                            if debug: print(f"[skip-bad-sl-bear] bar={bi} entry={entry_px:.2f} sl={sl_px:.2f}")
                            continue
                        sl_dist_pts = int(round((sl_px - entry_px) / meta.point))
                        if sl_dist_pts < cfg.min_sl_pts:
                            armed = False
                            if debug: print(f"[skip-tight-sl-bear] bar={bi} sl_dist={sl_dist_pts}pt")
                            continue
                        tp_px = _norm_price_inline(entry_px - cfg.rr_ratio * sl_dist_pts * meta.point, meta)

                    lots = _calc_lots_inline(balance, cfg.risk_pct, sl_dist_pts, meta)
                    if lots <= 0:
                        armed = False
                        continue
                    pos_active = True
                    pos_dir = armed_dir
                    pos_entry = entry_px
                    pos_sl = sl_px
                    pos_tp = tp_px
                    pos_lots = lots
                    deals.append(Deal(ts, "entry", pos_dir, lots, entry_px, 0.0))
                    if debug:
                        print(f"[FIRE] bar={bi} ts={ts} dir={pos_dir} entry={entry_px:.2f} "
                              f"sl={sl_px:.2f} tp={tp_px:.2f} lots={lots} sl_pts={sl_dist_pts}")
                    armed = False

    # Final summary
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


# ---------- inline helpers (avoid Numba round-trips since this sim is pure Python) ----------

def _norm_price_inline(price: float, meta: SymbolMeta) -> float:
    return round(round(price / meta.tick_size) * meta.tick_size, meta.digits)


def _calc_lots_inline(balance: float, risk_pct: float, sl_pts: int, meta: SymbolMeta) -> float:
    risk_money = balance * risk_pct / 100.0
    sl_money = (sl_pts * meta.point / meta.tick_size) * meta.tick_value
    if sl_money <= 0:
        return 0.0
    lots = risk_money / sl_money
    lots = max(meta.volume_min, min(meta.volume_max, lots))
    lots = round(lots / meta.volume_step) * meta.volume_step
    return round(lots, 2)


def _pnl_inline(direction: int, entry: float, close_px: float, lots: float, meta: SymbolMeta) -> float:
    diff = (close_px - entry) * direction
    return diff * lots * meta.tick_value / meta.tick_size
