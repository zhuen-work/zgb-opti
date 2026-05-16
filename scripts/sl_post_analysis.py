"""SL post-action feature logger.

For each parent SL hit today, logs the features needed to evaluate a
regime-gated hedge dispatch (retry-hedge vs reverse-hedge).

Per-SL features captured:
  - parent side / entry / SL / time_to_sl
  - session (LDN/NY) + ORB range_pts at session boundary
  - +15/30/60/120/240m mid-price deltas (signed: positive = continuation)
  - retry-hedge outcome: same-direction STOP at parent entry, sl_pts SL, tp_mult*sl_pts TP, 720m expire
  - reverse-hedge outcome: opposite-direction LIMIT at parent entry, same SL/TP geometry

Output:
  - console: per-SL table + continuation summary (existing v1 behavior)
  - CSV append: output/sl_post_log.csv (one row per SL, deduped by (ts, magic))

Stream cfg pulled from current v2.1_h hedge config (project_orb_live_trade_log_v3.md).
"""
from __future__ import annotations
import sys
import csv
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.mt5_accounts import init_account, get_broker_offset
from zgb_sim.tick_loader import kill_mt5_terminal
from zgb_sim.regime import (
    session_window_broker as regime_session_window,
    build_h1_bars, compute_atr, lookup_atr_pts_at, classify_regime,
)
import MetaTrader5 as mt5
import pandas as pd
import numpy as np

# Parent magics + per-stream sim params for hedge outcomes.
# sl_pts and tp_mult lifted from v2.1_h hedge config (project_orb_live_trade_log_v3).
STREAM_CFG = {
    1111: {"name": "S1", "sl_pts": 500, "tp_mult": 1.0},
    2222: {"name": "S2", "sl_pts": 400, "tp_mult": 0.75},
    3333: {"name": "S3", "sl_pts": 350, "tp_mult": 0.75},
    4444: {"name": "S4", "sl_pts": 650, "tp_mult": 1.0},
    5555: {"name": "S5", "sl_pts": 350, "tp_mult": 0.75},
    6666: {"name": "S6", "sl_pts": 400, "tp_mult": 0.75},
}
PARENT_MAGICS = set(STREAM_CFG.keys())
OFFSETS_MIN = [15, 30, 60, 120, 240]
EXPIRE_MIN = 720
POINT = 0.01

# Session windows in REAL UTC (EA uses TimeGMT). Broker timestamps from MT5
# are labeled-as-UTC but actually broker-time (UTC+3 on Vantage), so when we
# slice ticks by .ts >= range_start, we use BROKER-time labels.
# LDN: 04:00 real UTC = 07:00 broker; range 04:00-05:30 real = 07:00-08:30 broker
# NY:  10:00 real UTC = 13:00 broker; range 10:00-11:30 real = 13:00-14:30 broker
SESSION_RANGE_BROKER = {
    "LDN": (7, 30, 8, 30, 7),    # start_h, start_m=00 (offset 30 = 7:00+0:30 NO -- see below)
    "NY":  (13, 0, 14, 30, 13),
}

# Cleaner: explicit datetime ranges per session, derived from real-UTC config.
# Range = (start_hour_real_utc, range_minutes)
SESSION_CFG = {
    "LDN": {"start_h_real_utc": 4, "range_min": 90},
    "NY":  {"start_h_real_utc": 10, "range_min": 90},
}
BROKER_OFFSET_H = 3  # Default; overridden at runtime via auto-detect (see main()).


def session_window_broker(sl_time_broker: pd.Timestamp, broker_offset_h: int = None):
    """Pick session and return (range_start, range_end) in broker-time labels.

    broker_offset_h: if None, falls back to module BROKER_OFFSET_H constant.
                      Live scripts should pass auto-detected value via
                      get_broker_offset() to be DST-safe.
    """
    off_h = broker_offset_h if broker_offset_h is not None else BROKER_OFFSET_H
    # Session boundary: LDN broker hours run 07:00-12:30 (offset_h=3) or 06:00-11:30
    # (offset_h=2). Pivot point is half-way to NY start.
    pivot_broker_h = SESSION_CFG["NY"]["start_h_real_utc"] + off_h  # NY broker start
    h = sl_time_broker.hour
    if h < pivot_broker_h:
        sess = "LDN"
    else:
        sess = "NY"
    cfg = SESSION_CFG[sess]
    start_broker_h = cfg["start_h_real_utc"] + off_h
    base = sl_time_broker.normalize()
    rng_start = base.replace(hour=start_broker_h, minute=0)
    rng_end = rng_start + pd.Timedelta(minutes=cfg["range_min"])
    return sess, rng_start, rng_end


def compute_range_pts(ticks: pd.DataFrame, rng_start, rng_end) -> float:
    """ORB range_pts = (max_mid - min_mid) / point during the range window."""
    slc = ticks[(ticks["ts"] >= rng_start) & (ticks["ts"] <= rng_end)]
    if slc.empty:
        return float("nan")
    return (slc["mid"].max() - slc["mid"].min()) / POINT


def find_parent_entry(deals, magic, sl_time):
    """Find the most-recent DEAL_ENTRY_IN for this magic before sl_time.
    Returns (entry_time, entry_price) or (None, None)."""
    candidates = []
    for d in deals:
        if d.magic != magic:
            continue
        if d.entry != mt5.DEAL_ENTRY_IN:
            continue
        d_ts = pd.Timestamp(d.time, unit="s", tz="UTC")
        if d_ts > sl_time:
            continue
        candidates.append((d_ts, d.price))
    if not candidates:
        return None, None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0]


def simulate_hedge(ticks, sl_time, parent_entry, parent_side, hedge_kind,
                   sl_pts, tp_mult, expire_min=EXPIRE_MIN):
    """Simulate a hypothetical hedge.

    hedge_kind:
      'retry'   = same-direction STOP @ parent_entry  (catches reversion through entry, then continuation past)
      'reverse' = opposite-direction LIMIT @ parent_entry (catches retrace to entry, then resumed SL direction)

    Geometry (both kinds):
      entry = parent_entry
      SL    = entry +/- sl_pts*point  (mirror of parent)
      TP    = entry +/- tp_mult*sl_pts*point

    Returns (outcome, pnl_R) where pnl_R is signed R-multiple:
      TP outcome -> +tp_mult
      SL outcome -> -1.0
      EXPIRE     -> mark-to-market_pts / sl_pts (signed)
      NO_FIRE    -> 0.0
    """
    if hedge_kind == "retry":
        hedge_side = parent_side
    else:  # reverse
        hedge_side = "SELL" if parent_side == "BUY" else "BUY"

    sl_dist = sl_pts * POINT
    tp_dist = tp_mult * sl_pts * POINT
    if hedge_side == "BUY":
        hedge_sl_price = parent_entry - sl_dist
        hedge_tp_price = parent_entry + tp_dist
    else:
        hedge_sl_price = parent_entry + sl_dist
        hedge_tp_price = parent_entry - tp_dist

    end_time = sl_time + pd.Timedelta(minutes=expire_min)
    window = ticks[(ticks["ts"] >= sl_time) & (ticks["ts"] <= end_time)]
    if window.empty:
        return "NO_TICKS", 0.0

    mids = window["mid"].values

    # Find pending fire index.
    fire_idx = None
    for i, mid in enumerate(mids):
        if hedge_kind == "retry":
            # STOP order: BUY_STOP fires when price RISES to entry, SELL_STOP when price FALLS to entry.
            if hedge_side == "BUY" and mid >= parent_entry:
                fire_idx = i
                break
            if hedge_side == "SELL" and mid <= parent_entry:
                fire_idx = i
                break
        else:
            # LIMIT: SELL_LIMIT (above current) fires when price RISES to entry,
            #        BUY_LIMIT (below current) fires when price FALLS to entry.
            if hedge_side == "BUY" and mid <= parent_entry:
                fire_idx = i
                break
            if hedge_side == "SELL" and mid >= parent_entry:
                fire_idx = i
                break

    if fire_idx is None:
        return "NO_FIRE", 0.0

    # Track TP/SL/expire from fire onward.
    for mid in mids[fire_idx:]:
        if hedge_side == "BUY":
            if mid >= hedge_tp_price:
                return "TP", float(tp_mult)
            if mid <= hedge_sl_price:
                return "SL", -1.0
        else:
            if mid <= hedge_tp_price:
                return "TP", float(tp_mult)
            if mid >= hedge_sl_price:
                return "SL", -1.0

    # Expired — mark-to-market in R units.
    last_mid = mids[-1]
    if hedge_side == "BUY":
        mtm_pts = (last_mid - parent_entry) / POINT
    else:
        mtm_pts = (parent_entry - last_mid) / POINT
    return "EXPIRE", float(mtm_pts / sl_pts)


def main() -> int:
    now = datetime.now(timezone.utc)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    # Use full broker-day boundary (start + 1 day) — NOT real-UTC `now` —
    # to avoid the broker-tz bug where MT5 reads datetime args as broker
    # wall-clock and cuts off ~3h of recent trades. See
    # feedback_no_unverified_account_claims.md (2026-05-15 incident).
    end = start + timedelta(days=1)
    print(f"=== SL post-action analysis  {start.date()}  (broker-time labels) ===\n")

    spec = init_account("live")
    sym = spec.symbol
    try:
        # Auto-detect current broker offset (DST-safe). Use integer hours for
        # session_window_broker which builds session boundaries from it.
        broker_off_td = get_broker_offset(spec.symbol)
        broker_off_h = int(broker_off_td.total_seconds() // 3600)

        # Pull deals for entry lookup window (yesterday too, in case parent entered late prior day)
        lookback_start = start - timedelta(hours=12)
        deals = mt5.history_deals_get(lookback_start, end) or ()
        # Strict filter: keep deals where d.time (broker-as-epoch) is in
        # [lookback_start, end) using wall-clock-as-epoch comparison.
        ls_epoch = int(lookback_start.timestamp())
        end_epoch = int(end.timestamp())
        deals = [d for d in deals if ls_epoch <= d.time < end_epoch]

        sl_exits = []
        seen_tickets = set()
        for d in deals:
            if d.magic not in PARENT_MAGICS:
                continue
            if d.entry != mt5.DEAL_ENTRY_OUT:
                continue
            if not d.comment.startswith("[sl"):
                continue
            ts = pd.Timestamp(d.time, unit="s", tz="UTC")
            if ts < start:
                continue
            # Dedupe by ticket only — distinct positions can close at the
            # exact same (time, magic, price) during cluster stops.
            if d.ticket in seen_tickets:
                continue
            seen_tickets.add(d.ticket)
            sl_exits.append({
                "ts_broker": ts,
                "magic": d.magic,
                "stream": STREAM_CFG[d.magic]["name"],
                "side": "BUY" if d.type == mt5.DEAL_TYPE_SELL else "SELL",
                "sl_price": d.price,
                "pnl": d.profit,
            })
        print(f"  {len(sl_exits)} unique parent SL exits today\n")

        if not sl_exits:
            print("  (no SLs today)")
            return 0

        # Tick stream covering session range + 4h tail after last SL.
        last_sl = max(ev["ts_broker"] for ev in sl_exits)
        tick_start = start  # full day, captures range windows + entry resolution
        tick_end = last_sl + timedelta(hours=5)
        arr = mt5.copy_ticks_range(sym, tick_start, tick_end, mt5.COPY_TICKS_ALL)
        ticks = pd.DataFrame(arr)
        ticks["ts"] = pd.to_datetime(ticks["time_msc"], unit="ms", utc=True)
        ticks["mid"] = (ticks["bid"] + ticks["ask"]) / 2.0

        # H1 bars + ATR for regime classification.
        h1_bars = build_h1_bars(ticks)
        atr_series = compute_atr(h1_bars)

        # Enrich each SL with entry, session, range, regime, hedge sims.
        for ev in sl_exits:
            ent_ts, ent_px = find_parent_entry(deals, ev["magic"], ev["ts_broker"])
            ev["entry_ts"] = ent_ts
            ev["entry_price"] = ent_px
            ev["time_to_sl_min"] = (
                (ev["ts_broker"] - ent_ts).total_seconds() / 60.0 if ent_ts is not None else float("nan")
            )

            sess, rng_start, rng_end = session_window_broker(ev["ts_broker"], broker_off_h)
            ev["session"] = sess
            ev["range_pts"] = compute_range_pts(ticks, rng_start, rng_end)
            ev["atr_h1_20_pts"] = lookup_atr_pts_at(atr_series, rng_start) if rng_start is not None else float("nan")
            ev["range_atr_ratio"] = (
                ev["range_pts"] / ev["atr_h1_20_pts"]
                if (not pd.isna(ev["range_pts"]) and not pd.isna(ev["atr_h1_20_pts"]) and ev["atr_h1_20_pts"] > 0)
                else float("nan")
            )
            ev["regime"] = classify_regime(ev["range_pts"], ev["range_atr_ratio"])

            cfg = STREAM_CFG[ev["magic"]]
            if ent_px is not None:
                retry_out, retry_r = simulate_hedge(
                    ticks, ev["ts_broker"], ent_px, ev["side"],
                    "retry", cfg["sl_pts"], cfg["tp_mult"])
                rev_out, rev_r = simulate_hedge(
                    ticks, ev["ts_broker"], ent_px, ev["side"],
                    "reverse", cfg["sl_pts"], cfg["tp_mult"])
            else:
                retry_out, retry_r = "NO_ENTRY", 0.0
                rev_out, rev_r = "NO_ENTRY", 0.0
            ev["retry_outcome"] = retry_out
            ev["retry_r"] = retry_r
            ev["reverse_outcome"] = rev_out
            ev["reverse_r"] = rev_r

            # Continuation deltas at offsets (kept for console v1 compat).
            ev["delta_at"] = {}
            for off_min in OFFSETS_MIN:
                target_ts = ev["ts_broker"] + timedelta(minutes=off_min)
                fut = ticks[ticks["ts"] >= target_ts]
                if fut.empty:
                    ev["delta_at"][off_min] = float("nan")
                    continue
                fmid = fut.iloc[0]["mid"]
                # continuation = price moves in SL direction
                if ev["side"] == "BUY":
                    delta = ev["sl_price"] - fmid  # parent BUY: SL below entry; continuation = price keeps dropping
                else:
                    delta = fmid - ev["sl_price"]  # parent SELL: SL above entry; continuation = price keeps rising
                ev["delta_at"][off_min] = delta

        # --- Console output v2: extended table ---
        print(f"  Continuation (price-action) — positive = price moved in SL direction")
        print(f"  Hedge R   — R-multiple outcome (TP=+tp_mult, SL=-1.0, EXPIRE=mtm/sl_pts)\n")

        offs_hdr = " ".join(f"{o:>5}m" for o in OFFSETS_MIN)
        print(f"  {'Time':<19} {'Str':<4} {'Side':<4} {'Range':>5} {'Reg':<7} {'TtSL':>5} | {offs_hdr} | "
              f"{'Retry':>10} {'Reverse':>10}")
        print(f"  {'-'*19} {'-'*4} {'-'*4} {'-'*5} {'-'*7} {'-'*5} | "
              + " ".join("-"*6 for _ in OFFSETS_MIN) + " | " + "-"*10 + " " + "-"*10)

        for ev in sl_exits:
            offs_str = " ".join(
                (f"{ev['delta_at'][o]:+6.2f}" if not pd.isna(ev["delta_at"][o]) else "   n/a")
                for o in OFFSETS_MIN
            )
            retry_str = f"{ev['retry_outcome']:>3} {ev['retry_r']:+5.2f}"
            rev_str = f"{ev['reverse_outcome']:>3} {ev['reverse_r']:+5.2f}"
            rng_str = f"{ev['range_pts']:>5.0f}" if not pd.isna(ev["range_pts"]) else "  n/a"
            ttsl_str = f"{ev['time_to_sl_min']:>5.0f}" if not pd.isna(ev["time_to_sl_min"]) else "  n/a"
            print(f"  {ev['ts_broker'].strftime('%Y-%m-%d %H:%M'):<19} "
                  f"{ev['stream']:<4} {ev['side']:<4} {rng_str} {ev['regime']:<7} {ttsl_str} | {offs_str} | "
                  f"{retry_str} {rev_str}")

        # --- Continuation summary (unchanged from v1) ---
        print()
        print("  --- Continuation % per offset ---")
        for off_min in OFFSETS_MIN:
            vals = [ev["delta_at"][off_min] for ev in sl_exits if not pd.isna(ev["delta_at"][off_min])]
            if not vals:
                continue
            n_cont = sum(1 for v in vals if v > 0)
            pct = n_cont / len(vals) * 100
            print(f"    +{off_min:>3}m: {n_cont}/{len(vals)} = {pct:.0f}% continuation")

        # --- Hedge sim summary ---
        print()
        print("  --- Hypothetical hedge sim (per stream cfg) ---")
        retry_pnl = sum(ev["retry_r"] for ev in sl_exits)
        rev_pnl = sum(ev["reverse_r"] for ev in sl_exits)
        n_fire_retry = sum(1 for ev in sl_exits if ev["retry_outcome"] in {"TP", "SL", "EXPIRE"})
        n_fire_rev = sum(1 for ev in sl_exits if ev["reverse_outcome"] in {"TP", "SL", "EXPIRE"})
        n_retry_tp = sum(1 for ev in sl_exits if ev["retry_outcome"] == "TP")
        n_rev_tp = sum(1 for ev in sl_exits if ev["reverse_outcome"] == "TP")
        print(f"    Retry:   fired {n_fire_retry}/{len(sl_exits)}, TP {n_retry_tp}, sum R = {retry_pnl:+.2f}")
        print(f"    Reverse: fired {n_fire_rev}/{len(sl_exits)}, TP {n_rev_tp}, sum R = {rev_pnl:+.2f}")

        # --- Append to CSV log ---
        out_dir = ROOT / "output"
        out_dir.mkdir(exist_ok=True)
        csv_path = out_dir / "sl_post_log.csv"
        is_new = not csv_path.exists()
        with csv_path.open("a", newline="") as f:
            cols = [
                "date", "ts_broker", "stream", "magic", "side",
                "entry_price", "sl_price", "parent_pnl",
                "time_to_sl_min", "session", "range_pts",
                "atr_h1_20_pts", "range_atr_ratio", "regime",
                "delta_15m", "delta_30m", "delta_60m", "delta_120m", "delta_240m",
                "retry_outcome", "retry_r", "reverse_outcome", "reverse_r",
            ]
            w = csv.DictWriter(f, fieldnames=cols)
            if is_new:
                w.writeheader()
            for ev in sl_exits:
                w.writerow({
                    "date": ev["ts_broker"].date().isoformat(),
                    "ts_broker": ev["ts_broker"].isoformat(),
                    "stream": ev["stream"],
                    "magic": ev["magic"],
                    "side": ev["side"],
                    "entry_price": ev["entry_price"],
                    "sl_price": ev["sl_price"],
                    "parent_pnl": ev["pnl"],
                    "time_to_sl_min": round(ev["time_to_sl_min"], 2) if not pd.isna(ev["time_to_sl_min"]) else "",
                    "session": ev["session"],
                    "range_pts": round(ev["range_pts"], 1) if not pd.isna(ev["range_pts"]) else "",
                    "atr_h1_20_pts": round(ev["atr_h1_20_pts"], 1) if not pd.isna(ev["atr_h1_20_pts"]) else "",
                    "range_atr_ratio": round(ev["range_atr_ratio"], 2) if not pd.isna(ev["range_atr_ratio"]) else "",
                    "regime": ev["regime"],
                    "delta_15m": round(ev["delta_at"][15], 3) if not pd.isna(ev["delta_at"][15]) else "",
                    "delta_30m": round(ev["delta_at"][30], 3) if not pd.isna(ev["delta_at"][30]) else "",
                    "delta_60m": round(ev["delta_at"][60], 3) if not pd.isna(ev["delta_at"][60]) else "",
                    "delta_120m": round(ev["delta_at"][120], 3) if not pd.isna(ev["delta_at"][120]) else "",
                    "delta_240m": round(ev["delta_at"][240], 3) if not pd.isna(ev["delta_at"][240]) else "",
                    "retry_outcome": ev["retry_outcome"],
                    "retry_r": round(ev["retry_r"], 3),
                    "reverse_outcome": ev["reverse_outcome"],
                    "reverse_r": round(ev["reverse_r"], 3),
                })
        print(f"\n  Logged {len(sl_exits)} rows to {csv_path}")

    finally:
        mt5.shutdown()
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
