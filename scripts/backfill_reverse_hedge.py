"""Backfill: what would reverse-hedge have produced since Monday 2026-05-11?

For every parent SL between START and now:
  - Pull parent entry price from MT5 history
  - Simulate a reverse-hedge (opposite-direction LIMIT at parent_entry, mirror SL,
    tp_mult * sl_pts TP, 720min expire)
  - Compute hedge outcome (TP / SL / EXPIRE / NO_FIRE)
  - Estimate $ impact = (R-outcome) * (parent's actual SL $ loss)

Aggregate by day; final cumulative + per-regime breakdown.
"""
from __future__ import annotations
import sys
from datetime import datetime, timezone, timedelta, date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd
import MetaTrader5 as mt5

from zgb_sim.mt5_accounts import init_account
from zgb_sim.tick_loader import kill_mt5_terminal
from zgb_sim.regime import (
    session_window_broker, build_h1_bars, compute_atr, lookup_atr_pts_at,
    classify_regime, compute_range_pts,
)

# Per-v2.1_h hedge config
STREAM_CFG = {
    1111: {"name": "S1", "sl_pts": 500, "tp_mult": 1.0},
    2222: {"name": "S2", "sl_pts": 400, "tp_mult": 0.75},
    3333: {"name": "S3", "sl_pts": 350, "tp_mult": 0.75},
    4444: {"name": "S4", "sl_pts": 650, "tp_mult": 1.0},
    5555: {"name": "S5", "sl_pts": 350, "tp_mult": 0.75},
    6666: {"name": "S6", "sl_pts": 400, "tp_mult": 0.75},
}
PARENT_MAGICS = set(STREAM_CFG.keys())
POINT = 0.01
EXPIRE_MIN = 720
START_DATE = date(2026, 5, 11)  # Monday


def find_parent_entry(deals, magic, sl_time):
    """Most-recent DEAL_ENTRY_IN for this magic before sl_time."""
    best = None
    for d in deals:
        if d.magic != magic:
            continue
        if d.entry != mt5.DEAL_ENTRY_IN:
            continue
        ts = pd.Timestamp(d.time, unit="s", tz="UTC")
        if ts > sl_time:
            continue
        if best is None or ts > best[0]:
            best = (ts, d.price)
    return best if best is not None else (None, None)


def simulate_reverse_hedge(ticks, sl_time, parent_entry, parent_side,
                             sl_pts, tp_mult, expire_min=EXPIRE_MIN):
    """Reverse hedge: opposite-direction LIMIT at parent_entry. Returns (outcome, R-multiple)."""
    hedge_side = "SELL" if parent_side == "BUY" else "BUY"
    sl_dist = sl_pts * POINT
    tp_dist = tp_mult * sl_pts * POINT
    if hedge_side == "BUY":
        hedge_sl = parent_entry - sl_dist
        hedge_tp = parent_entry + tp_dist
    else:
        hedge_sl = parent_entry + sl_dist
        hedge_tp = parent_entry - tp_dist

    end_time = sl_time + pd.Timedelta(minutes=expire_min)
    window = ticks[(ticks["ts"] >= sl_time) & (ticks["ts"] <= end_time)]
    if window.empty:
        return "NO_TICKS", 0.0

    mids = window["mid"].values

    # LIMIT fires when price reaches the entry from the opposite side.
    # For BUY_LIMIT (below current price after parent SELL got stopped): mid <= entry
    # For SELL_LIMIT (above current price after parent BUY got stopped): mid >= entry
    fire_idx = None
    for i, mid in enumerate(mids):
        if hedge_side == "BUY" and mid <= parent_entry:
            fire_idx = i; break
        if hedge_side == "SELL" and mid >= parent_entry:
            fire_idx = i; break

    if fire_idx is None:
        return "NO_FIRE", 0.0

    for mid in mids[fire_idx:]:
        if hedge_side == "BUY":
            if mid >= hedge_tp: return "TP", float(tp_mult)
            if mid <= hedge_sl: return "SL", -1.0
        else:
            if mid <= hedge_tp: return "TP", float(tp_mult)
            if mid >= hedge_sl: return "SL", -1.0

    last_mid = mids[-1]
    if hedge_side == "BUY":
        mtm_pts = (last_mid - parent_entry) / POINT
    else:
        mtm_pts = (parent_entry - last_mid) / POINT
    return "EXPIRE", float(mtm_pts / sl_pts)


def main() -> int:
    now = datetime.now(timezone.utc)
    start_dt = datetime(START_DATE.year, START_DATE.month, START_DATE.day, tzinfo=timezone.utc)
    print(f"=== Reverse-hedge backfill  {START_DATE}  ->  {now.date()} ===\n")

    spec = init_account("live")
    sym = spec.symbol
    try:
        # BROKER-TZ FIX: extend mt5_end past `now` so MT5's broker-time-as-UTC
        # interpretation captures all recent trades. Then filter strictly to
        # [start, now] in broker-wall-clock-as-epoch. See
        # feedback_no_unverified_account_claims.md (2026-05-15 incident).
        from zgb_sim.mt5_accounts import get_broker_offset
        broker_off = get_broker_offset(spec.symbol)  # auto-detect (DST-safe)
        lookback = start_dt - timedelta(hours=12)
        mt5_end = now + broker_off
        deals = mt5.history_deals_get(lookback, mt5_end) or ()
        ls_epoch = int(lookback.timestamp())
        end_epoch = int(mt5_end.timestamp())
        deals = [d for d in deals if ls_epoch <= d.time <= end_epoch]
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
            if ts < start_dt:
                continue
            if d.ticket in seen_tickets:
                continue
            seen_tickets.add(d.ticket)
            sl_exits.append({
                "ts_broker": ts,
                "magic": d.magic,
                "stream": STREAM_CFG[d.magic]["name"],
                "side": "BUY" if d.type == mt5.DEAL_TYPE_SELL else "SELL",
                "sl_price": d.price,
                "parent_pnl": d.profit,
                "volume": d.volume,
            })
        print(f"  {len(sl_exits)} parent SLs between {START_DATE} and now\n")
        if not sl_exits:
            return 0

        # Pull ticks for full range + 5h tail
        last_sl = max(ev["ts_broker"] for ev in sl_exits)
        tick_start = start_dt - timedelta(hours=6)  # need pre-day for ATR
        tick_end = last_sl + timedelta(hours=5)
        print(f"  Fetching ticks {tick_start} -> {tick_end}...")
        arr = mt5.copy_ticks_range(sym, tick_start, tick_end, mt5.COPY_TICKS_ALL)
        ticks = pd.DataFrame(arr)
        ticks["ts"] = pd.to_datetime(ticks["time_msc"], unit="ms", utc=True)
        ticks["mid"] = (ticks["bid"] + ticks["ask"]) / 2.0
        print(f"  {len(ticks):,} ticks loaded\n")

        # H1 + ATR for regime tagging
        h1 = build_h1_bars(ticks)
        atr_series = compute_atr(h1)

        # Simulate per SL
        for ev in sl_exits:
            ent_ts, ent_px = find_parent_entry(deals, ev["magic"], ev["ts_broker"])
            ev["entry_price"] = ent_px
            cfg = STREAM_CFG[ev["magic"]]

            if ent_px is None:
                ev["rh_outcome"] = "NO_ENTRY"
                ev["rh_r"] = 0.0
            else:
                rh_out, rh_r = simulate_reverse_hedge(
                    ticks, ev["ts_broker"], ent_px, ev["side"],
                    cfg["sl_pts"], cfg["tp_mult"])
                ev["rh_outcome"] = rh_out
                ev["rh_r"] = rh_r

            # Compute hedge $ impact:
            # Parent SL $-loss == 1.0 R. So hedge_r maps directly to (parent_sl_$ * r_mult).
            parent_sl_dollar = abs(ev["parent_pnl"])
            ev["rh_dollar"] = ev["rh_r"] * parent_sl_dollar

            # Regime (uses auto-detected broker offset for session boundaries)
            broker_off_h = int(broker_off.total_seconds() // 3600)
            ldn_start_h = 4 + broker_off_h
            ny_start_h = 10 + broker_off_h
            ev_h = ev["ts_broker"].hour
            sess = ("LDN" if ldn_start_h <= ev_h < ny_start_h
                    else "NY" if ny_start_h <= ev_h < (ny_start_h + 9) else "OTHER")
            ev["session"] = sess
            rng_start, _ = (session_window_broker(ev["ts_broker"].date(), sess,
                                                    broker_offset_h=broker_off_h)
                            if sess != "OTHER" else (None, None))
            if rng_start is not None:
                rng_end = rng_start + pd.Timedelta(minutes=90)
                ev["range_pts"] = compute_range_pts(ticks, rng_start, rng_end)
                atr_pts = lookup_atr_pts_at(atr_series, rng_start)
                ratio = ev["range_pts"] / atr_pts if (atr_pts and atr_pts > 0) else float("nan")
                ev["regime"] = classify_regime(ev["range_pts"], ratio)
            else:
                ev["range_pts"] = float("nan")
                ev["regime"] = "UNKNOWN"

        # --- Per-SL detail ---
        print(f"  {'Time (broker)':<18} {'Str':<4} {'Sess':<5} {'Reg':<7} {'Range':>5} "
              f"{'Parent $':>10} {'RH out':>7} {'RH R':>5} {'RH $':>10}")
        print(f"  {'-'*18} {'-'*4} {'-'*5} {'-'*7} {'-'*5} {'-'*10} {'-'*7} {'-'*5} {'-'*10}")
        for ev in sorted(sl_exits, key=lambda e: e["ts_broker"]):
            rng_s = f"{ev['range_pts']:>5.0f}" if not pd.isna(ev["range_pts"]) else "  n/a"
            print(f"  {ev['ts_broker'].strftime('%Y-%m-%d %H:%M'):<18} {ev['stream']:<4} "
                  f"{ev['session']:<5} {ev['regime']:<7} {rng_s} "
                  f"${ev['parent_pnl']:>+8.0f} {ev['rh_outcome']:>7} "
                  f"{ev['rh_r']:>+5.2f} ${ev['rh_dollar']:>+8.0f}")

        # --- Per-day aggregation ---
        print(f"\n  --- Per-day aggregation ---")
        by_day = {}
        for ev in sl_exits:
            d = ev["ts_broker"].date()
            by_day.setdefault(d, {"sls": 0, "parent_loss": 0, "rh_pnl": 0,
                                    "rh_fire": 0, "rh_tp": 0, "rh_sl": 0, "regimes": []})
            by_day[d]["sls"] += 1
            by_day[d]["parent_loss"] += ev["parent_pnl"]
            by_day[d]["rh_pnl"] += ev["rh_dollar"]
            if ev["rh_outcome"] not in ("NO_FIRE", "NO_TICKS", "NO_ENTRY"):
                by_day[d]["rh_fire"] += 1
            if ev["rh_outcome"] == "TP":
                by_day[d]["rh_tp"] += 1
            elif ev["rh_outcome"] == "SL":
                by_day[d]["rh_sl"] += 1
            by_day[d]["regimes"].append(ev["regime"])

        print(f"  {'Date':<12} {'DOW':<5} {'SLs':>4} {'Regimes':<20} "
              f"{'Parent $':>10} {'RH fire':>8} {'TP/SL':>7} {'RH $ delta':>11} {'Net day':>11}")
        print(f"  {'-'*12} {'-'*5} {'-'*4} {'-'*20} {'-'*10} {'-'*8} {'-'*7} {'-'*11} {'-'*11}")
        tot_parent = 0
        tot_rh = 0
        for d in sorted(by_day):
            day = by_day[d]
            regimes_str = "/".join(sorted(set(day["regimes"])))
            dow = d.strftime("%a")
            net = day["parent_loss"] + day["rh_pnl"]
            print(f"  {d.isoformat():<12} {dow:<5} {day['sls']:>4} {regimes_str:<20} "
                  f"${day['parent_loss']:>+8.0f} {day['rh_fire']:>3}/{day['sls']:<3} "
                  f"{day['rh_tp']}/{day['rh_sl']:<3} ${day['rh_pnl']:>+9.0f} ${net:>+9.0f}")
            tot_parent += day["parent_loss"]
            tot_rh += day["rh_pnl"]

        print(f"\n  CUMULATIVE since {START_DATE}:")
        print(f"    Parent SL losses (sum of $-losses on parent SL exits): ${tot_parent:>+10,.0f}")
        print(f"    Reverse-hedge $ delta (if it had run):                  ${tot_rh:>+10,.0f}")
        print(f"    Net (parent losses + hedge recovery):                   ${tot_parent + tot_rh:>+10,.0f}")
        if tot_parent != 0:
            recovery_pct = tot_rh / abs(tot_parent) * 100
            print(f"    Recovery: {recovery_pct:.0f}% of parent SL losses offset by reverse-hedge")

        # --- Per-regime aggregation ---
        print(f"\n  --- Per-regime aggregation ---")
        by_regime = {}
        for ev in sl_exits:
            r = ev["regime"]
            by_regime.setdefault(r, {"sls": 0, "rh_pnl": 0, "rh_tp": 0, "rh_sl": 0, "rh_nofire": 0})
            by_regime[r]["sls"] += 1
            by_regime[r]["rh_pnl"] += ev["rh_dollar"]
            if ev["rh_outcome"] == "TP": by_regime[r]["rh_tp"] += 1
            elif ev["rh_outcome"] == "SL": by_regime[r]["rh_sl"] += 1
            else: by_regime[r]["rh_nofire"] += 1
        for r, d in sorted(by_regime.items()):
            print(f"    {r:<7}: {d['sls']:>3} SLs   {d['rh_tp']} TP / {d['rh_sl']} SL / {d['rh_nofire']} NO_FIRE   "
                  f"RH $ = ${d['rh_pnl']:>+8,.0f}")

    finally:
        mt5.shutdown()
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
