"""Asia-session outcome logger — feature for NY-session filtering thesis.

WINDOW CONVENTION (decided 2026-05-15): all session/watch hours below are
BROKER-clock (Vantage UTC+3 summer / UTC+2 winter). The ts column in the
tick stream returned by load_ticks is broker-time-labeled-as-UTC, so the
filters are internally consistent. "23:00->04:00 UTC" labels in the code
should be read as "23:00->04:00 broker" = real UTC 20:00->01:00 (summer)
or 21:00->02:00 (winter). This is the ICT-style "Asian session" by broker
clock, which is what the user prefers for matching their MT5 chart times.

For each trading day, computes the Asian range (broker 23:00 prior day ->
04:00 today, 5h) and INDEPENDENTLY simulates what a BUY_STOP at asia_high
and a SELL_STOP at asia_low would have done if armed at broker 04:00 and
watched through broker 22:00 (covers full LDN + NY in broker time).

Both sides are logged independently (vs real ORB where one would cancel the
other). This gives the full directional picture needed for filter analysis:
  - BUY  TP + SELL SL  -> directional UP day, trend bias
  - BUY  SL + SELL TP  -> directional DOWN day, trend bias
  - BUY  TP + SELL TP  -> high-volatility chop (both fired and ran)
  - BUY  SL + SELL SL  -> tight-range chop (both fake-broke and reverted)
  - either NO_FIRE     -> price stayed inside Asian range

CONFIG (canonical, NOT swept here — change in code if needed):
  SL_PTS = 400    (matches v2.1 S2/S6)
  RR     = 4.0
  WATCH  = 04:00 UTC -> 22:00 UTC (18h, covers LDN + NY)

Output: output/asia_session_log.csv (append-safe, deduped by date)

Usage:
  python scripts/asia_session_log.py            # today (run after 04:30 UTC)
  python scripts/asia_session_log.py --days 30  # backfill
  python scripts/asia_session_log.py --start 2026-04-25 --end 2026-05-15
"""
from __future__ import annotations
import sys
import csv
import argparse
from datetime import datetime, timezone, timedelta, date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd
import numpy as np

from zgb_sim.mt5_accounts import init_account
from zgb_sim.tick_loader import load_ticks, kill_mt5_terminal

# Canonical config
SL_PTS = 400
RR_RATIO = 4.0
POINT = 0.01

# Asian range: broker 23:00 (prior day) -> broker 04:00 (today)
# (Hours are broker-clock; ticks have broker-as-UTC labels so this filter is consistent.)
ASIA_START_HOUR_BROKER = 23   # of PRIOR broker day
ASIA_DURATION_H = 5
WATCH_START_HOUR_BROKER = 4   # of CURRENT broker day (= asia_end)
WATCH_DURATION_H = 18         # covers through broker 22:00 (post-NY-close)
# Aliases kept for readability of existing call sites:
ASIA_START_HOUR_UTC = ASIA_START_HOUR_BROKER
WATCH_START_HOUR_UTC = WATCH_START_HOUR_BROKER


def asia_window_utc(day: date) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Returns (asia_start, asia_end) in real UTC for the given trading `day`."""
    asia_end = pd.Timestamp(day).tz_localize("UTC").replace(hour=WATCH_START_HOUR_UTC, minute=0)
    asia_start = asia_end - pd.Timedelta(hours=ASIA_DURATION_H)
    return asia_start, asia_end


def watch_window_utc(day: date) -> tuple[pd.Timestamp, pd.Timestamp]:
    watch_start = pd.Timestamp(day).tz_localize("UTC").replace(hour=WATCH_START_HOUR_UTC, minute=0)
    watch_end = watch_start + pd.Timedelta(hours=WATCH_DURATION_H)
    return watch_start, watch_end


def simulate_breakout(ticks: pd.DataFrame, side: str, entry_price: float,
                        sl_pts: int, rr: float, watch_end: pd.Timestamp) -> dict:
    """Simulate a single-side ORB breakout. Returns dict with outcome + fire/exit ts.

    side='BUY': BUY_STOP at entry_price, SL below, TP above. Fires when ask >= entry.
    side='SELL': SELL_STOP at entry_price, SL above, TP below. Fires when bid <= entry.
    """
    sl_dist = sl_pts * POINT
    tp_dist = sl_dist * rr
    if side == "BUY":
        sl_price = entry_price - sl_dist
        tp_price = entry_price + tp_dist
    else:
        sl_price = entry_price + sl_dist
        tp_price = entry_price - tp_dist

    if ticks.empty:
        return {"outcome": "NO_DATA", "pnl_R": 0.0, "fire_ts": None, "exit_ts": None}

    # Find fire
    if side == "BUY":
        fire_mask = ticks["ask"] >= entry_price
    else:
        fire_mask = ticks["bid"] <= entry_price
    fire_idx = fire_mask.idxmax() if fire_mask.any() else None
    if fire_idx is None or not fire_mask.iloc[fire_idx if isinstance(fire_idx, int) else ticks.index.get_loc(fire_idx)]:
        return {"outcome": "NO_FIRE", "pnl_R": 0.0, "fire_ts": None, "exit_ts": None}

    fire_ts = ticks.loc[fire_idx, "ts"]
    post = ticks.loc[fire_idx:].copy()
    if post.empty:
        return {"outcome": "NO_FIRE", "pnl_R": 0.0, "fire_ts": fire_ts.isoformat(), "exit_ts": None}

    # Walk ticks for SL/TP
    if side == "BUY":
        sl_h = post.index[post["bid"] <= sl_price]
        tp_h = post.index[post["bid"] >= tp_price]
    else:
        sl_h = post.index[post["ask"] >= sl_price]
        tp_h = post.index[post["ask"] <= tp_price]

    sl_first = sl_h[0] if len(sl_h) else None
    tp_first = tp_h[0] if len(tp_h) else None

    if sl_first is None and tp_first is None:
        # Expired at watch_end with neither hit
        last_ts = post["ts"].iloc[-1]
        last_mid = (post["bid"].iloc[-1] + post["ask"].iloc[-1]) / 2
        if side == "BUY":
            mtm_pts = (last_mid - entry_price) / POINT
        else:
            mtm_pts = (entry_price - last_mid) / POINT
        return {"outcome": "EXPIRE", "pnl_R": float(mtm_pts / sl_pts),
                "fire_ts": fire_ts.isoformat(), "exit_ts": last_ts.isoformat()}

    if sl_first is not None and (tp_first is None or sl_first < tp_first):
        return {"outcome": "SL", "pnl_R": -1.0,
                "fire_ts": fire_ts.isoformat(),
                "exit_ts": post.loc[sl_first, "ts"].isoformat()}
    return {"outcome": "TP", "pnl_R": float(rr),
            "fire_ts": fire_ts.isoformat(),
            "exit_ts": post.loc[tp_first, "ts"].isoformat()}


def classify_dominant(buy_out: str, sell_out: str) -> str:
    """Bucket the day's directional bias from BUY+SELL outcomes."""
    if buy_out == "TP" and sell_out == "SL":
        return "UP_TREND"        # BUY worked, SELL faked
    if buy_out == "SL" and sell_out == "TP":
        return "DOWN_TREND"      # SELL worked, BUY faked
    if buy_out == "TP" and sell_out == "TP":
        return "BOTH_TP"         # high-vol both ran
    if buy_out == "SL" and sell_out == "SL":
        return "BOTH_SL"         # chop both faked
    if buy_out == "NO_FIRE" and sell_out == "NO_FIRE":
        return "INSIDE_RANGE"    # price never broke
    if buy_out == "NO_FIRE":
        return "SELL_ONLY"
    if sell_out == "NO_FIRE":
        return "BUY_ONLY"
    return "MIXED"


def process_day(ticks: pd.DataFrame, day: date) -> dict | None:
    asia_start, asia_end = asia_window_utc(day)
    watch_start, watch_end = watch_window_utc(day)

    asia_slc = ticks[(ticks["ts"] >= asia_start) & (ticks["ts"] < asia_end)]
    if asia_slc.empty:
        return None

    asia_high = float(asia_slc["ask"].max())
    asia_low = float(asia_slc["bid"].min())
    range_pts = (asia_high - asia_low) / POINT

    watch_slc = ticks[(ticks["ts"] >= watch_start) & (ticks["ts"] < watch_end)].reset_index(drop=True)

    buy_res = simulate_breakout(watch_slc, "BUY", asia_high, SL_PTS, RR_RATIO, watch_end)
    sell_res = simulate_breakout(watch_slc, "SELL", asia_low, SL_PTS, RR_RATIO, watch_end)

    return {
        "date": day.isoformat(),
        "asia_start_utc": asia_start.isoformat(),
        "asia_end_utc": asia_end.isoformat(),
        "asia_high": round(asia_high, 2),
        "asia_low": round(asia_low, 2),
        "asia_range_pts": round(range_pts, 1),
        "buy_outcome": buy_res["outcome"],
        "buy_pnl_R": round(buy_res["pnl_R"], 3),
        "buy_fire_ts": buy_res["fire_ts"] or "",
        "buy_exit_ts": buy_res["exit_ts"] or "",
        "sell_outcome": sell_res["outcome"],
        "sell_pnl_R": round(sell_res["pnl_R"], 3),
        "sell_fire_ts": sell_res["fire_ts"] or "",
        "sell_exit_ts": sell_res["exit_ts"] or "",
        "dominant": classify_dominant(buy_res["outcome"], sell_res["outcome"]),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, help="YYYY-MM-DD inclusive")
    ap.add_argument("--end", type=str, help="YYYY-MM-DD inclusive")
    ap.add_argument("--days", type=int, help="Backfill last N days (alt to start/end)")
    ap.add_argument("--account", default="sim", choices=["sim", "live"])
    args = ap.parse_args()

    today = datetime.now(timezone.utc).date()
    if args.days:
        start_d, end_d = today - timedelta(days=args.days), today
    elif args.start and args.end:
        start_d = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_d = datetime.strptime(args.end, "%Y-%m-%d").date()
    else:
        start_d = end_d = today

    print(f"=== Asia-session logger  {start_d} -> {end_d} (real UTC) ===\n")
    print(f"  Config: asia 23:00->04:00 UTC, watch 04:00->22:00, SL={SL_PTS}pt RR={RR_RATIO}\n")

    spec = init_account(args.account)
    sym = spec.symbol
    try:
        # Need 1 day pre-buffer (Asia starts day-before at 23:00 UTC)
        load_start = datetime(start_d.year, start_d.month, start_d.day, tzinfo=timezone.utc) - timedelta(hours=2)
        load_end = datetime(end_d.year, end_d.month, end_d.day, 23, 59, tzinfo=timezone.utc) + timedelta(hours=1)
        print(f"  Loading ticks {sym} {load_start.date()} -> {load_end.date()}...")
        ticks = load_ticks(sym, load_start, load_end, spread_pts=0)
        if ticks.empty:
            print("  No ticks loaded.")
            return 1
        ticks["ts"] = pd.to_datetime(ticks["ts"], utc=True)
        ticks = ticks.sort_values("ts").reset_index(drop=True)

        results = []
        d = start_d
        while d <= end_d:
            row = process_day(ticks, d)
            if row is not None:
                results.append(row)
            d = (pd.Timestamp(d) + pd.Timedelta(days=1)).date()

        if not results:
            print("  No sessions in window.")
            return 0

        # Console table
        print(f"\n  {'Date':<12} {'Range':>6}  {'BUY':<8} {'BUY R':>6}  {'SELL':<8} {'SELL R':>6}  Dominant")
        print(f"  {'-'*12} {'-'*6}  {'-'*8} {'-'*6}  {'-'*8} {'-'*6}  {'-'*12}")
        for r in results:
            print(f"  {r['date']:<12} {r['asia_range_pts']:>6.0f}  "
                  f"{r['buy_outcome']:<8} {r['buy_pnl_R']:>+6.2f}  "
                  f"{r['sell_outcome']:<8} {r['sell_pnl_R']:>+6.2f}  {r['dominant']}")

        # Distribution histogram
        from collections import Counter
        dist = Counter(r["dominant"] for r in results)
        print(f"\n  Dominant-pattern distribution ({len(results)} sessions):")
        for label, n in sorted(dist.items(), key=lambda kv: -kv[1]):
            pct = n / len(results) * 100
            bar = "#" * int(pct / 3)
            print(f"    {label:<14} {n:>3} ({pct:>4.0f}%)  {bar}")

        # Append/upsert to CSV (deduped by date)
        out_dir = ROOT / "output"
        out_dir.mkdir(exist_ok=True)
        csv_path = out_dir / "asia_session_log.csv"

        existing = {}
        if csv_path.exists():
            edf = pd.read_csv(csv_path)
            for _, row in edf.iterrows():
                existing[str(row["date"])] = dict(row)

        for r in results:
            existing[r["date"]] = r

        cols = ["date", "asia_start_utc", "asia_end_utc",
                "asia_high", "asia_low", "asia_range_pts",
                "buy_outcome", "buy_pnl_R", "buy_fire_ts", "buy_exit_ts",
                "sell_outcome", "sell_pnl_R", "sell_fire_ts", "sell_exit_ts",
                "dominant"]
        rows_out = sorted(existing.values(), key=lambda r: r["date"])
        with csv_path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in rows_out:
                w.writerow({c: r.get(c, "") for c in cols})
        print(f"\n  Wrote {len(rows_out)} rows to {csv_path}")

    finally:
        try:
            import MetaTrader5 as mt5
            mt5.shutdown()
        except Exception:
            pass
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
