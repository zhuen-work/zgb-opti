"""Live regime check — quick read of today's ORB regime.

Run this AFTER LDN range closes (>=05:30 real UTC = 08:30 broker) and/or
AFTER NY range closes (>=11:30 real UTC = 14:30 broker). Tells you what
regime we're in BEFORE pendings expire (gives 4h of advance warning).

Outputs for the current/just-completed session:
  - range_pts (the actual ORB box width)
  - atr_h1_20_pts (lookback ATR)
  - range_atr_ratio
  - regime label (TIGHT / NORMAL / WIDE)
  - decision guidance: which streams historically favor / disfavor this regime

Usage:
  python scripts/live_regime_check.py            # auto-detect which session is live
  python scripts/live_regime_check.py --session LDN
  python scripts/live_regime_check.py --session NY
"""
from __future__ import annotations
import sys
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd
import MetaTrader5 as mt5

from zgb_sim.mt5_accounts import init_account, get_broker_offset
from zgb_sim.tick_loader import kill_mt5_terminal
from zgb_sim.regime import (
    session_window_broker, build_h1_bars, compute_atr, lookup_atr_pts_at,
    classify_regime, compute_range_pts,
    TIGHT_RANGE_PTS, WIDE_RANGE_PTS, TIGHT_RATIO, WIDE_RATIO,
)

# Guidance for each regime (early thesis — refine as live data accumulates).
REGIME_GUIDANCE = {
    "TIGHT":  ("Whipsaw country. SLs likely fire fast, TPs often miss. "
                "Reverse-hedge would have won on 2026-05-13."),
    "NORMAL": ("Mixed regime. No strong directional bias. Expect mix of TPs and SLs."),
    "WIDE":   ("Cluster-break / trend day. Once price breaks, often continues. "
                "Hedges generally don't fire (price never returns to entry). "
                "Wider-SL streams (S4, R=650pt) historically worst on these days."),
    "UNKNOWN": ("Can't classify — session not yet closed or no tick data."),
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--session", choices=["LDN", "NY", "auto"], default="auto",
                    help="Which session to read (auto picks most-recently-closed)")
    ap.add_argument("--account", default="live", choices=["live", "sim"])
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    today = now.date()

    # Init MT5 first so we can auto-detect broker offset (DST-safe).
    spec = init_account(args.account)
    sym = spec.symbol
    try:
        broker_off = get_broker_offset(spec.symbol)
        cur_broker_h = (now + broker_off).hour

        # Auto-detect session: pick the most-recently-CLOSED ORB range.
        # LDN range closes 08:30 broker. NY range closes 14:30 broker.
        if args.session == "auto":
            if cur_broker_h >= 15:        # past NY range close
                session = "NY"
            elif cur_broker_h >= 13:      # NY range forming
                session = "NY"
                print(f"  [note] NY range is still forming (closes 14:30 broker). Showing in-progress read.")
            elif cur_broker_h >= 9:       # past LDN range close (broker 08:30)
                session = "LDN"
            elif cur_broker_h >= 7:       # LDN range forming
                session = "LDN"
                print(f"  [note] LDN range is still forming (closes 08:30 broker). Showing in-progress read.")
            else:
                print(f"  [info] Pre-session (broker hour {cur_broker_h}). Both LDN/NY ranges not yet formed.")
                print(f"  [info] LDN range opens 07:00 broker (04:00 UTC). NY opens 13:00 broker (10:00 UTC).")
                return 0
        else:
            session = args.session

        print(f"=== Live regime check  {today}  session={session}  broker_hour={cur_broker_h} ===\n")

        # Pull enough history for the 20-bar H1 ATR plus today's session.
        # BROKER-TZ FIX: copy_ticks_range treats datetime args as broker time.
        # Shift bounds forward by `broker_off` (auto-detected) so MT5 sees
        # current broker wall-clock. See feedback_no_unverified_account_claims.md.
        end = now + broker_off + timedelta(minutes=5)
        start = (now - timedelta(hours=30)).replace(minute=0, second=0, microsecond=0) + broker_off
        arr = mt5.copy_ticks_range(sym, start, end, mt5.COPY_TICKS_ALL)
        if arr is None or len(arr) == 0:
            print(f"  [error] No ticks pulled. last_error={mt5.last_error()}")
            return 1
        ticks = pd.DataFrame(arr)
        ticks["ts"] = pd.to_datetime(ticks["time_msc"], unit="ms", utc=True)
        ticks["mid"] = (ticks["bid"] + ticks["ask"]) / 2.0

        # Compute features
        # Pass auto-detected offset so session boundaries are DST-safe.
        broker_off_h = int(broker_off.total_seconds() // 3600)
        rng_start, rng_end = session_window_broker(today, session, broker_offset_h=broker_off_h)
        range_pts = compute_range_pts(ticks, rng_start, rng_end)
        h1_bars = build_h1_bars(ticks)
        atr_series = compute_atr(h1_bars)
        atr_pts = lookup_atr_pts_at(atr_series, rng_start)
        ratio = range_pts / atr_pts if (atr_pts and not pd.isna(atr_pts) and atr_pts > 0) else float("nan")
        regime = classify_regime(range_pts, ratio)

        # Output
        print(f"  Range window (broker): {rng_start.strftime('%Y-%m-%d %H:%M')} -> "
              f"{rng_end.strftime('%H:%M')}")
        print(f"  Symbol: {sym}")
        print()
        print(f"  range_pts:        {range_pts:>7.0f}    (TIGHT <{TIGHT_RANGE_PTS}, WIDE >{WIDE_RANGE_PTS})")
        atr_s = f"{atr_pts:>7.0f}" if not pd.isna(atr_pts) else "    n/a"
        print(f"  atr_h1_20_pts:    {atr_s}")
        ratio_s = f"{ratio:>7.2f}" if not pd.isna(ratio) else "    n/a"
        print(f"  range_atr_ratio:  {ratio_s}    (TIGHT <{TIGHT_RATIO}, WIDE >{WIDE_RATIO})")
        print()
        print(f"  >>> REGIME = {regime}")
        print()
        print(f"  Guidance: {REGIME_GUIDANCE[regime]}")

        # Pending status: are pendings still alive? (pendings expire 240min after range close)
        # rng_end is broker-time-labeled-as-UTC; current real-UTC "now" needs broker offset to compare.
        pending_expire = rng_end + pd.Timedelta(minutes=240)
        now_broker_labeled = pd.Timestamp(now + broker_off).tz_convert("UTC")
        if now_broker_labeled < rng_end:
            print(f"\n  Pendings not yet placed. Range still forming.")
        elif now_broker_labeled < pending_expire:
            minutes_left = int((pending_expire - now_broker_labeled).total_seconds() / 60)
            print(f"\n  Pendings active. Expire in ~{minutes_left} min "
                  f"(at {pending_expire.strftime('%H:%M')} broker).")
            if regime == "WIDE":
                print(f"  *** Consider: pause / risk-reduce on WIDE regime.")
        else:
            print(f"\n  Pendings already expired (or fired) — session is past.")

    finally:
        mt5.shutdown()
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
