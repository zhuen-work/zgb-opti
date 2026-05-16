"""ORB session regime classifier — labels each LDN/NY session by volatility regime.

Per-session features:
  - range_pts:        ORB box width (high-low during 90-min range window)
  - atr_h1_20_pts:    ATR over the previous 20 H1 bars at session start (lookback context)
  - range_atr_ratio:  range_pts / atr_h1_20_pts (relative volatility — current vs recent)

Regime label (MVP rule-based — to be replaced by KMeans once we have >=30 sessions):
  TIGHT   : range_pts <  1500 OR range_atr_ratio < 1.2  (chop/whipsaw country)
  WIDE    : range_pts >  3500 OR range_atr_ratio > 2.5  (cluster-break / trend regime)
  NORMAL  : everything else

These thresholds are first-pass calibrations from:
  - 2026-05-13 (whipsaw): range_pts=1183 -> TIGHT
  - 2026-05-14 (cluster-trend): range_pts=4397 -> WIDE
  - WFO IS median session range ~= 2500 -> NORMAL center

Usage:
  python scripts/regime_classifier.py --days 30        # backfill last 30 days
  python scripts/regime_classifier.py --start 2026-04-25 --end 2026-05-14
  python scripts/regime_classifier.py --today          # today only (for live use)

Outputs:
  - output/regime_log.csv (append-safe, deduped by date+session)
  - Console: per-day summary + regime histogram
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

# Session config — real UTC (EA uses TimeGMT)
SESSION_CFG = {
    "LDN": {"start_h": 4,  "range_min": 90},  # 04:00-05:30 real UTC
    "NY":  {"start_h": 10, "range_min": 90},  # 10:00-11:30 real UTC
}
POINT = 0.01

# Regime thresholds (rule-based MVP)
TIGHT_RANGE_PTS = 1500
WIDE_RANGE_PTS = 3500
TIGHT_RATIO = 1.2
WIDE_RATIO = 2.5
ATR_LOOKBACK_H1 = 20


def build_h1_bars(ticks: pd.DataFrame) -> pd.DataFrame:
    """Resample mid-price ticks to H1 OHLC bars (real-UTC labels)."""
    if ticks.empty:
        return pd.DataFrame()
    df = ticks[["ts", "mid"]].set_index("ts")
    h1 = df["mid"].resample("1h").agg(["first", "max", "min", "last"]).dropna()
    h1.columns = ["open", "high", "low", "close"]
    return h1


def compute_atr(h1_bars: pd.DataFrame, lookback: int = ATR_LOOKBACK_H1) -> pd.Series:
    """Wilder's ATR(lookback) on H1 bars. Returns series indexed by bar close ts."""
    if len(h1_bars) < 2:
        return pd.Series(dtype=float)
    h, l, c = h1_bars["high"], h1_bars["low"], h1_bars["close"]
    prev_c = c.shift(1)
    tr = pd.concat([(h - l), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    # Simple moving avg (Wilder smoothing is similar within 0.5% over 20 bars)
    return tr.rolling(lookback, min_periods=max(5, lookback // 4)).mean()


def classify_regime(range_pts: float, range_atr_ratio: float) -> str:
    """Rule-based regime label."""
    if pd.isna(range_pts):
        return "UNKNOWN"
    if range_pts < TIGHT_RANGE_PTS or (not pd.isna(range_atr_ratio) and range_atr_ratio < TIGHT_RATIO):
        return "TIGHT"
    if range_pts > WIDE_RANGE_PTS or (not pd.isna(range_atr_ratio) and range_atr_ratio > WIDE_RATIO):
        return "WIDE"
    return "NORMAL"


def classify_session(ticks: pd.DataFrame, h1_atr: pd.Series,
                       session: str, day: date) -> dict | None:
    """Compute features + label for one LDN/NY session on `day`."""
    cfg = SESSION_CFG[session]
    rng_start = datetime(day.year, day.month, day.day, cfg["start_h"], 0, tzinfo=timezone.utc)
    rng_end = rng_start + timedelta(minutes=cfg["range_min"])

    win = ticks[(ticks["ts"] >= pd.Timestamp(rng_start)) &
                 (ticks["ts"] <  pd.Timestamp(rng_end))]
    if win.empty:
        return None

    range_pts = (win["mid"].max() - win["mid"].min()) / POINT

    # ATR at session start: most recent ATR value strictly before rng_start
    atr_pts = float("nan")
    if not h1_atr.empty:
        prior = h1_atr.loc[h1_atr.index < pd.Timestamp(rng_start)]
        if not prior.empty:
            atr_price = float(prior.iloc[-1])
            atr_pts = atr_price / POINT

    ratio = range_pts / atr_pts if (atr_pts and not pd.isna(atr_pts) and atr_pts > 0) else float("nan")
    regime = classify_regime(range_pts, ratio)

    return {
        "date":             day.isoformat(),
        "session":          session,
        "session_start_utc": rng_start.isoformat(),
        "range_pts":        round(range_pts, 1),
        "atr_h1_20_pts":    round(atr_pts, 1) if not pd.isna(atr_pts) else None,
        "range_atr_ratio":  round(ratio, 2) if not pd.isna(ratio) else None,
        "regime":           regime,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end",   type=str, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--days",  type=int, help="Backfill last N days (alt to start/end)")
    ap.add_argument("--today", action="store_true", help="Today only")
    ap.add_argument("--account", default="live", choices=["live", "sim"])
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    today = now.date()
    if args.today:
        start_d, end_d = today, today
    elif args.days:
        start_d, end_d = today - timedelta(days=args.days), today
    elif args.start and args.end:
        start_d = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_d = datetime.strptime(args.end, "%Y-%m-%d").date()
    else:
        # Default: last 14 days
        start_d, end_d = today - timedelta(days=14), today

    print(f"=== Regime classifier  {start_d} -> {end_d}  (real UTC) ===\n")

    spec = init_account(args.account)
    sym = spec.symbol
    try:
        # Need 24h pre-buffer for ATR lookback
        start_dt = datetime(start_d.year, start_d.month, start_d.day, tzinfo=timezone.utc) - timedelta(hours=30)
        end_dt = datetime(end_d.year, end_d.month, end_d.day, 23, 59, tzinfo=timezone.utc) + timedelta(hours=1)
        print(f"  Loading ticks {sym} {start_dt.date()} -> {end_dt.date()} ...")
        ticks = load_ticks(sym, start_dt, end_dt, spread_pts=0)
        if ticks.empty:
            print("  No ticks loaded.")
            return 1
        # tick_loader returns df with 'ts' as datetime and 'bid'/'ask'
        if "mid" not in ticks.columns:
            ticks = ticks.copy()
            ticks["mid"] = (ticks["bid"] + ticks["ask"]) / 2.0
        if "ts" not in ticks.columns:
            # Older cache may store time differently — fall back
            raise RuntimeError("Tick frame missing 'ts' column")
        ticks["ts"] = pd.to_datetime(ticks["ts"], utc=True)

        # Build H1 bars + ATR series ONCE for the whole range
        print(f"  Building H1 bars + ATR({ATR_LOOKBACK_H1})...")
        h1 = build_h1_bars(ticks)
        atr_series = compute_atr(h1)

        # Iterate each day & each session
        results = []
        d = start_d
        while d <= end_d:
            for sess in ("LDN", "NY"):
                row = classify_session(ticks, atr_series, sess, d)
                if row is not None:
                    results.append(row)
            d += timedelta(days=1)

        if not results:
            print("  No sessions found in window.")
            return 0

        # Console: per-row table + histogram
        print(f"\n  {'Date':<12} {'Sess':<5} {'Start UTC':<20} {'Range':>6} {'ATR':>5} "
              f"{'R/ATR':>6}  Regime")
        print(f"  {'-'*12} {'-'*5} {'-'*20} {'-'*6} {'-'*5} {'-'*6}  {'-'*8}")
        for r in results:
            atr_s = f"{r['atr_h1_20_pts']:>5.0f}" if r['atr_h1_20_pts'] is not None else "  n/a"
            ratio_s = f"{r['range_atr_ratio']:>6.2f}" if r['range_atr_ratio'] is not None else "   n/a"
            print(f"  {r['date']:<12} {r['session']:<5} {r['session_start_utc'][:19]:<20} "
                  f"{r['range_pts']:>6.0f} {atr_s} {ratio_s}  {r['regime']}")

        # Histogram
        regimes = [r["regime"] for r in results]
        print(f"\n  Regime distribution ({len(results)} sessions):")
        for label in ("TIGHT", "NORMAL", "WIDE", "UNKNOWN"):
            n = regimes.count(label)
            pct = n / len(regimes) * 100 if regimes else 0
            bar = "#" * int(pct / 3)
            print(f"    {label:<8} {n:>3} ({pct:>4.0f}%)  {bar}")

        # Append to CSV (deduped by date+session — re-run replaces)
        out_dir = ROOT / "output"
        out_dir.mkdir(exist_ok=True)
        csv_path = out_dir / "regime_log.csv"

        existing = {}
        if csv_path.exists():
            existing_df = pd.read_csv(csv_path)
            for _, row in existing_df.iterrows():
                existing[(row["date"], row["session"])] = dict(row)

        for r in results:
            existing[(r["date"], r["session"])] = r

        cols = ["date", "session", "session_start_utc", "range_pts",
                "atr_h1_20_pts", "range_atr_ratio", "regime"]
        rows_out = sorted(existing.values(),
                            key=lambda r: (r["date"], 0 if r["session"] == "LDN" else 1))
        with csv_path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in rows_out:
                w.writerow({c: r.get(c, "") for c in cols})
        print(f"\n  Wrote {len(rows_out)} sessions to {csv_path}")

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
