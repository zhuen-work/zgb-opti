"""Study 2026 XAUUSD tick history (Feb -> May 1).

Goal: characterize the actual market regime so we can propose a strategy
that fits what gold IS doing, not what we'd like it to do. Focus on:

  1. Volatility profile (daily range, intraday ATR)
  2. Session character (Asia/LDN/NY ranges, expansion vs contraction)
  3. Time-of-day edges (continuation vs reversal by hour)
  4. Trend vs range classification (per-day)
  5. Move distribution (fat tails / impulse moves)
  6. Strategy implications — what archetypes might still work?
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks


SYMBOL = "XAUUSD"


def main() -> int:
    start = datetime(2026, 2, 1, tzinfo=timezone.utc)
    end = datetime(2026, 5, 2, tzinfo=timezone.utc)

    try:
        m = symbol_meta(SYMBOL)
        point = m["point"]
        print(f"Loading 2026 ticks Feb 1 -> May 2 (real broker data)...")
        ticks = load_ticks(SYMBOL, start, end, spread_pts=0)  # raw spreads for study
        print(f"  Loaded {len(ticks):,} ticks")

        ticks = ticks.copy()
        ticks["mid"] = (ticks["bid"] + ticks["ask"]) / 2
        ticks["spread_pts"] = (ticks["ask"] - ticks["bid"]) / point
        ticks["dt"] = pd.to_datetime(ticks["ts"])

        # -------- Resample to M15 OHLC --------
        ticks_idx = ticks.set_index("dt")
        m15 = ticks_idx["mid"].resample("15min").agg(["first", "max", "min", "last"]).dropna()
        m15.columns = ["open", "high", "low", "close"]
        m15["range_pts"] = (m15["high"] - m15["low"]) / point
        m15["return_pts"] = (m15["close"] - m15["open"]) / point
        m15["abs_return_pts"] = m15["return_pts"].abs()
        m15["hour"] = m15.index.hour
        m15["dow"] = m15.index.dayofweek
        # Filter weekends (Sat/Sun)
        m15 = m15[m15["dow"] < 5]
        # Filter zero-range bars (closed market)
        m15 = m15[m15["range_pts"] > 0]

        print("\n" + "=" * 88)
        print("  2026 XAUUSD CHARACTER STUDY (Feb 2 -> May 1, M15 mid-price)")
        print("=" * 88)

        # === 1. Volatility profile ===
        print("\n  == 1. VOLATILITY PROFILE ==")
        m15_avg = m15["range_pts"].mean()
        m15_med = m15["range_pts"].median()
        m15_p90 = m15["range_pts"].quantile(0.9)
        m15_p99 = m15["range_pts"].quantile(0.99)
        print(f"  M15 range pts:  mean={m15_avg:.0f}  median={m15_med:.0f}  "
              f"p90={m15_p90:.0f}  p99={m15_p99:.0f}")
        print(f"  M15 spread:     mean={ticks['spread_pts'].mean():.1f}pts  "
              f"median={ticks['spread_pts'].median():.0f}pts")

        # Daily ranges
        m15["date"] = m15.index.date
        daily = m15.groupby("date").agg(
            daily_high=("high", "max"),
            daily_low=("low", "min"),
        )
        daily["daily_range_pts"] = (daily["daily_high"] - daily["daily_low"]) / point
        print(f"  Daily range:    mean={daily['daily_range_pts'].mean():.0f}  "
              f"median={daily['daily_range_pts'].median():.0f}  "
              f"p10={daily['daily_range_pts'].quantile(0.1):.0f}  "
              f"p90={daily['daily_range_pts'].quantile(0.9):.0f}")

        # === 2. Session character ===
        print("\n  == 2. SESSION CHARACTER (UTC) ==")
        sessions = {
            "Asia (00-06)":  (0, 6),
            "Pre-LDN(06-07)": (6, 7),
            "LDN  (07-12)":  (7, 12),
            "US-LDN(12-13)": (12, 13),
            "NY   (13-17)":  (13, 17),
            "Late (17-21)":  (17, 21),
            "After(21-24)":  (21, 24),
        }
        print(f"  {'Session':<14} {'M15_count':>10} {'avg_range':>10} {'avg_|ret|':>10} {'p90_range':>10}")
        for name, (h0, h1) in sessions.items():
            sub = m15[(m15["hour"] >= h0) & (m15["hour"] < h1)]
            if len(sub) == 0:
                continue
            print(f"  {name:<14} {len(sub):>10,} "
                  f"{sub['range_pts'].mean():>10.0f} "
                  f"{sub['abs_return_pts'].mean():>10.0f} "
                  f"{sub['range_pts'].quantile(0.9):>10.0f}")

        # === 3. Continuation vs reversal at session opens ===
        print("\n  == 3. CONTINUATION VS REVERSAL ==")
        print("  After a strong M15 bar (|ret| > p75), does the next M15 bar:")
        m15["next_return_pts"] = m15["return_pts"].shift(-1)
        threshold = m15["abs_return_pts"].quantile(0.75)
        strong = m15[m15["abs_return_pts"] >= threshold].dropna(subset=["next_return_pts"])
        if len(strong) > 0:
            same_dir = ((strong["return_pts"] > 0) & (strong["next_return_pts"] > 0)) | \
                       ((strong["return_pts"] < 0) & (strong["next_return_pts"] < 0))
            print(f"    Continue same direction: {same_dir.sum()}/{len(strong)} "
                  f"({same_dir.mean()*100:.1f}%)")
            # Median continuation magnitude
            cont = strong[same_dir]["next_return_pts"].abs()
            rev = strong[~same_dir]["next_return_pts"].abs()
            print(f"    Median continuation move: {cont.median():.0f}pts  (n={len(cont)})")
            print(f"    Median reversal move: {rev.median():.0f}pts  (n={len(rev)})")
            edge_per_strong = (cont.sum() - rev.sum()) / len(strong)
            print(f"    Net edge per strong-bar follow: {edge_per_strong:+.1f}pts")

        # === 4. Per-hour continuation edge ===
        print("\n  == 4. HOURLY CONTINUATION EDGE ==")
        print("  After strong bar (|ret| >= p75), per-hour same-direction rate:")
        print(f"  {'Hour UTC':<10} {'n':>5} {'CONT%':>7} {'Med_cont':>9} {'Med_rev':>9} {'Net_edge':>9}")
        hourly_edge = []
        for h in range(0, 24):
            sub = strong[strong["hour"] == h]
            if len(sub) < 5:
                continue
            same = ((sub["return_pts"] > 0) & (sub["next_return_pts"] > 0)) | \
                   ((sub["return_pts"] < 0) & (sub["next_return_pts"] < 0))
            cont_pct = same.mean() * 100
            cont_med = sub[same]["next_return_pts"].abs().median() if same.any() else 0
            rev_med = sub[~same]["next_return_pts"].abs().median() if (~same).any() else 0
            edge = cont_med * (cont_pct/100) - rev_med * (1 - cont_pct/100)
            hourly_edge.append((h, len(sub), cont_pct, cont_med, rev_med, edge))
            print(f"  {h:>4}:00     {len(sub):>5} {cont_pct:>6.1f}% "
                  f"{cont_med:>9.0f} {rev_med:>9.0f} {edge:>+9.1f}")

        # === 5. Trend day vs range day classification ===
        print("\n  == 5. TREND DAY vs RANGE DAY ==")
        # Trend day: |close - open| / range > 0.5  (close near extreme)
        # Range day: |close - open| / range < 0.3
        daily_open_close = m15.groupby("date").agg(
            day_open=("open", "first"),
            day_close=("close", "last"),
            day_high=("high", "max"),
            day_low=("low", "min"),
        )
        daily_open_close["day_range"] = daily_open_close["day_high"] - daily_open_close["day_low"]
        daily_open_close["body_ratio"] = (
            (daily_open_close["day_close"] - daily_open_close["day_open"]).abs() /
            daily_open_close["day_range"].clip(lower=point)
        )
        trend_days = daily_open_close[daily_open_close["body_ratio"] > 0.5]
        range_days = daily_open_close[daily_open_close["body_ratio"] < 0.3]
        mixed = daily_open_close[(daily_open_close["body_ratio"] >= 0.3) &
                                   (daily_open_close["body_ratio"] <= 0.5)]
        n_total = len(daily_open_close)
        print(f"  Trend days (body/range > 0.5):  {len(trend_days):>3}  ({len(trend_days)/n_total*100:.0f}%)")
        print(f"  Range days (body/range < 0.3):  {len(range_days):>3}  ({len(range_days)/n_total*100:.0f}%)")
        print(f"  Mixed (0.3 - 0.5):              {len(mixed):>3}  ({len(mixed)/n_total*100:.0f}%)")

        # === 6. Move distribution ===
        print("\n  == 6. M15 RETURN DISTRIBUTION (impulse / fat tail check) ==")
        ret_pts = m15["return_pts"].dropna()
        std = ret_pts.std()
        kurt = ret_pts.kurt()
        skew = ret_pts.skew()
        # Count "impulse moves" (>3σ in 1 bar)
        impulses = ret_pts[ret_pts.abs() > 3 * std]
        print(f"  Std (M15 ret pts):    {std:.0f}")
        print(f"  Kurtosis:             {kurt:.1f}  (>3 = fatter than normal)")
        print(f"  Skew:                 {skew:+.2f}  (- = more big down moves)")
        print(f"  Impulses (|ret|>3σ):  {len(impulses)}/{len(ret_pts)} "
              f"({len(impulses)/len(ret_pts)*100:.2f}%)")

        # === 7. Volatility regime per week ===
        print("\n  == 7. WEEKLY VOLATILITY REGIME ==")
        m15["week"] = m15.index.to_period("W")
        weekly = m15.groupby("week").agg(
            avg_range=("range_pts", "mean"),
            n_bars=("range_pts", "count"),
            cum_ret=("return_pts", "sum"),
        )
        weekly["regime"] = pd.cut(weekly["avg_range"], bins=3,
                                   labels=["LOW_VOL", "MID_VOL", "HIGH_VOL"])
        print(f"  {'Week':<12} {'avg_range':>10} {'n_bars':>7} {'cum_ret':>9} {'regime':>10}")
        for wk, row in weekly.iterrows():
            print(f"  {str(wk):<12} {row['avg_range']:>10.0f} "
                  f"{int(row['n_bars']):>7} {row['cum_ret']:>+9.0f} "
                  f"{str(row['regime']):>10}")

    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
