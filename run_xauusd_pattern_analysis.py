"""XAUUSD weekly regime analysis — ATR-CV and directional bias.

Reads M1 OHLC data from parquet cache and computes weekly regime metrics:
  - ATR-CV: coefficient of variation of daily ATR (volatility regime)
  - dir_bias: absolute directional bias (trending vs. ranging)

Regime gate logic (FBO_FVG_v1):
  - ATR-CV < 0.45 AND dir_bias < 0.15 for 2+ weeks  -> cut risk 50%
  - ATR-CV < 0.45 AND dir_bias < 0.15 for 3+ weeks  -> stop trading
  - ATR-CV > 0.60 for 2+ consecutive weeks           -> consider early reopt (4w IS window)

Usage:
    python run_xauusd_pattern_analysis.py
"""
from __future__ import annotations

import io, sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

DATA_PATH   = Path("output/market_data/xauusd_m1_6m.parquet")
LOOKBACK_W  = 8    # weeks of history to show
ATR_PERIOD  = 14   # daily ATR period

# Thresholds
CV_LOW_GATE  = 0.45   # below this = low vol (possible range/chop)
CV_HIGH_GATE = 0.60   # above this = high vol (possible trend — consider 4w IS)
BIAS_GATE    = 0.15   # below this = weak directional bias


def _load_m1(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"M1 data not found: {path}\n"
            "Run run_download_xauusd_m1.py first to download market data."
        )
    df = pd.read_parquet(path)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.sort_values("datetime").reset_index(drop=True)


def _resample_daily(m1: pd.DataFrame) -> pd.DataFrame:
    m1 = m1.set_index("datetime")
    daily = m1.resample("D", label="left", closed="left").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
    ).dropna()
    return daily.reset_index()


def _compute_atr(daily: pd.DataFrame, period: int = 14) -> pd.Series:
    high  = daily["high"].values
    low   = daily["low"].values
    close = daily["close"].values
    tr    = np.maximum(high - low,
             np.maximum(np.abs(high - np.roll(close, 1)),
                        np.abs(low  - np.roll(close, 1))))
    tr[0] = high[0] - low[0]
    atr = pd.Series(tr).rolling(period).mean()
    return atr


def _resample_weekly(daily: pd.DataFrame, atr: pd.Series) -> pd.DataFrame:
    daily = daily.copy()
    daily["atr"] = atr.values

    daily["datetime"] = pd.to_datetime(daily["datetime"])
    daily = daily.set_index("datetime")

    weekly = daily.resample("W-FRI", label="right", closed="right").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        atr_mean=("atr", "mean"),
        atr_std=("atr", "std"),
        atr_min=("atr", "min"),
        atr_max=("atr", "max"),
        days=("atr", "count"),
    ).dropna(subset=["open", "close"])
    return weekly.reset_index()


def _compute_metrics(weekly: pd.DataFrame) -> pd.DataFrame:
    df = weekly.copy()

    # ATR-CV: std/mean of daily ATR within week
    df["atr_cv"] = df["atr_std"] / df["atr_mean"]
    df["atr_cv"] = df["atr_cv"].fillna(0.0)

    # Directional bias: |close - open| / (high - low) per week
    # Ranges 0 (pure range candle) to 1 (full directional)
    hl = df["high"] - df["low"]
    df["dir_bias"] = np.where(hl > 0, np.abs(df["close"] - df["open"]) / hl, 0.0)

    # Weekly return direction
    df["direction"] = np.where(df["close"] > df["open"], "UP", "DOWN")

    # Regime classification
    def _classify(row):
        cv     = row["atr_cv"]
        bias   = row["dir_bias"]
        if cv > CV_HIGH_GATE:
            return "HIGH-VOL"
        elif cv < CV_LOW_GATE and bias < BIAS_GATE:
            return "CHOP"
        elif cv < CV_LOW_GATE:
            return "LOW-VOL"
        else:
            return "NORMAL"

    df["regime"] = df.apply(_classify, axis=1)
    return df


def _gate_check(recent: pd.DataFrame) -> list[str]:
    warnings = []
    if len(recent) < 2:
        return warnings

    # Check last N weeks for sustained chop
    for lookback in [3, 2]:
        if len(recent) >= lookback:
            tail = recent.tail(lookback)
            chop_weeks = ((tail["atr_cv"] < CV_LOW_GATE) & (tail["dir_bias"] < BIAS_GATE)).sum()
            if chop_weeks >= lookback:
                if lookback == 3:
                    warnings.append(
                        f"STOP TRADING: {lookback} consecutive chop weeks "
                        f"(ATR-CV<{CV_LOW_GATE} + dir_bias<{BIAS_GATE})"
                    )
                else:
                    warnings.append(
                        f"CUT RISK 50%: {lookback} consecutive chop weeks "
                        f"(ATR-CV<{CV_LOW_GATE} + dir_bias<{BIAS_GATE})"
                    )
                break

    # Check for high-vol (4w IS hint)
    if len(recent) >= 2:
        tail2 = recent.tail(2)
        high_vol_weeks = (tail2["atr_cv"] > CV_HIGH_GATE).sum()
        if high_vol_weeks >= 2:
            warnings.append(
                f"CONSIDER 4w IS: {high_vol_weeks} consecutive high-vol weeks "
                f"(ATR-CV>{CV_HIGH_GATE})"
            )

    return warnings


def main():
    print("=" * 72)
    print(f"  XAUUSD Weekly Regime Analysis  (today: {date.today()})")
    print(f"  ATR-CV gate: <{CV_LOW_GATE}=low-vol  >{CV_HIGH_GATE}=high-vol")
    print(f"  Dir-bias gate: <{BIAS_GATE}=chop")
    print("=" * 72)

    m1    = _load_m1(DATA_PATH)
    print(f"\n  M1 data: {len(m1):,} bars  ({m1['datetime'].min().date()} -> {m1['datetime'].max().date()})")

    daily  = _resample_daily(m1)
    atr    = _compute_atr(daily, ATR_PERIOD)
    weekly = _resample_weekly(daily, atr)
    weekly = _compute_metrics(weekly)

    # Show last LOOKBACK_W weeks
    recent = weekly.tail(LOOKBACK_W).copy()

    print(f"\n  {'Week End':<12}  {'Dir':>5}  {'ATR-CV':>7}  {'DirBias':>8}  {'Regime':<10}  "
          f"{'ATR Avg':>8}  {'Days':>4}")
    print("  " + "-" * 68)
    for _, row in recent.iterrows():
        week_end_str = str(row["datetime"].date()) if hasattr(row["datetime"], "date") else str(row["datetime"])[:10]
        cv_flag   = " *" if row["atr_cv"] < CV_LOW_GATE or row["atr_cv"] > CV_HIGH_GATE else "  "
        bias_flag = " *" if row["dir_bias"] < BIAS_GATE else "  "
        print(f"  {week_end_str:<12}  {row['direction']:>5}  "
              f"{row['atr_cv']:>6.3f}{cv_flag}  {row['dir_bias']:>7.3f}{bias_flag}  "
              f"{row['regime']:<10}  {row['atr_mean']:>8.2f}  {int(row['days']):>4}")

    # Gate check
    warnings = _gate_check(recent)
    if warnings:
        print(f"\n{'='*72}")
        print("  *** REGIME GATE ALERTS ***")
        for w in warnings:
            print(f"  -> {w}")
        print("=" * 72)
    else:
        print(f"\n  Regime gate: OK (no alerts)")

    # Summary stats
    if len(recent) > 0:
        print(f"\n  Last {len(recent)}w summary:")
        print(f"    ATR-CV:   avg={recent['atr_cv'].mean():.3f}  "
              f"min={recent['atr_cv'].min():.3f}  max={recent['atr_cv'].max():.3f}")
        print(f"    DirBias:  avg={recent['dir_bias'].mean():.3f}  "
              f"min={recent['dir_bias'].min():.3f}  max={recent['dir_bias'].max():.3f}")
        regime_counts = recent["regime"].value_counts()
        print(f"    Regimes:  {dict(regime_counts)}")

    print("=" * 72)


if __name__ == "__main__":
    main()
