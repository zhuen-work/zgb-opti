"""Shared regime classification helpers.

Session-level features:
  - range_pts:        ORB box width during the 90-min range window
  - atr_h1_20_pts:    ATR over 20 H1 bars at session start (lookback context)
  - range_atr_ratio:  range_pts / atr_h1_20_pts (relative volatility)

Regime label (rule-based MVP — replace with KMeans once we have >=30 live sessions):
  TIGHT   : range_pts <  1500 OR ratio < 1.2   (chop / whipsaw)
  WIDE    : range_pts >  3500 OR ratio > 2.5   (cluster-break / trend)
  NORMAL  : everything else

Thresholds calibrated from 2026-05-13 (range 1183 = TIGHT) and 2026-05-14
(range 4397 = WIDE).
"""
from __future__ import annotations
from datetime import datetime, timezone, timedelta, date
from typing import Optional

import pandas as pd

# Session config in REAL UTC (EA uses TimeGMT after the 2026-05-07 fix).
SESSION_CFG = {
    "LDN": {"start_h_real_utc": 4,  "range_min": 90},
    "NY":  {"start_h_real_utc": 10, "range_min": 90},
}
BROKER_OFFSET_H = 3  # Vantage = UTC+3 (summer) / UTC+2 (winter, late Oct - late Mar).
# Hardcoded default for backward compat with existing callers (mostly sim/WFO scripts
# which only run on summer-period historical data). Live scripts should pass the
# auto-detected offset via the `broker_offset_h` arg in session_window_broker()
# to be DST-safe. Per feedback_no_unverified_account_claims.md (2026-05-15).
POINT = 0.01
ATR_LOOKBACK_H1 = 20

# Regime thresholds (MVP). EITHER absolute range_pts OR relative ratio can trigger.
TIGHT_RANGE_PTS = 1500
WIDE_RANGE_PTS = 3500
TIGHT_RATIO = 1.2
WIDE_RATIO = 2.5


def classify_regime(range_pts: Optional[float], range_atr_ratio: Optional[float]) -> str:
    """Rule-based regime label from observed features."""
    if range_pts is None or pd.isna(range_pts):
        return "UNKNOWN"
    if range_pts < TIGHT_RANGE_PTS:
        return "TIGHT"
    if range_pts > WIDE_RANGE_PTS:
        return "WIDE"
    if range_atr_ratio is not None and not pd.isna(range_atr_ratio):
        if range_atr_ratio < TIGHT_RATIO:
            return "TIGHT"
        if range_atr_ratio > WIDE_RATIO:
            return "WIDE"
    return "NORMAL"


def session_window_real_utc(day: date, session: str) -> tuple[datetime, datetime]:
    """(range_start, range_end) for given session, in REAL UTC."""
    cfg = SESSION_CFG[session]
    start = datetime(day.year, day.month, day.day, cfg["start_h_real_utc"], 0, tzinfo=timezone.utc)
    end = start + timedelta(minutes=cfg["range_min"])
    return start, end


def session_window_broker(day: date, session: str,
                            broker_offset_h: int = BROKER_OFFSET_H
                            ) -> tuple[pd.Timestamp, pd.Timestamp]:
    """(range_start, range_end) for given session, in BROKER-time labels.

    Used when the tick stream has broker-labeled-as-UTC timestamps (Vantage MT5).

    `broker_offset_h` defaults to module constant (3 = summer) for backward
    compatibility. Live scripts should pass the auto-detected offset:
        from zgb_sim.mt5_accounts import get_broker_offset
        off = int(get_broker_offset(sym).total_seconds() // 3600)
        rng_start, rng_end = session_window_broker(day, session, broker_offset_h=off)
    """
    cfg = SESSION_CFG[session]
    start_broker_h = cfg["start_h_real_utc"] + broker_offset_h
    base = pd.Timestamp(day).normalize().tz_localize("UTC")
    rng_start = base.replace(hour=start_broker_h, minute=0)
    rng_end = rng_start + pd.Timedelta(minutes=cfg["range_min"])
    return rng_start, rng_end


def compute_range_pts(ticks: pd.DataFrame, rng_start, rng_end) -> float:
    """ORB range_pts = (max_mid - min_mid) / point during the range window."""
    if ticks.empty:
        return float("nan")
    slc = ticks[(ticks["ts"] >= rng_start) & (ticks["ts"] < rng_end)]
    if slc.empty:
        return float("nan")
    return float((slc["mid"].max() - slc["mid"].min()) / POINT)


def build_h1_bars(ticks: pd.DataFrame) -> pd.DataFrame:
    """Resample mid-price ticks to H1 OHLC bars."""
    if ticks.empty:
        return pd.DataFrame()
    df = ticks[["ts", "mid"]].set_index("ts")
    h1 = df["mid"].resample("1h").agg(["first", "max", "min", "last"]).dropna()
    h1.columns = ["open", "high", "low", "close"]
    return h1


def compute_atr(h1_bars: pd.DataFrame, lookback: int = ATR_LOOKBACK_H1) -> pd.Series:
    """Wilder-style ATR(lookback) on H1 bars. Returns series indexed by bar close ts."""
    if len(h1_bars) < 2:
        return pd.Series(dtype=float)
    h, l, c = h1_bars["high"], h1_bars["low"], h1_bars["close"]
    prev_c = c.shift(1)
    tr = pd.concat([(h - l), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(lookback, min_periods=max(5, lookback // 4)).mean()


def lookup_atr_pts_at(atr_series: pd.Series, ts: pd.Timestamp) -> float:
    """Most-recent ATR value strictly before `ts`, converted to points."""
    if atr_series.empty:
        return float("nan")
    prior = atr_series.loc[atr_series.index < ts]
    if prior.empty:
        return float("nan")
    return float(prior.iloc[-1]) / POINT


def regime_for_session(ticks: pd.DataFrame, atr_series: pd.Series,
                         day: date, session: str,
                         broker_time_labels: bool = True) -> dict:
    """All-in-one: compute features + label for one session.

    broker_time_labels=True: ticks are broker-time-labeled-as-UTC (Vantage MT5 deals/ticks)
    broker_time_labels=False: ticks are real-UTC labels (sim/cached parquet)
    """
    if broker_time_labels:
        rng_start, rng_end = session_window_broker(day, session)
    else:
        rng_start_dt, rng_end_dt = session_window_real_utc(day, session)
        rng_start, rng_end = pd.Timestamp(rng_start_dt), pd.Timestamp(rng_end_dt)

    range_pts = compute_range_pts(ticks, rng_start, rng_end)
    atr_pts = lookup_atr_pts_at(atr_series, rng_start) if not atr_series.empty else float("nan")
    ratio = range_pts / atr_pts if (atr_pts and not pd.isna(atr_pts) and atr_pts > 0) else float("nan")
    regime = classify_regime(range_pts, ratio)
    return {
        "session": session,
        "range_pts": range_pts,
        "atr_h1_20_pts": atr_pts,
        "range_atr_ratio": ratio,
        "regime": regime,
    }


def load_regime_log(csv_path) -> dict:
    """Read regime_log.csv into a dict keyed by (date_str, session) for fast lookup."""
    from pathlib import Path
    p = Path(csv_path)
    if not p.exists():
        return {}
    df = pd.read_csv(p)
    out = {}
    for _, r in df.iterrows():
        out[(str(r["date"]), str(r["session"]))] = {
            "regime": str(r["regime"]),
            "range_pts": float(r["range_pts"]) if pd.notna(r["range_pts"]) else None,
            "atr_h1_20_pts": float(r["atr_h1_20_pts"]) if pd.notna(r["atr_h1_20_pts"]) else None,
            "range_atr_ratio": float(r["range_atr_ratio"]) if pd.notna(r["range_atr_ratio"]) else None,
        }
    return out
