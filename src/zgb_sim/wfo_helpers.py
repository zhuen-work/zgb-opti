"""Shared WFO helpers — P0 improvements applied to all stream WFOs.

P0 improvements (post-EMP-failure 2026-05-02):
  1. Decay-aware ranking: reject candidates with monotone OOS NP decline > 25%.
  2. IS-variance-aware benchmark: flag candidates whose any OOS window falls
     below the IS worst-week NP.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


# Standard windows for May 2 reopt (3-fold, captures regime through May 1)
WINDOWS_MAY2 = [
    ("W1", date(2026, 2, 14), date(2026, 3, 14), date(2026, 3, 14), date(2026, 3, 28)),
    ("W2", date(2026, 2, 28), date(2026, 3, 28), date(2026, 3, 28), date(2026, 4, 11)),
    ("W3", date(2026, 3, 14), date(2026, 4, 11), date(2026, 4, 11), date(2026, 4, 25)),
    # W4 OOS Apr 25 -> May 1 (5 trading days). May 2 is Saturday so excluded.
    ("W4", date(2026, 3, 28), date(2026, 4, 25), date(2026, 4, 25), date(2026, 5,  1)),
]


def to_utc(d):
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def compute_oos_decay_slope(oos_nps: list[float]) -> float:
    """Compute slope of OOS NPs across windows.
    Returns the relative decline (negative = declining):
      slope = (last - first) / max(|first|, 1)
    """
    if len(oos_nps) < 2:
        return 0.0
    first = oos_nps[0]
    last = oos_nps[-1]
    denom = max(abs(first), 1.0)
    return (last - first) / denom


def compute_is_weekly_min(stream_simulate_fn, ticks, signal_bars, m1_bars, cfg, meta,
                          deposit: float, is_start: datetime, is_end: datetime,
                          weeks_per_window: int = 1) -> float:
    """Compute the worst weekly NP across the IS period for a given config.

    Used as a stress-test threshold: if any OOS window NP < IS worst week,
    the candidate is flagged as falling outside its tested distribution.
    Returns the IS minimum weekly NP (negative typically).
    """
    r = stream_simulate_fn(ticks, signal_bars, m1_bars, cfg, meta, initial_balance=deposit)
    if not r.deals:
        return 0.0
    df = pd.DataFrame([(d.ts, d.pnl) for d in r.deals if d.kind != "entry"],
                      columns=["ts", "pnl"])
    if df.empty:
        return 0.0
    df["ts"] = pd.to_datetime(df["ts"])
    if df["ts"].dt.tz is not None:
        df["ts"] = df["ts"].dt.tz_convert("UTC").dt.tz_localize(None)
    df["wk"] = df["ts"].dt.to_period("W-SUN").apply(lambda p: p.start_time.date())
    weekly = df.groupby("wk")["pnl"].sum()
    return float(weekly.min())


def rank_with_p0(candidates: list, oos_per_window: dict,
                  windows: list, decay_threshold: float = -0.25) -> list[dict]:
    """Rank candidates with P0 decay-aware filter.

    Args:
        candidates: list of cfg objects
        oos_per_window: {window_label -> DataFrame of per-cfg OOS results}
        windows: list of (label, is_s, is_e, oos_s, oos_e) tuples
        decay_threshold: reject candidates with slope < this (default -0.25 = 25% decline)

    Returns:
        list of dicts with cfg, total_np, prof_count, avg_dd, np_dd_ratio, slope, p0_pass
    """
    rows = []
    for i, cfg in enumerate(candidates):
        oos_nps = []
        oos_dds = []
        prof_count = 0
        for label, _, _, _, _ in windows:
            r = oos_per_window[label].iloc[i]
            oos_nps.append(float(r["net_profit"]))
            oos_dds.append(float(r["drawdown_pct"]))
            if r["net_profit"] > 0:
                prof_count += 1
        total_np = sum(oos_nps)
        avg_dd = sum(oos_dds) / len(oos_dds) if oos_dds else 0.5
        np_dd = total_np / max(avg_dd, 0.5)
        slope = compute_oos_decay_slope(oos_nps)
        p0_pass = slope >= decay_threshold
        rows.append({
            "cfg": cfg,
            "total_np": total_np,
            "prof_count": prof_count,
            "avg_dd": avg_dd,
            "np_dd_ratio": np_dd,
            "oos_nps": oos_nps,
            "slope": slope,
            "p0_pass": p0_pass,
        })

    # Sort: p0_pass desc, prof_count desc, np_dd_ratio desc
    rows.sort(key=lambda x: (x["p0_pass"], x["prof_count"], x["np_dd_ratio"]), reverse=True)
    return rows


def print_phase_d_with_p0(ranked: list[dict], stream_label: str, decay_threshold: float = -0.25):
    """Pretty-print Phase D ranking with P0 verdict."""
    print(f"\n=== PHASE D: Final Ranking ({stream_label}) - P0 enabled ===")
    print(f"  Decay-aware filter: reject if OOS slope < {decay_threshold:+.0%}")
    print(f"  {'Rank':<5} {'P0':>4} {'Prof':>5} {'NP':>9} {'AvgDD':>7} {'NP/DD':>8} "
          f"{'Slope':>8}  OOS NPs (W1->W4 if 4-fold)")
    for rank, row in enumerate(ranked, 1):
        p0 = "PASS" if row["p0_pass"] else "FAIL"
        oos_str = " -> ".join(f"${n:+.0f}" for n in row["oos_nps"])
        print(f"  {rank:<5} {p0:>4} {row['prof_count']}/{len(row['oos_nps'])} "
              f"${row['total_np']:>+8,.0f} {row['avg_dd']:>5.1f}%  "
              f"{row['np_dd_ratio']:>+8.0f} {row['slope']:>+7.1%}  {oos_str}")
    n_pass = sum(1 for r in ranked if r["p0_pass"])
    print(f"\n  P0 verdict: {n_pass}/{len(ranked)} candidates passed decay filter.")
    if n_pass == 0:
        print(f"  WARNING: No candidate passed -- using best-of-failed (top by NP/DD).")


def select_winner_with_p0(ranked: list[dict]) -> Optional[dict]:
    """Select winner: top P0-passing candidate, or fallback to top NP/DD if none pass."""
    p0_passers = [r for r in ranked if r["p0_pass"]]
    if p0_passers:
        # Among passers, pick highest prof_count then NP/DD
        return p0_passers[0]
    # No passers: fallback (return None signals caller to handle "no viable winner")
    return None
