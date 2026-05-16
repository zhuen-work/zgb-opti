"""Cluster-correlation risk-reduction analyzer.

Question: does reducing per-stream risk on days with WIDE Asia range improve
portfolio NP/DD$ by avoiding cluster losses?

Background: when all 6 streams fire same direction in the same session, effective
exposure is 2-3x what the WFO assumes (per-stream-independence). MAY9 WFO does
not model this correlation. If we can predict cluster-likely days BEFORE LDN
range fires, we can pre-emptively reduce risk.

Predictor: Asia range_pts (broker 23:00 prior day -> 04:00 today, 5h window).
Already captured in output/asia_session_log.csv but only for live days.
For 77-day MAY9 OOS, compute Asia range from cached ticks.

Method:
  1. Load 77-day baseline parent sims (existing 6-stream MAY9 deals)
  2. Per day, compute Asia range_pts from cached ticks
  3. Per day, compute aggregate portfolio PnL (sum of 6 streams)
  4. Sweep grid: (Asia_threshold X) x (risk_multiplier M)
     For each cell, counterfactual = if Asia > X on that day, multiply day's
     PnL by M. Aggregate to portfolio NP/DD$.
  5. Report best cell + sensitivity table.

Risk_multiplier interpretation:
  M=1.0  : no change (baseline)
  M=0.5  : halve per-stream risk on flagged days
  M=0.25 : quarter risk
  M=0.0  : skip the day entirely (= disable EA)

Usage:
  python scripts/sim_cluster_risk_filter.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from collections import defaultdict

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.wfo_helpers import WINDOWS_MAY9 as WINDOWS, to_utc

from sim_wfo_hedge_retry import (STREAM_CFGS, make_stream_cfg,
                                  SYMBOL, DEPOSIT, SPREAD, POINT, CONTRACT,
                                  PARENT_RISK_PROD)


# Asia range definition (broker hours, matches asia_session_log.py convention)
ASIA_START_HOUR_BROKER = 23   # of PRIOR broker day
ASIA_DURATION_H = 5

# Sweep grid
ASIA_THRESHOLDS = [1500, 2000, 2500, 3000, 3500, 4000, 5000]  # in points
RISK_MULTIPLIERS = [0.0, 0.25, 0.5, 0.75]   # what we apply when threshold exceeded


def compute_asia_ranges(ticks: pd.DataFrame, start_date: date, end_date: date) -> dict:
    """For each broker date in [start_date, end_date], compute Asia range_pts.

    Returns {date: asia_range_pts}. Skips days without sufficient ticks.
    """
    ticks = ticks.copy()
    ticks["mid"] = (ticks["bid"] + ticks["ask"]) / 2.0
    out = {}
    d = start_date
    while d <= end_date:
        # Asia window in broker-as-UTC labels
        asia_end_wall = pd.Timestamp(d).tz_localize("UTC").replace(hour=4, minute=0)
        asia_start_wall = asia_end_wall - pd.Timedelta(hours=ASIA_DURATION_H)
        slc = ticks[(ticks["ts"] >= asia_start_wall) & (ticks["ts"] < asia_end_wall)]
        if len(slc) >= 100:  # at least 100 ticks for a valid range
            range_pts = (slc["mid"].max() - slc["mid"].min()) / POINT
            out[d] = float(range_pts)
        d = (pd.Timestamp(d) + pd.Timedelta(days=1)).date()
    return out


def build_per_day_pnl(per_stream_deals: dict) -> dict:
    """Aggregate per-stream deals into per-day portfolio PnL.

    Returns {date: total_pnl_$}.
    """
    by_day = defaultdict(float)
    for stream, deals in per_stream_deals.items():
        for d in deals:
            ts = pd.Timestamp(d["ts_ns"], tz="UTC")
            by_day[ts.date()] += d["pnl"]
    return dict(by_day)


def run_baseline_with_direction(stream: str, ticks, m1, m5, meta, risk_pct: float):
    """Run parent sim and return list of dicts with deal context."""
    cfg = make_stream_cfg(stream, risk_pct)
    r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
    open_positions = []
    closed = []
    for d in r.deals:
        ts_ns = pd.Timestamp(d.ts).value
        if d.kind == "entry":
            open_positions.append({
                "entry_ts_ns": ts_ns,
                "direction": int(d.direction),
                "lots": float(d.lots),
                "entry_price": float(d.price),
            })
            continue
        match_idx = -1
        for i, op in enumerate(open_positions):
            if op["direction"] == int(d.direction):
                match_idx = i; break
        if match_idx < 0:
            continue
        op = open_positions.pop(match_idx)
        closed.append({
            "ts_ns": ts_ns,
            "entry_ts_ns": op["entry_ts_ns"],
            "direction": int(d.direction),
            "entry_price": op["entry_price"],
            "exit_price": float(d.price),
            "lots": op["lots"],
            "pnl": float(d.pnl),
            "kind": d.kind,
        })
    return closed


def aggregate_running(per_day_pnl: dict) -> tuple[float, float, float]:
    """Compute (total_NP, max_DD%, NP/DD$) from per-day PnL dict."""
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    for d in sorted(per_day_pnl.keys()):
        bal += per_day_pnl[d]
        if bal > bal_max:
            bal_max = bal
        if (bal_max - bal) > dd_abs:
            dd_abs = bal_max - bal
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    ndd = (np_ / dd_abs) if dd_abs > 0 else 0
    return np_, dd_pct, ndd


def main() -> int:
    print("=" * 110)
    print("  CLUSTER-CORRELATION RISK FILTER  (Asia-range -> per-day risk multiplier)")
    print(f"  Predictor: Asia range_pts (broker 23:00 prior day -> 04:00 today)")
    print(f"  Sweep: thresholds {ASIA_THRESHOLDS}pt x multipliers {RISK_MULTIPLIERS}")
    print("=" * 110)

    try:
        from zgb_sim.mt5_accounts import init_account
        init_account("sim")
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        # Need to extend start back 1 day so Asia window for first day is covered
        start = to_utc(WINDOWS[0][1]) - timedelta(days=1)
        end = to_utc(WINDOWS[-1][4])
        print(f"\n  Loading data {start.date()} -> {end.date()}...")
        ticks = load_ticks(SYMBOL, start, end, spread_pts=SPREAD)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)
        print(f"  ticks={len(ticks):,}  M1={len(m1):,}  M5={len(m5):,}")

        # Compute Asia ranges per broker date
        print(f"\n  Computing Asia ranges...")
        first_day = (to_utc(WINDOWS[0][1])).date()
        last_day = (to_utc(WINDOWS[-1][4])).date()
        asia_ranges = compute_asia_ranges(ticks, first_day, last_day)
        print(f"  {len(asia_ranges)} days with valid Asia data")

        # Distribution
        ranges_sorted = sorted(asia_ranges.values())
        print(f"  Asia range_pts distribution: "
              f"min={ranges_sorted[0]:.0f}, p25={ranges_sorted[len(ranges_sorted)//4]:.0f}, "
              f"med={ranges_sorted[len(ranges_sorted)//2]:.0f}, "
              f"p75={ranges_sorted[3*len(ranges_sorted)//4]:.0f}, max={ranges_sorted[-1]:.0f}")

        # Run baseline parent sims for all 6 streams (1.5% risk each, full 77d)
        print(f"\n  Running baseline parent sims at {PARENT_RISK_PROD}% per stream...")
        per_stream_deals = {}
        for stream in STREAM_CFGS:
            deals = run_baseline_with_direction(stream, ticks, m1, m5, meta, PARENT_RISK_PROD)
            per_stream_deals[stream] = deals
            print(f"    {stream}: {len(deals)} closed deals")

        # Aggregate per-day PnL
        per_day_pnl = build_per_day_pnl(per_stream_deals)
        print(f"\n  {len(per_day_pnl)} trading days with P&L")

        # Per-day breakdown table sorted by Asia range desc (top 10 widest days)
        print(f"\n  Top-10 widest Asia days (sorted desc):")
        print(f"  {'Date':<12} {'Asia Rg':>8}  {'Day PnL':>10}  {'Cumulative':>11}")
        days_with_both = sorted(
            ((d, asia_ranges.get(d, float('nan')), per_day_pnl[d])
             for d in per_day_pnl if d in asia_ranges),
            key=lambda x: -x[1]
        )
        cum = 0
        for d, asia_r, pnl in days_with_both[:10]:
            cum += pnl
            print(f"  {d.isoformat():<12} {asia_r:>8.0f}  ${pnl:>+8,.0f}  ${cum:>+9,.0f}")

        # Baseline portfolio metrics
        b_np, b_dd, b_ndd = aggregate_running(per_day_pnl)
        print(f"\n  BASELINE (no filter): NP=${b_np:+,.0f}  DD={b_dd:.2f}%  NP/DD$={b_ndd:.2f}")

        # Sweep grid: for each (threshold, multiplier), counterfactual portfolio
        print(f"\n  --- COUNTERFACTUAL SWEEP ---")
        print(f"  Rule: if Asia_range > THRESHOLD that day, multiply day's PnL by MULTIPLIER")
        print()
        print(f"  {'Threshold':>10}  {'Mult':>6}  {'Days hit':>9}  {'NP':>10}  {'DD%':>6}  "
              f"{'NP/DD$':>7}  {'vs base NP':>12}  {'vs base NP/DD$':>16}")
        print(f"  {'-'*10}  {'-'*6}  {'-'*9}  {'-'*10}  {'-'*6}  {'-'*7}  {'-'*12}  {'-'*16}")

        results = []
        for thr in ASIA_THRESHOLDS:
            for mult in RISK_MULTIPLIERS:
                cf_pnl = {}
                hit_days = 0
                for d, pnl in per_day_pnl.items():
                    asia_r = asia_ranges.get(d)
                    if asia_r is not None and asia_r > thr:
                        cf_pnl[d] = pnl * mult
                        hit_days += 1
                    else:
                        cf_pnl[d] = pnl
                np_, dd, ndd = aggregate_running(cf_pnl)
                d_np = np_ - b_np
                d_ndd = ndd - b_ndd
                results.append({
                    "threshold": thr, "multiplier": mult, "days_hit": hit_days,
                    "np": np_, "dd": dd, "ndd": ndd, "d_np": d_np, "d_ndd": d_ndd
                })
                print(f"  {thr:>10}  {mult:>6.2f}  {hit_days:>9}  ${np_:>+8,.0f}  {dd:>5.2f}%  "
                      f"{ndd:>+7.2f}  ${d_np:>+10,.0f}  {d_ndd:>+15.2f}")

        # Best cell by NP/DD$
        best = max(results, key=lambda r: r["ndd"])
        print(f"\n  >>> BEST cell by NP/DD$:")
        print(f"      Threshold={best['threshold']}pt, multiplier={best['multiplier']:.2f}, "
              f"days hit={best['days_hit']}")
        print(f"      NP=${best['np']:+,.0f} (vs ${b_np:+,.0f}, delta=${best['d_np']:+,.0f})")
        print(f"      NP/DD$={best['ndd']:+.2f} (vs {b_ndd:+.2f}, delta={best['d_ndd']:+.2f})")
        if best['ndd'] > b_ndd:
            print(f"      *** IMPROVES portfolio NP/DD$ by {best['d_ndd']:+.2f} ***")
        else:
            print(f"      No improvement found.")

    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
