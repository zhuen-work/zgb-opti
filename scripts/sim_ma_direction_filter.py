"""MA-direction-filter test on parent ORB streams.

HYPOTHESES:
  WITH-trend filter: place BUY_STOP only if price > MA at session start;
                     place SELL_STOP only if price < MA at session start.
                     (= bet ORB break in the direction of the trend.)
  AGAINST-trend filter (inverse): place BUY_STOP only if price < MA;
                     place SELL_STOP only if price > MA.
                     (= bet ORB break against the trend = mean reversion.)

Counterfactual: re-run the existing parent sim deals through each filter, dropping
trades on the suppressed side. Compare NP/DD$ and PF vs no-filter baseline.

Sweep: MA period in {20, 50, 100, 200} on H1 bars, mode in {WITH, AGAINST}, all 6
parent streams, MAY9 windows.

Usage:
  python scripts/sim_ma_direction_filter.py
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path

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
                                  slice_window, aggregate, ts_arr_from_ticks,
                                  SYMBOL, DEPOSIT, SPREAD, POINT, CONTRACT,
                                  PARENT_RISK_SWEEP, PARENT_RISK_PROD)


# ==== Filter sweep grid ====
MA_PERIODS = [20, 50, 100, 200]   # H1 bars
MA_TIMEFRAME = "H1"
MODES = ["WITH", "AGAINST", "NONE"]   # NONE = baseline (no filter)


def run_baseline_with_direction(stream: str, ticks, m1, m5, meta, risk_pct: float):
    """Run parent sim and return list of dicts with full deal context.

    Each dict: {ts_ns, entry_ts_ns, direction (+1/-1), entry_price, sl_price, pnl, kind}
    """
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
        # find matching open position FIFO same direction
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


def build_h1_sma(bars_h1: pd.DataFrame, period: int) -> pd.Series:
    """SMA of close on H1 bars. Returns Series indexed by bar-close ts (broker-as-UTC)."""
    if bars_h1.empty:
        return pd.Series(dtype=float)
    s = bars_h1.set_index("ts")["close"]
    return s.rolling(period, min_periods=max(5, period // 4)).mean()


def lookup_ma_at(ma_series: pd.Series, ts_ns: int) -> float:
    """Most-recent MA value strictly before ts_ns. Returns nan if none."""
    if ma_series.empty:
        return float("nan")
    ts = pd.Timestamp(ts_ns, tz="UTC")
    prior = ma_series.loc[ma_series.index < ts]
    if prior.empty:
        return float("nan")
    return float(prior.iloc[-1])


def apply_ma_filter(closed_deals: list, ma_series: pd.Series, mode: str) -> list:
    """Filter deals based on MA rule applied at ENTRY time.

    Mode "NONE":   keep all
    Mode "WITH":   keep BUY  iff entry_price > MA
                   keep SELL iff entry_price < MA
    Mode "AGAINST": keep BUY  iff entry_price < MA
                    keep SELL iff entry_price > MA
    """
    if mode == "NONE":
        return list(closed_deals)
    out = []
    for d in closed_deals:
        ma = lookup_ma_at(ma_series, d["entry_ts_ns"])
        if pd.isna(ma):
            continue
        is_buy = d["direction"] == 1
        # Use entry_price (= price at the moment the pending fired).
        # Equivalent to using the price at session start since BUY_STOP at range_high
        # only fires when price >= range_high. If price is breaking up THROUGH
        # range_high, we already know price > range_high > some MA threshold.
        if mode == "WITH":
            keep = (is_buy and d["entry_price"] > ma) or (not is_buy and d["entry_price"] < ma)
        else:  # AGAINST
            keep = (is_buy and d["entry_price"] < ma) or (not is_buy and d["entry_price"] > ma)
        if keep:
            out.append(d)
    return out


def aggregate_dicts(closed_deals: list) -> tuple[float, float, float, int]:
    """Same aggregation as sim_wfo_hedge_retry.aggregate but for dict list."""
    pairs = [(d["ts_ns"], d["pnl"]) for d in closed_deals]
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gp = gl = 0.0
    for _, p in sorted(pairs, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p > 0: gp += p
        elif p < 0: gl += p
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    pf = (gp / abs(gl)) if gl < 0 else float("inf")
    return np_, dd_pct, pf, len(closed_deals)


def main() -> int:
    print("=" * 110)
    print("  MA-DIRECTION-FILTER TEST  (counterfactual on baseline parent ORB deals)")
    print(f"  Sweep: MA periods {MA_PERIODS} on {MA_TIMEFRAME} bars × modes {MODES}")
    print(f"  Streams: {list(STREAM_CFGS.keys())}")
    print(f"  Windows: MAY9 ({len(WINDOWS)} folds aggregated)")
    print("=" * 110)

    try:
        from zgb_sim.mt5_accounts import init_account
        init_account("sim")
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        start = to_utc(WINDOWS[0][1])
        end = to_utc(WINDOWS[-1][4])
        print(f"\n  Loading data {start.date()} -> {end.date()}...")
        full_ticks = load_ticks(SYMBOL, start, end, spread_pts=SPREAD)
        full_m1 = load_bars(SYMBOL, "M1", start, end)
        full_m5 = load_bars(SYMBOL, "M5", start, end)
        full_h1 = load_bars(SYMBOL, "H1", start, end)
        print(f"  ticks={len(full_ticks):,}  M1={len(full_m1):,}  M5={len(full_m5):,}  H1={len(full_h1):,}")

        # Pre-compute MA series for each period, then run baseline once per stream
        print(f"\n  Building MA series...")
        ma_by_period = {p: build_h1_sma(full_h1, p) for p in MA_PERIODS}

        # Run baseline once per stream over the FULL period (covers all 4 windows)
        print(f"\n  Running baseline parent sims at risk={PARENT_RISK_PROD}% per stream...")
        per_stream_deals = {}
        for stream in STREAM_CFGS:
            deals = run_baseline_with_direction(stream, full_ticks, full_m1, full_m5,
                                                  meta, PARENT_RISK_PROD)
            per_stream_deals[stream] = deals
            print(f"    {stream}: {len(deals)} closed deals")

        # Apply each filter and report per-stream + portfolio
        print(f"\n" + "=" * 110)
        print(f"  PER-STREAM filter outcomes (1.5% per stream, $10k start)")
        print("=" * 110)

        results = {}  # results[(period, mode)] = {stream: (np, dd, pf, n)}
        # NONE baseline
        results[("baseline", "NONE")] = {
            s: aggregate_dicts(per_stream_deals[s]) for s in STREAM_CFGS
        }
        for period in MA_PERIODS:
            for mode in ["WITH", "AGAINST"]:
                key = (f"H1_{period}", mode)
                results[key] = {}
                for s in STREAM_CFGS:
                    filtered = apply_ma_filter(per_stream_deals[s], ma_by_period[period], mode)
                    results[key][s] = aggregate_dicts(filtered)

        # Print per-stream comparison
        for stream in STREAM_CFGS:
            print(f"\n  {stream:<3}  {'Filter':<22} {'NP':>10} {'DD%':>6} {'NP/DD$':>8} "
                  f"{'PF':>5} {'Trades':>7}  {'vs base NP':>11}")
            base_np, base_dd, base_pf, base_n = results[("baseline", "NONE")][stream]
            base_ndd = (base_np / (base_dd/100 * (DEPOSIT + base_np))) if base_dd > 0 else 0
            print(f"     {'baseline (no filter)':<22} ${base_np:>+8,.0f} {base_dd:>5.2f}% "
                  f"{base_ndd:>+7.2f} {base_pf:>5.2f} {base_n:>7}        --")
            for period in MA_PERIODS:
                for mode in ["WITH", "AGAINST"]:
                    np_, dd, pf, n = results[(f"H1_{period}", mode)][stream]
                    ndd = (np_ / (dd/100 * (DEPOSIT + np_))) if dd > 0 else 0
                    label = f"H1-MA({period}) {mode}"
                    delta = np_ - base_np
                    print(f"     {label:<22} ${np_:>+8,.0f} {dd:>5.2f}% {ndd:>+7.2f} "
                          f"{pf:>5.2f} {n:>7} ${delta:>+9,.0f}")

        # Portfolio aggregate
        print(f"\n" + "=" * 110)
        print(f"  PORTFOLIO COMPARISON (all 6 streams combined)")
        print("=" * 110)
        all_baseline = sum((per_stream_deals[s] for s in STREAM_CFGS), [])
        b_np, b_dd, b_pf, b_n = aggregate_dicts(all_baseline)
        b_ndd = (b_np / (b_dd/100 * (DEPOSIT + b_np))) if b_dd > 0 else 0
        print(f"\n  {'Filter':<22} {'NP':>10} {'DD%':>6} {'NP/DD$':>8} {'PF':>5} {'Trades':>7}  "
              f"{'vs base NP':>11}  {'vs base NP/DD$':>15}")
        print(f"  {'baseline (no filter)':<22} ${b_np:>+8,.0f} {b_dd:>5.2f}% {b_ndd:>+7.2f} "
              f"{b_pf:>5.2f} {b_n:>7}        --              --")
        for period in MA_PERIODS:
            for mode in ["WITH", "AGAINST"]:
                combined = sum(
                    (apply_ma_filter(per_stream_deals[s], ma_by_period[period], mode)
                     for s in STREAM_CFGS), []
                )
                np_, dd, pf, n = aggregate_dicts(combined)
                ndd = (np_ / (dd/100 * (DEPOSIT + np_))) if dd > 0 else 0
                label = f"H1-MA({period}) {mode}"
                d_np = np_ - b_np
                d_ndd = ndd - b_ndd
                print(f"  {label:<22} ${np_:>+8,.0f} {dd:>5.2f}% {ndd:>+7.2f} "
                      f"{pf:>5.2f} {n:>7} ${d_np:>+9,.0f}  {d_ndd:>+14.2f}")

    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
