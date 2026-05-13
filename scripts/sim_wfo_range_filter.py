"""WFO sweep for range filter mode (MIN-only OR MAX-only), v2.2 sim.

Usage:
  python scripts/sim_wfo_range_filter.py --mode min   # sweep MIN, MAX disabled
  python scripts/sim_wfo_range_filter.py --mode max   # sweep MAX, MIN disabled

Each candidate threshold is evaluated as a 6-stream portfolio across the
4 WFO windows (IS + OOS). Parent entry params LOCKED at current v2.1 winners.

Output:
  output/wfo_range_filter_<mode>_may9/winner.json
  Phase D rank table + portfolio compare (filter ON vs filter OFF)
"""
from __future__ import annotations

import sys
import time
import json
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.wfo_helpers import (WINDOWS_MAY9 as WINDOWS, rank_with_p0,
                                  print_phase_d_with_p0, select_winner_with_p0,
                                  check_winner_boundaries, print_boundary_check, to_utc)

# Reuse v2.1 winners (locked entry params; matches live)
from sim_wfo_hedge_retry import (STREAM_CFGS, slice_window, aggregate,
                                  SYMBOL, DEPOSIT, SPREAD)

# Default disabled sentinels (filter off = pass any range)
MIN_DISABLED = 0
MAX_DISABLED = 999_999

# Sweep grids (include "disabled" sentinel as control to compare vs no-filter baseline)
GRID_MIN = [0, 50, 100, 150, 200, 300, 500, 800, 1200, 1500]    # 10 cells (0 = disabled control)
GRID_MAX = [999_999, 12000, 8000, 5000, 3500, 2500, 1500, 1000, 800]  # 9 cells (999999 = disabled control)

PARENT_RISK_SWEEP = 3.0   # invariant to ranking; matches sim_wfo_hedge_retry default
PARENT_RISK_PROD  = 1.5   # for portfolio compare


@dataclass(frozen=True)
class RangeFilterCfg:
    mode: str               # "min" or "max"
    threshold: int          # value of the active filter


def make_stream_cfg(stream: str, risk_pct: float, min_range_pts: int, max_range_pts: int) -> ORBConfig:
    sc = STREAM_CFGS[stream]
    return ORBConfig(
        risk_pct=risk_pct,
        range_minutes=sc["range_minutes"],
        buffer_pts=0,
        min_range_pts=int(min_range_pts),
        max_range_pts=int(max_range_pts),
        fixed_sl_pts=sc["fixed_sl_pts"],
        rr_ratio=sc["rr_ratio"],
        half_tp_ratio=sc["half_tp_ratio"],
        pending_expire_minutes=240,
        daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13,
        comment=stream,
    )


def run_portfolio_window(streams: list[str], ticks, m1, m5, meta,
                          min_pts: int, max_pts: int, risk_pct: float) -> list:
    """Return list of (ts_ns, pnl) closing deals across all streams in the window."""
    deals = []
    for s in streams:
        cfg = make_stream_cfg(s, risk_pct, min_pts, max_pts)
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        for d in r.deals:
            if d.kind == "entry":
                continue
            deals.append((pd.Timestamp(d.ts).value, d.pnl))
    return deals


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True, choices=["min", "max"],
                    help="min = sweep min_range_pts (max disabled); max = sweep max (min disabled)")
    args = ap.parse_args()
    mode = args.mode

    grid_values = GRID_MIN if mode == "min" else GRID_MAX
    grid_cfgs = [RangeFilterCfg(mode=mode, threshold=v) for v in grid_values]

    print("=" * 110)
    print(f"  RANGE FILTER SWEEP  mode={mode.upper()}_ONLY  ({len(grid_cfgs)} cells)")
    print(f"  Grid: {grid_values}")
    print(f"  Other filter: {'max=DISABLED (999999)' if mode == 'min' else 'min=DISABLED (0)'}")
    print(f"  Parent entry params LOCKED at v2.1 winners (STREAM_CFGS)")
    print(f"  Windows: WINDOWS_MAY9 (4 windows IS+OOS)")
    print(f"  Spread {SPREAD}pt, $10k baseline, parent (sweep) at {PARENT_RISK_SWEEP}%")
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
        full_m1 = load_bars(SYMBOL, "M1", start, end)
        full_m5 = load_bars(SYMBOL, "M5", start, end)
        full_ticks = load_ticks(SYMBOL, start, end, spread_pts=SPREAD)
        print(f"\n  Full data {start.date()}->{end.date()}: ticks={len(full_ticks):,} "
              f"M1={len(full_m1):,} M5={len(full_m5):,}")

        streams = list(STREAM_CFGS.keys())
        t_start = time.time()

        is_per: dict[str, pd.DataFrame] = {}
        oos_per: dict[str, pd.DataFrame] = {}

        for label, is_s, is_e, oos_s, oos_e in WINDOWS:
            for tag, (s, e), bucket in [("IS", (to_utc(is_s), to_utc(is_e)), is_per),
                                         ("OOS", (to_utc(oos_s), to_utc(oos_e)), oos_per)]:
                ticks = slice_window(full_ticks, "ts", s, e)
                m1 = slice_window(full_m1, "ts", s, e)
                m5 = slice_window(full_m5, "ts", s, e)
                rows = []
                for cfg in grid_cfgs:
                    if mode == "min":
                        min_pts = cfg.threshold
                        max_pts = MAX_DISABLED
                    else:
                        min_pts = MIN_DISABLED
                        max_pts = cfg.threshold
                    deals = run_portfolio_window(streams, ticks, m1, m5, meta,
                                                  min_pts, max_pts, PARENT_RISK_SWEEP)
                    np_, dd, pf = aggregate(deals)
                    rows.append({"threshold": cfg.threshold,
                                 "net_profit": np_, "drawdown_pct": dd, "profit_factor": pf,
                                 "n_trades": len(deals)})
                df = pd.DataFrame(rows)
                bucket[label] = df
                print(f"  {label} {tag} {s.date()}->{e.date()} done  cells={len(grid_cfgs)}  "
                      f"[{time.time()-t_start:.0f}s]")

        # Rank via P0
        ranked = rank_with_p0(grid_cfgs, oos_per, WINDOWS, decay_threshold=-0.25,
                               grid_configs=grid_cfgs, is_per_window=is_per)
        print_phase_d_with_p0(ranked[:10], f"range filter {mode.upper()}_ONLY",
                              decay_threshold=-0.25)
        winner = select_winner_with_p0(ranked) or ranked[0]
        flagged = check_winner_boundaries(winner["cfg"], grid_cfgs)
        print_boundary_check(flagged)

        w_threshold = winner["cfg"].threshold
        # Detect if winner is the "disabled control" cell
        is_control = (mode == "min" and w_threshold == 0) or (mode == "max" and w_threshold == MAX_DISABLED)
        print(f"\n  WINNER: mode={mode.upper()}_ONLY  threshold={w_threshold}"
              f"{'  (= NONE/disabled control — filtering does NOT help)' if is_control else ''}")

        out_dir = ROOT / "output" / f"wfo_range_filter_{mode}_may9"
        out_dir.mkdir(parents=True, exist_ok=True)
        wj = {"mode": mode, "threshold": int(w_threshold),
              "is_control_no_filter": is_control,
              "grid": grid_values}
        (out_dir / "winner.json").write_text(json.dumps(wj, indent=2))
        print(f"  Persisted: {out_dir / 'winner.json'}")

        # Portfolio compare at production sizing
        print("\n" + "=" * 110)
        print(f"  PORTFOLIO COMPARISON  Feb 21 -> May 9 (77d, $10k, {PARENT_RISK_PROD}% per stream)")
        print("=" * 110)
        ticks_full = slice_window(full_ticks, "ts", start, end)
        m1_full = slice_window(full_m1, "ts", start, end)
        m5_full = slice_window(full_m5, "ts", start, end)

        # Baseline: no filter
        deals_none = run_portfolio_window(streams, ticks_full, m1_full, m5_full, meta,
                                            MIN_DISABLED, MAX_DISABLED, PARENT_RISK_PROD)
        np_none, dd_none, pf_none = aggregate(deals_none)
        ndd_none = (np_none / (dd_none/100 * (DEPOSIT + np_none))) if dd_none > 0 else 0

        # Winner threshold (if not control)
        if mode == "min":
            min_w = w_threshold; max_w = MAX_DISABLED
        else:
            min_w = MIN_DISABLED; max_w = w_threshold
        deals_w = run_portfolio_window(streams, ticks_full, m1_full, m5_full, meta,
                                         min_w, max_w, PARENT_RISK_PROD)
        np_w, dd_w, pf_w = aggregate(deals_w)
        ndd_w = (np_w / (dd_w/100 * (DEPOSIT + np_w))) if dd_w > 0 else 0

        # Also compare vs current v2.1 baseline (both filters at 200/5000)
        deals_v21 = run_portfolio_window(streams, ticks_full, m1_full, m5_full, meta,
                                           200, 5000, PARENT_RISK_PROD)
        np_v21, dd_v21, pf_v21 = aggregate(deals_v21)
        ndd_v21 = (np_v21 / (dd_v21/100 * (DEPOSIT + np_v21))) if dd_v21 > 0 else 0

        print(f"\n  {'Variant':<28} {'NP':>10} {'DD%':>6} {'NP/DD$':>7} {'PF':>5} {'Trades':>7}")
        print(f"  {'NONE (no filter, control)':<28} ${np_none:>+8,.0f} {dd_none:>5.2f}% "
              f"{ndd_none:>7.2f} {pf_none:>5.2f} {len(deals_none):>7}")
        print(f"  {'v2.1 baseline (200/5000)':<28} ${np_v21:>+8,.0f} {dd_v21:>5.2f}% "
              f"{ndd_v21:>7.2f} {pf_v21:>5.2f} {len(deals_v21):>7}")
        label_w = f"{mode.upper()}_ONLY @ {w_threshold}"
        print(f"  {label_w:<28} ${np_w:>+8,.0f} {dd_w:>5.2f}% "
              f"{ndd_w:>7.2f} {pf_w:>5.2f} {len(deals_w):>7}")
        print(f"\n  Delta vs NONE:   NP={np_w - np_none:>+,.0f}  DD%={dd_w - dd_none:+.2f}p  "
              f"NP/DD$={ndd_w - ndd_none:+.2f}")
        print(f"  Delta vs v2.1:   NP={np_w - np_v21:>+,.0f}  DD%={dd_w - dd_v21:+.2f}p  "
              f"NP/DD$={ndd_w - ndd_v21:+.2f}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
