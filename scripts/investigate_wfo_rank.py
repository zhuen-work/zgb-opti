"""Investigate why WFO rank-1 (S1) underperforms live.

Phase 1: Decompose the rank_with_p0 sort key for top-10 candidates.
         Show per-window NP, prof_count, plateau, NP/DD ratio.

Phase 2: Test 5 alternative ranker variants. For each, pick top-3 and run
         a portfolio Phase E sim on Feb 14 -> May 1, 23pt, 3% total risk.
         Compare NP/DD$.

Variants:
  A: (p0_pass, prof_count, plateau)         <-- CURRENT
  B: (p0_pass, plateau, prof_count)
  C: (p0_pass, np_dd_ratio)
  D: (p0_pass, total_np)
  E: (p0_pass, sqrt(prof_count) * plateau)
"""
from __future__ import annotations

import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.wfo_helpers import (WINDOWS_MAY2 as WINDOWS, rank_with_p0,
                                  _build_plateau_lookup, _plateau_score_for, _swept_dims,
                                  compute_oos_decay_slope)

WFO_DIR = ROOT / "output" / "wfo_orb_may2"
SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
SPREAD = 30  # per feedback_default_test_conditions.md (all live = 30pt 2026-05-16)
START = datetime(2026, 2, 14, tzinfo=timezone.utc)
END = datetime(2026, 5, 1, tzinfo=timezone.utc)


def row_to_cfg(row):
    return ORBConfig(
        risk_pct=3.0, range_minutes=int(row["range_minutes"]),
        buffer_pts=0, min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=int(row["fixed_sl_pts"]),
        rr_ratio=float(row["rr_ratio"]),
        half_tp_ratio=round(float(row["half_tp_ratio"]), 2),
        pending_expire_minutes=240,
        daily_target_pct=float(row.get("daily_target_pct", 0.0)),
        daily_loss_pct=float(row.get("daily_loss_pct", 0.0)),
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True,  ny_start_hour=13, comment="ORB",
    )


def load_wfo():
    is_per = {l: pd.read_parquet(WFO_DIR / f"is_{l}.parquet") for l, *_ in WINDOWS}
    oos_per = {l: pd.read_parquet(WFO_DIR / f"oos_{l}.parquet") for l, *_ in WINDOWS}
    cands = [row_to_cfg(r) for _, r in oos_per["W1"].iterrows()]
    grid = [row_to_cfg(r) for _, r in is_per["W1"].iterrows()]
    return is_per, oos_per, cands, grid


def make_rows(cands, oos_per, grid, is_per):
    """Build per-candidate metric rows (no sorting)."""
    plateau_lookup, swept = _build_plateau_lookup(grid, is_per)
    rows = []
    for i, cfg in enumerate(cands):
        oos_nps = []; oos_dds = []; prof_count = 0
        for label, *_ in WINDOWS:
            r = oos_per[label].iloc[i]
            oos_nps.append(float(r["net_profit"]))
            oos_dds.append(float(r["drawdown_pct"]))
            if r["net_profit"] > 0: prof_count += 1
        total_np = sum(oos_nps)
        avg_dd = sum(oos_dds) / len(oos_dds) if oos_dds else 0.5
        np_dd = total_np / max(avg_dd, 0.5)
        slope = compute_oos_decay_slope(oos_nps)
        p0_pass = slope >= -0.25
        plateau, n_nb = _plateau_score_for(cfg, swept, plateau_lookup)
        if plateau is None: plateau = 0.0
        rows.append({
            "cfg": cfg, "total_np": total_np, "prof_count": prof_count,
            "avg_dd": avg_dd, "np_dd_ratio": np_dd, "oos_nps": oos_nps,
            "slope": slope, "p0_pass": p0_pass, "plateau_score": plateau,
        })
    return rows


VARIANTS = {
    "A_current":         lambda r: (r["p0_pass"], r["prof_count"], r["plateau_score"]),
    "B_plateau_first":   lambda r: (r["p0_pass"], r["plateau_score"], r["prof_count"]),
    "C_npdd_ratio":      lambda r: (r["p0_pass"], r["np_dd_ratio"]),
    "D_total_np":        lambda r: (r["p0_pass"], r["total_np"]),
    "E_sqrt_blend":      lambda r: (r["p0_pass"], math.sqrt(r["prof_count"]) * r["plateau_score"]),
}


def cfg_str(cfg):
    return f"R={cfg.range_minutes} SL={cfg.fixed_sl_pts} RR={cfg.rr_ratio} HTP={cfg.half_tp_ratio}"


def aggregate_portfolio(deals_pnl_pairs):
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    for _, p in sorted(deals_pnl_pairs, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    ndd = (np_ / dd_abs) if dd_abs > 0 else 0
    return np_, dd_pct, ndd


def run_portfolio_top3(top3_cfgs, ticks, m1, m5, meta, per_stream_risk: float):
    """Simulate top-3 streams in parallel (each at per_stream_risk%)."""
    deals = []
    per_s = []
    for i, base_cfg in enumerate(top3_cfgs, 1):
        cfg = ORBConfig(
            risk_pct=per_stream_risk,
            range_minutes=base_cfg.range_minutes, buffer_pts=0,
            min_range_pts=200, max_range_pts=5000,
            fixed_sl_pts=base_cfg.fixed_sl_pts, rr_ratio=base_cfg.rr_ratio,
            half_tp_ratio=base_cfg.half_tp_ratio, pending_expire_minutes=240,
            daily_target_pct=0.0, daily_loss_pct=0.0,
            ldn_enabled=True, ldn_start_hour=7,
            ny_enabled=True,  ny_start_hour=13, comment=f"S{i}",
        )
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        np_s = 0.0; trades = 0
        for d in r.deals:
            if d.kind != "entry":
                deals.append((pd.Timestamp(d.ts).value, d.pnl))
                np_s += d.pnl; trades += 1
        per_s.append((np_s, trades))
    np_, dd, ndd = aggregate_portfolio(deals)
    return np_, dd, ndd, per_s


def main():
    is_per, oos_per, cands, grid = load_wfo()
    rows = make_rows(cands, oos_per, grid, is_per)

    # --- PHASE 1: top-10 by current ranker, decomposed ---
    sorted_current = sorted(rows, key=VARIANTS["A_current"], reverse=True)
    print("=" * 130)
    print("  PHASE 1: Current ranker (A) top-10 — decomposed")
    print("=" * 130)
    print(f"  {'Rank':<4} {'P0':>4} {'Prof':>5} {'TotNP':>9} {'AvgDD':>6} "
          f"{'NP/DD':>6} {'Slope':>7} {'Plat$':>9}  {'OOS NPs (W1->W4)':<35}  Cfg")
    for i, r in enumerate(sorted_current[:10], 1):
        oos_str = "->".join(f"${n:+.0f}" for n in r["oos_nps"])
        print(f"  #{i:<3} {('PASS' if r['p0_pass'] else 'FAIL'):>4} "
              f"{r['prof_count']}/4 ${r['total_np']:>+7,.0f} {r['avg_dd']:>5.1f}% "
              f"{r['np_dd_ratio']:>+5.0f} {r['slope']:>+6.1%} ${r['plateau_score']:>+7,.0f}  "
              f"{oos_str:<35}  {cfg_str(r['cfg'])}")

    # --- show TOP candidates by EACH variant ---
    print("\n" + "=" * 130)
    print("  PHASE 1b: Top-3 by each ranker variant")
    print("=" * 130)
    top3_by_variant = {}
    for name, sort_key in VARIANTS.items():
        sorted_v = sorted(rows, key=sort_key, reverse=True)
        top3 = sorted_v[:3]
        top3_by_variant[name] = top3
        print(f"\n  {name}:")
        for i, r in enumerate(top3, 1):
            print(f"    #{i}  prof={r['prof_count']}/4  totNP=${r['total_np']:>+7,.0f}  "
                  f"NP/DD={r['np_dd_ratio']:>+5.0f}  plat=${r['plateau_score']:>+7,.0f}  "
                  f"slope={r['slope']:>+6.1%}  {cfg_str(r['cfg'])}")

    # --- PHASE 2: portfolio Phase E for each variant ---
    print("\n" + "=" * 130)
    print(f"  PHASE 2: Portfolio Phase E ({START.date()} -> {END.date()}, $10k, {SPREAD}pt, 3% / 3 = 1% per stream)")
    print("=" * 130)
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        m1 = load_bars(SYMBOL, "M1", START, END)
        m5 = load_bars(SYMBOL, "M5", START, END)
        ticks = load_ticks(SYMBOL, START, END, spread_pts=SPREAD)
        print(f"  Loaded ticks={len(ticks):,}\n")

        results = {}
        for name, top3 in top3_by_variant.items():
            cfgs = [r["cfg"] for r in top3]
            np_, dd, ndd, per_s = run_portfolio_top3(cfgs, ticks, m1, m5, meta, 1.0)
            results[name] = (np_, dd, ndd, cfgs, per_s)

        # Sort by NP/DD$
        sorted_results = sorted(results.items(), key=lambda kv: kv[1][2], reverse=True)
        print(f"  {'Variant':<22} {'NP':>10} {'DD%':>6} {'NP/DD$':>7} {'Top-3 (R/SL/RR/HTP)'}")
        for name, (np_, dd, ndd, cfgs, per_s) in sorted_results:
            cfg_summary = " | ".join(f"{c.range_minutes}/{c.fixed_sl_pts}/{c.rr_ratio}/{c.half_tp_ratio}"
                                       for c in cfgs)
            marker = "  <-- CURRENT" if name == "A_current" else ""
            print(f"  {name:<22} ${np_:>+8,.0f} {dd:>5.2f}% {ndd:>7.2f}  {cfg_summary}{marker}")

        # Per-variant per-stream NP
        print(f"\n  Per-stream NP (1% risk each, 76d):")
        print(f"  {'Variant':<22} {'S1 (rank1)':>15} {'S2 (rank2)':>15} {'S3 (rank3)':>15}")
        for name, (np_, dd, ndd, cfgs, per_s) in sorted_results:
            cells = "  ".join(f"${ps:>+7,.0f}({tr:>3}t)" for ps, tr in per_s)
            print(f"  {name:<22} {cells}")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
