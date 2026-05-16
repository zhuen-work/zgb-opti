"""Retro-rank the MAY9 WFO under alternative metrics, compare to live performance.

For each of the 15 MAY9 Phase-B-survivor candidates, computes:
  - Original metric: slope-based P0 (current rank_with_p0)
  - Alt 1: Walk-forward efficiency (OOS NP sum / IS NP sum)
  - Alt 2: Proportion of profitable OOS windows
  - Alt 3: Median per-window NP/DD$

Prints top-10 under each ranking and highlights where the live-deployed
S4/S5/S6 params land in each ordering. Lets us see whether an alternative
metric would have correctly predicted S5/S6 > S4 (the actual live order).

Live order observed past 7 days (live_check 2026-05-14):
  S4 (R=90/SL=650/RR=4/HTP=0.25):  −$6,002  (worst — was sim rank 1)
  S5 (R=90/SL=350/RR=4/HTP=0.5):   +$5,231  (was sim rank 2)
  S6 (R=90/SL=400/RR=4/HTP=0.5):   +$6,518  (was sim rank 3)
"""
from __future__ import annotations
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd

from zgb_sim.wfo_helpers import (
    compute_oos_decay_slope,
    compute_walk_forward_efficiency,
    compute_proportion_profitable,
    compute_per_window_npdd_median,
    WINDOWS_MAY9,
)

WFO_DIR = ROOT / "output" / "wfo_orb_may9"

# Live-deployed configs (the actual S4/S5/S6 streams running now)
LIVE_CFGS = [
    ("S4 (current rank 1)", 90, 650, 4.0, 0.25, "-$6,002  5W/13L"),
    ("S5 (current rank 2)", 90, 350, 4.0, 0.50, "+$5,231  7W/9L"),
    ("S6 (current rank 3)", 90, 400, 4.0, 0.50, "+$6,518  7W/9L"),
]

CFG_COLS = ["range_minutes", "fixed_sl_pts", "rr_ratio", "half_tp_ratio"]


def cfg_label(row) -> str:
    return (f"R={int(row['range_minutes'])} SL={int(row['fixed_sl_pts'])} "
            f"RR={row['rr_ratio']:.1f} HTP={row['half_tp_ratio']:.2f}")


def cfg_matches(row, range_min, sl, rr, htp) -> bool:
    return (int(row['range_minutes']) == range_min
            and int(row['fixed_sl_pts']) == sl
            and abs(row['rr_ratio'] - rr) < 1e-6
            and abs(row['half_tp_ratio'] - htp) < 1e-6)


def main() -> int:
    # Load per-window IS and OOS data
    is_dfs = {}
    oos_dfs = {}
    for label, _, _, _, _ in WINDOWS_MAY9:
        is_dfs[label] = pd.read_parquet(WFO_DIR / f"p1_is_{label}.parquet")
        oos_dfs[label] = pd.read_parquet(WFO_DIR / f"p1_oos_{label}.parquet")

    n_cands = len(oos_dfs["W1"])
    base_df = oos_dfs["W1"][CFG_COLS].reset_index(drop=True)

    # For each candidate, gather IS NPs and OOS NPs/DDs across all windows
    rows = []
    for i in range(n_cands):
        is_nps = [float(is_dfs[lbl].iloc[i]["net_profit"]) for lbl, _, _, _, _ in WINDOWS_MAY9]
        oos_nps = [float(oos_dfs[lbl].iloc[i]["net_profit"]) for lbl, _, _, _, _ in WINDOWS_MAY9]
        oos_dds = [float(oos_dfs[lbl].iloc[i]["drawdown_pct"]) for lbl, _, _, _, _ in WINDOWS_MAY9]
        cfg = base_df.iloc[i]
        rows.append({
            "idx": i,
            "cfg_label": cfg_label(cfg),
            "cfg_range": int(cfg["range_minutes"]),
            "cfg_sl": int(cfg["fixed_sl_pts"]),
            "cfg_rr": float(cfg["rr_ratio"]),
            "cfg_htp": float(cfg["half_tp_ratio"]),
            "is_nps": is_nps,
            "oos_nps": oos_nps,
            "oos_dds": oos_dds,
            "is_total": sum(is_nps),
            "oos_total": sum(oos_nps),
            "slope": compute_oos_decay_slope(oos_nps),
            "wfe": compute_walk_forward_efficiency(is_nps, oos_nps),
            "prop_prof": compute_proportion_profitable(oos_nps),
            "npdd_median": compute_per_window_npdd_median(oos_nps, oos_dds),
            "prof_count": sum(1 for n in oos_nps if n > 0),
            "avg_dd": sum(oos_dds) / len(oos_dds),
        })

    # Tag live deployments
    for r in rows:
        r["live_tag"] = ""
        for lbl, rng, sl, rr, htp, perf in LIVE_CFGS:
            if r["cfg_range"] == rng and r["cfg_sl"] == sl and abs(r["cfg_rr"] - rr) < 1e-6 and abs(r["cfg_htp"] - htp) < 1e-6:
                r["live_tag"] = f" <<< {lbl}: {perf}"

    def print_ranking(name: str, key_fn, descending=True):
        ordered = sorted(rows, key=key_fn, reverse=descending)
        print(f"\n=== {name} ===")
        print(f"  {'Rank':<5} {'Cfg':<35} {'OOS NP':>9} {'Slope':>7} {'WFE':>6} "
              f"{'Prof%':>6} {'Med NP/DD':>11} {'Tag':<55}")
        for rank, r in enumerate(ordered, 1):
            print(f"  {rank:<5} {r['cfg_label']:<35} "
                  f"${r['oos_total']:>+7,.0f} {r['slope']:>+6.0%} "
                  f"{r['wfe']:>6.2f} {r['prop_prof']:>6.0%} "
                  f"{r['npdd_median']:>+11,.0f}{r['live_tag']}")
        # Per-ranking, show where each live cfg landed
        live_ranks = []
        for r in rows:
            if r["live_tag"]:
                rk = next(i for i, rr in enumerate(ordered, 1) if rr["idx"] == r["idx"])
                live_ranks.append((r["live_tag"].strip(), rk))
        live_ranks.sort(key=lambda x: x[1])
        print(f"\n  Live-deployment ranks under this metric:")
        for tag, rk in live_ranks:
            print(f"    Rank {rk}: {tag}")

    # ORIGINAL: (p0_pass desc, prof_count desc, total_np desc) — slope is the gate
    def orig_key(r):
        p0_pass = r["slope"] >= -0.25  # current default
        return (p0_pass, r["prof_count"], r["oos_total"])

    print_ranking("ORIGINAL (slope-gated, prof_count primary)", orig_key)
    print_ranking("ALT 1: Walk-Forward Efficiency (OOS/IS)", lambda r: r["wfe"])
    print_ranking("ALT 2: Proportion of profitable OOS windows", lambda r: (r["prop_prof"], r["oos_total"]))
    print_ranking("ALT 3: Median per-window NP/DD$", lambda r: r["npdd_median"])

    # Combo: prop_prof primary, then WFE
    print_ranking("ALT 4: COMBO (prop_prof, then WFE)",
                  lambda r: (r["prop_prof"], r["wfe"]))

    # Score live S4/S5/S6 specifically across metrics
    print(f"\n=== Side-by-side: live S4/S5/S6 across all metrics ===")
    print(f"  {'Stream':<25} {'OOS NP':>9} {'Slope':>7} {'WFE':>6} {'Prof%':>6} {'Med NP/DD':>11}  Live")
    for lbl, rng, sl, rr, htp, perf in LIVE_CFGS:
        match = next((r for r in rows if r["cfg_range"] == rng and r["cfg_sl"] == sl
                       and abs(r["cfg_rr"] - rr) < 1e-6 and abs(r["cfg_htp"] - htp) < 1e-6), None)
        if match is None:
            print(f"  {lbl:<25}  (not in grid)")
            continue
        print(f"  {lbl:<25} ${match['oos_total']:>+7,.0f} {match['slope']:>+6.0%} "
              f"{match['wfe']:>6.2f} {match['prop_prof']:>6.0%} "
              f"{match['npdd_median']:>+11,.0f}  {perf}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
