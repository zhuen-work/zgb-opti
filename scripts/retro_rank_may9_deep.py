"""Deep retro-analysis of MAY9 WFO — adds trade-level metrics, stress-regime
performance, and portfolio correlation, then evaluates whether ANY combination
can predict the live order S6 > S5 > S4.

Builds on retro_rank_may9.py with new dimensions:
  - W4 NP (most-recent window only)
  - Recency-weighted NP
  - Min/Mean OOS PF (consistency)
  - PF stability (CV across windows)
  - NP per trade (trade-count-normalized edge)
  - TP rate, SL rate (mean across windows)
  - Stress-regime NP (only windows with DD >= 10%)
  - Pairwise NP correlation across all candidates

Honest goal: not to find a single magic metric, but to characterize WHAT
KIND of signal would have helped, and whether any such signal exists in
the WFO data.
"""
from __future__ import annotations
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd
import numpy as np

from zgb_sim.wfo_helpers import (
    compute_oos_decay_slope,
    compute_walk_forward_efficiency,
    compute_proportion_profitable,
    compute_per_window_npdd_median,
    compute_recency_weighted_np,
    compute_stress_regime_np,
    compute_min_pf,
    compute_pf_stability,
    compute_np_per_trade,
    WINDOWS_MAY9,
)

WFO_DIR = ROOT / "output" / "wfo_orb_may9"

LIVE_CFGS = [
    ("S4", 90, 650, 4.0, 0.25, -6002, "5W/13L"),
    ("S5", 90, 350, 4.0, 0.50, +5231, "7W/9L"),
    ("S6", 90, 400, 4.0, 0.50, +6518, "7W/9L"),
]
# Live order best->worst: S6 > S5 > S4

CFG_COLS = ["range_minutes", "fixed_sl_pts", "rr_ratio", "half_tp_ratio"]


def cfg_label(row) -> str:
    return (f"R={int(row['range_minutes'])} SL={int(row['fixed_sl_pts'])} "
            f"RR={row['rr_ratio']:.1f} HTP={row['half_tp_ratio']:.2f}")


def main() -> int:
    # Load per-window IS and OOS data
    is_dfs, oos_dfs = {}, {}
    for label, _, _, _, _ in WINDOWS_MAY9:
        is_dfs[label] = pd.read_parquet(WFO_DIR / f"p1_is_{label}.parquet")
        oos_dfs[label] = pd.read_parquet(WFO_DIR / f"p1_oos_{label}.parquet")

    n_cands = len(oos_dfs["W1"])
    base_df = oos_dfs["W1"][CFG_COLS].reset_index(drop=True)

    # Build per-candidate stat bundle
    rows = []
    for i in range(n_cands):
        oos_nps  = [float(oos_dfs[lbl].iloc[i]["net_profit"])    for lbl, *_ in WINDOWS_MAY9]
        oos_pfs  = [float(oos_dfs[lbl].iloc[i]["profit_factor"]) for lbl, *_ in WINDOWS_MAY9]
        oos_dds  = [float(oos_dfs[lbl].iloc[i]["drawdown_pct"])  for lbl, *_ in WINDOWS_MAY9]
        oos_trds = [int(oos_dfs[lbl].iloc[i]["trades"])          for lbl, *_ in WINDOWS_MAY9]
        oos_tps  = [int(oos_dfs[lbl].iloc[i]["tp"])              for lbl, *_ in WINDOWS_MAY9]
        oos_sls  = [int(oos_dfs[lbl].iloc[i]["sl"])              for lbl, *_ in WINDOWS_MAY9]
        is_nps   = [float(is_dfs[lbl].iloc[i]["net_profit"])     for lbl, *_ in WINDOWS_MAY9]

        cfg = base_df.iloc[i]
        rows.append({
            "idx": i,
            "cfg_label": cfg_label(cfg),
            "cfg_range": int(cfg["range_minutes"]),
            "cfg_sl":    int(cfg["fixed_sl_pts"]),
            "cfg_rr":    float(cfg["rr_ratio"]),
            "cfg_htp":   float(cfg["half_tp_ratio"]),
            "is_nps":  is_nps,
            "oos_nps": oos_nps,
            "oos_pfs": oos_pfs,
            "oos_dds": oos_dds,
            "oos_trds": oos_trds,
            "oos_tps": oos_tps,
            "oos_sls": oos_sls,
            # existing metrics
            "total_np":      sum(oos_nps),
            "slope":         compute_oos_decay_slope(oos_nps),
            "wfe":           compute_walk_forward_efficiency(is_nps, oos_nps),
            "prop_prof":     compute_proportion_profitable(oos_nps),
            "med_npdd":      compute_per_window_npdd_median(oos_nps, oos_dds),
            # new metrics
            "w4_np":         oos_nps[-1],
            "recency_np":    compute_recency_weighted_np(oos_nps),
            "min_np":        min(oos_nps),
            "min_pf":        compute_min_pf(oos_pfs),
            "mean_pf":       sum(oos_pfs) / len(oos_pfs),
            "pf_cv":         compute_pf_stability(oos_pfs),
            "np_per_trade":  compute_np_per_trade(oos_nps, oos_trds),
            "tp_rate":       sum(oos_tps) / max(sum(oos_trds), 1),
            "sl_rate":       sum(oos_sls) / max(sum(oos_trds), 1),
            "stress_np":     compute_stress_regime_np(oos_nps, oos_dds, dd_threshold=10.0),
        })

    # Tag live deployments
    for r in rows:
        r["live_tag"] = ""
        r["live_pnl"] = None
        for lbl, rng, sl, rr, htp, live_pnl, wlr in LIVE_CFGS:
            if r["cfg_range"] == rng and r["cfg_sl"] == sl and abs(r["cfg_rr"] - rr) < 1e-6 and abs(r["cfg_htp"] - htp) < 1e-6:
                r["live_tag"] = lbl
                r["live_pnl"] = live_pnl

    # ============================================================
    # Section 1: Per-stream stat dump for S4/S5/S6
    # ============================================================
    print("=" * 90)
    print("SECTION 1: Per-stream stat dump for live S4/S5/S6")
    print("=" * 90)
    print(f"\n{'Metric':<22} {'S4 (live -$6k)':>16} {'S5 (live +$5k)':>16} {'S6 (live +$7k)':>16}")
    print("-" * 90)

    by_lbl = {r["live_tag"]: r for r in rows if r["live_tag"]}
    metrics = [
        ("Total OOS NP",     "total_np",    "${:>+10,.0f}"),
        ("W4 NP",            "w4_np",       "${:>+10,.0f}"),
        ("Recency-weighted", "recency_np",  "${:>+10,.0f}"),
        ("Min OOS NP",       "min_np",      "${:>+10,.0f}"),
        ("Stress-regime NP", "stress_np",   "${:>+10,.0f}"),
        ("Mean OOS PF",      "mean_pf",     "{:>11.2f}"),
        ("Min OOS PF",       "min_pf",      "{:>11.2f}"),
        ("PF stability CV",  "pf_cv",       "{:>11.2f}"),
        ("NP per trade",     "np_per_trade","${:>+10,.2f}"),
        ("Mean TP rate",     "tp_rate",     "{:>11.1%}"),
        ("Mean SL rate",     "sl_rate",     "{:>11.1%}"),
        ("Slope",            "slope",       "{:>+11.0%}"),
        ("WFE",              "wfe",         "{:>11.2f}"),
        ("Prop_prof",        "prop_prof",   "{:>11.0%}"),
        ("Med NP/DD",        "med_npdd",    "${:>+10,.0f}"),
    ]
    for name, key, fmt in metrics:
        s4 = by_lbl.get("S4", {}).get(key, 0)
        s5 = by_lbl.get("S5", {}).get(key, 0)
        s6 = by_lbl.get("S6", {}).get(key, 0)
        print(f"{name:<22} {fmt.format(s4):>16} {fmt.format(s5):>16} {fmt.format(s6):>16}")

    print(f"\n  Live order (best -> worst):   S6 > S5 > S4")
    print(f"  For a metric to predict live, S6 must score HIGHEST and S4 LOWEST.")

    # ============================================================
    # Section 2: Test every metric for live-order prediction
    # ============================================================
    print("\n" + "=" * 90)
    print("SECTION 2: Which metric ordered S4/S5/S6 correctly?")
    print("=" * 90)
    print(f"\n  Live order: S6 ($+6,518) > S5 ($+5,231) > S4 ($-6,002)")
    print(f"  A metric 'predicts' if S6_score > S5_score > S4_score (or strictly inverse for cost metrics).")

    s4, s5, s6 = by_lbl["S4"], by_lbl["S5"], by_lbl["S6"]
    test_metrics = [
        ("Total OOS NP",     "total_np", False),
        ("W4 NP",            "w4_np", False),
        ("Recency-weighted", "recency_np", False),
        ("Min OOS NP",       "min_np", False),
        ("Stress-regime NP", "stress_np", False),
        ("Mean OOS PF",      "mean_pf", False),
        ("Min OOS PF",       "min_pf", False),
        ("PF stability CV",  "pf_cv", True),   # lower is better
        ("NP per trade",     "np_per_trade", False),
        ("Mean TP rate",     "tp_rate", False),
        ("Mean SL rate",     "sl_rate", True), # lower is better
        ("Slope",            "slope", False),
        ("WFE",              "wfe", False),
        ("Med NP/DD",        "med_npdd", False),
    ]
    print(f"\n  {'Metric':<22} {'S4':>10} {'S5':>10} {'S6':>10}  Ordered correctly?")
    print(f"  {'-'*22} {'-'*10} {'-'*10} {'-'*10}  {'-'*20}")
    n_correct = 0
    for name, key, lower_better in test_metrics:
        a, b, c = s4[key], s5[key], s6[key]
        if lower_better:
            ok = c < b < a  # S6 < S5 < S4
        else:
            ok = c > b > a  # S6 > S5 > S4
        mark = "YES" if ok else ("partial" if (lower_better and c < a) or (not lower_better and c > a) else "no")
        if ok:
            n_correct += 1
        print(f"  {name:<22} {a:>+10.3f} {b:>+10.3f} {c:>+10.3f}  {mark}")
    print(f"\n  {n_correct}/{len(test_metrics)} metrics fully predicted the live order S6 > S5 > S4.")

    # ============================================================
    # Section 3: Portfolio correlation — diversification analysis
    # ============================================================
    print("\n" + "=" * 90)
    print("SECTION 3: Inter-candidate NP correlation (diversification angle)")
    print("=" * 90)
    print("\n  If two candidates are highly correlated, they fail together. Cluster-stop")
    print("  days like 2026-05-13/14 reveal correlation that single-stream NP misses.")
    print("  A diversification-aware ranker would pick streams with LOW pairwise correlation.")

    # Build NP matrix (n_cands x n_windows)
    np_matrix = np.array([r["oos_nps"] for r in rows])
    # Correlation matrix
    if np_matrix.std(axis=1).min() > 0:
        corr = np.corrcoef(np_matrix)
    else:
        corr = np.eye(n_cands)

    # For each candidate, compute its uniqueness = 1 - mean(corr with everyone else)
    n = corr.shape[0]
    uniqueness = []
    for i in range(n):
        mean_corr = (corr[i].sum() - 1) / (n - 1)  # exclude self
        uniqueness.append(1 - mean_corr)
    for r, u in zip(rows, uniqueness):
        r["uniqueness"] = u

    # Print uniqueness for S4/S5/S6
    print(f"\n  Per-candidate uniqueness (1 - mean correlation with all others):")
    print(f"  {'Stream':<25} {'Uniqueness':>11}  {'Live ROI':>10}")
    for tag in ["S4", "S5", "S6"]:
        r = by_lbl[tag]
        print(f"  {r['cfg_label']:<25} {r['uniqueness']:>10.3f}  ${r['live_pnl']:>+9,.0f}")

    # ============================================================
    # Section 4: Bayesian noise check — is 7 days even enough?
    # ============================================================
    print("\n" + "=" * 90)
    print("SECTION 4: Sample noise — can we even distinguish these streams?")
    print("=" * 90)
    print(f"\n  Live trade counts (past 7d):  S4=18  S5=16  S6=16")
    print(f"  With RR=4.0 and 50% true win rate, 95% CI on observed WR after N trades:")
    for n_t in [16, 18, 30, 60]:
        p = 0.5
        se = (p * (1-p) / n_t) ** 0.5
        ci_low, ci_high = p - 1.96 * se, p + 1.96 * se
        print(f"    N={n_t}: 95% CI = [{ci_low:.0%}, {ci_high:.0%}] -- spread of {ci_high-ci_low:.0%}")
    print(f"\n  Observed S4 WR = 5/18 = 28%. Within 95% CI of a true 50% WR after 18 trades.")
    print(f"  Cannot reject null hypothesis 'S4 is the same edge as S5/S6'.")
    print(f"  ==> 7 days is sample-noise-floor. Need >=30 trades/stream to claim any divergence.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
