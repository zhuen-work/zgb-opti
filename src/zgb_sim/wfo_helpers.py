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

# May 9 reopt — 1 week rolled forward from MAY2. Captures recent post-fix data.
# Last OOS = May 2 -> May 9 covers May 4-8 trading week (5 trading days).
WINDOWS_MAY9 = [
    ("W1", date(2026, 2, 21), date(2026, 3, 21), date(2026, 3, 21), date(2026, 4,  4)),
    ("W2", date(2026, 3,  7), date(2026, 4,  4), date(2026, 4,  4), date(2026, 4, 18)),
    ("W3", date(2026, 3, 21), date(2026, 4, 18), date(2026, 4, 18), date(2026, 5,  2)),
    ("W4", date(2026, 4,  4), date(2026, 5,  2), date(2026, 5,  2), date(2026, 5,  9)),
]

# May 16 reopt — 1 week rolled forward from MAY9. Captures May 11-15 trading week
# (filter-disabled day 1+ plus the LDN cluster days). For Sat 2026-05-16 reopt.
WINDOWS_MAY16 = [
    ("W1", date(2026, 2, 28), date(2026, 3, 28), date(2026, 3, 28), date(2026, 4, 11)),
    ("W2", date(2026, 3, 14), date(2026, 4, 11), date(2026, 4, 11), date(2026, 4, 25)),
    ("W3", date(2026, 3, 28), date(2026, 4, 25), date(2026, 4, 25), date(2026, 5,  9)),
    ("W4", date(2026, 4, 11), date(2026, 5,  9), date(2026, 5,  9), date(2026, 5, 16)),
]

# May 23 reopt — 1 week rolled forward from MAY16. Captures May 18-22 trading
# (first week of v3 reverse-hedge live). For Sat 2026-05-23 reopt.
WINDOWS_MAY23 = [
    ("W1", date(2026, 3,  7), date(2026, 4,  4), date(2026, 4,  4), date(2026, 4, 18)),
    ("W2", date(2026, 3, 21), date(2026, 4, 18), date(2026, 4, 18), date(2026, 5,  2)),
    ("W3", date(2026, 4,  4), date(2026, 5,  2), date(2026, 5,  2), date(2026, 5, 16)),
    ("W4", date(2026, 4, 18), date(2026, 5, 16), date(2026, 5, 16), date(2026, 5, 23)),
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


def compute_walk_forward_efficiency(is_nps: list[float], oos_nps: list[float]) -> float:
    """Walk-forward efficiency = sum(OOS_NP) / sum(IS_NP).

    Measures generalization: how much of in-sample edge survives out-of-sample.
    Values close to 1.0 mean clean generalization. Below 0.5 = overfit. Above
    1.2 = lucky OOS or IS-conservative grid. Standard WF-analysis metric.

    Defensive: if sum(IS_NP) <= 0, returns 0.0 (the candidate was unprofitable
    even on its tuning data, so WFE is undefined / treat as worst).
    """
    is_sum = sum(is_nps)
    oos_sum = sum(oos_nps)
    if is_sum <= 0:
        return 0.0
    return oos_sum / is_sum


def compute_proportion_profitable(oos_nps: list[float]) -> float:
    """Fraction of OOS windows with positive NP.

    Binary consistency metric — robust to magnitude. A candidate with NPs
    [+100, +50, +200, +75] scores 1.0 (4/4 profitable); one with
    [+1000, -300, +800, -100] scores 0.5 (2/4) despite higher total NP.
    """
    if not oos_nps:
        return 0.0
    return sum(1 for n in oos_nps if n > 0) / len(oos_nps)


def compute_per_window_npdd_median(oos_nps: list[float], oos_dds: list[float]) -> float:
    """Median of per-window NP/DD$ ratios.

    Risk-adjusted return per window, taken as median (robust to outlier weeks).
    DD floor of 0.5% to avoid division blowups. Useful when total NP/DD$ is
    dominated by one big week — median tells you the "typical" week's edge.
    """
    if not oos_nps or len(oos_nps) != len(oos_dds):
        return 0.0
    ratios = [np_ / max(dd, 0.5) for np_, dd in zip(oos_nps, oos_dds)]
    ratios.sort()
    n = len(ratios)
    if n % 2 == 1:
        return float(ratios[n // 2])
    return float((ratios[n // 2 - 1] + ratios[n // 2]) / 2)


def compute_recency_weighted_np(oos_nps: list[float]) -> float:
    """NP weighted toward most-recent window.

    Linear ramp: W_i weight = (i+1) / sum(1..n). For 4 windows, weights
    are 0.1, 0.2, 0.3, 0.4 (W4 gets 40% of the weight, W1 gets 10%).
    Captures "what's working now" without slope's overreaction to direction.
    """
    if not oos_nps:
        return 0.0
    n = len(oos_nps)
    denom = n * (n + 1) / 2
    return sum((i + 1) * v for i, v in enumerate(oos_nps)) / denom


def compute_stress_regime_np(oos_nps: list[float], oos_dds: list[float],
                                dd_threshold: float = 10.0) -> float:
    """NP sum only across stress-regime windows (DD% >= threshold).

    Filters per-window NP to only the higher-DD weeks (typically the
    cluster-stop / fast-move regimes). A candidate with high stress-NP
    is robust on tough weeks; one whose NP comes only from low-DD weeks
    is regime-fragile.

    Returns sum across qualifying windows, or 0.0 if no windows qualify.
    """
    if not oos_nps or len(oos_nps) != len(oos_dds):
        return 0.0
    return sum(np_ for np_, dd in zip(oos_nps, oos_dds) if dd >= dd_threshold)


def compute_min_pf(oos_pfs: list[float]) -> float:
    """Minimum profit factor across OOS windows — robustness floor."""
    if not oos_pfs:
        return 0.0
    return min(oos_pfs)


def compute_pf_stability(oos_pfs: list[float]) -> float:
    """Coefficient of variation of PF across windows (lower = more stable).

    Returns std(PF) / mean(PF). NaN-safe (returns 0.0 if mean is 0).
    """
    if not oos_pfs:
        return 0.0
    mean = sum(oos_pfs) / len(oos_pfs)
    if mean <= 0:
        return 0.0
    var = sum((p - mean) ** 2 for p in oos_pfs) / len(oos_pfs)
    return (var ** 0.5) / mean


def compute_np_per_trade(oos_nps: list[float], oos_trades: list[int]) -> float:
    """Average NP per trade across all OOS windows (trade-count-normalized).

    Strips out trade-count effects — a candidate with 200 trades and $10k
    NP scores the same as one with 100 trades and $5k NP. Useful for
    comparing configs with different range/SL setups that produce
    different trade frequencies.
    """
    total_np = sum(oos_nps)
    total_trades = sum(oos_trades)
    if total_trades == 0:
        return 0.0
    return total_np / total_trades


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


def _swept_dims(grid_configs: list) -> list[tuple]:
    """Return [(field_name, sorted_unique_values)] for fields with >1 grid value."""
    import dataclasses
    if not grid_configs:
        return []
    fields = [f.name for f in dataclasses.fields(grid_configs[0])]
    swept = []
    for fld in fields:
        try:
            values = sorted({getattr(c, fld) for c in grid_configs})
        except TypeError:
            continue
        if len(values) >= 2:
            swept.append((fld, values))
    return swept


def _build_plateau_lookup(grid_configs: list, is_per_window: dict) -> tuple[dict, list]:
    """Build {param_tuple: avg_IS_NP} across IS windows for the full grid."""
    swept = _swept_dims(grid_configs)
    if not grid_configs or not is_per_window or not swept:
        return {}, swept
    avg_np = np.zeros(len(grid_configs))
    n_windows = 0
    for label, df in is_per_window.items():
        avg_np = avg_np + df["net_profit"].to_numpy()
        n_windows += 1
    if n_windows > 0:
        avg_np = avg_np / n_windows
    lookup = {}
    for i, cfg in enumerate(grid_configs):
        key = tuple(getattr(cfg, fld) for fld, _ in swept)
        lookup[key] = float(avg_np[i])
    return lookup, swept


def _plateau_score_for(cfg, swept: list, lookup: dict) -> tuple[Optional[float], int]:
    """Score = 0.5×own + 0.5×mean(neighbors). Neighbors differ by one swept dim by one step."""
    if not swept or not lookup:
        return None, 0
    own_key = tuple(getattr(cfg, fld) for fld, _ in swept)
    own = lookup.get(own_key)
    if own is None:
        return None, 0
    neighbor_nps = []
    for i, (fld, values) in enumerate(swept):
        cur = getattr(cfg, fld)
        try:
            idx = values.index(cur)
        except ValueError:
            continue
        for ni in (idx - 1, idx + 1):
            if 0 <= ni < len(values):
                nb_key = list(own_key)
                nb_key[i] = values[ni]
                nb_np = lookup.get(tuple(nb_key))
                if nb_np is not None:
                    neighbor_nps.append(nb_np)
    if not neighbor_nps:
        return own, 0
    neighbor_mean = sum(neighbor_nps) / len(neighbor_nps)
    return 0.5 * own + 0.5 * neighbor_mean, len(neighbor_nps)


def rank_with_p0(candidates: list, oos_per_window: dict,
                  windows: list, decay_threshold: float = -0.25,
                  grid_configs: Optional[list] = None,
                  is_per_window: Optional[dict] = None) -> list[dict]:
    """Rank candidates with P0 decay-aware filter and (optionally) plateau preference.

    Args:
        candidates: list of cfg objects (Phase B survivors)
        oos_per_window: {window_label -> DataFrame of per-cfg OOS results}
        windows: list of (label, is_s, is_e, oos_s, oos_e) tuples
        decay_threshold: reject candidates with slope < this (default -0.25 = 25% decline)
        grid_configs: full sweep grid (enables plateau scoring)
        is_per_window: {label -> DataFrame of per-cfg IS results} (enables plateau scoring)

    When grid_configs and is_per_window are both supplied, plateau_score is computed
    from IS-mean NP across windows for each candidate and its grid neighbors. The sort
    key is (p0_pass, prof_count, plateau_score) — prof_count primary tiebreak.

    Why prof_count primary (validated 2026-05-03): when shipping top-N as a portfolio,
    a 4/4 candidate is often STRUCTURALLY DIFFERENT from high-plateau candidates
    (different SL/HTP profile that wins consistently rather than by magnitude). This
    structural divergence provides natural diversification. Empirical test: swapping to
    plateau-primary picked 3 high-NP/high-DD candidates that were correlated; resulting
    top-3 portfolio had 11% LOWER NP/DD$ than prof_count-primary. prof_count's binary
    consistency signal correlates with "trades a different style", which is portfolio gold.

    Returns:
        list of dicts with cfg, total_np, prof_count, avg_dd, np_dd_ratio, oos_nps,
        slope, p0_pass, plateau_score, n_neighbors
    """
    plateau_lookup, swept = _build_plateau_lookup(grid_configs or [], is_per_window or {})
    use_plateau = bool(plateau_lookup)

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
        plateau_score, n_neighbors = _plateau_score_for(cfg, swept, plateau_lookup)
        rows.append({
            "cfg": cfg,
            "total_np": total_np,
            "prof_count": prof_count,
            "avg_dd": avg_dd,
            "np_dd_ratio": np_dd,
            "oos_nps": oos_nps,
            "slope": slope,
            "p0_pass": p0_pass,
            "plateau_score": plateau_score,
            "n_neighbors": n_neighbors,
        })

    if use_plateau:
        # Sort: p0_pass desc, prof_count desc, plateau_score desc.
        # prof_count is primary tiebreak because it correlates with structural divergence
        # from high-plateau winners — the resulting top-N portfolio is naturally diversified.
        # See docstring for empirical justification (2026-05-03 ranker test).
        rows.sort(key=lambda x: (x["p0_pass"], x["prof_count"],
                                   x["plateau_score"] if x["plateau_score"] is not None else float("-inf")),
                   reverse=True)
    else:
        rows.sort(key=lambda x: (x["p0_pass"], x["prof_count"], x["np_dd_ratio"]), reverse=True)
    return rows


def print_phase_d_with_p0(ranked: list[dict], stream_label: str, decay_threshold: float = -0.25):
    """Pretty-print Phase D ranking with P0 verdict and (if computed) plateau score."""
    has_plateau = any(r.get("plateau_score") is not None for r in ranked)
    print(f"\n=== PHASE D: Final Ranking ({stream_label}) - P0 enabled"
          f"{' + plateau' if has_plateau else ''} ===")
    print(f"  Decay-aware filter: reject if OOS slope < {decay_threshold:+.0%}")
    if has_plateau:
        print(f"  Plateau score = 0.5×own_IS_NP + 0.5×mean(grid-neighbors_IS_NP)")
    header = f"  {'Rank':<5} {'P0':>4} {'Prof':>5} {'NP':>9} {'AvgDD':>7} {'NP/DD':>8} {'Slope':>8}"
    if has_plateau:
        header += f" {'Plat$':>9} {'Nb':>3}"
    header += "  OOS NPs"
    print(header)
    for rank, row in enumerate(ranked, 1):
        p0 = "PASS" if row["p0_pass"] else "FAIL"
        oos_str = " -> ".join(f"${n:+.0f}" for n in row["oos_nps"])
        line = (f"  {rank:<5} {p0:>4} {row['prof_count']}/{len(row['oos_nps'])} "
                f"${row['total_np']:>+8,.0f} {row['avg_dd']:>5.1f}%  "
                f"{row['np_dd_ratio']:>+8.0f} {row['slope']:>+7.1%}")
        if has_plateau:
            ps = row.get("plateau_score")
            ps_str = f"${ps:>+8,.0f}" if ps is not None else f"{'--':>9}"
            line += f" {ps_str} {row.get('n_neighbors', 0):>3}"
        line += f"  {oos_str}"
        print(line)
    n_pass = sum(1 for r in ranked if r["p0_pass"])
    print(f"\n  P0 verdict: {n_pass}/{len(ranked)} candidates passed decay filter.")
    if n_pass == 0:
        tiebreak = "plateau_score" if has_plateau else "NP/DD"
        print(f"  WARNING: No candidate passed -- using best-of-failed (top by {tiebreak}).")


def select_winner_with_p0(ranked: list[dict]) -> Optional[dict]:
    """Select winner: top P0-passing candidate, or fallback to top NP/DD if none pass."""
    p0_passers = [r for r in ranked if r["p0_pass"]]
    if p0_passers:
        # Among passers, pick highest prof_count then NP/DD
        return p0_passers[0]
    # No passers: fallback (return None signals caller to handle "no viable winner")
    return None


def check_winner_boundaries(winner_cfg, grid_configs: list) -> list[dict]:
    """Detect whether the winner sits at the edge of any swept (multi-value) param.

    Auto-discovers swept dims by finding dataclass fields with >1 unique value
    across the grid. A winner at min/max of a dim means the grid was likely
    truncated -- extend that dim and re-run to know if a better optimum exists
    outside the tested range.

    Returns a list of dicts (one per swept dim). Each dict has keys:
      dim, value, grid_min, grid_max, n_values, at_boundary ("min"|"max"|None).
    """
    import dataclasses
    if not grid_configs:
        return []
    fields = [f.name for f in dataclasses.fields(grid_configs[0])]
    rows = []
    for fld in fields:
        try:
            values = sorted({getattr(c, fld) for c in grid_configs})
        except TypeError:
            continue  # unhashable / non-comparable values
        if len(values) < 2:
            continue
        wval = getattr(winner_cfg, fld)
        at = "min" if wval == values[0] else ("max" if wval == values[-1] else None)
        rows.append({
            "dim": fld,
            "value": wval,
            "grid_min": values[0],
            "grid_max": values[-1],
            "n_values": len(values),
            "at_boundary": at,
        })
    return rows


def print_boundary_check(flagged: list[dict]) -> bool:
    """Pretty-print the boundary check. Returns True if any dim is at boundary."""
    print("\n=== BOUNDARY CHECK ===")
    print("  Verify winner is not sitting at the edge of any swept param dim.")
    print(f"  {'Dim':<25} {'Winner':>10} {'GridMin':>10} {'GridMax':>10} {'N':>4}  Note")
    any_at = False
    for f in flagged:
        note = ""
        if f["at_boundary"] == "min":
            note = "AT MIN -- extend grid downward"
            any_at = True
        elif f["at_boundary"] == "max":
            note = "AT MAX -- extend grid upward"
            any_at = True
        print(f"  {f['dim']:<25} {str(f['value']):>10} {str(f['grid_min']):>10} "
              f"{str(f['grid_max']):>10} {f['n_values']:>4}  {note}")
    if any_at:
        print("\n  WARNING: winner at grid boundary in 1+ dims. Consider extending grid and re-running.")
    else:
        print("\n  PASS: all swept dims interior to grid. No boundary truncation detected.")
    return any_at


def print_rank_sanity_check(ranked: list[dict], top_n: int = 3,
                              np_gap_threshold: float = 0.30) -> bool:
    """Sanity-check the top-N ranking by surfacing NP/DD$ + magnitude tradeoffs.

    Why this exists (added 2026-05-06 after live-data investigation):
    The current ranker uses (p0_pass, prof_count, plateau_score) which prioritises
    cross-window stability. This sometimes elevates a low-NP-but-stable candidate
    to rank 1 over higher-NP candidates that failed in just one OOS window. Live
    data on the 2026-05-02 WFO winner (S1, IS NP $8,270, prof_count 4/4) confirmed
    this pattern: S1 had the LOWEST IS NP of the top 10 yet won rank 1 by being the
    only candidate positive in all 4 OOS windows. Live PF after 14 trades was 1.05,
    while ranks 2 and 3 (S2/S3) showed 2.53 and 1.93 — they carried the portfolio.

    The check: if rank-1's IS NP is materially below ranks 2/3 (gap > np_gap_threshold,
    default 30%), surface this so the user knows the rank reflects stability not
    contribution magnitude. Then they can consciously decide whether to deploy as-is
    or override.

    Returns True if a warning was emitted, False if all looks clean.
    """
    if len(ranked) < min(top_n, 2):
        return False
    print(f"\n=== RANK SANITY CHECK (top {top_n}) ===")
    print(f"  Surfaces stability-vs-magnitude tradeoffs in the top-N ranking.")
    print(f"  {'Rank':<5} {'IS NP':>10} {'NP/DD':>7} {'Prof':>5} {'Plat$':>10} {'Slope':>7}  "
          f"{'Cfg attrs that vary':<50}")
    top = ranked[:top_n]
    for i, r in enumerate(top, 1):
        c = r["cfg"]
        # Surface dataclass attrs the user likely cares about
        cfg_str_parts = []
        for fld in ("range_minutes", "fixed_sl_pts", "rr_ratio", "half_tp_ratio"):
            if hasattr(c, fld):
                cfg_str_parts.append(f"{fld[:3]}={getattr(c, fld)}")
        cfg_str = " ".join(cfg_str_parts) or repr(c)[:50]
        plat = r.get("plateau_score") or 0
        slope = r.get("slope", 0)
        print(f"  #{i:<3} ${r['total_np']:>+8,.0f} {r['np_dd_ratio']:>+6.0f} "
              f"{r['prof_count']}/{len(r['oos_nps'])} ${plat:>+8,.0f} {slope:>+6.1%}  {cfg_str}")

    rank1 = top[0]
    others = top[1:]
    if not others:
        return False
    max_other_np = max(r["total_np"] for r in others)
    if max_other_np <= 0:
        return False
    np_gap = (max_other_np - rank1["total_np"]) / abs(max_other_np)
    if np_gap < np_gap_threshold:
        print(f"\n  PASS: rank-1 NP within {np_gap_threshold:.0%} of the best alternative — "
              f"no magnitude/stability tradeoff to flag.")
        return False

    # Warning case
    print(f"\n  WARNING: Rank-1 IS NP (${rank1['total_np']:+,.0f}) is {np_gap:.0%} BELOW "
          f"the best alternative (${max_other_np:+,.0f}).")
    rank1_pc = rank1["prof_count"]
    if any(r["prof_count"] < rank1_pc for r in others):
        print(f"  Reason: rank-1 has prof_count {rank1_pc}/{len(rank1['oos_nps'])} (the prof_count "
              f"primary tiebreak in rank_with_p0 picks for stability over magnitude).")
    # Estimate per-stream NP contribution at equal sizing
    total_np_top_n = sum(r["total_np"] for r in top)
    if total_np_top_n > 0:
        print(f"\n  Expected per-stream IS-NP contribution at equal per-stream sizing:")
        for i, r in enumerate(top, 1):
            pct = r["total_np"] / total_np_top_n * 100 if total_np_top_n else 0
            print(f"    Rank {i}: ${r['total_np']:>+8,.0f} = {pct:>5.1f}% of top-{top_n} total NP")
    print(f"\n  Action items:")
    print(f"    1. If you want max NP/DD$ stability: ship as-is (current rank ordering).")
    print(f"    2. If you want max NP and accept slightly higher DD: consider promoting rank-2 "
          f"or running with rank-2/3 only (drop rank-1).")
    print(f"    3. Track live PF per stream — if rank-1 PF stays <1.5 over >=30 live trades, "
          f"demote it in the next reopt (live-aware ranker — see project_orb_live_trade_log.md "
          f"for tracking).")
    return True
