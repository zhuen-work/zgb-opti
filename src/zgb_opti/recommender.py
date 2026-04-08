"""Phase 2C: Weekly Deployment Recommender.

Converts existing research outputs (window effectiveness + robustness study)
into a practical weekly deployment recommendation.

Decision policy (rules-based, deterministic):

A. Window selection
   - Compute combined_score = 0.5 * effectiveness_norm + 0.5 * robustness_norm
   - effectiveness_norm derived from window_ranking composite_score
   - robustness_norm derived from robustness_ranking robustness_score
   - Highest combined_score wins; tie-break by lower effectiveness_rank

B. Parameter selection rule
   - If robustness says "stable_plateau" and robustness_score >= 0.70: top_result_is_safe
   - If robustness >= 0.55 and clustering_score >= 0.65:              top5_median_is_safer
   - If stability_score >= 0.65 but weaker neighbor:                  cluster_centroid_recommended
   - Else:                                                             avoid_parameter_update

C. Confidence
   - high:   combined_score >= 0.65 AND plateau in (stable_plateau) AND no spike flags
   - medium: combined_score >= 0.40 OR (high score but minor caution flags)
   - low:    otherwise

D. Deployment action
   - deploy:               confidence == high and no caution flags
   - deploy_with_caution:  confidence in (high, medium) and some caution flags present
   - hold:                 confidence == low or "spike_risk" or "conflicting_signals" present
   - insufficient_evidence: fewer than MIN_JOBS jobs for recommended window
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

MIN_JOBS = 3   # minimum rolling jobs needed for confident recommendation


# ---------------------------------------------------------------------------
# 1. Input loading
# ---------------------------------------------------------------------------

def load_recommender_inputs(research_dir: str | Path) -> dict[str, pd.DataFrame]:
    rd = Path(research_dir)
    required = {
        "window_ranking":    rd / "window_ranking.parquet",
        "robustness_ranking": rd / "robustness_ranking.parquet",
        "robustness_analysis": rd / "robustness_analysis.parquet",
        "selected_candidates": rd / "selected_candidates.parquet",
    }
    datasets: dict[str, pd.DataFrame] = {}
    for name, path in required.items():
        if not path.exists():
            raise FileNotFoundError(f"Required dataset missing: {path}")
        datasets[name] = pd.read_parquet(path)
    return datasets


def validate_required_inputs(datasets: dict[str, pd.DataFrame]) -> list[str]:
    """Return list of validation warnings (empty = all good)."""
    warns: list[str] = []
    wr = datasets["window_ranking"]
    rr = datasets["robustness_ranking"]

    if "window_weeks" not in wr.columns or "composite_score" not in wr.columns:
        warns.append("window_ranking missing expected columns")
    if "window_weeks" not in rr.columns or "robustness_score" not in rr.columns:
        warns.append("robustness_ranking missing expected columns")
    if len(wr) == 0 or len(rr) == 0:
        warns.append("One or more ranking tables are empty")
    return warns


# ---------------------------------------------------------------------------
# 2. Signal combination
# ---------------------------------------------------------------------------

def combine_window_signals(
    window_ranking: pd.DataFrame,
    robustness_ranking: pd.DataFrame,
) -> pd.DataFrame:
    """Merge effectiveness and robustness signals into one table per window.

    combined_score = 0.5 * eff_norm + 0.5 * rob_norm
    where norms map each score column to [0, 1] over observed range.
    """
    eff = window_ranking[["window_weeks", "composite_score"]].copy()
    rob = robustness_ranking[[
        "window_weeks", "robustness_score", "robustness_rank",
        "clustering_score", "stability_score", "neighbor_score",
        "plateau_score", "dominant_plateau_label",
        "mean_dropoff", "mean_n_neighbors",
    ]].copy()

    merged = eff.merge(rob, on="window_weeks", how="outer")

    def _norm(s: pd.Series) -> pd.Series:
        lo, hi = s.min(), s.max()
        return pd.Series(0.5, index=s.index) if hi == lo else (s - lo) / (hi - lo)

    merged["eff_norm"] = _norm(merged["composite_score"].fillna(0))
    merged["rob_norm"] = _norm(merged["robustness_score"].fillna(0))
    merged["combined_score"] = (merged["eff_norm"] * 0.5 + merged["rob_norm"] * 0.5).round(4)

    # Attach effectiveness rank (lower = better)
    if "rank" in window_ranking.columns:
        merged = merged.merge(
            window_ranking[["window_weeks", "rank"]].rename(columns={"rank": "effectiveness_rank"}),
            on="window_weeks", how="left",
        )
    else:
        merged["effectiveness_rank"] = merged["composite_score"].rank(ascending=False).astype(int)

    return merged.sort_values("combined_score", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# 3. Window choice
# ---------------------------------------------------------------------------

def choose_recommended_window(combined: pd.DataFrame) -> dict[str, Any]:
    """Pick best window: highest combined_score, tie-break by effectiveness_rank."""
    df = combined.sort_values(
        ["combined_score", "effectiveness_rank"],
        ascending=[False, True],
    )
    best = df.iloc[0]
    return best.to_dict()


# ---------------------------------------------------------------------------
# 4. Parameter selection rule
# ---------------------------------------------------------------------------

def choose_selection_rule(window_row: dict[str, Any]) -> str:
    rs = float(window_row.get("robustness_score", 0))
    cs = float(window_row.get("clustering_score", 0))
    ss = float(window_row.get("stability_score", 0))
    label = str(window_row.get("dominant_plateau_label", ""))

    if rs >= 0.70 and label == "stable_plateau":
        return "top_result_is_safe"
    if rs >= 0.55 and cs >= 0.65:
        return "top5_median_is_safer"
    if ss >= 0.65:
        return "cluster_centroid_recommended"
    return "avoid_parameter_update"


# ---------------------------------------------------------------------------
# 5. Caution flags
# ---------------------------------------------------------------------------

def build_caution_flags(
    window_row: dict[str, Any],
    combined: pd.DataFrame,
    selected_candidates: pd.DataFrame,
) -> list[str]:
    flags: list[str] = []

    label = str(window_row.get("dominant_plateau_label", ""))
    if label in ("sharp_spike", "extreme_spike"):
        flags.append("spike_risk")
    if label == "insufficient_evidence":
        flags.append("weak_neighbor_support")

    ss = float(window_row.get("stability_score", 1.0))
    if ss < 0.50:
        flags.append("low_cross_job_stability")

    nn = float(window_row.get("mean_n_neighbors", 99))
    if nn < 3:
        flags.append("weak_neighbor_support")

    # Conflicting signals: top effectiveness window ≠ top robustness window
    eff_top = combined.sort_values("eff_norm", ascending=False).iloc[0]["window_weeks"]
    rob_top = combined.sort_values("rob_norm", ascending=False).iloc[0]["window_weeks"]
    if eff_top != rob_top and window_row["window_weeks"] not in (eff_top, rob_top):
        flags.append("conflicting_signals")

    ww = window_row.get("window_weeks")
    n_jobs = (selected_candidates["window_weeks"] == ww).sum() if "window_weeks" in selected_candidates.columns else 0
    if n_jobs < MIN_JOBS:
        flags.append("low_sample_count")

    return sorted(set(flags))


# ---------------------------------------------------------------------------
# 6. Confidence
# ---------------------------------------------------------------------------

def assign_confidence(
    window_row: dict[str, Any],
    caution_flags: list[str],
) -> str:
    combined = float(window_row.get("combined_score", 0))
    label = str(window_row.get("dominant_plateau_label", ""))
    rs = float(window_row.get("robustness_score", 0))

    # spike_risk and conflicting_signals are hard blockers (→ low / hold)
    # low_sample_count caps at medium but still allows deployment with caution
    hard_blocking = {"spike_risk", "conflicting_signals"}
    soft_blocking = {"low_sample_count"}

    has_hard = bool(hard_blocking & set(caution_flags))
    has_soft = bool(soft_blocking & set(caution_flags))

    if has_hard:
        return "low"
    if combined >= 0.65 and rs >= 0.70 and label == "stable_plateau" and not has_soft:
        return "high"
    if combined >= 0.40:
        return "medium"
    return "low"


# ---------------------------------------------------------------------------
# 7. Deployment action
# ---------------------------------------------------------------------------

def decide_deployment_action(confidence: str, caution_flags: list[str]) -> str:
    if confidence == "low" or "spike_risk" in caution_flags:
        return "hold"
    if caution_flags:
        return "deploy_with_caution"
    if confidence == "high":
        return "deploy"
    return "deploy_with_caution"


# ---------------------------------------------------------------------------
# 8. Fallback rule
# ---------------------------------------------------------------------------

def choose_fallback_rule(window_row: dict[str, Any]) -> str:
    """Conservative fallback if primary rule cannot be applied."""
    ss = float(window_row.get("stability_score", 0))
    if ss >= 0.65:
        return "cluster_centroid_recommended"
    return "avoid_parameter_update"


# ---------------------------------------------------------------------------
# 9. Rationale
# ---------------------------------------------------------------------------

def build_rationale(
    window_row: dict[str, Any],
    selection_rule: str,
    confidence: str,
    action: str,
    caution_flags: list[str],
    combined: pd.DataFrame,
) -> str:
    ww = int(window_row["window_weeks"])
    cs = float(window_row["combined_score"])
    rs = float(window_row.get("robustness_score", 0))
    ss = float(window_row.get("stability_score", 0))
    label = window_row.get("dominant_plateau_label", "unknown")
    eff_rank = int(window_row.get("effectiveness_rank", 0))
    rob_rank = int(window_row.get("robustness_rank", 0))

    parts = [
        f"{ww}w selected (combined_score={cs:.3f}, eff_rank=#{eff_rank}, rob_rank=#{rob_rank}).",
        f"Robustness={rs:.3f}, stability={ss:.3f}, plateau={label}.",
        f"Selection rule '{selection_rule}' applied.",
    ]
    if caution_flags:
        parts.append(f"Caution flags: {', '.join(caution_flags)}.")
    parts.append(f"Action={action}, confidence={confidence}.")
    return " ".join(parts)


# ---------------------------------------------------------------------------
# 10. Evidence summary
# ---------------------------------------------------------------------------

def build_evidence_summary(combined: pd.DataFrame) -> str:
    lines = []
    for _, row in combined.iterrows():
        lines.append(
            f"  {int(row['window_weeks']):>2}w  combined={row['combined_score']:.3f}  "
            f"eff={row.get('composite_score', float('nan')):.3f}  "
            f"rob={row.get('robustness_score', float('nan')):.3f}  "
            f"label={row.get('dominant_plateau_label', 'n/a')}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 11. Recommendation record
# ---------------------------------------------------------------------------

def render_recommendation_record(
    window_row: dict[str, Any],
    selection_rule: str,
    fallback_rule: str,
    action: str,
    confidence: str,
    caution_flags: list[str],
    rationale: str,
    evidence_summary: str,
    risk_split: dict | None = None,
    deploy_preset_path: str | None = None,
) -> dict[str, Any]:
    active_strategies: list[int] | None = None
    total_risk_pct: float | None = None
    if risk_split is not None:
        active_strategies = [int(k[1:]) for k, v in sorted(risk_split.items()) if v > 0]
        total_risk_pct = float(sum(risk_split.values()))

    return {
        "as_of": date.today().isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "recommended_window": int(window_row["window_weeks"]),
        "recommended_selection_rule": selection_rule,
        "deployment_action": action,
        "confidence": confidence,
        "robustness_score": round(float(window_row.get("robustness_score", float("nan"))), 4),
        "robustness_rank": int(window_row.get("robustness_rank", 0)),
        "effectiveness_rank": int(window_row.get("effectiveness_rank", 0)),
        "combined_score": round(float(window_row["combined_score"]), 4),
        "robustness_label": str(window_row.get("dominant_plateau_label", "unknown")),
        "stability_score": round(float(window_row.get("stability_score", float("nan"))), 4),
        "clustering_score": round(float(window_row.get("clustering_score", float("nan"))), 4),
        "neighbor_score": round(float(window_row.get("neighbor_score", float("nan"))), 4),
        "rationale": rationale,
        "fallback_rule": fallback_rule,
        "caution_flags": "|".join(caution_flags) if caution_flags else "",
        "evidence_summary": evidence_summary,
        "risk_split": risk_split,
        "active_strategies": active_strategies,
        "total_risk_pct": total_risk_pct,
        "deploy_preset_path": deploy_preset_path,
    }


# ---------------------------------------------------------------------------
# 12. Output writers
# ---------------------------------------------------------------------------

def write_recommendation_outputs(
    record: dict[str, Any],
    research_dir: Path,
) -> dict[str, Path]:
    research_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}

    # Parquet + CSV
    df = pd.DataFrame([record])
    pq = research_dir / "deployment_recommendation.parquet"
    csv = research_dir / "deployment_recommendation.csv"
    df.to_parquet(pq, index=False)
    df.to_csv(csv, index=False)
    written["parquet"] = pq
    written["csv"] = csv

    # JSON
    j = research_dir / "deployment_recommendation.json"
    j.write_text(json.dumps(record, indent=2), encoding="utf-8")
    written["json"] = j

    # Human-readable TXT
    flags_str = ", ".join(record["caution_flags"].split("|")) if record["caution_flags"] else "none"
    txt_lines = [
        "Weekly Deployment Recommendation — Phase 2C",
        "=" * 48,
        f"As of              : {record['as_of']}",
        f"Recommended window : {record['recommended_window']}w",
        f"Selection rule     : {record['recommended_selection_rule']}",
        f"Deployment action  : {record['deployment_action']}",
        f"Confidence         : {record['confidence']}",
        f"Robustness score   : {record['robustness_score']}  [{record['robustness_label']}]",
        f"Combined score     : {record['combined_score']}  "
        f"(eff_rank=#{record['effectiveness_rank']}, rob_rank=#{record['robustness_rank']})",
        f"Caution flags      : {flags_str}",
        f"Fallback rule      : {record['fallback_rule']}",
        "",
        "Rationale:",
        f"  {record['rationale']}",
        "",
        "Evidence summary:",
        record["evidence_summary"],
    ]

    # Portfolio allocation section (if present)
    if record.get("risk_split") is not None:
        risk_split = record["risk_split"]
        active = record.get("active_strategies") or []
        total_risk = record.get("total_risk_pct", 0)
        alloc_parts = " ".join(f"{k}={v}" for k, v in sorted(risk_split.items()))
        active_str = ", ".join(f"S{s}" for s in active) if active else "none"
        txt_lines += [
            "",
            "Portfolio Allocation:",
            f"  Risk split         : {alloc_parts} (sum={int(total_risk)})",
            f"  Active strategies  : {active_str}",
        ]
        if record.get("deploy_preset_path"):
            txt_lines.append(f"  Deploy preset      : {record['deploy_preset_path']}")

    txt = research_dir / "deployment_recommendation.txt"
    txt.write_text("\n".join(txt_lines) + "\n", encoding="utf-8")
    written["txt"] = txt

    return written


# ---------------------------------------------------------------------------
# 13. Orchestrator
# ---------------------------------------------------------------------------

def run_deployment_recommender(research_dir: str | Path) -> dict[str, Any]:
    """Full Phase 2C pipeline.  Returns summary dict."""
    rd = Path(research_dir)

    datasets = load_recommender_inputs(rd)
    warns = validate_required_inputs(datasets)
    for w in warns:
        print(f"  WARN: {w}")

    window_ranking = datasets["window_ranking"]
    robustness_ranking = datasets["robustness_ranking"]
    selected = datasets["selected_candidates"]

    combined = combine_window_signals(window_ranking, robustness_ranking)
    window_row = choose_recommended_window(combined)

    selection_rule = choose_selection_rule(window_row)
    fallback_rule = choose_fallback_rule(window_row)
    caution_flags = build_caution_flags(window_row, combined, selected)
    confidence = assign_confidence(window_row, caution_flags)
    action = decide_deployment_action(confidence, caution_flags)
    rationale = build_rationale(window_row, selection_rule, confidence, action, caution_flags, combined)
    evidence_summary = build_evidence_summary(combined)

    record = render_recommendation_record(
        window_row, selection_rule, fallback_rule,
        action, confidence, caution_flags, rationale, evidence_summary,
    )

    written = write_recommendation_outputs(record, rd)

    return {
        "record": record,
        "outputs": {k: str(v) for k, v in written.items()},
        "windows_evaluated": sorted(combined["window_weeks"].astype(int).tolist()),
        "n_windows": len(combined),
    }
