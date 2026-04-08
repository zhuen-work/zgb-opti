"""Tests for Phase 2C Weekly Deployment Recommender."""
from pathlib import Path

import pandas as pd
import pytest

from zgb_opti.recommender import (
    assign_confidence,
    build_caution_flags,
    build_rationale,
    choose_fallback_rule,
    choose_recommended_window,
    choose_selection_rule,
    combine_window_signals,
    decide_deployment_action,
    render_recommendation_record,
    validate_required_inputs,
)

RESEARCH_DIR = Path(__file__).parent.parent / "output" / "research"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_window_ranking(windows=(3, 6, 12), best=12) -> pd.DataFrame:
    rows = []
    for i, w in enumerate(sorted(windows, key=lambda x: -int(x == best))):
        rows.append({
            "window_weeks": w,
            "rank": i + 1,
            "composite_score": round(1.0 - i * 0.15, 3),
        })
    return pd.DataFrame(rows)


def _make_robustness_ranking(windows=(3, 6, 12), best=12) -> pd.DataFrame:
    rows = []
    for i, w in enumerate(sorted(windows, key=lambda x: -int(x == best))):
        rows.append({
            "window_weeks": w,
            "robustness_rank": i + 1,
            "robustness_score": round(0.85 - i * 0.10, 3),
            "clustering_score": 0.65,
            "stability_score": 1.0 if w == best else 0.52,
            "neighbor_score": 0.78,
            "plateau_score": 1.0,
            "dominant_plateau_label": "stable_plateau",
            "mean_dropoff": 0.05,
            "mean_n_neighbors": 5.0,
        })
    return pd.DataFrame(rows)


def _make_selected(windows=(3, 6, 12), jobs_per=2) -> pd.DataFrame:
    rows = []
    for w in windows:
        for j in range(jobs_per):
            rows.append({"job_id": f"job_{w}_{j}", "window_weeks": w})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def test_combine_window_signals_shape():
    wr = _make_window_ranking()
    rr = _make_robustness_ranking()
    combined = combine_window_signals(wr, rr)
    assert len(combined) == 3
    assert "combined_score" in combined.columns
    assert combined["combined_score"].between(0, 1).all()


def test_combine_window_signals_best_first():
    wr = _make_window_ranking(best=12)
    rr = _make_robustness_ranking(best=12)
    combined = combine_window_signals(wr, rr)
    assert int(combined.iloc[0]["window_weeks"]) == 12


def test_choose_recommended_window_returns_dict():
    wr = _make_window_ranking(best=12)
    rr = _make_robustness_ranking(best=12)
    combined = combine_window_signals(wr, rr)
    row = choose_recommended_window(combined)
    assert isinstance(row, dict)
    assert int(row["window_weeks"]) == 12


def test_choose_selection_rule_high_robustness():
    row = {
        "robustness_score": 0.82,
        "clustering_score": 0.65,
        "stability_score": 1.0,
        "dominant_plateau_label": "stable_plateau",
    }
    assert choose_selection_rule(row) == "top_result_is_safe"


def test_choose_selection_rule_medium_robustness():
    row = {
        "robustness_score": 0.60,
        "clustering_score": 0.70,
        "stability_score": 0.5,
        "dominant_plateau_label": "mild_plateau",
    }
    assert choose_selection_rule(row) == "top5_median_is_safer"


def test_choose_selection_rule_stable_but_low_neighbor():
    row = {
        "robustness_score": 0.40,
        "clustering_score": 0.40,
        "stability_score": 0.80,
        "dominant_plateau_label": "sharp_spike",
    }
    assert choose_selection_rule(row) == "cluster_centroid_recommended"


def test_choose_selection_rule_fragile():
    row = {
        "robustness_score": 0.30,
        "clustering_score": 0.30,
        "stability_score": 0.30,
        "dominant_plateau_label": "extreme_spike",
    }
    assert choose_selection_rule(row) == "avoid_parameter_update"


def test_build_caution_flags_spike():
    wr = _make_window_ranking(best=12)
    rr = _make_robustness_ranking(best=12)
    combined = combine_window_signals(wr, rr)
    row = choose_recommended_window(combined)
    row = dict(row)
    row["dominant_plateau_label"] = "sharp_spike"
    flags = build_caution_flags(row, combined, _make_selected())
    assert "spike_risk" in flags


def test_build_caution_flags_clean():
    wr = _make_window_ranking(best=12)
    rr = _make_robustness_ranking(best=12)
    combined = combine_window_signals(wr, rr)
    row = choose_recommended_window(combined)
    flags = build_caution_flags(row, combined, _make_selected())
    assert "spike_risk" not in flags


def test_assign_confidence_high():
    row = {
        "combined_score": 0.80,
        "robustness_score": 0.83,
        "dominant_plateau_label": "stable_plateau",
    }
    assert assign_confidence(row, []) == "high"


def test_assign_confidence_low():
    row = {
        "combined_score": 0.80,
        "robustness_score": 0.80,
        "dominant_plateau_label": "stable_plateau",
    }
    # spike_risk is a hard blocker → always low regardless of scores
    assert assign_confidence(row, ["spike_risk"]) == "low"


def test_assign_confidence_medium_low_sample():
    row = {
        "combined_score": 0.80,
        "robustness_score": 0.83,
        "dominant_plateau_label": "stable_plateau",
    }
    # low_sample_count is soft: caps at medium but doesn't go low
    assert assign_confidence(row, ["low_sample_count"]) == "medium"


def test_deployment_action_deploy():
    assert decide_deployment_action("high", []) == "deploy"


def test_deployment_action_caution():
    assert decide_deployment_action("high", ["weak_neighbor_support"]) == "deploy_with_caution"


def test_deployment_action_hold():
    assert decide_deployment_action("low", []) == "hold"
    assert decide_deployment_action("medium", ["spike_risk"]) == "hold"


def test_choose_fallback_rule():
    assert choose_fallback_rule({"stability_score": 0.8}) == "cluster_centroid_recommended"
    assert choose_fallback_rule({"stability_score": 0.3}) == "avoid_parameter_update"


def test_render_recommendation_record_fields():
    row = {
        "window_weeks": 12,
        "combined_score": 0.9,
        "robustness_score": 0.83,
        "robustness_rank": 1,
        "effectiveness_rank": 1,
        "dominant_plateau_label": "stable_plateau",
        "stability_score": 1.0,
        "clustering_score": 0.65,
        "neighbor_score": 0.78,
    }
    rec = render_recommendation_record(
        row, "top_result_is_safe", "cluster_centroid_recommended",
        "deploy", "high", [], "rationale text", "evidence text",
    )
    required_fields = {
        "as_of", "recommended_window", "recommended_selection_rule",
        "deployment_action", "confidence", "robustness_score",
        "effectiveness_rank", "robustness_rank", "robustness_label",
        "rationale", "fallback_rule", "caution_flags", "evidence_summary",
    }
    assert required_fields.issubset(set(rec.keys()))
    assert rec["deployment_action"] == "deploy"
    assert rec["confidence"] == "high"


def test_validate_required_inputs_ok():
    datasets = {
        "window_ranking": pd.DataFrame({"window_weeks": [12], "composite_score": [0.9], "rank": [1]}),
        "robustness_ranking": pd.DataFrame({"window_weeks": [12], "robustness_score": [0.83]}),
        "robustness_analysis": pd.DataFrame(),
        "selected_candidates": pd.DataFrame(),
    }
    warns = validate_required_inputs(datasets)
    assert warns == []


# ---------------------------------------------------------------------------
# Smoke test against real data
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not (RESEARCH_DIR / "window_ranking.parquet").exists(),
    reason="Real research data not available",
)
def test_recommender_smoke_real_data(tmp_path):
    import shutil
    for name in ["window_ranking", "robustness_ranking", "robustness_analysis", "selected_candidates"]:
        src = RESEARCH_DIR / f"{name}.parquet"
        if src.exists():
            shutil.copy(src, tmp_path)

    from zgb_opti.recommender import run_deployment_recommender
    result = run_deployment_recommender(tmp_path)

    rec = result["record"]
    assert rec["recommended_window"] > 0
    assert rec["deployment_action"] in {"deploy", "deploy_with_caution", "hold", "insufficient_evidence"}
    assert rec["confidence"] in {"high", "medium", "low"}
    assert result["n_windows"] == 5

    # Check all 4 output files were written
    assert (tmp_path / "deployment_recommendation.parquet").exists()
    assert (tmp_path / "deployment_recommendation.csv").exists()
    assert (tmp_path / "deployment_recommendation.json").exists()
    assert (tmp_path / "deployment_recommendation.txt").exists()
