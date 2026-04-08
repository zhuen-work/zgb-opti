"""Tests for Phase 2B robustness analysis module."""
from pathlib import Path

import pandas as pd
import pytest

from zgb_opti.robustness import (
    classify_plateau_vs_spike,
    compute_neighbor_sensitivity,
    compute_parameter_stability,
    compute_topn_clustering,
    identify_parameter_columns,
    recommend_selection_rule,
    score_window_robustness,
)

RESEARCH_DIR = Path(__file__).parent.parent / "output" / "research"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_passes(n: int = 20, n_jobs: int = 2, seed: int = 42) -> pd.DataFrame:
    """Synthetic passes with deterministic parameter values."""
    import random
    rng = random.Random(seed)
    rows = []
    for jid in range(n_jobs):
        job_id = f"job_{jid}"
        for i in range(n):
            rows.append({
                "job_id": job_id,
                "window_weeks": 3 + jid * 3,
                "result": float(i),
                "param_A": float(rng.randint(1, 5)),
                "param_B": float(rng.randint(1, 3)),
            })
    return pd.DataFrame(rows)


def _make_selected(passes: pd.DataFrame) -> pd.DataFrame:
    return (
        passes.sort_values("result", ascending=False)
        .groupby("job_id", sort=False)
        .first()
        .reset_index()
    )


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def test_identify_parameter_columns():
    df = pd.DataFrame({"param_A": [1], "param_B": [2], "result": [3], "job_id": ["x"]})
    assert identify_parameter_columns(df) == ["param_A", "param_B"]


def test_identify_parameter_columns_empty():
    df = pd.DataFrame({"result": [1], "job_id": ["x"]})
    assert identify_parameter_columns(df) == []


def test_topn_clustering_returns_one_row_per_job():
    passes = _make_passes(n=30, n_jobs=3)
    param_cols = identify_parameter_columns(passes)
    result = compute_topn_clustering(passes, param_cols, top_n=5)
    assert len(result) == 3
    assert "clustering_score" in result.columns
    assert result["clustering_score"].between(0, 1).all()


def test_topn_clustering_single_pass():
    """Single pass per job → score = 1."""
    passes = _make_passes(n=1, n_jobs=2)
    param_cols = identify_parameter_columns(passes)
    result = compute_topn_clustering(passes, param_cols, top_n=5)
    assert (result["clustering_score"] == 1.0).all()


def test_parameter_stability_two_windows():
    passes = _make_passes(n=10, n_jobs=4)
    selected = _make_selected(passes)
    param_cols = identify_parameter_columns(passes)
    result = compute_parameter_stability(selected, param_cols)
    assert len(result) == selected["window_weeks"].nunique()
    assert "stability_score" in result.columns
    assert result["stability_score"].between(0, 1).all()


def test_neighbor_sensitivity_basic():
    passes = _make_passes(n=50, n_jobs=2)
    selected = _make_selected(passes)
    param_cols = identify_parameter_columns(passes)
    result = compute_neighbor_sensitivity(passes, selected, param_cols, radius=0.5)
    assert len(result) == len(selected)
    assert "neighbor_score" in result.columns


def test_classify_plateau_vs_spike_labels():
    df = pd.DataFrame({
        "job_id": ["a", "b", "c", "d"],
        "neighbor_score": [0.9, 0.6, 0.3, float("nan")],
        "neighbor_note": ["ok", "ok", "ok", "ok"],
        "n_neighbors": [5, 5, 5, 0],
        "dropoff": [0.1, 0.4, 0.7, float("nan")],
    })
    result = classify_plateau_vs_spike(df)
    assert result.loc[0, "plateau_label"] == "stable_plateau"
    assert result.loc[1, "plateau_label"] == "mild_plateau"
    assert result.loc[2, "plateau_label"] == "sharp_spike"


def test_score_window_robustness_shape():
    passes = _make_passes(n=20, n_jobs=4)
    selected = _make_selected(passes)
    param_cols = identify_parameter_columns(passes)
    clustering_df = compute_topn_clustering(passes, param_cols, top_n=5)
    stability_df = compute_parameter_stability(selected, param_cols)
    neighbor_raw = compute_neighbor_sensitivity(passes, selected, param_cols, radius=0.5)
    neighbor_df = classify_plateau_vs_spike(neighbor_raw)
    ranking = score_window_robustness(clustering_df, stability_df, neighbor_df, selected)
    assert len(ranking) == selected["window_weeks"].nunique()
    assert "robustness_score" in ranking.columns
    assert "robustness_rank" in ranking.columns
    assert ranking["robustness_score"].between(0, 1).all()


def test_recommend_selection_rule_returns_valid():
    passes = _make_passes(n=30, n_jobs=4)
    selected = _make_selected(passes)
    param_cols = identify_parameter_columns(passes)
    clustering_df = compute_topn_clustering(passes, param_cols, top_n=5)
    stability_df = compute_parameter_stability(selected, param_cols)
    neighbor_raw = compute_neighbor_sensitivity(passes, selected, param_cols, radius=0.5)
    neighbor_df = classify_plateau_vs_spike(neighbor_raw)
    ranking = score_window_robustness(clustering_df, stability_df, neighbor_df, selected)
    rule, note = recommend_selection_rule(ranking, neighbor_df, clustering_df)
    valid_rules = {
        "top_result_is_safe",
        "top5_median_is_safer",
        "cluster_centroid_recommended",
        "parameter_sensitivity_too_high",
    }
    assert rule in valid_rules
    assert isinstance(note, str) and len(note) > 0


# ---------------------------------------------------------------------------
# Smoke test against real research data (skipped if not available)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not (RESEARCH_DIR / "all_passes.parquet").exists(),
    reason="Real research data not available",
)
def test_robustness_smoke_real_data():
    from zgb_opti.robustness import run_robustness_analysis
    import tempfile, os

    with tempfile.TemporaryDirectory() as tmpdir:
        # Copy required parquets to temp dir
        import shutil
        for name in ["all_passes", "selected_candidates", "final_study_dataset", "window_ranking"]:
            src = RESEARCH_DIR / f"{name}.parquet"
            if src.exists():
                shutil.copy(src, tmpdir)
        result = run_robustness_analysis(tmpdir)

    assert result["n_rows_analyzed"] > 0
    assert result["n_windows"] == 5
    assert result["rule"] in {
        "top_result_is_safe",
        "top5_median_is_safer",
        "cluster_centroid_recommended",
        "parameter_sensitivity_too_high",
    }
    assert len(result["robustness_ranking"]) == 5
