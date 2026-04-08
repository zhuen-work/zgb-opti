"""Tests for Phase 4 research aggregation and candidate selection."""
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from zgb_opti.models import OptimizationJob
from zgb_opti.research import SELECTION_METHOD, _viability_mask, select_candidates

SAMPLE_PARQUET = (
    Path(__file__).parent.parent
    / "output/optimizations/opt_3w_2025-12-01_2025-12-21/passes.parquet"
)


def _make_job(job_id: str = "test_job") -> OptimizationJob:
    return OptimizationJob(
        job_id=job_id,
        window_weeks=3,
        train_start=date(2025, 12, 1),
        train_end=date(2025, 12, 21),
        symbol="XAUUSD",
        timeframe="M15",
        ea_name="ZGB5_v14_3",
        ini_path="jobs/generated/test.ini",
        output_dir="output/optimizations/test_job",
    )


def test_viability_mask_basic():
    df = pd.DataFrame({
        "trades": [10, 0, 5],
        "net_profit": [100.0, 50.0, -10.0],
        "drawdown_pct": [20.0, 10.0, 30.0],
        "profit_factor": [1.5, 1.2, 0.8],
    })
    mask = _viability_mask(df)
    assert mask.tolist() == [True, False, False]


def test_select_candidates_one_per_job():
    df = pd.DataFrame({
        "job_id": ["job_a", "job_a", "job_b"],
        "result": [100.0, 90.0, 80.0],
        "net_profit": [50.0, 40.0, 30.0],
        "profit_factor": [1.5, 1.3, 1.2],
        "drawdown_pct": [20.0, 25.0, 15.0],
        "trades": [10, 8, 6],
        "window_weeks": [3, 3, 4],
        "train_start": ["2025-12-01", "2025-12-01", "2025-12-08"],
        "train_end": ["2025-12-21", "2025-12-21", "2025-12-28"],
        "symbol": ["XAUUSD"] * 3,
        "timeframe": ["M15"] * 3,
        "ea_name": ["ZGB5_v14_3"] * 3,
    })
    candidates = select_candidates(df)
    assert len(candidates) == 2
    assert set(candidates["job_id"]) == {"job_a", "job_b"}
    # job_a best is result=100
    assert candidates.loc[candidates["job_id"] == "job_a", "result"].iloc[0] == 100.0
    assert candidates["selection_method"].iloc[0] == SELECTION_METHOD


@pytest.mark.skipif(not SAMPLE_PARQUET.exists(), reason="sample parquet not present (gitignored)")
def test_aggregate_real_job(tmp_path):
    from zgb_opti.research import aggregate_passes, build_research_dataset

    job = _make_job("opt_3w_2025-12-01_2025-12-21")
    # Point output_dir to the real folder
    job = job.model_copy(update={"output_dir": str(SAMPLE_PARQUET.parent)})

    all_passes, skipped = aggregate_passes([job])
    assert len(all_passes) == 100
    assert skipped == []
    assert "window_weeks" in all_passes.columns
    assert "symbol" in all_passes.columns


@pytest.mark.skipif(not SAMPLE_PARQUET.exists(), reason="sample parquet not present (gitignored)")
def test_build_research_dataset_smoke(tmp_path):
    from zgb_opti.research import build_research_dataset

    job = _make_job("opt_3w_2025-12-01_2025-12-21")
    job = job.model_copy(update={"output_dir": str(SAMPLE_PARQUET.parent)})

    summary = build_research_dataset([job], tmp_path / "research")
    assert summary["jobs_included"] == 1
    assert summary["total_passes"] == 100
    assert summary["candidates"] == 1
    assert (tmp_path / "research" / "all_passes.parquet").exists()
    assert (tmp_path / "research" / "selected_candidates.parquet").exists()
    assert (tmp_path / "research" / "jobs_summary.parquet").exists()
