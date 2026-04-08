"""Smoke tests for xml_parser. Uses the real copied XML if present (gitignored),
otherwise skips gracefully."""
from pathlib import Path

import pytest

from zgb_opti.xml_parser import METRIC_RENAME, _build_col_map, parse_optimization_xml

SAMPLE_XML = (
    Path(__file__).parent.parent
    / "output/optimizations/opt_3w_2025-12-01_2025-12-21/opt_3w_2025-12-01_2025-12-21.xml"
)
JOB_ID = "opt_3w_2025-12-01_2025-12-21"


def test_col_map_metrics():
    col_map = _build_col_map(list(METRIC_RENAME.keys()))
    assert col_map["Pass"] == "pass_id"
    assert col_map["Profit"] == "net_profit"
    assert col_map["Equity DD %"] == "drawdown_pct"
    assert col_map["Trades"] == "trades"


def test_col_map_params():
    col_map = _build_col_map(["InpSMA_Fast_S1", "InpFractalRR_S3"])
    assert col_map["InpSMA_Fast_S1"] == "param_InpSMA_Fast_S1"
    assert col_map["InpFractalRR_S3"] == "param_InpFractalRR_S3"


@pytest.mark.skipif(not SAMPLE_XML.exists(), reason="sample XML not present (gitignored)")
def test_parse_smoke():
    records, warns = parse_optimization_xml(SAMPLE_XML, JOB_ID)
    assert len(records) >= 100, f"Expected >=100 passes, got {len(records)}"

    first = records[0]
    assert first["job_id"] == JOB_ID
    assert "pass_id" in first
    assert "net_profit" in first
    assert "profit_factor" in first
    assert "drawdown_pct" in first
    assert "trades" in first

    param_cols = [k for k in first if k.startswith("param_")]
    assert len(param_cols) >= 3, f"Expected >=3 param cols, got {param_cols}"

    # pass_id should be numeric
    assert isinstance(first["pass_id"], float), f"pass_id type: {type(first['pass_id'])}"
    assert warns == [], f"Unexpected warnings: {warns}"


@pytest.mark.skipif(not SAMPLE_XML.exists(), reason="sample XML not present (gitignored)")
def test_parse_write_smoke(tmp_path):
    from zgb_opti.xml_parser import parse_and_write

    parquet_path, csv_path, n_rows, warns = parse_and_write(SAMPLE_XML, tmp_path, JOB_ID)

    assert parquet_path.exists()
    assert csv_path.exists()
    assert n_rows >= 100

    import pandas as pd
    df = pd.read_parquet(parquet_path)
    assert len(df) == n_rows
    assert "pass_id" in df.columns
    assert "param_InpSMA_Fast_S1" in df.columns
    assert df["pass_id"].dtype.name == "Int64"
