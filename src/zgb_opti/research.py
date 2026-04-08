"""Phase 4 & 5: cross-job aggregation, candidate selection, and forward testing."""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd

from zgb_opti import portfolio
from zgb_opti.models import OptimizationJob, StudyConfig

# Viability filters applied before ranking
_MIN_TRADES = 1
_MAX_DD_PCT = 80.0
_MIN_PROFIT_FACTOR = 1.0

SELECTION_METHOD = "result_desc_v1"


def _viability_mask(df: pd.DataFrame) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    if "trades" in df.columns:
        mask &= df["trades"].fillna(0) >= _MIN_TRADES
    if "net_profit" in df.columns:
        mask &= df["net_profit"].fillna(0) > 0
    if "drawdown_pct" in df.columns:
        mask &= df["drawdown_pct"].fillna(100) < _MAX_DD_PCT
    if "profit_factor" in df.columns:
        mask &= df["profit_factor"].fillna(0) >= _MIN_PROFIT_FACTOR
    return mask


def aggregate_passes(
    jobs: list[OptimizationJob],
) -> tuple[pd.DataFrame, list[str]]:
    """Load passes.parquet for every job, attach job metadata, return combined DataFrame.

    Returns (all_passes_df, skipped_job_ids).
    """
    frames: list[pd.DataFrame] = []
    skipped: list[str] = []

    for job in jobs:
        parquet_path = Path(job.output_dir) / "passes.parquet"
        if not parquet_path.exists():
            skipped.append(job.job_id)
            continue
        try:
            df = pd.read_parquet(parquet_path)
            df["window_weeks"] = job.window_weeks
            df["train_start"] = str(job.train_start)
            df["train_end"] = str(job.train_end)
            df["symbol"] = job.symbol
            df["timeframe"] = job.timeframe
            df["ea_name"] = job.ea_name
            frames.append(df)
            print(f"  loaded {job.job_id}: {len(df)} passes")
        except Exception as e:
            print(f"  WARNING: could not load {parquet_path}: {e}")
            skipped.append(job.job_id)

    if not frames:
        return pd.DataFrame(), skipped

    combined = pd.concat(frames, ignore_index=True)
    return combined, skipped


def select_candidates(all_passes: pd.DataFrame) -> pd.DataFrame:
    """Select one best-scoring pass per job after viability filtering.

    Selection method: result_desc_v1
      - filter: trades >= 1, net_profit > 0, drawdown_pct < 80, profit_factor >= 1.0
      - rank by: result (descending) — MT5's own optimization score
      - pick: top row per job_id
    """
    if all_passes.empty:
        return pd.DataFrame()

    viable = all_passes[_viability_mask(all_passes)].copy()

    if viable.empty:
        print("  WARNING: no viable passes after filtering")
        return pd.DataFrame()

    viable = viable.sort_values("result", ascending=False)
    candidates = viable.groupby("job_id", sort=False).first().reset_index()
    candidates["selection_method"] = SELECTION_METHOD
    return candidates


def select_portfolio_candidates(
    all_passes: pd.DataFrame,
) -> pd.DataFrame:
    """Select best-scoring pass per job after portfolio hedge-dependency filtering + viability.

    Selection method: portfolio_result_desc_v1
      - filter: hedge dependency rules (S2 requires S1, S5 requires S4)
      - filter: trades >= 1, net_profit > 0, drawdown_pct < 80, profit_factor >= 1.0
      - rank by: result (descending)
      - pick: top row per job_id
      - adds allocation columns S1..S5 per row
    """
    if all_passes.empty:
        return pd.DataFrame()

    print("  Filtering portfolio passes by hedge dependency rules...")
    portfolio_passes = portfolio.filter_portfolio_passes(all_passes)
    print(f"  Portfolio passes: {len(portfolio_passes)} / {len(all_passes)} total")

    if portfolio_passes.empty:
        print("  WARNING: no passes match portfolio allocation constraint")
        return pd.DataFrame()

    viable = portfolio_passes[_viability_mask(portfolio_passes)].copy()

    if viable.empty:
        print("  WARNING: no viable portfolio passes after filtering")
        return pd.DataFrame()

    viable = viable.sort_values("result", ascending=False)
    candidates = viable.groupby("job_id", sort=False).first().reset_index()
    candidates["selection_method"] = "portfolio_result_desc_v1"

    # Add allocation columns
    alloc_records = candidates.apply(portfolio.extract_allocation, axis=1)
    alloc_df = pd.DataFrame(list(alloc_records))
    for col in alloc_df.columns:
        candidates[col] = alloc_df[col].values

    return candidates


def _jobs_summary(all_passes: pd.DataFrame) -> pd.DataFrame:
    if all_passes.empty:
        return pd.DataFrame()

    viable_mask = _viability_mask(all_passes)

    def agg_job(grp: pd.DataFrame) -> pd.Series:
        viable = grp[viable_mask.loc[grp.index]]
        return pd.Series({
            "window_weeks": grp["window_weeks"].iloc[0],
            "train_start": grp["train_start"].iloc[0],
            "train_end": grp["train_end"].iloc[0],
            "symbol": grp["symbol"].iloc[0],
            "timeframe": grp["timeframe"].iloc[0],
            "ea_name": grp["ea_name"].iloc[0],
            "n_passes": len(grp),
            "n_viable": len(viable),
            "best_result": grp["result"].max() if "result" in grp.columns else None,
            "best_net_profit": grp["net_profit"].max() if "net_profit" in grp.columns else None,
            "best_profit_factor": grp["profit_factor"].max() if "profit_factor" in grp.columns else None,
            "min_drawdown_pct": grp["drawdown_pct"].min() if "drawdown_pct" in grp.columns else None,
            "max_trades": grp["trades"].max() if "trades" in grp.columns else None,
        })

    return all_passes.groupby("job_id", sort=False).apply(agg_job).reset_index()


def write_research_outputs(
    all_passes: pd.DataFrame,
    candidates: pd.DataFrame,
    research_dir: Path,
) -> dict[str, Path]:
    research_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}

    def _write(df: pd.DataFrame, stem: str) -> None:
        pq = research_dir / f"{stem}.parquet"
        csv = research_dir / f"{stem}.csv"
        df.to_parquet(pq, index=False)
        df.to_csv(csv, index=False)
        written[stem] = pq

    if not all_passes.empty:
        _write(all_passes, "all_passes")
        _write(_jobs_summary(all_passes), "jobs_summary")
    if not candidates.empty:
        _write(candidates, "selected_candidates")

    return written


def build_research_dataset(
    jobs: list[OptimizationJob],
    research_dir: str | Path,
    portfolio_mode: bool = False,
) -> dict:
    """Orchestrate aggregate → select → write. Returns summary dict."""
    rd = Path(research_dir)

    print(f"Aggregating passes from {len(jobs)} jobs...")
    all_passes, skipped = aggregate_passes(jobs)

    n_included = all_passes["job_id"].nunique() if not all_passes.empty else 0
    print(f"  included: {n_included} jobs, skipped: {len(skipped)}")
    if skipped:
        print(f"  skipped (no passes.parquet): {len(skipped)} jobs")

    if portfolio_mode:
        print("Selecting portfolio candidates...")
        candidates = select_portfolio_candidates(all_passes)
    else:
        print("Selecting candidates...")
        candidates = select_candidates(all_passes)
    print(f"  selected: {len(candidates)} candidates")

    print("Writing research outputs...")
    written = write_research_outputs(all_passes, candidates, rd)
    for stem, path in written.items():
        print(f"  {stem}: {path}")

    sel_method = "portfolio_result_desc_v1" if portfolio_mode else SELECTION_METHOD
    return {
        "jobs_total": len(jobs),
        "jobs_included": n_included,
        "jobs_skipped": len(skipped),
        "total_passes": len(all_passes),
        "candidates": len(candidates),
        "outputs": {k: str(v) for k, v in written.items()},
        "selection_method": sel_method,
    }



# ---------------------------------------------------------------------------
# Phase 5: Forward Testing Engine
# ---------------------------------------------------------------------------

def _extract_param_values(row: pd.Series) -> dict[str, str]:
    """Return {param_name: formatted_value} from param_ columns in a candidate row."""
    params: dict[str, str] = {}
    for col in row.index:
        if col.startswith("param_") and pd.notna(row[col]):
            name = col[len("param_"):]
            val = row[col]
            try:
                f = float(val)
                params[name] = str(int(f)) if f == int(f) else str(f)
            except (TypeError, ValueError):
                params[name] = str(val)
    return params


def _make_forward_spec(row: pd.Series, forward_root: Path, forward_weeks: int = 1) -> dict:
    """Compute forward test spec for one candidate row."""
    train_end = pd.to_datetime(row["train_end"]).date()
    forward_start = train_end + timedelta(days=1)
    forward_end = forward_start + timedelta(weeks=forward_weeks)
    original_job_id = row["job_id"]
    fwd_id = f"fwd_{original_job_id}"
    output_dir = forward_root / original_job_id
    return {
        "original_job_id": original_job_id,
        "fwd_id": fwd_id,
        "forward_start": forward_start,
        "forward_end": forward_end,
        "symbol": str(row.get("symbol", "")),
        "timeframe": str(row.get("timeframe", "")),
        "window_weeks": int(row.get("window_weeks", 0)),
        "ea_name": str(row.get("ea_name", "")),
        "output_dir": output_dir,
        "ini_path": output_dir / "tester.ini",
        "param_values": _extract_param_values(row),
    }


def _find_forward_report(spec: dict) -> Path | None:
    """Return path to existing forward report in output_dir, or None."""
    output_dir = Path(spec["output_dir"])
    fwd_id = spec["fwd_id"]
    for ext in (".xml", ".html", ".htm"):
        p = output_dir / f"{fwd_id}{ext}"
        if p.exists():
            return p
    return None


def _run_forward_mt5(spec: dict, config: StudyConfig) -> Path | None:
    """Write INI, launch MT5, collect artifact. Returns collected report path or None."""
    from zgb_opti.collector import copy_report_artifact, ensure_output_dir, find_report_artifact
    from zgb_opti.ini_writer import build_forward_ini_content
    from zgb_opti.launcher import run_mt5_job

    output_dir = Path(spec["output_dir"])
    ensure_output_dir(output_dir)

    ini_content = build_forward_ini_content(
        param_values=spec["param_values"],
        forward_start=spec["forward_start"],
        forward_end=spec["forward_end"],
        config=config,
        fwd_id=spec["fwd_id"],
        symbol=spec["symbol"],
        timeframe=spec["timeframe"],
    )
    ini_path = Path(spec["ini_path"])
    ini_path.write_text(ini_content, encoding="utf-8")

    exit_code = run_mt5_job(config.mt5_terminal_path, str(ini_path))
    print(f"  Exit code: {exit_code}")

    artifact = find_report_artifact(output_dir, spec["fwd_id"], config.mt5_terminal_path)
    if artifact is None:
        print(f"  WARNING: no report artifact found for {spec['fwd_id']}")
        return None
    try:
        copied = copy_report_artifact(artifact, output_dir)
        print(f"  Collected: {copied.name}")
        return copied
    except Exception as e:
        print(f"  WARNING: could not copy artifact: {e}")
        return artifact


def run_forward_tests(
    specs: list[dict],
    config: StudyConfig,
    skip_existing: bool = True,
) -> dict:
    """Run MT5 single backtest for each forward spec. Returns summary."""
    n = len(specs)
    executed = 0
    skipped = 0
    failed = 0

    for i, spec in enumerate(specs, 1):
        label = f"[{i}/{n}] {spec['fwd_id']}"
        existing = _find_forward_report(spec)
        if skip_existing and existing is not None:
            print(f"{label} — skip (report exists: {existing.name})")
            skipped += 1
            continue

        print(f"{label} — forward {spec['forward_start']} to {spec['forward_end']}")
        result = _run_forward_mt5(spec, config)
        if result is not None:
            executed += 1
        else:
            failed += 1

    return {"executed": executed, "skipped": skipped, "failed": failed}


def parse_all_forward_reports(specs: list[dict]) -> pd.DataFrame:
    """Parse forward report for each spec. Returns DataFrame with one row per job."""
    from zgb_opti.xml_parser import parse_forward_report

    rows: list[dict] = []
    for spec in specs:
        report_path = _find_forward_report(spec)
        if report_path is None:
            print(f"  WARNING: no report for {spec['fwd_id']}, skipping parse")
            continue
        try:
            metrics, warns = parse_forward_report(report_path, spec["original_job_id"])
            if warns:
                for w in warns:
                    print(f"  WARN [{spec['fwd_id']}]: {w}")
            metrics["forward_start"] = str(spec["forward_start"])
            metrics["forward_end"] = str(spec["forward_end"])
            rows.append(metrics)
            print(f"  Parsed {spec['fwd_id']}: trades={metrics.get('trades')}, "
                  f"net_profit={metrics.get('net_profit')}")
        except Exception as e:
            print(f"  ERROR parsing {spec['fwd_id']}: {e}")

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def merge_final_study(
    candidates: pd.DataFrame,
    forward_results: pd.DataFrame,
) -> pd.DataFrame:
    """Merge train candidates with forward results into final study dataset."""
    if forward_results.empty:
        return pd.DataFrame()

    # Rename forward metrics to forward_ prefix
    fwd_rename = {
        "net_profit": "forward_net_profit",
        "profit_factor": "forward_profit_factor",
        "drawdown_pct": "forward_drawdown_pct",
        "expected_payoff": "forward_expected_payoff",
        "trades": "forward_trades",
        "final_equity": "forward_final_equity",
    }
    fwd = forward_results.rename(columns=fwd_rename)

    merged = candidates.merge(fwd, on="job_id", how="inner")

    # Build ordered output columns
    core = [
        "job_id", "window_weeks",
        "train_start", "train_end", "forward_start", "forward_end",
        "net_profit", "profit_factor", "drawdown_pct",
        "forward_net_profit", "forward_profit_factor", "forward_drawdown_pct",
    ]
    param_cols = [c for c in candidates.columns if c.startswith("param_")]
    extra = [c for c in merged.columns if c not in core and c not in param_cols]
    ordered = [c for c in core + param_cols + extra if c in merged.columns]
    return merged[ordered]


def build_forward_dataset(
    candidates_parquet: str | Path,
    config: StudyConfig,
    research_dir: str | Path,
    skip_existing: bool = True,
) -> dict:
    """Orchestrate Phase 5: generate → run → parse → merge → write.

    Returns summary dict.
    """
    rd = Path(research_dir)
    forward_root = rd.parent / "forward_jobs"

    print(f"Loading candidates from {candidates_parquet}...")
    candidates = pd.read_parquet(candidates_parquet)
    print(f"  {len(candidates)} candidates loaded")

    fw = getattr(config, "forward_weeks", 1)
    specs = [_make_forward_spec(row, forward_root, forward_weeks=fw) for _, row in candidates.iterrows()]

    print(f"\n=== Running {len(specs)} forward tests ===")
    run_summary = run_forward_tests(specs, config, skip_existing=skip_existing)
    print(f"  Executed: {run_summary['executed']}, Skipped: {run_summary['skipped']}, "
          f"Failed: {run_summary['failed']}")

    print("\n=== Parsing forward reports ===")
    forward_results = parse_all_forward_reports(specs)
    print(f"  Parsed: {len(forward_results)} reports")

    if forward_results.empty:
        print("  WARNING: no forward results parsed; skipping merge")
        return {**run_summary, "parsed": 0, "final_study_rows": 0}

    # Write forward_results
    rd.mkdir(parents=True, exist_ok=True)
    fwd_pq = rd / "forward_results.parquet"
    fwd_csv = rd / "forward_results.csv"
    forward_results.to_parquet(fwd_pq, index=False)
    forward_results.to_csv(fwd_csv, index=False)
    print(f"\n  forward_results: {fwd_pq}")

    print("\n=== Building final study dataset ===")
    final = merge_final_study(candidates, forward_results)
    final_pq = rd / "final_study_dataset.parquet"
    final_csv = rd / "final_study_dataset.csv"
    final.to_parquet(final_pq, index=False)
    final.to_csv(final_csv, index=False)
    print(f"  final_study_dataset: {final_pq} ({len(final)} rows)")

    return {
        **run_summary,
        "parsed": len(forward_results),
        "final_study_rows": len(final),
        "outputs": {
            "forward_results": str(fwd_pq),
            "final_study_dataset": str(final_pq),
        },
    }
