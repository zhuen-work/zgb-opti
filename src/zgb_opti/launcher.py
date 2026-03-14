import subprocess
from datetime import datetime, timezone
from pathlib import Path

from zgb_opti.collector import append_manifest_row, default_manifest_path, ensure_output_dir
from zgb_opti.models import ManifestRow, OptimizationJob, StudyConfig


def run_mt5_job(mt5_terminal_path: str, ini_path: str) -> int:
    result = subprocess.run(
        [mt5_terminal_path, f"/config:{ini_path}"],
        check=False,
    )
    return result.returncode


def _make_row(job: OptimizationJob, **kwargs) -> ManifestRow:
    return ManifestRow(
        job_id=job.job_id,
        window_weeks=job.window_weeks,
        train_start=job.train_start,
        train_end=job.train_end,
        ini_path=job.ini_path,
        output_dir=job.output_dir,
        **kwargs,
    )


def _run_one_job(config: StudyConfig, job: OptimizationJob, manifest_path: Path) -> bool:
    """Run a single job with manifest logging. Returns True if launched (even if failed)."""
    if not Path(job.ini_path).exists():
        print(f"  WARNING: ini file not found, skipping: {job.ini_path}")
        return False

    started_at = datetime.now(timezone.utc)
    output_dir_note = "output_dir_prepared"
    try:
        ensure_output_dir(job.output_dir)
    except Exception as e:
        output_dir_note = "output_dir_prepare_failed"
        print(f"  WARNING: could not prepare output dir: {e}")

    append_manifest_row(
        manifest_path,
        _make_row(job, status="started", started_at=started_at, finished_at=None, notes=output_dir_note),
    )

    try:
        exit_code = run_mt5_job(config.mt5_terminal_path, job.ini_path)
    except Exception as e:
        finished_at = datetime.now(timezone.utc)
        print(f"  ERROR: failed to launch MT5: {e}")
        append_manifest_row(
            manifest_path,
            _make_row(job, status="failed", started_at=started_at, finished_at=finished_at, notes=f"launch_error={e}"),
        )
        return True

    finished_at = datetime.now(timezone.utc)
    status = "finished" if exit_code == 0 else "failed"
    print(f"  Exit code: {exit_code}")
    append_manifest_row(
        manifest_path,
        _make_row(job, status=status, started_at=started_at, finished_at=finished_at, notes=f"exit_code={exit_code}"),
    )
    return True


def run_single_job(config: StudyConfig, job: OptimizationJob) -> None:
    manifest_path = default_manifest_path()
    print(f"Running single job: {job.job_id}")
    _run_one_job(config, job, manifest_path)


def run_all_jobs(config: StudyConfig, jobs: list[OptimizationJob]) -> None:
    manifest_path = default_manifest_path()
    for job in jobs:
        print(f"Running job: {job.job_id}")
        _run_one_job(config, job, manifest_path)
