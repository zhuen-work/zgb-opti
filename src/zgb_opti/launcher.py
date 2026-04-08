import subprocess
from datetime import datetime, timezone
from pathlib import Path

from zgb_opti.collector import append_manifest_row, copy_report_artifact, default_manifest_path, ensure_output_dir, find_report_artifact
from zgb_opti.ini_writer import build_ini_content, write_ini_file
from zgb_opti.models import ManifestRow, OptimizationJob, StudyConfig
from zgb_opti.xml_parser import parse_and_write


def run_mt5_job(mt5_terminal_path: str, ini_path: str) -> int:
    abs_ini = str(Path(ini_path).resolve())  # absolute, backslashes on Windows
    cmd = [mt5_terminal_path, f"/config:{abs_ini}"]
    print(f"Launch command: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=False)
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
        print(f"  Generating ini: {job.ini_path}")
        ini_content = build_ini_content("", job, config)
        write_ini_file(job, ini_content)

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
    artifact = find_report_artifact(job.output_dir, job.job_id, config.mt5_terminal_path)
    if artifact is not None:
        found_posix = artifact.resolve().as_posix()
        try:
            copied = copy_report_artifact(artifact, job.output_dir)
            notes = f"exit_code={exit_code};report_found={found_posix};report_copied={copied.as_posix()}"
        except Exception:
            notes = f"exit_code={exit_code};report_found={found_posix};report_copied=failed"
    else:
        notes = f"exit_code={exit_code};report_found=none"
    append_manifest_row(
        manifest_path,
        _make_row(job, status=status, started_at=started_at, finished_at=finished_at, notes=notes),
    )
    return True


def run_single_job(config: StudyConfig, job: OptimizationJob) -> None:
    manifest_path = default_manifest_path()
    print(f"Running single job: {job.job_id}")
    _run_one_job(config, job, manifest_path)


def run_all_jobs(config: StudyConfig, jobs: list[OptimizationJob], skip_existing: bool = True) -> None:
    """Run jobs sequentially. Skips jobs whose XML report already exists when skip_existing=True."""
    manifest_path = default_manifest_path()
    n = len(jobs)
    for i, job in enumerate(jobs, 1):
        xml_path = Path(job.output_dir) / f"{job.job_id}.xml"
        if skip_existing and xml_path.exists():
            print(f"[{i}/{n}] Skipping {job.job_id} (XML exists)")
            if not (Path(job.output_dir) / "passes.parquet").exists():
                parse_job_report(job)
            continue
        print(f"[{i}/{n}] Running job: {job.job_id}")
        _run_one_job(config, job, manifest_path)
        parse_job_report(job)


def parse_job_report(job: OptimizationJob) -> None:
    """Parse the copied XML report for a job and append a manifest row."""
    manifest_path = default_manifest_path()
    output_dir = Path(job.output_dir)

    xml_path: Path | None = None
    for ext in (".xml", ".htm", ".html"):
        candidate = output_dir / f"{job.job_id}{ext}"
        if candidate.exists():
            xml_path = candidate
            break

    if xml_path is None:
        print(f"  WARNING: no report file found for {job.job_id} in {output_dir}")
        return

    print(f"  Parsing: {xml_path.name}")
    started_at = datetime.now(timezone.utc)
    try:
        parquet_path, csv_path, n_rows, warns = parse_and_write(xml_path, output_dir, job.job_id)
        finished_at = datetime.now(timezone.utc)
        warn_note = f";warnings={warns}" if warns else ""
        notes = f"rows={n_rows};parquet={parquet_path.as_posix()};csv={csv_path.as_posix()}{warn_note}"
        status = "parsed"
        print(f"  Parsed {n_rows} passes -> {parquet_path.name}, {csv_path.name}")
    except Exception as e:
        finished_at = datetime.now(timezone.utc)
        notes = f"parse_error={e}"
        status = "parse_failed"
        print(f"  ERROR parsing {job.job_id}: {e}")

    append_manifest_row(
        manifest_path,
        _make_row(job, status=status, started_at=started_at, finished_at=finished_at, notes=notes),
    )
