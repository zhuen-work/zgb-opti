import json
from datetime import timedelta
from pathlib import Path

from zgb_opti.models import OptimizationJob, StudyConfig


def build_optimization_jobs(config: StudyConfig) -> list[OptimizationJob]:
    jobs: list[OptimizationJob] = []

    for window_weeks in sorted(config.windows_weeks):
        window_days = window_weeks * 7
        step_days = config.step_weeks * 7
        train_start = config.study_start

        while True:
            train_end = train_start + timedelta(days=window_days - 1)
            if train_end > config.study_end:
                break

            start_iso = train_start.isoformat()
            end_iso = train_end.isoformat()
            job_id = f"opt_{window_weeks}w_{start_iso}_{end_iso}"

            jobs.append(
                OptimizationJob(
                    job_id=job_id,
                    window_weeks=window_weeks,
                    train_start=train_start,
                    train_end=train_end,
                    symbol=config.symbol,
                    timeframe=config.timeframe,
                    ea_name=config.ea_name,
                    ini_path=f"jobs/generated/{config.study_name}_{job_id}.ini",
                    output_dir=f"{config.output_root}/{job_id}",
                    status="pending",
                )
            )

            train_start = train_start + timedelta(days=step_days)

    return jobs


def write_jobs_to_json(jobs: list[OptimizationJob], output_dir: str | Path) -> list[Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    for job in jobs:
        data = job.model_dump(mode="json")
        path = out / f"{job.job_id}.json"
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        written.append(path)

    return written
