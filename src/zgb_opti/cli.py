import argparse
import sys
from collections import Counter

from zgb_opti.ini_writer import generate_ini_files_for_jobs
from zgb_opti.job_builder import build_optimization_jobs, write_jobs_to_json
from zgb_opti.launcher import run_all_jobs, run_single_job
from zgb_opti.study_config import load_study_config


def cmd_show_config(args: argparse.Namespace) -> None:
    try:
        cfg = load_study_config(args.config)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    fields = [
        ("study_name", cfg.study_name),
        ("symbol", cfg.symbol),
        ("timeframe", cfg.timeframe),
        ("ea_name", cfg.ea_name),
        ("ea_path", cfg.ea_path),
        ("base_set_path", cfg.base_set_path),
        ("mt5_terminal_path", cfg.mt5_terminal_path),
        ("output_root", cfg.output_root),
        ("windows_weeks", cfg.windows_weeks),
        ("study_start", cfg.study_start),
        ("study_end", cfg.study_end),
        ("step_weeks", cfg.step_weeks),
    ]
    for name, value in fields:
        print(f"{name}: {value}")


def cmd_build_jobs(args: argparse.Namespace) -> None:
    try:
        cfg = load_study_config(args.config)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    jobs = build_optimization_jobs(cfg)
    output_dir = "jobs/generated"
    write_jobs_to_json(jobs, output_dir)

    counts = Counter(j.window_weeks for j in jobs)
    print(f"Total jobs: {len(jobs)}")
    for w in sorted(counts):
        print(f"  {w}w: {counts[w]} jobs")
    print("First 3 job IDs:")
    for j in jobs[:3]:
        print(f"  {j.job_id}")
    print(f"Output directory: {output_dir}")


def cmd_write_ini(args: argparse.Namespace) -> None:
    try:
        cfg = load_study_config(args.config)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    jobs = build_optimization_jobs(cfg)
    try:
        written = generate_ini_files_for_jobs(jobs, cfg, args.base_ini)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"INI files written: {len(written)}")
    print("First 3 ini paths:")
    for p in written[:3]:
        print(f"  {p}")
    print(f"Base ini used: {args.base_ini}")


def cmd_run_jobs(args: argparse.Namespace) -> None:
    try:
        cfg = load_study_config(args.config)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    jobs = build_optimization_jobs(cfg)
    print(f"Total jobs: {len(jobs)}")
    run_all_jobs(cfg, jobs)
    print("Manifest written to: data/manifests/job_runs.jsonl")


def cmd_run_first_job(args: argparse.Namespace) -> None:
    try:
        cfg = load_study_config(args.config)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    jobs = build_optimization_jobs(cfg)
    print(f"Total jobs: {len(jobs)}")
    if not jobs:
        print("No jobs generated. Check study config date range and window settings.")
        sys.exit(1)

    job = jobs[0]
    print(f"Selected first job: {job.job_id}")
    run_single_job(cfg, job)
    print(f"Manifest written to: data/manifests/job_runs.jsonl")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="zgb_opti")
    subparsers = parser.add_subparsers(dest="command")

    show = subparsers.add_parser("show-config", help="Load and print the study config")
    show.add_argument("--config", required=True, metavar="PATH", help="Path to study YAML config")

    build = subparsers.add_parser("build-jobs", help="Generate rolling optimization job specs")
    build.add_argument("--config", required=True, metavar="PATH", help="Path to study YAML config")

    write_ini = subparsers.add_parser("write-ini", help="Generate MT5 tester ini files for all jobs")
    write_ini.add_argument("--config", required=True, metavar="PATH", help="Path to study YAML config")
    write_ini.add_argument("--base-ini", required=True, metavar="PATH", help="Path to base tester ini template")

    run = subparsers.add_parser("run-jobs", help="Launch MT5 optimization jobs sequentially")
    run.add_argument("--config", required=True, metavar="PATH", help="Path to study YAML config")

    first = subparsers.add_parser("run-first-job", help="Smoke-test: generate jobs and run only the first")
    first.add_argument("--config", required=True, metavar="PATH", help="Path to study YAML config")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "show-config":
        cmd_show_config(args)
    elif args.command == "build-jobs":
        cmd_build_jobs(args)
    elif args.command == "write-ini":
        cmd_write_ini(args)
    elif args.command == "run-jobs":
        cmd_run_jobs(args)
    elif args.command == "run-first-job":
        cmd_run_first_job(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
