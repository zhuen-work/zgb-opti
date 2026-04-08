import argparse
import sys
from collections import Counter

from zgb_opti.analysis import run_window_analysis
from zgb_opti.recommender import run_deployment_recommender
from zgb_opti.robustness import run_robustness_analysis
from zgb_opti.ini_writer import build_ini_content, generate_ini_files_for_jobs, write_ini_file
from zgb_opti.job_builder import build_optimization_jobs, write_jobs_to_json
from zgb_opti.launcher import parse_job_report, run_all_jobs, run_single_job
from zgb_opti.research import build_forward_dataset, build_research_dataset
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
    ini_content = build_ini_content("", job, cfg)
    ini_path = write_ini_file(job, ini_content)
    print(f"INI written: {ini_path}")
    run_single_job(cfg, job)
    parse_job_report(job)
    print(f"Manifest written to: data/manifests/job_runs.jsonl")


def cmd_parse_report(args: argparse.Namespace) -> None:
    try:
        cfg = load_study_config(args.config)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    jobs = build_optimization_jobs(cfg)
    if not jobs:
        print("No jobs generated.")
        sys.exit(1)

    if args.job_id:
        target = next((j for j in jobs if j.job_id == args.job_id), None)
        if target is None:
            print(f"Error: job_id '{args.job_id}' not found", file=sys.stderr)
            sys.exit(1)
        print(f"Parsing job: {target.job_id}")
        parse_job_report(target)
    else:
        parsed = 0
        for job in jobs:
            from pathlib import Path
            if any((Path(job.output_dir) / f"{job.job_id}{ext}").exists() for ext in (".xml", ".htm", ".html")):
                print(f"Parsing job: {job.job_id}")
                parse_job_report(job)
                parsed += 1
        print(f"Parsed {parsed}/{len(jobs)} jobs")

    print("Manifest written to: data/manifests/job_runs.jsonl")


def cmd_run_full_pipeline(args: argparse.Namespace) -> None:
    """Write inis → run all missing jobs (with parse) → build research dataset."""
    try:
        cfg = load_study_config(args.config)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    from pathlib import Path
    jobs = build_optimization_jobs(cfg)
    print(f"=== Step 1/3: Generate INI files ({len(jobs)} jobs) ===")
    from zgb_opti.ini_writer import generate_ini_files_for_jobs
    try:
        written = generate_ini_files_for_jobs(jobs, cfg, args.base_ini)
        print(f"INI files written: {len(written)}")
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    missing = [j for j in jobs if not (Path(j.output_dir) / f"{j.job_id}.xml").exists()]
    print(f"\n=== Step 2/3: Run MT5 jobs ({len(missing)} missing, {len(jobs)-len(missing)} skip) ===")
    run_all_jobs(cfg, jobs, skip_existing=True)

    print(f"\n=== Step 3/3: Build research dataset ===")
    summary = build_research_dataset(jobs, args.research_dir)
    print(f"\n--- Phase 4 dataset ready ---")
    print(f"Jobs:       {summary['jobs_included']}/{summary['jobs_total']}")
    print(f"Passes:     {summary['total_passes']}")
    print(f"Candidates: {summary['candidates']}")


def cmd_run_forward_tests(args: argparse.Namespace) -> None:
    """Phase 5: run forward tests for all selected candidates, parse results, merge."""
    try:
        cfg = load_study_config(args.config)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    from pathlib import Path
    candidates_path = Path(args.candidates)
    if not candidates_path.exists():
        print(f"Error: candidates file not found: {candidates_path}", file=sys.stderr)
        sys.exit(1)

    summary = build_forward_dataset(
        candidates_parquet=candidates_path,
        config=cfg,
        research_dir=args.research_dir,
        skip_existing=not args.rerun,
    )

    print("\n--- Phase 5 Forward Testing Complete ---")
    print(f"Forward tests executed:  {summary['executed']}")
    print(f"Forward tests skipped:   {summary['skipped']}")
    print(f"Forward tests failed:    {summary['failed']}")
    print(f"Reports parsed:          {summary['parsed']}")
    print(f"forward_results rows:    {summary['parsed']}")
    print(f"final_study_dataset rows:{summary['final_study_rows']}")
    if summary.get("outputs"):
        for k, v in summary["outputs"].items():
            print(f"  {k}: {v}")


def cmd_analyze_windows(args: argparse.Namespace) -> None:
    from pathlib import Path
    dataset_path = Path(args.research_dir) / "final_study_dataset.parquet"
    try:
        result = run_window_analysis(dataset_path, args.research_dir)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\nRows analyzed:   {result['n_rows']}")
    print(f"Windows compared: {len(result['windows'])}")
    print(f"Best window:      {result['top_window_weeks']}w")
    print("\nRanking:")
    for r in result["ranking"]:
        print(
            f"  #{int(r['rank'])}  {int(r['window_weeks']):>2}w  "
            f"score={r['composite_score']:.3f}  "
            f"median_np={r['median_forward_net_profit']:>10.2f}  "
            f"win%={r['profitable_forward_rate']:.0%}  "
            f"med_pf={r['median_forward_profit_factor']:.2f}  "
            f"med_dd={r['median_forward_drawdown_pct']:.1f}%"
        )
    print("\nOutputs written:")
    for k, v in result["outputs"].items():
        print(f"  {k}: {v}")


def cmd_recommend_deployment(args: argparse.Namespace) -> None:
    try:
        result = run_deployment_recommender(args.research_dir)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    rec = result["record"]
    flags_str = ", ".join(rec["caution_flags"].split("|")) if rec["caution_flags"] else "none"
    print(f"\nWindows evaluated  : {result['windows_evaluated']}")
    print(f"Recommended window : {rec['recommended_window']}w")
    print(f"Selection rule     : {rec['recommended_selection_rule']}")
    print(f"Deployment action  : {rec['deployment_action']}")
    print(f"Confidence         : {rec['confidence']}")
    print(f"Caution flags      : {flags_str}")
    print(f"Rationale          : {rec['rationale']}")
    print("\nOutputs written:")
    for k, v in result["outputs"].items():
        print(f"  {k}: {v}")


def cmd_analyze_robustness(args: argparse.Namespace) -> None:
    try:
        result = run_robustness_analysis(args.research_dir)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(result["summary_text"])
    print("Outputs written:")
    for k, v in result["outputs"].items():
        print(f"  {k}: {v}")


def cmd_build_research_dataset(args: argparse.Namespace) -> None:
    try:
        cfg = load_study_config(args.config)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    jobs = build_optimization_jobs(cfg)
    summary = build_research_dataset(jobs, args.research_dir)
    print(f"Done: {summary['jobs_included']}/{summary['jobs_total']} jobs included, "
          f"{summary['total_passes']} passes, {summary['candidates']} candidates")


def cmd_portfolio_recommend(args: argparse.Namespace) -> None:
    """Portfolio allocation recommender: filter passes to sum=10 allocs, rank, emit JSON+TXT."""
    import json
    from datetime import datetime, timezone
    from pathlib import Path

    from zgb_opti import portfolio
    from zgb_opti.research import aggregate_passes, select_candidates

    try:
        cfg = load_study_config(args.config)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    jobs = build_optimization_jobs(cfg)

    # Optionally filter to only 12-week windows if multiple are configured
    if args.window_weeks is not None:
        jobs = [j for j in jobs if j.window_weeks == args.window_weeks]
        print(f"Filtered to {args.window_weeks}w jobs: {len(jobs)}")

    print(f"Loading passes from {len(jobs)} jobs...")
    all_passes, skipped = aggregate_passes(jobs)
    if all_passes.empty:
        print("Error: no passes found. Run MT5 optimization first.", file=sys.stderr)
        sys.exit(1)
    print(f"  Total passes loaded: {len(all_passes)}")
    if skipped:
        print(f"  Skipped (no passes.parquet): {len(skipped)} jobs")

    print(f"\nSelecting best candidate per job (viability filter + rank by result)...")
    candidates = select_candidates(all_passes)

    if candidates.empty:
        print("No portfolio candidates found after filtering.", file=sys.stderr)
        sys.exit(1)

    print(f"  Candidates selected: {len(candidates)}")

    # Rank by result descending and show top candidates
    ranked = candidates.sort_values("result", ascending=False).reset_index(drop=True)
    top_n = min(args.top, len(ranked))
    print(f"\n=== Top {top_n} Portfolio Candidates (by result) ===")
    for i, (_, row) in enumerate(ranked.head(top_n).iterrows(), 1):
        alloc = portfolio.extract_allocation(row)
        alloc_str = portfolio.format_allocation_display(alloc)
        active = [k for k, v in alloc.items() if v > 0]
        print(
            f"  #{i:>2}  job={row.get('job_id', '?')}"
            f"  result={row.get('result', float('nan')):.4f}"
            f"  net_profit={row.get('net_profit', float('nan')):.2f}"
            f"  pf={row.get('profit_factor', float('nan')):.2f}"
            f"  dd={row.get('drawdown_pct', float('nan')):.1f}%"
            f"  alloc={alloc_str}"
            f"  active={active}"
        )

    # Best overall candidate
    best = ranked.iloc[0]
    best_alloc = portfolio.extract_allocation(best)
    best_active = [int(k[1:]) for k, v in sorted(best_alloc.items()) if v > 0]
    best_total_risk = sum(best_alloc.values())

    # Emit outputs
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build param dict for best candidate
    best_params: dict = {}
    for col in best.index:
        if col.startswith("param_"):
            import pandas as pd
            val = best[col]
            if pd.notna(val):
                name = col[len("param_"):]
                try:
                    f = float(val)
                    best_params[name] = int(f) if f == int(f) else f
                except (TypeError, ValueError):
                    best_params[name] = str(val)

    record = {
        "as_of": str(datetime.now(timezone.utc).date()),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "study_name": cfg.study_name,
        "symbol": cfg.symbol,
        "timeframe": cfg.timeframe,
        "selection_method": "portfolio_result_desc_v1",
        "candidates_evaluated": len(candidates),
        "best_job_id": str(best.get("job_id", "")),
        "best_result": float(best.get("result", float("nan"))),
        "best_net_profit": float(best.get("net_profit", float("nan"))),
        "best_profit_factor": float(best.get("profit_factor", float("nan"))),
        "best_drawdown_pct": float(best.get("drawdown_pct", float("nan"))),
        "best_trades": int(best.get("trades", 0)),
        "risk_split": best_alloc,
        "active_strategies": best_active,
        "total_risk_pct": float(best_total_risk),
        "params": best_params,
        "confidence_note": (
            "Portfolio candidate selected by highest MT5 optimization result score. "
            "No robustness or forward-test validation applied. "
            "Review active strategies and risk split before live deployment."
        ),
    }

    # Write JSON
    json_path = output_dir / "final_portfolio_recommendation.json"
    json_path.write_text(json.dumps(record, indent=2), encoding="utf-8")

    # Write TXT
    alloc_str = portfolio.format_allocation_display(best_alloc)
    active_str = ", ".join(f"S{s}" for s in best_active) if best_active else "none"
    txt_lines = [
        "Portfolio Deployment Recommendation",
        "=" * 48,
        f"As of              : {record['as_of']}",
        f"Study              : {cfg.study_name}",
        f"Symbol / TF        : {cfg.symbol} {cfg.timeframe}",
        f"Candidates eval'd  : {len(candidates)}",
        f"Selection method   : portfolio_result_desc_v1",
        "",
        "Best Candidate:",
        f"  Job ID           : {record['best_job_id']}",
        f"  Result score     : {record['best_result']:.4f}",
        f"  Net profit       : {record['best_net_profit']:.2f}",
        f"  Profit factor    : {record['best_profit_factor']:.2f}",
        f"  Drawdown         : {record['best_drawdown_pct']:.1f}%",
        f"  Trades           : {record['best_trades']}",
        "",
        "Portfolio Allocation:",
        f"  Risk split       : {alloc_str}",
        f"  Active strategies: {active_str}",
        "",
        "Confidence Note:",
        f"  {record['confidence_note']}",
        "",
        f"Top {top_n} candidates by result:",
    ]
    for i, (_, row) in enumerate(ranked.head(top_n).iterrows(), 1):
        alloc = portfolio.extract_allocation(row)
        txt_lines.append(
            f"  #{i:>2}  job={row.get('job_id', '?')}"
            f"  result={row.get('result', float('nan')):.4f}"
            f"  np={row.get('net_profit', float('nan')):.2f}"
            f"  alloc={portfolio.format_allocation_display(alloc)}"
        )

    txt_path = output_dir / "final_portfolio_recommendation.txt"
    txt_path.write_text("\n".join(txt_lines) + "\n", encoding="utf-8")

    print(f"\nOutputs written:")
    print(f"  json: {json_path}")
    print(f"  txt:  {txt_path}")


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

    parse = subparsers.add_parser("parse-report", help="Parse copied XML report(s) into passes.parquet + passes.csv")
    parse.add_argument("--config", required=True, metavar="PATH", help="Path to study YAML config")
    parse.add_argument("--job-id", metavar="JOB_ID", default=None, help="Parse a specific job (default: all jobs with a copied XML)")

    research = subparsers.add_parser("build-research-dataset", help="Aggregate parsed jobs, select candidates, write research outputs")
    research.add_argument("--config", required=True, metavar="PATH", help="Path to study YAML config")
    research.add_argument("--research-dir", metavar="PATH", default="output/research", help="Output directory for research files (default: output/research)")

    pipeline = subparsers.add_parser("run-full-pipeline", help="Write inis, run all missing MT5 jobs, parse, build research dataset")
    pipeline.add_argument("--config", required=True, metavar="PATH", help="Path to study YAML config")
    pipeline.add_argument("--base-ini", required=True, metavar="PATH", help="Path to base tester ini template")
    pipeline.add_argument("--research-dir", metavar="PATH", default="output/research", help="Output directory for research files (default: output/research)")

    analyze = subparsers.add_parser("analyze-windows", help="Phase 6: rank optimization window lengths by OOS forward performance")
    analyze.add_argument("--config", required=True, metavar="PATH", help="Path to study YAML config")
    analyze.add_argument("--research-dir", metavar="PATH", default="output/research", help="Research directory containing final_study_dataset.parquet (default: output/research)")

    recommend = subparsers.add_parser("recommend-deployment", help="Phase 2C: weekly deployment recommender — convert research to deployment decision")
    recommend.add_argument("--config", required=True, metavar="PATH", help="Path to study YAML config")
    recommend.add_argument("--research-dir", metavar="PATH", default="output/research", help="Research directory (default: output/research)")

    robustness = subparsers.add_parser("analyze-robustness", help="Phase 2B: parameter robustness study — plateau vs spike diagnostics")
    robustness.add_argument("--config", required=True, metavar="PATH", help="Path to study YAML config")
    robustness.add_argument("--research-dir", metavar="PATH", default="output/research", help="Research directory containing parquet files (default: output/research)")

    forward = subparsers.add_parser("run-forward-tests", help="Phase 5: run 1-week forward backtests for all selected candidates")
    forward.add_argument("--config", required=True, metavar="PATH", help="Path to study YAML config")
    forward.add_argument("--candidates", metavar="PATH", default="output/research/selected_candidates.parquet", help="Path to selected_candidates.parquet (default: output/research/selected_candidates.parquet)")
    forward.add_argument("--research-dir", metavar="PATH", default="output/research", help="Output directory for research files (default: output/research)")
    forward.add_argument("--rerun", action="store_true", help="Re-run even if forward report already exists")

    portfolio_rec = subparsers.add_parser(
        "portfolio-recommend",
        help="Portfolio allocation recommender: filter passes to constrained allocations, rank, emit recommendation",
    )
    portfolio_rec.add_argument("--config", required=True, metavar="PATH", help="Path to study YAML config")
    portfolio_rec.add_argument("--output-dir", metavar="PATH", default="output/portfolio", help="Output directory for recommendation files (default: output/portfolio)")
    portfolio_rec.add_argument("--alloc-total", type=int, default=10, metavar="N", help="Required sum of risk allocations (default: 10)")
    portfolio_rec.add_argument("--window-weeks", type=int, default=None, metavar="W", help="Filter to this window size only (default: all windows in config)")
    portfolio_rec.add_argument("--top", type=int, default=10, metavar="N", help="Number of top candidates to display (default: 10)")

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
    elif args.command == "parse-report":
        cmd_parse_report(args)
    elif args.command == "build-research-dataset":
        cmd_build_research_dataset(args)
    elif args.command == "run-full-pipeline":
        cmd_run_full_pipeline(args)
    elif args.command == "analyze-windows":
        cmd_analyze_windows(args)
    elif args.command == "recommend-deployment":
        cmd_recommend_deployment(args)
    elif args.command == "analyze-robustness":
        cmd_analyze_robustness(args)
    elif args.command == "run-forward-tests":
        cmd_run_forward_tests(args)
    elif args.command == "portfolio-recommend":
        cmd_portfolio_recommend(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
