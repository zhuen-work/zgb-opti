"""Full unattended run: all 5 window studies + baseline + comparison.

Steps (12 total):
  For each of 5 windows (2w, 4w, 6w, 8w, 10w):
    Step A: run-full-pipeline  (optimize 5 jobs + parse + research dataset)
    Step B: run-forward-tests  (5 x 2-week forward backtests)
  Step 11: baseline            (static portfolio_nofilter_best.set, 12-week backtest)
  Step 12: compare_windows     (merge results, rank, output comparison table)

All output is streamed to console and tee'd to run_window_study_all.log.
On failure the script stops immediately and prints the failing step.

Usage:
    python run_window_study_all.py
"""
from __future__ import annotations

import io
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

# Force UTF-8 output so MT5's unicode characters don't crash on Windows cp1252
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

# ---------------------------------------------------------------------------
# Study definitions
# ---------------------------------------------------------------------------
STUDIES = [
    ("2w",  "configs/study_window_2w.yaml"),
    ("4w",  "configs/study_window_4w.yaml"),
    ("6w",  "configs/study_window_6w.yaml"),
    ("8w",  "configs/study_window_8w.yaml"),
    ("10w", "configs/study_window_10w.yaml"),
]

# Steps: 2 per study (pipeline + forward) + baseline + compare
TOTAL_STEPS = len(STUDIES) * 2 + 2
BASE_INI    = "configs/sets/study_window_params.set"
LOG_FILE    = Path("run_window_study_all.log")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
_log_fh        = None
_step_times: list[float] = []   # elapsed seconds for each completed step
_step_num      = 0


def _open_log() -> None:
    global _log_fh
    _log_fh = LOG_FILE.open("a", encoding="utf-8")


def _log(msg: str, to_console: bool = True) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    if to_console:
        print(line, flush=True)
    if _log_fh:
        _log_fh.write(line + "\n")
        _log_fh.flush()


def _close_log() -> None:
    if _log_fh:
        _log_fh.close()


def _eta_str(remaining_steps: int) -> str:
    if not _step_times:
        return "--:--"
    avg = sum(_step_times) / len(_step_times)
    eta = datetime.now() + timedelta(seconds=avg * remaining_steps)
    return eta.strftime("%H:%M")


def _pct() -> str:
    return f"{100 * _step_num / TOTAL_STEPS:.0f}%"


def _progress_banner(step_label: str) -> None:
    remaining = TOTAL_STEPS - _step_num
    eta = _eta_str(remaining)
    bar_filled = int(30 * _step_num / TOTAL_STEPS)
    bar = "#" * bar_filled + "-" * (30 - bar_filled)
    _log(f"\n[{bar}] {_pct()}  Step {_step_num}/{TOTAL_STEPS}  |  {step_label}  |  ETA finish: {eta}")


# ---------------------------------------------------------------------------
# Subprocess runner
# ---------------------------------------------------------------------------

def run_step(cmd: list[str], step_label: str) -> None:
    global _step_num
    _step_num += 1
    _progress_banner(f"STARTING  {step_label}")
    t0 = time.time()

    _log(f"  cmd: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        stripped = line.rstrip()
        print(f"      {stripped}", flush=True)
        if _log_fh:
            _log_fh.write(f"      {stripped}\n")
            _log_fh.flush()
    proc.wait()

    elapsed = time.time() - t0
    _step_times.append(elapsed)

    if proc.returncode != 0:
        _log(f"\n  !! FAILED: {step_label}  (exit={proc.returncode}, {elapsed:.0f}s)")
        raise RuntimeError(f"Step failed: {step_label}")

    remaining = TOTAL_STEPS - _step_num
    eta = _eta_str(remaining)
    _log(f"  DONE: {step_label}  ({elapsed/60:.1f} min)  |  {_step_num}/{TOTAL_STEPS} steps complete  |  ETA finish: {eta}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    _open_log()

    header = [
        "=" * 70,
        f"  ZGB Window Study  —  started {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"  Studies : {', '.join(s for s, _ in STUDIES)}",
        f"  Total steps: {TOTAL_STEPS}  (10 pipeline + 10 forward + 1 baseline + 1 compare)",
        f"  Log: {LOG_FILE}",
        "=" * 70,
    ]
    for line in header:
        _log(line)

    try:
        # ------------------------------------------------------------------ #
        # Window studies                                                       #
        # ------------------------------------------------------------------ #
        for label, config in STUDIES:
            research_dir = f"output/window_study_{label}/research"
            candidates   = f"{research_dir}/selected_candidates.parquet"

            # Step A — optimize + parse + research dataset
            run_step(
                [sys.executable, "-m", "zgb_opti", "run-full-pipeline",
                 "--config",       config,
                 "--base-ini",     BASE_INI,
                 "--research-dir", research_dir],
                f"{label} | optimize + parse + research dataset",
            )

            # Step B — 2-week forward tests
            run_step(
                [sys.executable, "-m", "zgb_opti", "run-forward-tests",
                 "--config",       config,
                 "--candidates",   candidates,
                 "--research-dir", research_dir],
                f"{label} | 2-week forward tests",
            )

        # ------------------------------------------------------------------ #
        # Baseline                                                             #
        # ------------------------------------------------------------------ #
        run_step(
            [sys.executable, "run_baseline.py"],
            "baseline | portfolio_nofilter_best.set  12-week single backtest",
        )

        # ------------------------------------------------------------------ #
        # Comparison                                                           #
        # ------------------------------------------------------------------ #
        run_step(
            [sys.executable, "compare_windows.py"],
            "compare windows + baseline",
        )

        # ------------------------------------------------------------------ #
        # Summary                                                              #
        # ------------------------------------------------------------------ #
        total_elapsed = sum(_step_times)
        _log("\n" + "=" * 70)
        _log(f"  ALL DONE  —  total time: {total_elapsed/3600:.1f} hours")
        _log(f"  Results : output/window_comparison/comparison_summary.txt")
        _log(f"  Equity  : output/window_comparison/equity_curves.csv")
        _log(f"  Log     : {LOG_FILE}")
        _log("=" * 70)

    except RuntimeError as e:
        _log(f"\n  ABORTED at step {_step_num}/{TOTAL_STEPS}: {e}")
        _log(f"  Completed steps: {_step_num - 1}")
        _log(f"  See log for details: {LOG_FILE}")
        _close_log()
        sys.exit(1)

    except Exception:
        _log(f"\n  UNEXPECTED ERROR at step {_step_num}:\n{traceback.format_exc()}")
        _close_log()
        sys.exit(1)

    _close_log()


if __name__ == "__main__":
    main()
