"""Sequential batch runner for 6 v2-fractal WFOs: (may9, may16) × (spread30, spread60, spread120).
Sets the ZGB_SPREAD_PTS_OVERRIDE env var per run and invokes the parametric WFO script.
Prints the same [BOOT]/[WIN]/[OOS]/[RANK]/[DONE] markers as a single WFO so the
heartbeat_wfo.py parser keeps working.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

JOBS = [
    ("may9",  "30"),
    ("may9",  "60"),
    ("may9",  "120"),
    ("may16", "30"),
    ("may16", "60"),
    ("may16", "120"),
]

env_base = os.environ.copy()
env_base["PYTHONPATH"] = str(ROOT / "src")
env_base["PYTHONIOENCODING"] = "utf-8"

t_overall = time.time()
for i, (wkey, spread) in enumerate(JOBS, 1):
    print(f"\n############ JOB {i}/{len(JOBS)}: {wkey} spread={spread} ############", flush=True)
    env = dict(env_base)
    env["ZGB_SPREAD_PTS_OVERRIDE"] = spread
    t0 = time.time()
    rc = subprocess.call(
        [sys.executable, str(ROOT / "scripts" / "sim_wfo_orb_v2_fractal_parametric.py"),
         "--window-key", wkey],
        env=env,
        cwd=str(ROOT),
    )
    dt = time.time() - t0
    if rc != 0:
        print(f"[JOB-FAIL] {wkey} spread={spread} exit={rc} after {dt:.0f}s", flush=True)
    else:
        print(f"[JOB-OK] {wkey} spread={spread} done in {dt:.0f}s", flush=True)

total = time.time() - t_overall
print(f"\n[BATCH-DONE] {len(JOBS)} jobs in {total/60:.1f} min", flush=True)
