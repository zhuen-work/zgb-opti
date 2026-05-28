"""Heartbeat helper - emits a markdown-table status with full ETA breakdown.

Per user 2026-05-16: format must show the ETA derivation step-by-step so the
estimate is falsifiable (not just a black-box number).

Usage from a Monitor loop:
  while sleep 900; do python heartbeat_wfo.py <log> <out_dir> <start_epoch> [interval_min]; done
"""
import os, re, sys, glob
from datetime import datetime, timedelta

LOG = sys.argv[1] if len(sys.argv) > 1 else ""
OUT_DIR = sys.argv[2] if len(sys.argv) > 2 else ""
START_S = float(sys.argv[3]) if len(sys.argv) > 3 else 0.0
INTERVAL_MIN = int(sys.argv[4]) if len(sys.argv) > 4 else 15

now = datetime.now()
elapsed_min = int((now.timestamp() - START_S) / 60) if START_S else 0
hh = now.strftime("%H:%M")
nxt = (now + timedelta(minutes=INTERVAL_MIN)).strftime("%H:%M")

def _read_log(p):
    if not p:
        return ""
    try:
        with open(p, "rb") as f:
            raw = f.read()
    except FileNotFoundError:
        return ""
    # PowerShell Tee-Object default = UTF-16 LE with BOM.
    if raw.startswith(b"\xff\xfe"):
        return raw.decode("utf-16-le", errors="replace")
    if raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16-be", errors="replace")
    return raw.decode("utf-8", errors="replace")

log = _read_log(LOG)

# Phase from "=== PHASE X ===" markers
phases = re.findall(r"^=== (PHASE [A-D][^=]*) ===", log, re.M)
phase = phases[-1].strip() if phases else "Pre-warm / IS sweep starting"

# Parquet counts
n_is = len(glob.glob(f"{OUT_DIR}/p1_is_W*.parquet")) if OUT_DIR else 0
n_oos = len(glob.glob(f"{OUT_DIR}/p1_oos_W*.parquet")) if OUT_DIR else 0
n_parquet = n_is + n_oos

# Workers + RAM (psutil -> PowerShell fallback)
workers, ram_str = "?", "?"
try:
    import psutil
    py = [p for p in psutil.process_iter(["name", "memory_info"])
          if p.info["name"] and "python" in p.info["name"].lower()]
    workers = len(py)
    ram_str = f"{sum(p.info['memory_info'].rss for p in py)/1024**3:.1f} GB"
except Exception:
    try:
        import subprocess
        ps = ("$p=Get-Process python -EA SilentlyContinue;"
              "if($p){'{0}|{1:N1}' -f $p.Count,(($p|Measure WorkingSet64 -Sum).Sum/1GB)}else{'0|0'}")
        out = subprocess.run(["powershell", "-NoProfile", "-Command", ps],
                              capture_output=True, text=True, timeout=10).stdout.strip()
        if "|" in out:
            w, r = out.split("|", 1); workers = int(w); ram_str = f"{float(r)} GB"
    except Exception:
        pass

# Latest sub-sweep progress: "[done/total]  rate cfg/s  elapsed=Xs  eta=Ys"
m = list(re.finditer(r"\[(\d+)/(\d+)\]\s+(\d+\.\d+)\s*cfg/s.*?eta=(\d+)s", log))
N_IS_WINDOWS = 4

if m:
    done, total, rate, cur_eta_s = (int(m[-1][1]), int(m[-1][2]),
                                     float(m[-1][3]), int(m[-1][4]))
    cur_window_n = n_is + 1                     # 1-indexed; current is the in-progress one
    sub_sweep_min = cur_eta_s // 60
    per_win_min = int(total / max(rate, 0.01)) // 60
    windows_after_current = max(0, N_IS_WINDOWS - cur_window_n)
    phase_a_total_hours = (N_IS_WINDOWS * per_win_min) / 60.0
    phase_b_d_overhead_min = 5
    phase_c_oos_min = 5
    total_remaining_min = (sub_sweep_min + windows_after_current * per_win_min
                            + phase_c_oos_min + phase_b_d_overhead_min)
    total_eta = (now + timedelta(minutes=total_remaining_min)).strftime("%H:%M")
    hours_from_now = total_remaining_min / 60.0
    eta_label = f"{total_eta} (~{hours_from_now:.1f}h from now)"
    phase_a_label = (f"{N_IS_WINDOWS} x {per_win_min} min = ~{phase_a_total_hours:.1f}h total "
                     f"(W{cur_window_n} in progress, {windows_after_current} after)")
else:
    # Fallback: parse window-level progress lines from reverse-hedge / single-thread WFOs
    # Lines look like:  "  W1 IS 2026-02-28->2026-03-28 done streams=6  [992s]"
    # Each line = one fold+label done; total windows = 4 IS + 4 OOS = 8.
    win_re = re.compile(r"\s*(W\d+)\s+(IS|OOS)\s+\d{4}-\d{2}-\d{2}->\d{4}-\d{2}-\d{2}\s+done streams=\d+\s+\[(\d+)s\]")
    wins_done = list(win_re.finditer(log))
    N_TOTAL_WINS = N_IS_WINDOWS * 2  # IS + OOS
    if wins_done:
        n_wins = len(wins_done)
        latest_elapsed_s = int(wins_done[-1].group(3))
        avg_per_win_s = latest_elapsed_s / n_wins
        wins_left = max(0, N_TOTAL_WINS - n_wins)
        sub_sweep_min = 0
        per_win_min = int(avg_per_win_s // 60)
        done = n_wins; total = N_TOTAL_WINS
        cur_window_n = min(n_wins + 1, N_IS_WINDOWS)
        rate = 0.0  # no cfg/s granularity in this script
        windows_after_current = wins_left
        # Phase C/B/D inline in the per-window timing — only final aggregation remains
        post_sweep_overhead_min = 5
        total_remaining_min = int(wins_left * avg_per_win_s / 60) + post_sweep_overhead_min
        total_eta = (now + timedelta(minutes=total_remaining_min)).strftime("%H:%M")
        hours_from_now = total_remaining_min / 60.0
        eta_label = f"{total_eta} (~{hours_from_now:.1f}h from now)"
        phase_a_label = (f"{n_wins}/{N_TOTAL_WINS} windows done, avg ~{per_win_min} min/window "
                         f"({wins_left} remaining)")
    else:
        done = total = 0; rate = 0.0; cur_eta_s = 0
        sub_sweep_min = per_win_min = 0
        windows_after_current = N_IS_WINDOWS - 1
        eta_label = "? (no progress data yet)"
        cur_window_n = 1
        phase_a_label = "starting"

# Emit markdown table
print(f"| **Heartbeat - {hh} (+{elapsed_min}m elapsed)** |  |")
print("|---|---|")
print(f"| Phase | {phase} |")
print(f"| Parquets / Workers / RAM | {n_parquet}/8 / {workers} / {ram_str} |")
print(f"| Throughput | {rate:.2f} cfg/s |")
print(f"| IS-W{cur_window_n} sub-sweep remaining | {sub_sweep_min} min ({done}/{total}) |")
print(f"| Per-window sub-sweep | {total} cells / {rate:.2f} cfg/s = ~{per_win_min} min/window |")
print(f"| Phase A IS ({N_IS_WINDOWS} windows) | {phase_a_label} |")
print(f"| Phase C OOS (survivors ~30 cells) | <5 min |")
print(f"| Phase B/D + overhead | ~5 min |")
print(f"| **Total wall-clock ETA** | **{eta_label}** |")
print(f"| Next heartbeat | {nxt} |")
