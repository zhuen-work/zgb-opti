"""Compare FBO Stream WFO results across different timeframes.

Reads winner setfiles + sanity log lines for S1 (M30), S2-H4, S2-M15, and S3 (H1)
and produces a comparison table.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _grep_log_summary(log_path: Path) -> dict:
    """Extract Phase D winner row + sanity ET line from a WFO log."""
    if not log_path.exists():
        return {"missing": True}
    text = log_path.read_text(encoding="utf-8", errors="replace")

    # Sanity ET line
    m = re.search(r"Winner sanity ET:\s+(.+)$", text, re.MULTILINE)
    sanity = m.group(1).strip() if m else "MISSING"

    # Phase D rank 1
    m = re.search(
        r"Rank\s+Total OOS NP.*?\n\s+1\s+([+\-\$,\d.]+)\s+([+\-\d.]+)%\s+([\d.]+)%\s+([+\-\d]+)\s+(\d/\d)\s+(.+?)$",
        text, re.MULTILINE | re.DOTALL,
    )
    if m:
        rank1 = {
            "total_np": m.group(1),
            "roi": m.group(2) + "%",
            "avg_dd": m.group(3) + "%",
            "np_dd": m.group(4),
            "prof": m.group(5),
            "params": m.group(6).strip(),
        }
    else:
        rank1 = {"missing": True}

    # Profitable count per IS window
    is_counts = []
    for win in ("W1", "W2", "W3"):
        mw = re.search(rf"IS-{win}: top-5 by Recovery Factor \(of (\d+) profitable\)", text)
        is_counts.append(mw.group(1) if mw else "?")

    return {
        "sanity": sanity,
        "rank1": rank1,
        "is_profitable_counts": is_counts,
    }


STREAMS = [
    ("S1 (M30)", ROOT / "output" / "sim_wfo_fbo_s1_spread60.log"),
    ("S2 (H4)",  ROOT / "output" / "sim_wfo_fbo_s2_spread60.log"),
    ("S2-M15",   ROOT / "output" / "sim_wfo_fbo_s2_m15_spread60.log"),
    ("S3 (H1)",  ROOT / "output" / "sim_wfo_fbo_s3_spread60.log"),
]


def main() -> int:
    print("=" * 90)
    print("  FBO STREAM COMPARISON (spread=60, $10k, 3% risk, 4w IS / 2w OOS, ending Apr 25)")
    print("=" * 90)

    rows = []
    for name, log in STREAMS:
        info = _grep_log_summary(log)
        rows.append((name, info))

    # Summary table
    print(f"\n  {'Stream':<10}  {'IS prof. cnt (W1/W2/W3)':<28}  {'Rank-1 OOS NP':>14}  "
          f"{'AvgDD':>7}  {'NP/DD':>8}  {'Prof':>5}")
    print("  " + "-" * 88)
    for name, info in rows:
        if info.get("missing"):
            print(f"  {name:<10}  (log missing)")
            continue
        r = info.get("rank1", {})
        if r.get("missing"):
            print(f"  {name:<10}  (rank-1 not parsed)")
            continue
        is_cnt = "/".join(info["is_profitable_counts"])
        print(f"  {name:<10}  {is_cnt:<28}  {r['total_np']:>14}  "
              f"{r['avg_dd']:>7}  {r['np_dd']:>8}  {r['prof']:>5}")

    print()
    print("  Continuous Sanity ET (winner on Mar 14 -> Apr 25, $10k):")
    for name, info in rows:
        if info.get("missing") or "sanity" not in info:
            continue
        print(f"    {name:<10}  {info['sanity']}")

    print()
    print("  Rank-1 winner params:")
    for name, info in rows:
        if info.get("missing"):
            continue
        r = info.get("rank1", {})
        if r.get("missing"):
            continue
        print(f"    {name:<10}  {r['params']}")

    print()
    print("=" * 90)
    return 0


if __name__ == "__main__":
    sys.exit(main())
