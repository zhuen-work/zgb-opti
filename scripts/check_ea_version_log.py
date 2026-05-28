"""Sanity-check that the EA's OnInit Print() tag matches `#property version`.

Why: today (2026-05-25) we discovered the v6 EA's OnInit log still said
`[DT818_pro_v4]` because the developer (me) updated `#property version` but
forgot the Print tag. Symptoms: user can't tell what version is actually
loaded from the Experts log. Setfile/EA mismatches go unnoticed.

Rule:
  * Find `#property version "<N>.NN"` in the EA source.
  * Extract major version (e.g. "6.00" -> "v6").
  * Verify every `Print*("[DT818_pro_vX]"...` tag in the file matches major.
  * Exit non-zero on drift.

Wire into the compile workflow: run AFTER `metaeditor64.exe /compile:...`.
"""
from __future__ import annotations
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EA_PATH = ROOT / "ea" / "DT818_pro_v6.mq5"


def check(ea_path: Path) -> int:
    text = ea_path.read_text(encoding="utf-8", errors="replace")
    # Find #property version
    m = re.search(r'#property\s+version\s+"(\d+)\.(\d+)"', text)
    if not m:
        print(f"FAIL: no `#property version` line in {ea_path}")
        return 2
    major = m.group(1)
    expected_tag = f"DT818_pro_v{major}"
    print(f"  #property version = {m.group(0)} -> expected tag = [{expected_tag}]")

    # Scan all Print/PrintFormat tags that look like [DT818_pro_vN]
    tag_re = re.compile(r'Print[A-Za-z]*\(\s*"\[(DT818_pro_v\d+)\]')
    found = [(i + 1, mm.group(1))
             for i, line in enumerate(text.splitlines())
             for mm in [tag_re.search(line)] if mm]

    if not found:
        print(f"FAIL: no `[DT818_pro_vN]` Print tags found in {ea_path.name}")
        return 3

    mismatches = [(ln, tag) for ln, tag in found if tag != expected_tag]
    print(f"  Found {len(found)} tagged Print statements")
    for ln, tag in found:
        marker = "  OK" if tag == expected_tag else "  DRIFT"
        print(f"    {marker:<8} line {ln:>4}: [{tag}]")
    if mismatches:
        print(f"\nFAIL: {len(mismatches)} Print tags drift from `#property version`. "
              f"Update them to [{expected_tag}].")
        return 1
    print(f"\nOK: all Print tags match {expected_tag}.")
    return 0


if __name__ == "__main__":
    sys.exit(check(EA_PATH))
