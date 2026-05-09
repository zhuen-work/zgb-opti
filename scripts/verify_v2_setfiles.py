"""Verify all 3 v2 setfiles match the intended convention.

S1-3 = PREVIOUS week (MAY2 R1-3), S4-6 = CURRENT week (MAY9 R1-3).
_RiskPct: 3pct=0.5, 6pct=1.0, 9pct=1.5.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

EXPECTED = {
    "S1": {"src": "MAY2 R1", "RangeMinutes": 90, "FixedSL_Pts": 500, "RR_Ratio": 4.0, "HalfTP_Ratio": 0.25, "Magic": 1111},
    "S2": {"src": "MAY2 R2", "RangeMinutes": 90, "FixedSL_Pts": 400, "RR_Ratio": 4.0, "HalfTP_Ratio": 0.0,  "Magic": 2222},
    "S3": {"src": "MAY2 R3", "RangeMinutes": 90, "FixedSL_Pts": 350, "RR_Ratio": 4.0, "HalfTP_Ratio": 0.5,  "Magic": 3333},
    "S4": {"src": "MAY9 R1", "RangeMinutes": 90, "FixedSL_Pts": 650, "RR_Ratio": 4.0, "HalfTP_Ratio": 0.25, "Magic": 4444},
    "S5": {"src": "MAY9 R2", "RangeMinutes": 90, "FixedSL_Pts": 350, "RR_Ratio": 4.0, "HalfTP_Ratio": 0.5,  "Magic": 5555},
    "S6": {"src": "MAY9 R3", "RangeMinutes": 90, "FixedSL_Pts": 400, "RR_Ratio": 4.0, "HalfTP_Ratio": 0.5,  "Magic": 6666},
}
RISK_BY_FILE = {
    "dt818_pro_v2_3pct_may9_may2.set": 0.5,
    "dt818_pro_v2_6pct_may9_may2.set": 1.0,
    "dt818_pro_v2_9pct_may9_may2.set": 1.5,
}
TOTAL_BY_FILE = {
    "dt818_pro_v2_3pct_may9_may2.set": 3.0,
    "dt818_pro_v2_6pct_may9_may2.set": 6.0,
    "dt818_pro_v2_9pct_may9_may2.set": 9.0,
}


def parse_setfile(path: Path) -> dict:
    """Parse a Vantage-style setfile. Each line is `KEY=VALUE` or `KEY=VAL||...||N`."""
    out = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith(";"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        # Strip ||opti||...||N tail; comment after value (e.g. " ; DEPRECATED")
        val = val.split("||")[0].split(";")[0].strip()
        # Coerce to float when possible
        try:
            v_typed = float(val) if "." in val or val.replace("-", "").isdigit() else val
            if isinstance(v_typed, float) and v_typed.is_integer() and "." not in val:
                v_typed = int(v_typed)
        except ValueError:
            v_typed = val
        out[key.strip()] = v_typed
    return out


def verify_setfile(path: Path) -> tuple[bool, list[str]]:
    """Return (ok, errors)."""
    errs = []
    cfg = parse_setfile(path)
    name = path.name
    # _RiskPct
    expected_risk = RISK_BY_FILE[name]
    actual_risk = cfg.get("_RiskPct")
    if abs(float(actual_risk) - expected_risk) > 1e-6:
        errs.append(f"_RiskPct: expected {expected_risk}, got {actual_risk}")
    # Per-stream
    for s_label, exp in EXPECTED.items():
        for field in ("RangeMinutes", "FixedSL_Pts", "RR_Ratio", "HalfTP_Ratio", "Magic"):
            key = f"_ORB_{s_label}_{field}"
            actual = cfg.get(key)
            if actual is None:
                errs.append(f"{key}: MISSING")
                continue
            if abs(float(actual) - float(exp[field])) > 1e-6:
                errs.append(f"{key}: expected {exp[field]} (from {exp['src']}), got {actual}")
        # Enabled = true
        en_key = f"_ORB_{s_label}_Enabled"
        en = cfg.get(en_key)
        if str(en).lower() != "true":
            errs.append(f"{en_key}: expected 'true', got '{en}'")
    # Total risk maths
    total = float(actual_risk) * 6.0
    expected_total = TOTAL_BY_FILE[name]
    if abs(total - expected_total) > 1e-6:
        errs.append(f"total math: 6 streams * {actual_risk}% = {total}%, expected {expected_total}%")
    return (len(errs) == 0, errs)


def main() -> int:
    print("=" * 80)
    print("  Verify v2 setfiles match weekly-reopt convention")
    print("  S1-3 = PREVIOUS week (MAY2 R1-3) | S4-6 = CURRENT week (MAY9 R1-3)")
    print("=" * 80)
    all_ok = True
    for fname, total in TOTAL_BY_FILE.items():
        path = ROOT / "configs" / "sets" / fname
        ok, errs = verify_setfile(path)
        status = "PASS" if ok else "FAIL"
        print(f"\n  [{status}] {fname}  (total {total}%, per_stream {RISK_BY_FILE[fname]}%)")
        if errs:
            for e in errs:
                print(f"    - {e}")
            all_ok = False
        else:
            print(f"    All 6 streams + _RiskPct verified.")
    print("\n" + "=" * 80)
    print(f"  OVERALL: {'PASS' if all_ok else 'FAIL'}")
    print("=" * 80)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
