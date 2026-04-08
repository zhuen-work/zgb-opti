"""ZGB_DT818_Combined risk stage — sweep all 6 risk % params independently.

All strategy params (ZGB5 S1-S5 + DT818 T1/T2) are fixed from the existing
deployment .set files. Only the 6 risk allocation params are swept.

Stage 1 — Risk sweep (slow complete)
  Merge ZGB5 + DT818 .set files. Fix all strategy params.
  Sweep: InpRiskPercent_S1, InpReverseRiskPercent_S2, InpFractalRiskPercent_S3,
         InpBBMidRiskPercent_S4, InpBBMidInvRiskPercent_S5, _RiskPct
         each from 1.5 to 6.0 step 1.5 (4 values each = 4^6 = 4096 combos)
  Algorithm: Slow complete (Optimization=1), Model=1 (1m OHLC), Deposit=10K
  Period: Dec 21, 2025 -> Mar 14, 2026 (IS window)

Stage 2 — Validation backtest
  Single Every-Tick backtest of winning allocation.
  Model=0 (Every Tick), Deposit=10K

Output: configs/sets/combined_best_12w_mar14_reopt_jun06.set
        output/combined_risk_stage/
"""
from __future__ import annotations

import io
import json
import sys
from datetime import date
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ZGB5_SET_PATH  = Path("configs/sets/portfolio_best_12w_mar14_optb_s2r3_reopt_jun06.set")
DT818_SET_PATH = Path("configs/sets/dt818_best_12w_mar14_reopt_jun06.set")
OUTPUT_DIR     = Path("output/combined_risk_stage")
FINAL_SET_PATH = Path("configs/sets/combined_best_12w_mar14_reopt_jun06.set")

OPT_START = date(2025, 12, 21)
OPT_END   = date(2026, 3, 14)

MT5_TERMINAL = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN    = 18912087
MT5_SERVER   = "VantageInternational-Live 3"
EA_PATH      = "ZGB_DT818_Combined"
SYMBOL       = "XAUUSD"
PERIOD       = "M15"
SPREAD       = 45
DEPOSIT      = 10_000

DD_THRESHOLD = 40.0   # combined EA Every Tick DD runs ~1.7x 1m OHLC; allow up to 40% on sweep model

RISK_MIN  = 1.5
RISK_MAX  = 6.0
RISK_STEP = 1.5

# The 6 risk params to sweep (name in .set file)
RISK_PARAMS = [
    "InpRiskPercent_S1",
    "InpReverseRiskPercent_S2",
    "InpFractalRiskPercent_S3",
    "InpBBMidRiskPercent_S4",
    "InpBBMidInvRiskPercent_S5",
    "_RiskPct",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _read_set_lines(set_path: Path) -> list[str]:
    raw = set_path.read_bytes()
    if raw[:2] == b"\xff\xfe":
        text = raw.decode("utf-16-le", errors="replace").lstrip("\ufeff")
    else:
        text = raw.decode("utf-8", errors="replace")
    return [l.strip().replace("\x00", "") for l in text.splitlines()
            if l.strip().replace("\x00", "") and not l.strip().startswith(";")]


def _build_combined_base_lines() -> list[str]:
    return _read_set_lines(ZGB5_SET_PATH) + _read_set_lines(DT818_SET_PATH)


def _build_risk_set_lines(base_lines: list[str]) -> list[str]:
    """Fix all strategy params; mark the 6 risk params as sweepable."""
    sweep_line = f"{RISK_MIN}||{RISK_MIN}||{RISK_STEP}||{RISK_MIN}||{RISK_MAX}||Y"
    swept = set(RISK_PARAMS)
    found = set()
    out: list[str] = []

    for line in base_lines:
        if "=" not in line:
            out.append(line)
            continue
        name = line.partition("=")[0].strip()
        if name in swept:
            out.append(f"{name}={sweep_line}")
            found.add(name)
        else:
            out.append(line)

    # Add any not present in base
    for name in RISK_PARAMS:
        if name not in found:
            out.append(f"{name}={sweep_line}")

    return out


def _set_to_ini_lines(set_lines: list[str]) -> list[str]:
    """Convert 6-field .set to 5-field TesterInputs format."""
    out: list[str] = []
    for line in set_lines:
        if "=" in line and "||" in line:
            name, _, rest = line.partition("=")
            parts = rest.split("||")
            if len(parts) == 6:
                line = f"{name}={parts[0]}||{parts[3]}||{parts[2]}||{parts[4]}||{parts[5]}"
        out.append(line)
    return out


def _build_opti_ini(set_lines: list[str], from_date: date, to_date: date,
                    report_id: str) -> str:
    tester_inputs = "\n".join(_set_to_ini_lines(set_lines))
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\n"
        f"Server={MT5_SERVER}\n"
        "ProxyEnable=0\n"
        "CertInstall=0\n"
        "NewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n"
        "[Tester]\n"
        f"Expert={EA_PATH}\n"
        f"Symbol={SYMBOL}\n"
        f"Period={PERIOD}\n"
        "Model=1\n"
        "Optimization=1\n"
        "OptimizationCriterion=4\n"
        f"Deposit={DEPOSIT}\n"
        f"Spread={SPREAD}\n"
        f"FromDate={from_date.strftime('%Y.%m.%d')}\n"
        f"ToDate={to_date.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\n"
        "Visual=0\n"
        "TesterStart=1\n"
        "ReplaceReport=1\n"
        "ShutdownTerminal=1\n"
        f"Report={report_id}\n"
        "\n[TesterInputs]\n"
        f"{tester_inputs}\n"
    )


def _build_backtest_ini(set_lines: list[str], from_date: date, to_date: date,
                        report_id: str) -> str:
    clean_lines: list[str] = []
    for line in set_lines:
        if "=" in line:
            name, _, rhs = line.partition("=")
            value = rhs.split("||")[0].strip()
            clean_lines.append(f"{name.strip()}={value}")
        else:
            clean_lines.append(line)
    tester_inputs = "\n".join(clean_lines)
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\n"
        f"Server={MT5_SERVER}\n"
        "ProxyEnable=0\n"
        "CertInstall=0\n"
        "NewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n"
        "[Tester]\n"
        f"Expert={EA_PATH}\n"
        f"Symbol={SYMBOL}\n"
        f"Period={PERIOD}\n"
        "Model=0\n"
        "Optimization=0\n"
        f"Deposit={DEPOSIT}\n"
        f"Spread={SPREAD}\n"
        f"FromDate={from_date.strftime('%Y.%m.%d')}\n"
        f"ToDate={to_date.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\n"
        "Visual=0\n"
        "TesterStart=1\n"
        "ReplaceReport=1\n"
        "ShutdownTerminal=1\n"
        f"Report={report_id}\n"
        "\n[TesterInputs]\n"
        f"{tester_inputs}\n"
    )


def _run_optimization(job_id: str, ini_content: str, output_dir: Path) -> Path:
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job

    ini_path = output_dir / f"{job_id}.ini"
    ini_path.write_text(ini_content, encoding="utf-8")

    print(f"  Launching MT5 ({job_id})...")
    exit_code = run_mt5_job(MT5_TERMINAL, str(ini_path))
    print(f"  Exit code: {exit_code}")

    artifact = find_report_artifact(output_dir, job_id, MT5_TERMINAL)
    if artifact is None:
        raise RuntimeError(f"No report artifact found for {job_id}")
    collected = copy_report_artifact(artifact, output_dir)
    print(f"  Collected: {collected.name}")
    return collected


def _parse_opti_xml(xml_path: Path, job_id: str) -> "pd.DataFrame":
    import pandas as pd
    from zgb_opti.xml_parser import parse_optimization_xml, write_passes

    records, warns = parse_optimization_xml(xml_path, job_id)
    for w in warns:
        print(f"  WARN: {w}")

    parquet_path, _ = write_passes(records, xml_path.parent)
    print(f"  Parsed {len(records)} passes -> {parquet_path.name}")
    return pd.read_parquet(parquet_path)


def _select_best(df: "pd.DataFrame", dd_threshold: float) -> "pd.Series":
    import pandas as pd
    df = df.copy()
    df["net_profit"]   = pd.to_numeric(df["net_profit"],   errors="coerce")
    df["drawdown_pct"] = pd.to_numeric(df["drawdown_pct"], errors="coerce")
    low_dd = df[df["drawdown_pct"] < dd_threshold]
    pool   = low_dd if not low_dd.empty else df
    return pool.sort_values("net_profit", ascending=False).iloc[0]


def _write_deployment_set(risk_lines: list[str], best_pass: "pd.Series",
                          output_path: Path) -> None:
    out: list[str] = []
    for line in risk_lines:
        if "=" not in line:
            out.append(line)
            continue
        name, _, rhs = line.partition("=")
        name  = name.strip()
        parts = rhs.strip().split("||")
        is_optimized = len(parts) >= 6 and parts[-1].strip().upper() == "Y"

        if is_optimized:
            col = f"param_{name}"
            if col in best_pass.index:
                val = best_pass[col]
                try:
                    f = float(val)
                    val_str = str(int(f)) if f == int(f) else str(f)
                except (TypeError, ValueError):
                    val_str = str(val)
                out.append(f"{name}={val_str}||{val_str}||1||{val_str}||{val_str}||N")
            else:
                out.append(f"{name}={parts[0]}||{parts[0]}||1||{parts[0]}||{parts[0]}||N")
        else:
            out.append(line)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"  Written: {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    from zgb_opti.collector import find_report_artifact, copy_report_artifact
    from zgb_opti.launcher import run_mt5_job
    from zgb_opti.xml_parser import parse_forward_report

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    n_vals   = int(round((RISK_MAX - RISK_MIN) / RISK_STEP)) + 1
    n_combos = n_vals ** len(RISK_PARAMS)
    print("=" * 65)
    print(f"  ZGB_DT818_Combined risk allocation: {OPT_START} -> {OPT_END}")
    print(f"  ZGB5 set  : {ZGB5_SET_PATH}")
    print(f"  DT818 set : {DT818_SET_PATH}")
    print(f"  Sweep     : {', '.join(RISK_PARAMS)}")
    print(f"            : {RISK_MIN}-{RISK_MAX} step {RISK_STEP} ({n_vals} values each = {n_combos:,} combos)")
    print(f"  Algorithm : slow complete, deposit={DEPOSIT:,}")
    print(f"  Output    : {FINAL_SET_PATH}")
    print("=" * 65)

    # ------------------------------------------------------------------ #
    # Stage 1 — Risk allocation sweep                                     #
    # ------------------------------------------------------------------ #
    print(f"\n--- Stage 1: Risk allocation sweep (slow complete, {n_combos:,} combos) ---")
    job_id   = "risk_sweep_combined_m15_dec21_mar14"
    xml_path = OUTPUT_DIR / f"{job_id}.xml"

    if xml_path.exists():
        print(f"  Cached: {xml_path.name}")
    else:
        base_lines = _build_combined_base_lines()
        risk_lines = _build_risk_set_lines(base_lines)
        set_path   = OUTPUT_DIR / "risk_sweep.set"
        set_path.write_text("\n".join(risk_lines) + "\n", encoding="utf-8")
        print(f"  Params being swept:")
        for l in risk_lines:
            if "||Y" in l:
                print(f"    {l}")

        ini = _build_opti_ini(risk_lines, OPT_START, OPT_END, job_id)
        _run_optimization(job_id, ini, OUTPUT_DIR)

    df   = _parse_opti_xml(xml_path, job_id)
    best = _select_best(df, DD_THRESHOLD)

    # Print top 10
    print(f"\n  Top 10 passes (by net profit, DD < {DD_THRESHOLD}%):")
    cols = {f"param_{p}": p.replace("InpRiskPercent_", "").replace("Inp", "").replace("RiskPercent_", "").replace("_RiskPct", "DT818") for p in RISK_PARAMS}
    df_f = df.copy()
    df_f["net_profit"]   = df_f["net_profit"].astype(float)
    df_f["drawdown_pct"] = df_f["drawdown_pct"].astype(float)
    df_top = df_f[df_f["drawdown_pct"] < DD_THRESHOLD].sort_values("net_profit", ascending=False).head(10)

    header = f"  {'S1':>4} {'S2':>4} {'S3':>4} {'S4':>4} {'S5':>4} {'DT818':>5}  {'Net Profit':>14} {'PF':>6} {'DD%':>7} {'Trades':>7}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for _, row in df_top.iterrows():
        tag = " <--" if int(row.pass_id) == int(best.pass_id) else ""
        vals = [f"{float(row.get(f'param_{p}', 0)):>4.1f}" for p in RISK_PARAMS]
        print(f"  {'  '.join(vals)}  {float(row.net_profit):>+14,.0f} {float(row.profit_factor):>6.3f} {float(row.drawdown_pct):>6.2f}% {int(row.trades):>7}{tag}")

    print(f"\n  Best allocation:")
    for p in RISK_PARAMS:
        col = f"param_{p}"
        print(f"    {p:<32} : {float(best.get(col, 0)):.1f}%")
    print(f"    net_profit   : {float(best.net_profit):>+14,.0f}")
    print(f"    profit_factor: {float(best.profit_factor):.3f}")
    print(f"    drawdown_pct : {float(best.drawdown_pct):.2f}%")
    print(f"    trades       : {int(best.trades)}")

    # ------------------------------------------------------------------ #
    # Write deployment .set                                                #
    # ------------------------------------------------------------------ #
    print(f"\n--- Writing deployment .set ---")
    base_lines = _build_combined_base_lines()
    risk_lines = _build_risk_set_lines(base_lines)
    _write_deployment_set(risk_lines, best, FINAL_SET_PATH)

    # ------------------------------------------------------------------ #
    # Stage 2 — Validation backtest (Every Tick)                         #
    # ------------------------------------------------------------------ #
    print(f"\n--- Stage 2: Validation backtest (Every Tick, IS window) ---")
    val_job_id = "validation_risk_combined_m15_dec21_mar14"
    val_report = None
    for ext in (".htm", ".html", ".xml"):
        p = OUTPUT_DIR / f"{val_job_id}{ext}"
        if p.exists():
            val_report = p
            break

    if val_report:
        print(f"  Cached: {val_report.name}")
    else:
        final_lines = _read_set_lines(FINAL_SET_PATH)
        val_ini     = _build_backtest_ini(final_lines, OPT_START, OPT_END, val_job_id)
        ini_path    = OUTPUT_DIR / f"{val_job_id}.ini"
        ini_path.write_text(val_ini, encoding="utf-8")

        print("  Launching MT5 (single backtest)...")
        exit_code = run_mt5_job(MT5_TERMINAL, str(ini_path))
        print(f"  Exit code: {exit_code}")

        artifact = find_report_artifact(OUTPUT_DIR, val_job_id, MT5_TERMINAL)
        if artifact is None:
            print("  ERROR: no validation report found")
            sys.exit(1)
        val_report = copy_report_artifact(artifact, OUTPUT_DIR)
        print(f"  Collected: {val_report.name}")

    metrics, warns = parse_forward_report(val_report, val_job_id)
    for w in warns:
        print(f"  WARN: {w}")

    np_new = metrics.get("net_profit",    0) or 0
    pf_new = metrics.get("profit_factor", 0) or 0
    dd_new = metrics.get("drawdown_pct",  0) or 0
    tr_new = int(metrics.get("trades",    0) or 0)

    result = {
        "opt_start":   str(OPT_START),
        "opt_end":     str(OPT_END),
        "final_set":   str(FINAL_SET_PATH),
        "best_alloc":  {p: float(best.get(f"param_{p}", 0)) for p in RISK_PARAMS},
        **metrics,
    }
    out_json = OUTPUT_DIR / "result.json"
    out_json.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print()
    print("=" * 65)
    print(f"  DONE  —  {OPT_START} -> {OPT_END}")
    print(f"  Validation backtest (Every Tick, IS):")
    print(f"    Net Profit   : {np_new:>+14,.0f}")
    print(f"    Profit Factor: {pf_new:>14.3f}")
    print(f"    Drawdown %   : {dd_new:>13.1f}%")
    print(f"    Trades       : {tr_new:>14}")
    print("=" * 65)
    print(f"\n  Written: {out_json}")


if __name__ == "__main__":
    main()
