"""DT818_EA risk stage — sweep _LotMode and _RiskPct only.

Strategy params are fixed from the existing deployment .set (already optimized).
This stage finds the best lot sizing method and risk % on a realistic deposit.

Stage 1 — Risk sweep (slow complete)
  Fix all strategy params from dt818_best_12w_mar14_reopt_jun06.set.
  Sweep: _LotMode (0=Tier, 1=Balance%), _RiskPct (0.5-5.0 step 0.5).
  Combos: 2 x 10 = 20 passes.
  Algorithm: Slow complete (Optimization=1), Model=1 (1m OHLC), Deposit=10K
  Period: Dec 21, 2025 -> Mar 14, 2026 (IS window)

Stage 2 — Validation backtest
  Single Every-Tick backtest of best risk params.
  Model=0 (Every Tick), Deposit=10K

Output: configs/sets/dt818_best_12w_mar14_reopt_jun06.set (updated)
        output/dt818_risk_stage/
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
BASE_SET_PATH  = Path("configs/sets/dt818_best_12w_mar14_reopt_jun06.set")
OUTPUT_DIR     = Path("output/dt818_risk_stage")
FINAL_SET_PATH = Path("configs/sets/dt818_best_12w_mar14_reopt_jun06.set")

OPT_START = date(2025, 12, 21)
OPT_END   = date(2026, 3, 14)

MT5_TERMINAL = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN    = 18912087
MT5_SERVER   = "VantageInternational-Live 3"
EA_PATH      = "DT818_EA"
SYMBOL       = "XAUUSD"
PERIOD       = "M30"
SPREAD       = 45
DEPOSIT      = 10_000    # realistic deposit for both stages

DD_THRESHOLD = 20.0

RISK_PCT_MIN  = 0.5
RISK_PCT_MAX  = 8.0
RISK_PCT_STEP = 0.5


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


def _build_risk_set_lines(base_lines: list[str]) -> list[str]:
    """Take base .set (all params fixed), fix LotMode=1, sweep RiskPct only."""
    out: list[str] = []
    lotmode_added = False
    riskpct_added = False

    for line in base_lines:
        if "=" not in line:
            out.append(line)
            continue

        name = line.partition("=")[0].strip()

        if name == "_LotMode":
            out.append("_LotMode=1||1||1||1||1||N")
            lotmode_added = True
        elif name == "_RiskPct":
            out.append(f"_RiskPct={RISK_PCT_MIN}||{RISK_PCT_MIN}||{RISK_PCT_STEP}||{RISK_PCT_MIN}||{RISK_PCT_MAX}||Y")
            riskpct_added = True
        else:
            out.append(line)

    if not lotmode_added:
        out.insert(1, "_LotMode=1||1||1||1||1||N")
    if not riskpct_added:
        out.insert(2, f"_RiskPct={RISK_PCT_MIN}||{RISK_PCT_MIN}||{RISK_PCT_STEP}||{RISK_PCT_MIN}||{RISK_PCT_MAX}||Y")

    return out


def _set_to_ini_lines(set_lines: list[str]) -> list[str]:
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
        "OptimizationCriterion=0\n"
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


def _write_deployment_set(set_lines: list[str], best_pass: "pd.Series",
                          output_path: Path) -> None:
    out: list[str] = []
    for line in set_lines:
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

    print("=" * 65)
    print(f"  DT818_EA risk stage: {OPT_START} -> {OPT_END}  (IS window)")
    print(f"  Base set  : {BASE_SET_PATH}")
    n_combos = int((RISK_PCT_MAX - RISK_PCT_MIN) / RISK_PCT_STEP) + 1
    print(f"  Sweep     : _RiskPct {RISK_PCT_MIN}-{RISK_PCT_MAX} step {RISK_PCT_STEP}  ({n_combos} combos, LotMode=1 fixed)")
    print(f"  Algorithm : slow complete, deposit={DEPOSIT:,}")
    print(f"  Output    : {FINAL_SET_PATH}")
    print("=" * 65)

    # ------------------------------------------------------------------ #
    # Stage 1 — Risk sweep (slow complete, 20 combos)                     #
    # ------------------------------------------------------------------ #
    print("\n--- Stage 1: Risk sweep (slow complete, 20 combos) ---")
    job_id   = "risk_sweep_dt818_m30_dec21_mar14"
    xml_path = OUTPUT_DIR / f"{job_id}.xml"

    if xml_path.exists():
        print(f"  Cached: {xml_path.name}")
    else:
        base_lines  = _read_set_lines(BASE_SET_PATH)
        risk_lines  = _build_risk_set_lines(base_lines)
        set_path    = OUTPUT_DIR / "risk_sweep.set"
        set_path.write_text("\n".join(risk_lines) + "\n", encoding="utf-8")
        print(f"  Risk sweep .set written: {set_path.name}")
        print(f"  Params being swept:")
        for l in risk_lines:
            if "||Y" in l:
                print(f"    {l}")

        ini = _build_opti_ini(risk_lines, OPT_START, OPT_END, job_id)
        _run_optimization(job_id, ini, OUTPUT_DIR)

    df   = _parse_opti_xml(xml_path, job_id)
    best = _select_best(df, DD_THRESHOLD)

    lot_mode = int(float(best.get("param__LotMode", 0)))
    risk_pct = float(best.get("param__RiskPct", 0))

    print(f"\n  All passes:")
    print(f"  {'LotMode':<10} {'RiskPct':>8} {'Net Profit':>16} {'PF':>8} {'DD%':>8} {'Trades':>8}")
    print(f"  {'-'*62}")
    df_sorted = df.copy()
    df_sorted["net_profit"] = df_sorted["net_profit"].astype(float)
    for _, row in df_sorted.sort_values("net_profit", ascending=False).head(20).iterrows():
        lm  = int(float(row.get("param__LotMode", 0)))
        rp  = float(row.get("param__RiskPct", 0))
        tag = " <-- best" if int(row.pass_id) == int(best.pass_id) else ""
        print(f"  {'Tier' if lm==0 else 'Bal%':<10} {rp:>8.1f} {float(row.net_profit):>+16,.0f}"
              f" {float(row.profit_factor):>8.3f} {float(row.drawdown_pct):>7.2f}%"
              f" {int(row.trades):>8}{tag}")

    print(f"\n  Best: LotMode={'Tier' if lot_mode==0 else 'Balance%'}  RiskPct={risk_pct}%")
    print(f"    net_profit   : {float(best.net_profit):>+14,.0f}")
    print(f"    profit_factor: {float(best.profit_factor):.3f}")
    print(f"    drawdown_pct : {float(best.drawdown_pct):.2f}%")
    print(f"    trades       : {int(best.trades)}")

    # ------------------------------------------------------------------ #
    # Write deployment .set                                                #
    # ------------------------------------------------------------------ #
    print(f"\n--- Writing deployment .set ---")
    base_lines = _read_set_lines(BASE_SET_PATH)
    risk_lines = _build_risk_set_lines(base_lines)
    _write_deployment_set(risk_lines, best, FINAL_SET_PATH)

    # ------------------------------------------------------------------ #
    # Stage 2 — Validation backtest (Every Tick)                          #
    # ------------------------------------------------------------------ #
    print(f"\n--- Stage 2: Validation backtest (Every Tick, IS window) ---")
    val_job_id = "validation_risk_dt818_m30_dec21_mar14"
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

    # ------------------------------------------------------------------ #
    # Summary + comparison vs previous TierBase set                       #
    # ------------------------------------------------------------------ #
    result = {
        "opt_start":   str(OPT_START),
        "opt_end":     str(OPT_END),
        "final_set":   str(FINAL_SET_PATH),
        "lot_mode":    "Tier" if lot_mode == 0 else "Balance%",
        "risk_pct":    risk_pct,
        **metrics,
    }
    out_json = OUTPUT_DIR / "result.json"
    out_json.write_text(json.dumps(result, indent=2), encoding="utf-8")

    np_new = metrics.get("net_profit",    0) or 0
    pf_new = metrics.get("profit_factor", 0) or 0
    dd_new = metrics.get("drawdown_pct",  0) or 0
    tr_new = int(metrics.get("trades",    0) or 0)

    # Previous TierBase result (Dec 21-Mar 14 IS, 10K deposit)
    PREV_NP = 8626
    PREV_PF = 1.220
    PREV_DD = 24.2
    PREV_TR = 188

    print()
    print("=" * 65)
    print(f"  DONE  —  {OPT_START} -> {OPT_END}")
    print(f"  Winner: LotMode={'Tier' if lot_mode==0 else 'Balance%'}  RiskPct={risk_pct}%")
    print()
    print(f"  {'Metric':<20} {'New (risk opt)':>16} {'Prev (TierBase)':>16}  {'Delta':>10}")
    print(f"  {'-'*66}")
    print(f"  {'Net Profit':<20} {np_new:>+16,.0f} {PREV_NP:>+16,.0f}  {np_new-PREV_NP:>+10,.0f}")
    print(f"  {'Profit Factor':<20} {pf_new:>16.3f} {PREV_PF:>16.3f}  {pf_new-PREV_PF:>+10.3f}")
    print(f"  {'Drawdown %':<20} {dd_new:>15.1f}% {PREV_DD:>15.1f}%  {dd_new-PREV_DD:>+9.1f}%")
    print(f"  {'Trades':<20} {tr_new:>16} {PREV_TR:>16}")
    print("=" * 65)
    print(f"\n  Written: {out_json}")


if __name__ == "__main__":
    main()
