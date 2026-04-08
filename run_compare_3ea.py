"""3-EA comparison backtest: ZGB5 vs DT818 vs ZGB_DT818_Combined.

Runs Every-Tick backtests for Dec 21, 2025 -> Mar 21, 2026 at $10K deposit.
Uses the current best .set files for each EA.
"""
from __future__ import annotations

import io
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
MT5_TERMINAL = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN    = 18912087
MT5_SERVER   = "VantageInternational-Live 3"
SYMBOL       = "XAUUSD"
SPREAD       = 45
DEPOSIT      = 10_000

FROM_DATE = date(2025, 12, 21)
TO_DATE   = date(2026, 3, 21)

OUTPUT_DIR = Path("output/compare_3ea_dec21_mar21")

ZGB5_SET  = Path("configs/sets/portfolio_best_12w_mar14_optb_s2r3_reopt_jun06.set")
DT818_SET = Path("configs/sets/dt818_best_12w_mar14_reopt_jun06.set")


# ---------------------------------------------------------------------------
# INI builder
# ---------------------------------------------------------------------------
def _set_to_tester_inputs(set_lines: list[str]) -> str:
    """Convert .set lines (6-field format) to INI TesterInputs (5-field: name=val||val||step||min||max)."""
    out = []
    for line in set_lines:
        if "=" not in line:
            continue
        name, _, rhs = line.partition("=")
        parts = rhs.strip().split("||")
        # Keep first value; rebuild as 5-field (drop the Y/N flag)
        val = parts[0].strip()
        if len(parts) >= 5:
            out.append(f"{name.strip()}={val}||{parts[1].strip()}||{parts[2].strip()}||{parts[3].strip()}||{parts[4].strip()}")
        else:
            out.append(f"{name.strip()}={val}")
    return "\n".join(out)


def _build_backtest_ini(ea_path: str, period: str, tester_inputs: str, report_id: str) -> str:
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\n"
        f"Server={MT5_SERVER}\n"
        "ProxyEnable=0\n"
        "CertInstall=0\n"
        "NewsEnable=0\n"
        "\n"
        "[Charts]\n"
        "\n"
        "[Experts]\n"
        "\n"
        "[Tester]\n"
        f"Expert={ea_path}\n"
        f"Symbol={SYMBOL}\n"
        f"Period={period}\n"
        "Model=0\n"
        "Optimization=0\n"
        f"Deposit={DEPOSIT}\n"
        f"Spread={SPREAD}\n"
        f"FromDate={FROM_DATE.strftime('%Y.%m.%d')}\n"
        f"ToDate={TO_DATE.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\n"
        "Visual=0\n"
        "TesterStart=1\n"
        "ReplaceReport=1\n"
        "ShutdownTerminal=1\n"
        f"Report={report_id}\n"
        "\n"
        "[TesterInputs]\n"
        f"{tester_inputs}\n"
    )


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


def _run_backtest(job_id: str, ini_content: str) -> Path:
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job

    ini_path = OUTPUT_DIR / f"{job_id}.ini"
    ini_path.write_text(ini_content, encoding="utf-8")

    print(f"  Launching MT5 ({job_id})...")
    print(f"Launch command: {MT5_TERMINAL} /config:{ini_path.resolve()}")
    exit_code = run_mt5_job(MT5_TERMINAL, str(ini_path))
    print(f"  Exit code: {exit_code}")

    artifact = find_report_artifact(OUTPUT_DIR, job_id, MT5_TERMINAL)
    if artifact is None:
        raise RuntimeError(f"No report artifact found for {job_id}")
    collected = copy_report_artifact(artifact, OUTPUT_DIR)
    print(f"  Collected: {collected.name}")
    return collected


def _parse_metrics(report_path: Path, job_id: str) -> dict:
    from zgb_opti.xml_parser import parse_forward_report
    metrics, warns = parse_forward_report(report_path, job_id)
    for w in warns:
        print(f"  WARN: {w}")
    return metrics


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    date_str = f"{FROM_DATE.strftime('%Y-%m-%d')} -> {TO_DATE.strftime('%Y-%m-%d')}"
    print("=" * 66)
    print(f"  3-EA Comparison: {date_str}")
    print(f"  Symbol : {SYMBOL}  Spread={SPREAD}  Deposit=${DEPOSIT:,}  Model=Every Tick")
    print("=" * 66)

    results: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # 1. ZGB5_v14_3
    # ------------------------------------------------------------------
    job_id = "bt_zgb5_dec21_mar21"
    report = next((OUTPUT_DIR / f"{job_id}{ext}" for ext in (".htm", ".html")
                   if (OUTPUT_DIR / f"{job_id}{ext}").exists()), None)
    if report:
        print(f"\n[ZGB5] Cached: {report.name}")
    else:
        print(f"\n[ZGB5] Running backtest (ZGB5_v14_3, M15)...")
        zgb5_lines = _read_set_lines(ZGB5_SET)
        ini = _build_backtest_ini("ZGB5_v14_3", "M15", _set_to_tester_inputs(zgb5_lines), job_id)
        report = _run_backtest(job_id, ini)

    results["ZGB5 (M15)"] = _parse_metrics(report, job_id)

    # ------------------------------------------------------------------
    # 2. DT818_EA
    # ------------------------------------------------------------------
    job_id = "bt_dt818_dec21_mar21"
    report = next((OUTPUT_DIR / f"{job_id}{ext}" for ext in (".htm", ".html")
                   if (OUTPUT_DIR / f"{job_id}{ext}").exists()), None)
    if report:
        print(f"\n[DT818] Cached: {report.name}")
    else:
        print(f"\n[DT818] Running backtest (DT818_EA, M30)...")
        dt818_lines = _read_set_lines(DT818_SET)
        ini = _build_backtest_ini("DT818_EA", "M30", _set_to_tester_inputs(dt818_lines), job_id)
        report = _run_backtest(job_id, ini)

    results["DT818 (M30)"] = _parse_metrics(report, job_id)

    # ------------------------------------------------------------------
    # 3. ZGB_DT818_Combined — merge both .set files
    # ------------------------------------------------------------------
    job_id = "bt_combined_dec21_mar21"
    report = next((OUTPUT_DIR / f"{job_id}{ext}" for ext in (".htm", ".html")
                   if (OUTPUT_DIR / f"{job_id}{ext}").exists()), None)
    if report:
        print(f"\n[Combined] Cached: {report.name}")
    else:
        print(f"\n[Combined] Running backtest (ZGB_DT818_Combined, M15)...")
        zgb5_lines  = _read_set_lines(ZGB5_SET)
        dt818_lines = _read_set_lines(DT818_SET)
        combined_inputs = _set_to_tester_inputs(zgb5_lines + dt818_lines)
        ini = _build_backtest_ini("ZGB_DT818_Combined", "M15", combined_inputs, job_id)
        report = _run_backtest(job_id, ini)

    results["Combined (M15)"] = _parse_metrics(report, job_id)

    # ------------------------------------------------------------------
    # Summary table
    # ------------------------------------------------------------------
    print()
    print("=" * 66)
    print(f"  Comparison: {date_str}  |  ${DEPOSIT:,} deposit, Every Tick")
    print(f"  {'Metric':<28} {'ZGB5 (M15)':>12} {'DT818 (M30)':>12} {'Combined':>12}")
    print(f"  {'-'*64}")

    def _fmt_np(v):
        try:
            return f"{float(v):>+12,.0f}"
        except Exception:
            return f"{'N/A':>12}"

    def _fmt_f(v, fmt=".3f"):
        try:
            return f"{float(v):>12{fmt}}"
        except Exception:
            return f"{'N/A':>12}"

    r_zgb5 = results["ZGB5 (M15)"]
    r_dt   = results["DT818 (M30)"]
    r_comb = results["Combined (M15)"]

    print(f"  {'Net Profit':<28}"
          f"{_fmt_np(r_zgb5.get('net_profit'))}"
          f"{_fmt_np(r_dt.get('net_profit'))}"
          f"{_fmt_np(r_comb.get('net_profit'))}")
    print(f"  {'Profit Factor':<28}"
          f"{_fmt_f(r_zgb5.get('profit_factor'))}"
          f"{_fmt_f(r_dt.get('profit_factor'))}"
          f"{_fmt_f(r_comb.get('profit_factor'))}")
    print(f"  {'Drawdown %':<28}"
          f"{_fmt_f(r_zgb5.get('drawdown_pct'), '.1f')}%"
          f"{_fmt_f(r_dt.get('drawdown_pct'), '.1f')}%"
          f"{_fmt_f(r_comb.get('drawdown_pct'), '.1f')}%")
    print(f"  {'Trades':<28}"
          f"{int(r_zgb5.get('trades') or 0):>12}"
          f"{int(r_dt.get('trades') or 0):>12}"
          f"{int(r_comb.get('trades') or 0):>12}")
    print("=" * 66)


if __name__ == "__main__":
    main()
