"""DT818 deposit size comparison: $5K vs $50K, 8-week IS (Jan 24 - Mar 21, 2026), risk 6%."""
from __future__ import annotations

import io, sys
from datetime import date
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

MT5_TERMINAL = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN    = 18912087
MT5_SERVER   = "VantageInternational-Live 3"
SYMBOL       = "XAUUSD"
PERIOD       = "M30"
SPREAD       = 45

FROM_DATE = date(2026, 1, 24)
TO_DATE   = date(2026, 3, 21)

SET_FILE   = Path("configs/sets/DT818_6_mar21_exp_may16.set")
OUTPUT_DIR = Path("output/dt818_deposit_compare")

MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

DEPOSITS = [5_000, 50_000]


def _read_set_lines(path):
    raw = path.read_bytes()
    if raw[:2] == b"\xff\xfe":
        text = raw.decode("utf-16-le", errors="replace").lstrip("\ufeff")
    else:
        text = raw.decode("utf-8", errors="replace")
    return [l.strip().replace("\x00", "") for l in text.splitlines()
            if l.strip().replace("\x00", "") and not l.strip().startswith(";")]


def _set_to_tester_inputs(set_lines):
    out = []
    for line in set_lines:
        if "=" not in line:
            continue
        name, _, rhs = line.partition("=")
        parts = rhs.strip().split("||")
        val = parts[0].strip()
        if len(parts) >= 5:
            out.append(f"{name.strip()}={val}||{parts[1].strip()}||{parts[2].strip()}||{parts[3].strip()}||{parts[4].strip()}")
        else:
            out.append(f"{name.strip()}={val}")
    return "\n".join(out)


def _build_ini(tester_inputs, report_id, deposit):
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert=DT818_EA\nSymbol={SYMBOL}\nPeriod={PERIOD}\nModel=0\n"
        "Optimization=0\n"
        f"Deposit={deposit}\nSpread={SPREAD}\n"
        f"FromDate={FROM_DATE.strftime('%Y.%m.%d')}\nToDate={TO_DATE.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{tester_inputs}\n"
    )


def _run_backtest(job_id, ini_content):
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    ini_path = OUTPUT_DIR / f"{job_id}.ini"
    ini_path.write_text(ini_content, encoding="utf-8")
    print(f"  Launching MT5 ({job_id})...")
    rc = run_mt5_job(MT5_TERMINAL, str(ini_path))
    print(f"  Exit code: {rc}")
    art = find_report_artifact(OUTPUT_DIR, job_id, MT5_TERMINAL)
    if art is None:
        for ext in (".htm", ".html"):
            candidate = MT5_DATA_ROOT / f"{job_id}{ext}"
            if candidate.exists():
                art = candidate
                break
    if art is None:
        raise RuntimeError(f"No report found for {job_id}")
    collected = copy_report_artifact(art, OUTPUT_DIR)
    print(f"  Collected: {collected.name}")
    return collected


def _parse(report_path, job_id):
    from zgb_opti.xml_parser import parse_forward_report
    metrics, warns = parse_forward_report(report_path, job_id)
    for w in warns:
        print(f"  WARN: {w}")
    return metrics


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    set_lines = _read_set_lines(SET_FILE)
    tester_inputs = _set_to_tester_inputs(set_lines)

    print("=" * 62)
    print(f"  DT818 Deposit Comparison  |  Risk 6%  |  Every Tick")
    print(f"  Period: {FROM_DATE} -> {TO_DATE}")
    print(f"  Set: {SET_FILE.name}")
    print("=" * 62)

    results = {}
    for deposit in DEPOSITS:
        tag    = f"{'5k' if deposit == 5_000 else '50k'}"
        job_id = f"bt_dt818_6pct_{tag}"
        report = next((OUTPUT_DIR / f"{job_id}{ext}" for ext in (".htm", ".html")
                       if (OUTPUT_DIR / f"{job_id}{ext}").exists()), None)
        if report:
            print(f"\n[${deposit:,}] Cached: {report.name}")
        else:
            print(f"\n[${deposit:,}] Running backtest...")
            ini = _build_ini(tester_inputs, job_id, deposit)
            report = _run_backtest(job_id, ini)
        results[deposit] = _parse(report, job_id)

    r5  = results[5_000]
    r50 = results[50_000]

    print(f"\n{'=' * 62}")
    print(f"  {'Metric':<28} {'$5,000':>14} {'$50,000':>14}")
    print(f"  {'-'*58}")

    def np(v):
        try: return f"{float(v):>+14,.0f}"
        except: return f"{'N/A':>14}"
    def ff(v, fmt=".3f"):
        try: return f"{float(v):>14{fmt}}"
        except: return f"{'N/A':>14}"

    print(f"  {'Net Profit':<28}{np(r5.get('net_profit'))}{np(r50.get('net_profit'))}")
    print(f"  {'Profit Factor':<28}{ff(r5.get('profit_factor'))}{ff(r50.get('profit_factor'))}")
    print(f"  {'Drawdown %':<28}{ff(r5.get('drawdown_pct'), '.1f')}{ff(r50.get('drawdown_pct'), '.1f')}")
    print(f"  {'Trades':<28}{int(r5.get('trades') or 0):>14}{int(r50.get('trades') or 0):>14}")
    print(f"{'=' * 62}")


if __name__ == "__main__":
    main()
