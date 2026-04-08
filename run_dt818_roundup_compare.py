"""Compare MathFloor vs MathRound lot sizing: backtest both versions $5K and $50K at 4% risk."""
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

SET_FILE   = Path("configs/sets/DT818_4_mar21_exp_may16.set")
OUTPUT_DIR = Path("output/dt818_roundup_compare")

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

    print("=" * 70)
    print(f"  DT818 MathRound vs MathFloor  |  Risk 4%  |  Every Tick")
    print(f"  Period: {FROM_DATE} -> {TO_DATE}")
    print("=" * 70)

    # MathRound results (EA already updated — run fresh)
    round_results = {}
    for deposit in DEPOSITS:
        tag    = f"{'5k' if deposit == 5_000 else '50k'}"
        job_id = f"bt_dt818_round_{tag}"
        report = next((OUTPUT_DIR / f"{job_id}{ext}" for ext in (".htm", ".html")
                       if (OUTPUT_DIR / f"{job_id}{ext}").exists()), None)
        if report:
            print(f"\n[MathRound ${deposit:,}] Cached: {report.name}")
        else:
            print(f"\n[MathRound ${deposit:,}] Running...")
            report = _run_backtest(job_id, _build_ini(tester_inputs, job_id, deposit))
        round_results[deposit] = _parse(report, job_id)

    # MathFloor results (already cached from previous run)
    floor_results = {
        5_000:  _parse(Path("output/dt818_deposit_compare/bt_dt818_4pct_5k.htm"),  "bt_dt818_4pct_5k"),
        50_000: _parse(Path("output/dt818_deposit_compare/bt_dt818_4pct_50k.htm"), "bt_dt818_4pct_50k"),
    }

    # Summary
    print(f"\n{'=' * 70}")
    print(f"  {'Metric':<24} {'Floor $5K':>12} {'Round $5K':>12} {'Floor $50K':>12} {'Round $50K':>12}")
    print(f"  {'-'*66}")

    def np(v):
        try: return f"{float(v):>+12,.0f}"
        except: return f"{'N/A':>12}"
    def ff(v, fmt=".3f"):
        try: return f"{float(v):>12{fmt}}"
        except: return f"{'N/A':>12}"

    f5  = floor_results[5_000]
    r5  = round_results[5_000]
    f50 = floor_results[50_000]
    r50 = round_results[50_000]

    print(f"  {'Net Profit':<24}{np(f5.get('net_profit'))}{np(r5.get('net_profit'))}{np(f50.get('net_profit'))}{np(r50.get('net_profit'))}")
    print(f"  {'Profit Factor':<24}{ff(f5.get('profit_factor'))}{ff(r5.get('profit_factor'))}{ff(f50.get('profit_factor'))}{ff(r50.get('profit_factor'))}")
    print(f"  {'Drawdown %':<24}{ff(f5.get('drawdown_pct'),'.1f')}{ff(r5.get('drawdown_pct'),'.1f')}{ff(f50.get('drawdown_pct'),'.1f')}{ff(r50.get('drawdown_pct'),'.1f')}")
    print(f"  {'Trades':<24}{int(f5.get('trades') or 0):>12}{int(r5.get('trades') or 0):>12}{int(f50.get('trades') or 0):>12}{int(r50.get('trades') or 0):>12}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
