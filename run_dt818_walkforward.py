"""DT818 walk-forward: find best IS window length and shelf life.

For each IS length (4w, 8w, 12w) ending at the same date (Mar 14, 2026):
  Stage 1 — Genetic optimization (max balance, 1m OHLC, $10M deposit)
  Stage 2 — OOS backtest (Every Tick, $10K, Mar 14 – Mar 21)

Prints a comparison table of OOS performance per IS length.
"""
from __future__ import annotations

import io
import json
import sys
from datetime import date, timedelta
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

MT5_TERMINAL = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN    = 18912087
MT5_SERVER   = "VantageInternational-Live 3"
EA_PATH      = "DT818_EA"
SYMBOL       = "XAUUSD"
PERIOD       = "M30"
SPREAD       = 45
DEPOSIT_OPT  = 10_000_000
DEPOSIT_BT   = 10_000

OPT_END  = date(2026, 3, 14)
OOS_END  = date(2026, 3, 21)

OUTPUT_DIR = Path("output/dt818_walkforward")

# IS window lengths to test (weeks)
IS_LENGTHS_WEEKS = [4, 8, 12]

DD_THRESHOLD = 20.0


def _is_start(weeks: int) -> date:
    return OPT_END - timedelta(weeks=weeks)


def _read_set_lines(path: Path) -> list[str]:
    raw = path.read_bytes()
    if raw[:2] == b"\xff\xfe":
        text = raw.decode("utf-16-le", errors="replace").lstrip("\ufeff")
    else:
        text = raw.decode("utf-8", errors="replace")
    return [l.strip().replace("\x00", "") for l in text.splitlines()
            if l.strip().replace("\x00", "") and not l.strip().startswith(";")]


def _build_set_lines() -> list[str]:
    """DT818 Stage 1 set: all strategy params optimized, risk fixed neutral."""
    return [
        "_BaseMagic=1000||1000||1||1000||1000||N",
        "_OrderComment=DT818_EA",
        "_CapitalProtectionAmount=0.0||0.0||1||0.0||0.0||N",
        "_LotMode=1||1||1||1||1||N",
        "_RiskPct=3.0||3.0||1||3.0||3.0||N",
        "TierBase=2000||2000||1||2000||2000||N",
        "LotStep=0.01||0.01||1||0.01||0.01||N",
        "_Trade1=1||1||1||1||1||N",
        f"_time_frame=30||30||1||30||30||N",
        "_take_profit=10000||2000||500||2000||15000||Y",
        "_stop_loss=10000||2000||500||2000||15000||Y",
        "_EMA_Period1=5||3||1||3||20||Y",
        "_Bars=4||2||1||2||10||Y",
        "_RiskMode1=0||0||1||0||1||Y",
        "_TSL1=500||200||100||200||2000||Y",
        "_TSLA1=200||100||50||100||1000||Y",
        "_BETrigger1=2500||500||250||500||5000||Y",
        "_BEBuf1=450||100||50||100||1000||Y",
        "_Trade2=1||0||1||0||1||Y",
        f"_time_frame2=30||30||1||30||30||N",
        "_take_profit2=9500||2000||500||2000||15000||Y",
        "_stop_loss2=8500||2000||500||2000||15000||Y",
        "_EMA_Period2=5||3||1||3||20||Y",
        "_Bars2=4||2||1||2||10||Y",
        "_RiskMode2=1||0||1||0||2||Y",
        "_TSL2=4500||200||100||200||5000||Y",
        "_TSLA2=1000||100||50||100||2000||Y",
        "_BETrigger2=500||200||100||200||2000||Y",
        "_BEBuf2=300||50||50||50||1000||Y",
    ]


def _set_to_ini(set_lines: list[str]) -> str:
    out = []
    for line in set_lines:
        if "=" in line and "||" in line:
            name, _, rest = line.partition("=")
            parts = rest.split("||")
            if len(parts) == 6:
                out.append(f"{name}={parts[0]}||{parts[3]}||{parts[2]}||{parts[4]}||{parts[5]}")
            else:
                out.append(line)
        else:
            out.append(line)
    return "\n".join(out)


def _build_opti_ini(set_lines, from_date, to_date, report_id):
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod={PERIOD}\nModel=1\n"
        "Optimization=2\nOptimizationCriterion=0\n"
        f"Deposit={DEPOSIT_OPT}\nSpread={SPREAD}\n"
        f"FromDate={from_date.strftime('%Y.%m.%d')}\nToDate={to_date.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{_set_to_ini(set_lines)}\n"
    )


def _build_bt_ini(set_lines, from_date, to_date, report_id):
    clean = []
    for l in set_lines:
        if "=" in l:
            name, _, rhs = l.partition("=")
            clean.append(f"{name.strip()}={rhs.split('||')[0].strip()}")
        else:
            clean.append(l)
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod={PERIOD}\nModel=0\n"
        "Optimization=0\n"
        f"Deposit={DEPOSIT_BT}\nSpread={SPREAD}\n"
        f"FromDate={from_date.strftime('%Y.%m.%d')}\nToDate={to_date.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n" + "\n".join(clean) + "\n"
    )


MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

def _run_mt5(job_id, ini_content, output_dir):
    import shutil
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    ini_path = output_dir / f"{job_id}.ini"
    ini_path.write_text(ini_content, encoding="utf-8")
    print(f"  Launching MT5 ({job_id})...")
    rc = run_mt5_job(MT5_TERMINAL, str(ini_path))
    print(f"  Exit code: {rc}")
    art = find_report_artifact(output_dir, job_id, MT5_TERMINAL)
    # Explicit fallback: check MT5 data root directly
    if art is None:
        for ext in (".xml", ".htm", ".html"):
            candidate = MT5_DATA_ROOT / f"{job_id}{ext}"
            if candidate.exists():
                art = candidate
                break
    if art is None:
        raise RuntimeError(f"No report found for {job_id}")
    collected = copy_report_artifact(art, output_dir)
    print(f"  Collected: {collected.name}")
    return collected


def _parse_xml(xml_path, job_id):
    import pandas as pd
    from zgb_opti.xml_parser import parse_optimization_xml, write_passes
    records, warns = parse_optimization_xml(xml_path, job_id)
    for w in warns:
        print(f"  WARN: {w}")
    parquet_path, _ = write_passes(records, xml_path.parent)
    print(f"  Parsed {len(records)} passes -> {parquet_path.name}")
    return pd.read_parquet(parquet_path)


def _best(df, dd_threshold):
    import pandas as pd
    df = df.copy()
    df["net_profit"]   = pd.to_numeric(df["net_profit"],   errors="coerce")
    df["drawdown_pct"] = pd.to_numeric(df["drawdown_pct"], errors="coerce")
    low_dd = df[df["drawdown_pct"] < dd_threshold]
    pool   = low_dd if not low_dd.empty else df
    return pool.sort_values("net_profit", ascending=False).iloc[0]


def _write_set(set_lines, best_pass, output_path):
    out = []
    for line in set_lines:
        if "=" not in line:
            out.append(line); continue
        name, _, rhs = line.partition("=")
        name = name.strip()
        parts = rhs.strip().split("||")
        if len(parts) >= 6 and parts[-1].strip().upper() == "Y":
            col = f"param_{name}"
            if col in best_pass.index:
                val = best_pass[col]
                try:
                    f = float(val)
                    v = str(int(f)) if f == int(f) else str(f)
                except:
                    v = str(val)
                out.append(f"{name}={v}||{v}||1||{v}||{v}||N")
            else:
                out.append(f"{name}={parts[0]}||{parts[0]}||1||{parts[0]}||{parts[0]}||N")
        else:
            out.append(line)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(out) + "\n", encoding="utf-8")


def main():
    from zgb_opti.xml_parser import parse_forward_report

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 65)
    print(f"  DT818 walk-forward: IS window length comparison")
    print(f"  OPT end : {OPT_END}   OOS: {OPT_END} -> {OOS_END}")
    print(f"  IS lengths: {IS_LENGTHS_WEEKS} weeks")
    print("=" * 65)

    oos_results = {}

    for weeks in IS_LENGTHS_WEEKS:
        is_start = _is_start(weeks)
        tag      = f"{weeks}w"
        print(f"\n{'='*50}")
        print(f"  IS {weeks}w: {is_start} -> {OPT_END}")
        print(f"{'='*50}")

        # ---- Stage 1: genetic optimization ----
        opti_job = f"opti_dt818_{tag}"
        xml_path = OUTPUT_DIR / f"{opti_job}.xml"

        if xml_path.exists():
            print(f"  [Stage 1] Cached: {xml_path.name}")
        else:
            set_lines = _build_set_lines()
            ini = _build_opti_ini(set_lines, is_start, OPT_END, opti_job)
            _run_mt5(opti_job, ini, OUTPUT_DIR)

        df   = _parse_xml(xml_path, opti_job)
        best = _best(df, DD_THRESHOLD)
        print(f"  [Stage 1] Best: NP={float(best.net_profit):+,.0f}  PF={float(best.profit_factor):.3f}  DD={float(best.drawdown_pct):.2f}%  Trades={int(best.trades)}")

        # Write set with RiskPct overridden to 6%
        set_path = OUTPUT_DIR / f"dt818_{tag}.set"
        set_lines = _build_set_lines()
        _write_set(set_lines, best, set_path)
        # Override RiskPct to 6% for fair OOS comparison
        text = set_path.read_text(encoding="utf-8")
        text = "\n".join(
            f"_RiskPct=6.0||6.0||1||6.0||6.0||N" if l.startswith("_RiskPct=") else l
            for l in text.splitlines()
        ) + "\n"
        set_path.write_text(text, encoding="utf-8")
        print(f"  [Set] Written: {set_path.name} (RiskPct forced to 6%)")

        # ---- Stage 2: OOS backtest ----
        oos_job = f"oos_dt818_{tag}"
        oos_report = next(
            (OUTPUT_DIR / f"{oos_job}{ext}" for ext in (".htm", ".html")
             if (OUTPUT_DIR / f"{oos_job}{ext}").exists()), None
        )

        if oos_report:
            print(f"  [OOS] Cached: {oos_report.name}")
        else:
            final_lines = _read_set_lines(set_path)
            bt_ini = _build_bt_ini(final_lines, OPT_END, OOS_END, oos_job)
            oos_report = _run_mt5(oos_job, bt_ini, OUTPUT_DIR)

        metrics, _ = parse_forward_report(oos_report, oos_job)
        oos_results[tag] = metrics
        print(f"  [OOS] NP={float(metrics.get('net_profit',0)):+,.0f}  PF={float(metrics.get('profit_factor',0)):.3f}  DD={float(metrics.get('drawdown_pct',0)):.1f}%  Trades={int(metrics.get('trades',0))}")

    # ---- Summary ----
    print(f"\n{'='*65}")
    print(f"  OOS comparison: {OPT_END} -> {OOS_END} | Every Tick | $10K")
    print(f"  {'IS Length':<12} {'Net Profit':>14} {'PF':>8} {'DD%':>8} {'Trades':>8}")
    print(f"  {'-'*54}")
    for weeks in IS_LENGTHS_WEEKS:
        tag = f"{weeks}w"
        m = oos_results.get(tag, {})
        print(f"  {tag:<12} {float(m.get('net_profit',0)):>+14,.0f} {float(m.get('profit_factor',0)):>8.3f} {float(m.get('drawdown_pct',0)):>7.1f}% {int(m.get('trades',0)):>8}")
    print(f"{'='*65}")

    out_json = OUTPUT_DIR / "walkforward_result.json"
    out_json.write_text(json.dumps({
        tag: {k: float(v) if isinstance(v, (int, float)) else v for k, v in m.items()}
        for tag, m in oos_results.items()
    }, indent=2), encoding="utf-8")
    print(f"\n  Written: {out_json}")


if __name__ == "__main__":
    main()
