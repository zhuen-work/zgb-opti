"""DT818 re-optimization: 8-week IS ending Mar 21, 2026.

IS  : 2026-01-24 -> 2026-03-21  (8 weeks)
Algo: Genetic (Optimization=2), Model=1 (1m OHLC), Deposit=$10M
Output: configs/sets/dt818_best_8w_mar21.set  (RiskPct=6%)
"""
from __future__ import annotations

import io, sys, json
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
DD_THRESHOLD = 20.0

IS_START = date(2026, 1, 24)
IS_END   = date(2026, 3, 21)

OUTPUT_DIR  = Path("output/dt818_reopt_mar21")
SET_OUT     = Path("configs/sets/dt818_best_8w_mar21.set")

MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")


def _build_set_lines() -> list[str]:
    return [
        "_BaseMagic=1000||1000||1||1000||1000||N",
        "_OrderComment=DT818_EA",
        "_CapitalProtectionAmount=0.0||0.0||1||0.0||0.0||N",
        "_LotMode=1||1||1||1||1||N",
        "_RiskPct=3.0||3.0||1||3.0||3.0||N",
        "TierBase=2000||2000||1||2000||2000||N",
        "LotStep=0.01||0.01||1||0.01||0.01||N",
        "_Trade1=1||1||1||1||1||N",
        "_time_frame=30||30||1||30||30||N",
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
        "_time_frame2=30||30||1||30||30||N",
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


def _set_to_ini(set_lines):
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


def _run_mt5(job_id, ini_content, output_dir):
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    ini_path = output_dir / f"{job_id}.ini"
    ini_path.write_text(ini_content, encoding="utf-8")
    print(f"  Launching MT5 ({job_id})...")
    rc = run_mt5_job(MT5_TERMINAL, str(ini_path))
    print(f"  Exit code: {rc}")
    art = find_report_artifact(output_dir, job_id, MT5_TERMINAL)
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
    import pandas as pd
    from zgb_opti.xml_parser import parse_optimization_xml, write_passes

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print(f"  DT818 re-optimization: 8w IS ending Mar 21, 2026")
    print(f"  IS: {IS_START} -> {IS_END}")
    print("=" * 60)

    job_id   = "opti_dt818_8w_mar21"
    xml_path = OUTPUT_DIR / f"{job_id}.xml"

    if xml_path.exists():
        print(f"  [Cached] {xml_path.name}")
    else:
        set_lines = _build_set_lines()
        ini = _build_opti_ini(set_lines, IS_START, IS_END, job_id)
        _run_mt5(job_id, ini, OUTPUT_DIR)

    # Parse
    records, warns = parse_optimization_xml(xml_path, job_id)
    for w in warns:
        print(f"  WARN: {w}")
    parquet_path, _ = write_passes(records, xml_path.parent)
    print(f"  Parsed {len(records)} passes")

    df   = pd.read_parquet(parquet_path)
    best = _best(df, DD_THRESHOLD)
    print(f"\n  Best pass:")
    print(f"    NP={float(best.net_profit):+,.0f}  PF={float(best.profit_factor):.3f}  DD={float(best.drawdown_pct):.2f}%  Trades={int(best.trades)}")

    # Write set with RiskPct=6%
    set_lines = _build_set_lines()
    _write_set(set_lines, best, SET_OUT)
    text = SET_OUT.read_text(encoding="utf-8")
    text = "\n".join(
        "_RiskPct=6.0||6.0||1||6.0||6.0||N" if l.startswith("_RiskPct=") else l
        for l in text.splitlines()
    ) + "\n"
    SET_OUT.write_text(text, encoding="utf-8")
    print(f"\n  Written: {SET_OUT}")
    print(f"\n  Next re-optimization date: 2026-05-16  (8 weeks from Mar 21)")
    print("=" * 60)


if __name__ == "__main__":
    main()
