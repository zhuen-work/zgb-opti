"""Sweep S3 TP/SL params (prev-day High/Low breakout stream) on IS period.

S1+S2 fixed at prev best: TP=9000/12500, SL=7000/8000, EMA=5/4, Bars=4/6
S3 varied across TP x SL grid. Selects best by NP, prints full table.
"""
from __future__ import annotations

import io, sys
from datetime import date
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

MT5_TERMINAL  = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN     = 18912087
MT5_SERVER    = "VantageInternational-Live 3"
EA_PATH       = "DT818_max"
SYMBOL        = "XAUUSD"
PERIOD        = "M30"
SPREAD        = 45
DEPOSIT       = 10_000
RISK_PCT      = 4.0

IS_START = date(2026, 1, 24)
IS_END   = date(2026, 3, 21)

OUTPUT_DIR    = Path("output/dt818_s3_study")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

TF1_ENUM = 30
TF2_ENUM = 16388

_S1S2 = {
    "_time_frame": 30,    "_take_profit": 9000,   "_stop_loss": 7000,
    "_EMA_Period1": 5,    "_Bars": 4,              "_RiskMode1": 0,
    "_time_frame2": 16388, "_take_profit2": 12500, "_stop_loss2": 8000,
    "_EMA_Period2": 4,    "_Bars2": 6,             "_RiskMode2": 0,
}

# Grid to sweep
TP3_VALUES = [5000, 7000, 9000, 11000, 13000, 15000, 17000, 20000, 25000, 30000, 35000, 40000]
SL3_VALUES = [5000, 7000, 9000, 11000]


def _build_ini(tp3, sl3, report_id):
    p = _S1S2
    lines = "\n".join([
        "_BaseMagic=1000", "_OrderComment=DT818_A", "_OrderComment2=DT818_B",
        "_CapitalProtectionAmount=0.0", "_LotMode=1", f"_RiskPct={RISK_PCT}",
        "TierBase=2000", "LotStep=0.01", "_Trade1=1",
        f"_time_frame={p['_time_frame']}",
        f"_take_profit={p['_take_profit']}", f"_stop_loss={p['_stop_loss']}",
        f"_EMA_Period1={p['_EMA_Period1']}", f"_Bars={p['_Bars']}",
        f"_RiskMode1={p['_RiskMode1']}", "_Trade2=1",
        f"_time_frame2={p['_time_frame2']}",
        f"_take_profit2={p['_take_profit2']}", f"_stop_loss2={p['_stop_loss2']}",
        f"_EMA_Period2={p['_EMA_Period2']}", f"_Bars2={p['_Bars2']}",
        f"_RiskMode2={p['_RiskMode2']}",
        "_Trade3=1", "_OrderComment3=DT818_C",
        f"_take_profit3={tp3}", f"_stop_loss3={sl3}", "_RiskMode3=0",
    ])
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod={PERIOD}\nModel=0\n"
        "Optimization=0\n"
        f"Deposit={DEPOSIT}\nSpread={SPREAD}\n"
        f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{lines}\n"
    )


def _run_mt5(job_id, ini_content):
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    ini_path = OUTPUT_DIR / f"{job_id}.ini"
    ini_path.write_text(ini_content, encoding="utf-8")
    rc = run_mt5_job(MT5_TERMINAL, str(ini_path))
    art = find_report_artifact(OUTPUT_DIR, job_id, MT5_TERMINAL)
    if art is None:
        for ext in (".htm", ".html"):
            c = MT5_DATA_ROOT / f"{job_id}{ext}"
            if c.exists(): art = c; break
    if art is None:
        raise RuntimeError(f"No report found for {job_id}")
    return copy_report_artifact(art, OUTPUT_DIR)


def main():
    from zgb_opti.xml_parser import parse_forward_report

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 65)
    print(f"  S3 TP/SL sweep  —  IS {IS_START} → {IS_END}")
    print(f"  S1+S2 fixed (prev best). S3 = prev-day High/Low breakout")
    print(f"  TP3: {TP3_VALUES}")
    print(f"  SL3: {SL3_VALUES}")
    print(f"  Baseline S1+S2 only: NP=+$41,397  PF=2.090  DD=11.8%  Tr=88")
    print("=" * 65)

    results = []
    total = len(TP3_VALUES) * len(SL3_VALUES)
    idx = 0
    for tp3 in TP3_VALUES:
        for sl3 in SL3_VALUES:
            idx += 1
            job_id = f"s3_tp{tp3}_sl{sl3}"
            htm = OUTPUT_DIR / f"{job_id}.htm"
            if htm.exists():
                print(f"  [{idx:2d}/{total}] [cached] TP3={tp3}  SL3={sl3}")
            else:
                print(f"  [{idx:2d}/{total}] Running  TP3={tp3}  SL3={sl3} ...", end=" ", flush=True)
                _run_mt5(job_id, _build_ini(tp3, sl3, job_id))

            m, _ = parse_forward_report(htm, job_id)
            np_v = float(m.get("net_profit",    0))
            pf_v = float(m.get("profit_factor", 0))
            dd_v = float(m.get("drawdown_pct",  0))
            tr_v = int(m.get("trades", 0))
            print(f"NP={np_v:+,.0f}  PF={pf_v:.3f}  DD={dd_v:.1f}%  Tr={tr_v}")
            results.append((tp3, sl3, np_v, pf_v, dd_v, tr_v))

    results.sort(key=lambda x: x[2], reverse=True)

    print(f"\n{'=' * 65}")
    print(f"  {'TP3':>6}  {'SL3':>6}  {'NP':>10}  {'PF':>6}  {'DD':>6}  {'Tr':>4}")
    print("  " + "-" * 55)
    for tp3, sl3, np_v, pf_v, dd_v, tr_v in results:
        marker = " ◄" if (tp3, sl3) == (results[0][0], results[0][1]) else ""
        print(f"  {tp3:>6}  {sl3:>6}  {np_v:>+10,.0f}  {pf_v:>6.3f}  {dd_v:>5.1f}%  {tr_v:>4}{marker}")
    print("=" * 65)
    best = results[0]
    print(f"\n  Best S3: TP3={best[0]}  SL3={best[1]}  NP={best[2]:+,.0f}  PF={best[3]:.3f}  DD={best[4]:.1f}%  Tr={best[5]}")


if __name__ == "__main__":
    main()
