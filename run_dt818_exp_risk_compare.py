"""ET#2 params at varying risk levels: 4%, 6%, 8%, 10%, 12%.
IS: Jan 24 -> Mar 21, 2026.  Every Tick.  $10,000 deposit.
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
EA_PATH       = "DT818_exp"
SYMBOL        = "XAUUSD"
SPREAD        = 45
DEPOSIT       = 10_000

IS_START = date(2026, 1, 24)
IS_END   = date(2026, 3, 21)

OUTPUT_DIR    = Path("output/dt818_exp_risk_compare_3s_et1")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

TF1 = 30     # M30
TF2 = 16388  # H4
TF3 = 16385  # H1

# 3-stream ET#1 params
TP1, SL1, EMA1, BARS1 = 14500, 10500, 4, 4
TP2, SL2, EMA2, BARS2 = 20000,  8500, 4, 6
TP3, SL3, EMA3, BARS3 = 22500,  7000, 9, 6

TSL1, TSLA1, BET1, BEB1 =  500,  200, 2500, 450
TSL2, TSLA2, BET2, BEB2 = 4500, 1000,  500, 300
TSL3, TSLA3, BET3, BEB3 = 2000,  700, 2000, 700

RISK_LEVELS = [5, 7]


def _build_ini(risk_pct, report_id):
    lines = "\n".join([
        "_BaseMagic=1000", "_OrderComment=DT818_A", "_OrderComment2=DT818_B",
        "_OrderComment3=DT818_C",
        "_CapitalProtectionAmount=0.0", "_LotMode=1", f"_RiskPct={risk_pct}",
        "TierBase=2000", "LotStep=0.01",
        "_Trade1=1", f"_time_frame={TF1}",
        f"_take_profit={TP1}", f"_stop_loss={SL1}",
        f"_EMA_Period1={EMA1}", f"_Bars={BARS1}",
        "_RiskMode1=0",
        f"_TSL1={TSL1}", f"_TSLA1={TSLA1}", f"_BETrigger1={BET1}", f"_BEBuf1={BEB1}",
        "_Trade2=1", f"_time_frame2={TF2}",
        f"_take_profit2={TP2}", f"_stop_loss2={SL2}",
        f"_EMA_Period2={EMA2}", f"_Bars2={BARS2}",
        "_RiskMode2=0",
        f"_TSL2={TSL2}", f"_TSLA2={TSLA2}", f"_BETrigger2={BET2}", f"_BEBuf2={BEB2}",
        "_Trade3=1", f"_time_frame3={TF3}",
        f"_take_profit3={TP3}", f"_stop_loss3={SL3}",
        f"_EMA_Period3={EMA3}", f"_Bars3={BARS3}",
        "_RiskMode3=0",
        f"_TSL3={TSL3}", f"_TSLA3={TSLA3}", f"_BETrigger3={BET3}", f"_BEBuf3={BEB3}",
    ])
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod=M30\nModel=0\n"
        "Optimization=0\n"
        f"Deposit={DEPOSIT}\nSpread={SPREAD}\n"
        f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{lines}\n"
    )


def _run(job_id, risk_pct):
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    from zgb_opti.xml_parser import parse_forward_report

    for ext in (".htm", ".html"):
        cached = OUTPUT_DIR / f"{job_id}{ext}"
        if cached.exists():
            print(f"  [Cached] {cached.name}")
            m, _ = parse_forward_report(cached, job_id)
            return m

    ini_path = OUTPUT_DIR / f"{job_id}.ini"
    ini_path.write_text(_build_ini(risk_pct, job_id), encoding="utf-8")
    print(f"  Launching: {job_id}")
    rc = run_mt5_job(MT5_TERMINAL, str(ini_path))
    print(f"  Exit code: {rc}")
    art = find_report_artifact(OUTPUT_DIR, job_id, MT5_TERMINAL)
    if art is None:
        for ext in (".htm", ".html"):
            c = MT5_DATA_ROOT / f"{job_id}{ext}"
            if c.exists(): art = c; break
    if art is None:
        raise RuntimeError(f"No report found for {job_id}")
    htm = copy_report_artifact(art, OUTPUT_DIR)
    m, _ = parse_forward_report(htm, job_id)
    return m


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 68)
    print(f"  DT818_exp 3-stream ET#1 — risk comparison")
    print(f"  IS: {IS_START} -> {IS_END}   ${DEPOSIT:,}   Every Tick")
    print(f"  S1: {TP1}/{SL1}/EMA{EMA1}/B{BARS1}   S2: {TP2}/{SL2}/EMA{EMA2}/B{BARS2}   S3: {TP3}/{SL3}/EMA{EMA3}/B{BARS3}")
    print("=" * 68)

    results = []
    for risk in RISK_LEVELS:
        job_id = f"risk_{risk}pct"
        print(f"\nRunning: {risk}% risk")
        m = _run(job_id, risk)
        np_v  = float(m.get("net_profit",    0))
        pf_v  = float(m.get("profit_factor", 0))
        dd_v  = float(m.get("drawdown_pct",  0))
        tr_v  = int(m.get("trades", 0))
        np_dd = np_v / dd_v if dd_v > 0 else 0.0
        results.append(dict(risk=risk, np=np_v, pf=pf_v, dd=dd_v, trades=tr_v, np_dd=np_dd))

    print("\n" + "=" * 68)
    print(f"  {'Risk':<6}  {'NP':>10}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  {'NP/DD':>8}")
    print("  " + "-" * 50)
    for r in results:
        print(f"  {r['risk']:<5}%  {r['np']:>+10,.0f}  {r['pf']:>6.3f}  {r['dd']:>5.1f}%  {r['trades']:>4}  {r['np_dd']:>8,.0f}")
    print("=" * 68)


if __name__ == "__main__":
    main()
