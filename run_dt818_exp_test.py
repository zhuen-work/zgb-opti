"""Validation test for DT818_exp (4-stream hybrid) on IS period."""
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
SYMBOL        = "XAUUSD"
SPREAD        = 45
DEPOSIT       = 10_000
RISK_PCT      = 4.0

IS_START = date(2026, 1, 24)
IS_END   = date(2026, 3, 21)

OUTPUT_DIR    = Path("output/dt818_exp_test")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

# TF enum values (MQL5)
TF_M15 = 15
TF_H1  = 16385
TF_H4  = 16388

# S1/S2 best params (H1×H4 reopt)
S1 = dict(tf=TF_H1, ema=2, bb_period=35, bb_dev=1.5, tp=17000, sl=7000)
S2 = dict(tf=TF_H4, ema=2, bb_period=35, bb_dev=1.5, tp=33000, sl=6500)
# S3/S4 best params from DT818_max (DT818_4_m30xh4_mar21_exp_may16.set)
TF_M30 = 30
S3 = dict(tf=TF_M30, ema=5, bars=4, tp=9000,  sl=7000)
S4 = dict(tf=TF_H4,  ema=4, bars=6, tp=12500, sl=8000)

CONFIGS = {
    "exp_s1s2_h1xh4": dict(
        label="S1(H1 BB)+S2(H4 BB)",
        period="H1",
        t1=1, t2=1, t3=0, t4=0,
    ),
    "exp_s3_m30": dict(
        label="S3(M30 Frac) only",
        period="M30",
        t1=0, t2=0, t3=1, t4=0,
    ),
    "exp_s4_h4": dict(
        label="S4(H4 Frac) only",
        period="M30",
        t1=0, t2=0, t3=0, t4=1,
    ),
    "exp_s3s4_m30xh4": dict(
        label="S3+S4 (M30×H4 Frac)",
        period="M30",
        t1=0, t2=0, t3=1, t4=1,
    ),
    "exp_all4": dict(
        label="All 4 streams (M30 chart)",
        period="M30",
        t1=1, t2=1, t3=1, t4=1,
    ),
}


def _build_val_ini(cfg, report_id):
    lines = [
        "_BaseMagic=1000", "_OrderComment=DT818_A", "_OrderComment2=DT818_B",
        "_OrderComment3=DT818_C", "_OrderComment4=DT818_D",
        "_CapitalProtectionAmount=0.0", "_LotMode=1", f"_RiskPct={RISK_PCT}",
        "TierBase=2000", "LotStep=0.01",
        # S1
        f"_Trade1={cfg['t1']}",
        f"_time_frame={S1['tf']}",
        f"_take_profit={S1['tp']}",
        f"_stop_loss={S1['sl']}",
        f"_EMA_Period1={S1['ema']}",
        f"_BB_Period1={S1['bb_period']}",
        f"_BB_Dev1={S1['bb_dev']}",
        "_RiskMode1=0",
        # S2
        f"_Trade2={cfg['t2']}",
        f"_time_frame2={S2['tf']}",
        f"_take_profit2={S2['tp']}",
        f"_stop_loss2={S2['sl']}",
        f"_EMA_Period2={S2['ema']}",
        f"_BB_Period2={S2['bb_period']}",
        f"_BB_Dev2={S2['bb_dev']}",
        "_RiskMode2=0",
        # S3
        f"_Trade3={cfg['t3']}",
        f"_time_frame3={S3['tf']}",
        f"_take_profit3={S3['tp']}",
        f"_stop_loss3={S3['sl']}",
        f"_EMA_Period3={S3['ema']}",
        f"_Bars3={S3['bars']}",
        "_RiskMode3=0",
        # S4
        f"_Trade4={cfg['t4']}",
        f"_time_frame4={S4['tf']}",
        f"_take_profit4={S4['tp']}",
        f"_stop_loss4={S4['sl']}",
        f"_EMA_Period4={S4['ema']}",
        f"_Bars4={S4['bars']}",
        "_RiskMode4=0",
    ]
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert=DT818_exp\nSymbol={SYMBOL}\nPeriod={cfg['period']}\nModel=0\n"
        "Optimization=0\n"
        f"Deposit={DEPOSIT}\nSpread={SPREAD}\n"
        f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n" + "\n".join(lines) + "\n"
    )


def _run_mt5(job_id, ini_content):
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    ini_path = OUTPUT_DIR / f"{job_id}.ini"
    ini_path.write_text(ini_content, encoding="utf-8")
    for _ext in (".htm", ".html", ".xml"):
        _p = OUTPUT_DIR / f"{job_id}{_ext}"
        if _p.exists():
            _p.unlink()
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
    collected = copy_report_artifact(art, OUTPUT_DIR)
    print(f"  Collected: {collected.name}")
    return collected


def main():
    from zgb_opti.xml_parser import parse_forward_report

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("  DT818_exp 4-stream validation")
    print(f"  IS: {IS_START} → {IS_END}   ${DEPOSIT:,}   {RISK_PCT}% risk")
    print(f"  S1/S2 expected: NP=+$30,039  PF=3.930  DD=17.2%  Tr=29")
    print("=" * 70)

    results = {}
    for job_id, cfg in CONFIGS.items():
        print(f"\nRunning: {cfg['label']}")
        htm = _run_mt5(job_id, _build_val_ini(cfg, job_id))
        m, _ = parse_forward_report(htm, job_id)
        results[job_id] = dict(
            label  = cfg["label"],
            np     = float(m.get("net_profit",    0)),
            pf     = float(m.get("profit_factor", 0)),
            dd     = float(m.get("drawdown_pct",  0)),
            trades = int(m.get("trades", 0)),
        )

    print("\n" + "=" * 75)
    print(f"  {'Config':<34}  {'NP':>10}  {'PF':>6}  {'DD':>6}  {'Tr':>4}")
    print("  " + "-" * 65)
    for r in results.values():
        flag = " <-- DD!" if r['dd'] > 20 else ""
        print(f"  {r['label']:<34}  {r['np']:>+10,.0f}  {r['pf']:>6.3f}  {r['dd']:>5.1f}%  {r['trades']:>4}{flag}")
    print(f"  {'[target] S1+S2':<34}  {30039:>+10,}  {'3.930':>6}  {'17.2%':>6}  {29:>4}")
    print(f"  {'[ref]    DT818_max S1+S2 alone':<34}  {41397:>+10,}  {'2.090':>6}  {'11.8%':>6}  {88:>4}")
    print("=" * 75)


if __name__ == "__main__":
    main()
