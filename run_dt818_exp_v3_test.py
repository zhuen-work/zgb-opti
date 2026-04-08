"""DT818_exp_v3 BETrigger test — compares no-BE baseline vs several BETrigger ratios.

Base params: mar28 best (ET#2, NP/DD=2,761 at 4.9% risk)
  S1: TP=20000/SL=6000/Bars=6/EMA=25/HalfTP=0.7
  S2: TP=22000/SL=7000/Bars=6/EMA=5/HalfTP=0.4
  S3: TP=12000/SL=9000/Bars=8/EMA=10/HalfTP=0.4
IS: Feb 1 -> Mar 28, 2026
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
EA_PATH       = "DT818_exp_v3"
SYMBOL        = "XAUUSD"
PERIOD        = "M30"
SPREAD        = 45
DEPOSIT       = 10_000
RISK_PCT      = 7.0

IS_START = date(2026, 2, 1)
IS_END   = date(2026, 3, 28)
TAG      = "mar28_v3"

OUTPUT_DIR    = Path(f"output/dt818_exp_v3_test")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

TF1_ENUM = 30      # M30
TF2_ENUM = 16388   # H4
TF3_ENUM = 16385   # H1

# Base params (mar28 best)
S1_TP=20000; S1_SL=6000;  S1_BARS=6; S1_EMA=25; S1_HTP=0.7
S2_TP=22000; S2_SL=7000;  S2_BARS=6; S2_EMA=5;  S2_HTP=0.4
S3_TP=12000; S3_SL=9000;  S3_BARS=8; S3_EMA=10; S3_HTP=0.4

# Test cases: (label, be1, be2, be3)
# be=BETrigger ratio (0=disabled, e.g. 0.3 = move SL to BE when profit >= 30% of TP)
TEST_CASES = [
    ("baseline [no BE]",        0.0, 0.0, 0.0),
    ("BE=0.2 all",              0.2, 0.2, 0.2),
    ("BE=0.3 all",              0.3, 0.3, 0.3),
    ("BE=0.4 all",              0.4, 0.4, 0.4),
    ("BE=0.5 all",              0.5, 0.5, 0.5),
    ("BE=0.3 S1 only",          0.3, 0.0, 0.0),
    ("BE=0.3 S2+S3 only",       0.0, 0.3, 0.3),
    ("BE=0.2 S1 / 0.3 S2+S3",  0.2, 0.3, 0.3),
]


def _build_val_ini(be1, be2, be3, report_id):
    lines = "\n".join([
        "_BaseMagic=1000", "_OrderComment=DT818_A", "_OrderComment2=DT818_B",
        "_OrderComment3=DT818_C",
        "_CapitalProtectionAmount=0.0", "_LotMode=1", f"_RiskPct={RISK_PCT}",
        "TierBase=2000", "LotStep=0.01",
        f"_Trade1=1", f"_time_frame={TF1_ENUM}",
        f"_take_profit={S1_TP}", f"_stop_loss={S1_SL}",
        f"_Bars={S1_BARS}", f"_EMA_Period1={S1_EMA}",
        f"_HalfTP1={S1_HTP:.1f}", f"_BETrigger1={be1:.2f}",
        f"_Trade2=1", f"_time_frame2={TF2_ENUM}",
        f"_take_profit2={S2_TP}", f"_stop_loss2={S2_SL}",
        f"_Bars2={S2_BARS}", f"_EMA_Period2={S2_EMA}",
        f"_HalfTP2={S2_HTP:.1f}", f"_BETrigger2={be2:.2f}",
        f"_Trade3=1", f"_time_frame3={TF3_ENUM}",
        f"_take_profit3={S3_TP}", f"_stop_loss3={S3_SL}",
        f"_Bars3={S3_BARS}", f"_EMA_Period3={S3_EMA}",
        f"_HalfTP3={S3_HTP:.1f}", f"_BETrigger3={be3:.2f}",
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

def _run_val(job_id, be1, be2, be3, parse_forward_report):
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    for ext in (".htm", ".html"):
        cached = OUTPUT_DIR / f"{job_id}{ext}"
        if cached.exists():
            print(f"  [Cached] {cached.name}")
            m, _ = parse_forward_report(cached, job_id)
            return dict(np=float(m.get("net_profit", 0)), pf=float(m.get("profit_factor", 0)),
                        dd=float(m.get("drawdown_pct", 0)), trades=int(m.get("trades", 0)))
    ini_path = OUTPUT_DIR / f"{job_id}.ini"
    ini_path.write_text(_build_val_ini(be1, be2, be3, job_id), encoding="utf-8")
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
    return dict(np=float(m.get("net_profit", 0)), pf=float(m.get("profit_factor", 0)),
                dd=float(m.get("drawdown_pct", 0)), trades=int(m.get("trades", 0)))


def main():
    from zgb_opti.xml_parser import parse_forward_report
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 72)
    print(f"  DT818_exp_v3 JumpSL test  |  IS: {IS_START} -> {IS_END}")
    print(f"  Base: S1 TP={S1_TP}/SL={S1_SL}/B={S1_BARS}/EMA={S1_EMA}/HTP={S1_HTP}")
    print(f"        S2 TP={S2_TP}/SL={S2_SL}/B={S2_BARS}/EMA={S2_EMA}/HTP={S2_HTP}")
    print(f"        S3 TP={S3_TP}/SL={S3_SL}/B={S3_BARS}/EMA={S3_EMA}/HTP={S3_HTP}")
    print(f"  Risk: {RISK_PCT}%  Deposit: ${DEPOSIT:,}  Model: Every Tick")
    print(f"  BETrigger: ratio of TP at which SL moves to entry (0=disabled)")
    print("=" * 72)

    results = []
    for idx, (label, be1, be2, be3) in enumerate(TEST_CASES, 1):
        print(f"\n-- Test #{idx}: {label} --")
        job_id = f"v3_test_{idx}_{TAG}"
        m = _run_val(job_id, be1, be2, be3, parse_forward_report)
        np_dd = m["np"] / m["dd"] if m["dd"] > 0 else 0.0
        results.append(dict(idx=idx, label=label, np_dd=np_dd, **m))

    results.sort(key=lambda x: x["np_dd"], reverse=True)

    print(f"\n{'='*72}")
    print(f"  RESULTS  (sorted by NP/DD)")
    print("=" * 72)
    print(f"  {'#':<2}  {'NP':>12}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  {'NP/DD':>8}  Label")
    print("  " + "-" * 70)
    for r in results:
        print(f"  {r['idx']:<2}  {r['np']:>+12,.0f}  {r['pf']:>6.3f}  {r['dd']:>5.1f}%  "
              f"{r['trades']:>4}  {r['np_dd']:>8,.0f}  {r['label']}")
    print("=" * 72)
    print(f"\n  v2 reference (mar28, 7% risk): NP/DD=4,392  NP=+154,466  DD=35.2%")


if __name__ == "__main__":
    main()
