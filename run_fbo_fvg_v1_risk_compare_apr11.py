"""
Risk comparison ET — apr11 reopt params, IS period Feb 14 -> Apr 11
Runs 2%, 3%, 4%, 5% risk and shows side-by-side results.
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
SYMBOL        = "XAUUSD"
SPREAD        = 45
DEPOSIT       = 10_000
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

IS_START = date(2026, 2, 14)
IS_END   = date(2026, 4, 11)
TAG      = "apr11"

RISK_LEVELS = [2.0, 3.0, 4.0, 5.0]

# Fixed params (non-risk)
BASE_PARAMS = [
    "_BaseMagic=1000", "_CapitalProtectionAmount=0.0",
    "_LotMode=1", "TierBase=2000", "LotStep=0.01",
    "_PendingExpireBars=2",
    "_FBO1=1", "_OrderComment=FBO_A",  "_time_frame=30",
    "_take_profit=8000",  "_stop_loss=5000",  "_Bars=4",  "_EMA_Period1=5",  "_HalfTP1=0.5",
    "_FBO2=1", "_OrderComment2=FBO_B", "_time_frame2=16388",
    "_take_profit2=23000", "_stop_loss2=5000", "_Bars2=4", "_EMA_Period2=5",  "_HalfTP2=0.5",
    "_FBO3=1", "_OrderComment3=FBO_C", "_time_frame3=16385",
    "_take_profit3=18000", "_stop_loss3=7000", "_Bars3=4", "_EMA_Period3=15", "_HalfTP3=0.8",
    "_FVG1=1", "_OrderComment4=FVG_A", "_FVG_TF=16385",
    "_FVG_MinSize=1400", "_FVG_MaxAge=100", "_MaxZones=2",
    "_RR_Ratio=3.5", "_SL_Buffer=30", "_PendingExpireBars_F1=4",
    "_FVG2=1", "_OrderComment5=FVG_B", "_FVG_TF2=16388",
    "_FVG_MinSize2=2000", "_FVG_MaxAge2=200", "_MaxZones2=4",
    "_RR_Ratio2=4.0", "_SL_Buffer2=30", "_PendingExpireBars_F2=1",
]


def _build_ini(report_id, risk_pct):
    params = "\n".join([f"_RiskPct={risk_pct:.1f}"] + BASE_PARAMS)
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert=FBO_FVG_v1\nSymbol={SYMBOL}\nPeriod=M1\nModel=0\nOptimization=0\n"
        f"Deposit={DEPOSIT}\nSpread={SPREAD}\n"
        f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{params}\n"
    )


def run_et(job_id, risk_pct, output_dir):
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    from zgb_opti.xml_parser import parse_forward_report

    output_dir.mkdir(parents=True, exist_ok=True)
    for ext in (".htm", ".html"):
        cached = output_dir / f"{job_id}{ext}"
        if cached.exists():
            print(f"  [Cached] {cached.name}")
            m, _ = parse_forward_report(cached, job_id)
            return m

    ini = output_dir / f"{job_id}.ini"
    ini.write_text(_build_ini(job_id, risk_pct), encoding="utf-8")
    print(f"  Launching: {job_id}")
    run_mt5_job(MT5_TERMINAL, str(ini))

    art = find_report_artifact(output_dir, job_id, MT5_TERMINAL)
    if art is None:
        for ext in (".htm", ".html"):
            c = MT5_DATA_ROOT / f"{job_id}{ext}"
            if c.exists(): art = c; break
    if art is None:
        raise RuntimeError(f"No report: {job_id}")
    art = copy_report_artifact(art, output_dir)
    print(f"  Collected: {art.name}  ({art.stat().st_size/1e6:.1f}MB)")
    m, _ = parse_forward_report(art, job_id)
    return m


def main():
    from zgb_opti.xml_parser import parse_forward_report  # ensure importable

    print("=" * 72)
    print(f"  FBO_FVG_v1 Risk Comparison  (APR11 reopt, IS: {IS_START} -> {IS_END})")
    print(f"  Params: fbo_fvg_v1_Xpct_apr11_reopt_may23.set")
    print(f"  Risk levels: {RISK_LEVELS}")
    print("=" * 72)

    results = []
    for risk in RISK_LEVELS:
        risk_str = f"{int(risk)}pct"
        job_id = f"fbo_fvg_v1_risk_{risk_str}_{TAG}"
        output_dir = Path(f"output/{job_id}")
        print(f"\n  [{risk_str}]  RiskPct={risk:.1f}%")
        m = run_et(job_id, risk, output_dir)
        np_  = float(m.get("net_profit",    0))
        pf   = float(m.get("profit_factor", 0))
        dd   = float(m.get("drawdown_pct",  0))
        tr   = int(  m.get("trades",        0))
        npdd = np_ / dd if dd > 0 else 0.0
        print(f"  NP={np_:+,.0f}  PF={pf:.3f}  DD={dd:.1f}%  Trades={tr}  NP/DD={npdd:,.0f}")
        results.append((risk_str, risk, np_, pf, dd, tr, npdd))

    print(f"\n{'='*72}")
    print(f"  RISK COMPARISON — APR11 params  (IS: {IS_START} -> {IS_END})")
    print(f"  {'Risk':<6}  {'NP':>10}  {'PF':>6}  {'DD':>6}  {'Trades':>7}  {'NP/DD':>8}  {'NP/DD ratio':>12}")
    print(f"  {'-'*6}  {'-'*10}  {'-'*6}  {'-'*6}  {'-'*7}  {'-'*8}  {'-'*12}")
    base_npdd = results[0][6] if results else 1
    for risk_str, risk, np_, pf, dd, tr, npdd in results:
        ratio = npdd / base_npdd if base_npdd else 0
        print(f"  {risk_str:<6}  {np_:>+10,.0f}  {pf:>6.3f}  {dd:>5.1f}%  {tr:>7}  {npdd:>8,.0f}  {ratio:>12.2f}x")
    print("=" * 72)


if __name__ == "__main__":
    main()
