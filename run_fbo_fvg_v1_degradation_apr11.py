"""FBO_FVG_v1 weekly degradation tracker — apr11 params.

Runs weekly forward ETs from OOS start (Apr 11, 2026) through each Friday.
Shows how performance degrades week-over-week to identify when to early-reopt.
Param set: fbo_fvg_v1_3pct_apr11_reopt_may23
Next scheduled reopt: May 23, 2026
"""
from __future__ import annotations

import io, sys
from datetime import date, timedelta
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

MT5_TERMINAL  = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN     = 18912087
MT5_SERVER    = "VantageInternational-Live 3"
EA_PATH       = "FBO_FVG_v1"
SYMBOL        = "XAUUSD"
PERIOD        = "M1"
SPREAD        = 45
DEPOSIT       = 10_000

OOS_START     = date(2026, 4, 11)   # Day after IS end
NEXT_REOPT    = date(2026, 5, 23)   # Scheduled next reopt
TODAY         = date.today()

MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")
OUTPUT_DIR    = Path("output/fbo_fvg_v1_degradation_apr11")

# ── Apr11 reopt best params (8w IS: Feb 14 -> Apr 11) ────────────────────────
PARAMS = "\n".join([
    "_BaseMagic=1000", "_CapitalProtectionAmount=0.0",
    "_RiskPct=3.0", "_LotMode=1", "TierBase=2000", "LotStep=0.01",
    "_PendingExpireBars=2",
    # FBO S1 (M30): TP=8000/SL=5000/Bars=4/EMA=5/HalfTP=0.5
    "_FBO1=1", "_OrderComment=FBO_A", "_time_frame=30",
    "_take_profit=8000", "_stop_loss=5000", "_Bars=4", "_EMA_Period1=5", "_HalfTP1=0.5",
    # FBO S2 (H4): TP=23000/SL=5000/Bars=4/EMA=5/HalfTP=0.5
    "_FBO2=1", "_OrderComment2=FBO_B", "_time_frame2=16388",
    "_take_profit2=23000", "_stop_loss2=5000", "_Bars2=4", "_EMA_Period2=5", "_HalfTP2=0.5",
    # FBO S3 (H1): TP=18000/SL=7000/Bars=4/EMA=15/HalfTP=0.8
    "_FBO3=1", "_OrderComment3=FBO_C", "_time_frame3=16385",
    "_take_profit3=18000", "_stop_loss3=7000", "_Bars3=4", "_EMA_Period3=15", "_HalfTP3=0.8",
    # FVG S1 (H1): MinSize=1400/MaxAge=100/Zones=2/RR=3.5/SLBuf=30/Expiry=4
    "_FVG1=1", "_OrderComment4=FVG_A", "_FVG_TF=16385",
    "_FVG_MinSize=1400", "_FVG_MaxAge=100", "_MaxZones=2",
    "_RR_Ratio=3.5", "_SL_Buffer=30", "_PendingExpireBars_F1=4",
    # FVG S2 (H4): MinSize=2000/MaxAge=200/Zones=4/RR=4.0/SLBuf=30/Expiry=1
    "_FVG2=1", "_OrderComment5=FVG_B", "_FVG_TF2=16388",
    "_FVG_MinSize2=2000", "_FVG_MaxAge2=200", "_MaxZones2=4",
    "_RR_Ratio2=4.0", "_SL_Buffer2=30", "_PendingExpireBars_F2=1",
])


def _next_friday(d: date) -> date:
    """Return the next Friday on or after d."""
    days_ahead = 4 - d.weekday()  # Friday = 4
    if days_ahead < 0:
        days_ahead += 7
    return d + timedelta(days=days_ahead)


def _build_ini(report_id, from_date, to_date):
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod={PERIOD}\nModel=0\nOptimization=0\n"
        f"Deposit={DEPOSIT}\nSpread={SPREAD}\n"
        f"FromDate={from_date.strftime('%Y.%m.%d')}\nToDate={to_date.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{PARAMS}\n"
    )


def _run_et(job_id, from_date, to_date):
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    from zgb_opti.xml_parser import parse_forward_report

    for ext in (".htm", ".html"):
        cached = OUTPUT_DIR / f"{job_id}{ext}"
        if cached.exists():
            m, _ = parse_forward_report(cached, job_id)
            return m

    ini = OUTPUT_DIR / f"{job_id}.ini"
    ini.write_text(_build_ini(job_id, from_date, to_date), encoding="utf-8")
    print(f"  Launching: {job_id}  ({from_date} -> {to_date})")
    run_mt5_job(MT5_TERMINAL, str(ini))

    art = find_report_artifact(OUTPUT_DIR, job_id, MT5_TERMINAL)
    if art is None:
        for ext in (".htm", ".html"):
            c = MT5_DATA_ROOT / f"{job_id}{ext}"
            if c.exists(): art = c; break
    if art is None:
        raise RuntimeError(f"No report: {job_id}")
    art = copy_report_artifact(art, OUTPUT_DIR)
    m, _ = parse_forward_report(art, job_id)
    return m


def _weeks_between(start: date, end: date) -> list[date]:
    """Return list of Fridays from OOS_START through end (inclusive)."""
    fridays = []
    d = _next_friday(start)
    while d <= end:
        fridays.append(d)
        d += timedelta(weeks=1)
    return fridays


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Determine end of data: use today if before next reopt, else next reopt
    oos_end = min(TODAY, NEXT_REOPT)
    if oos_end <= OOS_START:
        print(f"  No OOS data yet (today={TODAY}, OOS starts {OOS_START})")
        return

    weeks = _weeks_between(OOS_START, oos_end)
    if not weeks:
        print(f"  Less than 1 full OOS week available (OOS starts {OOS_START}, today={TODAY})")
        return

    print("=" * 72)
    print(f"  FBO_FVG_v1 Degradation Tracker  (APR11 params)")
    print(f"  OOS: {OOS_START} -> {oos_end}  |  Next reopt: {NEXT_REOPT}")
    print(f"  Weekly windows from {OOS_START} to each Friday through {weeks[-1]}")
    print("=" * 72)

    results = []
    for week_end in weeks:
        job_id = f"degradation_apr11_{week_end.strftime('%b%d').lower()}"
        m = _run_et(job_id, OOS_START, week_end)
        np_  = float(m.get("net_profit",    0))
        pf   = float(m.get("profit_factor", 0))
        dd   = float(m.get("drawdown_pct",  0))
        tr   = int(  m.get("trades",        0))
        npdd = np_ / dd if dd > 0 else 0.0
        days = (week_end - OOS_START).days
        results.append((week_end, days, np_, pf, dd, tr, npdd))
        print(f"  W+{days//7:<2d}  {week_end}  NP={np_:+9,.0f}  PF={pf:.3f}  DD={dd:.1f}%  Tr={tr}  NP/DD={npdd:,.0f}")

    print(f"\n{'='*72}")
    print(f"  DEGRADATION SUMMARY  (APR11 params, OOS start={OOS_START})")
    print(f"  {'Week':<6}  {'End Date':<12}  {'NP':>10}  {'PF':>6}  {'DD':>6}  {'Trades':>7}  {'NP/DD':>8}")
    print(f"  {'-'*6}  {'-'*12}  {'-'*10}  {'-'*6}  {'-'*6}  {'-'*7}  {'-'*8}")
    for (week_end, days, np_, pf, dd, tr, npdd) in results:
        wk = f"W+{days//7}"
        print(f"  {wk:<6}  {str(week_end):<12}  {np_:>+10,.0f}  {pf:>6.3f}  "
              f"{dd:>5.1f}%  {tr:>7}  {npdd:>8,.0f}")
    print("=" * 72)

    # Regime gate check
    if results:
        latest_np, latest_dd = results[-1][2], results[-1][4]
        if latest_dd >= 15.0:
            print(f"\n  *** WARNING: DD={latest_dd:.1f}% >= 15% — consider early reopt ***")
        if results[-1][6] <= 0:
            print(f"\n  *** WARNING: NP/DD <= 0 — strategy in drawdown, monitor closely ***")
        weeks_remaining = (NEXT_REOPT - TODAY).days // 7
        print(f"\n  Days to next reopt ({NEXT_REOPT}): {(NEXT_REOPT - TODAY).days}d  (~{weeks_remaining}w)")


if __name__ == "__main__":
    main()
