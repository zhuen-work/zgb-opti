"""FVG S1+S2 (H1+H4) re-optimization — 2-phase: optimize S2 with S1 fixed, then ET.

FBO streams all disabled.
FVG S1 (H1) fixed at apr11 best params.
FVG S2 (H4) swept: MinSize2/MaxAge2/MaxZones2/RR_Ratio2/SL_Buffer2/PendingExpireBars_F2
IS: Feb 14 -> Apr 11, 2026  (8w)
Next reopt: May 23, 2026
Output set: configs/sets/fvg_ea_s2_3pct_h1h4_apr11_reopt_may23.set
"""
from __future__ import annotations

import io, sys
from datetime import date
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

DD_THRESHOLD  = 30.0
TOP_N         = 5

MT5_TERMINAL  = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN     = 18912087
MT5_SERVER    = "VantageInternational-Live 3"
EA_PATH       = "FBO_FVG_v2"
SYMBOL        = "XAUUSD"
PERIOD        = "M5"
SPREAD        = 45
DEPOSIT_OPT   = 10_000
DEPOSIT_VAL   = 10_000
RISK_PCT      = 3.0

TF_H1  = 16385   # H1
TF_H4  = 16388   # H4

IS_START = date(2026, 2, 14)
IS_END   = date(2026, 4, 11)
IS_TAG   = "apr11"

OUTPUT_DIR    = Path("output/fvg_v2_reopt_s2_apr11")
SET_OUT       = Path("configs/sets/fvg_v2_s2_3pct_h1h4_apr11_reopt.set")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

# ── FVG S1 fixed params (from run_fvg_ea_reopt.py best ET) ───────────────────
S1_MINSIZE = 1600
S1_MAXAGE  = 250
S1_ZONES   = 2
S1_RR      = 3.5
S1_SLBUF   = 40
S1_EXPIRY  = 7

# ── Sweep ranges for S2 ───────────────────────────────────────────────────────
MINSIZE_LO = 1200;  MINSIZE_HI = 2400; MINSIZE_STEP = 200   # was 800-3000; narrowed
MAXAGE_LO  = 100;   MAXAGE_HI  = 250;  MAXAGE_STEP  = 50   # was 50-300; narrowed
ZONES_LO   = 2;     ZONES_HI   = 4;    ZONES_STEP   = 1    # was 1-6; aligned with S1 (2-4)
RR_LO      = 3.0;   RR_HI      = 5.0;  RR_STEP      = 0.5  # was 2.0-5.0; drop 2.0,2.5
SLBUF_LO   = 20;    SLBUF_HI   = 50;   SLBUF_STEP   = 10   # was 20-60; drop 60
EXPIRY_LO  = 1;     EXPIRY_HI  = 7;    EXPIRY_STEP  = 2    # was 1-8 step 1; step 2 (1,3,5,7)

PASSES = (
    len(range(MINSIZE_LO, MINSIZE_HI + 1, MINSIZE_STEP)) *
    len(range(MAXAGE_LO,  MAXAGE_HI  + 1, MAXAGE_STEP))  *
    len(range(ZONES_LO,   ZONES_HI   + 1, ZONES_STEP))   *
    len([x * RR_STEP + RR_LO for x in range(int((RR_HI - RR_LO) / RR_STEP) + 1)]) *
    len(range(SLBUF_LO,  SLBUF_HI  + 1, SLBUF_STEP))  *
    len(range(EXPIRY_LO, EXPIRY_HI  + 1, EXPIRY_STEP))
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fix(name, val):
    return f"{name}={val}||{val}||1||{val}||{val}||N"

def _sweep(name, default, step, lo, hi):
    return f"{name}={default}||{default}||{step}||{lo}||{hi}||Y"

def _set_to_ini(lines):
    out = []
    for line in lines:
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

def _build_opti_ini(set_lines, report_id):
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod={PERIOD}\nModel=1\n"
        "Optimization=1\nOptimizationCriterion=0\n"
        f"Deposit={DEPOSIT_OPT}\nSpread={SPREAD}\n"
        f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{_set_to_ini(set_lines)}\n"
    )

def _run_mt5(job_id, ini_content):
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    ini_path = OUTPUT_DIR / f"{job_id}.ini"
    ini_path.write_text(ini_content, encoding="utf-8")
    print(f"  Launching: {job_id}")
    rc = run_mt5_job(MT5_TERMINAL, str(ini_path))
    print(f"  Exit code: {rc}")
    art = find_report_artifact(OUTPUT_DIR, job_id, MT5_TERMINAL)
    if art is None:
        for ext in (".xml", ".htm", ".html"):
            c = MT5_DATA_ROOT / f"{job_id}{ext}"
            if c.exists(): art = c; break
    if art is None:
        raise RuntimeError(f"No report found for {job_id}")
    collected = copy_report_artifact(art, OUTPUT_DIR)
    print(f"  Collected: {collected.name}  ({collected.stat().st_size/1e6:.1f}MB)")
    return collected

def _vi(row, col, d=0):
    try:    return int(float(row.get(col, d)))
    except: return int(d)

def _vf(row, col, d=0.0):
    try:    return float(row.get(col, d))
    except: return float(d)


def _common_lines():
    return [
        _fix("_BaseMagic", 1000),
        _fix("_CapitalProtectionAmount", "0.0"),
        _fix("_LotMode", 1), _fix("_RiskPct", f"{RISK_PCT}"),
        _fix("TierBase", 2000), _fix("LotStep", "0.01"),
        # FBO all off
        _fix("_FBO1", 0), _fix("_FBO2", 0), _fix("_FBO3", 0),
        "_OrderComment=FBO_A",
        _fix("_time_frame", 30), _fix("_take_profit", 10000), _fix("_stop_loss", 8000),
        _fix("_Bars", 4), _fix("_EMA_Period1", 5), _fix("_HalfTP1", "0.5"),
        _fix("_PendingExpireBars", 2),
        "_OrderComment2=FBO_B",
        _fix("_time_frame2", 16388), _fix("_take_profit2", 10000), _fix("_stop_loss2", 8000),
        _fix("_Bars2", 4), _fix("_EMA_Period2", 5), _fix("_HalfTP2", "0.5"),
        "_OrderComment3=FBO_C",
        _fix("_time_frame3", 16385), _fix("_take_profit3", 10000), _fix("_stop_loss3", 8000),
        _fix("_Bars3", 4), _fix("_EMA_Period3", 5), _fix("_HalfTP3", "0.5"),
        # FVG S1 fixed
        _fix("_FVG1", 1),
        "_OrderComment4=FVG_A",
        _fix("_FVG_TF", TF_H1),
        _fix("_FVG_MinSize", S1_MINSIZE),
        _fix("_FVG_MaxAge", S1_MAXAGE),
        _fix("_MaxZones", S1_ZONES),
        _fix("_RR_Ratio", f"{S1_RR:.1f}"),
        _fix("_SL_Buffer", S1_SLBUF),
        _fix("_PendingExpireBars_F1", S1_EXPIRY),
    ]

def _s2_sweep_lines():
    return [
        _fix("_FVG2", 1),
        "_OrderComment5=FVG_B",
        _fix("_FVG_TF2", TF_H4),
        _sweep("_FVG_MinSize2",         MINSIZE_LO, MINSIZE_STEP, MINSIZE_LO, MINSIZE_HI),
        _sweep("_FVG_MaxAge2",          MAXAGE_LO,  MAXAGE_STEP,  MAXAGE_LO,  MAXAGE_HI),
        _sweep("_MaxZones2",            ZONES_LO,   ZONES_STEP,   ZONES_LO,   ZONES_HI),
        _sweep("_RR_Ratio2",            RR_LO,      RR_STEP,      RR_LO,      RR_HI),
        _sweep("_SL_Buffer2",           SLBUF_LO,   SLBUF_STEP,   SLBUF_LO,   SLBUF_HI),
        _sweep("_PendingExpireBars_F2", EXPIRY_LO,  EXPIRY_STEP,  EXPIRY_LO,  EXPIRY_HI),
    ]


def _build_et_ini(report_id, minsize2, maxage2, zones2, rr2, slbuf2, expiry2):
    lines = "\n".join([
        "_BaseMagic=1000",
        "_CapitalProtectionAmount=0.0",
        f"_RiskPct={RISK_PCT}",
        "_LotMode=1",
        "TierBase=2000", "LotStep=0.01",
        "_FBO1=0", "_FBO2=0", "_FBO3=0",
        "_OrderComment=FBO_A",
        "_time_frame=30", "_take_profit=10000", "_stop_loss=8000",
        "_Bars=4", "_EMA_Period1=5", "_HalfTP1=0.5", "_PendingExpireBars=2",
        "_OrderComment2=FBO_B",
        "_time_frame2=16388", "_take_profit2=10000", "_stop_loss2=8000",
        "_Bars2=4", "_EMA_Period2=5", "_HalfTP2=0.5",
        "_OrderComment3=FBO_C",
        "_time_frame3=16385", "_take_profit3=10000", "_stop_loss3=8000",
        "_Bars3=4", "_EMA_Period3=5", "_HalfTP3=0.5",
        "_FVG1=1",
        "_OrderComment4=FVG_A",
        f"_FVG_TF={TF_H1}",
        f"_FVG_MinSize={S1_MINSIZE}",
        f"_FVG_MaxAge={S1_MAXAGE}",
        f"_MaxZones={S1_ZONES}",
        f"_RR_Ratio={S1_RR:.1f}",
        f"_SL_Buffer={S1_SLBUF}",
        f"_PendingExpireBars_F1={S1_EXPIRY}",
        "_FVG2=1",
        "_OrderComment5=FVG_B",
        f"_FVG_TF2={TF_H4}",
        f"_FVG_MinSize2={minsize2}",
        f"_FVG_MaxAge2={maxage2}",
        f"_MaxZones2={zones2}",
        f"_RR_Ratio2={rr2:.1f}",
        f"_SL_Buffer2={slbuf2}",
        f"_PendingExpireBars_F2={expiry2}",
    ])
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod={PERIOD}\nModel=0\n"
        "Optimization=0\n"
        f"Deposit={DEPOSIT_VAL}\nSpread={SPREAD}\n"
        f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{lines}\n"
    )


def _run_et(job_id, minsize2, maxage2, zones2, rr2, slbuf2, expiry2, parse_forward_report):
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
    ini_path.write_text(_build_et_ini(job_id, minsize2, maxage2, zones2, rr2, slbuf2, expiry2),
                        encoding="utf-8")
    print(f"  Launching: {job_id}")
    run_mt5_job(MT5_TERMINAL, str(ini_path))
    art = find_report_artifact(OUTPUT_DIR, job_id, MT5_TERMINAL)
    if art is None:
        for ext in (".htm", ".html"):
            c = MT5_DATA_ROOT / f"{job_id}{ext}"
            if c.exists(): art = c; break
    if art is None:
        raise RuntimeError(f"No ET report: {job_id}")
    htm = copy_report_artifact(art, OUTPUT_DIR)
    m, _ = parse_forward_report(htm, job_id)
    return dict(np=float(m.get("net_profit", 0)), pf=float(m.get("profit_factor", 0)),
                dd=float(m.get("drawdown_pct", 0)), trades=int(m.get("trades", 0)))


def main():
    import pandas as pd
    from zgb_opti.xml_parser import parse_optimization_xml, parse_forward_report, write_passes

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 68)
    print(f"  FVG S1+S2 (H1+H4) re-optimization  ({IS_TAG.upper()})")
    print(f"  IS: {IS_START} -> {IS_END}  EA={EA_PATH}")
    print(f"  S1 fixed: MinSz={S1_MINSIZE}  Age={S1_MAXAGE}  Zn={S1_ZONES}  "
          f"RR={S1_RR}  SLBuf={S1_SLBUF}  Exp={S1_EXPIRY}")
    print(f"  S2 sweep: {PASSES:,} passes  |  DD gate: {DD_THRESHOLD}%")
    print(f"  Output: {SET_OUT}")
    print("=" * 68)

    # ── Phase 1: Optimize FVG S2 with S1 fixed ───────────────────────────────
    print(f"\n  PHASE 1: FVG S2 sweep  ({PASSES:,} passes)")

    job_p1 = f"fvg_s2_opti_{IS_TAG}"
    xml_p1 = OUTPUT_DIR / f"{job_p1}.xml"
    p1_key_cols = [
        "param__FVG_MinSize2", "param__FVG_MaxAge2", "param__MaxZones2",
        "param__RR_Ratio2", "param__SL_Buffer2",
    ]

    if xml_p1.exists():
        print(f"  [Cached] {xml_p1.name}")
    else:
        set_lines = _common_lines() + _s2_sweep_lines()
        _run_mt5(job_p1, _build_opti_ini(set_lines, job_p1))

    records, warns = parse_optimization_xml(xml_p1, job_p1)
    for w in warns: print(f"  WARN: {w}")
    parquet, _ = write_passes(records, OUTPUT_DIR)
    print(f"  Parsed {len(records)} passes")

    df = pd.read_parquet(parquet)
    df["net_profit"]   = pd.to_numeric(df["net_profit"],   errors="coerce")
    df["drawdown_pct"] = pd.to_numeric(df["drawdown_pct"], errors="coerce")
    pool = df[df["drawdown_pct"] < DD_THRESHOLD]
    pool = pool if not pool.empty else df

    # Resolve PendingExpireBars_F2 column name (handles cache naming mismatch)
    p1_expiry_key = (
        next((c for c in ["param__PendingExpireBars_F2", "param__PendingExpireBars2"]
              if c in pool.columns), None)
        or next((c for c in pool.columns if "PendingExpireBars" in c and "2" in c), None)
    )
    if p1_expiry_key:
        p1_key_cols_full = p1_key_cols + [p1_expiry_key]
    else:
        p1_key_cols_full = p1_key_cols

    dedup_cols = [c for c in p1_key_cols_full if c in pool.columns]
    top5 = (pool.sort_values("net_profit", ascending=False)
                .drop_duplicates(subset=dedup_cols)
                .drop_duplicates(subset=["net_profit"])
                .head(TOP_N))

    print(f"\n  TOP {TOP_N} passes:")
    print(f"  {'#':<3}  {'NP':>12}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  "
          f"{'MinSz':>6}  {'Age':>5}  {'Zn':>3}  {'RR':>5}  {'SLBuf':>6}  {'Exp':>4}")
    print("  " + "-" * 76)
    for rank, (_, row) in enumerate(top5.iterrows(), 1):
        expiry_val = _vi(row, p1_expiry_key, 1) if p1_expiry_key else 1
        print(f"  {rank:<3}  {float(row.get('net_profit',0)):>+12,.0f}  "
              f"{float(row.get('profit_factor',0)):>6.3f}  {float(row.get('drawdown_pct',0)):>5.1f}%  "
              f"{_vi(row,'trades',0):>4}  "
              f"{_vi(row,'param__FVG_MinSize2',2000):>6}  "
              f"{_vi(row,'param__FVG_MaxAge2',200):>5}  "
              f"{_vi(row,'param__MaxZones2',4):>3}  "
              f"{_vf(row,'param__RR_Ratio2',4.0):>5.1f}  "
              f"{_vi(row,'param__SL_Buffer2',30):>6}  "
              f"{expiry_val:>4}")

    # ── Phase 2: ET validation on top-N ──────────────────────────────────────
    print(f"\n{'='*68}")
    print(f"  PHASE 2: ET validation  ${DEPOSIT_VAL:,}  {RISK_PCT:.0f}% risk")
    print("=" * 68)

    et_results = []
    for rank, (_, row) in enumerate(top5.iterrows(), 1):
        minsize2 = _vi(row, "param__FVG_MinSize2", 2000)
        maxage2  = _vi(row, "param__FVG_MaxAge2",   200)
        zones2   = _vi(row, "param__MaxZones2",       4)
        rr2      = _vf(row, "param__RR_Ratio2",     4.0)
        slbuf2   = _vi(row, "param__SL_Buffer2",     30)
        expiry2  = _vi(row, p1_expiry_key, 1) if p1_expiry_key else 1

        print(f"\n-- ET #{rank}  MinSz={minsize2}  Age={maxage2}  Zn={zones2}  "
              f"RR={rr2:.1f}  SLBuf={slbuf2}  Exp={expiry2} --")
        m = _run_et(f"fvg_s2_et_{rank}_{IS_TAG}", minsize2, maxage2, zones2, rr2, slbuf2, expiry2,
                    parse_forward_report)
        et_results.append(dict(rank=rank, minsize2=minsize2, maxage2=maxage2, zones2=zones2,
                               rr2=rr2, slbuf2=slbuf2, expiry2=expiry2, **m))

    for r in et_results:
        r["np_dd"] = r["np"] / r["dd"] if r["dd"] > 0 else 0.0
    et_results.sort(key=lambda x: x["np_dd"], reverse=True)

    print(f"\n  {'ET#':<4}  {'NP':>12}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  {'NP/DD':>8}  "
          f"{'MinSz':>6}  {'Age':>5}  {'Zn':>3}  {'RR':>5}  {'Exp':>4}")
    print("  " + "-" * 80)
    for r in et_results:
        print(f"  {r['rank']:<4}  {r['np']:>+12,.0f}  {r['pf']:>6.3f}  {r['dd']:>5.1f}%  "
              f"{r['trades']:>4}  {r['np_dd']:>8,.0f}  "
              f"{r['minsize2']:>6}  {r['maxage2']:>5}  {r['zones2']:>3}  "
              f"{r['rr2']:>5.1f}  {r['expiry2']:>4}")

    # Write best set (S1+S2 combined)
    best = et_results[0]
    set_lines = [
        "_BaseMagic=1000||1000||1||1000||1000||N",
        "_CapitalProtectionAmount=0.0||0.0||1||0.0||0.0||N",
        "_FBO1=0||0||1||0||0||N", "_FBO2=0||0||1||0||0||N", "_FBO3=0||0||1||0||0||N",
        "_FVG1=1||1||1||1||1||N",
        "_FVG2=1||1||1||1||1||N",
        "_LotMode=1||1||1||1||1||N",
        f"_RiskPct={RISK_PCT}||{RISK_PCT}||1||{RISK_PCT}||{RISK_PCT}||N",
        "TierBase=2000||2000||1||2000||2000||N",
        "LotStep=0.01||0.01||1||0.01||0.01||N",
        "_OrderComment4=FVG_A",
        f"_FVG_TF={TF_H1}||{TF_H1}||1||{TF_H1}||{TF_H1}||N",
        f"_FVG_MinSize={S1_MINSIZE}||{S1_MINSIZE}||1||{S1_MINSIZE}||{S1_MINSIZE}||N",
        f"_FVG_MaxAge={S1_MAXAGE}||{S1_MAXAGE}||1||{S1_MAXAGE}||{S1_MAXAGE}||N",
        f"_MaxZones={S1_ZONES}||{S1_ZONES}||1||{S1_ZONES}||{S1_ZONES}||N",
        f"_RR_Ratio={S1_RR:.1f}||{S1_RR:.1f}||1||{S1_RR:.1f}||{S1_RR:.1f}||N",
        f"_SL_Buffer={S1_SLBUF}||{S1_SLBUF}||1||{S1_SLBUF}||{S1_SLBUF}||N",
        f"_PendingExpireBars_F1={S1_EXPIRY}||{S1_EXPIRY}||1||{S1_EXPIRY}||{S1_EXPIRY}||N",
        "_OrderComment5=FVG_B",
        f"_FVG_TF2={TF_H4}||{TF_H4}||1||{TF_H4}||{TF_H4}||N",
        f"_FVG_MinSize2={best['minsize2']}||{best['minsize2']}||1||{best['minsize2']}||{best['minsize2']}||N",
        f"_FVG_MaxAge2={best['maxage2']}||{best['maxage2']}||1||{best['maxage2']}||{best['maxage2']}||N",
        f"_MaxZones2={best['zones2']}||{best['zones2']}||1||{best['zones2']}||{best['zones2']}||N",
        f"_RR_Ratio2={best['rr2']:.1f}||{best['rr2']:.1f}||1||{best['rr2']:.1f}||{best['rr2']:.1f}||N",
        f"_SL_Buffer2={best['slbuf2']}||{best['slbuf2']}||1||{best['slbuf2']}||{best['slbuf2']}||N",
        f"_PendingExpireBars_F2={best['expiry2']}||{best['expiry2']}||1||{best['expiry2']}||{best['expiry2']}||N",
    ]
    SET_OUT.parent.mkdir(parents=True, exist_ok=True)
    SET_OUT.write_text("\n".join(set_lines) + "\n", encoding="utf-8")
    print(f"\n  Written: {SET_OUT}")
    print(f"  Best: NP={best['np']:+,.0f}  PF={best['pf']:.3f}  "
          f"DD={best['dd']:.1f}%  NP/DD={best['np_dd']:,.0f}")
    print("=" * 68)
    print("  Done.")


if __name__ == "__main__":
    main()
