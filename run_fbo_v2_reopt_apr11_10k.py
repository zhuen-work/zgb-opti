"""FBO component reopt (via FBO_FVG_v2 EA, FVG streams disabled).

3-phase forward: S1(M30) -> S2(H4) -> S3(H1), then HalfTP sweep.
Phase 1-3: sweep TP/SL/Bars/EMA with HalfTP=0 fixed.
Phase 4:   all params fixed, sweep HalfTP1/2/3 together.
IS: Feb 14 -> Apr 11, 2026  (8w)
Output set: configs/sets/fbo_v2_3pct_m30xh4xh1_apr11_reopt.set
"""
from __future__ import annotations

import io, sys
from datetime import date
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

DD_THRESHOLD  = 20.0
TOP_N         = 5

MT5_TERMINAL  = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN     = 18912087
MT5_SERVER    = "VantageInternational-Live 3"
EA_PATH       = "FBO_FVG_v2"
SYMBOL        = "XAUUSD"
PERIOD        = "M30"
SPREAD        = 45
DEPOSIT_OPT   = 10_000
DEPOSIT_VAL   = 10_000
RISK_PCT_VAL  = 3.0

TF1_ENUM = 30      # M30
TF2_ENUM = 16388   # H4
TF3_ENUM = 16385   # H1

IS_START = date(2026, 2, 14)
IS_END   = date(2026, 4, 11)
IS_TAG   = "apr11"

OUTPUT_DIR    = Path("output/fbo_v2_reopt_apr11_10k")
SET_OUT       = Path("configs/sets/fbo_v2_3pct_m30xh4xh1_apr11_reopt_10k.set")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

# ── Sweep ranges ──────────────────────────────────────────────────────────────
TP_LO   = 8000;  TP_HI   = 25000; TP_STEP   = 1000
SL_LO   = 5000;  SL_HI   = 15000; SL_STEP   = 1000
BAR_LO  = 4;     BAR_HI  = 8;     BAR_STEP  = 2
EMA_LO  = 5;     EMA_HI  = 25;    EMA_STEP  = 5
HTP_LO  = 0.0;   HTP_HI  = 0.8;   HTP_STEP  = 0.1

TP_N   = len(range(TP_LO,  TP_HI  + 1, TP_STEP))
SL_N   = len(range(SL_LO,  SL_HI  + 1, SL_STEP))
BAR_N  = len(range(BAR_LO, BAR_HI + 1, BAR_STEP))
EMA_N  = len(range(EMA_LO, EMA_HI + 1, EMA_STEP))
HTP_N  = 9
PASSES_PER_PHASE = TP_N * SL_N * BAR_N * EMA_N
HTP_SWEEP_PASSES = HTP_N ** 3


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

def _load_best(xml_path, job_id, parse_optimization_xml, write_passes, pd, key_cols):
    records, warns = parse_optimization_xml(xml_path, job_id)
    for w in warns: print(f"  WARN: {w}")
    parquet, _ = write_passes(records, OUTPUT_DIR)
    print(f"  Parsed {len(records)} passes")
    df = pd.read_parquet(parquet)
    df["net_profit"]   = pd.to_numeric(df["net_profit"],   errors="coerce")
    df["drawdown_pct"] = pd.to_numeric(df["drawdown_pct"], errors="coerce")
    low_dd = df[df["drawdown_pct"] < DD_THRESHOLD]
    pool   = low_dd if not low_dd.empty else df
    cols   = [c for c in key_cols if c in pool.columns]
    best   = pool.sort_values("net_profit", ascending=False).drop_duplicates(subset=cols).iloc[0]
    return best, df


# ── Phase set-line builders ───────────────────────────────────────────────────

def _common_fixed():
    return [
        _fix("_BaseMagic", 1000),
        _fix("_CapitalProtectionAmount", "0.0"),
        _fix("_LotMode", 1), _fix("_RiskPct", "3.0"),
        _fix("TierBase", 2000), _fix("LotStep", "0.01"),
        _fix("_PendingExpireBars", 2),
        # FVG streams disabled — isolating FBO component
        _fix("_FVG1", 0), _fix("_FVG2", 0),
    ]

def _s1_sweep():
    return [
        "_OrderComment=FBO_A",
        _fix("_FBO1", 1), _fix("_time_frame", TF1_ENUM),
        _sweep("_take_profit",  TP_LO, TP_STEP,  TP_LO,  TP_HI),
        _sweep("_stop_loss",    SL_LO, SL_STEP,  SL_LO,  SL_HI),
        _sweep("_Bars",         BAR_LO, BAR_STEP, BAR_LO, BAR_HI),
        _sweep("_EMA_Period1",  EMA_LO, EMA_STEP, EMA_LO, EMA_HI),
        _fix("_HalfTP1", "0.0"),
    ]

def _s1_fixed(tp, sl, bars, ema, htp, include_htp=True):
    lines = [
        "_OrderComment=FBO_A",
        _fix("_FBO1", 1), _fix("_time_frame", TF1_ENUM),
        _fix("_take_profit", tp), _fix("_stop_loss", sl),
        _fix("_Bars", bars), _fix("_EMA_Period1", ema),
    ]
    if include_htp:
        lines.append(_fix("_HalfTP1", f"{htp:.1f}"))
    return lines

def _s2_sweep():
    return [
        "_OrderComment2=FBO_B",
        _fix("_FBO2", 1), _fix("_time_frame2", TF2_ENUM),
        _sweep("_take_profit2", TP_LO, TP_STEP,  TP_LO,  TP_HI),
        _sweep("_stop_loss2",   SL_LO, SL_STEP,  SL_LO,  SL_HI),
        _sweep("_Bars2",        BAR_LO, BAR_STEP, BAR_LO, BAR_HI),
        _sweep("_EMA_Period2",  EMA_LO, EMA_STEP, EMA_LO, EMA_HI),
        _fix("_HalfTP2", "0.0"),
    ]

def _s2_fixed(tp, sl, bars, ema, htp, include_htp=True):
    lines = [
        "_OrderComment2=FBO_B",
        _fix("_FBO2", 1), _fix("_time_frame2", TF2_ENUM),
        _fix("_take_profit2", tp), _fix("_stop_loss2", sl),
        _fix("_Bars2", bars), _fix("_EMA_Period2", ema),
    ]
    if include_htp:
        lines.append(_fix("_HalfTP2", f"{htp:.1f}"))
    return lines

def _s2_off():
    return ["_OrderComment2=FBO_B",
            _fix("_FBO2", 0), _fix("_time_frame2", TF2_ENUM),
            _fix("_take_profit2", 15000), _fix("_stop_loss2", 10000),
            _fix("_Bars2", 4), _fix("_EMA_Period2", 5), _fix("_HalfTP2", "0.0")]

def _s3_sweep():
    return [
        "_OrderComment3=FBO_C",
        _fix("_FBO3", 1), _fix("_time_frame3", TF3_ENUM),
        _sweep("_take_profit3", TP_LO, TP_STEP,  TP_LO,  TP_HI),
        _sweep("_stop_loss3",   SL_LO, SL_STEP,  SL_LO,  SL_HI),
        _sweep("_Bars3",        BAR_LO, BAR_STEP, BAR_LO, BAR_HI),
        _sweep("_EMA_Period3",  EMA_LO, EMA_STEP, EMA_LO, EMA_HI),
        _fix("_HalfTP3", "0.0"),
    ]

def _s3_fixed(tp, sl, bars, ema, htp, include_htp=True):
    lines = [
        "_OrderComment3=FBO_C",
        _fix("_FBO3", 1), _fix("_time_frame3", TF3_ENUM),
        _fix("_take_profit3", tp), _fix("_stop_loss3", sl),
        _fix("_Bars3", bars), _fix("_EMA_Period3", ema),
    ]
    if include_htp:
        lines.append(_fix("_HalfTP3", f"{htp:.1f}"))
    return lines

def _s3_off():
    return ["_OrderComment3=FBO_C",
            _fix("_FBO3", 0), _fix("_time_frame3", TF3_ENUM),
            _fix("_take_profit3", 15000), _fix("_stop_loss3", 10000),
            _fix("_Bars3", 4), _fix("_EMA_Period3", 5), _fix("_HalfTP3", "0.0")]


# ── ET validation ─────────────────────────────────────────────────────────────

def _build_val_ini(p, report_id):
    lines = "\n".join([
        "_BaseMagic=1000",
        "_CapitalProtectionAmount=0.0", "_LotMode=1", f"_RiskPct={RISK_PCT_VAL}",
        "TierBase=2000", "LotStep=0.01",
        "_PendingExpireBars=2",
        "_FVG1=0", "_FVG2=0",
        "_OrderComment=FBO_A",
        f"_FBO1=1", f"_time_frame={TF1_ENUM}",
        f"_take_profit={p['s1_tp']}",  f"_stop_loss={p['s1_sl']}",
        f"_Bars={p['s1_bars']}",        f"_EMA_Period1={p['s1_ema']}",
        f"_HalfTP1={p['s1_htp']:.1f}",
        "_OrderComment2=FBO_B",
        f"_FBO2=1", f"_time_frame2={TF2_ENUM}",
        f"_take_profit2={p['s2_tp']}", f"_stop_loss2={p['s2_sl']}",
        f"_Bars2={p['s2_bars']}",       f"_EMA_Period2={p['s2_ema']}",
        f"_HalfTP2={p['s2_htp']:.1f}",
        "_OrderComment3=FBO_C",
        f"_FBO3=1", f"_time_frame3={TF3_ENUM}",
        f"_take_profit3={p['s3_tp']}", f"_stop_loss3={p['s3_sl']}",
        f"_Bars3={p['s3_bars']}",       f"_EMA_Period3={p['s3_ema']}",
        f"_HalfTP3={p['s3_htp']:.1f}",
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

def _run_val(job_id, p, parse_forward_report):
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
    ini_path.write_text(_build_val_ini(p, job_id), encoding="utf-8")
    print(f"  Launching: {job_id}")
    rc = run_mt5_job(MT5_TERMINAL, str(ini_path))
    print(f"  Exit code: {rc}")
    art = find_report_artifact(OUTPUT_DIR, job_id, MT5_TERMINAL)
    if art is None:
        for ext in (".htm", ".html"):
            c = MT5_DATA_ROOT / f"{job_id}{ext}"
            if c.exists(): art = c; break
    if art is None:
        raise RuntimeError(f"No ET report found for {job_id}")
    htm = copy_report_artifact(art, OUTPUT_DIR)
    m, _ = parse_forward_report(htm, job_id)
    return dict(np=float(m.get("net_profit", 0)), pf=float(m.get("profit_factor", 0)),
                dd=float(m.get("drawdown_pct", 0)), trades=int(m.get("trades", 0)))


def _write_set(best_params):
    lines = [
        "_BaseMagic=1000||1000||1||1000||1000||N",
        "_CapitalProtectionAmount=0.0||0.0||1||0.0||0.0||N",
        "_RiskPct=3.0||3.0||1||3.0||3.0||N",
        "_LotMode=1||1||1||1||1||N",
        "TierBase=2000||2000||1||2000||2000||N",
        "LotStep=0.01||0.01||1||0.01||0.01||N",
        "_PendingExpireBars=2||2||1||2||2||N",
        "_FBO1=1||1||1||1||1||N",
        "_OrderComment=FBO_A",
        f"_time_frame={TF1_ENUM}||{TF1_ENUM}||1||{TF1_ENUM}||{TF1_ENUM}||N",
        f"_take_profit={best_params['s1_tp']}||{best_params['s1_tp']}||1||{best_params['s1_tp']}||{best_params['s1_tp']}||N",
        f"_stop_loss={best_params['s1_sl']}||{best_params['s1_sl']}||1||{best_params['s1_sl']}||{best_params['s1_sl']}||N",
        f"_Bars={best_params['s1_bars']}||{best_params['s1_bars']}||1||{best_params['s1_bars']}||{best_params['s1_bars']}||N",
        f"_EMA_Period1={best_params['s1_ema']}||{best_params['s1_ema']}||1||{best_params['s1_ema']}||{best_params['s1_ema']}||N",
        f"_HalfTP1={best_params['s1_htp']:.1f}||{best_params['s1_htp']:.1f}||1||{best_params['s1_htp']:.1f}||{best_params['s1_htp']:.1f}||N",
        "_FBO2=1||1||1||1||1||N",
        "_OrderComment2=FBO_B",
        f"_time_frame2={TF2_ENUM}||{TF2_ENUM}||1||{TF2_ENUM}||{TF2_ENUM}||N",
        f"_take_profit2={best_params['s2_tp']}||{best_params['s2_tp']}||1||{best_params['s2_tp']}||{best_params['s2_tp']}||N",
        f"_stop_loss2={best_params['s2_sl']}||{best_params['s2_sl']}||1||{best_params['s2_sl']}||{best_params['s2_sl']}||N",
        f"_Bars2={best_params['s2_bars']}||{best_params['s2_bars']}||1||{best_params['s2_bars']}||{best_params['s2_bars']}||N",
        f"_EMA_Period2={best_params['s2_ema']}||{best_params['s2_ema']}||1||{best_params['s2_ema']}||{best_params['s2_ema']}||N",
        f"_HalfTP2={best_params['s2_htp']:.1f}||{best_params['s2_htp']:.1f}||1||{best_params['s2_htp']:.1f}||{best_params['s2_htp']:.1f}||N",
        "_FBO3=1||1||1||1||1||N",
        "_OrderComment3=FBO_C",
        f"_time_frame3={TF3_ENUM}||{TF3_ENUM}||1||{TF3_ENUM}||{TF3_ENUM}||N",
        f"_take_profit3={best_params['s3_tp']}||{best_params['s3_tp']}||1||{best_params['s3_tp']}||{best_params['s3_tp']}||N",
        f"_stop_loss3={best_params['s3_sl']}||{best_params['s3_sl']}||1||{best_params['s3_sl']}||{best_params['s3_sl']}||N",
        f"_Bars3={best_params['s3_bars']}||{best_params['s3_bars']}||1||{best_params['s3_bars']}||{best_params['s3_bars']}||N",
        f"_EMA_Period3={best_params['s3_ema']}||{best_params['s3_ema']}||1||{best_params['s3_ema']}||{best_params['s3_ema']}||N",
        f"_HalfTP3={best_params['s3_htp']:.1f}||{best_params['s3_htp']:.1f}||1||{best_params['s3_htp']:.1f}||{best_params['s3_htp']:.1f}||N",
    ]
    SET_OUT.parent.mkdir(parents=True, exist_ok=True)
    SET_OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\n  Written: {SET_OUT}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    import pandas as pd
    from zgb_opti.xml_parser import parse_optimization_xml, parse_forward_report, write_passes

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 68)
    print(f"  FBO component reopt via FBO_FVG_v1  ({IS_TAG.upper()})")
    print(f"  IS: {IS_START} -> {IS_END}  EA={EA_PATH}  Chart={PERIOD}")
    print(f"  Passes per phase: {PASSES_PER_PHASE:,}  x3 = {PASSES_PER_PHASE*3:,}  "
          f"+ HalfTP: {HTP_SWEEP_PASSES}  |  Total: {PASSES_PER_PHASE*3 + HTP_SWEEP_PASSES:,}")
    print(f"  DD gate: {DD_THRESHOLD}%  |  Output: {SET_OUT}")
    print("=" * 68)

    # ── Phase 1: S1 alone ─────────────────────────────────────────────────────
    print(f"\n{'='*68}")
    print("  PHASE 1: S1 (M30) alone")
    print("=" * 68)

    job_f1 = f"fwd_f1_s1_{IS_TAG}"
    xml_f1 = OUTPUT_DIR / f"{job_f1}.xml"
    s1_key_cols = ["param__take_profit", "param__stop_loss", "param__Bars", "param__EMA_Period1"]

    if xml_f1.exists():
        print(f"  [Cached] {xml_f1.name}")
    else:
        set_lines = _common_fixed() + _s1_sweep() + _s2_off() + _s3_off()
        _run_mt5(job_f1, _build_opti_ini(set_lines, job_f1))

    best_s1, _ = _load_best(xml_f1, job_f1, parse_optimization_xml, write_passes, pd, s1_key_cols)
    s1_tp   = _vi(best_s1, "param__take_profit", 8000)
    s1_sl   = _vi(best_s1, "param__stop_loss",   5000)
    s1_bars = _vi(best_s1, "param__Bars",          4)
    s1_ema  = _vi(best_s1, "param__EMA_Period1",   5)
    s1_htp  = 0.0
    print(f"  Best: NP={float(best_s1.get('net_profit',0)):+,.0f}  "
          f"PF={float(best_s1.get('profit_factor',0)):.3f}  "
          f"DD={float(best_s1.get('drawdown_pct',0)):.1f}%  Tr={_vi(best_s1,'trades',0)}")
    print(f"  -> S1: TP={s1_tp}  SL={s1_sl}  Bars={s1_bars}  EMA={s1_ema}")

    # ── Phase 2: S2 [S1 fixed, S3 off] ───────────────────────────────────────
    print(f"\n{'='*68}")
    print("  PHASE 2: S2 (H4)  [S1 fixed]")
    print("=" * 68)

    job_f2 = f"fwd_f2_s2_{IS_TAG}"
    xml_f2 = OUTPUT_DIR / f"{job_f2}.xml"
    s2_key_cols = ["param__take_profit2", "param__stop_loss2", "param__Bars2", "param__EMA_Period2"]

    if xml_f2.exists():
        print(f"  [Cached] {xml_f2.name}")
    else:
        set_lines = _common_fixed() + _s1_fixed(s1_tp, s1_sl, s1_bars, s1_ema, s1_htp) + _s2_sweep() + _s3_off()
        _run_mt5(job_f2, _build_opti_ini(set_lines, job_f2))

    best_s2, _ = _load_best(xml_f2, job_f2, parse_optimization_xml, write_passes, pd, s2_key_cols)
    s2_tp   = _vi(best_s2, "param__take_profit2", 23000)
    s2_sl   = _vi(best_s2, "param__stop_loss2",    5000)
    s2_bars = _vi(best_s2, "param__Bars2",           4)
    s2_ema  = _vi(best_s2, "param__EMA_Period2",     5)
    s2_htp  = 0.0
    print(f"  Best: NP={float(best_s2.get('net_profit',0)):+,.0f}  "
          f"PF={float(best_s2.get('profit_factor',0)):.3f}  "
          f"DD={float(best_s2.get('drawdown_pct',0)):.1f}%  Tr={_vi(best_s2,'trades',0)}")
    print(f"  -> S2: TP={s2_tp}  SL={s2_sl}  Bars={s2_bars}  EMA={s2_ema}")

    # ── Phase 3: S3 [S1+S2 fixed] ────────────────────────────────────────────
    print(f"\n{'='*68}")
    print("  PHASE 3: S3 (H1)  [S1+S2 fixed]")
    print("=" * 68)

    job_f3 = f"fwd_f3_s3_{IS_TAG}"
    xml_f3 = OUTPUT_DIR / f"{job_f3}.xml"
    s3_key_cols = ["param__take_profit3", "param__stop_loss3", "param__Bars3", "param__EMA_Period3"]

    if xml_f3.exists():
        print(f"  [Cached] {xml_f3.name}")
    else:
        set_lines = (_common_fixed()
                     + _s1_fixed(s1_tp, s1_sl, s1_bars, s1_ema, s1_htp)
                     + _s2_fixed(s2_tp, s2_sl, s2_bars, s2_ema, s2_htp)
                     + _s3_sweep())
        _run_mt5(job_f3, _build_opti_ini(set_lines, job_f3))

    best_s3, _ = _load_best(xml_f3, job_f3, parse_optimization_xml, write_passes, pd, s3_key_cols)
    s3_tp   = _vi(best_s3, "param__take_profit3", 18000)
    s3_sl   = _vi(best_s3, "param__stop_loss3",    7000)
    s3_bars = _vi(best_s3, "param__Bars3",            4)
    s3_ema  = _vi(best_s3, "param__EMA_Period3",     15)
    s3_htp  = 0.0
    print(f"  Best: NP={float(best_s3.get('net_profit',0)):+,.0f}  "
          f"PF={float(best_s3.get('profit_factor',0)):.3f}  "
          f"DD={float(best_s3.get('drawdown_pct',0)):.1f}%  Tr={_vi(best_s3,'trades',0)}")
    print(f"  -> S3: TP={s3_tp}  SL={s3_sl}  Bars={s3_bars}  EMA={s3_ema}")

    print(f"\n  PHASE 1-3 SUMMARY:")
    print(f"    S1: TP={s1_tp}/SL={s1_sl}/B={s1_bars}/EMA={s1_ema}")
    print(f"    S2: TP={s2_tp}/SL={s2_sl}/B={s2_bars}/EMA={s2_ema}")
    print(f"    S3: TP={s3_tp}/SL={s3_sl}/B={s3_bars}/EMA={s3_ema}")

    # ── Phase 4: HalfTP sweep ─────────────────────────────────────────────────
    print(f"\n{'='*68}")
    print(f"  PHASE 4: HalfTP sweep  ({HTP_SWEEP_PASSES} passes)")
    print("=" * 68)

    job_htp = f"halftp_sweep_{IS_TAG}"
    xml_htp = OUTPUT_DIR / f"{job_htp}.xml"
    htp_key_cols = ["param__HalfTP1", "param__HalfTP2", "param__HalfTP3"]

    def _halftp_set_lines():
        return (_common_fixed()
                + _s1_fixed(s1_tp, s1_sl, s1_bars, s1_ema, 0.0, include_htp=False)
                + _s2_fixed(s2_tp, s2_sl, s2_bars, s2_ema, 0.0, include_htp=False)
                + _s3_fixed(s3_tp, s3_sl, s3_bars, s3_ema, 0.0, include_htp=False)
                + [
                    _sweep("_HalfTP1", HTP_LO, HTP_STEP, HTP_LO, HTP_HI),
                    _sweep("_HalfTP2", HTP_LO, HTP_STEP, HTP_LO, HTP_HI),
                    _sweep("_HalfTP3", HTP_LO, HTP_STEP, HTP_LO, HTP_HI),
                ])

    if xml_htp.exists():
        print(f"  [Cached] {xml_htp.name}")
    else:
        _run_mt5(job_htp, _build_opti_ini(_halftp_set_lines(), job_htp))

    records_htp, warns_htp = parse_optimization_xml(xml_htp, job_htp)
    for w in warns_htp: print(f"  WARN: {w}")
    parquet_htp, _ = write_passes(records_htp, OUTPUT_DIR)
    df_htp = pd.read_parquet(parquet_htp)
    df_htp["net_profit"]   = pd.to_numeric(df_htp["net_profit"],   errors="coerce")
    df_htp["drawdown_pct"] = pd.to_numeric(df_htp["drawdown_pct"], errors="coerce")
    pool_htp = df_htp[df_htp["drawdown_pct"] < DD_THRESHOLD]
    pool_htp = pool_htp if not pool_htp.empty else df_htp
    dedup_htp = [c for c in htp_key_cols if c in pool_htp.columns]
    top5_htp = (pool_htp.sort_values("net_profit", ascending=False)
                        .drop_duplicates(subset=dedup_htp)
                        .head(TOP_N))
    print(f"  Parsed {len(records_htp)} passes")

    print(f"\n  TOP {TOP_N} HalfTP combos (DD < {DD_THRESHOLD}%)")
    print(f"  {'#':<3}  {'NP':>14}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  {'HTP1':>5}  {'HTP2':>5}  {'HTP3':>5}")
    print("  " + "-" * 60)
    for rank, (_, row) in enumerate(top5_htp.iterrows(), 1):
        print(f"  {rank:<3}  {float(row.get('net_profit',0)):>+14,.0f}  "
              f"{float(row.get('profit_factor',0)):>6.3f}  {float(row.get('drawdown_pct',0)):>5.1f}%  "
              f"{_vi(row,'trades',0):>4}  {_vf(row,'param__HalfTP1',0):>5.1f}  "
              f"{_vf(row,'param__HalfTP2',0):>5.1f}  {_vf(row,'param__HalfTP3',0):>5.1f}")

    # ── ET validation ─────────────────────────────────────────────────────────
    print(f"\n{'='*68}")
    print(f"  ET VALIDATION  ${DEPOSIT_VAL:,}  {RISK_PCT_VAL:.0f}% risk")
    print("=" * 68)

    et_results = []
    for rank, (_, row) in enumerate(top5_htp.iterrows(), 1):
        p = dict(
            s1_tp=s1_tp, s1_sl=s1_sl, s1_bars=s1_bars, s1_ema=s1_ema,
            s1_htp=_vf(row, "param__HalfTP1", 0.0),
            s2_tp=s2_tp, s2_sl=s2_sl, s2_bars=s2_bars, s2_ema=s2_ema,
            s2_htp=_vf(row, "param__HalfTP2", 0.0),
            s3_tp=s3_tp, s3_sl=s3_sl, s3_bars=s3_bars, s3_ema=s3_ema,
            s3_htp=_vf(row, "param__HalfTP3", 0.0),
        )
        print(f"\n-- ET #{rank}  HTP=({p['s1_htp']:.1f}/{p['s2_htp']:.1f}/{p['s3_htp']:.1f}) --")
        m = _run_val(f"et_val_{rank}_{IS_TAG}", p, parse_forward_report)
        et_results.append(dict(rank=rank, params=p, **m))

    for r in et_results:
        r["np_dd"] = r["np"] / r["dd"] if r["dd"] > 0 else 0.0
    et_results.sort(key=lambda x: x["np_dd"], reverse=True)

    print(f"\n  {'ET#':<4}  {'NP':>12}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  {'NP/DD':>8}  "
          f"{'HTP1':>5}  {'HTP2':>5}  {'HTP3':>5}")
    print("  " + "-" * 72)
    for r in et_results:
        p = r["params"]
        print(f"  {r['rank']:<4}  {r['np']:>+12,.0f}  {r['pf']:>6.3f}  {r['dd']:>5.1f}%  {r['trades']:>4}  "
              f"{r['np_dd']:>8,.0f}  {p['s1_htp']:>5.1f}  {p['s2_htp']:>5.1f}  {p['s3_htp']:>5.1f}")

    # Write best set
    best_et = et_results[0]
    _write_set(best_et["params"])
    print(f"\n  Best ET NP={best_et['np']:+,.0f}  PF={best_et['pf']:.3f}  "
          f"DD={best_et['dd']:.1f}%  NP/DD={best_et['np_dd']:,.0f}")
    print("=" * 68)
    print("  Done.")


if __name__ == "__main__":
    main()
