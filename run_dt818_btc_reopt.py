"""DT818_exp BTCUSD reopt — forward (S1->S2->S3) + reverse (S3->S2->S1).

BTCUSD-specific adjustments vs XAUUSD:
  - SYMBOL = BTCUSD
  - TP/SL ranges scaled ~14x (price ratio BTC/XAU)
  - SPREAD = 1706 pts
  - IS: Feb 1 -> Mar 28, 2026  (8 weeks, uses available broker data)
  - Bars locked: S1=4, S2=6, S3=6 (same as XAUUSD best)
  - EMA swept: S1/S2 4-8 step 2, S3 3-9 step 2 (same as XAUUSD)
"""
from __future__ import annotations

import io, sys
from datetime import date
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

DD_THRESHOLD = 20.0
TOP_N        = 5

MT5_TERMINAL  = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN     = 18912087
MT5_SERVER    = "VantageInternational-Live 3"
EA_PATH       = "DT818_exp"
SYMBOL        = "BTCUSD"
PERIOD        = "M30"
SPREAD        = 1706
DEPOSIT_OPT   = 10_000_000
DEPOSIT_VAL   = 10_000
RISK_PCT_VAL  = 4.0

TF1_ENUM = 30      # M30
TF2_ENUM = 16388   # H4
TF3_ENUM = 16385   # H1

IS_START = date(2026, 2, 1)
IS_END   = date(2026, 3, 28)
IS_TAG   = "btc_mar28"

OUTPUT_DIR    = Path(f"output/dt818_{IS_TAG}")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

# ── Param definitions ─────────────────────────────────────────────────────────
# TP/SL scaled ~14x from XAUUSD best params.
# XAU: TP 2000-25000 step 500  → BTC: 30000-350000 step 10000
# XAU: SL 2000-15000 step 500  → BTC: 30000-210000 step 10000
# EMA and Bars: same as XAUUSD (bar-count based, not price-based)

S1_PARAMS = [
    ("_take_profit",  200000, 10000, 30000, 350000),   # 32 values
    ("_stop_loss",    150000, 10000, 30000, 210000),   # 19 values
    ("_EMA_Period1",  4,      2,     4,     8),         # 3 values → 1,824 passes
]
S2_PARAMS = [
    ("_take_profit2", 280000, 10000, 30000, 350000),   # 32 values
    ("_stop_loss2",   120000, 10000, 30000, 210000),   # 19 values
    ("_EMA_Period2",  4,      2,     4,     8),         # 3 values → 1,824 passes
]
S3_PARAMS = [
    ("_take_profit3", 315000, 10000, 30000, 350000),   # 32 values
    ("_stop_loss3",   100000, 10000, 30000, 210000),   # 19 values
    ("_EMA_Period3",  9,      2,     3,     9),         # 4 values → 2,432 passes
]

S1_DEFAULTS = {"_take_profit": 200000, "_stop_loss": 150000, "_EMA_Period1": 4, "_Bars": 4}
S2_DEFAULTS = {"_take_profit2": 280000, "_stop_loss2": 120000, "_EMA_Period2": 4, "_Bars2": 6}
S3_DEFAULTS = {"_take_profit3": 315000, "_stop_loss3": 100000, "_EMA_Period3": 9, "_Bars3": 6}

# Risk params scaled ~14x from XAUUSD values
S1_RISK_FIXES = [("_TSL1", 7000),   ("_TSLA1", 3000),  ("_BETrigger1", 35000), ("_BEBuf1", 6000)]
S2_RISK_FIXES = [("_TSL2", 63000),  ("_TSLA2", 14000), ("_BETrigger2", 7000),  ("_BEBuf2", 4000)]
S3_RISK_FIXES = [("_TSL3", 28000),  ("_TSLA3", 10000), ("_BETrigger3", 28000), ("_BEBuf3", 10000)]

ALL_PARAM_COLS = [
    "param__take_profit",  "param__stop_loss",  "param__EMA_Period1",
    "param__take_profit2", "param__stop_loss2", "param__EMA_Period2",
    "param__take_profit3", "param__stop_loss3", "param__EMA_Period3",
]


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
    for _ext in (".htm", ".html", ".xml"):
        _p = OUTPUT_DIR / f"{job_id}{_ext}"
        if _p.exists(): _p.unlink()
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

def _vi(row, col, d=0):
    try:    return int(float(row.get(col, d)))
    except: return int(d)

def _extract_s1(row):
    return {
        "_take_profit":  _vi(row, "param__take_profit",  S1_DEFAULTS["_take_profit"]),
        "_stop_loss":    _vi(row, "param__stop_loss",    S1_DEFAULTS["_stop_loss"]),
        "_EMA_Period1":  _vi(row, "param__EMA_Period1",  S1_DEFAULTS["_EMA_Period1"]),
        "_Bars":         S1_DEFAULTS["_Bars"],
    }

def _extract_s2(row):
    return {
        "_take_profit2": _vi(row, "param__take_profit2", S2_DEFAULTS["_take_profit2"]),
        "_stop_loss2":   _vi(row, "param__stop_loss2",   S2_DEFAULTS["_stop_loss2"]),
        "_EMA_Period2":  _vi(row, "param__EMA_Period2",  S2_DEFAULTS["_EMA_Period2"]),
        "_Bars2":        S2_DEFAULTS["_Bars2"],
    }

def _extract_s3(row):
    return {
        "_take_profit3": _vi(row, "param__take_profit3", S3_DEFAULTS["_take_profit3"]),
        "_stop_loss3":   _vi(row, "param__stop_loss3",   S3_DEFAULTS["_stop_loss3"]),
        "_EMA_Period3":  _vi(row, "param__EMA_Period3",  S3_DEFAULTS["_EMA_Period3"]),
        "_Bars3":        S3_DEFAULTS["_Bars3"],
    }


# ── Set line builders ─────────────────────────────────────────────────────────

def _common_fixes():
    return [
        _fix("_BaseMagic", 1000),
        "_OrderComment=DT818_A", "_OrderComment2=DT818_B", "_OrderComment3=DT818_C",
        _fix("_CapitalProtectionAmount", "0.0"),
        _fix("_LotMode", 1), _fix("_RiskPct", "3.0"),
        _fix("TierBase", 2000), _fix("LotStep", "0.01"),
    ]

def _s1_lines(sweep=True, fixed=None):
    p = fixed or S1_DEFAULTS
    lines = [_fix("_Trade1", 1), _fix("_time_frame", TF1_ENUM), _fix("_RiskMode1", 0)]
    lines += [_fix(n, v) for n, v in S1_RISK_FIXES]
    for name, default, step, lo, hi in S1_PARAMS:
        lines.append(_sweep(name, default, step, lo, hi) if sweep else _fix(name, p[name]))
    lines.append(_fix("_Bars", S1_DEFAULTS["_Bars"]))
    return lines

def _s2_lines(sweep=True, fixed=None):
    p = fixed or S2_DEFAULTS
    lines = [_fix("_Trade2", 1), _fix("_time_frame2", TF2_ENUM), _fix("_RiskMode2", 0)]
    lines += [_fix(n, v) for n, v in S2_RISK_FIXES]
    for name, default, step, lo, hi in S2_PARAMS:
        lines.append(_sweep(name, default, step, lo, hi) if sweep else _fix(name, p[name]))
    lines.append(_fix("_Bars2", S2_DEFAULTS["_Bars2"]))
    return lines

def _s3_lines(sweep=True, fixed=None):
    p = fixed or S3_DEFAULTS
    lines = [_fix("_Trade3", 1), _fix("_time_frame3", TF3_ENUM), _fix("_RiskMode3", 0)]
    lines += [_fix(n, v) for n, v in S3_RISK_FIXES]
    for name, default, step, lo, hi in S3_PARAMS:
        lines.append(_sweep(name, default, step, lo, hi) if sweep else _fix(name, p[name]))
    lines.append(_fix("_Bars3", S3_DEFAULTS["_Bars3"]))
    return lines

def _s1_off():
    return [_fix("_Trade1", 0), _fix("_time_frame", TF1_ENUM),
            _fix("_take_profit", S1_DEFAULTS["_take_profit"]),
            _fix("_stop_loss",   S1_DEFAULTS["_stop_loss"]),
            _fix("_EMA_Period1", S1_DEFAULTS["_EMA_Period1"])]

def _s2_off():
    return [_fix("_Trade2", 0), _fix("_time_frame2", TF2_ENUM),
            _fix("_take_profit2", S2_DEFAULTS["_take_profit2"]),
            _fix("_stop_loss2",   S2_DEFAULTS["_stop_loss2"]),
            _fix("_EMA_Period2",  S2_DEFAULTS["_EMA_Period2"])]

def _s3_off():
    return [_fix("_Trade3", 0), _fix("_time_frame3", TF3_ENUM),
            _fix("_take_profit3", S3_DEFAULTS["_take_profit3"]),
            _fix("_stop_loss3",   S3_DEFAULTS["_stop_loss3"]),
            _fix("_EMA_Period3",  S3_DEFAULTS["_EMA_Period3"])]


# ── ET validation ─────────────────────────────────────────────────────────────

def _build_val_ini(p, report_id):
    lines = "\n".join([
        "_BaseMagic=1000", "_OrderComment=DT818_A", "_OrderComment2=DT818_B",
        "_OrderComment3=DT818_C",
        "_CapitalProtectionAmount=0.0", "_LotMode=1", f"_RiskPct={RISK_PCT_VAL}",
        "TierBase=2000", "LotStep=0.01",
        "_Trade1=1", f"_time_frame={TF1_ENUM}", "_RiskMode1=0",
        f"_take_profit={p['tp1']}", f"_stop_loss={p['sl1']}",
        f"_EMA_Period1={p['ema1']}", f"_Bars={S1_DEFAULTS['_Bars']}",
        f"_TSL1={S1_RISK_FIXES[0][1]}", f"_TSLA1={S1_RISK_FIXES[1][1]}",
        f"_BETrigger1={S1_RISK_FIXES[2][1]}", f"_BEBuf1={S1_RISK_FIXES[3][1]}",
        "_Trade2=1", f"_time_frame2={TF2_ENUM}", "_RiskMode2=0",
        f"_take_profit2={p['tp2']}", f"_stop_loss2={p['sl2']}",
        f"_EMA_Period2={p['ema2']}", f"_Bars2={S2_DEFAULTS['_Bars2']}",
        f"_TSL2={S2_RISK_FIXES[0][1]}", f"_TSLA2={S2_RISK_FIXES[1][1]}",
        f"_BETrigger2={S2_RISK_FIXES[2][1]}", f"_BEBuf2={S2_RISK_FIXES[3][1]}",
        "_Trade3=1", f"_time_frame3={TF3_ENUM}", "_RiskMode3=0",
        f"_take_profit3={p['tp3']}", f"_stop_loss3={p['sl3']}",
        f"_EMA_Period3={p['ema3']}", f"_Bars3={S3_DEFAULTS['_Bars3']}",
        f"_TSL3={S3_RISK_FIXES[0][1]}", f"_TSLA3={S3_RISK_FIXES[1][1]}",
        f"_BETrigger3={S3_RISK_FIXES[2][1]}", f"_BEBuf3={S3_RISK_FIXES[3][1]}",
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


# ── One optimization phase ────────────────────────────────────────────────────

def _run_phase(label, job_id, set_lines, key_cols, parse_optimization_xml, write_passes, pd):
    xml_path = OUTPUT_DIR / f"{job_id}.xml"
    print(f"\n-- {label} --")
    if xml_path.exists():
        print(f"  [Cached] {xml_path.name}")
    else:
        _run_mt5(job_id, _build_opti_ini(set_lines, job_id))
    best, df = _load_best(xml_path, job_id, parse_optimization_xml, write_passes, pd, key_cols)
    np_v = float(best.get("net_profit",    0))
    pf_v = float(best.get("profit_factor", 0))
    dd_v = float(best.get("drawdown_pct",  0))
    tr_v = _vi(best, "trades", 0)
    print(f"  Best: NP={np_v:+,.0f}  PF={pf_v:.3f}  DD={dd_v:.1f}%  Tr={tr_v}")
    return best, df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    import pandas as pd
    from zgb_opti.xml_parser import parse_optimization_xml, parse_forward_report, write_passes

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    s1_p = len(range(30000, 350001, 10000)) * len(range(30000, 210001, 10000)) * 3
    s2_p = s1_p
    s3_p = len(range(30000, 350001, 10000)) * len(range(30000, 210001, 10000)) * 4

    print("=" * 68)
    print(f"  DT818_exp BTCUSD reopt  ({IS_TAG.upper()})")
    print(f"  IS: {IS_START} -> {IS_END}  EA={EA_PATH}  Chart={PERIOD}")
    print(f"  Symbol: {SYMBOL}  Spread: {SPREAD} pts (${SPREAD*0.01:.2f})")
    print(f"  TP/SL scaled ~14x from XAUUSD  |  Bars locked S1=4 S2=6 S3=6")
    print(f"  Passes per phase: S1={s1_p:,}  S2={s2_p:,}  S3={s3_p:,}")
    print(f"  Forward: S1->S2->S3  |  Reverse: S3->S2->S1")
    print(f"  Complete sweep per phase  |  DD gate: {DD_THRESHOLD}%")
    print("=" * 68)

    args = (parse_optimization_xml, write_passes, pd)

    # ════════════════════════════════════════════════════════════════════════
    # FORWARD PASS
    # ════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 68)
    print("  FORWARD PASS: S1 -> S2 -> S3")
    print("=" * 68)

    best_f1, _ = _run_phase(
        "F1: S1(M30) alone",
        f"fwd_f1_s1_{IS_TAG}",
        _common_fixes() + _s1_lines(sweep=True) + _s2_off() + _s3_off(),
        ["param__take_profit", "param__stop_loss", "param__EMA_Period1"],
        *args,
    )
    s1 = _extract_s1(best_f1)
    print(f"  -> S1 fixed: TP={s1['_take_profit']}  SL={s1['_stop_loss']}  EMA={s1['_EMA_Period1']}")

    best_f2, _ = _run_phase(
        "F2: S2(H4)  [S1 fixed]",
        f"fwd_f2_s2_{IS_TAG}",
        _common_fixes() + _s1_lines(sweep=False, fixed=s1) + _s2_lines(sweep=True) + _s3_off(),
        ["param__take_profit2", "param__stop_loss2", "param__EMA_Period2"],
        *args,
    )
    s2 = _extract_s2(best_f2)
    print(f"  -> S2 fixed: TP={s2['_take_profit2']}  SL={s2['_stop_loss2']}  EMA={s2['_EMA_Period2']}")

    _, df_f3 = _run_phase(
        "F3: S3(H1)  [S1+S2 fixed]  -- all streams on",
        f"fwd_f3_s3_{IS_TAG}",
        _common_fixes() + _s1_lines(sweep=False, fixed=s1) + _s2_lines(sweep=False, fixed=s2) + _s3_lines(sweep=True),
        ["param__take_profit3", "param__stop_loss3", "param__EMA_Period3"],
        *args,
    )
    df_f3 = df_f3.copy()
    for k, v in {**s1, **s2}.items():
        df_f3[f"param__{k.lstrip('_')}"] = v
    df_f3["param__Bars3"] = S3_DEFAULTS["_Bars3"]

    # ════════════════════════════════════════════════════════════════════════
    # REVERSE PASS
    # ════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 68)
    print("  REVERSE PASS: S3 -> S2 -> S1")
    print("=" * 68)

    best_r1, _ = _run_phase(
        "R1: S3(H1) alone",
        f"rev_r1_s3_{IS_TAG}",
        _common_fixes() + _s1_off() + _s2_off() + _s3_lines(sweep=True),
        ["param__take_profit3", "param__stop_loss3", "param__EMA_Period3"],
        *args,
    )
    s3 = _extract_s3(best_r1)
    print(f"  -> S3 fixed: TP={s3['_take_profit3']}  SL={s3['_stop_loss3']}  EMA={s3['_EMA_Period3']}")

    best_r2, _ = _run_phase(
        "R2: S2(H4)  [S3 fixed]",
        f"rev_r2_s2_{IS_TAG}",
        _common_fixes() + _s1_off() + _s2_lines(sweep=True) + _s3_lines(sweep=False, fixed=s3),
        ["param__take_profit2", "param__stop_loss2", "param__EMA_Period2"],
        *args,
    )
    s2r = _extract_s2(best_r2)
    print(f"  -> S2 fixed: TP={s2r['_take_profit2']}  SL={s2r['_stop_loss2']}  EMA={s2r['_EMA_Period2']}")

    _, df_r3 = _run_phase(
        "R3: S1(M30)  [S3+S2 fixed]  -- all streams on",
        f"rev_r3_s1_{IS_TAG}",
        _common_fixes() + _s1_lines(sweep=True) + _s2_lines(sweep=False, fixed=s2r) + _s3_lines(sweep=False, fixed=s3),
        ["param__take_profit", "param__stop_loss", "param__EMA_Period1"],
        *args,
    )
    df_r3 = df_r3.copy()
    for k, v in {**s2r, **s3}.items():
        df_r3[f"param__{k.lstrip('_')}"] = v
    df_r3["param__Bars"] = S1_DEFAULTS["_Bars"]

    # ════════════════════════════════════════════════════════════════════════
    # POOL, TOP 5
    # ════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 68)
    print(f"  TOP {TOP_N} -- pooled (DD < {DD_THRESHOLD}%)")
    print("=" * 68)

    combined = pd.concat([df_f3, df_r3], ignore_index=True)
    combined["net_profit"]   = pd.to_numeric(combined["net_profit"],   errors="coerce")
    combined["drawdown_pct"] = pd.to_numeric(combined["drawdown_pct"], errors="coerce")

    pool = combined[combined["drawdown_pct"] < DD_THRESHOLD]
    pool = pool if not pool.empty else combined

    dedup_cols = [c for c in ALL_PARAM_COLS if c in pool.columns]
    top5 = (pool.sort_values("net_profit", ascending=False)
                .drop_duplicates(subset=dedup_cols)
                .head(TOP_N))

    print(f"\n  {'#':<3}  {'NP':>12}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  "
          f"{'S1 TP/SL/EMA':<22}  {'S2 TP/SL/EMA':<22}  {'S3 TP/SL/EMA'}")
    print("  " + "-" * 105)
    for rank, (_, row) in enumerate(top5.iterrows(), 1):
        np_v = float(row.get("net_profit",    0))
        pf_v = float(row.get("profit_factor", 0))
        dd_v = float(row.get("drawdown_pct",  0))
        tr_v = _vi(row, "trades", 0)
        tp1  = _vi(row, "param__take_profit",  0); sl1 = _vi(row, "param__stop_loss",    0); e1 = _vi(row, "param__EMA_Period1", 0)
        tp2  = _vi(row, "param__take_profit2", 0); sl2 = _vi(row, "param__stop_loss2",   0); e2 = _vi(row, "param__EMA_Period2", 0)
        tp3  = _vi(row, "param__take_profit3", 0); sl3 = _vi(row, "param__stop_loss3",   0); e3 = _vi(row, "param__EMA_Period3", 0)
        print(f"  {rank:<3}  {np_v:>+12,.0f}  {pf_v:>6.3f}  {dd_v:>5.1f}%  {tr_v:>4}  "
              f"{tp1}/{sl1}/EMA{e1:<10}  {tp2}/{sl2}/EMA{e2:<10}  {tp3}/{sl3}/EMA{e3}")

    print("=" * 68)

    # ════════════════════════════════════════════════════════════════════════
    # ET VALIDATION
    # ════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 68)
    print(f"  ET VALIDATION -- Every Tick  ${DEPOSIT_VAL:,}  {RISK_PCT_VAL}% risk")
    print("=" * 68)

    et_results = []
    for rank, (_, row) in enumerate(top5.iterrows(), 1):
        p = dict(
            tp1=_vi(row, "param__take_profit",  0), sl1=_vi(row, "param__stop_loss",    0), ema1=_vi(row, "param__EMA_Period1", 0),
            tp2=_vi(row, "param__take_profit2", 0), sl2=_vi(row, "param__stop_loss2",   0), ema2=_vi(row, "param__EMA_Period2", 0),
            tp3=_vi(row, "param__take_profit3", 0), sl3=_vi(row, "param__stop_loss3",   0), ema3=_vi(row, "param__EMA_Period3", 0),
        )
        print(f"\n-- ET #{rank} --")
        m = _run_val(f"et_val_{rank}_{IS_TAG}", p, parse_forward_report)
        et_results.append(dict(rank=rank, params=p, **m))

    for r in et_results:
        r["np_dd"] = r["np"] / r["dd"] if r["dd"] > 0 else 0.0

    et_results.sort(key=lambda x: x["np_dd"], reverse=True)

    print(f"\n  {'ET#':<4}  {'NP':>12}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  {'NP/DD':>8}  "
          f"{'S1 TP/SL/EMA':<22}  {'S2 TP/SL/EMA':<22}  {'S3 TP/SL/EMA'}")
    print("  " + "-" * 115)
    for r in et_results:
        p = r["params"]
        print(f"  {r['rank']:<4}  {r['np']:>+12,.0f}  {r['pf']:>6.3f}  {r['dd']:>5.1f}%  {r['trades']:>4}  "
              f"{r['np_dd']:>8,.0f}  {p['tp1']}/{p['sl1']}/EMA{p['ema1']:<10}  "
              f"{p['tp2']}/{p['sl2']}/EMA{p['ema2']:<10}  {p['tp3']}/{p['sl3']}/EMA{p['ema3']}")
    print("=" * 68)


if __name__ == "__main__":
    main()
