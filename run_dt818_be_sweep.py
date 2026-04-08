"""Breakeven parameter sweep for DT818_exp.

Fixes all TP/SL/EMA/Bars at best ET#1 params (mar21 reopt).
Sets RiskMode=2 (breakeven) for all streams.
Sweeps BETrigger and BEBuf per stream — phased forward + reverse.
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
SYMBOL        = "XAUUSD"
PERIOD        = "M30"
SPREAD        = 45
DEPOSIT_OPT   = 10_000_000
DEPOSIT_VAL   = 10_000
RISK_PCT_VAL  = 4.0

TF1_ENUM = 30      # M30
TF2_ENUM = 16388   # H4
TF3_ENUM = 16385   # H1

IS_START = date(2026, 1, 24)
IS_END   = date(2026, 3, 21)
IS_TAG   = "mar21"

OUTPUT_DIR    = Path(f"output/dt818_be_sweep_{IS_TAG}")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

# ── Fixed best ET#1 params ────────────────────────────────────────────────────
S1_FIXED = {"_take_profit": 14500, "_stop_loss": 10500, "_EMA_Period1": 4, "_Bars": 4}
S2_FIXED = {"_take_profit2": 20000, "_stop_loss2": 8500, "_EMA_Period2": 4, "_Bars2": 6}
S3_FIXED = {"_take_profit3": 22500, "_stop_loss3": 7000, "_EMA_Period3": 9, "_Bars3": 6}

# ── BE sweep ranges ───────────────────────────────────────────────────────────
# BETrigger: points of profit before moving SL to break-even
# BEBuf: buffer above entry when setting BE (pts)

S1_BE_PARAMS = [
    ("_BETrigger1", 2500, 500, 500,  12000),   # 23 values
    ("_BEBuf1",     450,  100, 100,  1000),    # 10 values  → 230 passes
]
S2_BE_PARAMS = [
    ("_BETrigger2", 500,  500, 500,  18000),   # 35 values
    ("_BEBuf2",     300,  100, 100,  1000),    # 10 values  → 350 passes
]
S3_BE_PARAMS = [
    ("_BETrigger3", 2000, 500, 500,  20000),   # 39 values
    ("_BEBuf3",     700,  100, 100,  1000),    # 10 values  → 390 passes
]

S1_BE_DEFAULTS = {"_BETrigger1": 2500, "_BEBuf1": 450}
S2_BE_DEFAULTS = {"_BETrigger2": 500,  "_BEBuf2": 300}
S3_BE_DEFAULTS = {"_BETrigger3": 2000, "_BEBuf3": 700}

ALL_PARAM_COLS = [
    "param__BETrigger1", "param__BEBuf1",
    "param__BETrigger2", "param__BEBuf2",
    "param__BETrigger3", "param__BEBuf3",
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

def _extract_s1_be(row):
    return {
        "_BETrigger1": _vi(row, "param__BETrigger1", S1_BE_DEFAULTS["_BETrigger1"]),
        "_BEBuf1":     _vi(row, "param__BEBuf1",     S1_BE_DEFAULTS["_BEBuf1"]),
    }

def _extract_s2_be(row):
    return {
        "_BETrigger2": _vi(row, "param__BETrigger2", S2_BE_DEFAULTS["_BETrigger2"]),
        "_BEBuf2":     _vi(row, "param__BEBuf2",     S2_BE_DEFAULTS["_BEBuf2"]),
    }

def _extract_s3_be(row):
    return {
        "_BETrigger3": _vi(row, "param__BETrigger3", S3_BE_DEFAULTS["_BETrigger3"]),
        "_BEBuf3":     _vi(row, "param__BEBuf3",     S3_BE_DEFAULTS["_BEBuf3"]),
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

def _s1_lines(sweep_be=True, be_fixed=None):
    be = be_fixed or S1_BE_DEFAULTS
    lines = [
        _fix("_Trade1", 1), _fix("_time_frame", TF1_ENUM), _fix("_RiskMode1", 2),
        _fix("_take_profit",  S1_FIXED["_take_profit"]),
        _fix("_stop_loss",    S1_FIXED["_stop_loss"]),
        _fix("_EMA_Period1",  S1_FIXED["_EMA_Period1"]),
        _fix("_Bars",         S1_FIXED["_Bars"]),
        _fix("_TSL1", 500), _fix("_TSLA1", 200),  # TSL irrelevant for RiskMode=2
    ]
    for name, default, step, lo, hi in S1_BE_PARAMS:
        lines.append(_sweep(name, default, step, lo, hi) if sweep_be else _fix(name, be[name]))
    return lines

def _s2_lines(sweep_be=True, be_fixed=None):
    be = be_fixed or S2_BE_DEFAULTS
    lines = [
        _fix("_Trade2", 1), _fix("_time_frame2", TF2_ENUM), _fix("_RiskMode2", 2),
        _fix("_take_profit2",  S2_FIXED["_take_profit2"]),
        _fix("_stop_loss2",    S2_FIXED["_stop_loss2"]),
        _fix("_EMA_Period2",   S2_FIXED["_EMA_Period2"]),
        _fix("_Bars2",         S2_FIXED["_Bars2"]),
        _fix("_TSL2", 4500), _fix("_TSLA2", 1000),
    ]
    for name, default, step, lo, hi in S2_BE_PARAMS:
        lines.append(_sweep(name, default, step, lo, hi) if sweep_be else _fix(name, be[name]))
    return lines

def _s3_lines(sweep_be=True, be_fixed=None):
    be = be_fixed or S3_BE_DEFAULTS
    lines = [
        _fix("_Trade3", 1), _fix("_time_frame3", TF3_ENUM), _fix("_RiskMode3", 2),
        _fix("_take_profit3",  S3_FIXED["_take_profit3"]),
        _fix("_stop_loss3",    S3_FIXED["_stop_loss3"]),
        _fix("_EMA_Period3",   S3_FIXED["_EMA_Period3"]),
        _fix("_Bars3",         S3_FIXED["_Bars3"]),
        _fix("_TSL3", 2000), _fix("_TSLA3", 700),
    ]
    for name, default, step, lo, hi in S3_BE_PARAMS:
        lines.append(_sweep(name, default, step, lo, hi) if sweep_be else _fix(name, be[name]))
    return lines

def _s1_off():
    return [
        _fix("_Trade1", 0), _fix("_time_frame", TF1_ENUM),
        _fix("_BETrigger1", S1_BE_DEFAULTS["_BETrigger1"]),
        _fix("_BEBuf1",     S1_BE_DEFAULTS["_BEBuf1"]),
    ]

def _s2_off():
    return [
        _fix("_Trade2", 0), _fix("_time_frame2", TF2_ENUM),
        _fix("_BETrigger2", S2_BE_DEFAULTS["_BETrigger2"]),
        _fix("_BEBuf2",     S2_BE_DEFAULTS["_BEBuf2"]),
    ]

def _s3_off():
    return [
        _fix("_Trade3", 0), _fix("_time_frame3", TF3_ENUM),
        _fix("_BETrigger3", S3_BE_DEFAULTS["_BETrigger3"]),
        _fix("_BEBuf3",     S3_BE_DEFAULTS["_BEBuf3"]),
    ]


# ── ET validation ─────────────────────────────────────────────────────────────

def _build_val_ini(p, report_id):
    lines = "\n".join([
        "_BaseMagic=1000", "_OrderComment=DT818_A", "_OrderComment2=DT818_B",
        "_OrderComment3=DT818_C",
        "_CapitalProtectionAmount=0.0", "_LotMode=1", f"_RiskPct={RISK_PCT_VAL}",
        "TierBase=2000", "LotStep=0.01",
        "_Trade1=1", f"_time_frame={TF1_ENUM}", "_RiskMode1=2",
        f"_take_profit={S1_FIXED['_take_profit']}", f"_stop_loss={S1_FIXED['_stop_loss']}",
        f"_EMA_Period1={S1_FIXED['_EMA_Period1']}", f"_Bars={S1_FIXED['_Bars']}",
        "_TSL1=500", "_TSLA1=200",
        f"_BETrigger1={p['be1']}", f"_BEBuf1={p['buf1']}",
        "_Trade2=1", f"_time_frame2={TF2_ENUM}", "_RiskMode2=2",
        f"_take_profit2={S2_FIXED['_take_profit2']}", f"_stop_loss2={S2_FIXED['_stop_loss2']}",
        f"_EMA_Period2={S2_FIXED['_EMA_Period2']}", f"_Bars2={S2_FIXED['_Bars2']}",
        "_TSL2=4500", "_TSLA2=1000",
        f"_BETrigger2={p['be2']}", f"_BEBuf2={p['buf2']}",
        "_Trade3=1", f"_time_frame3={TF3_ENUM}", "_RiskMode3=2",
        f"_take_profit3={S3_FIXED['_take_profit3']}", f"_stop_loss3={S3_FIXED['_stop_loss3']}",
        f"_EMA_Period3={S3_FIXED['_EMA_Period3']}", f"_Bars3={S3_FIXED['_Bars3']}",
        "_TSL3=2000", "_TSLA3=700",
        f"_BETrigger3={p['be3']}", f"_BEBuf3={p['buf3']}",
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

    s1_p = len(range(500, 12001, 500)) * len(range(100, 1001, 100))
    s2_p = len(range(500, 18001, 500)) * len(range(100, 1001, 100))
    s3_p = len(range(500, 20001, 500)) * len(range(100, 1001, 100))

    print("=" * 68)
    print(f"  DT818_exp Breakeven Sweep  ({IS_TAG.upper()})")
    print(f"  IS: {IS_START} -> {IS_END}  EA={EA_PATH}  RiskMode=2 (BE)")
    print(f"  Fixed: S1 TP={S1_FIXED['_take_profit']} SL={S1_FIXED['_stop_loss']} EMA={S1_FIXED['_EMA_Period1']} Bars={S1_FIXED['_Bars']}")
    print(f"         S2 TP={S2_FIXED['_take_profit2']} SL={S2_FIXED['_stop_loss2']} EMA={S2_FIXED['_EMA_Period2']} Bars={S2_FIXED['_Bars2']}")
    print(f"         S3 TP={S3_FIXED['_take_profit3']} SL={S3_FIXED['_stop_loss3']} EMA={S3_FIXED['_EMA_Period3']} Bars={S3_FIXED['_Bars3']}")
    print(f"  Sweeping: BETrigger, BEBuf per stream")
    print(f"  Passes per phase: S1={s1_p:,}  S2={s2_p:,}  S3={s3_p:,}")
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
        _common_fixes() + _s1_lines(sweep_be=True) + _s2_off() + _s3_off(),
        ["param__BETrigger1", "param__BEBuf1"],
        *args,
    )
    be1 = _extract_s1_be(best_f1)
    print(f"  -> S1 BE fixed: BETrigger={be1['_BETrigger1']}  BEBuf={be1['_BEBuf1']}")

    best_f2, _ = _run_phase(
        "F2: S2(H4)  [S1 fixed]",
        f"fwd_f2_s2_{IS_TAG}",
        _common_fixes() + _s1_lines(sweep_be=False, be_fixed=be1) + _s2_lines(sweep_be=True) + _s3_off(),
        ["param__BETrigger2", "param__BEBuf2"],
        *args,
    )
    be2 = _extract_s2_be(best_f2)
    print(f"  -> S2 BE fixed: BETrigger={be2['_BETrigger2']}  BEBuf={be2['_BEBuf2']}")

    _, df_f3 = _run_phase(
        "F3: S3(H1)  [S1+S2 fixed]  -- all streams on",
        f"fwd_f3_s3_{IS_TAG}",
        _common_fixes() + _s1_lines(sweep_be=False, be_fixed=be1) + _s2_lines(sweep_be=False, be_fixed=be2) + _s3_lines(sweep_be=True),
        ["param__BETrigger3", "param__BEBuf3"],
        *args,
    )
    df_f3 = df_f3.copy()
    for k, v in {**be1, **be2}.items():
        df_f3[f"param__{k.lstrip('_')}"] = v

    # ════════════════════════════════════════════════════════════════════════
    # REVERSE PASS
    # ════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 68)
    print("  REVERSE PASS: S3 -> S2 -> S1")
    print("=" * 68)

    best_r1, _ = _run_phase(
        "R1: S3(H1) alone",
        f"rev_r1_s3_{IS_TAG}",
        _common_fixes() + _s1_off() + _s2_off() + _s3_lines(sweep_be=True),
        ["param__BETrigger3", "param__BEBuf3"],
        *args,
    )
    be3r = _extract_s3_be(best_r1)
    print(f"  -> S3 BE fixed: BETrigger={be3r['_BETrigger3']}  BEBuf={be3r['_BEBuf3']}")

    best_r2, _ = _run_phase(
        "R2: S2(H4)  [S3 fixed]",
        f"rev_r2_s2_{IS_TAG}",
        _common_fixes() + _s1_off() + _s2_lines(sweep_be=True) + _s3_lines(sweep_be=False, be_fixed=be3r),
        ["param__BETrigger2", "param__BEBuf2"],
        *args,
    )
    be2r = _extract_s2_be(best_r2)
    print(f"  -> S2 BE fixed: BETrigger={be2r['_BETrigger2']}  BEBuf={be2r['_BEBuf2']}")

    _, df_r3 = _run_phase(
        "R3: S1(M30)  [S3+S2 fixed]  -- all streams on",
        f"rev_r3_s1_{IS_TAG}",
        _common_fixes() + _s1_lines(sweep_be=True) + _s2_lines(sweep_be=False, be_fixed=be2r) + _s3_lines(sweep_be=False, be_fixed=be3r),
        ["param__BETrigger1", "param__BEBuf1"],
        *args,
    )
    df_r3 = df_r3.copy()
    for k, v in {**be2r, **be3r}.items():
        df_r3[f"param__{k.lstrip('_')}"] = v

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

    print(f"\n  {'#':<3}  {'NP':>10}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  "
          f"{'S1 BETrig/Buf':<16}  {'S2 BETrig/Buf':<16}  {'S3 BETrig/Buf'}")
    print("  " + "-" * 85)
    for rank, (_, row) in enumerate(top5.iterrows(), 1):
        np_v  = float(row.get("net_profit",    0))
        pf_v  = float(row.get("profit_factor", 0))
        dd_v  = float(row.get("drawdown_pct",  0))
        tr_v  = _vi(row, "trades", 0)
        be1_v = _vi(row, "param__BETrigger1", 0); buf1 = _vi(row, "param__BEBuf1", 0)
        be2_v = _vi(row, "param__BETrigger2", 0); buf2 = _vi(row, "param__BEBuf2", 0)
        be3_v = _vi(row, "param__BETrigger3", 0); buf3 = _vi(row, "param__BEBuf3", 0)
        print(f"  {rank:<3}  {np_v:>+10,.0f}  {pf_v:>6.3f}  {dd_v:>5.1f}%  {tr_v:>4}  "
              f"{be1_v}/{buf1:<10}  {be2_v}/{buf2:<10}  {be3_v}/{buf3}")

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
            be1=_vi(row, "param__BETrigger1", 0), buf1=_vi(row, "param__BEBuf1", 0),
            be2=_vi(row, "param__BETrigger2", 0), buf2=_vi(row, "param__BEBuf2", 0),
            be3=_vi(row, "param__BETrigger3", 0), buf3=_vi(row, "param__BEBuf3", 0),
        )
        print(f"\n-- ET #{rank} --")
        m = _run_val(f"et_val_{rank}_{IS_TAG}", p, parse_forward_report)
        et_results.append(dict(rank=rank, params=p, **m))

    for r in et_results:
        r["np_dd"] = r["np"] / r["dd"] if r["dd"] > 0 else 0.0

    et_results.sort(key=lambda x: x["np_dd"], reverse=True)

    print(f"\n  {'ET#':<4}  {'NP':>10}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  {'NP/DD':>8}  "
          f"{'S1 BETrig/Buf':<16}  {'S2 BETrig/Buf':<16}  {'S3 BETrig/Buf'}")
    print("  " + "-" * 100)
    for r in et_results:
        p = r["params"]
        print(f"  {r['rank']:<4}  {r['np']:>+10,.0f}  {r['pf']:>6.3f}  {r['dd']:>5.1f}%  {r['trades']:>4}  "
              f"{r['np_dd']:>8,.0f}  {p['be1']}/{p['buf1']:<10}  {p['be2']}/{p['buf2']:<10}  {p['be3']}/{p['buf3']}")

    print("=" * 68)
    print(f"\n  Baseline (no BE, RiskMode=0): NP=+78,131  DD=~2.5%  NP/DD=3,164")


if __name__ == "__main__":
    main()
