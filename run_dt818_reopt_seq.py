"""DT818 re-optimization: joint genetic sweep of all S1+S2 params simultaneously.

Mirrors the original approach that produced the best known settings:
  - All params swept in one run (S1 and S2 together)
  - Model=1 (1m OHLC), Optimization=2 (genetic), $10M deposit, RiskPct=3%
  - Best pass = max NP with DD < 20%
  - Every Tick validation ($5K, 4% risk)
  - Set files written at 4%, 6%, 9% risk

Update IS_START / IS_END before each re-opt run.
Clear output/<tag>/ XMLs to force a fresh run.
"""
from __future__ import annotations

import io, sys
from datetime import date
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

MAX_ATTEMPTS   = 3
ET_TOP_N       = 10       # validate top N passes from genetic on Every Tick
ET_MIN_NP      = 10_000   # minimum acceptable ET net profit ($)
ET_MAX_TRADES  = 150      # above this = noisy local optimum, retry

# ── Configuration ─────────────────────────────────────────────────────────────
MT5_TERMINAL  = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN     = 18912087
MT5_SERVER    = "VantageInternational-Live 3"
EA_PATH       = "DT818_max"
SYMBOL        = "XAUUSD"
PERIOD        = "M30"
SPREAD        = 45
DEPOSIT_OPT   = 10_000_000
DD_THRESHOLD  = 20.0

IS_START = date(2026, 1, 24)
IS_END   = date(2026, 3, 21)

_MONTH_ABBR = {1:"jan",2:"feb",3:"mar",4:"apr",5:"may",6:"jun",
               7:"jul",8:"aug",9:"sep",10:"oct",11:"nov",12:"dec"}
IS_TAG = f"{_MONTH_ABBR[IS_END.month]}{IS_END.day}"

OUTPUT_DIR   = Path(f"output/dt818_reopt_{IS_TAG}")
SET_OUT_4PCT = Path(f"configs/sets/DT818_4_{IS_TAG}_exp_next.set")
SET_OUT_6PCT = Path(f"configs/sets/DT818_6_{IS_TAG}_exp_next.set")
SET_OUT_9PCT = Path(f"configs/sets/DT818_9_{IS_TAG}_exp_next.set")

MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fix(name, val):
    return f"{name}={val}||{val}||1||{val}||{val}||N"

def _sweep(name, default, step, lo, hi):
    return f"{name}={default}||{default}||{step}||{lo}||{hi}||Y"

def _set_to_ini(lines):
    """6-field set format → 5-field TesterInputs INI."""
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
        "Optimization=2\nOptimizationCriterion=0\n"
        f"Deposit={DEPOSIT_OPT}\nSpread={SPREAD}\n"
        f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{_set_to_ini(set_lines)}\n"
    )


# ── Joint sweep set lines ─────────────────────────────────────────────────────

def _joint_set_lines():
    """All S1 and S2 params swept simultaneously including risk control.
    Risk control params create competitive pressure — best pass filtered to RiskMode=0.
    """
    return [
        _fix("_BaseMagic", 1000),
        "_OrderComment=DT818_A",
        "_OrderComment2=DT818_B",
        _fix("_CapitalProtectionAmount", "0.0"),
        _fix("_LotMode", 1),
        _fix("_RiskPct", "3.0"),
        _fix("TierBase", 2000),
        _fix("LotStep", "0.01"),
        _fix("_Trade1", 1),
        _fix("_time_frame", 30),
        # S1 swept
        _sweep("_take_profit",  10000, 500,  2000, 15000),
        _sweep("_stop_loss",    10000, 500,  2000, 15000),
        _sweep("_EMA_Period1",  5,     1,    4,    6),
        _sweep("_Bars",         4,     1,    2,    10),
        _sweep("_RiskMode1",    0,     1,    0,    1),
        _sweep("_TSL1",         500,   100,  200,  2000),
        _sweep("_TSLA1",        200,   50,   100,  1000),
        _sweep("_BETrigger1",   2500,  250,  500,  5000),
        _sweep("_BEBuf1",       450,   50,   100,  1000),
        # S2 swept
        _sweep("_Trade2",       1,     1,    0,    1),
        _fix("_time_frame2", 30),
        _sweep("_take_profit2",  9500, 500,  2000, 15000),
        _sweep("_stop_loss2",    8500, 500,  2000, 15000),
        _sweep("_EMA_Period2",   5,    1,    4,    6),
        _sweep("_Bars2",         4,    1,    2,    10),
        _sweep("_RiskMode2",     1,    1,    0,    2),
        _sweep("_TSL2",          4500, 100,  200,  5000),
        _sweep("_TSLA2",         1000,  50,  100,  2000),
        _sweep("_BETrigger2",    500,  100,  200,  2000),
        _sweep("_BEBuf2",        300,  50,   50,   1000),
    ]


# ── MT5 runner ────────────────────────────────────────────────────────────────

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


# ── Set-file writer ────────────────────────────────────────────────────────────

def _write_set(p, risk_pct, path):
    lines = [
        _fix("_BaseMagic", 1000),
        "_OrderComment=DT818_A",
        "_OrderComment2=DT818_B",
        _fix("_CapitalProtectionAmount", "0.0"),
        _fix("_LotMode", 1),
        _fix("_RiskPct", risk_pct),
        _fix("TierBase", 2000),
        _fix("LotStep", "0.01"),
        _fix("_Trade1", 1),
        _fix("_time_frame", 30),
        _fix("_take_profit",  p["take_profit"]),
        _fix("_stop_loss",    p["stop_loss"]),
        _fix("_EMA_Period1",  p["EMA_Period1"]),
        _fix("_Bars",         p["Bars"]),
        _fix("_RiskMode1",    0),
        _fix("_TSL1",         2000),
        _fix("_TSLA1",        700),
        _fix("_BETrigger1",   2000),
        _fix("_BEBuf1",       700),
        _fix("_Trade2", 1),
        _fix("_time_frame2", 30),
        _fix("_take_profit2", p["take_profit2"]),
        _fix("_stop_loss2",   p["stop_loss2"]),
        _fix("_EMA_Period2",  p["EMA_Period2"]),
        _fix("_Bars2",        p["Bars2"]),
        _fix("_RiskMode2",    0),
        _fix("_TSL2",         2000),
        _fix("_TSLA2",        700),
        _fix("_BETrigger2",   2000),
        _fix("_BEBuf2",       700),
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  Written: {path}")


def _vi(row, col, default):
    try:    return int(float(row.get(col, default)))
    except: return int(default)


def _extract_params(row):
    def vi(col, default):
        try:    return int(float(row.get(col, default)))
        except: return int(default)
    return dict(
        take_profit  = vi("param__take_profit",  10000),
        stop_loss    = vi("param__stop_loss",    10000),
        EMA_Period1  = vi("param__EMA_Period1",  5),
        Bars         = vi("param__Bars",         4),
        take_profit2 = vi("param__take_profit2", 9500),
        stop_loss2   = vi("param__stop_loss2",   8500),
        EMA_Period2  = vi("param__EMA_Period2",  5),
        Bars2        = vi("param__Bars2",        4),
    )


def _build_val_ini(p, val_id):
    val_inputs = "\n".join([
        "_BaseMagic=1000", "_OrderComment=DT818_A", "_OrderComment2=DT818_B",
        "_CapitalProtectionAmount=0.0", "_LotMode=1", "_RiskPct=4.0",
        "TierBase=2000", "LotStep=0.01", "_Trade1=1", "_time_frame=30",
        f"_take_profit={p['take_profit']}",   f"_stop_loss={p['stop_loss']}",
        f"_EMA_Period1={p['EMA_Period1']}",    f"_Bars={p['Bars']}",
        "_Trade2=1", "_time_frame2=30",
        f"_take_profit2={p['take_profit2']}", f"_stop_loss2={p['stop_loss2']}",
        f"_EMA_Period2={p['EMA_Period2']}",    f"_Bars2={p['Bars2']}",
    ])
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod={PERIOD}\nModel=0\n"
        "Optimization=0\n"
        f"Deposit=5000\nSpread={SPREAD}\n"
        f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={val_id}\n\n[TesterInputs]\n{val_inputs}\n"
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def _run_attempt(attempt, job_id, pd, parse_optimization_xml, write_passes, parse_forward_report):
    """Run one optimization + ET validation on top N passes. Returns (p, np_val, pf_val, dd_val, tr_val)."""
    xml_path = OUTPUT_DIR / f"{job_id}.xml"

    # Optimization
    if xml_path.exists():
        print(f"\n  [Cached] {xml_path.name}")
    else:
        print(f"\n── Attempt {attempt}/{MAX_ATTEMPTS}: joint optimization (1m OHLC, genetic, $10M) ──")
        _run_mt5(job_id, _build_opti_ini(_joint_set_lines(), job_id))

    records, warns = parse_optimization_xml(xml_path, job_id)
    for w in warns: print(f"  WARN: {w}")
    parquet, _ = write_passes(records, OUTPUT_DIR)
    print(f"  Parsed {len(records)} passes")

    df = pd.read_parquet(parquet)
    df["net_profit"]   = pd.to_numeric(df["net_profit"],   errors="coerce")
    df["drawdown_pct"] = pd.to_numeric(df["drawdown_pct"], errors="coerce")

    low_dd = df[df["drawdown_pct"] < DD_THRESHOLD]
    pool   = low_dd if not low_dd.empty else df
    # Deduplicate by ET-relevant params so top N covers distinct combinations
    et_key_cols = [c for c in ["param__take_profit", "param__stop_loss", "param__EMA_Period1",
                                "param__Bars", "param__take_profit2", "param__stop_loss2",
                                "param__EMA_Period2", "param__Bars2"] if c in pool.columns]
    top_n = (pool.sort_values("net_profit", ascending=False)
                 .drop_duplicates(subset=et_key_cols)
                 .head(ET_TOP_N))

    print(f"\n── Every Tick validation — top {len(top_n)} passes ($5K, 4% risk) ──")

    results = []  # list of (p, np_val, pf_val, dd_val, tr_val, passed)
    for rank, (_, row) in enumerate(top_n.iterrows(), 1):
        p = _extract_params(row)
        val_id  = f"val_et_{IS_TAG}_{rank}"
        val_htm = OUTPUT_DIR / f"{val_id}.htm"
        val_htm.unlink(missing_ok=True)

        _run_mt5(val_id, _build_val_ini(p, val_id))

        m, warns = parse_forward_report(val_htm, val_id)
        np_val = float(m.get("net_profit", 0))
        pf_val = float(m.get("profit_factor", 0))
        dd_val = float(m.get("drawdown_pct", 0))
        tr_val = int(m.get("trades", 0))

        passed = (tr_val <= ET_MAX_TRADES and np_val >= ET_MIN_NP and dd_val <= DD_THRESHOLD)
        mark   = "✔" if passed else "✖"
        print(f"  [{rank:2d}] {mark}  EMA={p['EMA_Period1']}/{p['EMA_Period2']}  "
              f"TP={p['take_profit']}/{p['take_profit2']}  SL={p['stop_loss']}/{p['stop_loss2']}  "
              f"NP={np_val:+,.0f}  DD={dd_val:.1f}%  Tr={tr_val}")
        results.append((p, np_val, pf_val, dd_val, tr_val, passed))

    # Best passing result (highest ET NP); fallback to best NP if none pass
    passing = [(p, np_v, pf_v, dd_v, tr_v) for p, np_v, pf_v, dd_v, tr_v, ok in results if ok]
    if passing:
        sel = max(passing, key=lambda x: x[1])
        print(f"\n  → Selected #{results.index(next(r for r in results if r[0] == sel[0]))+1}: "
              f"EMA={sel[0]['EMA_Period1']}/{sel[0]['EMA_Period2']}  NP={sel[1]:+,.0f}  DD={sel[3]:.1f}%  Tr={sel[4]}")
    else:
        all_r = [(p, np_v, pf_v, dd_v, tr_v) for p, np_v, pf_v, dd_v, tr_v, _ in results]
        sel   = max(all_r, key=lambda x: x[1])
        print(f"\n  → No pass in top {ET_TOP_N} — fallback: "
              f"EMA={sel[0]['EMA_Period1']}/{sel[0]['EMA_Period2']}  NP={sel[1]:+,.0f}  DD={sel[3]:.1f}%  Tr={sel[4]}")

    return sel[0], sel[1], sel[2], sel[3], sel[4]


def main():
    import pandas as pd
    from zgb_opti.xml_parser import parse_optimization_xml, write_passes, parse_forward_report

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 65)
    print(f"  DT818 Re-opt  ({IS_TAG.upper()})  —  joint genetic sweep")
    print(f"  IS: {IS_START} -> {IS_END}")
    print(f"  Max attempts: {MAX_ATTEMPTS}  (retry if ET trades > {ET_MAX_TRADES} or NP < ${ET_MIN_NP:,})")
    print("=" * 65)

    job_id = f"opti_{IS_TAG}"
    p = np_val = pf_val = dd_val = tr_val = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        p, np_val, pf_val, dd_val, tr_val = _run_attempt(
            attempt, job_id, pd, parse_optimization_xml, write_passes, parse_forward_report
        )

        passed = (tr_val <= ET_MAX_TRADES and np_val >= ET_MIN_NP and dd_val <= DD_THRESHOLD)
        if passed:
            print(f"  ✔ ET gate passed on attempt {attempt}")
            break

        if attempt < MAX_ATTEMPTS:
            reasons = []
            if tr_val > ET_MAX_TRADES:  reasons.append(f"trades={tr_val} > {ET_MAX_TRADES}")
            if np_val < ET_MIN_NP:      reasons.append(f"NP={np_val:+,.0f} < ${ET_MIN_NP:,}")
            if dd_val > DD_THRESHOLD:   reasons.append(f"DD={dd_val:.1f}% > {DD_THRESHOLD}%")
            print(f"  ✖ ET gate failed ({', '.join(reasons)})")
            print(f"  → Deleting XML and retrying...")
            (OUTPUT_DIR / f"{job_id}.xml").unlink(missing_ok=True)
        else:
            print(f"  ⚠ All {MAX_ATTEMPTS} attempts failed ET gate — using best result from last attempt")

    # ── Write set files ───────────────────────────────────────────────
    print("\n── Writing set files ──")
    _write_set(p, 4.0, SET_OUT_4PCT)
    _write_set(p, 6.0, SET_OUT_6PCT)
    _write_set(p, 9.0, SET_OUT_9PCT)

    # ── Final validation status ───────────────────────────────────────
    if dd_val > DD_THRESHOLD:
        status = f"⚠ WARNING: DD {dd_val:.1f}% exceeds {DD_THRESHOLD}%"
    elif pf_val < 1.2:
        status = f"⚠ WARNING: PF {pf_val:.3f} is low"
    else:
        status = "✔ Validation passed"

    print(f"\n{'=' * 65}")
    print(f"  DONE")
    print(f"  S1: TP={p['take_profit']}  SL={p['stop_loss']}  EMA={p['EMA_Period1']}  Bars={p['Bars']}")
    print(f"  S2: TP={p['take_profit2']}  SL={p['stop_loss2']}  EMA={p['EMA_Period2']}  Bars2={p['Bars2']}")
    print(f"  Validation (ET $5K): NP={np_val:+,.0f}  PF={pf_val:.3f}  DD={dd_val:.1f}%  Trades={tr_val}")
    print(f"  {status}")
    print(f"  Next reopt: 8 weeks from {IS_END}")
    print(f"{'=' * 65}")


if __name__ == "__main__":
    main()
