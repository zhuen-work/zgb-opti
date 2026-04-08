"""DT818_exp S3 param sweep: S1+S2 fixed at H1×H4 best, sweep S3 (M30).

S1 (H1) fixed: TP=17000, SL=7000, BB=35, Dev=1.5
S2 (H4) fixed: TP=33000, SL=6500, BB=35, Dev=1.5
S3 (M30) sweep: TP, SL, BB period, BB dev

Chart period: M30 (finest TF of the three).
IS period: Jan 24 -> Mar 21, 2026.
"""
from __future__ import annotations

import io, sys
from datetime import date
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

MAX_ATTEMPTS  = 3
ET_TOP_N      = 10
ET_MIN_NP     = 10_000
ET_MAX_TRADES = 150
DD_THRESHOLD  = 20.0
ET_RISK_PCT   = 4.0

MT5_TERMINAL  = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN     = 18912087
MT5_SERVER    = "VantageInternational-Live 3"
EA_PATH       = "DT818_exp"
SYMBOL        = "XAUUSD"
PERIOD        = "M30"
SPREAD        = 45
DEPOSIT_OPT   = 10_000_000

# S1+S2 best params from H1×H4 reopt
S1_TP = 17000; S1_SL = 7000;  S1_BB = 35; S1_DEV = 1.5  # H1
S2_TP = 33000; S2_SL = 6500;  S2_BB = 35; S2_DEV = 1.5  # H4

TF1_ENUM = 16385  # H1
TF2_ENUM = 16388  # H4
TF3_ENUM = 30     # M30

IS_START = date(2026, 1, 24)
IS_END   = date(2026, 3, 21)
IS_TAG   = "mar21"

OUTPUT_DIR    = Path(f"output/dt818_exp_s3_{IS_TAG}")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")


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


def _s3_set_lines():
    return [
        _fix("_BaseMagic", 1000),
        "_OrderComment=DT818_A",
        "_OrderComment2=DT818_B",
        "_OrderComment3=DT818_C",
        _fix("_CapitalProtectionAmount", "0.0"),
        _fix("_LotMode", 1),
        _fix("_RiskPct", "3.0"),
        _fix("TierBase", 2000),
        _fix("LotStep", "0.01"),
        # S1 fixed
        _fix("_Trade1", 1),
        _fix("_time_frame", TF1_ENUM),
        _fix("_take_profit", S1_TP),
        _fix("_stop_loss",   S1_SL),
        _fix("_EMA_Period1", 2),
        _fix("_BB_Period1",  S1_BB),
        _fix("_BB_Dev1",     S1_DEV),
        _fix("_RiskMode1",   0),
        # S2 fixed
        _fix("_Trade2", 1),
        _fix("_time_frame2", TF2_ENUM),
        _fix("_take_profit2", S2_TP),
        _fix("_stop_loss2",   S2_SL),
        _fix("_EMA_Period2",  2),
        _fix("_BB_Period2",   S2_BB),
        _fix("_BB_Dev2",      S2_DEV),
        _fix("_RiskMode2",    0),
        # S3 sweep
        _fix("_Trade3", 1),
        _fix("_time_frame3", TF3_ENUM),
        _sweep("_take_profit3", 9000,  500, 3000, 20000),
        _sweep("_stop_loss3",   7000,  500, 2000, 12000),
        _fix("_EMA_Period3",  2),
        _sweep("_BB_Period3", 30, 5, 10, 50),
        _sweep("_BB_Dev3",  "1.5", "0.5", "1.5", "3.0"),
        _fix("_RiskMode3",    0),
    ]


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

def _build_val_ini(p, val_id):
    val_inputs = "\n".join([
        "_BaseMagic=1000", "_OrderComment=DT818_A", "_OrderComment2=DT818_B",
        "_OrderComment3=DT818_C",
        "_CapitalProtectionAmount=0.0", "_LotMode=1", f"_RiskPct={ET_RISK_PCT}",
        "TierBase=2000", "LotStep=0.01",
        "_Trade1=1",
        f"_time_frame={TF1_ENUM}",
        f"_take_profit={S1_TP}", f"_stop_loss={S1_SL}",
        "_EMA_Period1=2", f"_BB_Period1={S1_BB}", f"_BB_Dev1={S1_DEV}", "_RiskMode1=0",
        "_Trade2=1",
        f"_time_frame2={TF2_ENUM}",
        f"_take_profit2={S2_TP}", f"_stop_loss2={S2_SL}",
        "_EMA_Period2=2", f"_BB_Period2={S2_BB}", f"_BB_Dev2={S2_DEV}", "_RiskMode2=0",
        "_Trade3=1",
        f"_time_frame3={TF3_ENUM}",
        f"_take_profit3={p['take_profit3']}", f"_stop_loss3={p['stop_loss3']}",
        "_EMA_Period3=2",
        f"_BB_Period3={p['BB_Period3']}", f"_BB_Dev3={p['BB_Dev3']}",
        "_RiskMode3=0",
    ])
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod={PERIOD}\nModel=0\n"
        "Optimization=0\n"
        f"Deposit=10000\nSpread={SPREAD}\n"
        f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={val_id}\n\n[TesterInputs]\n{val_inputs}\n"
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
        for ext in (".xml", ".htm", ".html"):
            c = MT5_DATA_ROOT / f"{job_id}{ext}"
            if c.exists(): art = c; break
    if art is None:
        raise RuntimeError(f"No report found for {job_id}")
    collected = copy_report_artifact(art, OUTPUT_DIR)
    print(f"  Collected: {collected.name}  ({collected.stat().st_size/1e6:.1f}MB)")
    return collected


def _extract_params(row):
    def vf(col, d):
        try: return float(row.get(col, d))
        except: return float(d)
    def vi(col, d):
        try: return int(float(row.get(col, d)))
        except: return int(d)
    return dict(
        take_profit3 = vi("param__take_profit3", 9000),
        stop_loss3   = vi("param__stop_loss3",   7000),
        BB_Period3   = vi("param__BB_Period3",   30),
        BB_Dev3      = vf("param__BB_Dev3",      1.5),
    )


def _run_attempt(attempt, job_id, pd, parse_optimization_xml, write_passes, parse_forward_report):
    xml_path = OUTPUT_DIR / f"{job_id}.xml"

    if xml_path.exists():
        print(f"\n  [Cached] {xml_path.name}")
    else:
        print(f"\n── Attempt {attempt}/{MAX_ATTEMPTS}: S3 genetic sweep (1m OHLC, $10M) ──")
        _run_mt5(job_id, _build_opti_ini(_s3_set_lines(), job_id))

    records, warns = parse_optimization_xml(xml_path, job_id)
    for w in warns: print(f"  WARN: {w}")
    parquet, _ = write_passes(records, OUTPUT_DIR)
    print(f"  Parsed {len(records)} passes")

    df = pd.read_parquet(parquet)
    df["net_profit"]   = pd.to_numeric(df["net_profit"],   errors="coerce")
    df["drawdown_pct"] = pd.to_numeric(df["drawdown_pct"], errors="coerce")

    low_dd = df[df["drawdown_pct"] < DD_THRESHOLD]
    pool   = low_dd if not low_dd.empty else df
    et_key_cols = [c for c in [
        "param__take_profit3", "param__stop_loss3", "param__BB_Period3", "param__BB_Dev3",
    ] if c in pool.columns]
    top_n = (pool.sort_values("net_profit", ascending=False)
                 .drop_duplicates(subset=et_key_cols)
                 .head(ET_TOP_N))

    print(f"\n── ET validation — top {len(top_n)} passes ($10K, {ET_RISK_PCT}% risk) ──")

    results = []
    for rank, (_, row) in enumerate(top_n.iterrows(), 1):
        p      = _extract_params(row)
        val_id = f"val_et_exp_s3_{IS_TAG}_a{attempt}_{rank}"
        (OUTPUT_DIR / f"{val_id}.htm").unlink(missing_ok=True)
        _run_mt5(val_id, _build_val_ini(p, val_id))
        m, _ = parse_forward_report(OUTPUT_DIR / f"{val_id}.htm", val_id)
        np_v = float(m.get("net_profit", 0))
        pf_v = float(m.get("profit_factor", 0))
        dd_v = float(m.get("drawdown_pct", 0))
        tr_v = int(m.get("trades", 0))
        passed = (tr_v <= ET_MAX_TRADES and np_v >= ET_MIN_NP and dd_v <= DD_THRESHOLD)
        mark = "✔" if passed else "✖"
        print(f"  [{rank:2d}] {mark}  BB3={p['BB_Period3']}  Dev3={p['BB_Dev3']}  "
              f"TP3={p['take_profit3']}  SL3={p['stop_loss3']}  "
              f"NP={np_v:+,.0f}  DD={dd_v:.1f}%  Tr={tr_v}")
        results.append((p, np_v, pf_v, dd_v, tr_v, passed))

    passing = [(p, np_v, pf_v, dd_v, tr_v) for p, np_v, pf_v, dd_v, tr_v, ok in results if ok]
    if passing:
        sel = max(passing, key=lambda x: x[1])
        print(f"\n  → Selected: BB3={sel[0]['BB_Period3']}  Dev3={sel[0]['BB_Dev3']}  "
              f"NP={sel[1]:+,.0f}  DD={sel[3]:.1f}%  Tr={sel[4]}")
    else:
        all_r = [(p, np_v, pf_v, dd_v, tr_v) for p, np_v, pf_v, dd_v, tr_v, _ in results]
        sel   = max(all_r, key=lambda x: x[1])
        print(f"\n  → No pass — fallback: BB3={sel[0]['BB_Period3']}  "
              f"NP={sel[1]:+,.0f}  DD={sel[3]:.1f}%  Tr={sel[4]}")

    return sel[0], sel[1], sel[2], sel[3], sel[4]


def main():
    import pandas as pd
    from zgb_opti.xml_parser import parse_optimization_xml, write_passes, parse_forward_report

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 68)
    print(f"  DT818_exp S3 sweep  ({IS_TAG.upper()})")
    print(f"  IS: {IS_START} -> {IS_END}  Chart=M30  EA=DT818_exp")
    print(f"  S1 (H1) fixed: TP={S1_TP}  SL={S1_SL}  BB={S1_BB}  Dev={S1_DEV}")
    print(f"  S2 (H4) fixed: TP={S2_TP}  SL={S2_SL}  BB={S2_BB}  Dev={S2_DEV}")
    print(f"  S3 (M30) sweep: TP/SL, BB period 10-50, BB dev 1.5-3.0")
    print(f"  Gate: Trades <= {ET_MAX_TRADES}, NP >= ${ET_MIN_NP:,}, DD <= {DD_THRESHOLD}%")
    print(f"  Baseline (DT818_max M30×H4 @ 4%): NP=+$41,397  PF=2.090  DD=11.8%  Tr=88")
    print("=" * 68)

    job_id = f"opti_exp_s3_{IS_TAG}"
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
            if tr_val > ET_MAX_TRADES: reasons.append(f"trades={tr_val} > {ET_MAX_TRADES}")
            if np_val < ET_MIN_NP:     reasons.append(f"NP={np_val:+,.0f} < ${ET_MIN_NP:,}")
            if dd_val > DD_THRESHOLD:  reasons.append(f"DD={dd_val:.1f}% > {DD_THRESHOLD}%")
            print(f"  ✖ ET gate failed ({', '.join(reasons)})")
            print(f"  → Deleting XML and retrying...")
            (OUTPUT_DIR / f"{job_id}.xml").unlink(missing_ok=True)
        else:
            print(f"  ⚠ All {MAX_ATTEMPTS} attempts failed ET gate — using best result")

    print(f"\n{'=' * 68}")
    print(f"  DONE  —  DT818_exp  H1×H4+M30  ({IS_TAG.upper()})")
    print(f"  S1 (H1): TP={S1_TP}  SL={S1_SL}  BB={S1_BB}  Dev={S1_DEV}  [fixed]")
    print(f"  S2 (H4): TP={S2_TP}  SL={S2_SL}  BB={S2_BB}  Dev={S2_DEV}  [fixed]")
    print(f"  S3 (M30): TP={p['take_profit3']}  SL={p['stop_loss3']}  BB={p['BB_Period3']}  Dev={p['BB_Dev3']}")
    print(f"  ET {ET_RISK_PCT}% risk: NP={np_val:+,.0f}  PF={pf_val:.3f}  DD={dd_val:.1f}%  Trades={tr_val}")
    print(f"  H1×H4 only (baseline): NP=+$30,039  PF=3.930  DD=17.2%  Trades=29")
    print(f"  DT818_max M30×H4:      NP=+$41,397  PF=2.090  DD=11.8%  Trades=88")
    print(f"{'=' * 68}")


if __name__ == "__main__":
    main()
