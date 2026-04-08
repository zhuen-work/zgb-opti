"""DT818_max MaxEntries study: run separate reopt for MaxEntries=1, 2, 3.
Gates scale proportionally: Trades <= 150*N, DD <= 20%*N.
"""
from __future__ import annotations

import io, sys
from datetime import date
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

MAX_ATTEMPTS     = 3
ET_TOP_N         = 10
ET_MIN_NP        = 10_000
ET_BASE_TRADES   = 150     # gate scales as ET_BASE_TRADES * MaxEntries
ET_BASE_DD       = 20.0    # gate scales as ET_BASE_DD * MaxEntries
ET_RISK_PCT      = 4.0
MAX_ENTRIES_LIST = [1, 2, 3]

MT5_TERMINAL  = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN     = 18912087
MT5_SERVER    = "VantageInternational-Live 3"
EA_PATH       = "DT818_max"
SYMBOL        = "XAUUSD"
PERIOD        = "M30"
SPREAD        = 45
DEPOSIT_OPT   = 10_000_000
DEPOSIT_VAL   = 5_000

TF1_ENUM = 30
TF2_ENUM = 16388

IS_START = date(2026, 1, 24)
IS_END   = date(2026, 3, 21)
IS_TAG   = "mar21"

MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")


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


def _joint_set_lines(max_entries):
    return [
        _fix("_BaseMagic", 1000),
        "_OrderComment=DT818_A",
        "_OrderComment2=DT818_B",
        _fix("_CapitalProtectionAmount", "0.0"),
        _fix("_MaxEntries", max_entries),
        _fix("_LotMode", 1),
        _fix("_RiskPct", "3.0"),
        _fix("TierBase", 2000),
        _fix("LotStep", "0.01"),
        _fix("_Trade1", 1),
        _fix("_time_frame", TF1_ENUM),
        _sweep("_take_profit",  10000, 500,  2000, 15000),
        _sweep("_stop_loss",    10000, 500,  2000, 15000),
        _sweep("_EMA_Period1",  5,     1,    4,    6),
        _sweep("_Bars",         4,     2,    2,    6),
        _fix("_RiskMode1",    0),
        _fix("_TSL1",         500),
        _fix("_TSLA1",        200),
        _fix("_BETrigger1",   2500),
        _fix("_BEBuf1",       450),
        _sweep("_Trade2",       1,     1,    0,    1),
        _fix("_time_frame2", TF2_ENUM),
        _sweep("_take_profit2",  9500, 500,  2000, 15000),
        _sweep("_stop_loss2",    8500, 500,  2000, 15000),
        _sweep("_EMA_Period2",   5,    1,    4,    6),
        _sweep("_Bars2",         4,    2,    2,    6),
        _fix("_RiskMode2",    0),
        _fix("_TSL2",         4500),
        _fix("_TSLA2",        1000),
        _fix("_BETrigger2",   500),
        _fix("_BEBuf2",       300),
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


def _build_val_ini(p, val_id, max_entries):
    val_inputs = "\n".join([
        "_BaseMagic=1000", "_OrderComment=DT818_A", "_OrderComment2=DT818_B",
        "_CapitalProtectionAmount=0.0", f"_MaxEntries={max_entries}",
        "_LotMode=1", f"_RiskPct={ET_RISK_PCT}",
        "TierBase=2000", "LotStep=0.01", "_Trade1=1",
        f"_time_frame={TF1_ENUM}",
        f"_take_profit={p['take_profit']}",   f"_stop_loss={p['stop_loss']}",
        f"_EMA_Period1={p['EMA_Period1']}",    f"_Bars={p['Bars']}",
        "_Trade2=1",
        f"_time_frame2={TF2_ENUM}",
        f"_take_profit2={p['take_profit2']}", f"_stop_loss2={p['stop_loss2']}",
        f"_EMA_Period2={p['EMA_Period2']}",    f"_Bars2={p['Bars2']}",
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
        f"Report={val_id}\n\n[TesterInputs]\n{val_inputs}\n"
    )


def _run_mt5(job_id, ini_content, output_dir):
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    ini_path = output_dir / f"{job_id}.ini"
    ini_path.write_text(ini_content, encoding="utf-8")
    print(f"  Launching: {job_id}")
    rc = run_mt5_job(MT5_TERMINAL, str(ini_path))
    print(f"  Exit code: {rc}")
    art = find_report_artifact(output_dir, job_id, MT5_TERMINAL)
    if art is None:
        for ext in (".xml", ".htm", ".html"):
            c = MT5_DATA_ROOT / f"{job_id}{ext}"
            if c.exists(): art = c; break
    if art is None:
        raise RuntimeError(f"No report found for {job_id}")
    collected = copy_report_artifact(art, output_dir)
    print(f"  Collected: {collected.name}  ({collected.stat().st_size/1e6:.1f}MB)")
    return collected


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


def _run_one(max_entries, pd, parse_optimization_xml, write_passes, parse_forward_report):
    output_dir   = Path(f"output/dt818_maxentries_{max_entries}_{IS_TAG}")
    output_dir.mkdir(parents=True, exist_ok=True)

    max_trades = ET_BASE_TRADES * max_entries
    max_dd     = ET_BASE_DD * max_entries
    job_id     = f"opti_maxe{max_entries}_{IS_TAG}"

    print(f"\n{'='*65}")
    print(f"  MaxEntries={max_entries}  |  Gate: Trades<={max_trades}, DD<={max_dd:.0f}%, NP>=${ET_MIN_NP:,}")
    print(f"{'='*65}")

    best_p = best_np = best_pf = best_dd = best_tr = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        xml_path = output_dir / f"{job_id}.xml"

        if xml_path.exists():
            print(f"\n  [Cached] {xml_path.name}")
        else:
            print(f"\n── Attempt {attempt}/{MAX_ATTEMPTS}: genetic sweep ──")
            _run_mt5(job_id, _build_opti_ini(_joint_set_lines(max_entries), job_id), output_dir)

        records, warns = parse_optimization_xml(xml_path, job_id)
        for w in warns: print(f"  WARN: {w}")
        parquet, _ = write_passes(records, output_dir)
        print(f"  Parsed {len(records)} passes")

        df = pd.read_parquet(parquet)
        df["net_profit"]   = pd.to_numeric(df["net_profit"],   errors="coerce")
        df["drawdown_pct"] = pd.to_numeric(df["drawdown_pct"], errors="coerce")

        low_dd = df[df["drawdown_pct"] < max_dd]
        pool   = low_dd if not low_dd.empty else df
        et_key_cols = [c for c in ["param__take_profit", "param__stop_loss", "param__EMA_Period1",
                                    "param__Bars", "param__take_profit2", "param__stop_loss2",
                                    "param__EMA_Period2", "param__Bars2"] if c in pool.columns]
        top_n = (pool.sort_values("net_profit", ascending=False)
                     .drop_duplicates(subset=et_key_cols)
                     .head(ET_TOP_N))

        print(f"\n── ET validation — top {len(top_n)} passes ($5K, {ET_RISK_PCT}% risk) ──")

        results = []
        for rank, (_, row) in enumerate(top_n.iterrows(), 1):
            p      = _extract_params(row)
            val_id = f"val_et_maxe{max_entries}_{IS_TAG}_a{attempt}_{rank}"
            val_htm = output_dir / f"{val_id}.htm"
            val_htm.unlink(missing_ok=True)

            _run_mt5(val_id, _build_val_ini(p, val_id, max_entries), output_dir)

            m, _ = parse_forward_report(val_htm, val_id)
            np_v = float(m.get("net_profit",    0))
            pf_v = float(m.get("profit_factor", 0))
            dd_v = float(m.get("drawdown_pct",  0))
            tr_v = int(m.get("trades", 0))

            passed = (tr_v <= max_trades and np_v >= ET_MIN_NP and dd_v <= max_dd)
            mark   = "✔" if passed else "✖"
            print(f"  [{rank:2d}] {mark}  EMA={p['EMA_Period1']}/{p['EMA_Period2']}  "
                  f"TP={p['take_profit']}/{p['take_profit2']}  SL={p['stop_loss']}/{p['stop_loss2']}  "
                  f"Bars={p['Bars']}/{p['Bars2']}  NP={np_v:+,.0f}  DD={dd_v:.1f}%  Tr={tr_v}")
            results.append((p, np_v, pf_v, dd_v, tr_v, passed))

        passing = [(p, np_v, pf_v, dd_v, tr_v) for p, np_v, pf_v, dd_v, tr_v, ok in results if ok]
        if passing:
            sel = max(passing, key=lambda x: x[1])
            print(f"\n  ✔ Gate passed on attempt {attempt}")
            best_p, best_np, best_pf, best_dd, best_tr = sel
            break

        all_r = [(p, np_v, pf_v, dd_v, tr_v) for p, np_v, pf_v, dd_v, tr_v, _ in results]
        sel   = max(all_r, key=lambda x: x[1])

        if attempt < MAX_ATTEMPTS:
            print(f"\n  ✖ Gate failed — retrying...")
            xml_path.unlink(missing_ok=True)
        else:
            print(f"\n  ⚠ All {MAX_ATTEMPTS} attempts failed — using best result")
            best_p, best_np, best_pf, best_dd, best_tr = sel

    return best_p, best_np, best_pf, best_dd, best_tr


def main():
    import pandas as pd
    from zgb_opti.xml_parser import parse_optimization_xml, write_passes, parse_forward_report

    print("=" * 65)
    print(f"  DT818_max MaxEntries Study  ({IS_TAG.upper()})")
    print(f"  IS: {IS_START} -> {IS_END}  |  S1=M30  S2=H4")
    print(f"  Testing MaxEntries: {MAX_ENTRIES_LIST}")
    print("=" * 65)

    summary = []
    for n in MAX_ENTRIES_LIST:
        p, np_val, pf_val, dd_val, tr_val = _run_one(
            n, pd, parse_optimization_xml, write_passes, parse_forward_report
        )
        summary.append((n, p, np_val, pf_val, dd_val, tr_val))

    print(f"\n{'='*75}")
    print(f"  SUMMARY  —  MaxEntries Study  ({IS_TAG.upper()})")
    print(f"  {'MaxE':>4}  {'EMA':>7}  {'TP S1/S2':>15}  {'SL S1/S2':>15}  {'NP':>10}  {'PF':>6}  {'DD':>6}  {'Tr':>4}")
    print(f"  {'-'*71}")
    for n, p, np_val, pf_val, dd_val, tr_val in summary:
        ema  = f"{p['EMA_Period1']}/{p['EMA_Period2']}"
        tp   = f"{p['take_profit']}/{p['take_profit2']}"
        sl   = f"{p['stop_loss']}/{p['stop_loss2']}"
        print(f"  {n:>4}  {ema:>7}  {tp:>15}  {sl:>15}  {np_val:>+10,.0f}  {pf_val:>6.3f}  {dd_val:>5.1f}%  {tr_val:>4}")
    print(f"{'='*75}")


if __name__ == "__main__":
    main()
