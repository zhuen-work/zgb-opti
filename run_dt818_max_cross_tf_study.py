"""DT818_max cross-timeframe study: find best S1×S2 TF combo for DT818_max.

Tests all 4×4 = 16 combinations of (M15, M30, H1, H4) for _time_frame × _time_frame2.
M5 excluded (failed gate in single-TF study: DD>30%, Trades>170).

For each combo:
  - Genetic sweep (1m OHLC, $10M), chart Period = finer of the two TFs
  - ET-validate top 10 distinct passes ($5K, 4% risk)
  - Best ET NP per combo (gate: trades <= 150, NP >= $10K, DD <= 20%)

Winner = highest ET NP among gate-passing combos.
Writes DT818_4/6/9_<tf1>x<tf2>_mar21_exp_next.set.

Baseline (M30×M30): NP=+$25,039  PF=1.780  DD=15.9%  Trades=105
"""
from __future__ import annotations

import io, sys
from datetime import date
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

# ── Gate / validation constants ───────────────────────────────────────────────
ET_TOP_N      = 10
ET_MIN_NP     = 10_000
ET_MAX_TRADES = 150
DD_THRESHOLD  = 20.0

# ── Configuration ─────────────────────────────────────────────────────────────
MT5_TERMINAL = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN    = 18912087
MT5_SERVER   = "VantageInternational-Live 3"
EA_PATH      = "DT818_max"
SYMBOL       = "XAUUSD"
SPREAD       = 45
DEPOSIT_OPT  = 10_000_000

IS_START = date(2026, 1, 24)
IS_END   = date(2026, 3, 21)
IS_TAG   = "mar21"

OUTPUT_DIR    = Path(f"output/dt818_max_cross_tf_{IS_TAG}")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

# Finer-to-coarser order used to pick chart Period= for cross-TF combos
TF_CANDIDATES = ["M15", "M30", "H1", "H4"]

# ENUM_TIMEFRAMES integer values for _time_frame .set param
TF_ENUM: dict[str, int] = {
    "M15": 15,
    "M30": 30,
    "H1":  16385,
    "H4":  16388,
}

# Chart period to use when S1 and S2 are on different TFs: pick the finer one
def _chart_period(tf1: str, tf2: str) -> str:
    return tf1 if TF_CANDIDATES.index(tf1) <= TF_CANDIDATES.index(tf2) else tf2


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


# ── Set lines ─────────────────────────────────────────────────────────────────

def _joint_set_lines(tf1_enum: int, tf2_enum: int) -> list[str]:
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
        _fix("_time_frame", tf1_enum),
        _sweep("_take_profit",  10000, 500,  2000, 15000),
        _sweep("_stop_loss",    10000, 500,  2000, 15000),
        _sweep("_EMA_Period1",  5,     1,    4,    6),
        _sweep("_Bars",         4,     1,    2,    10),
        _sweep("_RiskMode1",    0,     1,    0,    1),
        _sweep("_TSL1",         500,   100,  200,  2000),
        _sweep("_TSLA1",        200,   50,   100,  1000),
        _sweep("_BETrigger1",   2500,  250,  500,  5000),
        _sweep("_BEBuf1",       450,   50,   100,  1000),
        _sweep("_Trade2",       1,     1,    0,    1),
        _fix("_time_frame2", tf2_enum),
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


# ── INI builders ──────────────────────────────────────────────────────────────

def _build_opti_ini(set_lines, report_id, chart_period):
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod={chart_period}\nModel=1\n"
        "Optimization=2\nOptimizationCriterion=0\n"
        f"Deposit={DEPOSIT_OPT}\nSpread={SPREAD}\n"
        f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{_set_to_ini(set_lines)}\n"
    )

def _build_val_ini(p, val_id, chart_period):
    val_inputs = "\n".join([
        "_BaseMagic=1000", "_OrderComment=DT818_A", "_OrderComment2=DT818_B",
        "_CapitalProtectionAmount=0.0", "_LotMode=1", "_RiskPct=4.0",
        "TierBase=2000", "LotStep=0.01", "_Trade1=1",
        f"_time_frame={p['tf1_enum']}",
        f"_take_profit={p['take_profit']}",   f"_stop_loss={p['stop_loss']}",
        f"_EMA_Period1={p['EMA_Period1']}",    f"_Bars={p['Bars']}",
        "_Trade2=1",
        f"_time_frame2={p['tf2_enum']}",
        f"_take_profit2={p['take_profit2']}", f"_stop_loss2={p['stop_loss2']}",
        f"_EMA_Period2={p['EMA_Period2']}",    f"_Bars2={p['Bars2']}",
    ])
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod={chart_period}\nModel=0\n"
        "Optimization=0\n"
        f"Deposit=5000\nSpread={SPREAD}\n"
        f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={val_id}\n\n[TesterInputs]\n{val_inputs}\n"
    )


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
        _fix("_time_frame", p["tf1_enum"]),
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
        _fix("_time_frame2", p["tf2_enum"]),
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


def _extract_params(row, tf1_enum, tf2_enum):
    def vi(col, default):
        try:    return int(float(row.get(col, default)))
        except: return int(default)
    return dict(
        tf1_enum     = tf1_enum,
        tf2_enum     = tf2_enum,
        take_profit  = vi("param__take_profit",  10000),
        stop_loss    = vi("param__stop_loss",    10000),
        EMA_Period1  = vi("param__EMA_Period1",  5),
        Bars         = vi("param__Bars",         4),
        take_profit2 = vi("param__take_profit2", 9500),
        stop_loss2   = vi("param__stop_loss2",   8500),
        EMA_Period2  = vi("param__EMA_Period2",  5),
        Bars2        = vi("param__Bars2",        4),
    )


# ── Per-combo study ───────────────────────────────────────────────────────────

def _study_combo(tf1, tf2, combo_tag, pd, parse_optimization_xml, write_passes, parse_forward_report):
    """Genetic + ET validation for one (tf1, tf2) combo. Returns (p, np, pf, dd, trades, passed)."""
    chart_period = _chart_period(tf1, tf2)
    job_id       = f"opti_{combo_tag}_{IS_TAG}"
    xml_path     = OUTPUT_DIR / f"{job_id}.xml"

    print(f"\n{'─'*60}")
    print(f"  [{tf1}×{tf2}] Genetic (chart={chart_period}, 1m OHLC, $10M)")
    print(f"{'─'*60}")

    if xml_path.exists():
        print(f"  [Cached] {xml_path.name}")
    else:
        set_lines   = _joint_set_lines(TF_ENUM[tf1], TF_ENUM[tf2])
        ini_content = _build_opti_ini(set_lines, job_id, chart_period)
        _run_mt5(job_id, ini_content)

    records, warns = parse_optimization_xml(xml_path, job_id)
    for w in warns: print(f"  WARN: {w}")
    parquet, _ = write_passes(records, OUTPUT_DIR)
    print(f"  Parsed {len(records)} passes")

    df = pd.read_parquet(parquet)
    df["net_profit"]   = pd.to_numeric(df["net_profit"],   errors="coerce")
    df["drawdown_pct"] = pd.to_numeric(df["drawdown_pct"], errors="coerce")

    low_dd = df[df["drawdown_pct"] < DD_THRESHOLD]
    pool   = low_dd if not low_dd.empty else df
    et_key_cols = [c for c in ["param__take_profit", "param__stop_loss", "param__EMA_Period1",
                                "param__Bars", "param__take_profit2", "param__stop_loss2",
                                "param__EMA_Period2", "param__Bars2"] if c in pool.columns]
    top_n = (pool.sort_values("net_profit", ascending=False)
                 .drop_duplicates(subset=et_key_cols)
                 .head(ET_TOP_N))

    print(f"\n  [{tf1}×{tf2}] ET validation — top {len(top_n)} passes ($5K, 4% risk)")

    results = []
    for rank, (_, row) in enumerate(top_n.iterrows(), 1):
        p       = _extract_params(row, TF_ENUM[tf1], TF_ENUM[tf2])
        val_id  = f"val_et_{combo_tag}_{IS_TAG}_{rank}"
        val_htm = OUTPUT_DIR / f"{val_id}.htm"
        val_htm.unlink(missing_ok=True)

        _run_mt5(val_id, _build_val_ini(p, val_id, chart_period))

        m, warns = parse_forward_report(val_htm, val_id)
        np_v = float(m.get("net_profit",    0))
        pf_v = float(m.get("profit_factor", 0))
        dd_v = float(m.get("drawdown_pct",  0))
        tr_v = int(m.get("trades", 0))

        passed = (tr_v <= ET_MAX_TRADES and np_v >= ET_MIN_NP and dd_v <= DD_THRESHOLD)
        mark   = "✔" if passed else "✖"
        print(f"  [{tf1}×{tf2}][{rank:2d}] {mark}  EMA={p['EMA_Period1']}/{p['EMA_Period2']}  "
              f"TP={p['take_profit']}/{p['take_profit2']}  SL={p['stop_loss']}/{p['stop_loss2']}  "
              f"NP={np_v:+,.0f}  DD={dd_v:.1f}%  Tr={tr_v}")
        results.append((p, np_v, pf_v, dd_v, tr_v, passed))

    passing = [(p, np_v, pf_v, dd_v, tr_v) for p, np_v, pf_v, dd_v, tr_v, ok in results if ok]
    if passing:
        best = max(passing, key=lambda x: x[1])
        did_pass = True
    else:
        best = max([(p, np_v, pf_v, dd_v, tr_v) for p, np_v, pf_v, dd_v, tr_v, _ in results],
                   key=lambda x: x[1])
        did_pass = False

    return best[0], best[1], best[2], best[3], best[4], did_pass


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    import pandas as pd
    from zgb_opti.xml_parser import parse_optimization_xml, write_passes, parse_forward_report

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    combos = [(tf1, tf2) for tf1 in TF_CANDIDATES for tf2 in TF_CANDIDATES]
    combo_tags = {(tf1, tf2): f"{tf1.lower()}x{tf2.lower()}" for tf1, tf2 in combos}

    print("=" * 65)
    print(f"  DT818_max Cross-TF Study ({IS_TAG.upper()})")
    print(f"  IS: {IS_START} -> {IS_END}")
    print(f"  Candidates: {', '.join(TF_CANDIDATES)}  ({len(combos)} combos)")
    print(f"  Baseline (M30×M30): NP=+$25,039  PF=1.780  DD=15.9%  Trades=105")
    print(f"  Gate: Trades <= {ET_MAX_TRADES}, NP >= ${ET_MIN_NP:,}, DD <= {DD_THRESHOLD}%")
    print("=" * 65)

    results: dict[tuple, tuple] = {}

    for tf1, tf2 in combos:
        tag = combo_tags[(tf1, tf2)]
        p, np_v, pf_v, dd_v, tr_v, passed = _study_combo(
            tf1, tf2, tag, pd, parse_optimization_xml, write_passes, parse_forward_report,
        )
        results[(tf1, tf2)] = (p, np_v, pf_v, dd_v, tr_v, passed)

    # ── Comparison table ──────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print(f"  CROSS-TF COMPARISON  (Every Tick, $5K, 4% risk)")
    print(f"  {'S1×S2':<10} {'Gate':<5} {'Net Profit':>14} {'PF':>7} {'DD%':>7} {'Trades':>8}")
    print(f"  {'─'*58}")
    for tf1, tf2 in combos:
        p, np_v, pf_v, dd_v, tr_v, passed = results[(tf1, tf2)]
        mark  = "✔" if passed else "✖"
        label = f"{tf1}×{tf2}"
        print(f"  {label:<10} {mark:<5} {np_v:>+14,.0f} {pf_v:>7.3f} {dd_v:>6.1f}% {tr_v:>8}")

    # ── Pick winner ───────────────────────────────────────────────────
    passing = [(k, v) for k, v in results.items() if v[5]]
    if passing:
        winner_key = max(passing, key=lambda x: x[1][1])[0]
    else:
        print("\n  ⚠ No combo passed the ET gate — picking best NP regardless")
        winner_key = max(results, key=lambda k: results[k][1])

    tf1_w, tf2_w = winner_key
    p, np_v, pf_v, dd_v, tr_v, _ = results[winner_key]

    print(f"\n  -> Winner: {tf1_w}×{tf2_w}  NP={np_v:+,.0f}  PF={pf_v:.3f}  DD={dd_v:.1f}%  Trades={tr_v}")

    # ── Write set files ───────────────────────────────────────────────
    tag = combo_tags[winner_key]
    print(f"\n── Writing set files ({tf1_w}×{tf2_w}) ──")
    _write_set(p, 4.0, Path(f"configs/sets/DT818_4_{tag}_{IS_TAG}_exp_next.set"))
    _write_set(p, 6.0, Path(f"configs/sets/DT818_6_{tag}_{IS_TAG}_exp_next.set"))
    _write_set(p, 9.0, Path(f"configs/sets/DT818_9_{tag}_{IS_TAG}_exp_next.set"))

    print(f"\n{'=' * 65}")
    print(f"  DONE")
    print(f"  Winner combo : {tf1_w}×{tf2_w}  (chart={_chart_period(tf1_w, tf2_w)})")
    print(f"  S1: TP={p['take_profit']}  SL={p['stop_loss']}  EMA={p['EMA_Period1']}  Bars={p['Bars']}")
    print(f"  S2: TP={p['take_profit2']}  SL={p['stop_loss2']}  EMA={p['EMA_Period2']}  Bars2={p['Bars2']}")
    print(f"  Validation (ET $5K): NP={np_v:+,.0f}  PF={pf_v:.3f}  DD={dd_v:.1f}%  Trades={tr_v}")
    print(f"  Baseline (M30×M30):  NP=+$25,039  PF=1.780  DD=15.9%  Trades=105")
    print(f"{'=' * 65}")


if __name__ == "__main__":
    main()
