"""Full 2-phase re-optimization for DT818_EA.

Phase 1: Genetic optimization (1m OHLC, $10M) sweeping TP, SL, Bars, EMA
Phase 2: Every Tick validation ($5K) with best pass from Phase 1
Output : configs/sets/DT818_6_mar21_exp_may16.set (updated)

IS: 2026-01-24 -> 2026-03-21 (8 weeks)
"""
from __future__ import annotations

import io, sys, shutil
from datetime import date
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

MT5_TERMINAL  = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN     = 18912087
MT5_SERVER    = "VantageInternational-Live 3"
OUTPUT_DIR    = Path("output/dt818_full_reopt")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")
DD_THRESHOLD  = 20.0
IS_START      = date(2026, 1, 24)
IS_END        = date(2026, 3, 21)

SET_OUT_4PCT  = Path("configs/sets/DT818_4_mar21_exp_may16.set")
SET_OUT_6PCT  = Path("configs/sets/DT818_6_mar21_exp_may16.set")
SET_OUT_9PCT  = Path("configs/sets/DT818_9_mar21_exp_may16.set")

P1_INPUTS = (
    "_BaseMagic=1000\n"
    "_OrderComment=DT818_A\n"
    "_OrderComment2=DT818_B\n"
    "_CapitalProtectionAmount=0.0\n"
    "_LotMode=1\n"
    "_RiskPct=4.0\n"
    "TierBase=2000\n"
    "LotStep=0.01\n"
    "_Trade1=1\n"
    "_time_frame=30\n"
    "_take_profit=13500||1000||5000||20000||Y\n"
    "_stop_loss=10000||1000||5000||20000||Y\n"
    "_EMA_Period1=5||1||3||10||Y\n"
    "_Bars=4||1||2||8||Y\n"
    "_RiskMode1=0\n"
    "_TSL1=2000\n"
    "_TSLA1=700\n"
    "_BETrigger1=2000\n"
    "_BEBuf1=700\n"
    "_Trade2=1\n"
    "_time_frame2=30\n"
    "_take_profit2=13000||1000||5000||20000||Y\n"
    "_stop_loss2=10000||1000||5000||20000||Y\n"
    "_EMA_Period2=6||1||3||10||Y\n"
    "_Bars2=4||1||2||8||Y\n"
    "_RiskMode2=0\n"
    "_TSL2=2200\n"
    "_TSLA2=1850\n"
    "_BETrigger2=300\n"
    "_BEBuf2=700\n"
)


def _build_ini(tester_inputs, report_id, model, optimization, deposit, criterion=0):
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert=DT818_EA\nSymbol=XAUUSD\nPeriod=M30\nModel={model}\n"
        f"Optimization={optimization}\nOptimizationCriterion={criterion}\n"
        f"Deposit={deposit}\nSpread=45\n"
        f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{tester_inputs}\n"
    )


def _run_mt5(job_id, ini_content, expect_xml=False):
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    ini_path = OUTPUT_DIR / f"{job_id}.ini"
    ini_path.write_text(ini_content, encoding="utf-8")
    print(f"  Launching: {job_id}")
    rc = run_mt5_job(MT5_TERMINAL, str(ini_path))
    print(f"  Exit code: {rc}")

    ext_list = [".xml", ".htm", ".html"] if expect_xml else [".htm", ".html", ".xml"]
    art = find_report_artifact(OUTPUT_DIR, job_id, MT5_TERMINAL)
    if art is None:
        for ext in ext_list:
            c = MT5_DATA_ROOT / f"{job_id}{ext}"
            if c.exists(): art = c; break
    if art is None:
        raise RuntimeError(f"No report found for {job_id}")
    collected = copy_report_artifact(art, OUTPUT_DIR)
    print(f"  Collected: {collected.name}  ({collected.stat().st_size/1e6:.1f}MB)")
    return collected


def _best_pass(df):
    import pandas as pd
    df = df.copy()
    df["net_profit"]   = pd.to_numeric(df["net_profit"],   errors="coerce")
    df["drawdown_pct"] = pd.to_numeric(df["drawdown_pct"], errors="coerce")
    low_dd = df[df["drawdown_pct"] < DD_THRESHOLD]
    pool   = low_dd if not low_dd.empty else df
    return pool.sort_values("net_profit", ascending=False).iloc[0]


def _build_backtest_inputs(best):
    def v(col, default):
        try: return str(int(float(best.get(col, default))))
        except: return str(default)
    def vf(col, default):
        try: return str(float(best.get(col, default)))
        except: return str(default)

    return (
        f"_BaseMagic=1000\n"
        f"_OrderComment=DT818_A\n"
        f"_OrderComment2=DT818_B\n"
        f"_CapitalProtectionAmount=0.0\n"
        f"_LotMode=1\n"
        f"_RiskPct=4.0\n"
        f"TierBase=2000\n"
        f"LotStep=0.01\n"
        f"_Trade1=1\n"
        f"_time_frame=30\n"
        f"_take_profit={v('param__take_profit', 13500)}\n"
        f"_stop_loss={v('param__stop_loss', 10000)}\n"
        f"_EMA_Period1={v('param__EMA_Period1', 5)}\n"
        f"_Bars={v('param__Bars', 4)}\n"
        f"_RiskMode1=0\n"
        f"_TSL1=2000\n"
        f"_TSLA1=700\n"
        f"_BETrigger1=2000\n"
        f"_BEBuf1=700\n"
        f"_Trade2=1\n"
        f"_time_frame2=30\n"
        f"_take_profit2={v('param__take_profit2', 13000)}\n"
        f"_stop_loss2={v('param__stop_loss2', 10000)}\n"
        f"_EMA_Period2={v('param__EMA_Period2', 6)}\n"
        f"_Bars2={v('param__Bars2', 4)}\n"
        f"_RiskMode2=0\n"
        f"_TSL2=2200\n"
        f"_TSLA2=1850\n"
        f"_BETrigger2=300\n"
        f"_BEBuf2=700\n"
    )


def _write_set(inputs_str, risk_pct, output_path):
    lines = []
    for line in inputs_str.strip().splitlines():
        if "=" not in line: continue
        name, _, val = line.partition("=")
        val = val.strip()
        if name.strip() == "_RiskPct":
            val = str(risk_pct)
        lines.append(f"{name.strip()}={val}||{val}||1||{val}||{val}||N")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  Written: {output_path}")


def main():
    import pandas as pd
    from zgb_opti.xml_parser import parse_optimization_xml, write_passes, parse_forward_report

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 65)
    print("  DT818 Full Re-optimization")
    print(f"  IS: {IS_START} -> {IS_END}")
    print("=" * 65)

    # ── Phase 1: Genetic optimization ──────────────────────────────
    print("\n── Phase 1: Genetic optimization (1m OHLC, $10M) ──")
    p1_id   = "p1_dt818_full_reopt"
    xml_path = OUTPUT_DIR / f"{p1_id}.xml"

    if xml_path.exists():
        print(f"  [Cached] {xml_path.name}")
    else:
        ini = _build_ini(P1_INPUTS, p1_id, model=1, optimization=2, deposit=10_000_000)
        _run_mt5(p1_id, ini, expect_xml=True)

    records, warns = parse_optimization_xml(xml_path, p1_id)
    for w in warns: print(f"  WARN: {w}")
    parquet_path, _ = write_passes(records, OUTPUT_DIR)
    print(f"  Parsed {len(records)} passes")

    df   = pd.read_parquet(parquet_path)
    best = _best_pass(df)

    tp   = int(float(best.get("param__take_profit",  13500)))
    sl   = int(float(best.get("param__stop_loss",    10000)))
    tp2  = int(float(best.get("param__take_profit2", 13000)))
    sl2  = int(float(best.get("param__stop_loss2",   10000)))
    ema1 = int(float(best.get("param__EMA_Period1",  5)))
    ema2 = int(float(best.get("param__EMA_Period2",  6)))
    bars = int(float(best.get("param__Bars",          4)))
    bars2= int(float(best.get("param__Bars2",         4)))

    print(f"\n  Best pass:")
    print(f"    TP={tp}  SL={sl}  RR={tp/sl:.2f}  EMA1={ema1}  Bars={bars}")
    print(f"    TP2={tp2}  SL2={sl2}  RR2={tp2/sl2:.2f}  EMA2={ema2}  Bars2={bars2}")
    print(f"    NP={float(best.net_profit):+,.0f}  PF={float(best.profit_factor):.3f}  DD={float(best.drawdown_pct):.1f}%  Trades={int(best.trades)}")

    # ── Phase 2: Every Tick validation ─────────────────────────────
    print("\n── Phase 2: Every Tick validation ($5K, 4% risk) ──")
    bt_inputs = _build_backtest_inputs(best)
    p2_id     = "p2_dt818_full_reopt"
    htm_path  = OUTPUT_DIR / f"{p2_id}.htm"

    if htm_path.exists():
        print(f"  [Cached] {htm_path.name}")
    else:
        ini = _build_ini(bt_inputs, p2_id, model=0, optimization=0, deposit=5_000)
        _run_mt5(p2_id, ini)

    m, _ = parse_forward_report(htm_path, p2_id)
    print(f"\n  Validation (Every Tick, $5K, 4% risk):")
    print(f"    NP={float(m.get('net_profit',0)):+,.0f}  PF={float(m.get('profit_factor',0)):.3f}  DD={float(m.get('drawdown_pct',0)):.1f}%  Trades={int(m.get('trades',0))}")

    # ── Write set files ────────────────────────────────────────────
    print("\n── Writing set files ──")
    _write_set(bt_inputs, 4.0, SET_OUT_4PCT)
    _write_set(bt_inputs, 6.0, SET_OUT_6PCT)
    _write_set(bt_inputs, 9.0, SET_OUT_9PCT)

    print(f"\n{'=' * 65}")
    print(f"  DONE")
    print(f"  Best: TP={tp}/{tp2}  SL={sl}/{sl2}  EMA={ema1}/{ema2}  Bars={bars}/{bars2}")
    print(f"  Validation: NP={float(m.get('net_profit',0)):+,.0f}  PF={float(m.get('profit_factor',0)):.3f}  DD={float(m.get('drawdown_pct',0)):.1f}%")
    print(f"  Next reopt: 2026-05-16")
    print(f"{'=' * 65}")


if __name__ == "__main__":
    main()
