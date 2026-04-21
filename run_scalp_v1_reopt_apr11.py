"""Scalper v1 reopt — 2-phase: entry params, then time window.

Fixed: Risk=2.5%, Target=5%, Loss=10% (5x scale), MaxPositions=2
Phase 1: sweep DonchianBars/TakeProfit/StopLoss (96 passes)
Phase 2: sweep TradeStartHour/TradeEndHour with best entry fixed (20 passes)
ET validation on top-5 combos.
"""
from __future__ import annotations

import io, sys, re
from datetime import date
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)

DD_THRESHOLD  = 30.0
TOP_N         = 5

MT5_TERMINAL  = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN     = 18912087
MT5_SERVER    = "VantageInternational-Live 3"
EA_PATH       = "Scalper_v1"
SYMBOL        = "XAUUSD"
PERIOD        = "M5"
SPREAD        = 45
DEPOSIT_OPT   = 10_000
DEPOSIT_VAL   = 10_000

# Fixed params (5x scale)
RISK_PCT      = 2.5
TARGET_PCT    = 5.0
LOSS_PCT      = 10.0
MAX_POS       = 2

IS_START = date(2026, 2, 14)
IS_END   = date(2026, 4, 11)
IS_TAG   = "apr11"

OUTPUT_DIR    = Path("output/scalp_v1_reopt_apr11")
SET_OUT       = Path("configs/sets/scalp_v1_5x_apr11_reopt.set")
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

def _build_et_ini(set_lines, report_id):
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod={PERIOD}\nModel=0\n"
        "Optimization=0\n"
        f"Deposit={DEPOSIT_VAL}\nSpread={SPREAD}\n"
        f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{_set_to_ini(set_lines)}\n"
    )


def _common():
    return [
        _fix("_Magic", 2000),
        _fix("_RiskPct", RISK_PCT),
        _fix("_MaxPositions", MAX_POS),
        _fix("_EntryTF", 5),
        _fix("_PendingExpireBars", 1),
        _fix("_DailyTargetPct", TARGET_PCT),
        _fix("_DailyLossPct", LOSS_PCT),
        _fix("_BlockFriPM", "true"),
        "_OrderCommentTag=Scalp",
    ]


def main():
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    from zgb_opti.xml_parser import parse_optimization_xml, parse_forward_report, write_passes
    import pandas as pd

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 68)
    print(f"  Scalper v1 reopt  ({IS_TAG})")
    print(f"  IS: {IS_START} -> {IS_END}  EA={EA_PATH}")
    print(f"  Fixed: Risk={RISK_PCT}%  Target={TARGET_PCT}%  Loss={LOSS_PCT}%  MaxPos={MAX_POS}")
    print("=" * 68)

    # =====================================================================
    # PHASE 1: Entry params (DonchianBars × TakeProfit × StopLoss)
    # =====================================================================
    print("\n  PHASE 1: Entry params sweep (96 passes)")

    p1_set = _common() + [
        _sweep("_DonchianBars", 10, 5, 5, 30),       # 6 values: 5,10,15,20,25,30
        _sweep("_TakeProfit",   100, 50, 50, 200),   # 4 values: 50,100,150,200
        _sweep("_StopLoss",     100, 50, 50, 200),   # 4 values
        _fix("_TradeStartHour", 13), _fix("_TradeEndHour", 17),
    ]

    job_id = f"scalp_p1_{IS_TAG}"
    xml_cached = OUTPUT_DIR / f"{job_id}.xml"
    if xml_cached.exists():
        print(f"  [Cached] {xml_cached.name}")
        collected = xml_cached
    else:
        ini_path = OUTPUT_DIR / f"{job_id}.ini"
        ini_path.write_text(_build_opti_ini(p1_set, job_id), encoding="utf-8")
        print(f"  Launching: {job_id}")
        rc = run_mt5_job(MT5_TERMINAL, str(ini_path))
        print(f"  Exit code: {rc}")
        art = find_report_artifact(OUTPUT_DIR, job_id, MT5_TERMINAL)
        if art is None:
            c = MT5_DATA_ROOT / f"{job_id}.xml"
            if c.exists(): art = c
        collected = copy_report_artifact(art, OUTPUT_DIR)
        print(f"  Collected: {collected.name}")

    records, warns = parse_optimization_xml(collected, job_id)
    for w in warns: print(f"  WARN: {w}")
    parquet, _ = write_passes(records, OUTPUT_DIR)
    df = pd.read_parquet(parquet)
    df["net_profit"]   = pd.to_numeric(df["net_profit"],   errors="coerce")
    df["drawdown_pct"] = pd.to_numeric(df["drawdown_pct"], errors="coerce")
    df["profit_factor"]= pd.to_numeric(df["profit_factor"], errors="coerce")
    df["trades"]       = pd.to_numeric(df["trades"],       errors="coerce")

    low_dd = df[df["drawdown_pct"] < DD_THRESHOLD]
    pool = low_dd if not low_dd.empty else df

    donch_c = [c for c in df.columns if "DonchianBars" in c][0]
    tp_c    = [c for c in df.columns if "TakeProfit" in c][0]
    sl_c    = [c for c in df.columns if "StopLoss" in c][0]
    dedup_cols = [donch_c, tp_c, sl_c]

    best_p1 = (pool.sort_values("net_profit", ascending=False)
                   .drop_duplicates(subset=dedup_cols)
                   .iloc[0])
    p1_donch = int(float(best_p1[donch_c]))
    p1_tp    = int(float(best_p1[tp_c]))
    p1_sl    = int(float(best_p1[sl_c]))
    print(f"\n  Best P1: Donch={p1_donch}  TP={p1_tp}  SL={p1_sl}")
    print(f"    NP={best_p1['net_profit']:+,.0f}  PF={best_p1['profit_factor']:.3f}  DD={best_p1['drawdown_pct']:.1f}%  Tr={int(best_p1['trades'])}")

    # =====================================================================
    # PHASE 2: Time window (StartHour × EndHour)
    # =====================================================================
    print("\n  PHASE 2: Time window sweep")

    p2_set = _common() + [
        _fix("_DonchianBars", p1_donch),
        _fix("_TakeProfit",   p1_tp),
        _fix("_StopLoss",     p1_sl),
        _sweep("_TradeStartHour", 13, 1, 8, 16),     # 9 values
        _sweep("_TradeEndHour",   17, 1, 16, 22),    # 7 values → skipping invalid (start>=end) = ~45 valid
    ]

    job_id2 = f"scalp_p2_{IS_TAG}"
    xml_cached2 = OUTPUT_DIR / f"{job_id2}.xml"
    if xml_cached2.exists():
        print(f"  [Cached] {xml_cached2.name}")
        collected2 = xml_cached2
    else:
        ini_path2 = OUTPUT_DIR / f"{job_id2}.ini"
        ini_path2.write_text(_build_opti_ini(p2_set, job_id2), encoding="utf-8")
        print(f"  Launching: {job_id2}")
        rc = run_mt5_job(MT5_TERMINAL, str(ini_path2))
        print(f"  Exit code: {rc}")
        art = find_report_artifact(OUTPUT_DIR, job_id2, MT5_TERMINAL)
        if art is None:
            c = MT5_DATA_ROOT / f"{job_id2}.xml"
            if c.exists(): art = c
        collected2 = copy_report_artifact(art, OUTPUT_DIR)
        print(f"  Collected: {collected2.name}")

    records2, _ = parse_optimization_xml(collected2, job_id2)
    parquet2, _ = write_passes(records2, OUTPUT_DIR)
    df2 = pd.read_parquet(parquet2)
    df2["net_profit"]   = pd.to_numeric(df2["net_profit"],   errors="coerce")
    df2["drawdown_pct"] = pd.to_numeric(df2["drawdown_pct"], errors="coerce")
    df2["profit_factor"]= pd.to_numeric(df2["profit_factor"], errors="coerce")
    df2["trades"]       = pd.to_numeric(df2["trades"],       errors="coerce")

    sh_c = [c for c in df2.columns if "TradeStartHour" in c][0]
    eh_c = [c for c in df2.columns if "TradeEndHour" in c][0]
    df2[sh_c] = pd.to_numeric(df2[sh_c], errors="coerce")
    df2[eh_c] = pd.to_numeric(df2[eh_c], errors="coerce")

    # Filter: end > start
    df2 = df2[df2[eh_c] > df2[sh_c]]

    low_dd2 = df2[df2["drawdown_pct"] < DD_THRESHOLD]
    pool2 = low_dd2 if not low_dd2.empty else df2

    top5 = (pool2.sort_values("net_profit", ascending=False)
                 .drop_duplicates(subset=[sh_c, eh_c])
                 .drop_duplicates(subset=["net_profit"])
                 .head(TOP_N))

    print(f"\n  TOP {TOP_N} time windows:")
    print(f"  {'#':<3}  {'NP':>10}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  {'Start':>5}  {'End':>4}")
    print("  " + "-" * 50)
    for rank, (_, r) in enumerate(top5.iterrows(), 1):
        print(f"  {rank:<3}  {r['net_profit']:>+10,.0f}  {r['profit_factor']:>6.3f}  "
              f"{r['drawdown_pct']:>5.1f}%  {int(r['trades']):>4}  {int(r[sh_c]):>5}  {int(r[eh_c]):>4}")

    # =====================================================================
    # ET VALIDATION
    # =====================================================================
    print(f"\n{'=' * 68}")
    print(f"  ET VALIDATION  ${DEPOSIT_VAL:,}")
    print(f"{'=' * 68}")

    et_results = []
    for rank, (_, r) in enumerate(top5.iterrows(), 1):
        sh = int(r[sh_c]); eh = int(r[eh_c])
        et_set = _common() + [
            _fix("_DonchianBars", p1_donch),
            _fix("_TakeProfit",   p1_tp),
            _fix("_StopLoss",     p1_sl),
            _fix("_TradeStartHour", sh),
            _fix("_TradeEndHour",   eh),
        ]
        et_id = f"scalp_et_{rank}_{IS_TAG}"
        et_htm = OUTPUT_DIR / f"{et_id}.htm"
        if not et_htm.exists():
            ini = _build_et_ini(et_set, et_id)
            ini_path = OUTPUT_DIR / f"{et_id}.ini"
            ini_path.write_text(ini, encoding="utf-8")
            print(f"\n-- ET #{rank}  Start={sh}  End={eh} --")
            print(f"  Launching: {et_id}")
            rc = run_mt5_job(MT5_TERMINAL, str(ini_path))
            art = find_report_artifact(OUTPUT_DIR, et_id, MT5_TERMINAL)
            if art is None:
                for ext in (".htm", ".html"):
                    c = MT5_DATA_ROOT / f"{et_id}{ext}"
                    if c.exists(): art = c; break
            if art:
                import shutil
                shutil.copy2(art, et_htm)

        if et_htm.exists():
            rpt, _ = parse_forward_report(et_htm, et_id)
            et_results.append((rank, rpt, sh, eh))

    def _get(rpt, key, default=0):
        try: return float(rpt.get(key, default))
        except: return float(default)

    sorted_et = sorted(et_results,
                       key=lambda x: _get(x[1],"net_profit",0) / max(_get(x[1],"drawdown_pct",100),0.1),
                       reverse=True)

    print(f"\n  {'ET#':<5}  {'NP':>10}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  {'NP/DD':>6}  {'Start':>5}  {'End':>4}")
    print("  " + "-" * 66)
    for rank, rpt, sh, eh in sorted_et:
        np_val = _get(rpt, "net_profit")
        pf_val = _get(rpt, "profit_factor")
        dd_val = _get(rpt, "drawdown_pct")
        tr_val = int(_get(rpt, "trades"))
        npdd   = int(np_val / dd_val) if dd_val > 0 else 0
        print(f"  {rank:<5}  {np_val:>+10,.0f}  {pf_val:>6.3f}  {dd_val:>5.1f}%  {tr_val:>4}  {npdd:>6}  {sh:>5}  {eh:>4}")

    # Write best set file
    if sorted_et:
        best_rank, best_rpt, best_sh, best_eh = sorted_et[0]
        set_content = "\n".join([
            f"_Magic=2000||2000||1||2000||2000||N",
            f"_RiskPct={RISK_PCT}||{RISK_PCT}||1||{RISK_PCT}||{RISK_PCT}||N",
            f"_MaxPositions={MAX_POS}||{MAX_POS}||1||{MAX_POS}||{MAX_POS}||N",
            f"_EntryTF=5||5||1||5||5||N",
            f"_DonchianBars={p1_donch}||{p1_donch}||1||{p1_donch}||{p1_donch}||N",
            f"_TakeProfit={p1_tp}||{p1_tp}||1||{p1_tp}||{p1_tp}||N",
            f"_StopLoss={p1_sl}||{p1_sl}||1||{p1_sl}||{p1_sl}||N",
            f"_PendingExpireBars=1||1||1||1||1||N",
            f"_DailyTargetPct={TARGET_PCT}||{TARGET_PCT}||1||{TARGET_PCT}||{TARGET_PCT}||N",
            f"_DailyLossPct={LOSS_PCT}||{LOSS_PCT}||1||{LOSS_PCT}||{LOSS_PCT}||N",
            f"_TradeStartHour={best_sh}||{best_sh}||1||{best_sh}||{best_sh}||N",
            f"_TradeEndHour={best_eh}||{best_eh}||1||{best_eh}||{best_eh}||N",
            f"_BlockFriPM=true",
            f"_OrderCommentTag=Scalp",
        ]) + "\n"
        SET_OUT.parent.mkdir(parents=True, exist_ok=True)
        SET_OUT.write_text(set_content, encoding="utf-8")
        print(f"\n  Written: {SET_OUT}")

    print("=" * 68)
    print("  Done.")


if __name__ == "__main__":
    main()
