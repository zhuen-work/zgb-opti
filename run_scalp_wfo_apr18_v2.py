"""Scalper v1 Walk-Forward Optimization

3 rolling windows (4w IS / 2w OOS):
  W1: IS 2026-02-14 -> 2026-03-14  OOS 2026-03-14 -> 2026-03-28
  W2: IS 2026-02-28 -> 2026-03-28  OOS 2026-03-28 -> 2026-04-11
  W3: IS 2026-03-14 -> 2026-04-11  OOS 2026-04-11 -> 2026-04-21

Criterion: Recovery Factor (NP/DD). Per window: top-20 by RF.
Find params appearing in top-20 of 2+ windows = robust.
Validate robust set on all 3 OOS windows.
"""
from __future__ import annotations

import io, sys, re
from datetime import date
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)

MT5_TERMINAL  = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN     = 18912087
MT5_SERVER    = "VantageInternational-Live 3"
EA_PATH       = "Scalper_v1"
SYMBOL        = "XAUUSD"
PERIOD        = "M5"
SPREAD        = 45
DEPOSIT       = 10_000
RISK_PCT      = 2.5
TARGET_PCT    = 5.0
LOSS_PCT      = 10.0

WINDOWS = [
    ("w1", date(2026,2,7),  date(2026,3,7),  date(2026,3,7),  date(2026,3,21)),
    ("w2", date(2026,2,21), date(2026,3,21), date(2026,3,21), date(2026,4,4)),
    ("w3", date(2026,3,7),  date(2026,4,4),  date(2026,4,4),  date(2026,4,18)),
]

OUTPUT_DIR    = Path("output/scalp_wfo_apr18_v2")
SET_OUT       = Path("configs/sets/scalp_v1_wfo_apr18_v2.set")
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

def _build_opti_ini(set_lines, report_id, from_dt, to_dt):
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod={PERIOD}\nModel=1\n"
        "Optimization=1\nOptimizationCriterion=4\n"  # criterion=4 = Recovery Factor
        f"Deposit={DEPOSIT}\nSpread={SPREAD}\n"
        f"FromDate={from_dt.strftime('%Y.%m.%d')}\nToDate={to_dt.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{_set_to_ini(set_lines)}\n"
    )

def _build_et_ini(set_lines, report_id, from_dt, to_dt):
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod={PERIOD}\nModel=0\n"
        "Optimization=0\n"
        f"Deposit={DEPOSIT}\nSpread={SPREAD}\n"
        f"FromDate={from_dt.strftime('%Y.%m.%d')}\nToDate={to_dt.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{_set_to_ini(set_lines)}\n"
    )


def _base_params(donch, tp, sl, htp, maxpos, penbars):
    """Build param set for scalper with given optimizable values."""
    return [
        _fix("_Magic", 2000),
        _fix("_RiskPct", RISK_PCT),
        _fix("_MaxPositions", maxpos),
        _fix("_EntryTF", 5),
        _fix("_DonchianBars", donch),
        _fix("_TakeProfit", tp),
        _fix("_StopLoss", sl),
        _fix("_HalfTP_Ratio", htp),
        _fix("_PendingExpireBars", penbars),
        _fix("_DailyTargetPct", TARGET_PCT),
        _fix("_DailyLossPct", LOSS_PCT),
        _fix("_TradeStartHour", 14),
        _fix("_TradeEndHour", 22),
        "_BlockFriPM=true",
        "_OrderCommentTag=Scalp",
    ]


def _opt_params_sweep():
    """Build param set with sweep ranges."""
    return [
        _fix("_Magic", 2000),
        _fix("_RiskPct", RISK_PCT),
        _sweep("_MaxPositions", 2, 1, 1, 3),         # 3 values: 1,2,3
        _fix("_EntryTF", 5),
        _sweep("_DonchianBars", 20, 5, 15, 25),       # 3 values: 15,20,25
        _sweep("_TakeProfit",   150, 50, 100, 200),   # 3 values
        _sweep("_StopLoss",     75, 25, 50, 75),      # 2 values: 50,75
        _sweep("_HalfTP_Ratio", 0.0, 0.3, 0.0, 0.3),  # 2 values: 0.0,0.3
        _sweep("_PendingExpireBars", 1, 1, 1, 3),     # 3 values: 1,2,3
        _fix("_DailyTargetPct", TARGET_PCT),
        _fix("_DailyLossPct", LOSS_PCT),
        _fix("_TradeStartHour", 14),
        _fix("_TradeEndHour", 22),
        "_BlockFriPM=true",
        "_OrderCommentTag=Scalp",
    ]


def _run_mt5(ini_content, job_id):
    from zgb_opti.launcher import run_mt5_job
    from zgb_opti.collector import find_report_artifact, copy_report_artifact
    ini_path = OUTPUT_DIR / f"{job_id}.ini"
    ini_path.write_text(ini_content, encoding="utf-8")
    rc = run_mt5_job(MT5_TERMINAL, str(ini_path))
    print(f"  [{job_id}] exit={rc}")
    art = find_report_artifact(OUTPUT_DIR, job_id, MT5_TERMINAL)
    if art is None:
        for ext in (".xml", ".htm", ".html"):
            c = MT5_DATA_ROOT / f"{job_id}{ext}"
            if c.exists(): art = c; break
    if art is None:
        raise RuntimeError(f"No report found for {job_id}")
    return copy_report_artifact(art, OUTPUT_DIR)


def run_window_optimization(win_name, is_start, is_end):
    """Run IS optimization, return top 20 by Recovery Factor."""
    from zgb_opti.xml_parser import parse_optimization_xml, write_passes
    import pandas as pd

    job_id = f"scalp_wfo_{win_name}_is"
    xml_cached = OUTPUT_DIR / f"{job_id}.xml"
    if xml_cached.exists():
        print(f"  [Cached] {xml_cached.name}")
        art = xml_cached
    else:
        ini = _build_opti_ini(_opt_params_sweep(), job_id, is_start, is_end)
        print(f"\n  Window {win_name} IS: {is_start} -> {is_end}")
        art = _run_mt5(ini, job_id)

    records, _ = parse_optimization_xml(art, job_id)
    parquet, _ = write_passes(records, OUTPUT_DIR)
    df = pd.read_parquet(parquet)
    df["net_profit"]   = pd.to_numeric(df["net_profit"], errors="coerce")
    df["drawdown_pct"] = pd.to_numeric(df["drawdown_pct"], errors="coerce")
    df["profit_factor"]= pd.to_numeric(df["profit_factor"], errors="coerce")
    df["trades"]       = pd.to_numeric(df["trades"], errors="coerce")

    # Recovery factor = NP / DD%
    df["rf"] = df.apply(lambda r: r["net_profit"] / max(r["drawdown_pct"], 0.1), axis=1)

    # Only consider profitable passes with some trades
    df = df[(df["net_profit"] > 0) & (df["trades"] >= 10)]
    if df.empty:
        print(f"  Window {win_name}: no profitable passes!")
        return []

    # Identify param columns
    donch_c  = [c for c in df.columns if "DonchianBars" in c][0]
    tp_c     = [c for c in df.columns if "TakeProfit" in c][0]
    sl_c     = [c for c in df.columns if "StopLoss" in c][0]
    htp_c    = [c for c in df.columns if "HalfTP_Ratio" in c][0]
    maxpos_c = [c for c in df.columns if "MaxPositions" in c][0]
    penbars_c = [c for c in df.columns if "PendingExpireBars" in c][0]

    for c in [donch_c, tp_c, sl_c, htp_c, maxpos_c, penbars_c]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    top20 = df.sort_values("rf", ascending=False).head(20)
    print(f"  Window {win_name}: top 5 by RF:")
    print(f"    {'NP':>10}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  {'RF':>6}  Donch  TP  SL  HTP  MxP  PEB")
    for _, r in top20.head(5).iterrows():
        print(f"    {r['net_profit']:>+10,.0f}  {r['profit_factor']:>6.2f}  {r['drawdown_pct']:>5.1f}%  "
              f"{int(r['trades']):>4}  {r['rf']:>6.0f}  {int(r[donch_c]):>5}  {int(r[tp_c]):>3}  "
              f"{int(r[sl_c]):>3}  {r[htp_c]:>4.1f}  {int(r[maxpos_c]):>3}  {int(r[penbars_c]):>3}")

    results = []
    for _, r in top20.iterrows():
        results.append({
            "donch": int(r[donch_c]),
            "tp": int(r[tp_c]),
            "sl": int(r[sl_c]),
            "htp": round(float(r[htp_c]), 2),
            "maxpos": int(r[maxpos_c]),
            "penbars": int(r[penbars_c]),
            "rf": float(r["rf"]),
            "np": float(r["net_profit"]),
        })
    return results


def run_et(params, oos_start, oos_end, job_id):
    """Run ET on OOS with given params. Return (NP, PF, DD, Tr)."""
    from zgb_opti.xml_parser import parse_forward_report
    htm = OUTPUT_DIR / f"{job_id}.htm"
    if not htm.exists():
        set_lines = _base_params(params["donch"], params["tp"], params["sl"],
                                  params["htp"], params["maxpos"], params["penbars"])
        ini = _build_et_ini(set_lines, job_id, oos_start, oos_end)
        _run_mt5(ini, job_id)

    if not htm.exists():
        return None

    rpt, _ = parse_forward_report(htm, job_id)
    def _get(key, d=0):
        try: return float(rpt.get(key, d))
        except: return d

    return {
        "np": _get("net_profit"),
        "pf": _get("profit_factor"),
        "dd": _get("drawdown_pct"),
        "tr": int(_get("trades")),
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("  Scalper v1 Walk-Forward Optimization")
    print("  Criterion: Recovery Factor (NP/DD%)")
    print(f"  Windows: {len(WINDOWS)} × 4w IS / 2w OOS")
    print("=" * 70)

    # Phase A: Optimize each window's IS
    all_top = {}
    for win_name, is_s, is_e, _, _ in WINDOWS:
        all_top[win_name] = run_window_optimization(win_name, is_s, is_e)

    # Phase B: Find robust params (appear in 2+ windows' top-20)
    def param_key(p):
        return (p["donch"], p["tp"], p["sl"], p["htp"], p["maxpos"], p["penbars"])

    counts = {}
    for win_name, results in all_top.items():
        for p in results:
            k = param_key(p)
            counts.setdefault(k, {"count": 0, "windows": [], "total_rf": 0, "total_np": 0})
            counts[k]["count"] += 1
            counts[k]["windows"].append(win_name)
            counts[k]["total_rf"] += p["rf"]
            counts[k]["total_np"] += p["np"]

    robust = {k: v for k, v in counts.items() if v["count"] >= 2}
    print(f"\n  Robust params (in top-20 of 2+ windows): {len(robust)}")

    if not robust:
        print("  No robust params found! Using top IS RF as fallback.")
        # Fallback: take top from all windows combined
        all_combined = []
        for results in all_top.values():
            all_combined.extend(results)
        all_combined.sort(key=lambda p: p["rf"], reverse=True)
        robust = {param_key(p): {"count": 1, "windows": ["single"], "total_rf": p["rf"], "total_np": p["np"]}
                  for p in all_combined[:5]}

    # Phase C: Test robust params on all OOS windows
    print("\n" + "=" * 70)
    print("  PHASE C: OOS Validation")
    print("=" * 70)

    robust_scores = []
    for i, (k, info) in enumerate(sorted(robust.items(), key=lambda x: -x[1]["total_rf"])[:10], 1):
        donch, tp, sl, htp, maxpos, penbars = k
        params = {"donch": donch, "tp": tp, "sl": sl, "htp": htp, "maxpos": maxpos, "penbars": penbars}
        print(f"\n  #{i} Donch={donch} TP={tp} SL={sl} HTP={htp} MxP={maxpos} PEB={penbars}  (in {info['count']} windows: {info['windows']})")

        oos_total = {"np": 0, "tr": 0}
        oos_results = []
        all_profitable = True
        for win_name, _, _, oos_s, oos_e in WINDOWS:
            job_id = f"scalp_wfo_oos_{win_name}_{i}"
            r = run_et(params, oos_s, oos_e, job_id)
            if r is None:
                all_profitable = False
                print(f"    {win_name} OOS: MISSING REPORT")
                continue
            oos_results.append((win_name, r))
            oos_total["np"] += r["np"]
            oos_total["tr"] += r["tr"]
            if r["np"] <= 0:
                all_profitable = False
            print(f"    {win_name} OOS: NP={r['np']:+,.0f}  PF={r['pf']:.2f}  DD={r['dd']:.1f}%  Tr={r['tr']}")

        print(f"    Total OOS NP: {oos_total['np']:+,.0f}  All profitable: {all_profitable}")
        robust_scores.append({
            "params": params,
            "is_windows": info["windows"],
            "total_rf": info["total_rf"],
            "oos_total_np": oos_total["np"],
            "oos_all_profit": all_profitable,
            "oos_results": oos_results,
        })

    # Phase D: Select best (prefer all-profitable OOS, highest total OOS NP)
    print("\n" + "=" * 70)
    print("  FINAL RANKING")
    print("=" * 70)
    robust_scores.sort(key=lambda s: (s["oos_all_profit"], s["oos_total_np"]), reverse=True)

    print(f"\n  {'Rank':<4}  {'OOS Total NP':>12}  {'All Prof':>8}  Params")
    for rank, s in enumerate(robust_scores[:5], 1):
        p = s["params"]
        flag = "✓" if s["oos_all_profit"] else "✗"
        print(f"  {rank:<4}  {s['oos_total_np']:>+12,.0f}  {flag:>8}  Donch={p['donch']} TP={p['tp']} SL={p['sl']} HTP={p['htp']} MxP={p['maxpos']} PEB={p['penbars']}")

    if robust_scores:
        best = robust_scores[0]
        p = best["params"]
        set_content = "\n".join([
            f"_Magic=2000||2000||1||2000||2000||N",
            f"_RiskPct={RISK_PCT}||{RISK_PCT}||1||{RISK_PCT}||{RISK_PCT}||N",
            f"_MaxPositions={p['maxpos']}||{p['maxpos']}||1||{p['maxpos']}||{p['maxpos']}||N",
            f"_EntryTF=5||5||1||5||5||N",
            f"_DonchianBars={p['donch']}||{p['donch']}||1||{p['donch']}||{p['donch']}||N",
            f"_TakeProfit={p['tp']}||{p['tp']}||1||{p['tp']}||{p['tp']}||N",
            f"_StopLoss={p['sl']}||{p['sl']}||1||{p['sl']}||{p['sl']}||N",
            f"_HalfTP_Ratio={p['htp']}||{p['htp']}||1||{p['htp']}||{p['htp']}||N",
            f"_PendingExpireBars={p['penbars']}||{p['penbars']}||1||{p['penbars']}||{p['penbars']}||N",
            f"_DailyTargetPct={TARGET_PCT}||{TARGET_PCT}||1||{TARGET_PCT}||{TARGET_PCT}||N",
            f"_DailyLossPct={LOSS_PCT}||{LOSS_PCT}||1||{LOSS_PCT}||{LOSS_PCT}||N",
            f"_TradeStartHour=14||14||1||14||14||N",
            f"_TradeEndHour=22||22||1||22||22||N",
            f"_BlockFriPM=true",
            f"_OrderCommentTag=Scalp",
        ]) + "\n"
        SET_OUT.parent.mkdir(parents=True, exist_ok=True)
        SET_OUT.write_text(set_content, encoding="utf-8")
        print(f"\n  Written: {SET_OUT}")

    print("=" * 70)
    print("  Done.")


if __name__ == "__main__":
    main()
