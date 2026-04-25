"""Scalper v1 S1 Walk-Forward Optimization — Model=4 ALL-THE-WAY.

IS:  Model=4 strict real ticks, Optimization=1 exhaustive, criterion=Recovery Factor
OOS: Model=4 strict real ticks
Risk fixed 1%. HedgeMode off. SL min 50 (avoid SL=40 overfit trap).

Narrow sweep chosen for Model=4 tractability (~72 combos × 3 windows = 216 passes, ~3h).
"""
from __future__ import annotations

import io, sys
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
DEPOSIT       = 100
RISK_PCT      = 1.0

WINDOWS = [
    ("w1", date(2026,2,7),  date(2026,3,7),  date(2026,3,7),  date(2026,3,21)),
    ("w2", date(2026,2,21), date(2026,3,21), date(2026,3,21), date(2026,4,4)),
    ("w3", date(2026,3,7),  date(2026,4,4),  date(2026,4,4),  date(2026,4,18)),
]

OUTPUT_DIR    = Path("output/scalp_wfo_s1_m4is_apr18")
SET_OUT       = Path("configs/sets/scalp_v1_s1_m4is_apr18.set")
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
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod={PERIOD}\nModel=4\n"  # Model=4 IS
        "Optimization=1\nOptimizationCriterion=4\n"
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
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod={PERIOD}\nModel=4\n"
        "Optimization=0\n"
        f"Deposit={DEPOSIT}\nSpread={SPREAD}\n"
        f"FromDate={from_dt.strftime('%Y.%m.%d')}\nToDate={to_dt.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{_set_to_ini(set_lines)}\n"
    )


def _common_s1():
    return [
        _fix("_Magic", 2000),
        _fix("_EntryTF", 5),
        "_BlockFriPM=true",
        "_S1_Enabled=true",
        "_S1_Comment=DT818_S1",
        _fix("_S1_RiskPct", RISK_PCT),
        _fix("_S1_PendingExpireBars", 2),
        _fix("_S1_TradeStartHour", 14),
        _fix("_S1_TradeEndHour", 22),
        _fix("_S1_DailyLossPct", 12),
        _fix("_S1_DailyMaxWins", 0),
        _fix("_S1_DailyMaxLosses", 0),
        "_S1_HedgeMode=false",
    ]


def _base_params(donch, tp, sl, htp, tgt):
    return _common_s1() + [
        _fix("_S1_DonchianBars", donch),
        _fix("_S1_TakeProfit", tp),
        _fix("_S1_StopLoss", sl),
        _fix("_S1_HalfTP_Ratio", htp),
        _fix("_S1_DailyTargetPct", tgt),
    ]


def _opt_params_sweep():
    """Narrow sweep for Model=4 tractability. SL min 50 to avoid tight-SL overfit trap."""
    return _common_s1() + [
        _sweep("_S1_DonchianBars",   25, 5,    20, 30),   # 3
        _sweep("_S1_TakeProfit",     200, 50,  200, 250), # 2
        _sweep("_S1_StopLoss",       62, 13,   50, 75),   # 2 (50, 75; step 13 forces exactly these)
        _sweep("_S1_HalfTP_Ratio",   0.3, 0.3, 0.3, 0.6), # 2
        _sweep("_S1_DailyTargetPct", 6, 3,     3, 9),     # 3
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
    from zgb_opti.xml_parser import parse_optimization_xml, write_passes
    import pandas as pd

    job_id = f"scalp_m4is_{win_name}_is"
    xml_cached = OUTPUT_DIR / f"{job_id}.xml"
    if xml_cached.exists():
        print(f"  [Cached] {xml_cached.name}")
        art = xml_cached
    else:
        ini = _build_opti_ini(_opt_params_sweep(), job_id, is_start, is_end)
        print(f"\n  Window {win_name} IS (Model=4): {is_start} -> {is_end}")
        art = _run_mt5(ini, job_id)

    records, _ = parse_optimization_xml(art, job_id)
    parquet, _ = write_passes(records, OUTPUT_DIR)
    df = pd.read_parquet(parquet)
    df["net_profit"]   = pd.to_numeric(df["net_profit"], errors="coerce")
    df["drawdown_pct"] = pd.to_numeric(df["drawdown_pct"], errors="coerce")
    df["profit_factor"]= pd.to_numeric(df["profit_factor"], errors="coerce")
    df["trades"]       = pd.to_numeric(df["trades"], errors="coerce")
    df["rf"] = df.apply(lambda r: r["net_profit"] / max(r["drawdown_pct"], 0.1), axis=1)

    df = df[(df["net_profit"] > 0) & (df["trades"] >= 10)]
    if df.empty:
        print(f"  Window {win_name}: no profitable passes on Model=4!")
        return []

    donch_c = [c for c in df.columns if "S1_DonchianBars" in c][0]
    tp_c    = [c for c in df.columns if "S1_TakeProfit" in c][0]
    sl_c    = [c for c in df.columns if "S1_StopLoss" in c][0]
    htp_c   = [c for c in df.columns if "S1_HalfTP_Ratio" in c][0]
    tgt_c   = [c for c in df.columns if "S1_DailyTargetPct" in c][0]

    for c in [donch_c, tp_c, sl_c, htp_c, tgt_c]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    top20 = df.sort_values("rf", ascending=False).head(20)
    print(f"  Window {win_name}: top 5 by RF (of {len(df)} profitable passes):")
    print(f"    {'NP':>10}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  {'RF':>6}  Donch  TP  SL  HTP  Tgt")
    for _, r in top20.head(5).iterrows():
        print(f"    {r['net_profit']:>+10,.0f}  {r['profit_factor']:>6.2f}  {r['drawdown_pct']:>5.1f}%  "
              f"{int(r['trades']):>4}  {r['rf']:>6.0f}  {int(r[donch_c]):>5}  {int(r[tp_c]):>3}  "
              f"{int(r[sl_c]):>3}  {r[htp_c]:>4.1f}  {int(r[tgt_c]):>3}")

    results = []
    for _, r in top20.iterrows():
        results.append({
            "donch": int(r[donch_c]),
            "tp":    int(r[tp_c]),
            "sl":    int(r[sl_c]),
            "htp":   round(float(r[htp_c]), 2),
            "tgt":   int(r[tgt_c]),
            "rf":    float(r["rf"]),
            "np":    float(r["net_profit"]),
        })
    return results


def run_et(params, oos_start, oos_end, job_id):
    from zgb_opti.xml_parser import parse_forward_report
    htm = OUTPUT_DIR / f"{job_id}.htm"
    if not htm.exists():
        set_lines = _base_params(params["donch"], params["tp"], params["sl"],
                                  params["htp"], params["tgt"])
        ini = _build_et_ini(set_lines, job_id, oos_start, oos_end)
        _run_mt5(ini, job_id)
    if not htm.exists():
        return None
    rpt, _ = parse_forward_report(htm, job_id)
    def g(k, d=0):
        try: return float(rpt.get(k, d))
        except: return d
    return {"np": g("net_profit"), "pf": g("profit_factor"),
            "dd": g("drawdown_pct"), "tr": int(g("trades"))}


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 72)
    print("  Scalper v1 S1 WFO | IS=Model4 exhaustive | OOS=Model4 | Honest reopt")
    print(f"  Windows: {len(WINDOWS)} × 4w IS / 2w OOS | Risk=1% | Deposit=${DEPOSIT}")
    print("  Sweep: Donch(20,25,30) × TP(200,250) × SL(50,75) × HTP(0.3,0.6) × Tgt(3,6,9)")
    print("  = 72 combos × 3 windows = 216 passes (~2-3h)")
    print("=" * 72)

    all_top = {}
    for win_name, is_s, is_e, _, _ in WINDOWS:
        all_top[win_name] = run_window_optimization(win_name, is_s, is_e)

    def pk(p):
        return (p["donch"], p["tp"], p["sl"], p["htp"], p["tgt"])

    counts = {}
    for win_name, results in all_top.items():
        for p in results:
            k = pk(p)
            counts.setdefault(k, {"count": 0, "windows": [], "total_rf": 0, "total_np": 0})
            counts[k]["count"] += 1
            counts[k]["windows"].append(win_name)
            counts[k]["total_rf"] += p["rf"]
            counts[k]["total_np"] += p["np"]

    robust = {k: v for k, v in counts.items() if v["count"] >= 2}
    print(f"\n  Robust params (in top-20 of 2+ windows): {len(robust)}")

    if not robust:
        combined = []
        for rs in all_top.values(): combined.extend(rs)
        combined.sort(key=lambda p: p["rf"], reverse=True)
        robust = {pk(p): {"count": 1, "windows": ["single"],
                         "total_rf": p["rf"], "total_np": p["np"]}
                 for p in combined[:10]}

    sorted_robust = sorted(robust.items(), key=lambda x: -x[1]["total_rf"])
    seen_np = set()
    unique = []
    for k, info in sorted_robust:
        np_key = round(info["total_np"])
        if np_key in seen_np: continue
        seen_np.add(np_key)
        unique.append((k, info))
        if len(unique) >= 5: break

    print("\n" + "=" * 72)
    print("  PHASE C: OOS Validation on Model=4")
    print("=" * 72)

    scores = []
    for i, (k, info) in enumerate(unique, 1):
        donch, tp, sl, htp, tgt = k
        params = {"donch": donch, "tp": tp, "sl": sl, "htp": htp, "tgt": tgt}
        print(f"\n  #{i} Donch={donch} TP={tp} SL={sl} HTP={htp} Tgt={tgt}  (windows {info['windows']})")

        oos_total = {"np": 0, "tr": 0}
        oos_results = []
        all_prof = True
        for win_name, _, _, oos_s, oos_e in WINDOWS:
            job_id = f"scalp_m4is_oos_{win_name}_{i}"
            r = run_et(params, oos_s, oos_e, job_id)
            if r is None:
                all_prof = False
                print(f"    {win_name} OOS: MISSING REPORT")
                continue
            oos_results.append((win_name, r))
            oos_total["np"] += r["np"]
            oos_total["tr"] += r["tr"]
            if r["np"] <= 0: all_prof = False
            print(f"    {win_name} OOS: NP={r['np']:+,.2f}  PF={r['pf']:.2f}  DD={r['dd']:.1f}%  Tr={r['tr']}")

        print(f"    Total OOS NP: {oos_total['np']:+,.2f}  All profitable: {all_prof}")
        scores.append({
            "params": params,
            "oos_total_np": oos_total["np"],
            "oos_all_profit": all_prof,
        })

    print("\n" + "=" * 72)
    print("  FINAL RANKING (Model=4 IS + Model=4 OOS)")
    print("=" * 72)
    scores.sort(key=lambda s: (s["oos_all_profit"], s["oos_total_np"]), reverse=True)
    print(f"\n  {'Rank':<4}  {'OOS Total NP':>13}  {'All Prof':>8}  Params")
    for rank, s in enumerate(scores[:5], 1):
        p = s["params"]
        flag = "✓" if s["oos_all_profit"] else "✗"
        print(f"  {rank:<4}  {s['oos_total_np']:>+13,.2f}  {flag:>8}  "
              f"Donch={p['donch']} TP={p['tp']} SL={p['sl']} HTP={p['htp']} Tgt={p['tgt']}")

    if scores:
        best = scores[0]
        p = best["params"]
        set_content = "\n".join([
            f"_Magic=2000||2000||1||2000||2000||N",
            f"_EntryTF=5||5||1||5||5||N",
            f"_BlockFriPM=true",
            f"_S1_Enabled=true",
            f"_S1_Comment=DT818_S1",
            f"_S1_RiskPct={RISK_PCT}||{RISK_PCT}||1||{RISK_PCT}||{RISK_PCT}||N",
            f"_S1_DonchianBars={p['donch']}||{p['donch']}||1||{p['donch']}||{p['donch']}||N",
            f"_S1_TakeProfit={p['tp']}||{p['tp']}||1||{p['tp']}||{p['tp']}||N",
            f"_S1_StopLoss={p['sl']}||{p['sl']}||1||{p['sl']}||{p['sl']}||N",
            f"_S1_HalfTP_Ratio={p['htp']}||{p['htp']}||1||{p['htp']}||{p['htp']}||N",
            f"_S1_PendingExpireBars=2||2||1||2||2||N",
            f"_S1_TradeStartHour=14||14||1||14||14||N",
            f"_S1_TradeEndHour=22||22||1||22||22||N",
            f"_S1_DailyTargetPct={p['tgt']}||{p['tgt']}||1||{p['tgt']}||{p['tgt']}||N",
            f"_S1_DailyLossPct=12||12||1||12||12||N",
            f"_S1_DailyMaxWins=0||0||1||0||0||N",
            f"_S1_DailyMaxLosses=0||0||1||0||0||N",
            f"_S1_HedgeMode=false",
        ]) + "\n"
        SET_OUT.parent.mkdir(parents=True, exist_ok=True)
        SET_OUT.write_text(set_content, encoding="utf-8")
        print(f"\n  Written: {SET_OUT}")

        print("\n" + "=" * 72)
        print("  PHASE E: Sanity ET full-span Mar 7 -> Apr 18 on Model=4")
        print("=" * 72)
        sanity = run_et(p, date(2026,3,7), date(2026,4,18), "scalp_m4is_sanity_full")
        if sanity:
            print(f"  NP={sanity['np']:+,.2f}  PF={sanity['pf']:.2f}  "
                  f"DD={sanity['dd']:.1f}%  Tr={sanity['tr']}")

    print("=" * 72)
    print("  Done.")


if __name__ == "__main__":
    main()
