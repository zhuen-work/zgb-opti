"""Scalper v1 S1 (US session) Walk-Forward Optimization

S2 disabled. S1 swept for US session (14-22 UTC default). RiskPct fixed at 1%.

3 rolling windows (4w IS / 2w OOS):
  W1: IS 2026-02-7  -> 2026-03-7  OOS 2026-03-7 -> 2026-03-21
  W2: IS 2026-02-21 -> 2026-03-21 OOS 2026-03-21 -> 2026-04-4
  W3: IS 2026-03-7  -> 2026-04-4  OOS 2026-04-4 -> 2026-04-18

Criterion: Recovery Factor (NP/DD). Per window: top-20 by RF.
Cross-window robust + OOS validation.
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
DEPOSIT       = 100
RISK_PCT      = 1.0

WINDOWS = [
    ("w1", date(2026,2,7),  date(2026,3,7),  date(2026,3,7),  date(2026,3,21)),
    ("w2", date(2026,2,21), date(2026,3,21), date(2026,3,21), date(2026,4,4)),
    ("w3", date(2026,3,7),  date(2026,4,4),  date(2026,4,4),  date(2026,4,18)),
]

OUTPUT_DIR    = Path("output/scalp_wfo_s1_1pct_apr18")
SET_OUT       = Path("configs/sets/scalp_v1_s1_1pct_apr18.set")
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
        f"Expert={EA_PATH}\nSymbol={SYMBOL}\nPeriod={PERIOD}\nModel=0\n"  # Model=0 for IS speed
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


def _common_disabled_s2():
    """Disable S2, set S2 placeholders."""
    return [
        _fix("_Magic", 2000),
        _fix("_EntryTF", 5),
        "_BlockFriPM=true",
        "_S2_Enabled=false",
        "_S2_Comment=DT818_S2",
        _fix("_S2_RiskPct", "1.0"),
        _fix("_S2_DonchianBars", 20),
        _fix("_S2_TakeProfit", 150),
        _fix("_S2_StopLoss", 50),
        _fix("_S2_HalfTP_Ratio", "0.3"),
        _fix("_S2_PendingExpireBars", 2),
        _fix("_S2_TradeStartHour", 1),
        _fix("_S2_TradeEndHour", 8),
        _fix("_S2_DailyTargetPct", "6"),
        _fix("_S2_DailyLossPct", "12"),
        _fix("_S2_DailyMaxWins", 0),
        _fix("_S2_DailyMaxLosses", 0),
        "_S2_HedgeMode=false",
    ]


def _base_params(donch, tp, sl, htp, tgt, loss, hedge):
    """S1 fixed values (for OOS validation)."""
    hedge_str = "true" if hedge else "false"
    return _common_disabled_s2() + [
        "_S1_Enabled=true",
        "_S1_Comment=DT818_S1",
        _fix("_S1_RiskPct", RISK_PCT),
        _fix("_S1_DonchianBars", donch),
        _fix("_S1_TakeProfit", tp),
        _fix("_S1_StopLoss", sl),
        _fix("_S1_HalfTP_Ratio", htp),
        _fix("_S1_PendingExpireBars", 2),
        _fix("_S1_TradeStartHour", 14),
        _fix("_S1_TradeEndHour", 22),
        _fix("_S1_DailyTargetPct", tgt),
        _fix("_S1_DailyLossPct", loss),
        _fix("_S1_DailyMaxWins", 0),
        _fix("_S1_DailyMaxLosses", 0),
        f"_S1_HedgeMode={hedge_str}",
    ]


def _opt_params_sweep():
    """Narrowed sweep. RiskPct fixed 1%. Loss fixed 12 (non-binding at 1%)."""
    return _common_disabled_s2() + [
        "_S1_Enabled=true",
        "_S1_Comment=DT818_S1",
        _fix("_S1_RiskPct", RISK_PCT),
        _sweep("_S1_DonchianBars",   20, 5,  15, 30),
        _sweep("_S1_TakeProfit",     150, 50, 100, 200),
        _sweep("_S1_StopLoss",       75, 25, 50, 75),
        _sweep("_S1_HalfTP_Ratio",   0.3, 0.3, 0.3, 0.6),
        _fix("_S1_PendingExpireBars", 2),
        _fix("_S1_TradeStartHour", 14),
        _fix("_S1_TradeEndHour", 22),
        _sweep("_S1_DailyTargetPct", 6, 3, 3, 9),
        _fix("_S1_DailyLossPct",     12),
        _fix("_S1_DailyMaxWins", 0),
        _fix("_S1_DailyMaxLosses", 0),
        "_S1_HedgeMode=false||false||1||true||Y",
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
    df["rf"] = df.apply(lambda r: r["net_profit"] / max(r["drawdown_pct"], 0.1), axis=1)

    df = df[(df["net_profit"] > 0) & (df["trades"] >= 10)]
    if df.empty:
        print(f"  Window {win_name}: no profitable passes!")
        return []

    donch_c = [c for c in df.columns if "S1_DonchianBars" in c][0]
    tp_c    = [c for c in df.columns if "S1_TakeProfit" in c][0]
    sl_c    = [c for c in df.columns if "S1_StopLoss" in c][0]
    htp_c   = [c for c in df.columns if "S1_HalfTP_Ratio" in c][0]
    tgt_c   = [c for c in df.columns if "S1_DailyTargetPct" in c][0]
    hedge_c = [c for c in df.columns if "S1_HedgeMode" in c][0]

    for c in [donch_c, tp_c, sl_c, htp_c, tgt_c]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    def _hedge_bool(v):
        return str(v).strip().lower() in ("true", "1", "yes")

    top20 = df.sort_values("rf", ascending=False).head(20)
    print(f"  Window {win_name}: top 5 by RF:")
    print(f"    {'NP':>10}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  {'RF':>6}  Donch  TP  SL  HTP  Tgt  Hdg")
    for _, r in top20.head(5).iterrows():
        print(f"    {r['net_profit']:>+10,.0f}  {r['profit_factor']:>6.2f}  {r['drawdown_pct']:>5.1f}%  "
              f"{int(r['trades']):>4}  {r['rf']:>6.0f}  {int(r[donch_c]):>5}  {int(r[tp_c]):>3}  "
              f"{int(r[sl_c]):>3}  {r[htp_c]:>4.1f}  {int(r[tgt_c]):>3}  "
              f"{'Y' if _hedge_bool(r[hedge_c]) else 'N':>3}")

    results = []
    for _, r in top20.iterrows():
        results.append({
            "donch": int(r[donch_c]),
            "tp": int(r[tp_c]),
            "sl": int(r[sl_c]),
            "htp": round(float(r[htp_c]), 2),
            "tgt": int(r[tgt_c]),
            "loss": 12,
            "hedge": _hedge_bool(r[hedge_c]),
            "rf": float(r["rf"]),
            "np": float(r["net_profit"]),
        })
    return results


def run_et(params, oos_start, oos_end, job_id):
    from zgb_opti.xml_parser import parse_forward_report
    htm = OUTPUT_DIR / f"{job_id}.htm"
    if not htm.exists():
        set_lines = _base_params(params["donch"], params["tp"], params["sl"],
                                  params["htp"], params["tgt"], params["loss"],
                                  params["hedge"])
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
    print("  Scalper v1 S1 (US) Walk-Forward Optimization")
    print("  Criterion: Recovery Factor (NP/DD%)")
    print(f"  Windows: {len(WINDOWS)} × 4w IS / 2w OOS  |  Risk=1% fixed  |  Deposit=${DEPOSIT}")
    print("=" * 70)

    all_top = {}
    for win_name, is_s, is_e, _, _ in WINDOWS:
        all_top[win_name] = run_window_optimization(win_name, is_s, is_e)

    def param_key(p):
        return (p["donch"], p["tp"], p["sl"], p["htp"], p["tgt"], p["loss"], p["hedge"])

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
        all_combined = []
        for results in all_top.values():
            all_combined.extend(results)
        all_combined.sort(key=lambda p: p["rf"], reverse=True)
        robust = {param_key(p): {"count": 1, "windows": ["single"], "total_rf": p["rf"], "total_np": p["np"]}
                  for p in all_combined[:5]}

    print("\n" + "=" * 70)
    print("  PHASE C: OOS Validation")
    print("=" * 70)

    # Dedupe: keep only unique total_np (rounded) — collapses daily-limit ties that produce identical NPs
    sorted_robust = sorted(robust.items(), key=lambda x: -x[1]["total_rf"])
    seen_np = set()
    unique_robust = []
    for k, info in sorted_robust:
        np_key = round(info["total_np"])
        if np_key in seen_np: continue
        seen_np.add(np_key)
        unique_robust.append((k, info))
        if len(unique_robust) >= 5: break

    robust_scores = []
    for i, (k, info) in enumerate(unique_robust, 1):
        donch, tp, sl, htp, tgt, loss, hedge = k
        params = {"donch": donch, "tp": tp, "sl": sl, "htp": htp,
                  "tgt": tgt, "loss": loss, "hedge": hedge}
        hdg = "Y" if hedge else "N"
        print(f"\n  #{i} Donch={donch} TP={tp} SL={sl} HTP={htp} Tgt={tgt} Hedge={hdg}  (in {info['count']} windows: {info['windows']})")

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

    print("\n" + "=" * 70)
    print("  FINAL RANKING")
    print("=" * 70)
    robust_scores.sort(key=lambda s: (s["oos_all_profit"], s["oos_total_np"]), reverse=True)

    print(f"\n  {'Rank':<4}  {'OOS Total NP':>12}  {'All Prof':>8}  Params")
    for rank, s in enumerate(robust_scores[:5], 1):
        p = s["params"]
        flag = "✓" if s["oos_all_profit"] else "✗"
        hdg = "Y" if p["hedge"] else "N"
        print(f"  {rank:<4}  {s['oos_total_np']:>+12,.0f}  {flag:>8}  Donch={p['donch']} TP={p['tp']} SL={p['sl']} HTP={p['htp']} Tgt={p['tgt']} Hedge={hdg}")

    if robust_scores:
        best = robust_scores[0]
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
            f"_S1_DailyLossPct={p['loss']}||{p['loss']}||1||{p['loss']}||{p['loss']}||N",
            f"_S1_DailyMaxWins=0||0||1||0||0||N",
            f"_S1_DailyMaxLosses=0||0||1||0||0||N",
            f"_S1_HedgeMode={'true' if p['hedge'] else 'false'}",
            f"_S2_Enabled=false",
            f"_S2_Comment=DT818_S2",
            f"_S2_RiskPct=1.0||1.0||1||1.0||1.0||N",
            f"_S2_DonchianBars=20||20||1||20||20||N",
            f"_S2_TakeProfit=150||150||1||150||150||N",
            f"_S2_StopLoss=50||50||1||50||50||N",
            f"_S2_HalfTP_Ratio=0.3||0.3||1||0.3||0.3||N",
            f"_S2_PendingExpireBars=2||2||1||2||2||N",
            f"_S2_TradeStartHour=1||1||1||1||1||N",
            f"_S2_TradeEndHour=8||8||1||8||8||N",
            f"_S2_DailyTargetPct=6||6||1||6||6||N",
            f"_S2_DailyLossPct=12||12||1||12||12||N",
            f"_S2_DailyMaxWins=0||0||1||0||0||N",
            f"_S2_DailyMaxLosses=0||0||1||0||0||N",
        ]) + "\n"
        SET_OUT.parent.mkdir(parents=True, exist_ok=True)
        SET_OUT.write_text(set_content, encoding="utf-8")
        print(f"\n  Written: {SET_OUT}")

    print("=" * 70)
    print("  Done.")


if __name__ == "__main__":
    main()
