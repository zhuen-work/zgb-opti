"""FVG HalfTP sweep — sweep _HalfTP_F1 and _HalfTP_F2 with best FVG params fixed.

Run AFTER S1 and S2 reopts are complete. Update S1/S2 best params below.
81 passes (9 × 9), then ET validation on top 5.
"""
from __future__ import annotations

import io, sys
from datetime import date
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

DD_THRESHOLD  = 30.0
TOP_N         = 5

MT5_TERMINAL  = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN     = 18912087
MT5_SERVER    = "VantageInternational-Live 3"
EA_PATH       = "FBO_FVG_v2"
SYMBOL        = "XAUUSD"
PERIOD        = "M5"
SPREAD        = 45
DEPOSIT_OPT   = 10_000
DEPOSIT_VAL   = 10_000
RISK_PCT      = 3.0

TF_H1  = 16385
TF_H4  = 16388

IS_START = date(2026, 2, 14)
IS_END   = date(2026, 4, 11)
IS_TAG   = "apr11"

OUTPUT_DIR    = Path("output/fvg_v2_halftp_apr11")
SET_OUT       = Path("configs/sets/fvg_v2_3pct_h1h4_halftp_apr11_reopt.set")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

# ── Best FVG S1 params (UPDATE after S1 reopt) ──────────────────────────────
S1_MINSIZE = 1600
S1_MAXAGE  = 250
S1_ZONES   = 2
S1_RR      = 3.5
S1_SLBUF   = 40
S1_EXPIRY  = 7

# ── Best FVG S2 params (from S2 reopt) ───────────────────────────────────────
S2_MINSIZE = 2400
S2_MAXAGE  = 250
S2_ZONES   = 2
S2_RR      = 4.5
S2_SLBUF   = 50
S2_EXPIRY  = 7

# ── HalfTP sweep range ──────────────────────────────────────────────────────
HTP_LO  = 0.0
HTP_HI  = 0.8
HTP_STEP = 0.1
HTP_N   = 9   # 0.0, 0.1, ..., 0.8
TOTAL_PASSES = HTP_N * HTP_N  # 81

# ── Helpers ──────────────────────────────────────────────────────────────────
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


def main():
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    from zgb_opti.xml_parser import parse_optimization_xml, write_passes
    import pandas as pd

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 68)
    print(f"  FVG HalfTP sweep  ({TOTAL_PASSES} passes)")
    print(f"  IS: {IS_START} -> {IS_END}  EA={EA_PATH}")
    print(f"  S1 fixed: MinSz={S1_MINSIZE}  Age={S1_MAXAGE}  Zn={S1_ZONES}  RR={S1_RR}  SLBuf={S1_SLBUF}  Exp={S1_EXPIRY}")
    print(f"  S2 fixed: MinSz={S2_MINSIZE}  Age={S2_MAXAGE}  Zn={S2_ZONES}  RR={S2_RR}  SLBuf={S2_SLBUF}  Exp={S2_EXPIRY}")
    print(f"  DD gate: {DD_THRESHOLD}%")
    print("=" * 68)

    # ── Build set lines ──────────────────────────────────────────────────────
    set_lines = [
        _fix("_BaseMagic", 1000),
        _fix("_CapitalProtectionAmount", "0.0"),
        _fix("_LotMode", 1), _fix("_RiskPct", RISK_PCT),
        _fix("TierBase", 2000), _fix("LotStep", "0.01"),
        _fix("_PendingExpireBars", 2),
        # FBO disabled
        _fix("_FBO1", 0), _fix("_FBO2", 0), _fix("_FBO3", 0),
        "_OrderComment=FBO_A", "_OrderComment2=FBO_B", "_OrderComment3=FBO_C",
        _fix("_time_frame", 30), _fix("_time_frame2", TF_H4), _fix("_time_frame3", TF_H1),
        _fix("_take_profit", 15000), _fix("_take_profit2", 15000), _fix("_take_profit3", 11000),
        _fix("_stop_loss", 5000), _fix("_stop_loss2", 10000), _fix("_stop_loss3", 10000),
        _fix("_Bars", 4), _fix("_Bars2", 6), _fix("_Bars3", 8),
        _fix("_EMA_Period1", 5), _fix("_EMA_Period2", 25), _fix("_EMA_Period3", 10),
        _fix("_HalfTP1", "0.0"), _fix("_HalfTP2", "0.0"), _fix("_HalfTP3", "0.0"),
        # FVG S1 fixed
        _fix("_FVG1", 1),
        "_OrderComment4=FVG_A",
        _fix("_FVG_TF", TF_H1),
        _fix("_FVG_MinSize", S1_MINSIZE),
        _fix("_FVG_MaxAge", S1_MAXAGE),
        _fix("_MaxZones", S1_ZONES),
        _fix("_RR_Ratio", S1_RR),
        _fix("_SL_Buffer", S1_SLBUF),
        _fix("_PendingExpireBars_F1", S1_EXPIRY),
        _sweep("_HalfTP_F1", HTP_LO, HTP_STEP, HTP_LO, HTP_HI),
        # FVG S2 fixed
        _fix("_FVG2", 1),
        "_OrderComment5=FVG_B",
        _fix("_FVG_TF2", TF_H4),
        _fix("_FVG_MinSize2", S2_MINSIZE),
        _fix("_FVG_MaxAge2", S2_MAXAGE),
        _fix("_MaxZones2", S2_ZONES),
        _fix("_RR_Ratio2", S2_RR),
        _fix("_SL_Buffer2", S2_SLBUF),
        _fix("_PendingExpireBars_F2", S2_EXPIRY),
        _sweep("_HalfTP_F2", HTP_LO, HTP_STEP, HTP_LO, HTP_HI),
    ]

    # ── Phase 1: HalfTP sweep ───────────────────────────────────────────────
    job_id = f"fvg_halftp_sweep_{IS_TAG}"
    xml_cached = OUTPUT_DIR / f"{job_id}.xml"
    if xml_cached.exists():
        print(f"  [Cached] {xml_cached.name}")
        collected = xml_cached
    else:
        ini = _build_opti_ini(set_lines, job_id)
        ini_path = OUTPUT_DIR / f"{job_id}.ini"
        ini_path.write_text(ini, encoding="utf-8")
        print(f"  Launching: {job_id}")
        rc = run_mt5_job(MT5_TERMINAL, str(ini_path))
        print(f"  Exit code: {rc}")
        art = find_report_artifact(OUTPUT_DIR, job_id, MT5_TERMINAL)
        if art is None:
            for ext in (".xml",):
                c = MT5_DATA_ROOT / f"{job_id}{ext}"
                if c.exists(): art = c; break
        if art is None:
            raise RuntimeError(f"No report found for {job_id}")
        collected = copy_report_artifact(art, OUTPUT_DIR)
        print(f"  Collected: {collected.name}  ({collected.stat().st_size/1e6:.1f}MB)")

    records, warns = parse_optimization_xml(collected, job_id)
    for w in warns: print(f"  WARN: {w}")
    parquet, _ = write_passes(records, OUTPUT_DIR)
    print(f"  Parsed {len(records)} passes")

    df = pd.read_parquet(parquet)
    df["net_profit"]   = pd.to_numeric(df["net_profit"],   errors="coerce")
    df["drawdown_pct"] = pd.to_numeric(df["drawdown_pct"], errors="coerce")
    df["trades"]       = pd.to_numeric(df["trades"],       errors="coerce")
    df["profit_factor"]= pd.to_numeric(df["profit_factor"], errors="coerce")

    low_dd = df[df["drawdown_pct"] < DD_THRESHOLD]
    pool   = low_dd if not low_dd.empty else df

    htp1_key = [c for c in df.columns if "HalfTP_F1" in c]
    htp2_key = [c for c in df.columns if "HalfTP_F2" in c]
    htp1_col = htp1_key[0] if htp1_key else None
    htp2_col = htp2_key[0] if htp2_key else None

    top5 = (pool.sort_values("net_profit", ascending=False)
                .drop_duplicates(subset=["net_profit"])
                .head(TOP_N))

    print(f"\n  TOP {TOP_N} HalfTP combos (DD < {DD_THRESHOLD}%)")
    print(f"  {'#':<3}  {'NP':>12}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  {'HTP_F1':>7}  {'HTP_F2':>7}")
    print("  " + "-" * 56)
    for rank, (_, r) in enumerate(top5.iterrows(), 1):
        h1 = float(r[htp1_col]) if htp1_col else 0
        h2 = float(r[htp2_col]) if htp2_col else 0
        print(f"  {rank:<3}  {r['net_profit']:>+12,.0f}  {r['profit_factor']:>6.3f}  "
              f"{r['drawdown_pct']:>5.1f}%  {int(r['trades']):>4}  {h1:>7.1f}  {h2:>7.1f}")

    # ── Phase 2: ET validation ───────────────────────────────────────────────
    print(f"\n{'=' * 68}")
    print(f"  ET VALIDATION  ${DEPOSIT_VAL:,}  {RISK_PCT}% risk")
    print(f"{'=' * 68}")

    et_results = []
    for rank, (_, r) in enumerate(top5.iterrows(), 1):
        h1 = float(r[htp1_col]) if htp1_col else 0
        h2 = float(r[htp2_col]) if htp2_col else 0

        et_set = [
            _fix("_BaseMagic", 1000),
            _fix("_CapitalProtectionAmount", "0.0"),
            _fix("_LotMode", 1), _fix("_RiskPct", RISK_PCT),
            _fix("TierBase", 2000), _fix("LotStep", "0.01"),
            _fix("_PendingExpireBars", 2),
            _fix("_FBO1", 0), _fix("_FBO2", 0), _fix("_FBO3", 0),
            "_OrderComment=FBO_A", "_OrderComment2=FBO_B", "_OrderComment3=FBO_C",
            _fix("_time_frame", 30), _fix("_time_frame2", TF_H4), _fix("_time_frame3", TF_H1),
            _fix("_take_profit", 15000), _fix("_take_profit2", 15000), _fix("_take_profit3", 11000),
            _fix("_stop_loss", 5000), _fix("_stop_loss2", 10000), _fix("_stop_loss3", 10000),
            _fix("_Bars", 4), _fix("_Bars2", 6), _fix("_Bars3", 8),
            _fix("_EMA_Period1", 5), _fix("_EMA_Period2", 25), _fix("_EMA_Period3", 10),
            _fix("_HalfTP1", "0.0"), _fix("_HalfTP2", "0.0"), _fix("_HalfTP3", "0.0"),
            _fix("_FVG1", 1), "_OrderComment4=FVG_A",
            _fix("_FVG_TF", TF_H1),
            _fix("_FVG_MinSize", S1_MINSIZE), _fix("_FVG_MaxAge", S1_MAXAGE),
            _fix("_MaxZones", S1_ZONES), _fix("_RR_Ratio", S1_RR),
            _fix("_SL_Buffer", S1_SLBUF), _fix("_PendingExpireBars_F1", S1_EXPIRY),
            _fix("_HalfTP_F1", h1),
            _fix("_FVG2", 1), "_OrderComment5=FVG_B",
            _fix("_FVG_TF2", TF_H4),
            _fix("_FVG_MinSize2", S2_MINSIZE), _fix("_FVG_MaxAge2", S2_MAXAGE),
            _fix("_MaxZones2", S2_ZONES), _fix("_RR_Ratio2", S2_RR),
            _fix("_SL_Buffer2", S2_SLBUF), _fix("_PendingExpireBars_F2", S2_EXPIRY),
            _fix("_HalfTP_F2", h2),
        ]

        et_id = f"fvg_halftp_et_{rank}_{IS_TAG}"
        et_htm = OUTPUT_DIR / f"{et_id}.htm"
        if et_htm.exists():
            print(f"\n  [Cached] {et_htm.name}")
        else:
            ini = _build_et_ini(et_set, et_id)
            ini_path = OUTPUT_DIR / f"{et_id}.ini"
            ini_path.write_text(ini, encoding="utf-8")
            print(f"\n-- ET #{rank}  HTP_F1={h1}  HTP_F2={h2} --")
            print(f"  Launching: {et_id}")
            rc = run_mt5_job(MT5_TERMINAL, str(ini_path))

            art = find_report_artifact(OUTPUT_DIR, et_id, MT5_TERMINAL)
            if art is None:
                for ext in (".htm", ".html"):
                    c = MT5_DATA_ROOT / f"{et_id}{ext}"
                    if c.exists(): art = c; break
            if art:
                copy_report_artifact(art, OUTPUT_DIR)

        # Parse ET result
        from zgb_opti.xml_parser import parse_forward_report
        htm_path = OUTPUT_DIR / f"{et_id}.htm"
        if not htm_path.exists():
            for ext in (".htm", ".html"):
                c = MT5_DATA_ROOT / f"{et_id}{ext}"
                if c.exists():
                    import shutil
                    shutil.copy2(c, htm_path)
                    break
        if htm_path.exists():
            rpt, _ = parse_forward_report(htm_path, et_id)
            et_results.append((rank, rpt, h1, h2))

    # ── Print ET comparison ──────────────────────────────────────────────────
    print(f"\n  {'ET#':<5}  {'NP':>12}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  {'NP/DD':>8}  {'HTP_F1':>7}  {'HTP_F2':>7}")
    print("  " + "-" * 72)
    # parse_forward_report returns dict with keys like 'net_profit', 'profit_factor', etc.
    def _get(rpt, key, default=0):
        try: return float(rpt.get(key, default))
        except: return float(default)

    sorted_et = sorted(et_results, key=lambda x: _get(x[1], "net_profit", 0) / max(_get(x[1], "drawdown_pct", 100), 0.1), reverse=True)
    for rank, rpt, h1, h2 in sorted_et:
        np_val = _get(rpt, "net_profit")
        pf_val = _get(rpt, "profit_factor")
        dd_val = _get(rpt, "drawdown_pct")
        tr_val = int(_get(rpt, "trades"))
        npdd   = int(np_val / dd_val) if dd_val > 0 else 0
        print(f"  {rank:<5}  {np_val:>+12,.0f}  {pf_val:>6.3f}  {dd_val:>5.1f}%  {tr_val:>4}  {npdd:>8,}  {h1:>7.1f}  {h2:>7.1f}")

    # ── Write best set file ──────────────────────────────────────────────────
    if sorted_et:
        best_rank, best_rpt, best_h1, best_h2 = sorted_et[0]
        best_np = _get(best_rpt, "net_profit")
        best_pf = _get(best_rpt, "profit_factor")
        best_dd = _get(best_rpt, "drawdown_pct")
        best_npdd = int(best_np / best_dd * 100) if best_dd > 0 else 0

        set_content = "\n".join([
            f"_BaseMagic=1000||1000||1||1000||1000||N",
            f"_CapitalProtectionAmount=0.0||0.0||1||0.0||0.0||N",
            f"_RiskPct={RISK_PCT}||{RISK_PCT}||1||{RISK_PCT}||{RISK_PCT}||N",
            f"_LotMode=1||1||1||1||1||N",
            f"TierBase=2000||2000||1||2000||2000||N",
            f"LotStep=0.01||0.01||1||0.01||0.01||N",
            f"_PendingExpireBars=2||2||1||2||2||N",
            f"_FBO1=0||0||1||0||0||N", f"_FBO2=0||0||1||0||0||N", f"_FBO3=0||0||1||0||0||N",
            f"_FVG1=1||1||1||1||1||N",
            f"_OrderComment4=FVG_A",
            f"_FVG_TF={TF_H1}||{TF_H1}||1||{TF_H1}||{TF_H1}||N",
            f"_FVG_MinSize={S1_MINSIZE}||{S1_MINSIZE}||1||{S1_MINSIZE}||{S1_MINSIZE}||N",
            f"_FVG_MaxAge={S1_MAXAGE}||{S1_MAXAGE}||1||{S1_MAXAGE}||{S1_MAXAGE}||N",
            f"_MaxZones={S1_ZONES}||{S1_ZONES}||1||{S1_ZONES}||{S1_ZONES}||N",
            f"_RR_Ratio={S1_RR}||{S1_RR}||1||{S1_RR}||{S1_RR}||N",
            f"_SL_Buffer={S1_SLBUF}||{S1_SLBUF}||1||{S1_SLBUF}||{S1_SLBUF}||N",
            f"_PendingExpireBars_F1={S1_EXPIRY}||{S1_EXPIRY}||1||{S1_EXPIRY}||{S1_EXPIRY}||N",
            f"_HalfTP_F1={best_h1}||{best_h1}||1||{best_h1}||{best_h1}||N",
            f"_FVG2=1||1||1||1||1||N",
            f"_OrderComment5=FVG_B",
            f"_FVG_TF2={TF_H4}||{TF_H4}||1||{TF_H4}||{TF_H4}||N",
            f"_FVG_MinSize2={S2_MINSIZE}||{S2_MINSIZE}||1||{S2_MINSIZE}||{S2_MINSIZE}||N",
            f"_FVG_MaxAge2={S2_MAXAGE}||{S2_MAXAGE}||1||{S2_MAXAGE}||{S2_MAXAGE}||N",
            f"_MaxZones2={S2_ZONES}||{S2_ZONES}||1||{S2_ZONES}||{S2_ZONES}||N",
            f"_RR_Ratio2={S2_RR}||{S2_RR}||1||{S2_RR}||{S2_RR}||N",
            f"_SL_Buffer2={S2_SLBUF}||{S2_SLBUF}||1||{S2_SLBUF}||{S2_SLBUF}||N",
            f"_PendingExpireBars_F2={S2_EXPIRY}||{S2_EXPIRY}||1||{S2_EXPIRY}||{S2_EXPIRY}||N",
            f"_HalfTP_F2={best_h2}||{best_h2}||1||{best_h2}||{best_h2}||N",
        ]) + "\n"

        SET_OUT.parent.mkdir(parents=True, exist_ok=True)
        SET_OUT.write_text(set_content, encoding="utf-8")
        print(f"\n  Written: {SET_OUT}")
        print(f"  Best: NP={best_np:+,.0f}  PF={best_pf:.3f}  DD={best_dd:.1f}%  NP/DD={best_npdd:,}")

    print("=" * 68)
    print("  Done.")


if __name__ == "__main__":
    main()
