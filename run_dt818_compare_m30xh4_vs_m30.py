"""Compare M30×H4 reopt best vs M30×M30 set file on same IS period."""
from __future__ import annotations

import io, sys
from datetime import date
from pathlib import Path

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

MT5_TERMINAL  = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN     = 18912087
MT5_SERVER    = "VantageInternational-Live 3"
EA_PATH       = "DT818_max"
SYMBOL        = "XAUUSD"
SPREAD        = 45
DEPOSIT       = 10_000
RISK_PCT      = 4.0

IS_START = date(2026, 1, 24)
IS_END   = date(2026, 3, 21)

OUTPUT_DIR    = Path("output/dt818_compare_m30xh4_vs_m30")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

_PREV_BEST = {
    "_time_frame": 30, "_take_profit": 9000, "_stop_loss": 7000,
    "_EMA_Period1": 5, "_Bars": 4, "_RiskMode1": 0,
    "_time_frame2": 16388, "_take_profit2": 12500, "_stop_loss2": 8000,
    "_EMA_Period2": 4, "_Bars2": 6, "_RiskMode2": 0,
}

_NEW_ASYM = {
    "_time_frame": 30, "_take_profit": 13000, "_stop_loss": 7500,
    "_EMA_Period1": 5, "_Bars": 4, "_RiskMode1": 0,
    "_time_frame2": 16388, "_take_profit2": 19500, "_stop_loss2": 9500,
    "_EMA_Period2": 5, "_Bars2": 6, "_RiskMode2": 0,
}

_S3_H1 = {"_take_profit3": 9000, "_stop_loss3": 7000, "_time_frame3": 16385, "_EMA_Period3": 5, "_Bars3": 3}

CONFIGS = {
    "cmp_max_s1s2_is": dict(
        label="DT818_max   S1+S2  IS",
        period="M30", risk=4.0, inputs=_PREV_BEST, ea="DT818_max",
    ),
    "cmp_inv_s1s2_is": dict(
        label="DT818_inv   S1+S2  IS",
        period="M30", risk=4.0, inputs=_PREV_BEST, ea="DT818_inv",
    ),
}


def _build_val_ini(inputs, period, report_id, risk, max_entries=1, start=None, end=None, ea=None, s3=None):
    if start is None: start = IS_START
    if end   is None: end   = IS_END
    lines = [
        "_BaseMagic=1000", "_OrderComment=DT818_A", "_OrderComment2=DT818_B",
        "_CapitalProtectionAmount=0.0", "_LotMode=1", f"_RiskPct={risk}",
        "TierBase=2000", "LotStep=0.01", "_Trade1=1",
        f"_time_frame={inputs['_time_frame']}",
        f"_take_profit={inputs['_take_profit']}",
        f"_stop_loss={inputs['_stop_loss']}",
        f"_EMA_Period1={inputs['_EMA_Period1']}",
        f"_Bars={inputs['_Bars']}",
        f"_RiskMode1={inputs['_RiskMode1']}",
        "_Trade2=1",
        f"_time_frame2={inputs['_time_frame2']}",
        f"_take_profit2={inputs['_take_profit2']}",
        f"_stop_loss2={inputs['_stop_loss2']}",
        f"_EMA_Period2={inputs['_EMA_Period2']}",
        f"_Bars2={inputs['_Bars2']}",
        f"_RiskMode2={inputs['_RiskMode2']}",
    ]
    if s3:
        lines += [
            "_Trade3=1", "_OrderComment3=DT818_C",
            f"_take_profit3={s3['_take_profit3']}",
            f"_stop_loss3={s3['_stop_loss3']}",
            f"_time_frame3={s3.get('_time_frame3', 16408)}",
            f"_EMA_Period3={s3.get('_EMA_Period3', 5)}",
            f"_Bars3={s3.get('_Bars3', 3)}",
            "_RiskMode3=0",
        ]
    else:
        lines.append("_Trade3=0")
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert={ea or EA_PATH}\nSymbol={SYMBOL}\nPeriod={period}\nModel=0\n"
        "Optimization=0\n"
        f"Deposit={DEPOSIT}\nSpread={SPREAD}\n"
        f"FromDate={start.strftime('%Y.%m.%d')}\nToDate={end.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n" + "\n".join(lines) + "\n"
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
    print(f"Launch command: {MT5_TERMINAL} /config:{ini_path}")
    rc = run_mt5_job(MT5_TERMINAL, str(ini_path))
    print(f"Exit code: {rc}")
    art = find_report_artifact(OUTPUT_DIR, job_id, MT5_TERMINAL)
    if art is None:
        for ext in (".htm", ".html"):
            c = MT5_DATA_ROOT / f"{job_id}{ext}"
            if c.exists(): art = c; break
    if art is None:
        raise RuntimeError(f"No report found for {job_id}")
    collected = copy_report_artifact(art, OUTPUT_DIR)
    print(f"Collected: {collected.name}")
    return collected


def main():
    from zgb_opti.xml_parser import parse_forward_report

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print(f"  DT818 Compare: Prev best set  vs  Asymmetric TP reopt")
    print(f"  IS: {IS_START} → {IS_END}   ET ${DEPOSIT:,}  (risk per config)")
    print("=" * 60)

    results = {}
    for job_id, cfg in CONFIGS.items():
        print(f"\nRunning: {cfg['label']}")
        dr = cfg.get("date_range")
        s, e = (dr if dr else (IS_START, IS_END))
        htm = _run_mt5(job_id, _build_val_ini(cfg["inputs"], cfg["period"], job_id, cfg["risk"], cfg.get("max_entries", 1), s, e, cfg.get("ea"), cfg.get("s3")))
        m, _ = parse_forward_report(htm, job_id)
        results[job_id] = dict(
            label  = cfg["label"],
            np     = float(m.get("net_profit",    0)),
            pf     = float(m.get("profit_factor", 0)),
            dd     = float(m.get("drawdown_pct",  0)),
            trades = int(m.get("trades", 0)),
        )

    print("\n" + "=" * 72)
    print(f"  {'Config':<42}  {'NP':>10}  {'PF':>6}  {'DD':>6}  {'Tr':>4}")
    print("  " + "-" * 68)
    prev_window = None
    for r in results.values():
        window = r['label'].split()[-1] if len(r['label'].split()) > 1 else ""
        if prev_window and window != prev_window:
            print()
        prev_window = window
        print(f"  {r['label']:<42}  {r['np']:>+10,.0f}  {r['pf']:>6.3f}  {r['dd']:>5.1f}%  {r['trades']:>4}")
    print("=" * 72)


if __name__ == "__main__":
    main()
