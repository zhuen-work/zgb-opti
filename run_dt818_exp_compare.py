"""ET comparison: old set (dt818_best_7_m30xh4xh1_mar21_reopt_may16) vs new reopt params.
Both run at 4% risk on Every Tick model.
IS: Jan 24 -> Mar 21, 2026.
"""
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
EA_PATH       = "DT818_exp"
SYMBOL        = "XAUUSD"
SPREAD        = 45
DEPOSIT       = 10_000
RISK_PCT      = 4.0

IS_START = date(2026, 1, 24)
IS_END   = date(2026, 3, 21)

OUTPUT_DIR    = Path("output/dt818_exp_compare")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

TF1 = 30     # M30
TF2 = 16388  # H4
TF3 = 16385  # H1

CONFIGS = {
    "old_set": dict(
        label="Old set (may16)",
        ea="DT818_3s",
        tp1=9000,  sl1=7000,  ema1=5, bars1=4,
        tp2=12500, sl2=8000,  ema2=4, bars2=6,
        tp3=16000, sl3=10500, ema3=3, bars3=6,
    ),
    "new_reopt": dict(
        label="New reopt (apr02)",
        ea="DT818_exp",
        tp1=11000, sl1=8500, ema1=4, bars1=3,
        tp2=20000, sl2=7000, ema2=5, bars2=6,
        tp3=19500, sl3=9000, ema3=3, bars3=4,
    ),
}


def _build_val_ini(cfg, report_id):  # cfg must include 'ea' key
    lines = "\n".join([
        "_BaseMagic=1000", "_OrderComment=DT818_A", "_OrderComment2=DT818_B",
        "_OrderComment3=DT818_C",
        "_CapitalProtectionAmount=0.0", "_LotMode=1", f"_RiskPct={RISK_PCT}",
        "TierBase=2000", "LotStep=0.01",
        "_Trade1=1",
        f"_time_frame={TF1}",
        f"_take_profit={cfg['tp1']}",  f"_stop_loss={cfg['sl1']}",
        f"_EMA_Period1={cfg['ema1']}", f"_Bars={cfg['bars1']}",
        "_RiskMode1=0",
        "_Trade2=1",
        f"_time_frame2={TF2}",
        f"_take_profit2={cfg['tp2']}", f"_stop_loss2={cfg['sl2']}",
        f"_EMA_Period2={cfg['ema2']}", f"_Bars2={cfg['bars2']}",
        "_RiskMode2=0",
        "_Trade3=1",
        f"_time_frame3={TF3}",
        f"_take_profit3={cfg['tp3']}", f"_stop_loss3={cfg['sl3']}",
        f"_EMA_Period3={cfg['ema3']}", f"_Bars3={cfg['bars3']}",
        "_RiskMode3=0",
    ])
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert={cfg['ea']}\nSymbol={SYMBOL}\nPeriod=M30\nModel=0\n"
        "Optimization=0\n"
        f"Deposit={DEPOSIT}\nSpread={SPREAD}\n"
        f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{lines}\n"
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
        for ext in (".htm", ".html"):
            c = MT5_DATA_ROOT / f"{job_id}{ext}"
            if c.exists(): art = c; break
    if art is None:
        raise RuntimeError(f"No report found for {job_id}")
    return copy_report_artifact(art, OUTPUT_DIR)


def main():
    from zgb_opti.xml_parser import parse_forward_report

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 68)
    print(f"  DT818_exp ET comparison  —  {RISK_PCT}% risk  Every Tick")
    print(f"  IS: {IS_START} -> {IS_END}   ${DEPOSIT:,}   EA={EA_PATH}")
    print("=" * 68)

    results = {}
    for job_id, cfg in CONFIGS.items():
        print(f"\nRunning: {cfg['label']}  [EA={cfg['ea']}]")
        htm = _run_mt5(job_id, _build_val_ini(cfg, job_id))
        m, _ = parse_forward_report(htm, job_id)
        results[job_id] = dict(
            label  = cfg["label"],
            np     = float(m.get("net_profit",    0)),
            pf     = float(m.get("profit_factor", 0)),
            dd     = float(m.get("drawdown_pct",  0)),
            trades = int(m.get("trades", 0)),
        )

    print("\n" + "=" * 68)
    print(f"  {'Config':<22}  {'NP':>10}  {'PF':>6}  {'DD':>6}  {'Tr':>4}")
    print("  " + "-" * 56)
    for r in results.values():
        print(f"  {r['label']:<22}  {r['np']:>+10,.0f}  {r['pf']:>6.3f}  {r['dd']:>5.1f}%  {r['trades']:>4}")
    print("=" * 68)


if __name__ == "__main__":
    main()
