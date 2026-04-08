"""DT818_exp TF combo screening — EMA fixed at 2, BB 20/2.0, scaled TP/SL per TF."""
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
SYMBOL        = "XAUUSD"
SPREAD        = 45
DEPOSIT       = 10_000
RISK_PCT      = 4.0

IS_START = date(2026, 1, 24)
IS_END   = date(2026, 3, 21)

OUTPUT_DIR    = Path("output/dt818_exp_tf_screen")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

# EMA fixed at 2, BB 20/2.0, TP/SL scaled per TF
CONFIGS = {
    "exp_m5xm15":  dict(label="M5×M15",   chart="M5",  tf1=5,     tf2=15,    tp1=3000,  sl1=2000,  tp2=4000,  sl2=2500),
    "exp_m15xm30": dict(label="M15×M30",  chart="M15", tf1=15,    tf2=30,    tp1=6000,  sl1=4000,  tp2=8000,  sl2=5000),
    "exp_m30xh1":  dict(label="M30×H1",   chart="M30", tf1=30,    tf2=16385, tp1=8000,  sl1=5500,  tp2=12000, sl2=8000),
    "exp_m30xh4":  dict(label="M30×H4",   chart="M30", tf1=30,    tf2=16388, tp1=9000,  sl1=7000,  tp2=14000, sl2=9000),
    "exp_h1xh4":   dict(label="H1×H4",    chart="H1",  tf1=16385, tf2=16388, tp1=12000, sl1=8000,  tp2=16000, sl2=10000),
}


def _build_val_ini(cfg, report_id):
    lines = [
        "_BaseMagic=1000", "_OrderComment=DT818_A", "_OrderComment2=DT818_B",
        "_CapitalProtectionAmount=0.0", "_LotMode=1", f"_RiskPct={RISK_PCT}",
        "TierBase=2000", "LotStep=0.01",
        "_Trade1=1",
        f"_time_frame={cfg['tf1']}",
        f"_take_profit={cfg['tp1']}",
        f"_stop_loss={cfg['sl1']}",
        "_EMA_Period1=2",
        "_BB_Period1=20",
        "_BB_Dev1=2.0",
        "_RiskMode1=0",
        "_Trade2=1",
        f"_time_frame2={cfg['tf2']}",
        f"_take_profit2={cfg['tp2']}",
        f"_stop_loss2={cfg['sl2']}",
        "_EMA_Period2=2",
        "_BB_Period2=20",
        "_BB_Dev2=2.0",
        "_RiskMode2=0",
    ]
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert=DT818_exp\nSymbol={SYMBOL}\nPeriod={cfg['chart']}\nModel=0\n"
        "Optimization=0\n"
        f"Deposit={DEPOSIT}\nSpread={SPREAD}\n"
        f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
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
    print(f"  Launching: {job_id}")
    rc = run_mt5_job(MT5_TERMINAL, str(ini_path))
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
    print("  DT818_exp TF combo screen — EMA=2 fixed, BB 20/2.0")
    print(f"  IS: {IS_START} → {IS_END}   ${DEPOSIT:,}   {RISK_PCT}% risk")
    print(f"  Baseline (DT818_max M30×H4): NP=+$41,397  PF=2.090  DD=11.8%  Tr=88")
    print("=" * 68)

    results = {}
    for job_id, cfg in CONFIGS.items():
        print(f"\nRunning: DT818_exp {cfg['label']}")
        htm = _run_mt5(job_id, _build_val_ini(cfg, job_id))
        m, _ = parse_forward_report(htm, job_id)
        results[job_id] = dict(
            label  = cfg["label"],
            np     = float(m.get("net_profit",    0)),
            pf     = float(m.get("profit_factor", 0)),
            dd     = float(m.get("drawdown_pct",  0)),
            trades = int(m.get("trades", 0)),
        )

    print("\n" + "=" * 72)
    print(f"  {'TF Combo':<14}  {'NP':>10}  {'PF':>6}  {'DD':>6}  {'Tr':>4}")
    print("  " + "-" * 56)
    for r in results.values():
        print(f"  {r['label']:<14}  {r['np']:>+10,.0f}  {r['pf']:>6.3f}  {r['dd']:>5.1f}%  {r['trades']:>4}")
    print(f"  {'DT818_max M30×H4':<14}  {41397:>+10,}  {'2.090':>6}  {'11.8%':>6}  {88:>4}")
    print("=" * 72)


if __name__ == "__main__":
    main()
