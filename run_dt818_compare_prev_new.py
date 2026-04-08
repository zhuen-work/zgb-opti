"""Compare previous vs new DT818 settings on Every Tick ($5K, 4% risk, same IS period)."""
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
PERIOD        = "M30"
SPREAD        = 45
DEPOSIT       = 5_000
IS_START      = date(2026, 1, 24)
IS_END        = date(2026, 3, 21)
OUTPUT_DIR    = Path("output/dt818_compare_prev_new")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")

PREV = dict(
    take_profit=13500, stop_loss=10000, EMA_Period1=5, Bars=4,
    RiskMode1=0, TSL1=2000, TSLA1=700, BETrigger1=2000, BEBuf1=700,
    take_profit2=13000, stop_loss2=10000, EMA_Period2=6, Bars2=4,
    RiskMode2=0, TSL2=2200, TSLA2=1850, BETrigger2=300, BEBuf2=700,
)

NEW = dict(
    take_profit=11000, stop_loss=13000, EMA_Period1=4, Bars=4,
    RiskMode1=2, TSL1=2000, TSLA1=700, BETrigger1=3250, BEBuf1=950,
    take_profit2=12500, stop_loss2=10500, EMA_Period2=14, Bars2=4,
    RiskMode2=2, TSL2=2200, TSLA2=1850, BETrigger2=1800, BEBuf2=1000,
)


def _inputs(p):
    return "\n".join([
        "_BaseMagic=1000", "_OrderComment=DT818_A", "_OrderComment2=DT818_B",
        "_CapitalProtectionAmount=0.0", "_LotMode=1", "_RiskPct=4.0",
        "TierBase=2000", "LotStep=0.01", "_Trade1=1", "_time_frame=30",
        f"_take_profit={p['take_profit']}", f"_stop_loss={p['stop_loss']}",
        f"_EMA_Period1={p['EMA_Period1']}", f"_Bars={p['Bars']}",
        f"_RiskMode1={p['RiskMode1']}", f"_TSL1={p['TSL1']}", f"_TSLA1={p['TSLA1']}",
        f"_BETrigger1={p['BETrigger1']}", f"_BEBuf1={p['BEBuf1']}",
        "_Trade2=1", "_time_frame2=30",
        f"_take_profit2={p['take_profit2']}", f"_stop_loss2={p['stop_loss2']}",
        f"_EMA_Period2={p['EMA_Period2']}", f"_Bars2={p['Bars2']}",
        f"_RiskMode2={p['RiskMode2']}", f"_TSL2={p['TSL2']}", f"_TSLA2={p['TSLA2']}",
        f"_BETrigger2={p['BETrigger2']}", f"_BEBuf2={p['BEBuf2']}",
    ])


def _build_ini(tester_inputs, report_id):
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
        "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
        f"Expert=DT818_EA\nSymbol={SYMBOL}\nPeriod={PERIOD}\nModel=0\n"
        "Optimization=0\n"
        f"Deposit={DEPOSIT}\nSpread={SPREAD}\n"
        f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
        f"Report={report_id}\n\n[TesterInputs]\n{tester_inputs}\n"
    )


def _run(job_id, ini):
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    ini_path = OUTPUT_DIR / f"{job_id}.ini"
    ini_path.write_text(ini, encoding="utf-8")
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


def _parse(path, job_id):
    from zgb_opti.xml_parser import parse_forward_report
    m, warns = parse_forward_report(path, job_id)
    for w in warns: print(f"  WARN: {w}")
    return m


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print("=" * 62)
    print(f"  DT818 Previous vs New  |  ET $5K 4%  |  IS period")
    print(f"  {IS_START} -> {IS_END}")
    print("=" * 62)

    results = {}
    for label, params in [("prev", PREV), ("new", NEW)]:
        job_id = f"bt_dt818_{label}"
        cached = next((OUTPUT_DIR / f"{job_id}{e}" for e in (".htm", ".html")
                       if (OUTPUT_DIR / f"{job_id}{e}").exists()), None)
        if cached:
            print(f"\n[{label.upper()}] Cached: {cached.name}")
            results[label] = _parse(cached, job_id)
        else:
            print(f"\n[{label.upper()}] Running...")
            htm = _run(job_id, _build_ini(_inputs(params), job_id))
            results[label] = _parse(htm, job_id)

    p = results["prev"]
    n = results["new"]

    def nf(v):
        try: return f"{float(v):>+12,.0f}"
        except: return f"{'N/A':>12}"
    def ff(v, fmt=".3f"):
        try: return f"{float(v):>12{fmt}}"
        except: return f"{'N/A':>12}"

    print(f"\n{'=' * 62}")
    print(f"  {'Metric':<24} {'Previous':>14} {'New':>14}")
    print(f"  {'-'*54}")
    print(f"  {'Net Profit':<24}{nf(p.get('net_profit'))}{nf(n.get('net_profit'))}")
    print(f"  {'Return %':<24}{ff(p.get('return_pct','0'),'.1f')}{ff(n.get('return_pct','0'),'.1f')}")
    print(f"  {'Profit Factor':<24}{ff(p.get('profit_factor'))}{ff(n.get('profit_factor'))}")
    print(f"  {'Drawdown %':<24}{ff(p.get('drawdown_pct'),'.1f')}{ff(n.get('drawdown_pct'),'.1f')}")
    print(f"  {'Trades':<24}{int(p.get('trades') or 0):>14}{int(n.get('trades') or 0):>14}")
    print(f"{'=' * 62}")

    # Winner
    try:
        pnp = float(p.get('net_profit', 0))
        nnp = float(n.get('net_profit', 0))
        winner = "PREVIOUS" if pnp >= nnp else "NEW"
        print(f"\n  Winner: {winner} settings")
    except:
        pass


if __name__ == "__main__":
    main()
