"""Optimize _Bars (2-10) and _stop_loss (2000-15000 step 1000) for DT818_EA.
IS: 2026-01-24 -> 2026-03-21, genetic, Model=1, Deposit=10M
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
OUTPUT_DIR    = Path("output/dt818_bars_sl_opti")
MT5_DATA_ROOT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400")
DD_THRESHOLD  = 20.0
IS_START      = date(2026, 1, 24)
IS_END        = date(2026, 3, 21)

TESTER_INPUTS = (
    "_BaseMagic=1000||1000||1||1000||1000||N\n"
    "_OrderComment=DT818_A\n"
    "_OrderComment2=DT818_B\n"
    "_CapitalProtectionAmount=0.0||0.0||1||0.0||0.0||N\n"
    "_LotMode=1||1||1||1||1||N\n"
    "_RiskPct=4.0||4.0||1||4.0||4.0||N\n"
    "TierBase=2000||2000||1||2000||2000||N\n"
    "LotStep=0.01||0.01||1||0.01||0.01||N\n"
    "_Trade1=1||1||1||1||1||N\n"
    "_time_frame=30||30||1||30||30||N\n"
    "_take_profit=13500||13500||1||13500||13500||N\n"
    "_stop_loss=10000||2000||1000||2000||15000||Y\n"
    "_EMA_Period1=5||5||1||5||5||N\n"
    "_Bars=4||2||1||2||10||Y\n"
    "_RiskMode1=0||0||1||0||0||N\n"
    "_TSL1=2000||2000||1||2000||2000||N\n"
    "_TSLA1=700||700||1||700||700||N\n"
    "_BETrigger1=2000||2000||1||2000||2000||N\n"
    "_BEBuf1=700||700||1||700||700||N\n"
    "_Trade2=1||1||1||1||1||N\n"
    "_time_frame2=30||30||1||30||30||N\n"
    "_take_profit2=13000||13000||1||13000||13000||N\n"
    "_stop_loss2=10000||2000||1000||2000||15000||Y\n"
    "_EMA_Period2=6||6||1||6||6||N\n"
    "_Bars2=4||2||1||2||10||Y\n"
    "_RiskMode2=0||0||1||0||0||N\n"
    "_TSL2=2200||2200||1||2200||2200||N\n"
    "_TSLA2=1850||1850||1||1850||1850||N\n"
    "_BETrigger2=300||300||1||300||300||N\n"
    "_BEBuf2=700||700||1||700||700||N\n"
)

def main():
    import pandas as pd
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    from zgb_opti.xml_parser import parse_optimization_xml, write_passes

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    job_id   = "opti_dt818_bars_sl"
    xml_path = OUTPUT_DIR / f"{job_id}.xml"

    if xml_path.exists():
        print(f"[Cached] {xml_path.name}")
    else:
        ini = (
            "[Common]\n"
            f"Login={MT5_LOGIN}\nServer={MT5_SERVER}\nProxyEnable=0\nCertInstall=0\nNewsEnable=0\n"
            "\n[Charts]\n\n[Experts]\n\n[Tester]\n"
            "Expert=DT818_EA\nSymbol=XAUUSD\nPeriod=M30\nModel=1\n"
            "Optimization=2\nOptimizationCriterion=0\n"
            "Deposit=10000000\nSpread=45\n"
            f"FromDate={IS_START.strftime('%Y.%m.%d')}\nToDate={IS_END.strftime('%Y.%m.%d')}\n"
            "ForwardMode=0\nVisual=0\nTesterStart=1\nReplaceReport=1\nShutdownTerminal=1\n"
            f"Report={job_id}\n\n[TesterInputs]\n{TESTER_INPUTS}\n"
        )
        ini_path = OUTPUT_DIR / f"{job_id}.ini"
        ini_path.write_text(ini, encoding="utf-8")
        print("Running optimization: _Bars (2-10) x _stop_loss (2000-15000 step 1000), genetic...")
        rc = run_mt5_job(MT5_TERMINAL, str(ini_path))
        print(f"Exit code: {rc}")
        art = find_report_artifact(OUTPUT_DIR, job_id, MT5_TERMINAL)
        if art is None:
            for ext in (".xml", ".htm", ".html"):
                c = MT5_DATA_ROOT / f"{job_id}{ext}"
                if c.exists(): art = c; break
        if art is None:
            raise RuntimeError("No report found")
        copy_report_artifact(art, OUTPUT_DIR)

    records, warns = parse_optimization_xml(xml_path, job_id)
    for w in warns: print(f"WARN: {w}")
    parquet_path, _ = write_passes(records, OUTPUT_DIR)
    print(f"Parsed {len(records)} passes")

    df = pd.read_parquet(parquet_path)
    df["net_profit"]   = pd.to_numeric(df["net_profit"],   errors="coerce")
    df["drawdown_pct"] = pd.to_numeric(df["drawdown_pct"], errors="coerce")

    low_dd = df[df["drawdown_pct"] < DD_THRESHOLD]
    pool   = low_dd if not low_dd.empty else df
    top    = pool.sort_values("net_profit", ascending=False).head(10)

    print(f"\nTop 10 passes (DD < {DD_THRESHOLD}%):")
    print(f"{'NP':>12} {'PF':>7} {'DD%':>6} {'Trades':>7} {'Bars':>5} {'Bars2':>6} {'SL':>7} {'SL2':>7}")
    print("-" * 65)
    for _, r in top.iterrows():
        print(f"{float(r.net_profit):>+12,.0f} {float(r.profit_factor):>7.3f} "
              f"{float(r.drawdown_pct):>6.1f} {int(r.trades):>7} "
              f"{int(float(r.get('param__Bars', 0))):>5} "
              f"{int(float(r.get('param__Bars2', 0))):>6} "
              f"{int(float(r.get('param__stop_loss', 0))):>7} "
              f"{int(float(r.get('param__stop_loss2', 0))):>7}")

    best = pool.sort_values("net_profit", ascending=False).iloc[0]
    print(f"\nBest: Bars={int(float(best.get('param__Bars',0)))}  Bars2={int(float(best.get('param__Bars2',0)))}  "
          f"SL={int(float(best.get('param__stop_loss',0)))}  SL2={int(float(best.get('param__stop_loss2',0)))}")
    print(f"      NP={float(best.net_profit):+,.0f}  PF={float(best.profit_factor):.3f}  "
          f"DD={float(best.drawdown_pct):.1f}%  Trades={int(best.trades)}")
    print(f"\nBaseline (Bars=4, SL=10000): NP=+21,217  PF=1.810  DD=15.1%  Trades=100")

if __name__ == "__main__":
    main()
