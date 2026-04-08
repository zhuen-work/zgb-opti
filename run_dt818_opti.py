"""DT818_EA 12-week optimization on XAUUSD.

Stage 1 — All parameters (genetic, per timeframe candidate)
  Sweeps M5, M15, M30, H1, H4. Runs one genetic pass per timeframe.
  Optimize: EMA periods, SL/TP, TSL, BE params, Trade2 enable/disable, lot sizing.
  Criterion: max Balance (OptimizationCriterion=0).
  Algorithm: Genetic (Optimization=2), Model=1 (1m OHLC), Deposit=10M
  Period: Dec 21, 2025 -> Mar 14, 2026

Stage 2 — Validation backtest
  Single Every-Tick backtest of winning timeframe + best params.
  Model=0 (Every Tick), Deposit=10K

Output: configs/sets/dt818_best_12w_mar14_reopt_jun06.set
        output/dt818_12w_opti/
"""
from __future__ import annotations

import io
import json
import sys
from datetime import date
from pathlib import Path

# Force UTF-8 output
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
OUTPUT_DIR     = Path("output/dt818_12w_opti")
FINAL_SET_PATH = Path("configs/sets/dt818_best_12w_mar14_reopt_jun06.set")

OPT_START = date(2025, 12, 21)
OPT_END   = date(2026, 3, 14)

MT5_TERMINAL = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN    = 18912087
MT5_SERVER   = "VantageInternational-Live 3"
EA_PATH      = "DT818_EA"
SYMBOL       = "XAUUSD"
SPREAD       = 45

DEPOSIT       = 10_000_000   # Stage 1 genetic optimization
DEPOSIT_VALID = 10_000       # Stage 2 validation backtest

DD_THRESHOLD = 20.0          # max drawdown % for candidate selection

# MT5 ENUM_TIMEFRAMES integer values for .set files
TF_ENUM: dict[str, int] = {
    "M5":  5,
    "M15": 15,
    "M30": 30,
    "H1":  16385,
    "H4":  16388,
}
CANDIDATE_TIMEFRAMES = list(TF_ENUM.keys())


# ---------------------------------------------------------------------------
# Build DT818_EA parameter set lines
# Format: name=value||default||step||min||max||flag  (flag: Y=optimize, N=fixed)
# ---------------------------------------------------------------------------
def _build_dt818_set_lines(tf_value: int) -> list[str]:
    """Build Stage 1 .set lines for DT818_EA with both timeframes fixed to tf_value."""
    tf = tf_value
    return [
        # --- Fixed: general ---
        "_BaseMagic=1000||1000||1||1000||1000||N",
        "_OrderComment=DT818_EA",
        "_CapitalProtectionAmount=0.0||0.0||1||0.0||0.0||N",

        # --- Fixed: lot sizing (LotMode=1, RiskPct fixed neutral for Stage 1) ---
        "_LotMode=1||1||1||1||1||N",
        "_RiskPct=3.0||3.0||1||3.0||3.0||N",
        "TierBase=2000||2000||1||2000||2000||N",
        "LotStep=0.01||0.01||1||0.01||0.01||N",

        # --- Fixed: Trade1 always on, timeframe fixed per candidate ---
        "_Trade1=1||1||1||1||1||N",
        f"_time_frame={tf}||{tf}||1||{tf}||{tf}||N",

        # --- Optimized: Trade 1 strategy params ---
        "_take_profit=4500||1000||500||1000||10000||Y",
        "_stop_loss=6000||1000||500||1000||10000||Y",
        "_EMA_Period1=30||5||5||5||100||Y",
        "_Bars=3||2||1||2||6||Y",
        "_RiskMode1=1||0||1||0||2||Y",
        "_TSL1=2000||500||500||500||5000||Y",
        "_TSLA1=500||100||100||100||1000||Y",
        "_BETrigger1=2000||500||500||500||5000||Y",
        "_BEBuf1=100||50||50||50||500||Y",

        # --- Optimized: Trade 2 enable + params ---
        "_Trade2=0||0||1||0||1||Y",
        f"_time_frame2={tf}||{tf}||1||{tf}||{tf}||N",
        "_take_profit2=4500||1000||500||1000||10000||Y",
        "_stop_loss2=6000||1000||500||1000||10000||Y",
        "_EMA_Period2=25||5||5||5||100||Y",
        "_Bars2=3||2||1||2||6||Y",
        "_RiskMode2=1||0||1||0||2||Y",
        "_TSL2=2000||500||500||500||5000||Y",
        "_TSLA2=500||100||100||100||1000||Y",
        "_BETrigger2=2000||500||500||500||5000||Y",
        "_BEBuf2=100||50||50||50||500||Y",
    ]


# ---------------------------------------------------------------------------
# INI builders
# ---------------------------------------------------------------------------
def _set_to_ini_lines(set_lines: list[str]) -> list[str]:
    """Convert 6-field .set format to 5-field MT5 [TesterInputs] format.

    .set format : value||default||step||min||max||flag
    INI format  : value||min||step||max||flag
    """
    out: list[str] = []
    for line in set_lines:
        if "=" in line and "||" in line:
            name, _, rest = line.partition("=")
            parts = rest.split("||")
            if len(parts) == 6:
                # drop parts[1] (default), reorder: value, min, step, max, flag
                line = f"{name}={parts[0]}||{parts[3]}||{parts[2]}||{parts[4]}||{parts[5]}"
        out.append(line)
    return out


def _build_opti_ini(set_lines: list[str], from_date: date, to_date: date,
                    report_id: str, period: str, optimization: int = 2) -> str:
    """Build MT5 INI for a genetic optimization run."""
    tester_inputs = "\n".join(_set_to_ini_lines(set_lines))
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\n"
        f"Server={MT5_SERVER}\n"
        "ProxyEnable=0\n"
        "CertInstall=0\n"
        "NewsEnable=0\n"
        "\n"
        "[Charts]\n"
        "\n"
        "[Experts]\n"
        "\n"
        "[Tester]\n"
        f"Expert={EA_PATH}\n"
        f"Symbol={SYMBOL}\n"
        f"Period={period}\n"
        "Model=1\n"
        f"Optimization={optimization}\n"
        "OptimizationCriterion=0\n"
        f"Deposit={DEPOSIT}\n"
        f"Spread={SPREAD}\n"
        f"FromDate={from_date.strftime('%Y.%m.%d')}\n"
        f"ToDate={to_date.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\n"
        "Visual=0\n"
        "TesterStart=1\n"
        "ReplaceReport=1\n"
        "ShutdownTerminal=1\n"
        f"Report={report_id}\n"
        "\n"
        "[TesterInputs]\n"
        f"{tester_inputs}\n"
    )


def _build_backtest_ini(set_lines: list[str], from_date: date, to_date: date,
                        report_id: str, period: str) -> str:
    """Build MT5 INI for a single Every-Tick backtest (no optimization)."""
    clean_lines: list[str] = []
    for line in set_lines:
        if "=" in line:
            name, _, rhs = line.partition("=")
            value = rhs.split("||")[0].strip()
            clean_lines.append(f"{name.strip()}={value}")
        else:
            clean_lines.append(line)
    tester_inputs = "\n".join(clean_lines)
    return (
        "[Common]\n"
        f"Login={MT5_LOGIN}\n"
        f"Server={MT5_SERVER}\n"
        "ProxyEnable=0\n"
        "CertInstall=0\n"
        "NewsEnable=0\n"
        "\n"
        "[Charts]\n"
        "\n"
        "[Experts]\n"
        "\n"
        "[Tester]\n"
        f"Expert={EA_PATH}\n"
        f"Symbol={SYMBOL}\n"
        f"Period={period}\n"
        "Model=0\n"
        "Optimization=0\n"
        f"Deposit={DEPOSIT_VALID}\n"
        f"Spread={SPREAD}\n"
        f"FromDate={from_date.strftime('%Y.%m.%d')}\n"
        f"ToDate={to_date.strftime('%Y.%m.%d')}\n"
        "ForwardMode=0\n"
        "Visual=0\n"
        "TesterStart=1\n"
        "ReplaceReport=1\n"
        "ShutdownTerminal=1\n"
        f"Report={report_id}\n"
        "\n"
        "[TesterInputs]\n"
        f"{tester_inputs}\n"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _read_set_lines(set_path: Path) -> list[str]:
    """Read .set file, return non-blank non-comment lines."""
    raw = set_path.read_bytes()
    if raw[:2] == b"\xff\xfe":
        text = raw.decode("utf-16-le", errors="replace").lstrip("\ufeff")
    else:
        text = raw.decode("utf-8", errors="replace")
    return [l.strip().replace("\x00", "") for l in text.splitlines()
            if l.strip().replace("\x00", "") and not l.strip().startswith(";")]


def _run_optimization(job_id: str, ini_content: str, output_dir: Path) -> Path:
    """Write INI, run MT5, collect XML. Returns path to collected XML."""
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job

    ini_path = output_dir / f"{job_id}.ini"
    ini_path.write_text(ini_content, encoding="utf-8")

    print(f"  Launching MT5 ({job_id})...")
    exit_code = run_mt5_job(MT5_TERMINAL, str(ini_path))
    print(f"  Exit code: {exit_code}")

    artifact = find_report_artifact(output_dir, job_id, MT5_TERMINAL)
    if artifact is None:
        raise RuntimeError(f"No report artifact found for {job_id}")
    collected = copy_report_artifact(artifact, output_dir)
    print(f"  Collected: {collected.name}")
    return collected


def _parse_opti_xml(xml_path: Path, job_id: str) -> "pd.DataFrame":
    """Parse optimization XML, return DataFrame of passes."""
    import pandas as pd
    from zgb_opti.xml_parser import parse_optimization_xml, write_passes

    records, warns = parse_optimization_xml(xml_path, job_id)
    for w in warns:
        print(f"  WARN: {w}")

    parquet_path, _ = write_passes(records, xml_path.parent)
    print(f"  Parsed {len(records)} passes -> {parquet_path.name}")
    return pd.read_parquet(parquet_path)


def _select_best(df: "pd.DataFrame", dd_threshold: float) -> "pd.Series":
    """Select best pass: highest net_profit with DD below threshold.
    Falls back to unconstrained best if none meet threshold."""
    import pandas as pd

    df = df.copy()
    df["net_profit"]   = pd.to_numeric(df["net_profit"],   errors="coerce")
    df["drawdown_pct"] = pd.to_numeric(df["drawdown_pct"], errors="coerce")

    low_dd = df[df["drawdown_pct"] < dd_threshold]
    pool   = low_dd if not low_dd.empty else df

    return pool.sort_values("net_profit", ascending=False).iloc[0]


def _write_deployment_set(set_lines: list[str], best_pass: "pd.Series",
                          output_path: Path) -> None:
    """Write final clean .set: all Y-flagged params replaced with best_pass values."""
    out: list[str] = []
    for line in set_lines:
        if "=" not in line:
            out.append(line)
            continue

        name, _, rhs = line.partition("=")
        name  = name.strip()
        parts = rhs.strip().split("||")
        is_optimized = len(parts) >= 6 and parts[-1].strip().upper() == "Y"

        if is_optimized:
            col = f"param_{name}"
            if col in best_pass.index:
                val = best_pass[col]
                try:
                    f = float(val)
                    val_str = str(int(f)) if f == int(f) else str(f)
                except (TypeError, ValueError):
                    val_str = str(val)
                out.append(f"{name}={val_str}||{val_str}||1||{val_str}||{val_str}||N")
            else:
                out.append(f"{name}={parts[0]}||{parts[0]}||1||{parts[0]}||{parts[0]}||N")
        else:
            out.append(line)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"  Written: {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    import pandas as pd
    from zgb_opti.collector import find_report_artifact, copy_report_artifact
    from zgb_opti.launcher import run_mt5_job
    from zgb_opti.xml_parser import parse_forward_report

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 65)
    print(f"  DT818_EA optimization: {OPT_START} -> {OPT_END}")
    print(f"  Symbol    : {SYMBOL}  Spread={SPREAD}")
    print(f"  Timeframes: {', '.join(CANDIDATE_TIMEFRAMES)}")
    print(f"  Stage 1   : genetic per timeframe (deposit={DEPOSIT:,})")
    print(f"  Stage 2   : validation backtest (deposit={DEPOSIT_VALID:,})")
    print(f"  Output    : {FINAL_SET_PATH}")
    print("=" * 65)

    # ------------------------------------------------------------------ #
    # Stage 1 — Genetic optimization per candidate timeframe              #
    # ------------------------------------------------------------------ #
    print("\n--- Stage 1: Genetic optimization per timeframe ---")

    tf_results: dict[str, tuple[pd.Series, list[str]]] = {}

    for tf in CANDIDATE_TIMEFRAMES:
        job_id   = f"s1_dt818_{tf.lower()}_dec21_mar14"
        xml_path = OUTPUT_DIR / f"{job_id}.xml"

        print(f"\n  [{tf}] job: {job_id}")

        if xml_path.exists():
            print(f"  [{tf}] Cached: {xml_path.name}")
        else:
            set_lines   = _build_dt818_set_lines(TF_ENUM[tf])
            ini_content = _build_opti_ini(set_lines, OPT_START, OPT_END, job_id, period=tf)
            _run_optimization(job_id, ini_content, OUTPUT_DIR)

        df   = _parse_opti_xml(xml_path, job_id)
        best = _select_best(df, DD_THRESHOLD)

        print(f"  [{tf}] Best pass:")
        print(f"         pass_id      : {int(best.pass_id)}")
        print(f"         net_profit   : {float(best.net_profit):>+14,.0f}")
        print(f"         profit_factor: {float(best.profit_factor):.3f}")
        print(f"         drawdown_pct : {float(best.drawdown_pct):.2f}%")
        print(f"         trades       : {int(best.trades)}")

        tf_results[tf] = (best, _build_dt818_set_lines(TF_ENUM[tf]))

    # ------------------------------------------------------------------ #
    # Pick winner timeframe                                                #
    # ------------------------------------------------------------------ #
    print("\n--- Timeframe comparison ---")
    print(f"  {'TF':<6} {'Net Profit':>16} {'PF':>8} {'DD%':>8} {'Trades':>8}")
    print(f"  {'-'*50}")
    for tf in CANDIDATE_TIMEFRAMES:
        b, _ = tf_results[tf]
        print(f"  {tf:<6} {float(b.net_profit):>+16,.0f} {float(b.profit_factor):>8.3f}"
              f" {float(b.drawdown_pct):>7.2f}% {int(b.trades):>8}")

    winner_tf = max(tf_results, key=lambda tf: float(tf_results[tf][0]["net_profit"]))
    winner_best, winner_set_lines = tf_results[winner_tf]

    print(f"\n  -> Winner: {winner_tf}  (net_profit={float(winner_best.net_profit):+,.0f})")

    # ------------------------------------------------------------------ #
    # Write deployment .set                                                #
    # ------------------------------------------------------------------ #
    print(f"\n--- Writing deployment .set ---")
    _write_deployment_set(winner_set_lines, winner_best, FINAL_SET_PATH)

    # ------------------------------------------------------------------ #
    # Stage 2 — Validation backtest (Every Tick)                          #
    # ------------------------------------------------------------------ #
    print(f"\n--- Stage 2: Validation backtest (Every Tick, {winner_tf}) ---")
    val_job_id = f"validation_dt818_{winner_tf.lower()}_dec21_mar14"
    val_report = None
    for ext in (".htm", ".html", ".xml"):
        p = OUTPUT_DIR / f"{val_job_id}{ext}"
        if p.exists():
            val_report = p
            break

    if val_report:
        print(f"  Cached: {val_report.name}")
    else:
        final_lines = _read_set_lines(FINAL_SET_PATH)
        val_ini     = _build_backtest_ini(final_lines, OPT_START, OPT_END, val_job_id, period=winner_tf)
        ini_path    = OUTPUT_DIR / f"{val_job_id}.ini"
        ini_path.write_text(val_ini, encoding="utf-8")

        print("  Launching MT5 (single backtest)...")
        exit_code = run_mt5_job(MT5_TERMINAL, str(ini_path))
        print(f"  Exit code: {exit_code}")

        artifact = find_report_artifact(OUTPUT_DIR, val_job_id, MT5_TERMINAL)
        if artifact is None:
            print("  ERROR: no validation report found")
            sys.exit(1)
        val_report = copy_report_artifact(artifact, OUTPUT_DIR)
        print(f"  Collected: {val_report.name}")

    metrics, warns = parse_forward_report(val_report, val_job_id)
    for w in warns:
        print(f"  WARN: {w}")

    # ------------------------------------------------------------------ #
    # Summary                                                              #
    # ------------------------------------------------------------------ #
    np_val = metrics.get("net_profit",    0) or 0
    pf_val = metrics.get("profit_factor", 0) or 0
    dd_val = metrics.get("drawdown_pct",  0) or 0

    result = {
        "opt_start":    str(OPT_START),
        "opt_end":      str(OPT_END),
        "final_set":    str(FINAL_SET_PATH),
        "winner_tf":    winner_tf,
        "stage1_pass":  int(winner_best.pass_id),
        "stage1_np":    float(winner_best.net_profit),
        "tf_comparison": {
            tf: {
                "net_profit":    float(tf_results[tf][0].net_profit),
                "profit_factor": float(tf_results[tf][0].profit_factor),
                "drawdown_pct":  float(tf_results[tf][0].drawdown_pct),
                "trades":        int(tf_results[tf][0].trades),
            }
            for tf in CANDIDATE_TIMEFRAMES
        },
        **metrics,
    }

    out_json = OUTPUT_DIR / "result.json"
    out_json.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print()
    print("=" * 65)
    print(f"  DONE  —  {OPT_START} -> {OPT_END}")
    print(f"  Winner timeframe : {winner_tf}")
    print(f"  Final .set       : {FINAL_SET_PATH}")
    print()
    print(f"  Validation backtest (Every Tick):")
    print(f"    Net Profit   : {np_val:>+14,.0f}")
    print(f"    Profit Factor: {pf_val:>14.3f}")
    print(f"    Drawdown %   : {dd_val:>13.1f}%")
    print(f"    Trades       : {int(metrics.get('trades', 0) or 0):>14}")
    print("=" * 65)
    print(f"\n  Written: {out_json}")


if __name__ == "__main__":
    main()
