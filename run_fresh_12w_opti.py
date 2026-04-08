"""Fresh 12-week two-stage optimization ending March 14, 2026.

Stage 1 — Strategy parameters (genetic, ~25-35 min)
  Fix risk at 3% per strategy.
  Optimize: RR, SMA/BB periods, MinSL, MaxSL for all 5 strategies.
  Base: configs/sets/study_window_params.set
  Period: Dec 21, 2025 -> Mar 14, 2026 (12 weeks)
  Algorithm: Genetic (Optimization=2), Model=1 (1m OHLC), Deposit=10M

Stage 2 — Risk allocation (slow complete, ~15 min)
  Fix strategy params from Stage 1 best pass.
  Optimize: risk % per strategy, 1.5 to 6.0 step 1.5 (4 values each = 1024 combos).
  Algorithm: Slow complete (Optimization=1), Model=1 (1m OHLC), Deposit=10K

Stage 3 — BB filter test (slow complete, ~5 min)
  Fix strategy + risk params from Stage 1/2 best. Enable BB width filter.
  Optimize: BBWidth Period (10-50 step 5), Deviation (1.0-3.0 step 0.5), MaxPoints (500-3000 step 500).
  Algorithm: Slow complete (Optimization=1), 270 combos, Deposit=10M
  Compare best BB-filter-on result vs Stage 2 (BB filter off) — deploy winner.

Stage 4 — Validation backtest
  Run single backtest with final settings to get clean metrics.

Output: configs/sets/portfolio_best_12w_mar14_reopt_jun06.set
        output/fresh_12w_opti/
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
STAGE1_BASE_SET = Path("configs/sets/study_window_params.set")
OUTPUT_DIR      = Path("output/fresh_12w_opti")
FINAL_SET_PATH  = Path("configs/sets/portfolio_best_12w_mar14_reopt_jun06.set")

OPT_START = date(2025, 12, 21)
OPT_END   = date(2026, 3, 14)

MT5_TERMINAL = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN    = 18912087
MT5_SERVER   = "VantageInternational-Live 3"
EA_PATH      = "ZGB5_v14_3"
SYMBOL       = "XAUUSD"
TIMEFRAME    = "M15"
DEPOSIT         = 10_000_000   # Stage 1 & 3 (strategy/filter optimizations)
DEPOSIT_STAGE2  = 10_000       # Stage 2 (risk allocation)
SPREAD          = 45           # Spread override in points — applied to all stages

# Risk ranges for Stage 2
RISK_PARAMS = [
    "InpRiskPercent_S1",
    "InpFractalRiskPercent_S3",
    "InpBBMidRiskPercent_S4",
    "InpBBMidInvRiskPercent_S5",
]
RISK_MIN  = 1.5
RISK_MAX  = 6.0
RISK_STEP = 1.5

# S2 risk fixed at 1.5% — not swept — reduces chop exposure in live execution
S2_RISK_FIXED = 1.5

# Candidate selection: best by net_profit where DD% is below this
DD_THRESHOLD = 20.0

# Force a specific risk allocation for Stage 2 (skips MT5 run, uses these values directly).
# Set to None to use normal optimizer selection.
FORCE_RISK_ALLOCATION: dict | None = {
    "InpRiskPercent_S1":         3.0,   # S1 (SMA Trend): active, PF=2.4 solo
    "InpFractalRiskPercent_S3":  6.0,   # S3 (Fractal): active, PF=1.86 solo
    # S4 and S5 excluded — see FORCE_DISABLE_STRATS below
    # S1+S3 combo: DD=32.9%, PF=2.1 (Every Tick IS) — meets target
}

# Force-disable specific strategies in all stages.
# Combo scan result: S4 has PF=0.75/DD=74% in Every Tick IS window — excluded.
#                   S5 has 0 trades in IS window — excluded.
# Set to empty list to disable no strategies (normal operation).
FORCE_DISABLE_STRATS: list[str] = ["S4", "S5"]

# Map strategy code -> (enable_param, risk_param)
_STRAT_ENABLE_PARAM = {
    "S4": "InpEnableBBMid_S4",
    "S5": "InpEnableBBMidInv_S5",
}

# BB filter ranges for Stage 3
BB_FILTER_PARAMS = ["InpBBWidth_Period", "InpBBWidth_Deviation", "InpBBWidthMaxPoints"]
BB_PERIOD_MIN,  BB_PERIOD_MAX,  BB_PERIOD_STEP  = 10,   50,   5
BB_DEV_MIN,     BB_DEV_MAX,     BB_DEV_STEP     = 1.0,  3.0,  0.5
BB_MAXPTS_MIN,  BB_MAXPTS_MAX,  BB_MAXPTS_STEP  = 500,  1500, 500


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _read_set_lines(set_path: Path) -> list[str]:
    """Read .set file, return non-blank non-comment lines (raw text)."""
    raw = set_path.read_bytes()
    if raw[:2] == b"\xff\xfe":
        text = raw.decode("utf-16-le", errors="replace").lstrip("\ufeff")
    else:
        text = raw.decode("utf-8", errors="replace")
    return [l.strip().replace("\x00", "") for l in text.splitlines()
            if l.strip().replace("\x00", "") and not l.strip().startswith(";")]


def _parse_set_lines(lines: list[str]) -> dict[str, str]:
    """Return {param_name: raw_rhs} for all param=... lines."""
    params: dict[str, str] = {}
    for line in lines:
        if "=" in line:
            name, _, rhs = line.partition("=")
            params[name.strip()] = rhs.strip()
    return params


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
                    report_id: str, optimization: int, deposit: int = DEPOSIT) -> str:
    """Build MT5 INI content for an optimization run.

    optimization: 1 = slow complete, 2 = genetic
    """
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
        f"Period={TIMEFRAME}\n"
        "Model=1\n"
        f"Optimization={optimization}\n"
        f"Deposit={deposit}\n"
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
                        report_id: str) -> str:
    """Build MT5 INI content for a single backtest (no optimization)."""
    # For backtest: strip MT5 optimization suffixes (||...) from each line
    clean_lines: list[str] = []
    for line in set_lines:
        if "=" in line:
            name, _, rhs = line.partition("=")
            value = rhs.split("||")[0].strip()
            clean_lines.append(f"{name.strip()}={value}")
        else:
            clean_lines.append(line)
    tester_inputs = "\n".join(clean_lines)
    # (backtest uses plain value=X lines, no multi-field format needed)
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
        f"Period={TIMEFRAME}\n"
        "Model=0\n"
        "Optimization=0\n"
        f"Deposit={DEPOSIT_STAGE2}\n"
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

    best = pool.sort_values("net_profit", ascending=False).iloc[0]
    return best


# ---------------------------------------------------------------------------
# Stage 2 set builder
# ---------------------------------------------------------------------------
def _build_stage2_set_lines(stage1_set_lines: list[str],
                             best_pass: "pd.Series") -> list[str]:
    """Build Stage 2 .set lines: strategy params fixed from best_pass,
    risk params set to 1.5-6.0 step 1.5 (Y for optimization)."""
    out: list[str] = []
    for line in stage1_set_lines:
        if "=" not in line:
            out.append(line)
            continue

        name, _, rhs = line.partition("=")
        name = name.strip()
        parts = rhs.strip().split("||")
        currently_optimized = len(parts) >= 6 and parts[-1].strip().upper() == "Y"

        if name == "InpEnableReverseStops_S2":
            # S2 disabled — broken in live execution
            out.append(f"{name}=false||false||1||false||false||N")

        elif name == "InpReverseRiskPercent_S2":
            # S2 disabled — fix risk at 0 equivalent (keep param but won't fire)
            out.append(f"{name}=1.5||1.5||1||1.5||1.5||N")

        elif any(name == _STRAT_ENABLE_PARAM.get(s) for s in FORCE_DISABLE_STRATS):
            # Force-disable this strategy
            out.append(f"{name}=false||false||1||false||false||N")

        elif name in RISK_PARAMS:
            # Check if this risk param belongs to a force-disabled strategy
            _disabled_risk_params = {
                "S4": "InpBBMidRiskPercent_S4",
                "S5": "InpBBMidInvRiskPercent_S5",
            }
            is_disabled_risk = any(
                name == _disabled_risk_params.get(s) for s in FORCE_DISABLE_STRATS
            )
            if is_disabled_risk:
                # Strategy disabled — fix risk at minimum, it won't fire
                out.append(f"{name}=1.5||1.5||1||1.5||1.5||N")
            else:
                # Active strategy — set optimization range 1.5 to 6.0 step 1.5
                out.append(f"{name}={RISK_MIN}||{RISK_MIN}||{RISK_STEP}||{RISK_MIN}||{RISK_MAX}||Y")

        elif currently_optimized:
            # Fix to Stage 1 best value
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
                # param not in passes (shouldn't happen), keep original but fix N
                out.append(f"{name}={parts[0]}||{parts[0]}||1||{parts[0]}||{parts[0]}||N")

        else:
            # Not optimized — keep as-is
            out.append(line)

    return out


# ---------------------------------------------------------------------------
# Stage 3 BB filter set builder
# ---------------------------------------------------------------------------
def _build_stage3_bbfilter_set_lines(stage2_set_lines: list[str],
                                      s2_best: "pd.Series") -> list[str]:
    """Build Stage 3 .set lines: strategy + risk params fixed from Stage 2 best,
    InpEnableBBWidthFilter fixed to true, BB filter params set for optimization."""
    out: list[str] = []
    for line in stage2_set_lines:
        if "=" not in line:
            out.append(line)
            continue

        name, _, rhs = line.partition("=")
        name = name.strip()
        parts = rhs.strip().split("||")
        currently_optimized = len(parts) >= 6 and parts[-1].strip().upper() == "Y"

        if name == "InpEnableBBWidthFilter":
            # Force filter on, fixed
            out.append(f"{name}=true||true||1||true||true||N")

        elif name == "InpBBWidth_Period":
            out.append(f"{name}={BB_PERIOD_MIN}||{BB_PERIOD_MIN}||{BB_PERIOD_STEP}||{BB_PERIOD_MIN}||{BB_PERIOD_MAX}||Y")

        elif name == "InpBBWidth_Deviation":
            out.append(f"{name}={BB_DEV_MIN}||{BB_DEV_MIN}||{BB_DEV_STEP}||{BB_DEV_MIN}||{BB_DEV_MAX}||Y")

        elif name == "InpBBWidthMaxPoints":
            out.append(f"{name}={BB_MAXPTS_MIN}||{BB_MAXPTS_MIN}||{BB_MAXPTS_STEP}||{BB_MAXPTS_MIN}||{BB_MAXPTS_MAX}||Y")

        elif currently_optimized:
            # Fix risk params from Stage 2 best
            col = f"param_{name}"
            if col in s2_best.index:
                val = s2_best[col]
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

    return out


# ---------------------------------------------------------------------------
# Final deployment .set writer
# ---------------------------------------------------------------------------
def _write_deployment_set(stage2_set_lines: list[str],
                           best_pass: "pd.Series",
                           output_path: Path) -> None:
    """Write final clean .set: all params fixed, no optimization suffixes.
    Replaces any Y-flagged params with values from best_pass; all others kept as-is.
    Works for both Stage 2 (BB off) and Stage 3 (BB on) deployment sets."""
    out: list[str] = []
    for line in stage2_set_lines:
        if "=" not in line:
            out.append(line)
            continue

        name, _, rhs = line.partition("=")
        name = name.strip()
        parts = rhs.strip().split("||")
        currently_optimized = len(parts) >= 6 and parts[-1].strip().upper() == "Y"

        if currently_optimized:
            # Use best pass value
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
    print(f"  Fresh 12-week optimization: {OPT_START} -> {OPT_END}")
    print(f"  Stage 1: genetic      (strategy params, 3% fixed risk, deposit={DEPOSIT:,})")
    print(f"  Stage 2: slow complete (risk 1.5-6.0% step 1.5, 1024 combos, deposit={DEPOSIT_STAGE2:,})")
    print(f"  Stage 3: slow complete (BB filter on, 270 combos, deposit={DEPOSIT_STAGE2:,})")
    print(f"  Stage 4: validation backtest — deploy winner of Stage 2 vs 3")
    print(f"  Output : {FINAL_SET_PATH}")
    print("=" * 65)

    # ------------------------------------------------------------------ #
    # Stage 1 — Strategy parameters (genetic)                             #
    # ------------------------------------------------------------------ #
    print("\n--- Stage 1: Strategy parameters (genetic) ---")
    s1_job_id = "s1_strategy_params_genetic_dec21_mar14"
    s1_xml    = OUTPUT_DIR / f"{s1_job_id}.xml"

    if s1_xml.exists():
        print(f"  Cached: {s1_xml.name}")
    else:
        s1_lines   = _read_set_lines(STAGE1_BASE_SET)
        s1_ini     = _build_opti_ini(s1_lines, OPT_START, OPT_END, s1_job_id, optimization=2)
        _run_optimization(s1_job_id, s1_ini, OUTPUT_DIR)
        s1_xml = OUTPUT_DIR / f"{s1_job_id}.xml"

    s1_df   = _parse_opti_xml(s1_xml, s1_job_id)
    s1_best = _select_best(s1_df, DD_THRESHOLD)

    print(f"\n  Stage 1 best pass:")
    print(f"    pass_id      : {int(s1_best.pass_id)}")
    print(f"    net_profit   : {s1_best.net_profit:>+14,.0f}")
    print(f"    profit_factor: {s1_best.profit_factor:.3f}")
    print(f"    drawdown_pct : {s1_best.drawdown_pct:.2f}%")
    print(f"    trades       : {int(s1_best.trades)}")
    print(f"  Strategy params:")
    for col in sorted(s1_df.columns):
        if col.startswith("param_") and col not in [f"param_{r}" for r in RISK_PARAMS]:
            print(f"    {col[6:]}: {s1_best[col]}")

    # ------------------------------------------------------------------ #
    # Stage 2 — Risk allocation (slow complete, 1024 combos)              #
    # ------------------------------------------------------------------ #
    print("\n--- Stage 2: Risk allocation (slow complete, 1024 combos) ---")
    s2_job_id = "s2_risk_alloc_slowcomplete_dec21_mar14"
    s2_xml    = OUTPUT_DIR / f"{s2_job_id}.xml"

    if s2_xml.exists():
        print(f"  Cached: {s2_xml.name}")
    else:
        s1_lines    = _read_set_lines(STAGE1_BASE_SET)
        s2_lines    = _build_stage2_set_lines(s1_lines, s1_best)
        s2_set_path = OUTPUT_DIR / "stage2_risk_alloc.set"
        s2_set_path.write_text("\n".join(s2_lines) + "\n", encoding="utf-8")
        print(f"  Stage 2 .set written: {s2_set_path.name}")

        s2_ini = _build_opti_ini(s2_lines, OPT_START, OPT_END, s2_job_id, optimization=1,
                                  deposit=DEPOSIT_STAGE2)
        _run_optimization(s2_job_id, s2_ini, OUTPUT_DIR)
        s2_xml = OUTPUT_DIR / f"{s2_job_id}.xml"

    if FORCE_RISK_ALLOCATION is not None:
        import pandas as pd
        print(f"  FORCED allocation override — skipping optimizer selection")
        forced_row = {f"param_{k}": v for k, v in FORCE_RISK_ALLOCATION.items()}
        # Pull strategy params from s1_best, override risk params
        s2_df   = _parse_opti_xml(s2_xml, s2_job_id)
        s2_best = _select_best(s2_df, DD_THRESHOLD)
        for k, v in forced_row.items():
            s2_best[k] = v
    else:
        s2_df   = _parse_opti_xml(s2_xml, s2_job_id)
        s2_best = _select_best(s2_df, DD_THRESHOLD)

    print(f"\n  Stage 2 best pass:")
    print(f"    pass_id      : {int(s2_best.pass_id)}")
    print(f"    net_profit   : {s2_best.net_profit:>+14,.0f}")
    print(f"    profit_factor: {s2_best.profit_factor:.3f}")
    print(f"    drawdown_pct : {s2_best.drawdown_pct:.2f}%")
    print(f"    trades       : {int(s2_best.trades)}")
    print(f"  Risk allocation:")
    for r in RISK_PARAMS:
        col = f"param_{r}"
        if col in s2_best.index:
            print(f"    {r}: {s2_best[col]}%")

    # ------------------------------------------------------------------ #
    # Stage 3 — BB filter test (slow complete, 270 combos)                #
    # ------------------------------------------------------------------ #
    print("\n--- Stage 3: BB filter test (slow complete, 270 combos) ---")
    s3_job_id = "s3_bbfilter_slowcomplete_dec21_mar14"
    s3_xml    = OUTPUT_DIR / f"{s3_job_id}.xml"

    s1_lines = _read_set_lines(STAGE1_BASE_SET)
    s2_lines = _build_stage2_set_lines(s1_lines, s1_best)

    if s3_xml.exists():
        print(f"  Cached: {s3_xml.name}")
    else:
        s3_lines    = _build_stage3_bbfilter_set_lines(s2_lines, s2_best)
        s3_set_path = OUTPUT_DIR / "stage3_bbfilter.set"
        s3_set_path.write_text("\n".join(s3_lines) + "\n", encoding="utf-8")
        print(f"  Stage 3 .set written: {s3_set_path.name}")

        s3_ini = _build_opti_ini(s3_lines, OPT_START, OPT_END, s3_job_id, optimization=1,
                                  deposit=DEPOSIT_STAGE2)
        _run_optimization(s3_job_id, s3_ini, OUTPUT_DIR)
        s3_xml = OUTPUT_DIR / f"{s3_job_id}.xml"

    s3_lines = _build_stage3_bbfilter_set_lines(s2_lines, s2_best)
    s3_df    = _parse_opti_xml(s3_xml, s3_job_id)
    s3_best  = _select_best(s3_df, DD_THRESHOLD)

    print(f"\n  Stage 3 best pass (BB filter ON):")
    print(f"    pass_id      : {int(s3_best.pass_id)}")
    print(f"    net_profit   : {s3_best.net_profit:>+14,.0f}")
    print(f"    profit_factor: {s3_best.profit_factor:.3f}")
    print(f"    drawdown_pct : {s3_best.drawdown_pct:.2f}%")
    print(f"    trades       : {int(s3_best.trades)}")
    for p in BB_FILTER_PARAMS:
        col = f"param_{p}"
        if col in s3_best.index:
            print(f"    {p}: {s3_best[col]}")

    # ------------------------------------------------------------------ #
    # Compare Stage 2 (BB off) vs Stage 3 (BB on) — deploy winner         #
    # ------------------------------------------------------------------ #
    s2_np = float(s2_best.net_profit)
    s3_np = float(s3_best.net_profit)
    bb_filter_wins = s3_np > s2_np

    print(f"\n  Comparison:")
    print(f"    Stage 2 (BB off) net_profit: {s2_np:>+14,.0f}")
    print(f"    Stage 3 (BB on)  net_profit: {s3_np:>+14,.0f}")
    if bb_filter_wins:
        print(f"  -> BB filter ON wins (+{s3_np - s2_np:,.0f}) — deploying Stage 3 params")
        _write_deployment_set(s3_lines, s3_best, FINAL_SET_PATH)
    else:
        print(f"  -> BB filter OFF wins (+{s2_np - s3_np:,.0f}) — deploying Stage 2 params")
        _write_deployment_set(s2_lines, s2_best, FINAL_SET_PATH)

    # ------------------------------------------------------------------ #
    # Stage 4 — Validation backtest                                        #
    # ------------------------------------------------------------------ #
    print(f"\n--- Stage 4: Validation backtest ---")
    val_job_id = "validation_dec21_mar14"
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
        val_ini     = _build_backtest_ini(final_lines, OPT_START, OPT_END, val_job_id)
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
    result = {
        "opt_start":       str(OPT_START),
        "opt_end":         str(OPT_END),
        "final_set":       str(FINAL_SET_PATH),
        "bb_filter_used":  bb_filter_wins,
        "stage1_pass":     int(s1_best.pass_id),
        "stage2_pass":     int(s2_best.pass_id),
        "stage3_pass":     int(s3_best.pass_id),
        "stage2_np":       s2_np,
        "stage3_np":       s3_np,
        "risk": {r: float(s2_best.get(f"param_{r}", 0)) for r in RISK_PARAMS},
        **metrics,
    }
    out_json = OUTPUT_DIR / "result.json"
    out_json.write_text(json.dumps(result, indent=2), encoding="utf-8")

    # Load baseline for comparison
    baseline_json = Path("output/baseline/baseline_result.json")
    baseline: dict = {}
    if baseline_json.exists():
        baseline = json.loads(baseline_json.read_text(encoding="utf-8"))

    np_new      = metrics.get("net_profit", 0) or 0
    np_base     = baseline.get("net_profit", 0) or 0
    pf_new      = metrics.get("profit_factor", 0) or 0
    pf_base     = baseline.get("profit_factor", 0) or 0
    dd_new      = metrics.get("drawdown_pct", 0) or 0
    dd_base     = baseline.get("drawdown_pct", 0) or 0

    print()
    print("=" * 65)
    print(f"  DONE  —  {OPT_START} -> {OPT_END}")
    print(f"  Final .set  : {FINAL_SET_PATH}")
    print(f"  BB filter   : {'ON (Stage 3 won)' if bb_filter_wins else 'OFF (Stage 2 won)'}")
    print()
    print(f"  {'Metric':<20} {'Fresh optimized':>18} {'Baseline (old set)':>18}  {'Delta':>10}")
    print(f"  {'-'*68}")
    print(f"  {'Net Profit':<20} {np_new:>+18,.0f} {np_base:>+18,.0f}  {np_new - np_base:>+10,.0f}")
    print(f"  {'Profit Factor':<20} {pf_new:>18.3f} {pf_base:>18.3f}  {pf_new - pf_base:>+10.3f}")
    print(f"  {'Drawdown %':<20} {dd_new:>17.1f}% {dd_base:>17.1f}%  {dd_new - dd_base:>+9.1f}%")
    print(f"  {'Trades':<20} {int(metrics.get('trades', 0) or 0):>18} {int(baseline.get('trades', 0) or 0):>18}")
    print()
    print(f"  Risk allocation (Stage 2):")
    for r in RISK_PARAMS:
        print(f"    {r}: {result['risk'].get(r, '?')}%")
    print("=" * 65)
    print(f"\n  Written: {out_json}")


if __name__ == "__main__":
    main()
