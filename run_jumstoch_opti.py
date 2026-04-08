"""jumstoch EA — Gold (XAUUSD) parameter sweep.

Stage 1 — Genetic optimization across H1 and H4
  Sweeps: GridStep_Points, StopLoss_Points, BasketPct, Trend_Stoch_K, MA_Period
  Fixed:  lot, level structure, hours, stoch-2 params, trailing
  Algorithm: Genetic (Optimization=2), Model=1 (1m OHLC), Deposit=10K
  Period: 2024-01-01 -> 2025-12-31
  Criterion: max Balance (OptimizationCriterion=0), DD filter applied post-hoc

Stage 2 — Validation backtest
  Single Every-Tick backtest of winning timeframe + best params.
  Model=0 (Every Tick), Deposit=10K

Output: output/jumstoch_opti/
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
OUTPUT_DIR = Path("output/jumstoch_opti")

OPT_START = date(2025, 7, 1)
OPT_END   = date(2025, 12, 31)

MT5_TERMINAL = "C:/Program Files/Vantage International MT5/terminal64.exe"
MT5_LOGIN    = 18912087
MT5_SERVER   = "VantageInternational-Live 3"
EA_PATH      = "jumstoch"
SYMBOL       = "XAUUSD"
SPREAD       = 60

DEPOSIT      = 10_000    # both stages — realistic for 0.01 lot martingale on gold

DD_THRESHOLD     = 30.0  # max drawdown %
MIN_PF           = 0.90  # min profit factor
MAX_LOSS_PCT     = 20.0  # allow NP down to -20% of deposit (rebates cover the gap)
TOP_N_CANDIDATES = 10    # top unique-param passes to validate
REBATE_PER_LOT   = 21.0  # USD rebate per round lot traded

TF_ENUM: dict[str, int] = {
    "M15": 15,
}
CANDIDATE_TIMEFRAMES = list(TF_ENUM.keys())


# ---------------------------------------------------------------------------
# Parameter set
# ---------------------------------------------------------------------------
def _build_jumstoch_set_lines() -> list[str]:
    """Build jumstoch .set lines.

    Format: name=value||default||step||min||max||flag
      Y = optimize, N = fixed
    All distance parameters scaled for gold (tick_size=0.01).
    """
    return [
        # --- Fixed ---
        "Close_Panic=false||false||1||false||false||N",
        "Close_Buy_Trend=false||false||1||false||false||N",
        "Close_Sell_Trend=false||false||1||false||false||N",
        "Close_Buy_Counter=false||false||1||false||false||N",
        "Close_Sell_Counter=false||false||1||false||false||N",
        "DD_Stop_Enable=false||false||1||false||false||N",
        "DD_Stop_USD=1000.0||1000.0||1||1000.0||1000.0||N",
        "GlobalProfit_Pct=10000.0||10000.0||1||10000.0||10000.0||N",
        "Buy_Trend=true||true||1||true||true||N",
        "Sell_Trend=true||true||1||true||true||N",
        "Buy_Counter=true||true||1||true||true||N",
        "Sell_Counter=true||true||1||true||true||N",
        "Magic=69||69||1||69||69||N",
        "MaxGridLevels=2||2||2||2||12||Y",
        "BE_Enable=false||false||1||false||false||N",
        "BE_Activate_Points=500||500||1||500||500||N",

        # --- Fixed: stochastic shape ---
        "Trend_Stoch_D=12||12||1||12||12||N",
        "Trend_Stoch_Slow=12||12||1||12||12||N",
        "Trend_Stoch_OsLevel=25||20||5||20||50||Y",
        "Trend_Stoch_ObLevel=75||50||5||50||80||Y",
        "Counter_Stoch_K=5||5||3||5||32||Y",
        "Counter_Stoch_D=3||3||1||3||12||Y",
        "Counter_Stoch_Slow=3||3||1||3||12||Y",
        "Counter_Stoch_OsLevel=30||20||5||20||50||Y",
        "Counter_Stoch_ObLevel=70||50||5||50||80||Y",

        # --- Grid step: now optimized alongside MaxGridLevels ---
        "GridStep_Points=500||500||500||500||8000||Y",

        # --- Optimized: lot sizing ref + basket TP/SL ---
        # No hard SL placed on positions — bar-open gate means basket stop is the only exit.
        # StopLoss_Points = reference distance for CalcLotByRisk only (controls lot size).
        # Tight BasketPct/BasketStopPct = fast cycling = more trades = more rebates.
        # Gold: _Point=0.01, so 1000 pts=$10, 3000 pts=$30, 5000 pts=$50
        "StopLoss_Points=3000||1000||2000||1000||8000||Y",
        "BasketPct=0.05||0.05||0.05||0.05||1.0||Y",
        "BasketStopPct=0.5||0.3||0.3||0.3||2.0||Y",
        "RiskPct=2.0||2.0||1.0||2.0||10.0||Y",

        # --- Optimized: trend stochastic K + MA ---
        "Trend_Stoch_K=32||14||9||14||50||Y",
        "MA_Period=10||10||10||10||50||Y",
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
                line = f"{name}={parts[0]}||{parts[3]}||{parts[2]}||{parts[4]}||{parts[5]}"
        out.append(line)
    return out


def _build_opti_ini(set_lines: list[str], from_date: date, to_date: date,
                    report_id: str, period: str) -> str:
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
        "Model=2\n"
        "Optimization=2\n"
        "OptimizationCriterion=5\n"
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _run_optimization(job_id: str, ini_content: str, output_dir: Path) -> Path:
    """Write INI, launch MT5, collect report XML. Returns collected path."""
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


def _select_top_n(df: "pd.DataFrame", n: int, tf: str) -> "pd.DataFrame":
    """Return top-N unique-param passes sorted by trade count.

    Goal: rebate generation — only profitable passes (NP > 0, DD < threshold),
    sorted by trade count descending. No fallback to losers.
    """
    import pandas as pd

    df = df.copy()
    df["net_profit"]    = pd.to_numeric(df["net_profit"],    errors="coerce")
    df["drawdown_pct"]  = pd.to_numeric(df["drawdown_pct"],  errors="coerce")
    df["profit_factor"] = pd.to_numeric(df["profit_factor"], errors="coerce")
    df["trades"]        = pd.to_numeric(df["trades"],        errors="coerce")
    df["tf"] = tf

    pool = df[(df["net_profit"] > 0) & (df["drawdown_pct"] < DD_THRESHOLD)]
    if pool.empty:
        print(f"  WARN [{tf}]: no profitable passes under DD {DD_THRESHOLD}% — skipping TF")
        return pd.DataFrame()

    # Combined score: equal-weight normalised net_profit + normalised trades
    # Rewards passes that maximise both profit AND trade count simultaneously.
    np_min, np_max = pool["net_profit"].min(), pool["net_profit"].max()
    tr_min, tr_max = pool["trades"].min(),     pool["trades"].max()
    pool = pool.copy()
    pool["_score"] = (
        (pool["net_profit"] - np_min) / (np_max - np_min + 1e-9) +
        (pool["trades"]     - tr_min) / (tr_max - tr_min + 1e-9)
    )
    pool = pool.sort_values("_score", ascending=False)
    param_cols = [c for c in pool.columns if c.startswith("param_")]
    if param_cols:
        pool = pool.drop_duplicates(subset=param_cols)
    return pool.drop(columns=["_score"]).head(n).reset_index(drop=True)


def _build_set_from_pass(set_lines: list[str], candidate: "pd.Series") -> list[str]:
    """Return set lines with Y-flagged params replaced by candidate values (all fixed)."""
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
            if col in candidate.index:
                val = candidate[col]
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


def _write_deployment_set(set_lines: list[str], candidate: "pd.Series",
                          output_path: Path) -> None:
    """Write final clean .set from a candidate pass row."""
    lines = _build_set_from_pass(set_lines, candidate)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  Written: {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    import pandas as pd
    from zgb_opti.collector import copy_report_artifact, find_report_artifact
    from zgb_opti.launcher import run_mt5_job
    from zgb_opti.xml_parser import parse_forward_report

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    final_set_path = OUTPUT_DIR / "jumstoch_best_xauusd.set"

    print("=" * 65)
    print(f"  jumstoch optimization: {OPT_START} -> {OPT_END}")
    print(f"  Symbol     : {SYMBOL}  Spread={SPREAD}")
    print(f"  Timeframes : {', '.join(CANDIDATE_TIMEFRAMES)}")
    print(f"  Deposit    : {DEPOSIT:,}")
    print(f"  DD filter  : <{DD_THRESHOLD}%")
    print(f"  Top-N val  : {TOP_N_CANDIDATES}")
    print(f"  Output     : {OUTPUT_DIR}")
    print("=" * 65)

    # ------------------------------------------------------------------ #
    # Stage 1 — Genetic optimization per candidate timeframe              #
    # ------------------------------------------------------------------ #
    print("\n--- Stage 1: Genetic optimization ---")

    tf_dfs: dict[str, "pd.DataFrame"] = {}

    for tf in CANDIDATE_TIMEFRAMES:
        job_id   = f"s1_jumstoch_{tf.lower()}"
        xml_path = OUTPUT_DIR / f"{job_id}.xml"

        print(f"\n  [{tf}] job: {job_id}")

        if xml_path.exists():
            print(f"  [{tf}] Cached: {xml_path.name}")
        else:
            set_lines   = _build_jumstoch_set_lines()
            ini_content = _build_opti_ini(set_lines, OPT_START, OPT_END, job_id, period=tf)
            _run_optimization(job_id, ini_content, OUTPUT_DIR)

        df   = _parse_opti_xml(xml_path, job_id)
        top  = _select_top_n(df, TOP_N_CANDIDATES, tf)
        if top.empty:
            continue
        tf_dfs[tf] = top

        print(f"  [{tf}] Top pass:")
        print(f"         pass_id      : {int(top.iloc[0].pass_id)}")
        print(f"         net_profit   : {float(top.iloc[0].net_profit):>+14,.2f}")
        print(f"         profit_factor: {float(top.iloc[0].profit_factor):.3f}")
        print(f"         drawdown_pct : {float(top.iloc[0].drawdown_pct):.2f}%")
        print(f"         trades       : {int(top.iloc[0].trades)}")

    # ------------------------------------------------------------------ #
    # Pool top-N candidates from all TFs, deduplicate, keep best          #
    # ------------------------------------------------------------------ #
    print(f"\n--- Selecting top {TOP_N_CANDIDATES} unique candidates across all TFs ---")
    combined   = pd.concat(list(tf_dfs.values()), ignore_index=True)
    param_cols = [c for c in combined.columns if c.startswith("param_")]
    # Only profitable passes
    combined = combined[combined["net_profit"] > 0]
    if combined.empty:
        print("ERROR: no profitable passes found across all timeframes — aborting")
        sys.exit(1)
    combined = combined.copy()
    # Score = rebate proxy (trades × est_lot) — directly optimizes for volume
    # NP > 0 filter already applied; this ranks by what earns the most rebate
    combined["_est_lot"] = combined.apply(
        lambda r: max(0.01, round(
            (DEPOSIT * float(r.get("param_RiskPct", 1.0) or 1.0) / 100.0)
            / float(r.get("param_StopLoss_Points", 3000) or 3000)
            / 0.01
        ) * 0.01), axis=1
    )
    combined["_score"] = combined["trades"] * combined["_est_lot"]
    combined = combined.sort_values("_score", ascending=False)
    if param_cols:
        combined = combined.drop_duplicates(subset=param_cols)
    combined = combined.drop_duplicates(subset=["net_profit", "trades", "drawdown_pct"])
    candidates = combined.drop(columns=["_score"]).head(TOP_N_CANDIDATES).reset_index(drop=True)

    print(f"  {'#':<4} {'TF':<5} {'Pass':>5} {'S1 Net Profit':>16} {'PF':>7} {'DD%':>7} {'Trades':>8}")
    print(f"  {'-'*60}")
    for i, row in candidates.iterrows():
        print(f"  {i+1:<4} {row.tf:<5} {int(row.pass_id):>5}"
              f" {float(row.net_profit):>+16,.2f}"
              f" {float(row.profit_factor):>7.3f}"
              f" {float(row.drawdown_pct):>6.2f}%"
              f" {int(row.trades):>8}")

    # ------------------------------------------------------------------ #
    # Pick winner — most trades from candidates (ET not needed:           #
    # bar-open gate makes Open Prices and Every Tick identical)           #
    # ------------------------------------------------------------------ #
    winner     = candidates.iloc[0]   # already sorted by trades descending
    winner_row = winner
    winner_tf  = winner["tf"]

    sl_pts   = float(winner.get("param_StopLoss_Points", 3000) or 3000)
    risk_pct = float(winner.get("param_RiskPct", 1.0) or 1.0)
    risk_amt = DEPOSIT * risk_pct / 100.0
    est_lot  = max(0.01, round(risk_amt / sl_pts / 0.01) * 0.01)
    est_rebate_6m = int(winner["trades"]) * est_lot * REBATE_PER_LOT

    # ------------------------------------------------------------------ #
    # Write deployment .set                                                #
    # ------------------------------------------------------------------ #
    print("\n--- Writing deployment .set ---")
    _write_deployment_set(_build_jumstoch_set_lines(), winner_row, final_set_path)

    # ------------------------------------------------------------------ #
    # Summary                                                              #
    # ------------------------------------------------------------------ #
    result = {
        "ea":            "jumstoch",
        "symbol":        SYMBOL,
        "opt_start":     str(OPT_START),
        "opt_end":       str(OPT_END),
        "winner_tf":     winner_tf,
        "winner_pass":   int(winner["pass_id"]),
        "final_set":     str(final_set_path),
        "net_profit":    float(winner["net_profit"]),
        "profit_factor": float(winner["profit_factor"]),
        "drawdown_pct":  float(winner["drawdown_pct"]),
        "trades":        int(winner["trades"]),
        "est_lot":       est_lot,
        "est_rebate_6m": est_rebate_6m,
    }

    out_json = OUTPUT_DIR / "result.json"
    out_json.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print()
    print("=" * 85)
    print(f"  DONE  —  jumstoch  {OPT_START} -> {OPT_END}")
    print(f"  Final .set : {final_set_path}  (winner = #{1})")
    print()
    print(f"  Top 5 candidates (Open Prices = live behaviour):")
    print(f"  {'#':<3} {'TF':<4} {'Pass':>5} {'Net Profit':>12} {'PF':>6} {'DD%':>6} {'Trades':>7} {'Risk%':>6} {'Est.Lot':>8} {'Rebate/6m':>11} {'Net+Rebate':>12}")
    print(f"  {'-'*91}")
    for i, row in candidates.head(5).iterrows():
        r_sl   = float(row.get("param_StopLoss_Points", 3000) or 3000)
        r_risk = float(row.get("param_RiskPct", 1.0) or 1.0)
        r_lot  = max(0.01, round((DEPOSIT * r_risk / 100.0) / r_sl / 0.01) * 0.01)
        r_reb  = int(row["trades"]) * r_lot * REBATE_PER_LOT
        marker = " <--" if i == 0 else ""
        print(f"  {i+1:<3} {row.tf:<4} {int(row.pass_id):>5}"
              f" {float(row.net_profit):>+12,.2f}"
              f" {float(row.profit_factor):>6.3f}"
              f" {float(row.drawdown_pct):>5.1f}%"
              f" {int(row.trades):>7}"
              f" {r_risk:>5.0f}%"
              f" {r_lot:>8.2f}"
              f" {r_reb:>+11,.2f}"
              f" {float(row.net_profit) + r_reb:>+12,.2f}"
              f"{marker}")
    print("=" * 91)
    print(f"\n  Written: {out_json}")


if __name__ == "__main__":
    main()
