"""Phase 6: Window Effectiveness Analysis — rank rolling optimization windows by OOS performance."""
from __future__ import annotations

from pathlib import Path

import pandas as pd


def compute_window_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Group by window_weeks and compute OOS (forward) metrics.

    Returns a DataFrame with one row per window length, sorted by window_weeks.
    """

    def _agg(grp: pd.DataFrame) -> pd.Series:
        fwd_np = grp["forward_net_profit"]
        fwd_pf = grp["forward_profit_factor"]
        fwd_dd = grp["forward_drawdown_pct"]
        fwd_ep = grp.get("forward_expected_payoff", pd.Series(dtype=float))
        fwd_eq = grp.get("forward_final_equity", pd.Series(dtype=float))

        profitable = (fwd_np > 0).sum()
        n = len(grp)

        row: dict = {
            "count_jobs": n,
            "mean_forward_net_profit": fwd_np.mean(),
            "median_forward_net_profit": fwd_np.median(),
            "worst_forward_net_profit": fwd_np.min(),
            "best_forward_net_profit": fwd_np.max(),
            "profitable_forward_rate": profitable / n if n else 0.0,
            "mean_forward_profit_factor": fwd_pf.mean(),
            "median_forward_profit_factor": fwd_pf.median(),
            "mean_forward_drawdown_pct": fwd_dd.mean(),
            "median_forward_drawdown_pct": fwd_dd.median(),
        }
        if "forward_expected_payoff" in grp.columns:
            row["mean_forward_expected_payoff"] = grp["forward_expected_payoff"].mean()
            row["median_forward_expected_payoff"] = grp["forward_expected_payoff"].median()
        if "forward_final_equity" in grp.columns:
            row["mean_forward_final_equity"] = grp["forward_final_equity"].mean()

        return pd.Series(row)

    analysis = (
        df.groupby("window_weeks", sort=True)
        .apply(_agg)
        .reset_index()
    )
    return analysis


def rank_windows(analysis: pd.DataFrame) -> pd.DataFrame:
    """Rank windows by OOS performance.

    Priority:
      1. higher median_forward_net_profit
      2. higher profitable_forward_rate
      3. higher median_forward_profit_factor
      4. lower median_forward_drawdown_pct
    """
    df = analysis.copy()

    # Composite score: normalise each criterion to [0,1] then weight equally.
    # Transparent: each component is also kept as a column.
    def _norm(s: pd.Series, ascending: bool = True) -> pd.Series:
        mn, mx = s.min(), s.max()
        if mx == mn:
            return pd.Series(0.5, index=s.index)
        normed = (s - mn) / (mx - mn)
        return normed if ascending else 1 - normed

    df["score_median_np"] = _norm(df["median_forward_net_profit"], ascending=True)
    df["score_profitable_rate"] = _norm(df["profitable_forward_rate"], ascending=True)
    df["score_pf"] = _norm(df["median_forward_profit_factor"], ascending=True)
    df["score_dd"] = _norm(df["median_forward_drawdown_pct"], ascending=False)  # lower is better

    df["composite_score"] = (
        df["score_median_np"] * 0.4
        + df["score_profitable_rate"] * 0.3
        + df["score_pf"] * 0.2
        + df["score_dd"] * 0.1
    )

    df = df.sort_values("composite_score", ascending=False).reset_index(drop=True)
    df.insert(0, "rank", range(1, len(df) + 1))
    return df


def write_analysis_outputs(
    analysis: pd.DataFrame,
    ranking: pd.DataFrame,
    research_dir: Path,
) -> dict[str, Path]:
    research_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}

    def _write(frame: pd.DataFrame, stem: str) -> None:
        pq = research_dir / f"{stem}.parquet"
        csv = research_dir / f"{stem}.csv"
        frame.to_parquet(pq, index=False)
        frame.to_csv(csv, index=False)
        written[stem] = pq

    _write(analysis, "window_analysis")
    _write(ranking, "window_ranking")

    # Human-readable summary
    top = ranking.iloc[0]
    lines = [
        "Window Effectiveness Analysis — Phase 6",
        "=" * 44,
        f"Rows analyzed : {analysis['count_jobs'].sum():.0f}",
        f"Windows compared: {len(analysis)}",
        f"Best window   : {int(top['window_weeks'])}w  "
        f"(score={top['composite_score']:.3f}, "
        f"median_fwd_np={top['median_forward_net_profit']:.2f}, "
        f"profitable_rate={top['profitable_forward_rate']:.0%})",
        "",
        "Ranking:",
    ]
    for _, row in ranking.iterrows():
        lines.append(
            f"  #{int(row['rank'])}  {int(row['window_weeks']):>2}w  "
            f"score={row['composite_score']:.3f}  "
            f"median_np={row['median_forward_net_profit']:>10.2f}  "
            f"win%={row['profitable_forward_rate']:.0%}  "
            f"med_pf={row['median_forward_profit_factor']:.2f}  "
            f"med_dd={row['median_forward_drawdown_pct']:.1f}%"
        )

    summary_path = research_dir / "window_analysis_summary.txt"
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    written["window_analysis_summary"] = summary_path

    return written


def run_window_analysis(
    final_dataset_path: str | Path,
    research_dir: str | Path,
) -> dict:
    """Orchestrate Phase 6. Returns summary dict."""
    rd = Path(research_dir)
    p = Path(final_dataset_path)

    if not p.exists():
        raise FileNotFoundError(f"final_study_dataset not found: {p}")

    df = pd.read_parquet(p)
    n_rows = len(df)

    required = {"window_weeks", "forward_net_profit", "forward_profit_factor", "forward_drawdown_pct"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    analysis = compute_window_analysis(df)
    ranking = rank_windows(analysis)
    written = write_analysis_outputs(analysis, ranking, rd)

    top_window = int(ranking.iloc[0]["window_weeks"])
    windows = sorted(df["window_weeks"].unique().tolist())

    return {
        "n_rows": n_rows,
        "windows": windows,
        "top_window_weeks": top_window,
        "outputs": {k: str(v) for k, v in written.items()},
        "ranking": ranking[["rank", "window_weeks", "composite_score",
                              "median_forward_net_profit", "profitable_forward_rate",
                              "median_forward_profit_factor", "median_forward_drawdown_pct"]].to_dict("records"),
    }
