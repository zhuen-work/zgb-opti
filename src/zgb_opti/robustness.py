"""Phase 2B: Parameter Robustness Study.

Determines whether optimizer-selected parameters are robust plateaus or fragile spikes.

Four component scores per window:
  clustering_score   — top-N passes cluster tightly (low dispersion = good)
  stability_score    — selected parameters recur consistently across jobs (low spread = good)
  neighbor_score     — selected result is close to neighborhood median (small dropoff = good)
  plateau_score      — neighborhood classification (stable plateau = good)

Final robustness_score = 0.30*clustering + 0.30*stability + 0.30*neighbor + 0.10*plateau
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import NamedTuple

import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TOP_N = 10                # top passes used for clustering analysis
NEIGHBOR_RADIUS = 0.25   # normalized parameter distance threshold for neighbors
MIN_NEIGHBORS = 3        # minimum neighbors required for plateau classification

_WEIGHTS = {
    "clustering": 0.30,
    "stability": 0.30,
    "neighbor": 0.30,
    "plateau": 0.10,
}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_research_datasets(research_dir: str | Path) -> dict[str, pd.DataFrame]:
    """Load required parquet files.  Raises FileNotFoundError for missing required files."""
    rd = Path(research_dir)
    required = {
        "all_passes": rd / "all_passes.parquet",
        "selected_candidates": rd / "selected_candidates.parquet",
        "final_study_dataset": rd / "final_study_dataset.parquet",
        "window_ranking": rd / "window_ranking.parquet",
    }
    datasets: dict[str, pd.DataFrame] = {}
    for name, path in required.items():
        if not path.exists():
            raise FileNotFoundError(f"Required dataset missing: {path}")
        datasets[name] = pd.read_parquet(path)
    return datasets


def identify_parameter_columns(df: pd.DataFrame) -> list[str]:
    """Return sorted list of param_ columns present in df."""
    return sorted(c for c in df.columns if c.startswith("param_"))


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _param_ranges(df: pd.DataFrame, param_cols: list[str]) -> dict[str, float]:
    """Return per-param range (max - min) over the whole dataframe."""
    ranges: dict[str, float] = {}
    for col in param_cols:
        lo = df[col].min()
        hi = df[col].max()
        ranges[col] = float(hi - lo) if (hi - lo) > 0 else 1.0
    return ranges


def _normalise_params(df: pd.DataFrame, param_cols: list[str],
                      ranges: dict[str, float]) -> pd.DataFrame:
    """Return DataFrame of normalised param values in [0, 1]."""
    normed = df[param_cols].copy().astype(float)
    for col in param_cols:
        normed[col] = normed[col] / ranges[col]
    return normed


def _euclidean_distance(a: pd.Series, b: pd.Series) -> float:
    diff = a.values - b.values
    return float(math.sqrt((diff * diff).sum()))


# ---------------------------------------------------------------------------
# A. Top-N clustering
# ---------------------------------------------------------------------------

def compute_topn_clustering(
    all_passes: pd.DataFrame,
    param_cols: list[str],
    top_n: int = TOP_N,
) -> pd.DataFrame:
    """For each job, measure dispersion of top-N passes in parameter space.

    clustering_score = 1 - mean_pairwise_distance_normalised
    High score → tight cluster → parameters agree at the top.
    """
    ranges = _param_ranges(all_passes, param_cols)
    rows: list[dict] = []

    for job_id, grp in all_passes.groupby("job_id", sort=False):
        if "result" not in grp.columns or grp["result"].isna().all():
            rows.append({"job_id": job_id, "clustering_score": float("nan"),
                         "top_n_used": 0, "mean_pairwise_dist": float("nan")})
            continue

        top = grp.nlargest(min(top_n, len(grp)), "result")
        if len(top) < 2:
            rows.append({"job_id": job_id, "clustering_score": 1.0,
                         "top_n_used": len(top), "mean_pairwise_dist": 0.0})
            continue

        normed = _normalise_params(top, param_cols, ranges)
        vectors = normed.values.tolist()
        n = len(vectors)
        total_dist = 0.0
        count = 0
        for i in range(n):
            for j in range(i + 1, n):
                a = pd.Series(vectors[i])
                b = pd.Series(vectors[j])
                diff = a.values - b.values
                total_dist += float(math.sqrt((diff * diff).sum()))
                count += 1

        # Theoretical max distance for p-dim unit cube: sqrt(p)
        max_possible = math.sqrt(len(param_cols))
        mean_dist = total_dist / count if count else 0.0
        score = max(0.0, 1.0 - mean_dist / max_possible) if max_possible > 0 else 1.0

        rows.append({
            "job_id": job_id,
            "clustering_score": round(score, 4),
            "top_n_used": n,
            "mean_pairwise_dist": round(mean_dist, 4),
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# B. Parameter stability across jobs within window
# ---------------------------------------------------------------------------

def compute_parameter_stability(
    selected: pd.DataFrame,
    param_cols: list[str],
) -> pd.DataFrame:
    """Within each window, measure how consistently parameter sets recur.

    stability_score = 1 - mean_normalised_std_across_params
    High score → same parameters chosen repeatedly within window.
    """
    if "window_weeks" not in selected.columns:
        return pd.DataFrame(columns=["window_weeks", "stability_score"])

    ranges = _param_ranges(selected, param_cols)
    rows: list[dict] = []

    for window, grp in selected.groupby("window_weeks", sort=True):
        if len(grp) < 2:
            rows.append({"window_weeks": window, "stability_score": 1.0,
                         "n_jobs": len(grp), "mean_param_cv": 0.0})
            continue

        normed = _normalise_params(grp, param_cols, ranges)
        # Mean coefficient of variation in normalised space
        stds = normed.std(ddof=0)
        mean_std = float(stds.mean())
        score = max(0.0, 1.0 - mean_std * 2)  # *2 so 0.5 std → 0 score

        rows.append({
            "window_weeks": window,
            "stability_score": round(score, 4),
            "n_jobs": len(grp),
            "mean_param_cv": round(mean_std, 4),
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# C. Neighbor sensitivity
# ---------------------------------------------------------------------------

def compute_neighbor_sensitivity(
    all_passes: pd.DataFrame,
    selected: pd.DataFrame,
    param_cols: list[str],
    radius: float = NEIGHBOR_RADIUS,
    min_neighbors: int = MIN_NEIGHBORS,
) -> pd.DataFrame:
    """For each selected candidate, find passes within radius in normalised space.

    neighbor_score = 1 - mean_dropoff
    dropoff = (selected_result - neighbor_median_result) / |selected_result|
    High score → small dropoff → plateau-like surface.
    """
    ranges = _param_ranges(all_passes, param_cols)
    rows: list[dict] = []

    # Pre-normalise all passes per job
    all_normed = _normalise_params(all_passes, param_cols, ranges)
    all_normed["_job_id"] = all_passes["job_id"].values
    all_normed["_result"] = all_passes["result"].values

    for _, cand in selected.iterrows():
        job_id = cand["job_id"]
        sel_result = float(cand.get("result", float("nan")))

        job_passes = all_normed[all_normed["_job_id"] == job_id]
        if job_passes.empty or pd.isna(sel_result):
            rows.append({"job_id": job_id, "neighbor_score": float("nan"),
                         "n_neighbors": 0, "dropoff": float("nan"),
                         "neighbor_median_result": float("nan")})
            continue

        sel_vec = pd.Series([float(cand[col]) / ranges[col] for col in param_cols])
        dists = job_passes[param_cols].apply(
            lambda row: math.sqrt(((row.values - sel_vec.values) ** 2).sum()), axis=1
        )
        neighbors = job_passes[dists <= radius]
        # Exclude exact match (distance ~0) to avoid self
        neighbors = neighbors[dists[dists <= radius] > 1e-9]

        n = len(neighbors)
        if n < min_neighbors:
            rows.append({"job_id": job_id, "neighbor_score": 0.5,
                         "n_neighbors": n, "dropoff": float("nan"),
                         "neighbor_median_result": float("nan"),
                         "neighbor_note": "insufficient_neighbors"})
            continue

        neighbor_median = float(neighbors["_result"].median())
        if abs(sel_result) < 1e-9:
            dropoff = 0.0
        else:
            dropoff = max(0.0, (sel_result - neighbor_median) / abs(sel_result))

        score = max(0.0, 1.0 - dropoff)

        rows.append({
            "job_id": job_id,
            "neighbor_score": round(score, 4),
            "n_neighbors": n,
            "dropoff": round(dropoff, 4),
            "neighbor_median_result": round(neighbor_median, 4),
            "neighbor_note": "ok",
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# D. Plateau vs spike classification
# ---------------------------------------------------------------------------

def classify_plateau_vs_spike(neighbor_df: pd.DataFrame) -> pd.DataFrame:
    """Add plateau_label and plateau_score columns based on neighbor sensitivity."""
    df = neighbor_df.copy()

    def _classify(row: pd.Series) -> tuple[str, float]:
        note = str(row.get("neighbor_note", ""))
        if note == "insufficient_neighbors":
            return "insufficient_evidence", 0.5
        ns = row.get("neighbor_score", float("nan"))
        if pd.isna(ns):
            return "unknown", 0.5
        if ns >= 0.75:
            return "stable_plateau", 1.0
        if ns >= 0.50:
            return "mild_plateau", 0.75
        if ns >= 0.25:
            return "sharp_spike", 0.25
        return "extreme_spike", 0.0

    labels, scores = zip(*df.apply(_classify, axis=1)) if len(df) else ([], [])
    df["plateau_label"] = list(labels)
    df["plateau_score"] = list(scores)
    return df


# ---------------------------------------------------------------------------
# E. Window-level robustness score
# ---------------------------------------------------------------------------

def score_window_robustness(
    clustering_df: pd.DataFrame,
    stability_df: pd.DataFrame,
    neighbor_df: pd.DataFrame,
    selected: pd.DataFrame,
) -> pd.DataFrame:
    """Combine per-job scores into per-window robustness_score."""
    # Merge per-job scores
    merged = selected[["job_id", "window_weeks"]].copy()
    merged = merged.merge(
        clustering_df[["job_id", "clustering_score"]], on="job_id", how="left"
    )
    merged = merged.merge(
        neighbor_df[["job_id", "neighbor_score", "plateau_score", "plateau_label",
                     "n_neighbors", "dropoff"]].rename(columns={"plateau_label": "_plabel"}),
        on="job_id", how="left",
    )

    # Per-window aggregation
    rows: list[dict] = []
    for window, grp in merged.groupby("window_weeks", sort=True):
        stab_row = stability_df[stability_df["window_weeks"] == window]
        stab_score = float(stab_row["stability_score"].iloc[0]) if len(stab_row) else float("nan")
        clust = float(grp["clustering_score"].mean(skipna=True))
        neigh = float(grp["neighbor_score"].mean(skipna=True))
        plat = float(grp["plateau_score"].mean(skipna=True))

        components = {
            "clustering_score": round(clust, 4),
            "stability_score": round(stab_score, 4),
            "neighbor_score": round(neigh, 4),
            "plateau_score": round(plat, 4),
        }

        # Use fallback 0.5 for NaN components
        c = {k: (v if not math.isnan(v) else 0.5) for k, v in components.items()}
        robustness = (
            c["clustering_score"] * _WEIGHTS["clustering"]
            + c["stability_score"] * _WEIGHTS["stability"]
            + c["neighbor_score"] * _WEIGHTS["neighbor"]
            + c["plateau_score"] * _WEIGHTS["plateau"]
        )

        # Dominant plateau label
        plabels = grp["_plabel"].dropna()
        dominant_label = plabels.mode().iloc[0] if len(plabels) > 0 else "unknown"

        rows.append({
            "window_weeks": window,
            "n_jobs": len(grp),
            **components,
            "robustness_score": round(robustness, 4),
            "dominant_plateau_label": dominant_label,
            "mean_dropoff": round(float(grp["dropoff"].mean(skipna=True)), 4),
            "mean_n_neighbors": round(float(grp["n_neighbors"].mean(skipna=True)), 1),
        })

    df = pd.DataFrame(rows).sort_values("robustness_score", ascending=False).reset_index(drop=True)
    df.insert(0, "robustness_rank", range(1, len(df) + 1))
    return df


# ---------------------------------------------------------------------------
# F. Recommendation engine
# ---------------------------------------------------------------------------

def recommend_selection_rule(
    robustness_ranking: pd.DataFrame,
    neighbor_df: pd.DataFrame,
    clustering_df: pd.DataFrame,
) -> tuple[str, str]:
    """Return (rule, confidence_note) based on robustness diagnostics."""
    if robustness_ranking.empty:
        return "insufficient_data", "No data available."

    top = robustness_ranking.iloc[0]
    rs = float(top["robustness_score"])
    ns = float(top["neighbor_score"])
    cs = float(top["clustering_score"])
    ss = float(top["stability_score"])
    label = str(top["dominant_plateau_label"])

    if rs >= 0.70 and label in ("stable_plateau",):
        rule = "top_result_is_safe"
        note = f"High robustness ({rs:.2f}). Top pass sits on a stable plateau."
    elif rs >= 0.55 and cs >= 0.65:
        rule = "top5_median_is_safer"
        note = (f"Moderate robustness ({rs:.2f}), tight clustering ({cs:.2f}). "
                "Use median of top-5 passes instead of top-1.")
    elif ss >= 0.65:
        rule = "cluster_centroid_recommended"
        note = (f"Parameters stable across jobs ({ss:.2f}) but local sensitivity detected. "
                "Use cluster centroid of top-10 passes.")
    else:
        rule = "parameter_sensitivity_too_high"
        note = (f"Low robustness ({rs:.2f}), high sensitivity. "
                "Optimizer result may not generalise.")

    return rule, note


# ---------------------------------------------------------------------------
# G. Output writers
# ---------------------------------------------------------------------------

def write_robustness_outputs(
    robustness_ranking: pd.DataFrame,
    analysis_detail: pd.DataFrame,
    research_dir: Path,
    summary_text: str,
) -> dict[str, Path]:
    research_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}

    def _write(df: pd.DataFrame, stem: str) -> None:
        pq = research_dir / f"{stem}.parquet"
        csv = research_dir / f"{stem}.csv"
        df.to_parquet(pq, index=False)
        df.to_csv(csv, index=False)
        written[stem] = pq

    _write(analysis_detail, "robustness_analysis")
    _write(robustness_ranking, "robustness_ranking")

    txt_path = research_dir / "robustness_summary.txt"
    txt_path.write_text(summary_text, encoding="utf-8")
    written["robustness_summary"] = txt_path

    return written


def render_summary(
    robustness_ranking: pd.DataFrame,
    rule: str,
    note: str,
    n_rows: int,
    param_cols: list[str],
) -> str:
    lines = [
        "Parameter Robustness Study — Phase 2B",
        "=" * 46,
        f"Rows analyzed    : {n_rows}",
        f"Windows compared : {len(robustness_ranking)}",
        f"Parameters       : {', '.join(c.replace('param_','') for c in param_cols)}",
        "",
        "Robustness Ranking:",
    ]
    for _, row in robustness_ranking.iterrows():
        lines.append(
            f"  #{int(row['robustness_rank'])}  {int(row['window_weeks']):>2}w  "
            f"score={row['robustness_score']:.3f}  "
            f"clust={row['clustering_score']:.3f}  "
            f"stab={row['stability_score']:.3f}  "
            f"neigh={row['neighbor_score']:.3f}  "
            f"plat={row['plateau_score']:.3f}  "
            f"[{row['dominant_plateau_label']}]"
        )
    lines += [
        "",
        f"Recommended rule : {rule}",
        f"Confidence note  : {note}",
    ]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# H. Orchestrator
# ---------------------------------------------------------------------------

def run_robustness_analysis(
    research_dir: str | Path,
    top_n: int = TOP_N,
    radius: float = NEIGHBOR_RADIUS,
) -> dict:
    """Full Phase 2B pipeline.  Returns summary dict."""
    rd = Path(research_dir)
    datasets = load_research_datasets(rd)

    all_passes = datasets["all_passes"]
    selected = datasets["selected_candidates"]

    param_cols = identify_parameter_columns(all_passes)
    if not param_cols:
        raise ValueError("No param_ columns found in all_passes.")

    print(f"Parameter columns: {param_cols}")
    print(f"Total passes: {len(all_passes)}  Selected candidates: {len(selected)}")

    # A. Clustering
    print("Computing top-N clustering...")
    clustering_df = compute_topn_clustering(all_passes, param_cols, top_n=top_n)

    # B. Stability
    print("Computing parameter stability...")
    stability_df = compute_parameter_stability(selected, param_cols)

    # C + D. Neighbor sensitivity + plateau classification
    print("Computing neighbor sensitivity...")
    neighbor_raw = compute_neighbor_sensitivity(
        all_passes, selected, param_cols, radius=radius
    )
    neighbor_df = classify_plateau_vs_spike(neighbor_raw)

    # E. Window-level scores
    print("Scoring windows...")
    robustness_ranking = score_window_robustness(
        clustering_df, stability_df, neighbor_df, selected
    )

    # F. Recommendation
    rule, note = recommend_selection_rule(robustness_ranking, neighbor_df, clustering_df)

    # Build detail table: per-job merged info
    detail = (
        selected[["job_id", "window_weeks"] + param_cols].copy()
        .merge(clustering_df[["job_id", "clustering_score", "top_n_used", "mean_pairwise_dist"]],
               on="job_id", how="left")
        .merge(neighbor_df[["job_id", "neighbor_score", "n_neighbors", "dropoff",
                             "neighbor_median_result", "plateau_label", "plateau_score"]],
               on="job_id", how="left")
    )
    detail["window_weeks"] = detail["window_weeks"].astype(int)

    summary_text = render_summary(robustness_ranking, rule, note,
                                  len(all_passes), param_cols)
    written = write_robustness_outputs(robustness_ranking, detail, rd, summary_text)

    return {
        "n_rows_analyzed": len(all_passes),
        "n_windows": len(robustness_ranking),
        "param_cols": param_cols,
        "rule": rule,
        "confidence_note": note,
        "top_robustness_window": int(robustness_ranking.iloc[0]["window_weeks"]),
        "robustness_ranking": robustness_ranking.to_dict("records"),
        "outputs": {k: str(v) for k, v in written.items()},
        "summary_text": summary_text,
    }
