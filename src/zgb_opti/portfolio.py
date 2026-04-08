"""Portfolio allocation engine for 5-strategy constrained risk allocation.

Constraint: S1 + S2 + S3 + S4 + S5 = ALLOC_TOTAL (default 10), integer values.
Zero allocation -> strategy disabled (enable flag = false).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

# Hedge/inverse strategies that require their parent to be active.
# Key = child strategy index, value = parent strategy index.
# S2 (Reverse Stops) is a hedge of S1; S5 (BB Mid Inverse) is a hedge of S4.
HEDGE_DEPENDENCIES: dict[int, int] = {2: 1, 5: 4}

STRATEGY_PARAMS = {
    1: {"enable": "InpEnable_S1", "risk": "InpRiskPercent_S1"},
    2: {"enable": "InpEnableReverseStops_S2", "risk": "InpReverseRiskPercent_S2"},
    3: {"enable": "InpEnableFractalStops_S3", "risk": "InpFractalRiskPercent_S3"},
    4: {"enable": "InpEnableBBMid_S4", "risk": "InpBBMidRiskPercent_S4"},
    5: {"enable": "InpEnableBBMidInv_S5", "risk": "InpBBMidInvRiskPercent_S5"},
}


def allocation_is_valid(alloc: tuple[int, ...]) -> bool:
    """Return True iff the allocation obeys hedge dependency rules.

    Rules (from HEDGE_DEPENDENCIES):
      - S2 requires S1: alloc[1] > 0 requires alloc[0] > 0
      - S5 requires S4: alloc[4] > 0 requires alloc[3] > 0
    """
    for child_idx, parent_idx in HEDGE_DEPENDENCIES.items():
        if alloc[child_idx - 1] > 0 and alloc[parent_idx - 1] == 0:
            return False
    return True


def allocation_to_params(alloc: tuple[int, ...]) -> dict[str, str]:
    """Map allocation tuple to param dict for MT5 ini.

    Returns {param_name: value_string} with both enable and risk params for all 5 strategies.
    For each strategy i: if alloc[i-1] == 0 -> enable=false; else enable=true.
    """
    params: dict[str, str] = {}
    for i in range(1, 6):
        risk_val = alloc[i - 1]
        enable_name = STRATEGY_PARAMS[i]["enable"]
        risk_name = STRATEGY_PARAMS[i]["risk"]
        params[enable_name] = "false" if risk_val == 0 else "true"
        params[risk_name] = str(risk_val)
    return params


def _risk_param_cols() -> list[str]:
    """Return list of param_ column names for risk percents."""
    return [f"param_{STRATEGY_PARAMS[i]['risk']}" for i in range(1, 6)]


def filter_portfolio_passes(df: pd.DataFrame) -> pd.DataFrame:
    """Filter passes DataFrame to rows with valid hedge dependency rules.

    Each strategy's risk is optimized independently — no sum constraint.
    """
    if df.empty:
        return df

    risk_cols = _risk_param_cols()
    missing = [c for c in risk_cols if c not in df.columns]
    if missing:
        print(f"  WARNING: risk param columns not found: {missing}")
        return pd.DataFrame()

    numeric_sub = df[risk_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    def _row_is_valid(row: pd.Series) -> bool:
        vals = [float(row[c]) for c in risk_cols]
        return allocation_is_valid(tuple(int(v) for v in vals))

    dep_mask = numeric_sub.apply(_row_is_valid, axis=1)
    return df[dep_mask].copy()


def extract_allocation(row: pd.Series) -> dict[str, int]:
    """Extract risk allocation per strategy from a candidate row.

    Returns {"S1": r1, "S2": r2, "S3": r3, "S4": r4, "S5": r5}.
    """
    result: dict[str, int] = {}
    for i in range(1, 6):
        col = f"param_{STRATEGY_PARAMS[i]['risk']}"
        val = row.get(col, 0)
        try:
            result[f"S{i}"] = int(float(val))
        except (TypeError, ValueError):
            result[f"S{i}"] = 0
    return result


def validate_allocation_sum(alloc: dict[str, int], total: int = 10) -> bool:
    """Return True iff sum of allocation values == total."""
    return sum(alloc.values()) == total


def format_allocation_display(alloc: dict[str, int]) -> str:
    """Return human-readable string like 'S1=4 S2=2 S3=2 S4=1 S5=1 (sum=10)'."""
    parts = [f"{k}={v}" for k, v in sorted(alloc.items())]
    total = sum(alloc.values())
    return " ".join(parts) + f" (sum={total})"


def write_preset_from_candidate(
    candidate_row: pd.Series,
    output_path: Path,
    base_set_path: Path,
) -> Path:
    """Write a deployment .set file from a candidate row and base .set template.

    Reads base_set_path, updates param values from candidate_row's param_ columns,
    sets enable flags based on allocation (ri=0 -> false, else true).
    Writes simple ParamName=value format (for single backtest, not optimization).
    Returns output_path.
    """
    # Read base set file (handle UTF-16 LE or UTF-8)
    raw = base_set_path.read_bytes()
    if raw[:2] == b"\xff\xfe":
        text = raw.decode("utf-16-le", errors="replace").lstrip("\ufeff")
    else:
        text = raw.decode("utf-8", errors="replace")

    # Build param override map from candidate row
    overrides: dict[str, str] = {}
    for col in candidate_row.index:
        if col.startswith("param_") and pd.notna(candidate_row[col]):
            name = col[len("param_"):]
            val = candidate_row[col]
            try:
                f = float(val)
                overrides[name] = str(int(f)) if f == int(f) else str(f)
            except (TypeError, ValueError):
                overrides[name] = str(val)

    # Apply allocation-based enable flags
    alloc = extract_allocation(candidate_row)
    for i in range(1, 6):
        enable_name = STRATEGY_PARAMS[i]["enable"]
        risk_val = alloc.get(f"S{i}", 0)
        overrides[enable_name] = "false" if risk_val == 0 else "true"

    # Parse and rewrite lines: emit simple ParamName=value format
    out_lines: list[str] = []
    for line in text.splitlines():
        stripped = line.strip().replace("\x00", "")
        if not stripped or stripped.startswith(";"):
            continue
        # Extract param name (before first =)
        if "=" in stripped:
            param_name = stripped.split("=", 1)[0]
            if param_name in overrides:
                out_lines.append(f"{param_name}={overrides[param_name]}")
            else:
                # Keep original value (strip MT5 optimization suffix fields)
                out_lines.append(f"{param_name}={stripped.split('=', 1)[1].split('||')[0]}")
        else:
            out_lines.append(stripped)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
    return output_path
