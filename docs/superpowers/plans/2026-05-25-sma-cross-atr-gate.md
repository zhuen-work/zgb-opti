# V4: SMA Cross-Exit + ATR Regime Gate — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Extend V3 (`sma_cross_exit`) with a session-level ATR/range_pts regime gate. Sweep gate ∈ {0.0, 0.10, 0.15, 0.20, 0.25, 0.30} across 4 weekly windows × 6 streams.

**Architecture:** New config field `sma_cross_atr_gate`. `simulate_fast` precomputes `sess_cross_enabled` bool array (one entry per session). JIT loop adds `and sess_cross_enabled[pos_session[i]]` predicate to the V3 cross-exit block.

**Branch:** `feature/ma7-trail-post-htp` (continues from V3 at `72829a0`).

**Spec:** [docs/superpowers/specs/2026-05-25-sma-cross-atr-gate-design.md](../specs/2026-05-25-sma-cross-atr-gate-design.md)

---

## File Structure

**Modified:**
- `src/zgb_sim/orb.py` — add `sma_cross_atr_gate` field.
- `src/zgb_sim/orb_fast.py` — per-session ATR precompute + JIT predicate.

**Created:**
- `scripts/sim_orb_sma_cross_atr_gate_sweep.py` — 7-mode sweep driver.
- `docs/reports/2026-05-25-sma-cross-atr-gate-sweep.md` — driver writes.

---

## Task V4.1: Add `sma_cross_atr_gate` field to ORBConfig

**Files:**
- Modify: `src/zgb_sim/orb.py`.

- [ ] **Step 1**: Find `sma_cross_exit: bool = False`. Add after:

```python
    # ATR-based session regime gate for sma_cross_exit (V4).
    # 0.0 = all sessions enabled (V3 behavior).
    # >0 = enable cross-exit only for sessions where M5-ATR(14)/range_pts > gate.
    # Higher value = restrict cross-exit to choppier sessions; clean trends bypass it.
    sma_cross_atr_gate: float = 0.0
```

- [ ] **Step 2**: Verify:
```
$env:PYTHONPATH = "src"
python -c "from zgb_sim.orb import ORBConfig; c = ORBConfig(); print('atr_gate=', c.sma_cross_atr_gate)"
```
Expected: `atr_gate= 0.0`.

- [ ] **Step 3**: Commit:
```
git add src/zgb_sim/orb.py
git commit -m "$(cat <<'EOF'
feat(sim): add sma_cross_atr_gate field to ORBConfig (V4 regime gate)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task V4.2: Precompute `sess_cross_enabled` array in `simulate_fast`

**Approach:** at simulate_fast level, after sessions are built and m5_close_ts is known, compute per-session ATR(14) using the 14 M5 close-to-close diffs ending at session_range_end. Compare ratio to gate; produce `sess_cross_enabled: bool[n_sessions]` array. Pass into `_run_sim`.

**Files:**
- Modify: `src/zgb_sim/orb_fast.py`.

- [ ] **Step 1**: Add a Python helper (NOT JIT) at module top, after imports:

```python
def _build_sess_cross_enabled(
    sess_range_end_ns: np.ndarray,
    sess_range_high: np.ndarray,
    sess_range_low: np.ndarray,
    m5_ts_ns: np.ndarray,
    m5_closes: np.ndarray,
    point: float,
    atr_gate: float,
) -> np.ndarray:
    """For each session, compute M5-ATR(14)/range_pts. Returns bool[n_sessions]
    where True = atr_ratio > atr_gate. When atr_gate <= 0, all True."""
    n = len(sess_range_end_ns)
    out = np.zeros(n, dtype=np.bool_)
    if atr_gate <= 0.0:
        out[:] = True
        return out
    # Need close-to-close diffs.
    diffs = np.abs(np.diff(m5_closes))  # len = len(m5_closes) - 1
    for si in range(n):
        rh = sess_range_high[si]
        rl = sess_range_low[si]
        if rh <= 0 or rl <= 0:
            out[si] = False
            continue
        range_pts = (rh - rl) / point
        if range_pts <= 0:
            out[si] = False
            continue
        # Find M5 bars whose close_ts <= range_end. Close of bar i is m5_ts_ns[i] + 5min.
        # Use searchsorted on m5_ts_ns (open times); want last bar whose open + 5min <= range_end.
        end_ts = sess_range_end_ns[si]
        # bar i's close_ts = m5_ts_ns[i] + 5min. We want max i such that m5_ts_ns[i] + 5min <= end_ts
        # => m5_ts_ns[i] <= end_ts - 5min.
        FIVE_MIN_NS = np.int64(5 * 60 * 1_000_000_000)
        cutoff = end_ts - FIVE_MIN_NS
        idx_end = int(np.searchsorted(m5_ts_ns, cutoff, side='right'))  # exclusive
        if idx_end < 15:
            # Not enough bars for ATR(14) → conservatively disable.
            out[si] = False
            continue
        # ATR(14) = mean(diffs[idx_end-14:idx_end]).
        # diffs[i] = |close[i+1] - close[i]|, so diffs[idx_end-14:idx_end] uses
        # closes[idx_end-14..idx_end].
        atr_in_price = float(np.mean(diffs[idx_end - 14:idx_end]))
        atr_pts = atr_in_price / point
        ratio = atr_pts / range_pts
        out[si] = ratio > atr_gate
    return out
```

- [ ] **Step 2**: In `simulate_fast`, find the existing precompute block that handles `sma_cross_exit`. AFTER the cross-exit precompute, add:

```python
    # V4 ATR gate: per-session boolean array.
    if bool(cfg.sma_cross_exit):
        m5_closes = m5_bars["close"].values.astype(np.float64)
        sess_cross_enabled = _build_sess_cross_enabled(
            re_ns, rh_arr, rl_arr,
            m5_ts_ns, m5_closes,
            float(meta.point),
            float(cfg.sma_cross_atr_gate),
        )
    else:
        sess_cross_enabled = np.empty(0, dtype=np.bool_)
```

- [ ] **Step 3**: Extend `_run_sim` signature. Find `m5_cross_signal,                # int8[M]: +1 bullish / -1 bearish / 0 none per M5 bar`. Add after:

```python
    m5_cross_signal,                # int8[M]: +1 bullish / -1 bearish / 0 none per M5 bar
    sess_cross_enabled,             # bool[n_sessions]: V4 ATR-gate result per session
):
```

- [ ] **Step 4**: Update the `_run_sim` call site in `simulate_fast`. Find the trailer ending `m5_cross_signal,\n    )`. Change to:

```python
        bool(cfg.sma_cross_exit),
        m5_cross_signal,
        sess_cross_enabled,
    )
```

- [ ] **Step 5**: Smoke test:
```
$env:PYTHONPATH = "src"
python -c "import zgb_sim.orb_fast; print('ok')"
```
Expected: `ok`.

- [ ] **Step 6**: V1 safety:
```
pytest tests/zgb_sim/test_orb_fast_ma_trail.py -v -m integration
```
Expected: PASS.

- [ ] **Step 7**: Commit:
```
git add src/zgb_sim/orb_fast.py
git commit -m "$(cat <<'EOF'
chore(sim): precompute per-session ATR-gate array and wire into _run_sim

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task V4.3: Add `sess_cross_enabled` predicate to V3 cross-exit block

**Files:**
- Modify: `src/zgb_sim/orb_fast.py`.

- [ ] **Step 1**: Grep for `# V3 cross-exit: close post-HTP runners` (the V3 block header). Inside that block, find the line:
```python
                if not pos_active[i] or not pos_is_runner[i] or not pos_htp_fired[i]:
                    continue
```

Replace with:
```python
                if not pos_active[i] or not pos_is_runner[i] or not pos_htp_fired[i]:
                    continue
                # V4 regime gate: skip cross-exit for sessions disabled by ATR gate.
                if len(sess_cross_enabled) > 0:
                    sess_i = pos_session[i]
                    if sess_i < 0 or sess_i >= len(sess_cross_enabled):
                        continue
                    if not sess_cross_enabled[sess_i]:
                        continue
```

(Defensive bounds check on sess_i — sessions can in principle be -1 if assigned before fire.)

- [ ] **Step 2**: Smoke test:
```
$env:PYTHONPATH = "src"
python -c "import zgb_sim.orb_fast; print('ok')"
```
Expected: `ok`.

- [ ] **Step 3**: V3 sanity — run V3 driver and confirm `gate=0.0` reproduces V3 numbers.

This step verifies that `sma_cross_atr_gate=0.0` (default for V3) behavior is unchanged.

```
$env:PYTHONPATH = "src"
python scripts/sim_orb_sma_cross_exit_ab.py
```
Compare `on` portfolio NP/DD$_hc per window against `docs/reports/2026-05-25-sma-cross-exit-ab.md` — they must match (within rounding).

- [ ] **Step 4**: Commit:
```
git add src/zgb_sim/orb_fast.py
git commit -m "$(cat <<'EOF'
feat(sim): apply session-level ATR gate predicate to cross-exit (V4)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task V4.4: Sweep driver

**Files:**
- Create: `scripts/sim_orb_sma_cross_atr_gate_sweep.py`
- Create: `docs/reports/2026-05-25-sma-cross-atr-gate-sweep.md`

- [ ] **Step 1**: Create driver. Copy `scripts/sim_orb_sma_cross_exit_ab.py` as a template; modify the MODES list and add the gate value to `_build_cfg`:

```python
"""V4 sweep driver: SMA cross-exit + ATR session gate across 4 windows.

Run:  $env:PYTHONPATH = "src"; python scripts/sim_orb_sma_cross_atr_gate_sweep.py

Outputs:
  docs/reports/2026-05-25-sma-cross-atr-gate-sweep.md
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast
from zgb_sim.scalper_v1 import SymbolMeta
from sim_orb_oos_today import fetch_window
from sim_orb_oos_today_hedge_v6 import parse_v6_setfile


SETFILE = ROOT / "configs" / "sets" / "dt818_pro_v6_9pct_may23_may16.set"
REPORT  = ROOT / "docs" / "reports" / "2026-05-25-sma-cross-atr-gate-sweep.md"
DEPOSIT = 10_000.0
SPREAD  = 30
RISK_PER_STREAM = 1.5

WINDOWS = [
    ("W1", datetime(2026, 4, 25, tzinfo=timezone.utc), datetime(2026, 5, 2,  tzinfo=timezone.utc)),
    ("W2", datetime(2026, 5, 2,  tzinfo=timezone.utc), datetime(2026, 5, 9,  tzinfo=timezone.utc)),
    ("W3", datetime(2026, 5, 9,  tzinfo=timezone.utc), datetime(2026, 5, 16, tzinfo=timezone.utc)),
    ("W4", datetime(2026, 5, 16, tzinfo=timezone.utc), datetime(2026, 5, 23, tzinfo=timezone.utc)),
]

# Modes: (label, sma_cross_exit, atr_gate)
MODES = [
    ("off",   False, 0.0),
    ("g0.00", True,  0.0),
    ("g0.10", True,  0.10),
    ("g0.15", True,  0.15),
    ("g0.20", True,  0.20),
    ("g0.25", True,  0.25),
    ("g0.30", True,  0.30),
]


def _meta() -> SymbolMeta:
    return SymbolMeta(point=0.01, tick_size=0.01, tick_value=1.0, stops_level_pts=0,
                      volume_min=0.01, volume_max=100.0, volume_step=0.01, digits=2)


def _build_cfg(stream: dict, cross_on: bool, gate: float) -> ORBConfig:
    return ORBConfig(
        risk_pct=RISK_PER_STREAM,
        range_minutes=stream["range_minutes"],
        fixed_sl_pts=stream["fixed_sl_pts"],
        rr_ratio=stream["rr_ratio"],
        half_tp_ratio=stream["half_tp_ratio"],
        pending_expire_minutes=stream["pending_expire_minutes"],
        fractal_confirm=True, fractal_width=5,
        sma_cross_exit=cross_on,
        sma_cross_atr_gate=gate,
    )


def _haircut(net: float, dd: float) -> tuple[float, float]:
    net_hc = net * 0.94
    nd = (net_hc / dd) if dd > 0 else 0.0
    return net_hc, nd


def main() -> int:
    cfg_data = parse_v6_setfile(SETFILE)
    streams = cfg_data["streams"]

    rows = []
    for wname, wstart, wend in WINDOWS:
        sym, ticks, m1, m5 = fetch_window(None, wstart, wend, spread_pts=SPREAD, account="sim")
        days = (wend - wstart).days
        for sidx, s in enumerate(streams, start=1):
            for mode_name, on, gate in MODES:
                r = simulate_fast(ticks, m5, m1, _build_cfg(s, on, gate), _meta(), DEPOSIT)
                net_hc, nd_hc = _haircut(r.net_profit, r.max_drawdown)
                rows.append({
                    "window": wname, "days": days, "stream": f"S{sidx}",
                    "mode": mode_name,
                    "net": r.net_profit, "dd": r.max_drawdown,
                    "pf": r.profit_factor, "trades": r.trades,
                    "net_hc": net_hc, "nd_hc": nd_hc,
                })

    df = pd.DataFrame(rows)

    portfolio = df.groupby(["window", "days", "mode"], as_index=False).agg(
        net=("net", "sum"), dd=("dd", "sum"),
        trades=("trades", "sum"), net_hc=("net_hc", "sum"),
    )
    portfolio["nd_hc"] = portfolio.apply(
        lambda r: (r["net_hc"] / r["dd"]) if r["dd"] > 0 else 0.0, axis=1)

    off_nd = portfolio[portfolio["mode"] == "off"].set_index("window")["nd_hc"]
    portfolio["delta_nd_hc"] = portfolio.apply(
        lambda r: r["nd_hc"] - off_nd.get(r["window"], 0.0), axis=1)
    portfolio["delta_pct"] = portfolio.apply(
        lambda r: ((r["nd_hc"] - off_nd.get(r["window"], 0.0)) /
                   off_nd.get(r["window"], np.nan) * 100)
                  if off_nd.get(r["window"], 0.0) != 0 else np.nan,
        axis=1)

    summary = []
    for mode_name, _, _ in MODES:
        if mode_name == "off":
            continue
        sub = portfolio[portfolio["mode"] == mode_name]
        wins = int((sub["delta_pct"] >= 10).sum())
        regressions = int((sub["delta_pct"] <= -15).sum())
        mean_delta = float(sub["delta_pct"].mean())
        summary.append({"mode": mode_name, "wins_ge10": wins,
                        "regressions_le-15": regressions, "mean_delta_pct": mean_delta})
    summary_df = pd.DataFrame(summary)

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    with REPORT.open("w", encoding="utf-8") as f:
        f.write("# V4: SMA Cross-Exit + ATR Gate Sweep (2026-05-25)\n\n")
        f.write(f"Setfile: `{SETFILE.name}`  ·  spread {SPREAD}pt  ·  deposit ${DEPOSIT:,.0f}  ·  risk {RISK_PER_STREAM}%/stream\n\n")
        f.write("## Portfolio per (window, mode)\n\n")
        f.write(portfolio.to_markdown(index=False))
        f.write("\n\n## Per-mode summary (vs off baseline)\n\n")
        f.write(summary_df.to_markdown(index=False))

        candidates = [r for r in summary if r["regressions_le-15"] == 0 and r["wins_ge10"] >= 3]
        f.write("\n\n## Verdict\n\n")
        if candidates:
            winner = max(candidates, key=lambda r: r["mean_delta_pct"])
            f.write(f"- **ADVANCE** with `sma_cross_atr_gate={winner['mode']}` "
                    f"(mean Δ%={winner['mean_delta_pct']:+.1f}, wins {winner['wins_ge10']}/4, "
                    f"regressions {winner['regressions_le-15']}/4).\n")
        else:
            f.write("- **REJECT V4** -- no gate value clears (>=3 wins at +10% AND 0 regressions <= -15%).\n")
            for r in summary:
                f.write(f"  - {r['mode']}: mean Δ%={r['mean_delta_pct']:+.1f}, "
                        f"wins {r['wins_ge10']}/4, regressions {r['regressions_le-15']}/4\n")
        f.write("\n\n## Per-stream detail\n\n")
        f.write(df.to_markdown(index=False))

    print(f"wrote {REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2**: Dry-run W4 only.

Edit `WINDOWS` to one entry, run:
```
$env:PYTHONPATH = "src"
python scripts/sim_orb_sma_cross_atr_gate_sweep.py
```
Expected: report written. Inspect — `g0.00` portfolio NP/DD$_hc must match V3's W4 on (`2.3424` from V3 report). Other gate values should show varying numbers as gate restricts which sessions enable cross-exit.

- [ ] **Step 3**: Restore 4 windows + full run. ~15 min.

- [ ] **Step 4**: Commit:
```
git add scripts/sim_orb_sma_cross_atr_gate_sweep.py docs/reports/2026-05-25-sma-cross-atr-gate-sweep.md
git commit -m "$(cat <<'EOF'
feat(sim): V4 sweep — SMA cross-exit + ATR session gate (6 gate values)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task V4.5: Memory update

- [ ] **Step 1**: Read the report verdict.
- [ ] **Step 2**: Append `## V4 (cross-exit + ATR session gate)` section to `project_ma7_trail_test_2026_05_25.md`. Include:
   - Per-mode summary table (gate, wins, regressions, mean Δ%).
   - Winning gate value (or REJECT).
   - One-line read on whether the regime gate finally cracks the W1 problem.
- [ ] **Step 3**: Update `description:` field and the top-line verdict.
- [ ] No git for memory.

---

## Self-Review

- [ ] `g0.00` reproduces V3's on numbers in all 4 windows (sanity).
- [ ] V1 baseline-drift safety test passes.
- [ ] At least one window's verdict line is present in report.
- [ ] All commits stage only modified files (no `git add -A`).
