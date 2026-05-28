# MA7 Trail Retrace-Gate (V2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Extend the V1 MA7 trail (already implemented on `feature/ma7-trail-post-htp` at HEAD `b38ed78`) with a per-runner HWM-retrace gate that delays trail activation until the runner has given back at least `retrace_pct` of its peak unrealized profit. Sweep `retrace_pct ∈ {0.20, 0.30, 0.40}` plus the V1 control (`0.0`) plus the off-baseline across the same 4 windows, and pick a winner (or REJECT outright).

**Architecture:** Add `ma_trail_retrace_pct` to `ORBConfig`. Add two per-position state arrays: `pos_hwm_profit` (float64), `pos_ma7_armed` (bool). Per-tick: track HWM of unrealized P&L for HTP-fired runners; arm trail when current unrealized has retraced ≥ `retrace_pct × hwm`. Switch the V1 ratchet predicate from `pos_htp_fired` to `pos_ma7_armed`.

**Tech Stack:** Same as V1 — numpy / pandas / numba. Builds on V1's 10 commits.

**Spec:** [docs/superpowers/specs/2026-05-25-ma7-trail-retrace-gate-design.md](../specs/2026-05-25-ma7-trail-retrace-gate-design.md)

**Conventions:**
- Defaults must preserve existing behavior. `ma_trail_retrace_pct: float = 0.0` means "arm immediately on HTP fire" (== V1 logic). With `ma_trail=False`, the new state arrays do nothing.
- Stage only the file(s) each task modifies.
- Each task = one commit.

---

## File Structure

**Modified:**
- `src/zgb_sim/orb.py` — add `ma_trail_retrace_pct` field.
- `src/zgb_sim/orb_fast.py` — add HWM/armed state arrays, per-tick HWM tracking + arm-gate, switch ratchet predicate.

**Created:**
- `scripts/sim_orb_ma7_trail_retrace_sweep.py` — sweep driver (5 modes × 6 streams × 4 windows).
- `docs/reports/2026-05-25-ma7-trail-retrace-sweep.md` — sweep report (filled by driver run).
- (Memory file via host workflow.)

---

## Task V2.1: Add `ma_trail_retrace_pct` field to ORBConfig

**Files:**
- Modify: `src/zgb_sim/orb.py` — add field next to `ma_trail`.

- [ ] **Step 1: Add config field**

Open `src/zgb_sim/orb.py`. Find `ma_trail: bool = False`. Add immediately after:

```python
    # Retrace-from-HWM gate for the MA7 trail.
    # 0.0 = arm immediately on HTP fire (V1 behavior).
    # >0 = arm only after current unrealized PnL has retraced by at least this
    # fraction of HWM since HTP fired (e.g. 0.25 = wait for 25% giveback).
    ma_trail_retrace_pct: float = 0.0
```

- [ ] **Step 2: Verify**

```
$env:PYTHONPATH = "src"
python -c "from zgb_sim.orb import ORBConfig; c = ORBConfig(); print('retrace=', c.ma_trail_retrace_pct)"
```
Expected: `retrace= 0.0`.

- [ ] **Step 3: Commit**

```
git add src/zgb_sim/orb.py
git commit -m "$(cat <<'EOF'
feat(sim): add ma_trail_retrace_pct gate field to ORBConfig

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task V2.2: Add per-runner HWM + armed state arrays

**Files:**
- Modify: `src/zgb_sim/orb_fast.py` — beside the existing `pos_is_runner` / `pos_htp_fired` / `pos_ma7_last_idx` arrays.

- [ ] **Step 1: Add state arrays**

Grep for `pos_ma7_last_idx = np.zeros(MAX_POSITIONS, dtype=np.int64)` to find the existing block. After that line, add:

```python
    # Retrace-gate state for MA7 trail (V2)
    pos_hwm_profit = np.zeros(MAX_POSITIONS, dtype=np.float64)
    pos_ma7_armed = np.zeros(MAX_POSITIONS, dtype=np.bool_)
```

- [ ] **Step 2: Extend `_run_sim` signature**

Find the V1 signature line `ma_trail,                       # bool: enable SMA(7) trail on runner post-HTP` and add a new parameter after the existing `m5_close_ts, m5_sma7,` line:

```python
    ma_trail,                       # bool: enable SMA(7) trail on runner post-HTP
    m5_close_ts, m5_sma7,           # int64[M], float64[M]: close ts + SMA7 per M5 bar
    ma_trail_retrace_pct,           # float64: HWM retrace fraction to arm trail (0.0 = V1)
):
```

- [ ] **Step 3: Update call site**

Find `bool(cfg.ma_trail),\n        m5_close_ts, m5_sma7,\n    )` in `simulate_fast`. Change to:

```python
        bool(cfg.ma_trail),
        m5_close_ts, m5_sma7,
        float(cfg.ma_trail_retrace_pct),
    )
```

- [ ] **Step 4: Reset state at position creation**

In the position-add block (V1 Task 5 added `pos_is_runner[slot] = False`, `pos_htp_fired[slot] = False`, `pos_ma7_last_idx[slot] = 0`). Add after `pos_ma7_last_idx[slot] = 0`:

```python
                    pos_hwm_profit[slot] = 0.0
                    pos_ma7_armed[slot] = False
```

- [ ] **Step 5: Smoke test**

```
$env:PYTHONPATH = "src"
python -c "import zgb_sim.orb_fast; print('ok')"
```
Expected: `ok`.

- [ ] **Step 6: Commit**

```
git add src/zgb_sim/orb_fast.py
git commit -m "$(cat <<'EOF'
chore(sim): add HWM+armed state arrays and retrace-pct sim wiring

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task V2.3: Per-tick HWM update + arm-gate check

**Files:**
- Modify: `src/zgb_sim/orb_fast.py` — insert HWM/arm block BEFORE the V1 MA7 ratchet block.

The MA7 ratchet block lives just after the V1 fractal-trail block (added in Task 7). We insert HWM-tracking + arm-check IMMEDIATELY BEFORE the MA7 ratchet block. Both blocks iterate active positions, but they have different gating predicates (HWM uses htp_fired; ratchet uses ma7_armed).

- [ ] **Step 1: Add HWM update + arm-gate block**

Grep for `# MA7 post-HTP trail: ratchet runner SL toward SMA7 on closed M5 bars.` (the V1 ratchet block header). IMMEDIATELY BEFORE that line, add:

```python
        # MA7 retrace-gate (V2): track HWM of unrealized profit per HTP-fired runner;
        # arm the trail once current unrealized has retraced >= retrace_pct of HWM.
        if ma_trail:
            for i in range(MAX_POSITIONS):
                if not pos_active[i] or not pos_is_runner[i] or not pos_htp_fired[i]:
                    continue
                close_px_i = bid if pos_dir[i] == 1 else ask
                upnl = _pnl(pos_dir[i], pos_entry[i], close_px_i,
                            pos_lots[i], tick_value, tick_size)
                if upnl > pos_hwm_profit[i]:
                    pos_hwm_profit[i] = upnl
                if not pos_ma7_armed[i]:
                    if ma_trail_retrace_pct <= 0.0:
                        # V1 behavior: arm immediately.
                        pos_ma7_armed[i] = True
                    else:
                        # Need positive HWM to compute meaningful retrace.
                        if pos_hwm_profit[i] > 0.0:
                            arm_threshold = pos_hwm_profit[i] * (1.0 - ma_trail_retrace_pct)
                            if upnl <= arm_threshold:
                                pos_ma7_armed[i] = True
```

- [ ] **Step 2: Switch the MA7 ratchet predicate from `pos_htp_fired` to `pos_ma7_armed`**

In the V1 MA7 ratchet block, change the inner skip condition from:

```python
                if not pos_active[i] or not pos_is_runner[i] or not pos_htp_fired[i]:
                    continue
```

to:

```python
                if not pos_active[i] or not pos_is_runner[i] or not pos_ma7_armed[i]:
                    continue
```

- [ ] **Step 3: Smoke test**

```
$env:PYTHONPATH = "src"
python -c "import zgb_sim.orb_fast; print('ok')"
```
Expected: `ok`.

- [ ] **Step 4: Verify V1 baseline still passes**

```
$env:PYTHONPATH = "src"
pytest tests/zgb_sim/test_orb_fast_ma_trail.py -v -m integration
```
Expected: PASS. (`ma_trail=False` ignores the new code; result unchanged.)

If FAIL — the new code has a side effect when `ma_trail=False`; debug before proceeding.

- [ ] **Step 5: Commit**

```
git add src/zgb_sim/orb_fast.py
git commit -m "$(cat <<'EOF'
feat(sim): HWM-retrace gate for MA7 trail; ratchet predicate now pos_ma7_armed

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task V2.4: Sweep driver — 4 windows × 6 streams × 5 modes

**Files:**
- Create: `scripts/sim_orb_ma7_trail_retrace_sweep.py`
- Create: `docs/reports/2026-05-25-ma7-trail-retrace-sweep.md` (driver writes this)

The driver fetches each window's ticks/M5 ONCE and reuses across all 6 streams × 5 modes. Modes:

| Label | ma_trail | retrace_pct |
|---|---|---|
| off | False | (n/a) |
| r0.0 | True | 0.0 |
| r0.2 | True | 0.20 |
| r0.3 | True | 0.30 |
| r0.4 | True | 0.40 |

**Critical API reminders (confirmed in V1 Task 9):**
- `simulate_fast(ticks, m5_bars, m1_bars, cfg, meta, initial_balance)` — that exact arg order.
- `SimResult.max_drawdown` not `dd_abs`. `SimResult.net_profit`, `SimResult.profit_factor`, `SimResult.trades`.
- `fetch_window(None, start, end, spread_pts=30, account="sim")` from `scripts/sim_orb_oos_today.py` returns `(sym, ticks, m1, m5)`.
- `parse_v6_setfile(path)` from `scripts/sim_orb_oos_today_hedge_v6.py` returns dict with `["streams"]` list.

- [ ] **Step 1: Create driver**

Create `scripts/sim_orb_ma7_trail_retrace_sweep.py`:

```python
"""V2 sweep driver: retrace-gated MA7 trail across 4 windows × 6 streams × 5 modes.

Run:  $env:PYTHONPATH = "src"; python scripts/sim_orb_ma7_trail_retrace_sweep.py

Outputs:
  docs/reports/2026-05-25-ma7-trail-retrace-sweep.md  — portfolio + detail + verdict

Per spec docs/superpowers/specs/2026-05-25-ma7-trail-retrace-gate-design.md.
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
REPORT  = ROOT / "docs" / "reports" / "2026-05-25-ma7-trail-retrace-sweep.md"
DEPOSIT = 10_000.0
SPREAD  = 30
RISK_PER_STREAM = 1.5

WINDOWS = [
    ("W1", datetime(2026, 4, 25, tzinfo=timezone.utc), datetime(2026, 5, 2,  tzinfo=timezone.utc)),
    ("W2", datetime(2026, 5, 2,  tzinfo=timezone.utc), datetime(2026, 5, 9,  tzinfo=timezone.utc)),
    ("W3", datetime(2026, 5, 9,  tzinfo=timezone.utc), datetime(2026, 5, 16, tzinfo=timezone.utc)),
    ("W4", datetime(2026, 5, 16, tzinfo=timezone.utc), datetime(2026, 5, 23, tzinfo=timezone.utc)),
]

MODES = [
    ("off",  False, 0.0),
    ("r0.0", True,  0.0),
    ("r0.2", True,  0.20),
    ("r0.3", True,  0.30),
    ("r0.4", True,  0.40),
]


def _meta() -> SymbolMeta:
    return SymbolMeta(point=0.01, tick_size=0.01, tick_value=1.0, stops_level_pts=0,
                      volume_min=0.01, volume_max=100.0, volume_step=0.01, digits=2)


def _build_cfg(stream: dict, ma_trail: bool, retrace_pct: float) -> ORBConfig:
    return ORBConfig(
        risk_pct=RISK_PER_STREAM,
        range_minutes=stream["range_minutes"],
        fixed_sl_pts=stream["fixed_sl_pts"],
        rr_ratio=stream["rr_ratio"],
        half_tp_ratio=stream["half_tp_ratio"],
        pending_expire_minutes=stream["pending_expire_minutes"],
        fractal_confirm=True, fractal_width=5,
        ma_trail=ma_trail,
        ma_trail_retrace_pct=retrace_pct,
    )


def _haircut(net: float, dd: float) -> tuple[float, float]:
    """6% NP haircut + NP/DD$_hc ratio per feedback_sim_vs_live_calibration."""
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
            for mode_name, ma, rp in MODES:
                r = simulate_fast(ticks, m5, m1, _build_cfg(s, ma, rp), _meta(), DEPOSIT)
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

    # Compute deltas vs off baseline per window.
    off_nd = portfolio[portfolio["mode"] == "off"].set_index("window")["nd_hc"]
    portfolio["delta_nd_hc"] = portfolio.apply(
        lambda r: r["nd_hc"] - off_nd.get(r["window"], 0.0), axis=1)
    portfolio["delta_pct"]   = portfolio.apply(
        lambda r: ((r["nd_hc"] - off_nd.get(r["window"], 0.0)) /
                   off_nd.get(r["window"], np.nan) * 100)
                  if off_nd.get(r["window"], 0.0) != 0 else np.nan,
        axis=1)

    # Decision: per non-off mode, count wins (Δ% >= +10), regressions (Δ% <= -15), mean Δ%.
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
        f.write("# MA7 Trail Retrace-Gate Sweep (2026-05-25)\n\n")
        f.write(f"Setfile: `{SETFILE.name}`  ·  spread {SPREAD}pt  ·  deposit ${DEPOSIT:,.0f}  ·  risk {RISK_PER_STREAM}%/stream\n\n")
        f.write("## Portfolio per (window, mode)\n\n")
        f.write(portfolio.to_markdown(index=False))
        f.write("\n\n## Per-mode decision summary (vs `off` baseline)\n\n")
        f.write(summary_df.to_markdown(index=False))

        # Verdict
        candidates = [r for r in summary if r["regressions_le-15"] == 0 and r["wins_ge10"] >= 3]
        f.write("\n\n## Verdict\n\n")
        if candidates:
            winner = max(candidates, key=lambda r: r["mean_delta_pct"])
            f.write(f"- **ADVANCE** with `retrace_pct={winner['mode']}` "
                    f"(mean Δ% = {winner['mean_delta_pct']:+.1f}, wins {winner['wins_ge10']}/4, "
                    f"regressions {winner['regressions_le-15']}/4).\n")
        else:
            f.write("- **REJECT V2** — no `retrace_pct` value clears the threshold "
                    "(needs ≥3 wins at +10% AND 0 regressions ≤ −15%).\n")
            for r in summary:
                f.write(f"  - {r['mode']}: mean Δ% = {r['mean_delta_pct']:+.1f}, "
                        f"wins {r['wins_ge10']}/4, regressions {r['regressions_le-15']}/4\n")
        f.write("\n\n## Per-stream detail\n\n")
        f.write(df.to_markdown(index=False))

    print(f"wrote {REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Dry-run on W4 only first**

Edit `WINDOWS` to keep only `("W4", ...)`. Run:
```
$env:PYTHONPATH = "src"
python scripts/sim_orb_ma7_trail_retrace_sweep.py
```
Expected: writes report. Inspect — `r0.0` portfolio row should approximate V1's W4 on-mode numbers (NP/DD$_hc ≈ 1.85). If not, the retrace gate isn't reproducing V1 when `retrace_pct=0.0`. Investigate before continuing.

- [ ] **Step 3: Restore all 4 windows, run full sweep**

Restore `WINDOWS` list, run again. ETA ~10–15 minutes.

- [ ] **Step 4: Commit driver + report**

```
git add scripts/sim_orb_ma7_trail_retrace_sweep.py docs/reports/2026-05-25-ma7-trail-retrace-sweep.md
git commit -m "$(cat <<'EOF'
feat(sim): retrace-gated MA7 trail sweep (5 modes x 6 streams x 4 windows)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task V2.5: Memory update with verdict

**Files:**
- Modify: `C:\Users\Zhu-En\.claude\projects\c--Users-Zhu-En-zgb-opti\memory\project_ma7_trail_test_2026_05_25.md`
- (Existing memory; we append a "V2 follow-up" section. Don't create a new memory file.)

- [ ] **Step 1: Read the report's Verdict section**

Open `docs/reports/2026-05-25-ma7-trail-retrace-sweep.md`. Read the verdict.

- [ ] **Step 2: Append V2 outcome to memory**

Edit the existing memory file: append a "## V2 follow-up (retrace gate)" section with:
- The winning `retrace_pct` (or "REJECTED" if no winner).
- Per-mode summary table (mode, wins, regressions, mean Δ%).
- A one-line "what to do next" (advance to WFO with this dim, or close the line of investigation).

Also update the `description:` field at top to reflect V2 outcome.

- [ ] **Step 3: Commit (no git for memory — that's outside the repo)**

Memory is in a separate dir; no git action.

---

## Self-Review Checklist

After V2.4 driver run:

- [ ] `r0.0` mode reproduces V1's `on` numbers (sanity check — retrace_pct=0 == V1).
- [ ] No baseline drift: V1's safety test `pytest tests/zgb_sim/test_orb_fast_ma_trail.py -m integration` still passes.
- [ ] Per-window deltas computed against the SAME-WINDOW off baseline (not cross-window).
- [ ] Decision verdict line present in report.
