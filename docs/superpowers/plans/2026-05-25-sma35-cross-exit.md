# SMA(3)×SMA(5) Cross-Exit (V3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add an event-based exit that closes post-HTP runners when an opposing SMA(3)×SMA(5) cross fires on M5 closes AND the runner is in profit. Run A/B against v6 setfile across the same 4 weekly windows used in V1/V2.

**Architecture:** New flag `sma_cross_exit: bool = False` on `ORBConfig`. `simulate_fast` precomputes three M5-aligned arrays (sma3, sma5, cross_signal). JIT loop walks the cross_signal array per runner (same pattern as V1's MA7 ratchet) and triggers a discrete close at current bid/ask when conditions match. Mutually exclusive with `ma_trail` — guard rail in the wrapper.

**Tech Stack:** Same as V1/V2.

**Spec:** [docs/superpowers/specs/2026-05-25-sma35-cross-exit-design.md](../specs/2026-05-25-sma35-cross-exit-design.md)

**Branch:** `feature/ma7-trail-post-htp` (continues from V2 at `32e90f7`).

**Conventions:**
- Default OFF — `sma_cross_exit=False` must produce byte-identical output to baseline (verified by V1's existing safety test continuing to pass).
- Stage only the files each task modifies.
- One task per commit.

---

## File Structure

**Modified:**
- `src/zgb_sim/orb.py` — add `sma_cross_exit` field.
- `src/zgb_sim/orb_fast.py` — sma3/sma5/cross_signal precompute + JIT exit logic.

**Created:**
- `src/zgb_sim/sma_cross.py` — pure-Python helper computing `(m5_close_ts, m5_cross_signal)` arrays. TDD.
- `tests/zgb_sim/test_sma_cross.py` — unit tests for the helper.
- `scripts/sim_orb_sma_cross_exit_ab.py` — A/B driver (off vs on, 4 windows × 6 streams).
- `docs/reports/2026-05-25-sma-cross-exit-ab.md` — driver writes report.

---

## Task V3.1: Add `sma_cross_exit` field to ORBConfig

**Files:**
- Modify: `src/zgb_sim/orb.py`.

- [ ] **Step 1: Add field**

Find `ma_trail_retrace_pct: float = 0.0`. Add immediately after:

```python
    # SMA(3) x SMA(5) cross exit on M5 closes (V3 follow-up to ma_trail).
    # When True, post-HTP runners close on opposite-direction cross while in profit.
    # Mutually exclusive with ma_trail (simulate_fast raises if both True).
    sma_cross_exit: bool = False
```

- [ ] **Step 2: Verify**

```
$env:PYTHONPATH = "src"
python -c "from zgb_sim.orb import ORBConfig; c = ORBConfig(); print('sma_cross_exit=', c.sma_cross_exit)"
```
Expected: `sma_cross_exit= False`.

- [ ] **Step 3: Commit**

```
git add src/zgb_sim/orb.py
git commit -m "$(cat <<'EOF'
feat(sim): add sma_cross_exit flag to ORBConfig (default off)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task V3.2: SMA cross-signal precompute helper (TDD)

A pure-Python helper that takes M5 bars and returns `(m5_close_ts, m5_cross_signal)` where:
- `m5_close_ts: int64[N]` — same as V1's `sma7_on_m5_closes` (open_ns + 5min).
- `m5_cross_signal: int8[N]` — +1 bullish, −1 bearish, 0 no-cross, at the close of each M5 bar.

Crossing logic: compare `sma3[i] - sma5[i]` sign vs `sma3[i-1] - sma5[i-1]` sign. First 5 bars (insufficient data for both SMAs) get `signal = 0`.

**Files:**
- Create: `src/zgb_sim/sma_cross.py`
- Test: `tests/zgb_sim/test_sma_cross.py`

- [ ] **Step 1: Write failing tests**

Create `tests/zgb_sim/test_sma_cross.py`:

```python
import numpy as np
import pandas as pd
import pytest
from zgb_sim.sma_cross import sma_cross_on_m5_closes


def _bars(ts_start: str, closes: list[float]) -> pd.DataFrame:
    n = len(closes)
    ts = pd.date_range(ts_start, periods=n, freq="5min", tz="UTC")
    return pd.DataFrame({
        "ts": ts.tz_localize(None),
        "open": closes, "high": closes, "low": closes, "close": closes,
    })


def test_empty_bars_returns_empty_arrays():
    bars = pd.DataFrame({"ts": [], "open": [], "high": [], "low": [], "close": []})
    close_ts, signal = sma_cross_on_m5_closes(bars)
    assert close_ts.shape == (0,)
    assert signal.shape == (0,)


def test_close_ts_is_open_plus_5min_int64():
    bars = _bars("2026-01-01 07:00", [100.0] * 10)
    close_ts, signal = sma_cross_on_m5_closes(bars)
    expected = np.int64(pd.Timestamp("2026-01-01 07:05").value)
    assert close_ts[0] == expected
    assert close_ts.dtype == np.int64
    assert signal.dtype == np.int8


def test_first_five_bars_signal_zero():
    bars = _bars("2026-01-01 07:00", [10, 11, 12, 13, 14, 15, 14, 13, 12, 11])
    _, signal = sma_cross_on_m5_closes(bars)
    # First 4 bars: SMA5 NaN. Bar 4 (index): SMA5 defined but no prior SMA5 to
    # compare against, so signal is 0. Crosses can only fire from bar 5 onward.
    assert all(signal[:5] == 0)


def test_bullish_cross_detected():
    # Pattern: closes that take SMA3 from below SMA5 to above SMA5.
    # Falling then rising sharply.
    closes = [50, 49, 48, 47, 46, 45, 48, 52, 56, 60, 64, 68]
    bars = _bars("2026-01-01 07:00", closes)
    _, signal = sma_cross_on_m5_closes(bars)
    # Somewhere in the rising portion, SMA3 (faster) will cross above SMA5.
    assert (signal == 1).any(), f"expected at least one bullish cross, got {signal.tolist()}"


def test_bearish_cross_detected():
    closes = [50, 51, 52, 53, 54, 55, 52, 48, 44, 40, 36, 32]
    bars = _bars("2026-01-01 07:00", closes)
    _, signal = sma_cross_on_m5_closes(bars)
    assert (signal == -1).any(), f"expected at least one bearish cross, got {signal.tolist()}"


def test_steady_uptrend_no_cross():
    # Monotonic uptrend: SMA3 stays above SMA5 the whole time after warmup.
    closes = list(range(100, 130))
    bars = _bars("2026-01-01 07:00", closes)
    _, signal = sma_cross_on_m5_closes(bars)
    # After warmup (bars 5+), no cross should fire.
    assert (signal[5:] == 0).all(), \
        f"expected no crosses in monotonic uptrend, got {signal.tolist()}"
```

- [ ] **Step 2: Run failing tests**

```
$env:PYTHONPATH = "src"
pytest tests/zgb_sim/test_sma_cross.py -v
```
Expected: 6 failures (ModuleNotFoundError: zgb_sim.sma_cross).

- [ ] **Step 3: Implementation**

Create `src/zgb_sim/sma_cross.py`:

```python
"""SMA(3) x SMA(5) cross signal on M5 closes for the V3 cross-exit feature.

The JIT loop wants two flat numpy arrays:
  close_ts_ns:     int64 — close-time of each M5 bar in ns since epoch
  cross_signal:    int8  — +1 bullish, -1 bearish, 0 no-cross, at that bar's close

First 5 bars (warmup) always 0.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

M5_NS = np.int64(5 * 60 * 1_000_000_000)


def sma_cross_on_m5_closes(m5_bars: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    if len(m5_bars) == 0:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.int8)
    ts = m5_bars["ts"]
    if hasattr(ts.dt, "tz") and ts.dt.tz is not None:
        ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)
    open_ns = ts.values.astype("datetime64[ns]").astype(np.int64)
    close_ts = (open_ns + M5_NS).astype(np.int64)

    closes = m5_bars["close"].values.astype(np.float64)
    sma3 = pd.Series(closes).rolling(3, min_periods=3).mean().to_numpy()
    sma5 = pd.Series(closes).rolling(5, min_periods=5).mean().to_numpy()

    n = len(closes)
    signal = np.zeros(n, dtype=np.int8)
    # Need both SMAs defined at i AND i-1 to detect cross.
    # SMA5 needs 5 closes -> first defined at index 4 -> first comparable at index 5.
    for i in range(5, n):
        prev_diff = sma3[i - 1] - sma5[i - 1]
        cur_diff  = sma3[i]     - sma5[i]
        if np.isnan(prev_diff) or np.isnan(cur_diff):
            continue
        if prev_diff >= 0 and cur_diff < 0:
            signal[i] = -1  # bearish
        elif prev_diff < 0 and cur_diff >= 0:
            signal[i] = 1   # bullish
    return close_ts, signal
```

- [ ] **Step 4: Run tests, verify pass**

```
pytest tests/zgb_sim/test_sma_cross.py -v
```
Expected: 6 passed.

- [ ] **Step 5: Commit**

```
git add src/zgb_sim/sma_cross.py tests/zgb_sim/test_sma_cross.py
git commit -m "$(cat <<'EOF'
feat(sim): add SMA(3)xSMA(5) M5 cross-signal precompute helper

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task V3.3: Wire cross_signal arrays + sma_cross_exit flag into `_run_sim`

Extend `_run_sim` signature with two new params: `sma_cross_exit` (bool) and `m5_cross_signal` (int8 array). The existing `m5_close_ts` is REUSED (same array works for both MA7 and SMA-cross lookups — they're all M5 bar close timestamps).

Wire defaults through `simulate_fast`. Guard against `ma_trail AND sma_cross_exit` both True.

**Files:**
- Modify: `src/zgb_sim/orb_fast.py`.

- [ ] **Step 1: Add params to `_run_sim` signature**

Find the line `ma_trail_retrace_pct,           # float64: HWM retrace fraction to arm trail (0.0 = V1)`. Add after:

```python
    ma_trail_retrace_pct,           # float64: HWM retrace fraction to arm trail (0.0 = V1)
    sma_cross_exit,                 # bool: enable SMA(3)x(5) cross exit on M5 (V3)
    m5_cross_signal,                # int8[M]: +1 bullish / -1 bearish / 0 none per M5 bar
):
```

- [ ] **Step 2: Wire wrapper precompute + guard**

In `simulate_fast`, find the block that precomputes `m5_close_ts, m5_sma7` for `ma_trail`. AFTER that block (and before the call to `_run_sim`), add:

```python
    # V3 cross-exit precompute (only when enabled).
    if bool(cfg.sma_cross_exit) and bool(cfg.ma_trail):
        raise ValueError(
            "ORBConfig.ma_trail and sma_cross_exit are mutually exclusive; "
            "set at most one to True.")
    if bool(cfg.sma_cross_exit):
        from .sma_cross import sma_cross_on_m5_closes
        m5_close_ts_x, m5_cross_signal = sma_cross_on_m5_closes(m5_bars)
        # If both V1 (ma_trail) and V3 are off (cross only), m5_close_ts wasn't
        # computed above. Use the cross helper's close_ts (same M5 bars, same logic).
        if not bool(cfg.ma_trail):
            m5_close_ts = m5_close_ts_x
    else:
        m5_cross_signal = np.empty(0, dtype=np.int8)
```

- [ ] **Step 3: Update `_run_sim` call site**

Find the call site that currently ends:
```python
        bool(cfg.ma_trail),
        m5_close_ts, m5_sma7,
        float(cfg.ma_trail_retrace_pct),
    )
```

Change to:
```python
        bool(cfg.ma_trail),
        m5_close_ts, m5_sma7,
        float(cfg.ma_trail_retrace_pct),
        bool(cfg.sma_cross_exit),
        m5_cross_signal,
    )
```

- [ ] **Step 4: Add per-position state arrays for cross-exit**

In `_run_sim`, find the block initialising MA7 state arrays:
```python
    pos_hwm_profit = np.zeros(MAX_POSITIONS, dtype=np.float64)
    pos_ma7_armed = np.zeros(MAX_POSITIONS, dtype=np.bool_)
```
Add immediately after:

```python
    # V3 cross-exit state: per-runner index into m5_cross_signal so a single
    # cross can't trigger more than once for the same runner.
    pos_cross_last_idx = np.zeros(MAX_POSITIONS, dtype=np.int64)
```

- [ ] **Step 5: Reset state at position creation**

In the position-add block (same place V1/V2 init their state), add after `pos_ma7_armed[slot] = False`:

```python
                    pos_cross_last_idx[slot] = 0
```

- [ ] **Step 6: Smoke test**

```
$env:PYTHONPATH = "src"
python -c "import zgb_sim.orb_fast; print('ok')"
```
Expected: `ok`.

- [ ] **Step 7: Verify V1 safety test still passes**

```
pytest tests/zgb_sim/test_orb_fast_ma_trail.py -v -m integration
```
Expected: PASS (sma_cross_exit defaults to False; no behavior change).

- [ ] **Step 8: Commit**

```
git add src/zgb_sim/orb_fast.py
git commit -m "$(cat <<'EOF'
chore(sim): wire sma_cross_exit signature + guard into _run_sim

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task V3.4: Cross-exit logic in JIT loop

Insert the cross-exit block AFTER the V1 MA7 ratchet block and BEFORE the existing SL/TP existing-positions loop (`# SL/TP on EXISTING positions`).

Each tick, for each post-HTP runner: advance `pos_cross_last_idx[i]` through `m5_close_ts` up to current tick. For each bar passed, if `m5_cross_signal[idx]` opposes runner direction AND unrealized P&L > 0, close the runner at current bid/ask and emit a `D_OTHER` deal. Mark position inactive. Once closed, skip remaining bars for this runner.

**Files:**
- Modify: `src/zgb_sim/orb_fast.py`.

- [ ] **Step 1: Add cross-exit block**

Grep for `# SL/TP on EXISTING positions` (the V1 anchor). Immediately BEFORE that comment, add:

```python
        # V3 cross-exit: close post-HTP runners on opposite-direction SMA(3)x(5)
        # cross while in profit. One-shot per cross-bar; processed before SL/TP check.
        if sma_cross_exit and len(m5_close_ts) > 0:
            for i in range(MAX_POSITIONS):
                if not pos_active[i] or not pos_is_runner[i] or not pos_htp_fired[i]:
                    continue
                idx = pos_cross_last_idx[i]
                closed_here = False
                while idx < len(m5_close_ts) and m5_close_ts[idx] <= ts_ns:
                    sig = m5_cross_signal[idx]
                    if sig != 0:
                        # Side filter: bearish closes longs, bullish closes shorts.
                        if (sig == -1 and pos_dir[i] == 1) or (sig == 1 and pos_dir[i] == -1):
                            close_px = bid if pos_dir[i] == 1 else ask
                            upnl = _pnl(pos_dir[i], pos_entry[i], close_px,
                                        pos_lots[i], tick_value, tick_size)
                            if upnl > 0.0:
                                balance += upnl
                                realized_today += upnl
                                if deal_count < deal_ts.shape[0]:
                                    deal_ts[deal_count] = ts_ns
                                    deal_kind[deal_count] = D_OTHER
                                    deal_dir[deal_count] = pos_dir[i]
                                    deal_lots[deal_count] = pos_lots[i]
                                    deal_price[deal_count] = close_px
                                    deal_pnl[deal_count] = upnl
                                    deal_count += 1
                                if balance > balance_max:
                                    balance_max = balance
                                cur_dd = balance_max - balance
                                if cur_dd > dd_abs:
                                    dd_abs = cur_dd
                                pos_active[i] = False
                                closed_here = True
                                break
                    idx += 1
                if not closed_here:
                    pos_cross_last_idx[i] = idx
```

- [ ] **Step 2: Smoke test**

```
$env:PYTHONPATH = "src"
python -c "import zgb_sim.orb_fast; print('ok')"
```
Expected: `ok`.

- [ ] **Step 3: V1 baseline safety still passes**

```
pytest tests/zgb_sim/test_orb_fast_ma_trail.py -v -m integration
```
Expected: PASS.

- [ ] **Step 4: Commit**

```
git add src/zgb_sim/orb_fast.py
git commit -m "$(cat <<'EOF'
feat(sim): SMA(3)x(5) cross-exit closes profitable post-HTP runners

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task V3.5: A/B driver — 4 windows × 6 streams × {off, on}

Build the driver. Same structure as V1's `sim_orb_ma7_trail_ab.py` — change config + report path.

**Files:**
- Create: `scripts/sim_orb_sma_cross_exit_ab.py`
- Create: `docs/reports/2026-05-25-sma-cross-exit-ab.md` (driver writes).

**API reminders (confirmed in V1 Task 9):**
- `simulate_fast(ticks, m5_bars, m1_bars, cfg, meta, initial_balance)`.
- `SimResult.max_drawdown`, `net_profit`, `profit_factor`, `trades`.
- `fetch_window(None, start, end, spread_pts, account="sim")` returns `(sym, ticks, m1, m5)`.
- `parse_v6_setfile(path)` returns dict with `["streams"]`.
- Reference template: `scripts/sim_orb_ma7_trail_ab.py`.

- [ ] **Step 1: Create driver**

Create `scripts/sim_orb_sma_cross_exit_ab.py`:

```python
"""V3 A/B driver: SMA(3)x(5) cross-exit on v6 setfile across 4 weekly windows.

Run:  $env:PYTHONPATH = "src"; python scripts/sim_orb_sma_cross_exit_ab.py

Outputs:
  docs/reports/2026-05-25-sma-cross-exit-ab.md  — comparison table + verdict

Per spec docs/superpowers/specs/2026-05-25-sma35-cross-exit-design.md.
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
REPORT  = ROOT / "docs" / "reports" / "2026-05-25-sma-cross-exit-ab.md"
DEPOSIT = 10_000.0
SPREAD  = 30
RISK_PER_STREAM = 1.5

WINDOWS = [
    ("W1", datetime(2026, 4, 25, tzinfo=timezone.utc), datetime(2026, 5, 2,  tzinfo=timezone.utc)),
    ("W2", datetime(2026, 5, 2,  tzinfo=timezone.utc), datetime(2026, 5, 9,  tzinfo=timezone.utc)),
    ("W3", datetime(2026, 5, 9,  tzinfo=timezone.utc), datetime(2026, 5, 16, tzinfo=timezone.utc)),
    ("W4", datetime(2026, 5, 16, tzinfo=timezone.utc), datetime(2026, 5, 23, tzinfo=timezone.utc)),
]


def _meta() -> SymbolMeta:
    return SymbolMeta(point=0.01, tick_size=0.01, tick_value=1.0, stops_level_pts=0,
                      volume_min=0.01, volume_max=100.0, volume_step=0.01, digits=2)


def _build_cfg(stream: dict, cross_on: bool) -> ORBConfig:
    return ORBConfig(
        risk_pct=RISK_PER_STREAM,
        range_minutes=stream["range_minutes"],
        fixed_sl_pts=stream["fixed_sl_pts"],
        rr_ratio=stream["rr_ratio"],
        half_tp_ratio=stream["half_tp_ratio"],
        pending_expire_minutes=stream["pending_expire_minutes"],
        fractal_confirm=True, fractal_width=5,
        sma_cross_exit=cross_on,
    )


def _haircut(net: float, dd: float) -> tuple[float, float]:
    """6% NP haircut per feedback_sim_vs_live_calibration."""
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
            for mode_name, on in [("off", False), ("on", True)]:
                r = simulate_fast(ticks, m5, m1, _build_cfg(s, on), _meta(), DEPOSIT)
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

    pivot = portfolio.pivot(index=["window", "days"], columns="mode",
                            values=["net", "dd", "trades", "net_hc", "nd_hc"])
    pivot["delta_nd_hc"] = pivot[("nd_hc", "on")] - pivot[("nd_hc", "off")]
    pivot["delta_pct"] = (pivot["delta_nd_hc"] /
                          pivot[("nd_hc", "off")].replace(0, np.nan)) * 100

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    with REPORT.open("w", encoding="utf-8") as f:
        f.write("# SMA(3) x SMA(5) Cross-Exit A/B Report (2026-05-25)\n\n")
        f.write(f"Setfile: `{SETFILE.name}`  ·  spread {SPREAD}pt  ·  deposit ${DEPOSIT:,.0f}  ·  risk {RISK_PER_STREAM}%/stream\n\n")
        f.write("## Portfolio per window\n\n")
        f.write(pivot.to_markdown())
        f.write("\n\n## Per-stream detail\n\n")
        f.write(df.to_markdown(index=False))
        wins = int((pivot["delta_pct"] >= 10).sum())
        regressions = int((pivot["delta_pct"] <= -15).sum())
        f.write("\n\n## Decision\n\n")
        f.write(f"- Windows where on improves NP/DD$_hc by >=+10%: **{wins} / 4**\n")
        f.write(f"- Windows where on regresses NP/DD$_hc by <=-15%: **{regressions} / 4**\n")
        if wins >= 3 and regressions == 0:
            f.write("- **Verdict: ADVANCE** to full WFO with sma_cross_exit as a dim.\n")
        elif regressions > 0:
            f.write("- **Verdict: REJECT** -- regime-dependent regression observed.\n")
        else:
            f.write("- **Verdict: ITERATE** -- mixed; revisit signal periods or filter.\n")
    print(f"wrote {REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Dry-run on W4 only**

Edit `WINDOWS` to keep only W4. Run:
```
$env:PYTHONPATH = "src"
python scripts/sim_orb_sma_cross_exit_ab.py
```
Expected: report file written. Inspect — `on` should differ from `off` (cross fires sometimes). `off` portfolio NP/DD$_hc should equal V1 W4 off-baseline 0.774.

- [ ] **Step 3: Restore 4 windows + full run**

ETA ~5–8 min.

- [ ] **Step 4: Commit driver + report**

```
git add scripts/sim_orb_sma_cross_exit_ab.py docs/reports/2026-05-25-sma-cross-exit-ab.md
git commit -m "$(cat <<'EOF'
feat(sim): A/B driver for SMA(3)x(5) cross-exit across 4 weekly windows

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task V3.6: Memory + index update

**Files:**
- Modify: `C:\Users\Zhu-En\.claude\projects\c--Users-Zhu-En-zgb-opti\memory\project_ma7_trail_test_2026_05_25.md`

- [ ] **Step 1: Read verdict from report**

Open `docs/reports/2026-05-25-sma-cross-exit-ab.md`, read decision section.

- [ ] **Step 2: Append V3 section to existing memory file**

Add a `## V3 (SMA(3)x(5) cross-exit)` section to the existing memory file under V2's section. Include:
- Per-window portfolio delta_pct table (W1..W4).
- Wins / regressions / verdict.
- One-line read on whether V3 succeeded where V1/V2 failed.

Update the `description:` frontmatter to reflect V3 outcome.

- [ ] **Step 3: No git for memory** (outside repo).

---

## Self-Review Checklist

- [ ] `tests/zgb_sim/test_sma_cross.py` — 6/6 green.
- [ ] V1 safety test still passes after V3.3 and V3.4.
- [ ] `sma_cross_exit=False` results unchanged from baseline.
- [ ] Mutually exclusive guard works (`ma_trail=True, sma_cross_exit=True` raises ValueError).
- [ ] Report verdict line present and consistent with summary numbers.
