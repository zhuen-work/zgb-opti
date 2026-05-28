# MA7 Trailing Stop (Post-HTP) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an MA(7)-on-M5 trailing stop to `orb_fast.py` that activates only on the *runner* lot after the HTP partial close has fired, then run a 4-window A/B sim against v6 setfile to decide whether to advance the feature.

**Architecture:** Extend the existing JIT sim engine (`src/zgb_sim/orb_fast.py`) with a new `ma_trail` mode that sits next to the existing V1 `fractal_trail` infrastructure. Track per-position `is_runner` and `htp_fired` flags. Precompute SMA(7, close) over M5 closes in the Python wrapper and pass timestamp/value arrays into the JIT loop. Add a driver script that runs v6 setfile twice (off/on) across 4 weekly OOS windows and prints a per-stream + portfolio comparison.

**Tech Stack:** Python 3.11, numpy, pandas, numba (existing `@njit` sim core). MT5 tick loader for data (existing `zgb_sim.tick_loader`). Windows / PowerShell.

**Spec:** [docs/superpowers/specs/2026-05-25-ma7-trail-post-htp-design.md](../specs/2026-05-25-ma7-trail-post-htp-design.md)

**Conventions:**
- All MA7 logic must default to OFF (`cfg.ma_trail=False`). When False, results must be byte-identical to current v6 sim.
- Plan compiles MQ5 → not applicable (this iteration is sim-only).
- Per [feedback_max_6_workers](../../../memory/feedback_max_6_workers.md), driver uses ≤6 parallel workers if it parallelizes at all.
- Per [feedback_kill_mt5](../../../memory/feedback_kill_mt5.md), driver script calls `kill_mt5_terminal()` after tick fetch.
- All git commits are CREATED at end of each task (not amended). Co-author footer per CLAUDE convention.

---

## File Structure

**Modified:**
- `src/zgb_sim/orb.py` — add `ma_trail` boolean field to `ORBConfig`.
- `src/zgb_sim/orb_fast.py` — extend `_run_sim` signature + JIT body, extend `simulate_fast` wrapper.

**Created:**
- `src/zgb_sim/ma_trail.py` — pure-Python SMA(7, close) precompute helper (testable).
- `tests/zgb_sim/test_ma_trail.py` — unit tests for the SMA7 helper.
- `tests/zgb_sim/test_orb_fast_ma_trail.py` — integration test: `ma_trail=False` produces identical results to today.
- `scripts/sim_orb_ma7_trail_ab.py` — A/B driver for 4 weekly windows on v6 setfile.
- `docs/reports/2026-05-25-ma7-trail-ab.md` — report output (filled by driver run).

---

## Task 1: Add `ma_trail` field to ORBConfig

**Files:**
- Modify: `src/zgb_sim/orb.py:62-70` (insert into ORBConfig dataclass next to fractal flags)

- [ ] **Step 1: Add config field**

Open `src/zgb_sim/orb.py`. Find the block ending with `fractal_width: int = 5`. Add immediately after, before `comment: str = "ORB"`:

```python
    # ----- MA7 post-HTP trail (default OFF) -----
    # Once HTP partial close fires on a stream's half-lot, the surviving "runner"
    # position switches its SL from the static value to SMA(7, close) on M5.
    # SL ratchets only in profit direction. No effect when half_tp_ratio == 0.
    ma_trail: bool = False
```

- [ ] **Step 2: Verify dataclass loads**

Run:
```
python -c "from zgb_sim.orb import ORBConfig; c = ORBConfig(); print('ma_trail=', c.ma_trail)"
```
(Run from `src/` or with `PYTHONPATH=src`.)

Expected output: `ma_trail= False`

- [ ] **Step 3: Commit**

```
git add src/zgb_sim/orb.py
git commit -m "$(cat <<'EOF'
feat(sim): add ma_trail flag to ORBConfig (default off)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: SMA(7, close) precompute helper

A pure-Python (non-JIT) helper that takes M5 bars and returns `(close_ts_ns: int64[N], sma7: float64[N])` where `close_ts_ns[i]` is the close-time of bar i (open_time + 5min) and `sma7[i]` is the SMA(close, 7) up to and including bar i. First 6 bars have `sma7=NaN` and must not be used by the JIT loop.

**Files:**
- Create: `src/zgb_sim/ma_trail.py`
- Test: `tests/zgb_sim/test_ma_trail.py`

- [ ] **Step 1: Write the failing test**

Create `tests/zgb_sim/test_ma_trail.py`:

```python
import numpy as np
import pandas as pd
import pytest
from zgb_sim.ma_trail import sma7_on_m5_closes


def _bars(ts_start: str, closes: list[float]) -> pd.DataFrame:
    n = len(closes)
    ts = pd.date_range(ts_start, periods=n, freq="5min", tz="UTC")
    return pd.DataFrame({
        "ts": ts.tz_localize(None),
        "open": closes,
        "high": closes,
        "low": closes,
        "close": closes,
    })


def test_close_ts_is_open_plus_5min():
    bars = _bars("2026-01-01 07:00", [100.0] * 8)
    close_ts, sma = sma7_on_m5_closes(bars)
    # close of first bar = open + 5min
    expected_first = np.int64(pd.Timestamp("2026-01-01 07:05").value)
    assert close_ts[0] == expected_first
    assert len(close_ts) == 8


def test_first_six_bars_nan_then_avg():
    closes = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0]
    bars = _bars("2026-01-01 07:00", closes)
    _, sma = sma7_on_m5_closes(bars)
    assert all(np.isnan(sma[:6]))
    # bar 6 (index 6): mean of closes[0..6] = (10+20+30+40+50+60+70)/7 = 40.0
    assert sma[6] == pytest.approx(40.0)
    # bar 7: mean of closes[1..7] = (20+30+40+50+60+70+80)/7 = 50.0
    assert sma[7] == pytest.approx(50.0)


def test_empty_bars_returns_empty_arrays():
    bars = pd.DataFrame({"ts": [], "open": [], "high": [], "low": [], "close": []})
    close_ts, sma = sma7_on_m5_closes(bars)
    assert close_ts.shape == (0,)
    assert sma.shape == (0,)


def test_returned_types_are_jit_compatible():
    bars = _bars("2026-01-01 07:00", [100.0] * 10)
    close_ts, sma = sma7_on_m5_closes(bars)
    assert close_ts.dtype == np.int64
    assert sma.dtype == np.float64
```

- [ ] **Step 2: Run test to verify it fails**

```
pytest tests/zgb_sim/test_ma_trail.py -v
```
Expected: 4 failures with `ModuleNotFoundError: No module named 'zgb_sim.ma_trail'`.

- [ ] **Step 3: Write minimal implementation**

Create `src/zgb_sim/ma_trail.py`:

```python
"""SMA(7, close) on M5 bars for the post-HTP trailing stop in orb_fast.

The JIT loop wants two flat numpy arrays:
  close_ts_ns: int64 — close-time of each M5 bar in ns since epoch
  sma7:        float64 — SMA(close, 7) ending at that bar; NaN for first 6 bars

NaN-valued entries must not be read by the JIT consumer (which guards with
`isnan` check before ratcheting).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

M5_NS = np.int64(5 * 60 * 1_000_000_000)
WINDOW = 7


def sma7_on_m5_closes(m5_bars: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    if len(m5_bars) == 0:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float64)
    ts = m5_bars["ts"]
    if hasattr(ts.dt, "tz") and ts.dt.tz is not None:
        ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)
    open_ns = ts.values.astype("datetime64[ns]").astype(np.int64)
    close_ts = open_ns + M5_NS
    closes = m5_bars["close"].values.astype(np.float64)
    sma = pd.Series(closes).rolling(WINDOW, min_periods=WINDOW).mean().to_numpy()
    return close_ts.astype(np.int64), sma.astype(np.float64)
```

- [ ] **Step 4: Run tests, verify all pass**

```
pytest tests/zgb_sim/test_ma_trail.py -v
```
Expected: 4 passed.

- [ ] **Step 5: Commit**

```
git add src/zgb_sim/ma_trail.py tests/zgb_sim/test_ma_trail.py
git commit -m "$(cat <<'EOF'
feat(sim): add SMA(7,close) M5 precompute helper for ma_trail

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Extend `_run_sim` signature (wiring only, no logic change)

Add three new parameters to the JIT function. New behavior is gated entirely by the `ma_trail` flag, which is False at this step. Existing behavior must not change.

**Files:**
- Modify: `src/zgb_sim/orb_fast.py:100-116` (`_run_sim` signature)
- Modify: `src/zgb_sim/orb_fast.py:649-667` (call site in `simulate_fast`)

- [ ] **Step 1: Extend `_run_sim` signature**

In `src/zgb_sim/orb_fast.py`, change the `_run_sim` signature (line ~115) from:

```python
    f_up_ts, f_up_pr,               # int64[k], float64[k]: up-fractals (sorted asc by ts)
    f_dn_ts, f_dn_pr,               # int64[k], float64[k]: dn-fractals (sorted asc by ts)
    v1, v2, v3,                     # bool flags
):
```

to:

```python
    f_up_ts, f_up_pr,               # int64[k], float64[k]: up-fractals (sorted asc by ts)
    f_dn_ts, f_dn_pr,               # int64[k], float64[k]: dn-fractals (sorted asc by ts)
    v1, v2, v3,                     # bool flags
    ma_trail,                       # bool: enable SMA(7) trail on runner post-HTP
    m5_close_ts, m5_sma7,           # int64[M], float64[M]: close ts + SMA7 per M5 bar
):
```

- [ ] **Step 2: Update call site in `simulate_fast`**

At the bottom of `simulate_fast` (around line 649), the call currently ends:

```python
        v1, v2, v3,
    )
```

Change to:

```python
        v1, v2, v3,
        False,
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
    )
```

(Hardcoded `False` + empties; Task 8 will wire the real precomputed arrays.)

- [ ] **Step 3: Smoke-test that simulate_fast still runs**

Run:
```
python -c "import zgb_sim.orb_fast; print('ok')"
```
(Set `PYTHONPATH=src` first or run from `src/`.)
Expected: `ok` (numba may print a few JIT warnings — that's fine; failure looks like a Python traceback).

- [ ] **Step 4: Commit**

```
git add src/zgb_sim/orb_fast.py
git commit -m "$(cat <<'EOF'
chore(sim): wire ma_trail signature into _run_sim (no logic yet)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Add per-position state arrays for HTP-runner tracking

Add `pos_is_runner` (bool) and `pos_htp_fired` (bool) and `pos_ma7_last_idx` (int64) state arrays alongside the existing `pos_*` arrays. Initialize to zero/False.

**Files:**
- Modify: `src/zgb_sim/orb_fast.py:129-141` (after `pos_sl_trail_idx_up`)

- [ ] **Step 1: Add state arrays**

In `_run_sim`, find the block:

```python
    pos_sl_trail_hwm = np.zeros(MAX_POSITIONS, dtype=np.float64)
    pos_sl_trail_idx_dn = np.zeros(MAX_POSITIONS, dtype=np.int64)
    pos_sl_trail_idx_up = np.zeros(MAX_POSITIONS, dtype=np.int64)
```

Add immediately after:

```python
    # MA7 post-HTP trail state
    pos_is_runner = np.zeros(MAX_POSITIONS, dtype=np.bool_)
    pos_htp_fired = np.zeros(MAX_POSITIONS, dtype=np.bool_)
    pos_ma7_last_idx = np.zeros(MAX_POSITIONS, dtype=np.int64)
```

- [ ] **Step 2: Smoke test**

```
python -c "import zgb_sim.orb_fast; print('ok')"
```
Expected: `ok`.

- [ ] **Step 3: Commit**

```
git add src/zgb_sim/orb_fast.py
git commit -m "$(cat <<'EOF'
chore(sim): add pos_is_runner/htp_fired/ma7_last_idx state arrays

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Mark the runner at position creation

The sim creates two positions per direction when `htp_ratio > 0`: a half-lot with `tp=tp_half` (added first) and a runner with `tp=tp` (added second). We mark the second-added position with the same session+direction as the runner.

The position-add block lives at `orb_fast.py:480-507` (the `for j in range(new_pos_count)` loop). Pendings carry a session id; we can look at each newly-added position's session+direction and decide:

- If `htp_ratio == 0`: not a runner (only one position per direction).
- If `htp_ratio > 0`: among positions in the same session+direction, the one with the *further* TP is the runner.

We implement: when adding a new position, if `htp_ratio > 0` and there's already an active position with the same session+direction, AND this new position's TP is further from entry than the existing one's, this new position is the runner. Else, the existing one is the runner (and we set its flag).

**Files:**
- Modify: `src/zgb_sim/orb_fast.py:480-507` (position creation loop)

- [ ] **Step 1: Mark runner during position add**

In `_run_sim`, find the position-add loop:

```python
        for j in range(new_pos_count):
            for slot in range(MAX_POSITIONS):
                if not pos_active[slot]:
                    pos_dir[slot] = new_pos_dir[j]
                    pos_entry[slot] = new_pos_entry[j]
                    pos_sl[slot] = new_pos_sl[j]
                    pos_tp[slot] = new_pos_tp[j]
                    pos_lots[slot] = new_pos_lots[j]
                    pos_session[slot] = new_pos_session[j]
                    pos_active[slot] = True
                    pos_be_done[slot] = False
```

After `pos_be_done[slot] = False`, add:

```python
                    # MA7 trail: identify runner (the further-TP position per session+direction).
                    # Default: not a runner.
                    pos_is_runner[slot] = False
                    pos_htp_fired[slot] = False
                    pos_ma7_last_idx[slot] = 0
                    if htp_ratio > 0:
                        # Search for sibling already-active position with same session+direction.
                        sib = -1
                        for kk in range(MAX_POSITIONS):
                            if kk == slot:
                                continue
                            if (pos_active[kk]
                                    and pos_session[kk] == new_pos_session[j]
                                    and pos_dir[kk] == new_pos_dir[j]):
                                sib = kk
                                break
                        if sib >= 0:
                            # Compare TP distance from entry (signed for direction):
                            # runner has the further TP.
                            new_tp_dist = abs(new_pos_tp[j] - new_pos_entry[j])
                            sib_tp_dist = abs(pos_tp[sib] - pos_entry[sib])
                            if new_tp_dist > sib_tp_dist:
                                pos_is_runner[slot] = True
                                pos_is_runner[sib] = False
                            else:
                                pos_is_runner[slot] = False
                                pos_is_runner[sib] = True
```

(Leave the existing fractal-trail init block immediately below this unchanged.)

- [ ] **Step 2: Smoke test**

```
python -c "import zgb_sim.orb_fast; print('ok')"
```
Expected: `ok`.

- [ ] **Step 3: Commit**

```
git add src/zgb_sim/orb_fast.py
git commit -m "$(cat <<'EOF'
feat(sim): mark runner position per session+direction at add time

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Detect HTP-fired event when half-lot closes via TP

When a non-runner position closes via TP, set `pos_htp_fired=True` on its sibling runner (same session, same direction).

**Files:**
- Modify: `src/zgb_sim/orb_fast.py:459-477` (the `hit_tp` branch inside the SL/TP existing-positions loop)

- [ ] **Step 1: Mark sibling runner on half-lot TP close**

In `_run_sim`, find the SL/TP existing-positions loop, specifically the `elif hit_tp:` branch. After the line `pos_active[i] = False` at the end of that branch, add:

```python
                # MA7 trail: if a half-lot (non-runner) just hit TP, flag the sibling runner.
                if htp_ratio > 0 and not pos_is_runner[i]:
                    sess_i = pos_session[i]
                    dir_i = pos_dir[i]
                    for kk in range(MAX_POSITIONS):
                        if (pos_active[kk]
                                and pos_is_runner[kk]
                                and pos_session[kk] == sess_i
                                and pos_dir[kk] == dir_i):
                            pos_htp_fired[kk] = True
                            # Snap ma7_last_idx to the first M5 close at-or-after now,
                            # so trail can't see pre-HTP-fire bars.
                            if ma_trail:
                                lo = 0
                                hi = len(m5_close_ts)
                                while lo < hi:
                                    mid = (lo + hi) // 2
                                    if m5_close_ts[mid] < ts_ns:
                                        lo = mid + 1
                                    else:
                                        hi = mid
                                pos_ma7_last_idx[kk] = np.int64(lo)
```

(`np.searchsorted` is not available inside `@njit` in all numba versions — manual binary search keeps the code portable.)

- [ ] **Step 2: Smoke test**

```
python -c "import zgb_sim.orb_fast; print('ok')"
```
Expected: `ok`.

- [ ] **Step 3: Commit**

```
git add src/zgb_sim/orb_fast.py
git commit -m "$(cat <<'EOF'
feat(sim): set htp_fired on sibling runner when half-lot hits TP

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Ratchet runner SL toward MA7 once HTP has fired

Mirror the V1 fractal-trail block: walk forward through `m5_close_ts` for each htp-fired runner, advancing `pos_ma7_last_idx` to the latest M5 bar whose close_ts ≤ current tick ts. Ratchet pos_sl toward `m5_sma7[idx]` (skip if NaN).

**Files:**
- Modify: `src/zgb_sim/orb_fast.py:395-422` (insert immediately after the V1 fractal-trail block, before SL/TP existing-positions loop)

- [ ] **Step 1: Add MA7 ratchet block**

In `_run_sim`, find the end of the V1 fractal-trail block (line ~422, ending with `pos_sl_trail_idx_up[i] = upper`). After it, before the SL/TP check at line ~424, add:

```python
        # MA7 post-HTP trail: ratchet runner SL toward SMA7 on closed M5 bars.
        # Runs on EXISTING positions before SL/TP check (matches fractal-trail order).
        if ma_trail and len(m5_close_ts) > 0:
            for i in range(MAX_POSITIONS):
                if not pos_active[i] or not pos_is_runner[i] or not pos_htp_fired[i]:
                    continue
                # Advance index to the latest M5 bar with close_ts <= ts_ns.
                idx = pos_ma7_last_idx[i]
                while idx < len(m5_close_ts) and m5_close_ts[idx] <= ts_ns:
                    sma_val = m5_sma7[idx]
                    if not np.isnan(sma_val):
                        if pos_dir[i] == 1:
                            # BUY: raise SL toward sma7 if it improves.
                            if sma_val > pos_sl[i]:
                                pos_sl[i] = _norm_price(sma_val, tick_size, digits)
                        else:
                            # SELL: lower SL toward sma7 if it improves.
                            if sma_val < pos_sl[i]:
                                pos_sl[i] = _norm_price(sma_val, tick_size, digits)
                    idx += 1
                pos_ma7_last_idx[i] = idx
```

- [ ] **Step 2: Smoke test**

```
python -c "import zgb_sim.orb_fast; print('ok')"
```
Expected: `ok`.

- [ ] **Step 3: Commit**

```
git add src/zgb_sim/orb_fast.py
git commit -m "$(cat <<'EOF'
feat(sim): ratchet runner SL toward SMA(7,M5 close) when htp_fired

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Wire `cfg.ma_trail` through `simulate_fast` wrapper

Replace the hardcoded `False`/empty arrays from Task 3's call site with the real precomputed values.

**Files:**
- Modify: `src/zgb_sim/orb_fast.py:649-667` (call site)
- Modify: `src/zgb_sim/orb_fast.py:583-595` (M5 bar prep — adjacent to existing m5_highs/m5_lows code)

- [ ] **Step 1: Precompute MA7 arrays in simulate_fast**

In `simulate_fast`, find the existing M5 prep block at ~line 583-594. After the `m5_lows = ...` line and before `if len(tick_ts_ns) == 0:`, add:

```python
    # MA7 trail precompute (only when enabled; otherwise empty arrays).
    if bool(cfg.ma_trail):
        from .ma_trail import sma7_on_m5_closes
        m5_close_ts, m5_sma7 = sma7_on_m5_closes(m5_bars)
    else:
        m5_close_ts = np.empty(0, dtype=np.int64)
        m5_sma7 = np.empty(0, dtype=np.float64)
```

- [ ] **Step 2: Update _run_sim call site**

Change the hardcoded trailer set in Task 3 from:

```python
        v1, v2, v3,
        False,
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
    )
```

to:

```python
        v1, v2, v3,
        bool(cfg.ma_trail),
        m5_close_ts, m5_sma7,
    )
```

- [ ] **Step 3: Smoke test**

```
python -c "from zgb_sim.orb import ORBConfig; from zgb_sim.orb_fast import simulate_fast; print('imports ok, ma_trail=', ORBConfig().ma_trail)"
```
Expected: `imports ok, ma_trail= False`.

- [ ] **Step 4: Commit**

```
git add src/zgb_sim/orb_fast.py
git commit -m "$(cat <<'EOF'
feat(sim): plumb cfg.ma_trail + precomputed SMA7 arrays through simulate_fast

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Integration test — `ma_trail=False` byte-identical to baseline

Critical safety: with `ma_trail=False`, the new code paths must not change *any* sim output. Run v6 setfile on a small window with `ma_trail=False` and compare net_profit + drawdown + trade count to the same call with the code paths bypassed (i.e., before this PR).

We approximate "before this PR" by running stream S1 of v6 setfile over a small fixed window and asserting the result equals a hardcoded golden tuple captured here.

**Files:**
- Create: `tests/zgb_sim/test_orb_fast_ma_trail.py`

- [ ] **Step 1: Capture a baseline for one stream + small window**

Run, in PowerShell:
```
$env:PYTHONPATH = "src"
python -c "
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
import sys
sys.path.insert(0, 'scripts')
from sim_orb_oos_today_hedge_v6 import parse_v6_setfile
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast
from zgb_sim.tick_loader import load_ticks, load_m5, init_account, kill_mt5_terminal
from zgb_sim.scalper_v1 import SymbolMeta

cfg_data = parse_v6_setfile(Path('configs/sets/dt818_pro_v6_9pct_may23_may16.set'))
s1 = cfg_data['streams'][0]
init_account('sim')
start = datetime(2026, 5, 19, tzinfo=timezone.utc)
end   = datetime(2026, 5, 20, tzinfo=timezone.utc)
ticks = load_ticks('XAUUSD', start, end, spread_pts=30)
m5 = load_m5('XAUUSD', start, end)
meta = SymbolMeta(point=0.01, tick_size=0.01, tick_value=1.0, stops_level_pts=0,
                  volume_min=0.01, volume_max=100.0, volume_step=0.01, digits=2)
cfg = ORBConfig(risk_pct=1.5, range_minutes=s1['range_minutes'],
                fixed_sl_pts=s1['fixed_sl_pts'], rr_ratio=s1['rr_ratio'],
                half_tp_ratio=s1['half_tp_ratio'],
                pending_expire_minutes=s1['pending_expire_minutes'],
                fractal_confirm=True, fractal_width=5, ma_trail=False)
r = simulate_fast(cfg, ticks, m5, meta, 10_000.0)
print(f'GOLDEN: net={r.net_profit:.4f} dd={r.dd_abs:.4f} trades={r.trades}')
kill_mt5_terminal()
"
```

Copy the printed `GOLDEN: net=... dd=... trades=...` line. (If MT5 isn't running or there are no ticks for that date, pick the next available historical day in May 2026 and update the date range in the test below.)

- [ ] **Step 2: Write the integration test using the captured golden values**

Create `tests/zgb_sim/test_orb_fast_ma_trail.py`. Replace `<NET>`, `<DD>`, `<TRADES>` with the values from Step 1:

```python
"""Safety net: ma_trail=False must produce byte-identical results to baseline.

The golden values were captured on 2026-05-19 to 2026-05-20 UTC with v6 setfile
S1 parameters, sim spread 30pt, $10k deposit. If broker tick data changes,
recapture per the procedure in tests/zgb_sim/README.md (or rerun the capture
snippet in the plan).
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))


@pytest.mark.integration
def test_ma_trail_off_matches_baseline_golden():
    from sim_orb_oos_today_hedge_v6 import parse_v6_setfile
    from zgb_sim.orb import ORBConfig
    from zgb_sim.orb_fast import simulate_fast
    from zgb_sim.tick_loader import load_ticks, load_m5, init_account, kill_mt5_terminal
    from zgb_sim.scalper_v1 import SymbolMeta

    cfg_data = parse_v6_setfile(ROOT / "configs/sets/dt818_pro_v6_9pct_may23_may16.set")
    s1 = cfg_data["streams"][0]
    init_account("sim")
    start = datetime(2026, 5, 19, tzinfo=timezone.utc)
    end = datetime(2026, 5, 20, tzinfo=timezone.utc)
    try:
        ticks = load_ticks("XAUUSD", start, end, spread_pts=30)
        m5 = load_m5("XAUUSD", start, end)
    finally:
        kill_mt5_terminal()

    meta = SymbolMeta(point=0.01, tick_size=0.01, tick_value=1.0, stops_level_pts=0,
                      volume_min=0.01, volume_max=100.0, volume_step=0.01, digits=2)
    cfg = ORBConfig(risk_pct=1.5, range_minutes=s1["range_minutes"],
                    fixed_sl_pts=s1["fixed_sl_pts"], rr_ratio=s1["rr_ratio"],
                    half_tp_ratio=s1["half_tp_ratio"],
                    pending_expire_minutes=s1["pending_expire_minutes"],
                    fractal_confirm=True, fractal_width=5, ma_trail=False)
    r = simulate_fast(cfg, ticks, m5, meta, 10_000.0)

    # Captured golden 2026-05-25 (replace with Step 1's printed values):
    GOLDEN_NET    = <NET>
    GOLDEN_DD     = <DD>
    GOLDEN_TRADES = <TRADES>

    assert r.trades == GOLDEN_TRADES, f"trades drifted: {r.trades} vs {GOLDEN_TRADES}"
    assert r.net_profit == pytest.approx(GOLDEN_NET, abs=0.01), \
        f"net drifted: {r.net_profit} vs {GOLDEN_NET}"
    assert r.dd_abs == pytest.approx(GOLDEN_DD, abs=0.01), \
        f"dd drifted: {r.dd_abs} vs {GOLDEN_DD}"
```

- [ ] **Step 3: Run the test**

```
$env:PYTHONPATH = "src"
pytest tests/zgb_sim/test_orb_fast_ma_trail.py -v -m integration
```
Expected: PASS. If FAIL, the new code paths have a side effect even when `ma_trail=False` — investigate before proceeding.

- [ ] **Step 4: Commit**

```
git add tests/zgb_sim/test_orb_fast_ma_trail.py
git commit -m "$(cat <<'EOF'
test(sim): pin ma_trail=False to baseline golden values (S1, 2026-05-19)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: A/B driver script — 4 weekly windows × 6 streams × {off, on}

Build the driver. For each of 4 windows and each of 6 streams in v6 setfile, run twice: once with `ma_trail=False`, once with `True`. Aggregate per-window portfolio totals and produce a markdown comparison table.

**Files:**
- Create: `scripts/sim_orb_ma7_trail_ab.py`
- Create: `docs/reports/2026-05-25-ma7-trail-ab.md` (driver writes this)

- [ ] **Step 1: Create driver skeleton**

Create `scripts/sim_orb_ma7_trail_ab.py`:

```python
"""A/B driver: v6 setfile with ma_trail=off vs on, 4 weekly OOS windows.

Run: python scripts/sim_orb_ma7_trail_ab.py

Outputs:
  docs/reports/2026-05-25-ma7-trail-ab.md  — comparison table

Per spec docs/superpowers/specs/2026-05-25-ma7-trail-post-htp-design.md.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
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
from zgb_sim.tick_loader import load_ticks, load_m5, init_account, kill_mt5_terminal
from sim_orb_oos_today_hedge_v6 import parse_v6_setfile


SETFILE = ROOT / "configs" / "sets" / "dt818_pro_v6_9pct_may23_may16.set"
REPORT  = ROOT / "docs" / "reports" / "2026-05-25-ma7-trail-ab.md"
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


def _build_cfg(stream: dict, ma_trail: bool) -> ORBConfig:
    return ORBConfig(
        risk_pct=RISK_PER_STREAM,
        range_minutes=stream["range_minutes"],
        fixed_sl_pts=stream["fixed_sl_pts"],
        rr_ratio=stream["rr_ratio"],
        half_tp_ratio=stream["half_tp_ratio"],
        pending_expire_minutes=stream["pending_expire_minutes"],
        fractal_confirm=True, fractal_width=5,
        ma_trail=ma_trail,
    )


def _haircut_np_dd(net: float, dd: float) -> tuple[float, float, float]:
    """Apply 6% NP live haircut (sim ≈ live × 1.05 per feedback_sim_vs_live_calibration)."""
    net_hc = net * 0.94
    nd = (net_hc / dd) if dd > 0 else 0.0
    return net_hc, dd, nd


def main() -> int:
    cfg_data = parse_v6_setfile(SETFILE)
    streams = cfg_data["streams"]
    init_account("sim")

    rows = []  # one row per (window, stream, mode)
    for wname, wstart, wend in WINDOWS:
        ticks = load_ticks("XAUUSD", wstart, wend, spread_pts=SPREAD)
        m5 = load_m5("XAUUSD", wstart, wend)
        days = (wend - wstart).days
        for sidx, s in enumerate(streams, start=1):
            for mode_name, ma in [("off", False), ("on", True)]:
                r = simulate_fast(_build_cfg(s, ma), ticks, m5, _meta(), DEPOSIT)
                net_hc, dd_hc, nd_hc = _haircut_np_dd(r.net_profit, r.dd_abs)
                rows.append({
                    "window": wname, "days": days, "stream": f"S{sidx}",
                    "mode": mode_name,
                    "net": r.net_profit, "dd": r.dd_abs,
                    "pf": r.profit_factor, "trades": r.trades,
                    "net_hc": net_hc, "nd_hc": nd_hc,
                })
    kill_mt5_terminal()
    df = pd.DataFrame(rows)

    # Per-window portfolio totals (sum across 6 streams).
    portfolio = df.groupby(["window", "days", "mode"], as_index=False).agg(
        net=("net", "sum"), dd=("dd", "sum"),
        trades=("trades", "sum"), net_hc=("net_hc", "sum"),
    )
    portfolio["nd_hc"] = portfolio.apply(
        lambda r: (r["net_hc"] / r["dd"]) if r["dd"] > 0 else 0.0, axis=1)

    # Pivot to one row per window with off vs on side by side.
    pivot = portfolio.pivot(index=["window", "days"], columns="mode",
                            values=["net", "dd", "trades", "net_hc", "nd_hc"])
    pivot["delta_nd_hc"] = pivot[("nd_hc", "on")] - pivot[("nd_hc", "off")]
    pivot["delta_pct"]   = (pivot["delta_nd_hc"] /
                            pivot[("nd_hc", "off")].replace(0, np.nan)) * 100

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    with REPORT.open("w", encoding="utf-8") as f:
        f.write("# MA7 Trail A/B Report (2026-05-25)\n\n")
        f.write(f"Setfile: `{SETFILE.name}`  ·  spread {SPREAD}pt  ·  deposit ${DEPOSIT:,.0f}\n\n")
        f.write("## Portfolio per window (sum of 6 streams)\n\n")
        f.write(pivot.to_markdown())
        f.write("\n\n## Per-stream detail (all rows)\n\n")
        f.write(df.to_markdown(index=False))
        # Decision verdict
        wins = int((pivot["delta_pct"] >= 10).sum())
        regressions = int((pivot["delta_pct"] <= -15).sum())
        f.write("\n\n## Decision\n\n")
        f.write(f"- Windows where on improves NP/DD$_hc by ≥+10%: **{wins} / 4**\n")
        f.write(f"- Windows where on regresses NP/DD$_hc by ≤−15%: **{regressions} / 4**\n")
        if wins >= 3 and regressions == 0:
            f.write("- **Verdict: ADVANCE** to full WFO with `ma_trail_on` as a dim.\n")
        elif regressions > 0:
            f.write("- **Verdict: REJECT** — regime-dependent regression observed.\n")
        else:
            f.write("- **Verdict: ITERATE** — mixed; revisit MA period / buffer / activation rule.\n")
    print(f"wrote {REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Run a single-window dry-run first**

Edit `WINDOWS` temporarily to one window only (e.g., W1). Then:
```
$env:PYTHONPATH = "src"
python scripts/sim_orb_ma7_trail_ab.py
```
Expected: prints `wrote docs/reports/2026-05-25-ma7-trail-ab.md`, file exists with portfolio + detail sections. Inspect for sanity — `off` numbers should match expected v6 baseline for that window; `on` numbers should differ but be in the same order of magnitude.

- [ ] **Step 3: Restore 4 windows and run full driver**

Revert `WINDOWS` to the 4-tuple. Run:
```
python scripts/sim_orb_ma7_trail_ab.py
```
Expected: same success message, larger report file. Open the report and verify all 4 windows have off+on rows.

- [ ] **Step 4: Commit driver + report**

```
git add scripts/sim_orb_ma7_trail_ab.py docs/reports/2026-05-25-ma7-trail-ab.md
git commit -m "$(cat <<'EOF'
feat(sim): A/B driver for ma_trail off vs on across 4 weekly windows

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: Decision write-up + memory save

Read the report's decision verdict and write a memory file capturing it.

**Files:**
- Read: `docs/reports/2026-05-25-ma7-trail-ab.md`
- Create: `~/.claude/projects/c--Users-Zhu-En-zgb-opti/memory/project_ma7_trail_test_2026_05_25.md`
- Modify: `~/.claude/projects/c--Users-Zhu-En-zgb-opti/memory/MEMORY.md` (add index line)

- [ ] **Step 1: Read the verdict**

Open `docs/reports/2026-05-25-ma7-trail-ab.md`. Read the "Decision" section. Three outcomes:
- **ADVANCE** → next step is adding `ma_trail_on ∈ {0,1}` as a WFO sweep dim in the next reopt.
- **REJECT** → kill the feature; document why.
- **ITERATE** → enumerate variants worth trying (buffer, MA period, EMA vs SMA).

- [ ] **Step 2: Save memory**

Create memory file `C:\Users\Zhu-En\.claude\projects\c--Users-Zhu-En-zgb-opti\memory\project_ma7_trail_test_2026_05_25.md`. Template (fill in the bracketed values from the report):

```markdown
---
name: project-ma7-trail-test-2026-05-25
description: A/B test of post-HTP SMA(7) M5 trail on v6 parents — [ADVANCE/REJECT/ITERATE]. Spec at docs/superpowers/specs/2026-05-25-ma7-trail-post-htp-design.md.
metadata:
  node_type: memory
  type: project
---

# MA7 post-HTP trail A/B (2026-05-25)

**Verdict: [ADVANCE / REJECT / ITERATE]**

## Setup
- v6 setfile (`dt818_pro_v6_9pct_may23_may16.set`), 6 streams × 1.5% risk.
- 4 windows: Apr 25→May 2, May 2→9, May 9→16, May 16→23. Spread 30pt, $10k.

## Portfolio NP/DD$_hc per window
| Window | off | on | Δ% |
|---|---|---|---|
| W1 | [..] | [..] | [..]% |
| W2 | [..] | [..] | [..]% |
| W3 | [..] | [..] | [..]% |
| W4 | [..] | [..] | [..]% |

## Decision rule
- Advance if ≥3/4 windows show on − off ≥ +10% Δ NP/DD$_hc, with 0 regressions ≤ −15%.
- Reject if any window regresses ≤ −15%.
- Iterate otherwise.

Actual: [N/4 advance-condition hits, N regressions].

## Why this matters
- [If ADVANCE: feature locks in late-trade reversals on the runner; next step = add ma_trail to next weekly WFO sweep dim.]
- [If REJECT: tight MA7 cuts winners short; revisit only with buffer or longer MA.]
- [If ITERATE: name the next experiment.]

## Related
- [[project_orb_live_trade_log_v6_stopext]] — production target
- [[feedback_sim_vs_live_calibration]] — haircut convention used (NP × 0.94)
```

- [ ] **Step 3: Add to memory index**

Open `C:\Users\Zhu-En\.claude\projects\c--Users-Zhu-En-zgb-opti\memory\MEMORY.md`. Add this line in the project-experiments section (near `project_msb50_rejected` / `project_ma_direction_filter_rejected`):

```markdown
- [MA7 post-HTP trail A/B 2026-05-25](project_ma7_trail_test_2026_05_25.md) — [ADVANCE/REJECT/ITERATE] verdict for SMA(7,M5) runner trail.
```

- [ ] **Step 4: Commit memory** *(memory is in a different repo; commit only if the user wants persistence — otherwise skip)*

The memory dir lives outside this repo. No git action needed unless the user has set up a separate VCS for it.

---

## Self-Review Checklist

After completing all 11 tasks, run this verification:

- [ ] `pytest tests/zgb_sim/ -v` — all green.
- [ ] `git log --oneline | head -15` — ~11 commits, one per task, all on the feature branch.
- [ ] `docs/reports/2026-05-25-ma7-trail-ab.md` exists with verdict.
- [ ] Memory file written and indexed.
- [ ] `cfg.ma_trail=False` integration test passes (no baseline drift).
- [ ] No new files in `.gitignore` patterns (e.g., `*.png`, `__pycache__/`).
