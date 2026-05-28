# MA7 Trail — Retrace-from-HWM Gate (V2)

**Date:** 2026-05-25
**Status:** Approved follow-up to [2026-05-25-ma7-trail-post-htp-design.md](2026-05-25-ma7-trail-post-htp-design.md) (REJECTED in binary form).
**Branch:** `feature/ma7-trail-post-htp` (extends V1 work on same branch).

## Motivation

V1 (unconditional MA7 trail after HTP) rejected because it cut directional winners short in trending weeks (W1: −41%, W2: −16%). Same V1 helped in choppy/reversal weeks (W3 turned per-stream −$123 → +$580; W4 halved DD and doubled NP).

The failure mode is specific: in clean trends, price barely retraces from its high-water mark of unrealized profit, so MA7 (which sits ~30–80pt below recent closes) cuts the trade early on any tiny pullback. In chop, price retraces meaningfully from HWM before the trail engages — which is when MA7 catches the reversal.

A gate that **only arms the trail after price has retraced ≥ X% from HWM** should preserve the W3/W4 wins and remove the W1/W2 regressions.

## Feature

Add an HWM-retrace gate that delays MA7 trail activation until the runner has given back at least `retrace_pct` of its peak unrealized profit since HTP fired.

### Logic

Per runner with `pos_htp_fired = True`:

1. Track `hwm_profit = max(hwm_profit, current_unrealized_pnl)` every tick after HTP.
2. While `not ma7_armed`: if `current_unrealized_pnl <= hwm_profit × (1 - retrace_pct)`, set `ma7_armed = True`. Once armed, stay armed.
3. MA7 ratchet block (Task 7 of V1) uses `pos_ma7_armed` as predicate instead of `pos_htp_fired`.

When `retrace_pct == 0.0`, behavior is identical to V1 (arms immediately on HTP fire). This preserves the `ma_trail=True, retrace_pct=0.0` configuration as the V1 control case in a sweep.

### Spec

| Dimension | Value |
|---|---|
| New config field | `ma_trail_retrace_pct: float = 0.0` (additive; default = V1 behavior) |
| Default for `retrace_pct=0.0` | Immediate arm on HTP fire (= V1) |
| Default for `retrace_pct=0.25` | Arm only after 25% giveback from HWM |
| HWM start time | First tick after HTP fires |
| HWM unit | Unrealized P&L in $ (per-position, using current bid/ask + lots + tick_value) |
| State carried | `pos_hwm_profit` (float64), `pos_ma7_armed` (bool) |
| Scope | All 6 parents (S1–S6), runners only |
| Hedges | Still untouched |
| Deployment | None. Sim only. |

## Sim test plan

### Sweep grid

For each of 4 weekly OOS windows × 6 streams, run with:
- `ma_trail=False` (baseline, no trail)
- `ma_trail=True, retrace_pct=0.0` (V1 control — should reproduce V1 numbers)
- `ma_trail=True, retrace_pct=0.20`
- `ma_trail=True, retrace_pct=0.30`
- `ma_trail=True, retrace_pct=0.40`

5 modes × 6 streams × 4 windows = 120 sims. ETA ~15 min.

### Windows (same as V1)

| Window | Range | Days |
|---|---|---|
| W1 | 2026-04-25 → 2026-05-02 | 7 |
| W2 | 2026-05-02 → 2026-05-09 | 7 |
| W3 | 2026-05-09 → 2026-05-16 | 7 |
| W4 | 2026-05-16 → 2026-05-23 | 7 |

Spread: 30pt. Deposit: $10k. Risk: 1.5%/stream.

### Output

Per-window portfolio table (one row per mode) with NP, DD, NP_hc, NP/DD$_hc, Δ vs off-baseline.
Per-stream detail table (Days column included per repo convention).

### Decision rule

Pick the `retrace_pct` value (out of 0.20 / 0.30 / 0.40) that wins on:

1. **Mean Δ NP/DD$_hc vs baseline** across 4 windows is highest, AND
2. No single window regresses ≤ −15% vs baseline, AND
3. At least 3 of 4 windows show Δ ≥ +10% vs baseline.

If no value meets all three, **REJECT V2 too** (document as "binary feature insufficient — needs a different signal").
If one value wins, **ADVANCE to full WFO** with `(ma_trail, retrace_pct)` as a joint dim in next weekly reopt.

## Non-goals

- No EA `.mq5` changes (still sim-only).
- No tuning of MA period or buffer.
- No hedge-side trailing.
- No live deployment.

## Open questions

None. (Sweep covers the parameter space.)

## Risks

- **Sample size:** 4 windows is light. If a parameter wins by chance on this set, full WFO will catch it.
- **HWM-from-HTP definition:** Choosing "HWM since HTP fired" rather than "HWM since entry" is the design call. If pre-HTP excursion was already large but never triggered HTP, the choice doesn't matter (HWM tracking hasn't started). If HTP fires near the top of an excursion, HWM starts high — that's the intended behavior.
- **`retrace_pct=0.0` reproducibility:** With `retrace_pct=0.0` the gate must arm on the first tick after HTP-fired (i.e., when `current_unrealized >= hwm × (1-0)`, which is trivially true). This must produce numbers identical to V1's `ma_trail=True` results. The sweep includes this as a self-consistency check.
