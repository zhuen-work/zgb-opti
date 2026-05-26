## Sweep-and-Reclaim Counter-Stream (SR_v1) — Design

**Date:** 2026-05-27
**Author:** Zhu-En (with Claude)
**Status:** Design approved, awaiting implementation plan

## Goal

Build a counter-trend stream that fades **failed ORB breakouts**: when price sweeps an ORB range edge and closes back inside on the same M5 bar, take the opposite side. Stage 1 is a feasibility gate run standalone (no ORB co-running) to determine whether the strategy produces positive haircut-NP/DD$ on its own. Match the rejection workflow that killed MA-direction, ATR-SL, MSB50, and the MA7-trail series.

## Background

- ORB catches breakouts; user asked for the counterpart that catches trend exhaustion and reversals.
- Prior reversal attempts on this instrument have struggled: FBO removed 2026-05-03; mean-reversion (Asia-fade, FBF) dead; ICTM_v1 on hold; MSB50_v1 rejected 2026-05-23.
- Common thread in past failures: weak structural confirmation (range-fade without sweep, RSI/oscillator divergence). SR_v1 anchors on the **sweep + same-bar close back inside** structural signal.

## Concept

Six counter-streams `SR_S1..SR_S6`, each paired to one ORB stream's range geometry (`range_start`, `range_end`, `range_high`, `range_low`, `pending_expire_minutes`, active window). Stage 1 runs the SR streams **standalone** (ORB OFF). If Stage 1 passes, Stage 2 scopes integration (alongside-half-risk vs replacement) — Stage 2 is out of scope for this spec.

## Setup detection

For each M5 bar `b` whose close time falls in `[range_end, range_end + pending_expire_minutes]`:

- **SELL setup:** `b.high > range_high` AND `b.close < range_high`
- **BUY setup:**  `b.low  < range_low`  AND `b.close > range_low`

Rules:
- First qualifying bar in the window arms the entry (no later re-arm even if a deeper sweep follows).
- If neither side qualifies in the window, session is skipped (logged `skipped: no_sweep`).
- If the armed entry fails to fill before window close, it expires (logged `expired: no_fill`).
- If the entry fills and then SLs, no second attempt that session.
- Only one armed entry per session per stream.
- **Dual-sweep bar** (rare: bar.high > range_high AND bar.low < range_low AND range_low < bar.close < range_high) → skip (logged `skipped: dual_sweep`). The directional signal is ambiguous; not worth a tiebreak heuristic at the screen stage.

## Entry, SL, TP

Two entry-style variants (test both):

| Variant | Entry order (SELL side; mirror for BUY) |
|---|---|
| `V_stop`  | `SELL_STOP`  at `sweep_bar.low − buffer_pts` |
| `V_limit` | `SELL_LIMIT` at `range_high` |

Exits (both variants, SELL side; mirror for BUY):

- **SL** = `sweep_bar.high + buffer_pts`
- **TP** = `range_low`

`buffer_pts = 0` for the screen (sweep depth itself is the natural buffer). Position sizing **1.0% risk per setup**, lot size derived from `(SL distance in pts) × point_value` matching the ORB sim convention.

### Edge-case skips

Require `entry_price > TP` for SELL (`entry_price < TP` for BUY). Concretely:

- **V_stop SELL:** skip if `sweep_bar.low − buffer_pts ≤ range_low` (the sweep wick already reached or exceeded TP — no room to profit). Logged `skipped: no_rr`.
- **V_stop BUY:** mirror — skip if `sweep_bar.high + buffer_pts ≥ range_high`.
- **V_limit:** entry sits at the range edge by definition, so `entry > TP` always holds. No additional check.

SL feasibility (SL beyond entry in the loss direction) is automatically satisfied because `sweep_bar.high ≥ sweep_bar.low` and `sweep_bar.high > range_high` by the sweep condition; analogous for BUY.

For `V_limit`: if next-bar open already breaches the range edge (price never re-tests `range_high`/`range_low`), the limit simply doesn't fill and expires at window close — standard MT5 limit behavior, no special handling needed.

## Stage 1 — Test matrix

| Config | Description |
|---|---|
| `baseline` | Existing 6-stream ORB portfolio (current top-6 from latest WFO) — reference number |
| `SR_stop`  | 6-stream SR portfolio, V_stop entry, ORB OFF |
| `SR_limit` | 6-stream SR portfolio, V_limit entry, ORB OFF |

3 configs total. SR streams reuse the parameter set of their paired ORB stream for range geometry (range_start, range_end, pending_expire_minutes) — no separate range-window sweep at this stage.

### Test conditions

Per `feedback_default_test_conditions.md`:

- Window: 2026-02-14 → 2026-04-25 (~70 days)
- Per-stream risk: 1.0%
- Total portfolio risk: 6.0% (6 streams)
- Spread: 30 pt (sim default)
- Initial balance: $10,000
- Symbol: XAUUSD (sim variant per `reference_mt5_account_registry.md`)

### Per-config artifacts

**Per stream (SR_S1..SR_S6 × 2 configs = 12 rows):**

- NP, DD$, PF, trades, win_rate
- **skip-rate** (sessions skipped because no qualifying sweep) — diagnoses whether the setup is rare or common
- **expire-rate** (armed entries that expired without filling) — diagnoses entry-style fitness (especially for `V_limit`)

**Portfolio (deal-merged across 6 SR streams):**

- NP, DD$, PF, NP/DD$
- Live haircut: NP × 0.94, PF − 0.25
- haircut-NP/DD$

**Diagnostic — correlation matrix:**

- Weekly-NP correlation `SR_Sn ↔ ORB_Sn` for each pair (6 values)
- Mean pair correlation and per-pair table
- Informs Stage 2 even if Stage 1 only just passes

## Decision rule

| Outcome | Action |
|---|---|
| Both configs: haircut-NP ≤ 0 **OR** haircut-NP/DD$ < 0.5 | **REJECT** — write `project_sweep_reclaim_v1_rejected.md` memory, delete config flags / sim helper. No Stage 2. |
| At least one config: haircut-NP > 0 **AND** haircut-NP/DD$ ≥ 0.5 **AND** mean SR↔ORB pair correlation ≤ +0.30 | **PASS** — promote winning config to Stage 2 design (alongside-half-risk vs replacement, plus WFO param sweep on `buffer_pts` and optional `min_sweep_depth_pts` filter). |
| Positive NP/DD$ but mean pair correlation > +0.30 | **PARTIAL** — log results in a project memory, no Stage 2 (insufficient diversification value to justify added complexity). |

### Threshold rationale

- **0.5 haircut-NP/DD$:** soft pass for an untuned strategy. ORB at default sits around 1.0 NP/DD$. A counter-stream that fails to clear 0.5 standalone has no realistic path to improving the portfolio in Stage 2.
- **Correlation ≤ +0.30:** SR is structurally an inverse of ORB on shared sessions. If realized correlation is high-positive, the "diversifier" thesis is wrong and there's no portfolio reason to add it even if standalone NP is positive.
- The 1.10× baseline-haircut-NP/DD$ rule (used by the fractal screen) is **not** Stage 1's gate because Stage 1 doesn't add SR to the ORB portfolio — that's Stage 2's question.

## Implementation surface

New code (planned, not part of this spec):

- `scripts/sim_orb_sweep_reclaim_screen.py` — driver script running the 3 configs end-to-end. Mirrors structure of `scripts/sim_orb_fractal_screen.py`.
- Sim-helper config fields (default OFF):
  - `sweep_reclaim_mode: Optional[str] = None` — one of `"stop"`, `"limit"`, or unset (no SR).
  - `sweep_buffer_pts: int = 0`.
- Helper function in the existing ORB sim module: `_sweep_setup_for_bar(bar, range_high, range_low) -> Optional[SetupSide]` returning `"SELL"`, `"BUY"`, or `None`.
- Entry/SL/TP construction reuses ORB's existing fill-modeling code; only the order placement and price levels differ.

No EA changes in this spec. EA wiring is Stage 2+ only if Stage 1 passes.

## Out of scope

- Buffer / min-sweep-depth parameter sweeps (deferred to Stage 2 / WFO).
- Stage 2 integration design (alongside vs replace, risk-split).
- Live deployment, EA changes, setfile generation, `D:\` mirroring.
- Combined V_stop + V_limit variants.

## References

- `feedback_default_test_conditions.md` — test window, spread, risk per stream
- `feedback_portfolio_sim_after_rotation.md` — haircut rule (NP × 0.94, PF − 0.25)
- `feedback_show_all_stream_params.md` — output report must enumerate all 6 streams
- `project_fbo_removed_2026_05_03.md`, `project_msb50_rejected.md`, `project_mean_reversion_dead_on_gold.md` — prior reversal-attempt history
- `docs/superpowers/specs/2026-05-23-orb-fractal-screen-design.md` — structural template for screen → decision-rule workflow
