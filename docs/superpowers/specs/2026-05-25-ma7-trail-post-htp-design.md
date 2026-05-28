# MA7 Trailing Stop (Post-HTP) — Design Spec

**Date:** 2026-05-25
**Status:** Approved for sim-only test
**Author:** brainstorm session (Zhu-En + Claude)
**Related:** [DT818_pro_v6 live trade log](../../../memory/project_orb_live_trade_log_v6_stopext.md), [orb_fast V1 fractal-trail](../../../src/zgb_sim/orb_fast.py)

## Motivation

v6 parents exit via fixed SL + RR-based TP, with HTP closing 20–40% of the lot at a partial-TP distance. On reversal scenarios — like the M5 dive-then-bounce on XAUUSD.sc 2026-05-25 14:25–14:45 — the remaining post-HTP lot rides the original fixed TP and gives back unrealized profit when price snaps back to entry.

A reactive trail on the remaining lot would lock in more of those mid-trade excursions.

## Feature

Add an MA(7)-on-M5 trailing stop that activates once the parent's HTP partial close has fired. From that point, the remaining lot's SL ratchets toward the SMA(7, close) value of M5 bars and only moves in the direction of profit.

### Trail rule

On every closed M5 bar, compute `sma7 = mean(close[-7:])`.

- **Long:** if `sma7 > current_SL`, set `current_SL = sma7`. Never lower.
- **Short:** if `sma7 < current_SL`, set `current_SL = sma7`. Never raise.

Fixed RR-TP remains active as an upper exit. Entry SL is *replaced* once the trail starts ratcheting past it (which only happens after HTP — so disaster-stop coverage is intact during the pre-HTP phase).

### Spec

| Dimension | Value |
|---|---|
| MA type | Simple (SMA) |
| Period | 7 |
| Source | Close |
| Timeframe | M5 |
| Buffer | 0pt (pure MA7 value) |
| Activation | After HTP partial close fires on that parent |
| Scope | Parents S1–S6 only |
| Hedges | Untouched — 8111–8666 keep existing STOP-ext SL/TP |
| Deployment | None. Sim only. |

## Sim test plan

### Implementation

1. Extend `src/zgb_sim/orb_fast.py` with an `ma_trail` mode flag (sits next to the existing `fractal_trail` infrastructure at `orb_fast.py:395-422`).
2. New driver script `scripts/sim_orb_ma7_trail_ab.py` running v6 setfile (`configs/sets/dt818_pro_v6_9pct_may23_may16.set`) twice — `ma_trail=off` (control) and `ma_trail=on` (treatment) — across 4 weekly OOS windows.

### Windows

| Window | Range | Days |
|---|---|---|
| W1 | 2026-04-25 → 2026-05-02 | 7 |
| W2 | 2026-05-02 → 2026-05-09 | 7 |
| W3 | 2026-05-09 → 2026-05-16 | 7 |
| W4 | 2026-05-16 → 2026-05-23 | 7 |

Spread: 30pt sim default. Deposit: $10k. Risk: per-setfile (1.5% × 6 = 9% total).

### Output

Per-window table, per-stream + portfolio totals:

| Stream | Days | NP_off | DD_off | PF_off | Trades_off | NP/DD$_off_hc | NP_on | DD_on | PF_on | Trades_on | NP/DD$_on_hc | Δ NP/DD$_hc |

Live haircut applied (NP × 0.94, PF − 0.25) per [feedback_sim_vs_live_calibration](../../../memory/feedback_sim_vs_live_calibration.md).

Aggregate summary row: count of windows where `NP/DD$_on_hc - NP/DD$_off_hc > 0` (out of 4).

### Decision rule

- **Advance** to full WFO integration (adding `ma_trail_on ∈ {0,1}` as a sweep dim in the next weekly reopt) **if** portfolio NP/DD$_hc improves by **≥ +10%** in **≥ 3 of 4** windows.
- **Reject** if any single window regresses **≤ −15%** in portfolio NP/DD$_hc (sign of regime-dependent breakage).
- **Iterate** (revisit MA period / buffer / activation rules) if neither condition triggers — mixed signal.

## Non-goals

- No EA `.mq5` changes in this iteration.
- No tuning of MA period or buffer in this iteration — that's deferred to the WFO step if the binary test passes.
- No hedge-side trailing — keeps STOP-ext hedge mechanism as a clean separate experiment.
- No live deployment. Sim only.

## Open questions

None at design time. (MA period / buffer optimization is deliberately deferred per the decision rule above.)

## Risks

- **Regime sensitivity:** MA7 on M5 is tight. Could be excellent in trending regimes and pessimal in chop. The 4-window test should expose this if it's pathological.
- **HTP interaction:** If HTP rarely fires on some streams (e.g., S5 has RR=2.0 which means HTP at 0.4×TP=320pt is reached often), MA7 will rarely engage on others. Per-stream activation rates need to be reported alongside NP deltas.
- **Sim-vs-live friction:** MA7 reads "last closed M5 bar." Live EA execution latency on Vantage could shift effective SL placement. Friction observation deferred until/if we advance to live.

## Implementation order

1. Add `ma_trail` mode to `orb_fast.py` (with off-by-default flag).
2. Write `scripts/sim_orb_ma7_trail_ab.py` driver.
3. Run 4 windows × 2 modes (8 sim runs total).
4. Produce comparison table + decision-rule verdict.
5. Memory: save findings as `project_ma7_trail_test_2026_05_25.md` regardless of outcome (reject memory if rejected, per `project_ma_direction_filter_rejected` precedent).
