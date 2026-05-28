# SMA(3)×SMA(5) Cross-Exit on M5 (V3)

**Date:** 2026-05-25
**Status:** Approved for sim-only test.
**Predecessors:** [V1 ma7 trail](2026-05-25-ma7-trail-post-htp-design.md) (REJECTED), [V2 retrace-gate](2026-05-25-ma7-trail-retrace-gate-design.md) (REJECTED).
**Branch:** `feature/ma7-trail-post-htp` (extends same branch).

## Motivation

V1/V2 trail-based experiments REJECTED because a forward-only MA7 ratchet engages even on small intra-trade wiggles in trending weeks, cutting winners short. The MA7 trail can't distinguish "noise wiggle in a clean trend" from "actual reversal in chop" without lookahead.

A SMA(3)×SMA(5) cross is a discrete event-based reversal signal, not a continuous ratchet. In clean trends, the cross never fires (SMA3 stays on the trend side of SMA5 the whole way). In choppy/reversal regimes, the cross fires when momentum actually turns. This should preserve trend-week winners while still locking in profit on real reversals.

## Feature

After HTP partial close fires on a parent, the runner becomes monitored for an opposite-direction SMA(3)×SMA(5) cross on M5 closes. When a relevant cross is observed AND the runner is in profit, the runner closes at the current bid/ask (a discrete market-style exit, not a trail).

### Cross detection

On each closed M5 bar `i`, compute `sma3[i] = mean(close[i-2:i+1])` and `sma5[i] = mean(close[i-4:i+1])`. Define:

| Cross at bar `i` | Condition |
|---|---|
| Bearish (`-1`) | `sma3[i-1] >= sma5[i-1]` AND `sma3[i] < sma5[i]` |
| Bullish (`+1`) | `sma3[i-1] < sma5[i-1]` AND `sma3[i] >= sma5[i]` |
| None (`0`) | else |

First 5 bars: signal = 0 (insufficient data).

### Exit rule

For each post-HTP runner at every tick:
- If `m5_cross_signal` at the latest closed M5 bar ≤ `ts_ns` is **bearish** AND runner is **LONG** AND `unrealized_pnl > 0` → close at current `bid`.
- If signal is **bullish** AND runner is **SHORT** AND `unrealized_pnl > 0` → close at current `ask`.
- Cross signal is consumed once per runner (a single cross can't close a runner more than once).

Fixed RR-TP and entry SL remain active.

### Spec

| Dimension | Value |
|---|---|
| Cross signal | SMA(3) × SMA(5) on M5 close |
| Detection | At each M5 bar close |
| Exit type | Discrete market close at current bid/ask (`D_OTHER` deal kind) |
| Direction filter | Side-filtered (cross-direction must oppose runner direction) |
| Profit filter | Unrealized P&L > 0 at the moment of cross consumption |
| Scope | Parents S1–S6 only, **runners only**, **post-HTP only** |
| Hedges | Untouched |
| Mutually exclusive with | `ma_trail` (sim raises a clear error if both flags True) |
| Deployment | None. Sim only. |

## Sim test plan

### Implementation

1. New config field: `sma_cross_exit: bool = False`.
2. `simulate_fast` precomputes `m5_sma3`, `m5_sma5`, and a derived `m5_cross_signal: int8[N]` array.
3. JIT loop walks `m5_cross_signal` per runner (similar to V1's MA7 ratchet walking pattern), but instead of ratcheting SL, it triggers a discrete close.

### Windows (same as V1/V2)

| Window | Range | Days |
|---|---|---|
| W1 | 2026-04-25 → 2026-05-02 | 7 |
| W2 | 2026-05-02 → 2026-05-09 | 7 |
| W3 | 2026-05-09 → 2026-05-16 | 7 |
| W4 | 2026-05-16 → 2026-05-23 | 7 |

Spread: 30pt. Deposit: $10k. Risk: 1.5%/stream.

### A/B grid

2 modes (`off`, `on`) × 6 streams × 4 windows = 48 sims. ~5–8 min.

### Decision rule

Same as V1/V2:

- **ADVANCE** if ≥3/4 windows show Δ NP/DD$_hc ≥ +10% AND 0 regressions ≤ −15%.
- **REJECT** if any window regresses ≤ −15%.
- **ITERATE** otherwise.

## Non-goals

- No EA `.mq5` changes.
- No tuning of SMA periods (locked to 3, 5 per user spec).
- No hedge-side close-on-cross.
- No live deployment.

## Open questions

None.

## Risks

- **Late signal:** SMA(3,5) on M5 may fire after the move has already exhausted; could close at near-zero unrealized profit just before price recovers.
- **False crosses on consolidation:** crosses during sideways action may close winners prematurely.
- **Confirmation lag:** signal known only after M5 bar closes (so up to 5 min late).

## Key difference vs V1/V2

V1/V2 modified SL (ratchet toward MA7). V3 modifies position lifecycle (discrete close on cross). When V3 doesn't fire, the trade is unaffected — same NP/DD as off-baseline. This is the structural fix for the W1/W2 trend-week regression: in trends, the cross never fires, so the trade runs to RR-TP just like off-baseline.
