# V4: SMA Cross-Exit with Session-Level ATR Regime Gate

**Date:** 2026-05-25
**Status:** Approved for sim-only test.
**Predecessors:** V1 (REJECT), V2 (REJECT), V3 (REJECT by W1 only).
**Branch:** `feature/ma7-trail-post-htp` (continues).

## Motivation

V3 SMA(3)×(5) cross-exit was the first variant that genuinely shrank the trend-week regression (W2 escaped −15% threshold, W1 went from −41% to −27%). Only W1 prevented ADVANCE.

W1's regime is "clean directional trend with minimal M5 oscillation". The cross-exit fires anyway because even clean trends have occasional pullbacks that produce a SMA(3)×SMA(5) cross — and those crosses cut otherwise-winning trades.

A session-level regime gate that disables the cross-exit on low-volatility / clean-trend sessions should rescue W1 while preserving the W3/W4 wins (which are high-volatility / chop sessions).

## Feature

Per session, compute `atr_ratio = M5_ATR(14) / session_range_pts` at the session's range-end moment. If `atr_ratio > θ`, runners from that session have cross-exit enabled. Otherwise, cross-exit is disabled for that session (runner falls through to fixed RR-TP / SL like off-baseline).

### Logic

1. At session range-end (existing event in sim), compute M5-ATR(14): mean of true-range (high − low, but using close-to-close diff for simplicity in this sim) of the last 14 M5 bars whose close_ts ≤ session_range_end.
2. `atr_pts = atr_in_price / point` (convert to points like other distances in the sim).
3. `atr_ratio = atr_pts / range_pts`.
4. If `atr_ratio > sma_cross_atr_gate`, set per-session flag `sess_cross_enabled[si] = True`. Else False.
5. In the cross-exit block (V3 logic), add a predicate `and sess_cross_enabled[pos_session[i]]`.

### Spec

| Dimension | Value |
|---|---|
| ATR definition | M5 close-to-close abs diff, 14-bar rolling mean (simplified ATR, since we don't track high/low in JIT context for ATR purpose) |
| Lookback window | 14 M5 bars ending at session range-end |
| Threshold | New config field `sma_cross_atr_gate: float = 0.0` |
| `gate=0.0` | All sessions enabled (= V3 full behavior). Sanity check sweep value. |
| `gate=0.10..0.30` | Increasingly restrictive. Higher gate = fewer sessions enabled. |
| Scope | Same as V3: parents only, runners only, post-HTP only |
| Mutually exclusive with | `ma_trail` (existing guard) |
| Deployment | None. Sim only. |

## Sim test plan

### Sweep grid

For each window × 6 streams:
- mode `off`: baseline, no cross exit
- mode `g0.00`: cross exit on, gate=0.0 (sanity = V3 full)
- mode `g0.10`: gate=0.10
- mode `g0.15`: gate=0.15
- mode `g0.20`: gate=0.20
- mode `g0.25`: gate=0.25
- mode `g0.30`: gate=0.30

7 modes × 6 streams × 4 windows = 168 sims. ~15 min.

### Windows / setup

Same as V1/V2/V3.

### Decision rule

Pick the `gate` value (among 0.10–0.30) that:
1. Has ≥3 windows with Δ NP/DD$_hc ≥ +10% vs off-baseline, AND
2. Has 0 windows with regression ≤ −15%, AND
3. Has the highest mean Δ%.

If multiple pass criteria 1+2, pick by criterion 3.
If none pass, **REJECT V4** — confirms the trend-week barrier is structural, not solvable via session-level gating.

## Non-goals

- No EA `.mq5` changes.
- Not optimizing the ATR window (locked to 14).
- Not optimizing the SMA cross periods (locked to 3, 5).
- No live deployment.

## Risks

- **Sample size:** 4 windows × 7 modes is light. A winner here would need WFO confirmation.
- **ATR definition:** using close-to-close diff instead of true high-low is a simplification. May understate volatility on gap days. Acceptable for the first cut.
- **Session-level granularity:** sessions are LDN (07:00 UTC) and NY (13:00 UTC) on the v6 setfile. The gate could be too coarse — a session might shift regime mid-bars. Accept this limitation; if regime shifts within a session are dominant, a different mechanism is needed.

## Why this should work (per V1/V2/V3 evidence)

- W1 is so clean that even rare cross signals cost profit. With a high enough gate, W1 sessions get disabled → W1 reverts to off-baseline ≈ +0% Δ → no longer a regression.
- W3/W4 chop has high ATR/range ratios; sessions there stay enabled → wins preserved.
- W2 is borderline; depends on session-level mix.

If V4 fails too, the structural conclusion firms up: post-HTP early-exit features fundamentally don't add value on this strategy mix because trend weeks always cost more than chop weeks save.
