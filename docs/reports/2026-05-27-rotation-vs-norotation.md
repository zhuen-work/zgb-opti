# Rotation vs No-Rotation — Forward Test on Live Ticks (2026-05-27)

**Question:** Is *top-6 from latest WFO only* (no-rotation) better than *top-3 from prev-week WFO + top-3 from current-week WFO* (rotation) on a live account?

**Method:** 3 clean forward-OOS windows where neither setfile-construction strategy peeked at the test data. Architecture held constant at v7 (fractal-confirm + SMA(8,21) cross-exit + STOP-on-extension hedge). Portfolio deal-merged across 6 streams on shared $10k @ 1.5%/stream (9% total) with 30pt spread, live XAUUSD.sc ticks.

**Driver:** `scripts/compare_rotation_vs_norotation.py`

## Forward windows tested

| Window | Date range | Days | "prev" WFO | "latest" WFO |
|---|---|---|---|---|
| W_FWD1 | 2026-05-09 → 2026-05-16 | 7 | may2 | may9 |
| W_FWD2 | 2026-05-16 → 2026-05-23 | 7 | may9 | may16 |
| W_FWD3 | 2026-05-23 → 2026-05-27 | 4 | may16 | may23 |

Each WFO's training/OOS ended at the date in its name, so testing on dates AFTER that name is true forward.

## Headline results

| Window | NO-ROT NP | ROT NP | Δ (rot − norot) | NO-ROT NDD | ROT NDD | Winner |
|---|---|---|---|---|---|---|
| W_FWD1 | +$2,202 | +$2,284 | **+$82** | 0.74 | 0.91 | ROT (marginal) |
| W_FWD2 | +$3,153 | +$3,231 | **+$78** | 1.44 | 1.47 | ROT (marginal) |
| W_FWD3 | +$1,500 | +$346 | **−$1,154** | 0.73 | 0.14 | **NO-ROT (decisive)** |
| **Total** | **+$6,855** | **+$5,861** | **−$994** | — | — | **NO-ROT** |

- **Window count:** ROT wins 2, NO-ROT wins 1
- **Aggregate NP:** NO-ROT wins by **$994** over 18 days
- **Magnitude:** ROT wins are ~$80 each; NO-ROT's single win is $1,154

## Why W_FWD3 was decisive

W_FWD1 and W_FWD2 were normal-regime weeks. W_FWD3 (2026-05-23 onwards) was the **catastrophic whipsaw regime** — the same period where the live account took −$92k in 1 week.

In W_FWD3:
- **may16 picks** (3 of 6 in ROT setfile): favored RR=3.5/4.0 with **HTP=0.2** — optimized on a trending regime that no longer applied
- **may23 picks** (all 6 in NO-ROT, 3 of 6 in ROT): favored **HTP=0.4** with RR 2.5-4.0 — adapted to choppier recent data

When the regime hit chop, **may23 picks adapted better** because their training had the most recent regime data. The may16 picks were stale.

This is the **regime-adaptation argument for no-rotation**: the latest WFO is regime-current; mixing in older picks dilutes that adaptation.

## Setfile composition (W_FWD3, the decisive window)

### NO-ROT (top-6 from may23 only)

| Stream | Range | SL | RR | HTP | Exp | Source |
|---|---|---|---|---|---|---|
| S1 | 90 | 550 | 4.0 | 0.4 | 240 | may23 r#1 |
| S2 | 90 | 400 | 3.5 | 0.4 | 240 | may23 r#2 |
| S3 | 90 | 550 | 3.5 | 0.4 | 240 | may23 r#3 |
| S4 | 90 | 400 | 3.0 | 0.4 | 240 | may23 r#4 |
| S5 | 90 | 400 | 2.5 | 0.4 | 240 | may23 r#5 |
| S6 | 90 | 550 | 2.5 | 0.4 | 240 | may23 r#6 |

All HTP=0.4. RR spread 2.5-4.0. Recent regime-adapted.

### ROT (may16 top-3 + may23 top-3)

| Stream | Range | SL | RR | HTP | Exp | Source |
|---|---|---|---|---|---|---|
| S1 | 90 | 550 | 3.5 | **0.2** | 240 | may16 r#1 |
| S2 | 90 | 550 | 3.5 | 0.4 | 240 | may16 r#2 |
| S3 | 90 | 550 | 4.0 | 0.4 | 240 | may16 r#3 |
| S4 | 90 | 550 | 4.0 | 0.4 | 240 | may23 r#1 |
| S5 | 90 | 400 | 3.5 | 0.4 | 240 | may23 r#2 |
| S6 | 90 | 550 | 3.5 | 0.4 | 240 | may23 r#3 |

S1 has HTP=0.2 (tighter partial close = bigger remaining lot when SL'd in chop). S3/S4 have RR=4.0 (trade has to travel further to TP — more likely to SL in chop).

## Conclusion

**On a live account, NO-ROTATION is better.** Across 18 days of clean forward testing on live ticks:
- NO-ROT total: +$6,855
- ROT total: +$5,861
- Aggregate delta: **−$994 for ROT**

The existing `feedback_no_rotation_use_top6` rule is **validated** by this test.

### Nuance

ROT marginally outperformed in stable regimes (W_FWD1, W_FWD2 each +$80). Its diversity provides tiny risk reduction when the market behaves normally. But in stressful regime-shift weeks (W_FWD3), no-rotation reacts to the recent shift and protects better.

If you ever consider rotation again, it would only be defensible during clearly calm regimes — and even then, the upside is ~$80/week vs ~$1,150 downside in shift weeks. Asymmetric reward: stay with NO-ROT.

## Artifacts

- Driver: `scripts/compare_rotation_vs_norotation.py`
- Per-WFO parquet sources: `output/wfo_orb_may{2,9,16}/`, `output/wfo_orb_v2_may23/`
- Architecture used: v7 (fractal_confirm=True width=5, sma_cross_exit=True (8,21), STOP-ext hedge ExtPts=100 TPMult=3.0 SLMult=1.0)

## Related memory

- [[feedback_no_rotation_use_top6]] — the rule this test validates
- [[project_v3_v7_comparison_2026_05_27]] — companion analysis showing v7 architecture wins
- [[project_v7_smacross_2026_05_26]] — v7 deployment
