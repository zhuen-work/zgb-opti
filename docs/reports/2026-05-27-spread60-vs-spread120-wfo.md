# may23 WFO at SPREAD=30/60/120pt — forward live-tick test (2026-05-27)

> **UPDATE 2026-05-27:** original report was 60 vs 120; expanded to add spread=30 (live-match) WFO. **Spread=30 picks dramatically outperform both 60 and 120 on forward live test** — 4× the NP, 3× the NP/DD$. See "3-WAY UPDATE" section at the bottom.

---

## Original report (60 vs 120 only)


**Question:** Does running the may23 WFO at a more conservative spread (120pt = 2× standard) produce winners that perform better on a live account?

**Method:**
1. Ran two parallel WFOs on identical grids (144 configs per window × 4 WINDOWS_MAY23 = 576 sims each), differing only in spread:
   - `output/wfo_orb_v2_may23/` — standard 60pt spread (existing)
   - `output/wfo_orb_v2_may23_spread120/` — 120pt spread (new, via `scripts/sim_wfo_orb_v2_fractal_spread120.py`)
2. Extracted top-6 from each.
3. Ran both setfiles on live XAUUSD.sc ticks for the forward window 2026-05-23 → 2026-05-27 using v7 architecture (fractal-confirm + SMA(8,21) cross-exit + STOP-on-extension hedge), $10k base, 9% risk, 30pt live-match spread.

## Top-6 picks per WFO

| Stream | **60pt WFO** | **120pt WFO** |
|---|---|---|
| S1 | R90 SL550 RR4.0 HTP0.4 | **R60** SL550 RR4.0 **HTP0.0** |
| S2 | R90 SL400 RR3.5 HTP0.4 | R60 SL400 RR3.5 HTP0.0 |
| S3 | R90 SL550 RR3.5 HTP0.4 | R60 SL550 RR3.5 HTP0.0 |
| S4 | R90 SL400 RR3.0 HTP0.4 | R60 SL400 **RR2.5** HTP0.0 |
| S5 | R90 SL400 RR2.5 HTP0.4 | R60 SL550 RR4.0 HTP0.4 |
| S6 | R90 SL550 RR2.5 HTP0.4 | R60 SL400 RR4.0 HTP0.4 |

**Pattern:** higher WFO spread (120pt) shifts winners to Range=60 + mostly HTP=0.0 — the ranker learned that with wider spread, partial-closing at HTP eats too much profit, and longer 90-min ranges create more setups that bleed to spread cost.

WFO rank quality:
- 60pt: top-6 all **prof_count=4/4**, NP/DD$ 2900–3500
- 120pt: top-6 all **prof_count=3/4**, NP/DD$ 600–2400 (lower because spread cost reduces all sims)

## Forward live-tick results (5-23 → 5-27, 4 days)

| Metric | 60pt-WFO | 120pt-WFO | Diff (120−60) |
|---|---|---|---|
| Net Profit | +$902 | **+$1,213** | **+$311 (+34.5%)** |
| Drawdown $ | $2,055 | $3,571 | +$1,517 |
| **NP/DD$** | **0.44** | 0.34 | **−0.10** |
| Trades | 90 | 72 | −18 |
| Wins | 42 | 28 | −14 |
| Streams positive | 5 of 6 | 3 of 6 | dispersion shift |

### Per-stream

| Stream | 60pt NP | 120pt NP |
|---|---|---|
| S1 | −$86 | −$392 |
| S2 | +$246 | **+$929** |
| S3 | +$37 | −$461 |
| S4 | +$128 | +$475 |
| S5 | +$12 | −$380 |
| S6 | +$565 | **+$1,042** |

## Read

**Split verdict:**
- 120pt WINS on **raw NP** by +$311 (+34.5%)
- 60pt WINS on **risk-adjusted (NP/DD$)** by 0.44 vs 0.34
- 120pt has **higher dispersion** — fewer trades, bigger swings (S2: +$929 vs $246; S6: +$1,042 vs $565), but also bigger losses (S1, S3, S5 all worse)
- 60pt has **smoother profile** — 5 of 6 streams positive vs only 3 of 6 for 120pt

**Why this makes sense:** the 120pt WFO selected picks robust to slippage (Range=60, HTP=0.0) — when slippage doesn't materialize on test ticks (live spread is actually 21–22pt), those picks become over-conservative. They generate fewer trades with bigger variance — slow-moving setups bet on bigger captures per trade.

**Conclusion:** 60pt WFO remains the better spread choice for production — the +34% NP gain from 120pt isn't worth the 74% DD spike. Risk-adjusted, 60pt is decisively better. The split-stream profile (5 of 6 positive for 60pt vs 3 of 6 for 120pt) also favors 60pt.

**Caveat:** 4-day window is short — signal is weak. A longer test could shift the verdict either way. But the structural finding (120pt picks have higher dispersion / lower NDD) is likely to persist.

## Practical takeaways

1. **Keep using 60pt for production WFO** — it's the better spread for risk-adjusted live performance.
2. **120pt picks have a specific niche:** if you want fewer-but-bigger trades for psychological reasons (less noise), the 120pt picks are an interesting alternate cohort. But you pay in drawdown.
3. **The HTP=0 + R=60 cluster** the 120pt WFO surfaces is genuinely different from the standard top-6. Worth knowing this cluster exists if you ever want to manually diversify or stress-test the production setfile.

## Artifacts

- 60pt WFO output: `output/wfo_orb_v2_may23/oos_rank.csv`
- 120pt WFO output: `output/wfo_orb_v2_may23_spread120/oos_rank.csv`
- 120pt WFO driver: `scripts/sim_wfo_orb_v2_fractal_spread120.py`
- Comparison driver: `scripts/compare_spread60_vs_spread120_wfo.py`
- Spread override mechanism: `ZGB_SPREAD_PTS_OVERRIDE` env var in `src/zgb_sim/tick_loader.py`

## Related

- [[feedback_sim_vs_live_calibration]] — Sim ≈ live × 1.05 (60pt is the standard WFO spread; 30pt is the live-match comparison spread)
- [[feedback_default_test_conditions]] — 30pt for live-match
- [[project_v7_smacross_2026_05_26]] — current production architecture used in this test

---

## 3-WAY UPDATE: spread=30/60/120 (2026-05-27)

Added a third WFO at SPREAD=30pt (matching live actual ~21-22pt spread). Methodology unchanged otherwise. Re-ran all 3 setfiles' top-6 on the same forward window.

### Updated 3-way results

| Metric | **spread30** | spread60 | spread120 | Winner |
|---|---|---|---|---|
| Net Profit | **+$3,814** | +$902 | +$1,213 | spread30 (4.2× / 3.1×) |
| Drawdown $ | $2,682 | $2,055 | $3,571 | spread60 |
| **NP/DD$** | **1.42** | 0.44 | 0.34 | **spread30 (3.2× / 4.2×)** |
| Profit Factor | **1.97** | 1.25 | 1.24 | spread30 |
| Streams positive | 5 of 6 | 5 of 6 (tiny mags) | 3 of 6 | spread30 |

### spread30 top-6 picks (hybrid R=60 and R=90)

| Stream | Params | WFO prof_count |
|---|---|---|
| S1 | R60 SL400 RR4.0 HTP0.0 | 4/4 |
| S2 | R60 SL400 RR2.5 HTP0.0 | 4/4 |
| S3 | R60 SL400 RR4.0 HTP0.4 | 4/4 |
| S4 | R60 SL400 RR3.5 HTP0.4 | 4/4 |
| S5 | R90 SL550 RR4.0 HTP0.4 | 4/4 |
| S6 | R90 SL400 RR4.0 HTP0.4 | 4/4 |

All 6 picks 4/4 profitable in WFO OOS (vs 60pt: 4/4, vs 120pt: only 3/4). Mix of R=60 and R=90.

### Per-stream live NP

| Stream | spread30 | spread60 | spread120 |
|---|---|---|---|
| S1 | **+$1,167** | −$86 | −$392 |
| S2 | +$475 | +$246 | +$929 |
| S3 | **+$1,042** | +$37 | −$461 |
| S4 | +$852 | +$128 | +$475 |
| S5 | −$86 | +$12 | −$380 |
| S6 | +$364 | +$565 | +$1,042 |

spread30 has the biggest single winners (S1 $1,167, S3 $1,042, S4 $852) plus the smallest loser.

### Updated conclusion

**The 60pt standard WFO spread is too conservative for current live conditions.** Live actual spread is **21-22pt** — running WFO at 30pt produces:
- 4× the live NP vs 60pt
- 3× the NP/DD$
- All 6 picks 4/4 profitable (vs 60pt: 4/4, 120pt: 3/4)

The "more conservative = more robust" intuition is **wrong on this regime**. Over-correcting for slippage that doesn't materialize filters out the best setups (R=60 short-range with HTP=0.0) because their aggregate spread cost dominates their small profits — at the inflated spread assumption.

### Methodology implication

For production WFO going forward, **use spread=30pt** (live-match) rather than the historical 60pt default. The 60pt default in `tick_loader.py` and `SYMBOL_DEFAULT_SPREAD_PTS["XAUUSD"]` was set when live spread was higher (per the inline comment "bumped 55→60 2026-05-15"). Live spread has dropped — WFO should follow.

**Caveat:** 4-day forward window is short. Confirm with a longer test before changing the default permanently. But the **3× NP/DD$ improvement** is strong enough that running v7 weekly reopts at spread=30 going forward is well-justified.

### Updated artifacts

- 30pt WFO output: `output/wfo_orb_v2_may23_spread30/oos_rank.csv`
- 30pt WFO driver: `scripts/sim_wfo_orb_v2_fractal_spread30.py`
- 3-way comparison driver: `scripts/compare_3way_spread_wfos.py`
