# 3×3 WFO Spread Forward Test — multi-window validation (2026-05-27)

**Question:** Does WFO spread=30pt produce better live-forward picks than 60pt or 120pt, across multiple forward windows (not just the 4-day W_FWD3)?

**Answer:** Yes — spread30 wins aggregate NP by **54% over 60pt** and **115% over 120pt** across 3 independent forward windows (17 days), with **essentially identical DD %** (19.03% vs 18.97% vs 25.13%) and **1.5× better risk-adjusted (NDD)**.

## Test design

3 forward-OOS windows, each truly forward for both prev/latest WFO (neither saw the test data):

| Window | Test range | Days | WFO era tested |
|---|---|---|---|
| W_FWD1 | 2026-05-09 → 2026-05-16 | 7 | may9 |
| W_FWD2 | 2026-05-16 → 2026-05-23 | 7 | may16 |
| W_FWD3 | 2026-05-23 → 2026-05-27 | 4 | may23 |

For each era × spread ∈ {30, 60, 120}, ran a v2-fractal WFO via `scripts/sim_wfo_orb_v2_fractal_parametric.py` with the same 144-config grid. 9 WFOs total (6 new + 3 existing from prior may23 sub-test).

Forward portfolio simulation: v7 architecture (fractal_confirm + SMA(8,21) cross-exit + STOP-on-extension hedge), $10k base, 9% risk (1.5%/stream × 6), **30pt live-match test spread** (validated by per-hour live audit showing live spread is 21-22pt with max 29pt over 14 days).

## Per-window results

| Window | spread30 | spread60 | spread120 |
|---|---|---|---|
| **W_FWD1** (5-9 → 5-16) | NP $1,022 / DD $3,044 (23.8%) / NDD 0.34 / PF 1.09 | **NP $1,260** / DD $3,044 (23.5%) / NDD 0.41 / PF 1.11 | NP $334 / DD $3,817 (27.7%) / NDD 0.09 / PF 1.03 |
| **W_FWD2** (5-16 → 5-23) | **NP $4,744** / DD $2,217 (13.9%) / NDD 2.14 / PF 1.71 | NP $4,049 / DD $2,408 (15.6%) / NDD 1.68 / PF 1.61 | NP $2,902 / DD $2,389 (16.9%) / NDD 1.21 / PF 1.32 |
| **W_FWD3** (5-23 → 5-27) | **NP $3,814** / DD $2,682 (19.3%) / NDD 1.42 / PF 1.97 | NP $902 / DD $2,055 (17.9%) / NDD 0.44 / PF 1.25 | NP $1,213 / DD $3,571 (30.8%) / NDD 0.34 / PF 1.24 |

Window wins: **spread30 = 2, spread60 = 1, spread120 = 0**

## Aggregate (3 windows = ~18 days)

| Metric | spread30 | spread60 | spread120 | Winner |
|---|---|---|---|---|
| Total NP | **+$9,580** | +$6,211 | +$4,450 | spread30 |
| Total DD $ | $7,943 | **$7,507** | $9,777 | spread60 (marginal) |
| Mean DD % | **19.03%** | **18.97%** | 25.13% | tied 30/60 (0.06% diff) |
| Mean NDD | **1.30** | 0.84 | 0.55 | spread30 (1.5×) |
| Mean PF | **1.59** | 1.32 | 1.20 | spread30 |

## Live spread reality (per-hour audit, 14 days)

| Statistic | Value |
|---|---|
| Median spread | 21-22 pts (across all 24 hours) |
| Mean spread | 21.6 pts |
| p99 spread | 27 pts |
| Max spread | 29 pts |
| % ticks > 30pt | **0.00%** |

The 30pt WFO assumption is conservative — actual live is 21-22pt. The 60pt default is **~2.7× live reality**; 120pt is ~5.5× live reality. This confirms the structural argument: WFO selection should match the spread regime where the strategy will deploy.

## Conclusion

**Use spread=30pt for production WFO going forward.** Evidence:
1. Wins NP across all 3 forward windows on aggregate.
2. Wins risk-adjusted (NDD) by 1.5× over 60pt.
3. Loses DD% by only 0.06% to 60pt (essentially equal risk).
4. Wins 2 of 3 windows; loses 1 marginally by $238.
5. Live spread audit confirms 30pt is grounded in real-data (not a 4-day anomaly).

### Remaining caveats

- **3 windows = 17 days.** Still small sample. Confirm with another forward cycle after deploying.
- **Live execution friction (~110% scaled) is unchanged.** spread30's 54% sim improvement may shrink to ~25-30% live after friction.
- **All 3 windows on XAUUSD.** May not generalize to other symbols (each has its own spread regime).
- **Test architecture is v7.** spread30 picks at v6 or earlier might be slightly different.

## Methodology recommendation

For the next weekly v7 reopt:

```powershell
$env:ZGB_SPREAD_PTS_OVERRIDE = "30"
python scripts/sim_wfo_orb_v7_smacross.py
```

The `ZGB_SPREAD_PTS_OVERRIDE` env var was added to `src/zgb_sim/tick_loader.py` and forces all per-symbol spread defaults to the override value. Workers inherit the env var on spawn.

### Optional: bake into default

If after 2-3 more weekly reopts the spread30 advantage continues to hold, change `SYMBOL_DEFAULT_SPREAD_PTS["XAUUSD"] = 60` to `30` in tick_loader.py.

## Artifacts

- WFO outputs (9 total):
  - `output/wfo_orb_v2_may9_spread30/`, `_spread60/`, `_spread120/`
  - `output/wfo_orb_v2_may16_spread30/`, `_spread60/`, `_spread120/`
  - `output/wfo_orb_v2_may23_spread30/`, `output/wfo_orb_v2_may23/` (60pt), `output/wfo_orb_v2_may23_spread120/`
- Parametric WFO driver: `scripts/sim_wfo_orb_v2_fractal_parametric.py`
- Batch runner: `scripts/run_6_wfos_batch.py`
- 3×3 comparison driver: `scripts/compare_3x3_spread_forward.py`
- Live spread audit: `scripts/live_spread_distribution_2weeks.py`

## Related

- [[feedback_sim_vs_live_calibration]] — sim ≈ live × 1.05 (was 5-8% NP haircut; spread=30 makes this almost exact)
- [[feedback_default_test_conditions]] — 30pt for live-match analysis (now also: WFO selection)
- [[feedback_always_show_dd_pct]] — DD % shown alongside DD $ per user rule
- [[project_spread60_vs_spread120_wfo_2026_05_27]] — earlier 4-day test (W_FWD3 only); this report extends to 3 windows
- [[project_rotation_vs_norotation_2026_05_27]] — companion: validates no-rotation rule on same 3 windows
