# ORB × Fractals Screening Test — Design

**Date:** 2026-05-23
**Author:** Zhu-En (with Claude)
**Status:** Design approved, awaiting implementation plan

## Goal

Test three independent ways to combine Bill Williams fractals with the existing ORB strategy, decide which (if any) advance to full WFO. Match the rejection workflow that killed MA-direction and ATR-SL filters.

## Variants (all default OFF, isolated config flags)

| ID | Field | Mechanism |
|---|---|---|
| V1 | `fractal_trail: bool = False` | After a parent fills, on every newly confirmed opposite-side fractal: for a BUY, set `SL = max(current_SL, down_fractal_price)`; for a SELL, set `SL = min(current_SL, up_fractal_price)`. SL therefore ratchets only in the profit direction, never adversely; it can move SL below or above entry depending on how far the fractal sits relative to entry. Existing HTP still applies independently. |
| V2 | `fractal_confirm: bool = False` | After the ORB range window closes, the pending stop order is NOT placed immediately. It arms only after a same-direction fractal confirms above (for BUY_STOP) / below (for SELL_STOP) the range break level. If no qualifying fractal confirms before `pending_expire_minutes`, the session is skipped. |
| V3 | `fractal_range: bool = False` | Range H/L is computed as the highest confirmed up-fractal and lowest confirmed down-fractal within the time window `[range_start, range_end)`. If fewer than one of each is confirmed, the session is skipped (no pending placed). Replaces bar-extreme H/L. |

Plus `fractal_width: int = 5` — bars on each side required (3 or 5). Sweep both per user direction.

## Fractal definition (canonical)

For width `w`, an up-fractal at bar index `i` requires:
- `high[i] > high[i-k]` for all `k ∈ {1..w//2}`
- `high[i] > high[i+k]` for all `k ∈ {1..w//2}`

For `w=5`: 2 bars each side, confirmed at `i+2` (2-bar lag).
For `w=3`: 1 bar each side, confirmed at `i+1` (1-bar lag).

Down-fractals mirrored on `low`. Computed on M5 bars (same TF as the ORB EA).

**No-peek guarantee:** A fractal at bar `i` is only usable from bar `i + w//2` onward. Implementation must take the fractal timestamp as `m5_bars["ts"].iloc[i + w//2]` (close time of confirming bar).

**V3 fractal-availability caveat:** Because confirmation lags by `w//2` bars, the last `w//2` bars of the range window can never host a usable fractal. For a 30-min range = 6 M5 bars with w=5, only the first 4 bars can produce a confirmed fractal. With w=3, the first 5 bars. This is a property of fractals, not a bug — but it tightens V3's effective range window. Sessions with no qualifying fractal of either side are skipped (logged as `skipped: no_fractal`).

## Configurations to run

7 configs × 6 streams = **42 sim runs**:

| Config | V1 | V2 | V3 | width |
|---|---|---|---|---|
| baseline | off | off | off | — |
| V1_w3 | on | off | off | 3 |
| V1_w5 | on | off | off | 5 |
| V2_w3 | off | on | off | 3 |
| V2_w5 | off | on | off | 5 |
| V3_w3 | off | off | on | 3 |
| V3_w5 | off | off | on | 5 |

Combined (all-on) variants explicitly excluded per scope decision.

## Test conditions

Per `feedback_default_test_conditions.md`:
- Window: 2026-02-14 → 2026-04-25 (~70 days)
- Per-stream risk: 1.0%
- Total portfolio risk: 6.0% (6 streams)
- Spread: 30 pt (sim default)
- Initial balance: $10,000
- Symbol: XAUUSD (sim variant per `reference_mt5_account_registry.md`)

Streams: current 6-stream rotation (S1..S6 from latest WFO winners).

## Decision protocol

**Phase 1 — Per-stream screen**
For each of 6 streams × 7 configs, run `simulate()` and record:
- NP, DD$, PF, trades, win_rate

**Phase 2 — Portfolio sim + live haircut** (mandatory per `feedback_portfolio_sim_after_rotation.md`)
For each of the 7 configs:
- Deal-merge all 6 streams
- Compute portfolio NP, DD$, PF, NP/DD$
- Apply live haircut: NP × 0.94, PF − 0.25
- Compare haircut-NP/DD$ to baseline-haircut-NP/DD$

**Phase 3 — Decision rule**
A variant advances to WFO iff portfolio haircut-NP/DD$ ≥ 1.10 × baseline haircut-NP/DD$.

- If 0 variants pass → write `project_orb_fractal_rejected.md` memory, delete config fields (no dead code per `feature_dev` guidance).
- If 1+ variants pass → leave fields in `ORBConfig`, propose WFO sweep for surviving variant(s) in a follow-up turn.

## Files

**Modified:**
- `src/zgb_sim/orb.py` — add 4 config fields (`fractal_trail`, `fractal_confirm`, `fractal_range`, `fractal_width`); add helper `_confirmed_fractals(m5_bars, width) -> dict[str, np.ndarray]` returning `{"up_ts": ts_array, "up_price": price_array, "dn_ts": ..., "dn_price": ...}`; integrate at 3 branch points (entry-arm gate, range computation, trail-on-fill update).
- `src/zgb_sim/orb_fast.py` — same helper + flag plumbing (orb_fast mirrors orb for perf-sensitive paths).

**Created:**
- `scripts/sim_orb_fractal_screen.py` — driver: loops over 7 configs × 6 streams, writes per-stream + portfolio CSVs. Template = `scripts/sim_orb_htp_ab.py`.
- `output/fractal_screen_2026_05_23/per_stream.csv` — rows: (stream, config, NP, DD$, PF, trades, WR)
- `output/fractal_screen_2026_05_23/portfolio.csv` — rows: (config, NP, NP_hc, DD$, PF, PF_hc, NP_DD_ratio_hc, advances_to_wfo)
- `output/fractal_screen_2026_05_23/summary.md` — human-readable comparison + decision per variant.

## Non-goals

- No EA-side changes. EA stays on current proven ORB logic; fractal mechanics live in sim only until a variant survives screen → WFO → portfolio gates.
- No WFO in this spec — gated behind screen decision.
- No fractal-based hedge logic — `reverse-hedge` smart-TP design is independent.
- No combined-all-on variant — per scope decision.
- No new TF — M5 only, matches EA bar TF.

## Why this shape

1. **Single screening script** is cheaper than 3 scripts and gives directly comparable tables.
2. **Isolated config flags** mean failed variants delete cleanly without leaving dead branches.
3. **Per-stream + portfolio + haircut + 10% threshold** matches the proven rejection workflow (MA-direction 8/8 rejected, ATR-SL 11/11 rejected, both via the same pipeline).
4. **Width sweep** in the screen rather than separately costs only 2× compute on a 70-day window and prevents a second-round "did we pick the wrong width" question.
