"""Generate v7 HYBRID setfile from may23 WFOs:
  S1-S3 = top-3 from spread=30pt WFO  (live-match assumption, hypothesis being tested)
  S4-S6 = top-3 from spread=60pt WFO  (historical conservative default, control)

Why hybrid: per 3-window forward test 2026-05-27, spread30 picks aggregate to
+$9,580 vs +$6,211 for spread60 across 17 days (54% better NP, 1.5× NDD,
0.06% DD% diff). But this is sim — real-account validation needed before
fully switching to spread30. Hybrid splits the bet so live trade-level data
will reveal which cohort is genuinely better.

Each stream's _Comment field encodes the cohort so live_check + journals can
attribute P&L per cohort:
  ORB_S1_v7sp30_may23 ... ORB_S3_v7sp30_may23
  ORB_S4_v7sp60_may23 ... ORB_S6_v7sp60_may23

Writes:
  configs/sets/dt818_pro_v7_9pct_may30_may23_hybrid.set
  configs/sets/dt818_pro_v7_6pct_may30_may23_hybrid.set
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_V7 = ROOT / "configs/sets/dt818_pro_v7_9pct_may30_may23.set"
DIR_SP30 = ROOT / "output/wfo_orb_v2_may23_spread30"
DIR_SP60 = ROOT / "output/wfo_orb_v2_may23"  # existing 60pt at the no-suffix path


def _pick_rank_csv(d: Path, use_hedged: bool) -> Path:
    """Return hedged rank if requested AND present, else parent-only rank.
    Default is parent-only because hedge rescoring collapses sp30/sp60 cohort
    distinction (picks converge on R=90/HTP=0.4 cluster regardless of spread)
    — the hybrid's cohort-diversification rationale dies and S1==S4 / S2==S5
    duplicates appear. Pass --use-hedged explicitly to override.
    """
    if use_hedged:
        hedged = d / "oos_rank_hedged.csv"
        if hedged.exists():
            return hedged
        print(f"[WARN] --use-hedged set but no oos_rank_hedged.csv in {d.name}; using parent rank")
    return d / "oos_rank.csv"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--use-hedged", action="store_true",
                     help="Use oos_rank_hedged.csv (parent+hedge composite) instead of parent-only rank. "
                          "WARNING: this collapses sp30/sp60 cohort distinction on may23.")
    args = ap.parse_args()

    if not TEMPLATE_V7.exists():
        print(f"missing template: {TEMPLATE_V7}")
        return 1
    template = TEMPLATE_V7.read_text(encoding="utf-8")

    rank_sp30 = _pick_rank_csv(DIR_SP30, args.use_hedged)
    rank_sp60 = _pick_rank_csv(DIR_SP60, args.use_hedged)
    print(f"[SOURCE] sp30 picks  <- {rank_sp30.relative_to(ROOT)}")
    print(f"[SOURCE] sp60 picks  <- {rank_sp60.relative_to(ROOT)}")
    sp30 = pd.read_csv(rank_sp30).head(3)
    sp60 = pd.read_csv(rank_sp60).head(3)
    print(f"spread30 top-3 (for S1-S3):")
    for _, r in sp30.iterrows():
        print(f"  rank#{int(r['rank'])} R{int(r['range_minutes'])} SL{int(r['fixed_sl_pts'])} "
              f"RR{r['rr_ratio']} HTP{r['half_tp_ratio']} Exp{int(r['pending_expire_minutes'])}  "
              f"NP/DD${float(r['np_dd_ratio']):.0f}  prof={int(r['prof_count'])}/4")
    print(f"spread60 top-3 (for S4-S6):")
    for _, r in sp60.iterrows():
        print(f"  rank#{int(r['rank'])} R{int(r['range_minutes'])} SL{int(r['fixed_sl_pts'])} "
              f"RR{r['rr_ratio']} HTP{r['half_tp_ratio']} Exp{int(r['pending_expire_minutes'])}  "
              f"NP/DD${float(r['np_dd_ratio']):.0f}  prof={int(r['prof_count'])}/4")

    # Build hybrid: 6 rows = sp30[0..3] + sp60[0..3]
    hybrid = pd.concat([sp30.iloc[:3].assign(_cohort="sp30"),
                         sp60.iloc[:3].assign(_cohort="sp60")], ignore_index=True)

    for risk_per_stream, total_pct in [(1.5, 9), (1.0, 6)]:
        out_name = f"dt818_pro_v7_{total_pct}pct_may30_may23_hybrid.set"
        out_path = ROOT / "configs/sets" / out_name
        text = template

        # Replace the header block
        new_header = f"""; DT818_pro_v7 HYBRID - 6-stream ORB rank portfolio + 6 STOP-ext hedges + SMA(8,21) cross-exit
; Generated 2026-05-28 — HYBRID setfile combining two WFO spread assumptions:
;   S1-S3 = top-3 from may23 WFO at SPREAD=30pt (live-match assumption)
;     output/wfo_orb_v2_may23_spread30/oos_rank.csv
;   S4-S6 = top-3 from may23 WFO at SPREAD=60pt (historical conservative default)
;     output/wfo_orb_v2_may23/oos_rank.csv  (no _spread suffix = 60pt default)
;
; Rationale: 3-window forward test (2026-05-27) showed spread30 picks aggregate
; to +54% NP vs spread60 at ~identical DD% across 17 days. But sim != live.
; This hybrid runs BOTH cohorts in production so live trade-level data reveals
; which is genuinely better. After 2-4 weeks of live data, switch fully to the
; winning cohort.
;
; Cohort tracking — each stream's _Comment encodes the cohort:
;   S1-S3: ORB_S{{N}}_v7sp30_may23  (spread=30 cohort)
;   S4-S6: ORB_S{{N}}_v7sp60_may23  (spread=60 cohort)
; live_check.py + journal can group P&L by comment-prefix for cohort attribution.
;
; PARENT total risk: {total_pct}.0% ({risk_per_stream}% × 6 streams).
; Hedges: unchanged from v7 STOP-ext (ExtPts=100, TPMult=3.0, SLMult=1.0).
;
; Spread guard: _MaxSpreadPts=60
"""
        text = re.sub(r"^;.*?Spread guard.*?\n", new_header, text, count=1, flags=re.DOTALL)

        # Set _RiskPct
        text = re.sub(r"^_RiskPct=[^\n]+",
                      f"_RiskPct={risk_per_stream}||{risk_per_stream}||1||{risk_per_stream}||{risk_per_stream}||N",
                      text, count=1, flags=re.M)

        # Per-stream overrides
        for i, (_, r) in enumerate(hybrid.iterrows(), start=1):
            range_m = int(r['range_minutes'])
            sl = int(r['fixed_sl_pts'])
            rr = float(r['rr_ratio'])
            htp = round(float(r['half_tp_ratio']), 2)
            exp = int(r['pending_expire_minutes'])
            nd = float(r['np_dd_ratio'])
            prof = int(r['prof_count'])
            cohort = r['_cohort']  # sp30 or sp60
            orig_rank = int(r['rank'])

            text = re.sub(
                rf"; ===== ORB_S{i} -- [^\n]+",
                f"; ===== ORB_S{i} -- {cohort} rank#{orig_rank} {prof}/4 NP/DD$={nd:.0f} (magic {i*1111}) =====",
                text, count=1,
            )
            text = re.sub(rf"_ORB_S{i}_Comment=[^\n]+",
                          f"_ORB_S{i}_Comment=ORB_S{i}_v7{cohort}_may23", text, count=1)
            text = re.sub(rf"_ORB_S{i}_RangeMinutes=[^\n]+",
                          f"_ORB_S{i}_RangeMinutes={range_m}||{range_m}||1||{range_m}||{range_m}||N",
                          text, count=1)
            text = re.sub(rf"_ORB_S{i}_FixedSL_Pts=[^\n]+",
                          f"_ORB_S{i}_FixedSL_Pts={sl}||{sl}||1||{sl}||{sl}||N",
                          text, count=1)
            text = re.sub(rf"_ORB_S{i}_RR_Ratio=[^\n]+",
                          f"_ORB_S{i}_RR_Ratio={rr}||{rr}||1||{rr}||{rr}||N",
                          text, count=1)
            text = re.sub(rf"_ORB_S{i}_HalfTP_Ratio=[^\n]+",
                          f"_ORB_S{i}_HalfTP_Ratio={htp}||{htp}||1||{htp}||{htp}||N",
                          text, count=1)
            text = re.sub(rf"_ORB_S{i}_PendingExpireMinutes=[^\n]+",
                          f"_ORB_S{i}_PendingExpireMinutes={exp}||{exp}||1||{exp}||{exp}||N",
                          text, count=1)

        # Hedge comments reflect cohort; ALSO correct hedge magics to the standard
        # 8000+N*111 scheme (template inherits the gen_setfile_v6 bug that shipped
        # 8111/9222/10333/11444/12555/13666 — see project_setfile_hedge_magic_bug_2026_05_28).
        for i in range(1, 7):
            cohort = "sp30" if i <= 3 else "sp60"
            text = re.sub(rf"_HEDGE_S{i}_Comment=[^\n]+",
                          f"_HEDGE_S{i}_Comment=ORB_S{i}r_v7{cohort}_may23", text, count=1)
            hmagic = 8000 + i * 111  # S1->8111, S2->8222 ... S6->8666
            text = re.sub(rf"_HEDGE_S{i}_Magic=[^\n]+",
                          f"_HEDGE_S{i}_Magic={hmagic}||{hmagic}||1||{hmagic}||{hmagic}||N",
                          text, count=1)

        out_path.write_text(text, encoding="utf-8")
        print(f"wrote {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
