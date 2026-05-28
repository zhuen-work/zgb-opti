"""Show param differences between WFO@spread30 and WFO@spread60 top-6 picks
across may9, may16, may23 eras.
"""
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

ERAS = [
    ("may9",  ROOT/"output/wfo_orb_v2_may9_spread30/oos_rank.csv",
              ROOT/"output/wfo_orb_v2_may9_spread60/oos_rank.csv"),
    ("may16", ROOT/"output/wfo_orb_v2_may16_spread30/oos_rank.csv",
              ROOT/"output/wfo_orb_v2_may16_spread60/oos_rank.csv"),
    ("may23", ROOT/"output/wfo_orb_v2_may23_spread30/oos_rank.csv",
              ROOT/"output/wfo_orb_v2_may23/oos_rank.csv"),
]

def fmt(r):
    return f"R{int(r['range_minutes']):>3} SL{int(r['fixed_sl_pts']):>4} RR{r['rr_ratio']:<4} HTP{r['half_tp_ratio']:<4}"

def signature(df: pd.DataFrame):
    """Return set of (R,SL,RR,HTP) signatures for top-6."""
    return [(int(r['range_minutes']), int(r['fixed_sl_pts']),
             round(float(r['rr_ratio']), 2),
             round(float(r['half_tp_ratio']), 2))
            for _, r in df.iterrows()]

print("=" * 110)
print("  WFO@spread=30 vs WFO@spread=60 — top-6 param diff per era")
print("=" * 110)

for era, p30, p60 in ERAS:
    df30 = pd.read_csv(p30).head(6)
    df60 = pd.read_csv(p60).head(6)
    print(f"\n  [{era}]")
    print(f"  {'#':<3}  {'spread=30pt picks':<38}  {'prof':>5}  {'NDD':>6}  | "
          f"{'spread=60pt picks':<38}  {'prof':>5}  {'NDD':>6}  |  {'Same?':<6}")
    print("  " + "-" * 110)
    sig30 = signature(df30)
    sig60 = signature(df60)
    set30 = set(sig30); set60 = set(sig60)
    for i in range(6):
        r30 = df30.iloc[i]; r60 = df60.iloc[i]
        same_rank = sig30[i] == sig60[i]
        in_other = sig30[i] in set60 or sig60[i] in set30
        if same_rank:
            mark = "EXACT"
        elif in_other:
            mark = "shift"
        else:
            mark = "DIFF"
        print(f"  {i+1:<3}  {fmt(r30):<38}  {int(r30['prof_count']):>5}  "
              f"{r30['np_dd_ratio']:>6.0f}  | {fmt(r60):<38}  {int(r60['prof_count']):>5}  "
              f"{r60['np_dd_ratio']:>6.0f}  |  {mark:<6}")

    # Set overlap
    common = set30 & set60
    only30 = set30 - set60
    only60 = set60 - set30
    print(f"\n    Overlap (top-6 set membership, ignoring order):")
    print(f"      common to both: {len(common)} of 6")
    print(f"      only in spread30 top-6: {len(only30)}")
    print(f"      only in spread60 top-6: {len(only60)}")
    if only30:
        print(f"    spread30-exclusive picks:")
        for s in sorted(only30):
            print(f"      R{s[0]:>3} SL{s[1]:>4} RR{s[2]:<4} HTP{s[3]:<4}")
    if only60:
        print(f"    spread60-exclusive picks:")
        for s in sorted(only60):
            print(f"      R{s[0]:>3} SL{s[1]:>4} RR{s[2]:<4} HTP{s[3]:<4}")

print()
print("=" * 110)
print("  Aggregate pattern across all 3 eras")
print("=" * 110)
all30 = []; all60 = []
for era, p30, p60 in ERAS:
    all30 += signature(pd.read_csv(p30).head(6))
    all60 += signature(pd.read_csv(p60).head(6))

import collections
def cat_counts(sigs):
    htp0 = sum(1 for s in sigs if s[3] == 0.0)
    htp04 = sum(1 for s in sigs if abs(s[3] - 0.4) < 0.01)
    r60 = sum(1 for s in sigs if s[0] == 60)
    r90 = sum(1 for s in sigs if s[0] == 90)
    rr_avg = sum(s[2] for s in sigs) / len(sigs) if sigs else 0
    sl_avg = sum(s[1] for s in sigs) / len(sigs) if sigs else 0
    return dict(htp0=htp0, htp04=htp04, r60=r60, r90=r90,
                rr_avg=rr_avg, sl_avg=sl_avg, n=len(sigs))

c30 = cat_counts(all30)
c60 = cat_counts(all60)
print(f"\n  Across 18 picks (6 streams × 3 eras):")
print(f"  {'Attribute':<25}  {'spread30':>12}  {'spread60':>12}  {'Diff':>8}")
print(f"  {'-' * 60}")
print(f"  {'HTP=0.0 (no partial)':<25}  {c30['htp0']:>9} /18  {c60['htp0']:>9} /18  {c30['htp0']-c60['htp0']:>+8d}")
print(f"  {'HTP=0.4 (partial close)':<25}  {c30['htp04']:>9} /18  {c60['htp04']:>9} /18  {c30['htp04']-c60['htp04']:>+8d}")
print(f"  {'Range=60 (short)':<25}  {c30['r60']:>9} /18  {c60['r60']:>9} /18  {c30['r60']-c60['r60']:>+8d}")
print(f"  {'Range=90 (medium)':<25}  {c30['r90']:>9} /18  {c60['r90']:>9} /18  {c30['r90']-c60['r90']:>+8d}")
print(f"  {'Mean RR':<25}  {c30['rr_avg']:>12.2f}  {c60['rr_avg']:>12.2f}  {c30['rr_avg']-c60['rr_avg']:>+8.2f}")
print(f"  {'Mean SL (pts)':<25}  {c30['sl_avg']:>12.0f}  {c60['sl_avg']:>12.0f}  {c30['sl_avg']-c60['sl_avg']:>+8.0f}")
print()
