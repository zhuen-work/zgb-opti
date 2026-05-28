"""Generate v7 setfiles from v6 template + v7 WFO top-6 + new SMA cross-exit fields.

Writes:
  configs/sets/dt818_pro_v7_9pct_may30_may23.set  (1.5%/stream × 6 = 9%)
  configs/sets/dt818_pro_v7_6pct_may30_may23.set  (1.0%/stream × 6 = 6%)
"""
from __future__ import annotations

import re
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "configs/sets/dt818_pro_v6_9pct_may23_may16.set"
RANK_CSV = ROOT / "output/wfo_orb_v7_smacross_may23/oos_rank.csv"

# Widened-grid v7 WFO yields 5 robust ranks. Duplicate rank#1 for S6 to keep
# 6-stream parity with v6 for fair portfolio comparison.
TOP_N = 6


def main():
    template = TEMPLATE.read_text()
    raw = pd.read_csv(RANK_CSV)
    if len(raw) < TOP_N:
        # Repeat rank-1 to fill remaining slots.
        dup = pd.concat([raw, raw.head(TOP_N - len(raw))], ignore_index=True).head(TOP_N)
        ranks = dup.copy()
    else:
        ranks = raw.head(TOP_N)
    print(f"Using top-{TOP_N} from {RANK_CSV.name}:")
    for _, r in ranks.iterrows():
        print(f"  rank#{int(r['rank'])} "
              f"Range={int(r['range_minutes'])} SL={int(r['fixed_sl_pts'])} "
              f"RR={r['rr_ratio']} HTP={r['half_tp_ratio']} "
              f"Exp={int(r['pending_expire_minutes'])}  "
              f"NP/DD${float(r['np_dd_ratio']):.0f}  "
              f"prof={int(r['prof_count'])}/4")

    for risk_per_stream, total_pct in [(1.5, 9), (1.0, 6)]:
        out_name = f"dt818_pro_v7_{total_pct}pct_may30_may23.set"
        out_path = ROOT / "configs/sets" / out_name
        text = template

        # Header
        new_header = f"""; DT818_pro_v7 - 6-stream ORB rank portfolio + 6 STOP-ext hedges + SMA(8,21) cross-exit
; Generated 2026-05-26 from output/wfo_orb_v7_smacross_may23/oos_rank.csv
;                       hedges unchanged from v6 STOP-ext (ExtPts=100, TPMult=3.0, SLMult=1.0)
; Use with ea/DT818_pro_v7.mq5.
;
; v7 vs v6 key changes:
;   NEW: SMA(8) x SMA(21) cross-exit on M5 for post-HTP runners (LOCKED periods).
;     - When SMA3-of-8 crosses below SMA-of-21 on M5 close AND runner is LONG
;       and in profit, runner closes at market.
;     - When SMA-of-8 crosses above SMA-of-21 AND runner is SHORT and in profit,
;       runner closes at market.
;     - Activates AFTER HTP partial close fires on the parent stream.
;     - Hedges (8xxx) untouched.
;   Rationale (output/wfo_orb_v7_smacross_may23/):
;     V5 sweep 2026-05-25 showed s8_21 produces +$759 4-wk NP gain over
;     baseline with 0 windows regressing >=-15%. WFO May 23 confirms top-7
;     all 4/4 profitable with cross-exit active.
;
; v7 sim performance (4-window WFO, $10k, 6%/stream, 30pt):
;   rank#1 (Range=60 SL=550 RR=3.5 HTP=0.4): total_np=$39,399 NP/DD$=2311
;   rank#2 (Range=90 SL=550 RR=4.0 HTP=0.4): total_np=$60,626 NP/DD$=3420
;   rank#3 (Range=90 SL=400 RR=3.5 HTP=0.4): total_np=$51,293 NP/DD$=3441
;
; PARENT total risk: {total_pct}.0% ({risk_per_stream}% × 6 streams).
; Hedge magics: 8111-8666 (STOP-ext, unchanged from v6).
;
; Spread guard: _MaxSpreadPts=60
"""
        # Replace header (everything from start through "; Spread guard:" line)
        text = re.sub(r"^;.*?Spread guard.*?\n", new_header, text, count=1, flags=re.DOTALL)

        # Set _RiskPct
        text = re.sub(r"^_RiskPct=[^\n]+",
                      f"_RiskPct={risk_per_stream}||{risk_per_stream}||1||{risk_per_stream}||{risk_per_stream}||N",
                      text, count=1, flags=re.M)

        # Add SMA cross-exit globals after _ORB_FractalWidth line
        sma_lines = (
            "_ORB_SMA_CrossExit=true                       ; v7: SMA(8)x(21) cross-exit on post-HTP runner\n"
            "_ORB_SMA_FastPeriod=8||8||1||8||8||N           ; LOCKED 2026-05-26 (V5 sweep winner)\n"
            "_ORB_SMA_SlowPeriod=21||21||1||21||21||N       ; LOCKED 2026-05-26 (V5 sweep winner)\n"
        )
        text = re.sub(r"(_ORB_FractalWidth=[^\n]+\n)", r"\1" + sma_lines, text, count=1)

        # Per-stream parent overrides from WFO top-6
        for i, (_, r) in enumerate(ranks.iterrows(), start=1):
            range_m = int(r['range_minutes'])
            sl = int(r['fixed_sl_pts'])
            rr = float(r['rr_ratio'])
            htp = round(float(r['half_tp_ratio']), 2)
            exp = int(r['pending_expire_minutes'])
            nd = float(r['np_dd_ratio'])
            prof = int(r['prof_count'])

            # Section header comment
            text = re.sub(
                rf"; ===== ORB_S{i} -- [^\n]+",
                f"; ===== ORB_S{i} -- WFO rank#{int(r['rank'])} {prof}/4 NP/DD$={nd:.0f} (magic {i*1111}) =====",
                text, count=1,
            )
            text = re.sub(rf"_ORB_S{i}_Comment=[^\n]+",
                          f"_ORB_S{i}_Comment=ORB_S{i}_v7_may23", text, count=1)
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

        # Also update hedge comments to v7
        for i in range(1, 7):
            text = re.sub(rf"_HEDGE_S{i}_Comment=ORB_S{i}r_v6_may23",
                          f"_HEDGE_S{i}_Comment=ORB_S{i}r_v7_may23", text, count=1)

        out_path.write_text(text, encoding="utf-8")
        print(f"wrote {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
