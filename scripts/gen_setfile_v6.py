"""Generate v6 setfile (6pct + 9pct): v5 parents + STOP-ext hedges ENABLED.

Parents = same as v5 (top-6 from output/wfo_orb_v5_expire_extend/oos_rank.csv).
Hedges  = ENABLED, STOP-on-extension with WFO winner params (ExtPts=100,
          TPMult=3.0, SLMult=1.0 uniform across all 6 streams).
Globals = same as v5 (_ORB_FractalConfirm=true, _ORB_FractalWidth=5).
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

STREAMS = [
    # (n, magic, sl, rr, htp, expire, wfo_note)
    (1, 1111, 550, 4.0, 0.2,  720, "WFO rank#1 4/4 NP/DD$=3875"),
    (2, 2222, 400, 3.5, 0.4,  240, "WFO rank#2 4/4 NP/DD$=3498"),
    (3, 3333, 550, 4.0, 0.2,  480, "WFO rank#3 4/4 NP/DD$=4039"),
    (4, 4444, 550, 4.0, 0.2,  240, "WFO rank#4 4/4 NP/DD$=3587"),
    (5, 5555, 550, 2.0, 0.4,  720, "WFO rank#5 4/4 NP/DD$=3632"),
    (6, 6666, 400, 3.5, 0.4,  720, "WFO rank#7 3/4 (sub'd from rank#6 Exp=1440)"),
]

# Uniform STOP-ext hedge per stream (from output/wfo_hedge_geometry_may23/ winner)
HEDGE_EXT_PTS  = 100
HEDGE_TP_MULT  = 3.0
HEDGE_SL_MULT  = 1.0
HEDGE_EXPIRE   = 240
HEDGE_F1       = 1800


def make_setfile(risk_pct: float) -> str:
    pct_label = f"{int(risk_pct*6)}pct"
    lines = [
        f"; DT818_pro_v6 - 6-stream ORB rank portfolio + 6 STOP-ON-EXTENSION hedges",
        f"; Generated 2026-05-24 PT2 from output/wfo_orb_v5_expire_extend/oos_rank.csv (parents)",
        f";                          + output/wfo_hedge_geometry_may23/ winner (STOP-ext: ExtPts=100, TPMult=3.0, SLMult=1.0)",
        f"; Use with ea/DT818_pro_v6.mq5 (compiled 2026-05-24).",
        f";",
        f"; v6 vs v5 key changes:",
        f";   1. HEDGES RE-ENABLED with STOP-on-extension geometry (was DISABLED in v5).",
        f";   2. Parent layer UNCHANGED (V2 fractal-confirmed entry; same 6 streams as v5).",
        f";",
        f"; STOP-ext hedge geometry (uniform across all 6 streams):",
        f";   On parent BUY  SL → SELL_STOP at parent_SL - {HEDGE_EXT_PTS}pt",
        f";   On parent SELL SL → BUY_STOP  at parent_SL + {HEDGE_EXT_PTS}pt",
        f";   Hedge SL  = parent_SL_dist × {HEDGE_SL_MULT} (against continuation)",
        f";   Hedge TP  = {HEDGE_EXT_PTS}pt × {HEDGE_TP_MULT} = {int(HEDGE_EXT_PTS*HEDGE_TP_MULT)}pt (with continuation)",
        f";   Hedge lots = parent_lots / {HEDGE_SL_MULT} (risk-equalized)",
        f";",
        f"; v6 sim performance (Feb 14 → May 23, ${{deposit}}, {{risk}}/stream, 30pt):",
        f";   Parents only (v5):           NP=$263,547  DD=5.10%  NP/DD$=18.46  WR=57.0%",
        f";   Parents + STOP-ext (v6):    NP=$291,672  DD=5.32%  NP/DD$=17.76  WR=60.9%",
        f";   Delta: +$28,125 NP (+11%) for +$2,142 DD (+0.22pp) — 13:1 NP-per-DD ratio.",
        f";   STOP-ext OOS uniformly positive across all 4 weeks (no decay observed).",
        f";",
        f"; Per-stream parents (all Range=90, fractal_confirm=true, fractal_width=5):",
    ]
    for (n, magic, sl, rr, htp, exp, note) in STREAMS:
        lines.append(f";   S{n}: SL={sl} RR={rr} HTP={htp} Exp={exp}  -- {note}")
    lines += [
        f";",
        f"; PARENT total risk: {risk_pct*6:.1f}% ({risk_pct}% × 6 streams). Range filter DISABLED.",
        f"; Hedge magics: 8111-8666 (same as v5 reverse, but v6 STOP-ext logic).",
        f"; Deprecated v5 inputs (kept for setfile compat): _HEDGE_S*_PartialFraction, _HEDGE_S*_ProfitMult.",
        f";",
        f"; Spread guard: _MaxSpreadPts=60",
        f"",
        f"; ===== Global account =====",
        f"_CapitalProtectionAmount=0.0||0.0||1||0.0||0.0||N",
        f"_RiskPct={risk_pct}||{risk_pct}||1||{risk_pct}||{risk_pct}||N",
        f"_LotMode=1||1||1||1||1||N",
        f"TierBase=2000||2000||1||2000||2000||N",
        f"LotStep=0.01||0.01||1||0.01||0.01||N",
        f"_MaxSpreadPts=60||60||1||60||60||N",
        f"",
        f"; ===== ORB shared params (v5/v6 globals incl. V2 fractal-confirm) =====",
        f"_BrokerGMTOffsetHours=0||0||1||0||0||N  ; DEPRECATED post 2026-05-07 (EA uses TimeGMT)",
        f"_ORB_MinRangePts=0||0||1||0||0||N             ; range filter DISABLED",
        f"_ORB_MaxRangePts=999999||999999||1||999999||999999||N",
        f"_ORB_LDN_Enabled=true",
        f"_ORB_LDN_StartHour=4||4||1||4||4||N",
        f"_ORB_NY_Enabled=true",
        f"_ORB_NY_StartHour=10||10||1||10||10||N",
        f"_ORB_FractalConfirm=true                     ; v5/v6: V2 gate ALL streams",
        f"_ORB_FractalWidth=5||5||1||3||5||N           ; Bill Williams 5-bar fractal",
        f"",
    ]
    # Parent stream sections
    for (n, magic, sl, rr, htp, exp, note) in STREAMS:
        lines += [
            f"; ===== ORB_S{n} -- {note} (magic {magic}) =====",
            f"_ORB_S{n}_Enabled=true",
            f"_ORB_S{n}_Magic={magic}||{magic}||1||{magic}||{magic}||N",
            f"_ORB_S{n}_Comment=ORB_S{n}",
            f"_ORB_S{n}_RangeMinutes=90||90||1||90||90||N",
            f"_ORB_S{n}_FixedSL_Pts={sl}||{sl}||1||{sl}||{sl}||N",
            f"_ORB_S{n}_RR_Ratio={rr}||{rr}||1||{rr}||{rr}||N",
            f"_ORB_S{n}_HalfTP_Ratio={htp}||{htp}||1||{htp}||{htp}||N",
            f"_ORB_S{n}_PendingExpireMinutes={exp}||{exp}||1||{exp}||{exp}||N",
            f"_ORB_S{n}_DailyTargetPct=0.0||0.0||1||0.0||0.0||N",
            f"_ORB_S{n}_DailyLossPct=0.0||0.0||1||0.0||0.0||N",
            f"",
        ]
    # Hedge stream sections (STOP-ext, ENABLED)
    for (n, magic, sl, rr, htp, exp, _note) in STREAMS:
        hmagic = 8000 + magic - 1000  # 1111 -> 8111
        lines += [
            f"; ===== HEDGE_S{n}r -- STOP-on-extension (v6) =====",
            f"_HEDGE_S{n}_Enabled=true",
            f"_HEDGE_S{n}_Magic={hmagic}||{hmagic}||1||{hmagic}||{hmagic}||N",
            f"_HEDGE_S{n}_Comment=ORB_S{n}r",
            f"_HEDGE_S{n}_ParentMagic={magic}||{magic}||1||{magic}||{magic}||N",
            f"_HEDGE_S{n}_FixedSL_Pts={sl}||{sl}||1||{sl}||{sl}||N    ; (legacy field, unused by v6)",
            f"_HEDGE_S{n}_RR_Ratio=3.0||3.0||1||3.0||3.0||N            ; (legacy field, unused by v6)",
            f"_HEDGE_S{n}_ExpireMinutes={HEDGE_EXPIRE}||{HEDGE_EXPIRE}||1||{HEDGE_EXPIRE}||{HEDGE_EXPIRE}||N",
            f"_HEDGE_S{n}_MaxSecondsAfterEntry={HEDGE_F1}||{HEDGE_F1}||1||{HEDGE_F1}||{HEDGE_F1}||N",
            f"_HEDGE_S{n}_PartialFraction=0.5||0.5||0.05||0.5||0.7||N  ; DEPRECATED in v6 (v5 smart-TP field)",
            f"_HEDGE_S{n}_ProfitMult=3.0||3.0||0.1||1.2||4.0||N        ; DEPRECATED in v6",
            f"_HEDGE_S{n}_SLMult={HEDGE_SL_MULT}||{HEDGE_SL_MULT}||1||{HEDGE_SL_MULT}||{HEDGE_SL_MULT}||N",
            f"_HEDGE_S{n}_ExtPts={HEDGE_EXT_PTS}||{HEDGE_EXT_PTS}||1||{HEDGE_EXT_PTS}||{HEDGE_EXT_PTS}||N    ; v6: points past parent SL",
            f"_HEDGE_S{n}_TPMult={HEDGE_TP_MULT}||{HEDGE_TP_MULT}||0.1||{HEDGE_TP_MULT}||{HEDGE_TP_MULT}||N  ; v6: TP = ExtPts × TPMult",
            f"",
        ]
    return "\n".join(lines)


def main():
    out_dir = ROOT / "configs" / "sets"
    out_dir.mkdir(parents=True, exist_ok=True)
    d_dir = Path("D:/v6")
    d_dir.mkdir(parents=True, exist_ok=True)

    for risk, label in ((1.0, "6pct"), (1.5, "9pct")):
        content = make_setfile(risk)
        fn = f"dt818_pro_v6_{label}_may23_may16.set"
        p_repo = out_dir / fn
        p_d = d_dir / fn
        p_repo.write_text(content, encoding="utf-8")
        p_d.write_text(content, encoding="utf-8")
        print(f"Wrote: {fn}  ({len(content)} bytes)")
        print(f"   repo: {p_repo}")
        print(f"   D:/:  {p_d}")

    print(f"\n=== Parent table (same in 6pct + 9pct, identical to v5) ===")
    print(f"  {'Stream':<6} {'Magic':>5} {'SL':>5} {'RR':>5} {'HTP':>5} {'Exp':>5}")
    for (n, magic, sl, rr, htp, exp, _) in STREAMS:
        print(f"  S{n:<5} {magic:>5} {sl:>5} {rr:>5} {htp:>5} {exp:>5}")

    print(f"\n=== Hedge table (v6 STOP-ext, all 6 streams ENABLED) ===")
    print(f"  All hedges uniform: ExtPts={HEDGE_EXT_PTS}, TPMult={HEDGE_TP_MULT}, SLMult={HEDGE_SL_MULT}, ")
    print(f"                      ExpireMin={HEDGE_EXPIRE}, F1={HEDGE_F1}")
    print(f"  Magics: 8111-8666 (parent_magic 1111-6666)")

    print(f"\n=== RiskPct ===")
    print(f"  6pct: _RiskPct=1.0  (6.0% total)")
    print(f"  9pct: _RiskPct=1.5  (9.0% total)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
