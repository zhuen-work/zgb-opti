"""Generate v5 setfile (6pct + 9pct) from WFO winners.

Per [[feedback_no_rotation_use_top6]]: S1..S6 = top 6 from latest WFO
(output/wfo_orb_v5_expire_extend/oos_rank.csv).
Per [[feedback_setfile_mirror_d_drive]]: write BOTH variants, mirror to D:/v5/.
Per [[feedback_always_show_setfile_settings]]: print parent table + md5 at end.

Hedge layer: DISABLED. WFO + fractal + alpha/F1 sweeps all showed hedge
broken on recent gold tape (W4 OOS uniformly negative across all combos).
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Top-6 from output/wfo_orb_v5_expire_extend/oos_rank.csv
# S6 substituted: rank#6 had Exp=1440 (extreme); use rank#7 (same SL/RR/HTP, Exp=720) per memory rule
STREAMS = [
    # (magic, sl, rr, htp, expire, wfo_note)
    (1, 1111, 550, 4.0, 0.2,  720, "WFO rank#1 4/4 NP/DD$=3875"),
    (2, 2222, 400, 3.5, 0.4,  240, "WFO rank#2 4/4 NP/DD$=3498"),
    (3, 3333, 550, 4.0, 0.2,  480, "WFO rank#3 4/4 NP/DD$=4039 (best NP/DD$ of any 4/4)"),
    (4, 4444, 550, 4.0, 0.2,  240, "WFO rank#4 4/4 NP/DD$=3587"),
    (5, 5555, 550, 2.0, 0.4,  720, "WFO rank#5 4/4 NP/DD$=3632"),
    (6, 6666, 400, 3.5, 0.4,  720, "WFO rank#7 3/4 NP/DD$=3713 (substituted from rank#6 Exp=1440)"),
]


def make_setfile(risk_pct: float) -> str:
    pct_label = f"{int(risk_pct*6)}pct"  # 1.0 -> 6pct, 1.5 -> 9pct
    lines = [
        f"; DT818_pro_v5 - 6-stream ORB rank portfolio (NO ROTATION, FRACTAL-CONFIRMED, HEDGES DISABLED)",
        f"; Generated 2026-05-24 from output/wfo_orb_v5_expire_extend/oos_rank.csv",
        f"; Use with ea/DT818_pro_v5.mq5 (compiled 2026-05-24 with global _ORB_FractalConfirm input).",
        f";",
        f"; v5 vs v4 key changes:",
        f";   1. Global _ORB_FractalConfirm=true + _ORB_FractalWidth=5 (V2 fractal-confirmed entry)",
        f";   2. NO ROTATION (S1..S6 = ranks 1..6 from a single WFO, no prev-week mixing)",
        f";   3. Per-stream PendingExpireMinutes varies (was 240 uniform; WFO showed 480-720 better)",
        f";   4. ALL HEDGES DISABLED (hedge WFO failed decay-filter + recent tape broke premise)",
        f";",
        f"; Source: output/wfo_orb_v5_expire_extend/oos_rank.csv (1125-config grid x 4 may23 windows)",
        f"; Comparison vs v4 may23 (May 2-23, 30pt, 1%/stream): +44% NP, halved DD, 3.32x NP/DD$_hc.",
        f";",
        f"; Per-stream params (all Range=90, fractal_confirm=true, fractal_width=5):",
    ]
    for (s, magic, sl, rr, htp, exp, note) in STREAMS:
        lines.append(f";   S{s}: SL={sl} RR={rr} HTP={htp} Exp={exp}  -- {note}")
    lines += [
        f";",
        f"; PARENT total risk: {risk_pct*6:.1f}% ({risk_pct}% x 6 streams). Range filter DISABLED.",
        f"; Hedge layer: DISABLED in this setfile. Hedge WFO 2026-05-24 P0 fail + W4 OOS -$2,458;",
        f";   fractal-gate, alpha sweep, F1 sweep all unable to fix decay.",
        f";   Hedge inputs kept (legacy values) but _HEDGE_S*_Enabled=false.",
        f";",
        f"; Spread guard: _MaxSpreadPts=60 (Vantage max observed 32pt; sim default 30pt)",
        f"",
        f"; ===== Global account =====",
        f"_CapitalProtectionAmount=0.0||0.0||1||0.0||0.0||N",
        f"_RiskPct={risk_pct}||{risk_pct}||1||{risk_pct}||{risk_pct}||N",
        f"_LotMode=1||1||1||1||1||N",
        f"TierBase=2000||2000||1||2000||2000||N",
        f"LotStep=0.01||0.01||1||0.01||0.01||N",
        f"_MaxSpreadPts=60||60||1||60||60||N",
        f"",
        f"; ===== ORB shared params (v5 globals incl. NEW FractalConfirm + FractalWidth) =====",
        f"_BrokerGMTOffsetHours=0||0||1||0||0||N  ; DEPRECATED post 2026-05-07 (EA uses TimeGMT)",
        f"_ORB_MinRangePts=0||0||1||0||0||N             ; range filter DISABLED",
        f"_ORB_MaxRangePts=999999||999999||1||999999||999999||N",
        f"_ORB_LDN_Enabled=true",
        f"_ORB_LDN_StartHour=4||4||1||4||4||N",
        f"_ORB_NY_Enabled=true",
        f"_ORB_NY_StartHour=10||10||1||10||10||N",
        f"_ORB_FractalConfirm=true                     ; v5: V2 gate, ALL streams",
        f"_ORB_FractalWidth=5||5||1||3||5||N           ; Bill Williams 5-bar fractal",
        f"",
    ]
    for (s, magic, sl, rr, htp, exp, note) in STREAMS:
        lines += [
            f"; ===== ORB_S{s} -- {note} (magic {magic}) =====",
            f"_ORB_S{s}_Enabled=true",
            f"_ORB_S{s}_Magic={magic}||{magic}||1||{magic}||{magic}||N",
            f"_ORB_S{s}_Comment=ORB_S{s}",
            f"_ORB_S{s}_RangeMinutes=90||90||1||90||90||N",
            f"_ORB_S{s}_FixedSL_Pts={sl}||{sl}||1||{sl}||{sl}||N",
            f"_ORB_S{s}_RR_Ratio={rr}||{rr}||1||{rr}||{rr}||N",
            f"_ORB_S{s}_HalfTP_Ratio={htp}||{htp}||1||{htp}||{htp}||N",
            f"_ORB_S{s}_PendingExpireMinutes={exp}||{exp}||1||{exp}||{exp}||N",
            f"_ORB_S{s}_DailyTargetPct=0.0||0.0||1||0.0||0.0||N",
            f"_ORB_S{s}_DailyLossPct=0.0||0.0||1||0.0||0.0||N",
            f"",
        ]
    # Hedge inputs (DISABLED). Keep legacy values so re-enabling later is easy.
    hedge_legacy = {
        1: (550, 3.0, 1.0, 0.5, 3.0),
        2: (400, 3.5, 1.0, 0.5, 3.0),
        3: (550, 3.0, 1.0, 0.5, 3.0),
        4: (550, 3.0, 1.2, 0.5, 3.5),
        5: (550, 3.0, 1.2, 0.5, 3.5),
        6: (400, 3.0, 1.2, 0.5, 3.5),
    }
    for (s, magic, _sl, _rr, _htp, _exp, _note) in STREAMS:
        hsl, hrr, hslm, halpha, hpm = hedge_legacy[s]
        hmagic = 8000 + magic - 1000  # 1111 -> 8111, 2222 -> 8222, ...
        lines += [
            f"; ===== HEDGE_S{s}r -- DISABLED in v5; legacy WFO values kept for re-enable =====",
            f"_HEDGE_S{s}_Enabled=false",
            f"_HEDGE_S{s}_Magic={hmagic}||{hmagic}||1||{hmagic}||{hmagic}||N",
            f"_HEDGE_S{s}_Comment=ORB_S{s}r",
            f"_HEDGE_S{s}_ParentMagic={magic}||{magic}||1||{magic}||{magic}||N",
            f"_HEDGE_S{s}_FixedSL_Pts={hsl}||{hsl}||1||{hsl}||{hsl}||N",
            f"_HEDGE_S{s}_RR_Ratio={hrr}||{hrr}||1||{hrr}||{hrr}||N",
            f"_HEDGE_S{s}_ExpireMinutes=240||240||1||240||240||N",
            f"_HEDGE_S{s}_MaxSecondsAfterEntry=1800||1800||1||1800||1800||N",
            f"_HEDGE_S{s}_PartialFraction={halpha}||{halpha}||0.05||0.5||0.7||N",
            f"_HEDGE_S{s}_ProfitMult={hpm}||{hpm}||0.1||1.2||4.0||N",
            f"_HEDGE_S{s}_SLMult={hslm}||{hslm}||1||{hslm}||{hslm}||N",
            f"",
        ]
    return "\n".join(lines)


def main():
    out_dir = ROOT / "configs" / "sets"
    out_dir.mkdir(parents=True, exist_ok=True)
    d_dir = Path("D:/v5")
    d_dir.mkdir(parents=True, exist_ok=True)

    for risk, label in ((1.0, "6pct"), (1.5, "9pct")):
        content = make_setfile(risk)
        fn = f"dt818_pro_v5_{label}_may23_may16.set"
        p_repo = out_dir / fn
        p_d = d_dir / fn
        p_repo.write_text(content, encoding="utf-8")
        p_d.write_text(content, encoding="utf-8")
        # md5 check
        m_repo = hashlib.md5(content.encode()).hexdigest()
        m_d = hashlib.md5(p_d.read_bytes()).hexdigest()
        status = "OK" if m_repo == m_d else "MISMATCH"
        print(f"[{status}] md5={m_repo}  {fn}")
        print(f"        repo: {p_repo}")
        print(f"        D:/  : {p_d}")

    print(f"\n=== Parent table (same in both 6pct + 9pct) ===")
    print(f"  {'Stream':<6} {'Magic':>5} {'SL':>5} {'RR':>5} {'HTP':>5} {'Exp':>5}  WFO note")
    for (s, magic, sl, rr, htp, exp, note) in STREAMS:
        print(f"  S{s:<5} {magic:>5} {sl:>5} {rr:>5} {htp:>5} {exp:>5}  {note}")

    print(f"\n=== Hedge table (DISABLED in v5) ===")
    print(f"  All 6 hedges have _HEDGE_S*_Enabled=false. Legacy WFO values kept for re-enable.")

    print(f"\n=== RiskPct ===")
    print(f"  6pct: _RiskPct=1.0 (1.0% x 6 streams = 6% total)")
    print(f"  9pct: _RiskPct=1.5 (1.5% x 6 streams = 9% total)")

    print(f"\n=== Global new v5 inputs ===")
    print(f"  _ORB_FractalConfirm=true")
    print(f"  _ORB_FractalWidth=5")
    return 0


if __name__ == "__main__":
    sys.exit(main())
