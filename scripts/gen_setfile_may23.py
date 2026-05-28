"""One-off: generate v4 6pct + 9pct setfiles for MAY23 reopt.

Reads:
  - output/wfo_orb_may23/winner_p1.json (informational; rotation table is hardcoded)
  - output/wfo_hedge_reverse_may23/winner.json (per-stream sl_mult/alpha/pm)

Writes:
  - configs/sets/dt818_pro_v4_6pct_may23_may16.set
  - configs/sets/dt818_pro_v4_9pct_may23_may16.set
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
hedge = json.loads((ROOT / "output" / "wfo_hedge_reverse_may23" / "winner.json").read_text())

# Rotation: S1-S3 = old MAY16 S4-S6; S4-S6 = new MAY23 R1-R3
STREAMS = {
    "S1": dict(magic=1111, sl=550, rr=4.0, htp=0.4, src="MAY16 R2 (was S4)"),
    "S2": dict(magic=2222, sl=550, rr=3.5, htp=0.4, src="MAY16 R3 (was S5)"),
    "S3": dict(magic=3333, sl=550, rr=3.0, htp=0.4, src="MAY16 R4 (was S6)"),
    "S4": dict(magic=4444, sl=400, rr=2.0, htp=0.0, src="MAY23 R1 (NEW, boundary-accepted)"),
    "S5": dict(magic=5555, sl=400, rr=2.0, htp=0.6, src="MAY23 R2 (NEW)"),
    "S6": dict(magic=6666, sl=400, rr=2.0, htp=0.4, src="MAY23 R3 (NEW)"),
}
HEDGE_M = {"S1": 8111, "S2": 8222, "S3": 8333, "S4": 8444, "S5": 8555, "S6": 8666}


def header(risk_label: str) -> str:
    pct = "9.0" if risk_label == "9pct" else "6.0"
    rp = "1.5" if risk_label == "9pct" else "1.0"
    lines = []
    a = lines.append
    a("; DT818_pro_v4 - 6-stream ORB rank portfolio + 6 SMART-TP REVERSE hedges (MAY23 reopt)")
    a("; Generated 2026-05-23 from wfo_orb_may23/winner_p1.json + wfo_hedge_reverse_may23/winner.json")
    a("; Use with ea/DT818_pro_v4.mq5 (compiled 2026-05-20 with two-stage smart-TP geometry).")
    a(";")
    a(f"; PARENT total risk: {pct}% ({rp}% x 6 streams). Range filter DISABLED.")
    a("; ROTATION 2026-05-23:")
    a(";   S1-S3 = MAY16 R2/R3/R4 (PREVIOUS-week WFO, rotated down from S4-S6), magics 1111/2222/3333")
    a(";   S4-S6 = MAY23 R1/R2/R3 (CURRENT-week WFO), magics 4444/5555/6666")
    a(";   S4-S6 winners ALL ship at grid floor (SL=400 RR=2.0) -- boundary warning acknowledged per user 2026-05-23.")
    a(";   Diversity collapse: ranks 1-4 all SL=400 RR=2.0, only HTP differs (0.0/0.6/0.4/0.2).")
    a(";")
    a("; SMART-TP REVERSE-HEDGE config (from wfo_hedge_reverse_may23/winner.json):")
    a(";   Direction:    OPPOSITE of parent")
    a(";   Order type:   TWO LIMITs per parent SL at parent_entry:")
    a(";                   _s1: closes alpha fraction at BE-of-combined-loss")
    a(";                   _s2: closes (1-alpha) fraction at profit_mult * combined-loss")
    a(";   SL distance:  parent_SL_pts * sl_mult  (per-stream)")
    a(";   TP geometry:  tp1_dist = sl_dist * sl_mult / alpha")
    a(";                 tp2_dist = sl_dist * sl_mult * profit_mult / (1 - alpha)")
    a(";   Validity:     alpha > 1 / (profit_mult + 1)  (winner=0.5 + pm>=2.5 satisfies)")
    a(";   Lots:         parent_lots / sl_mult  (risk-equalized)")
    a(";   Global expire_minutes = 240   (4h)")
    a(";   Global F1 filter      = 1800s (30min cap)")
    a(";   Global regime_gate    = off")
    a(";   Hedge magics: 8111/8222/8333/8444/8555/8666 (each places TWO sub-orders per SL).")
    a(";")
    a("; MAY23 hedge WFO portfolio compare: P0 FAILED (slope -108.3%, best-of-failed used)")
    a(";   Total IS+OOS NP across 4 folds: $91,832 (W1 $+38,566 -> W2 $+23,017 -> W3 $+33,461 -> W4 $-3,212)")
    a(";   W4 OOS (5/16-5/23) is the recent live week -- hedge took -$3k in sim, but live W4 hedge +$1k incl 5/22 +$17.6k save.")
    a(";")
    a("; Per-stream MAY23 hedge winners (alpha=0.5 for all, sl_mult & profit_mult per stream):")
    for s in ("S1", "S2", "S3", "S4", "S5", "S6"):
        sl = hedge["per_stream_sl_mult"][s]
        al = hedge["per_stream_partial_fraction"][s]
        pm = hedge["per_stream_profit_mult"][s]
        a(f";   {s}: sl_mult={sl}, alpha={al}, pm={pm}")
    a(";")
    a("; NOTE: _HEDGE_Sn_RR_Ratio is DECORATIVE in v4 smart-TP mode (EA computes")
    a(";       tp1_dist/tp2_dist from sl_mult+alpha+profit_mult). Kept for legacy display.")
    a(";")
    a("; Spread guard: _MaxSpreadPts=60 (Vantage max observed 32pt; sim default 30pt)")
    a("")
    a("; ===== Global account =====")
    a("_CapitalProtectionAmount=0.0||0.0||1||0.0||0.0||N")
    a(f"_RiskPct={rp}||{rp}||1||{rp}||{rp}||N")
    a("_LotMode=1||1||1||1||1||N")
    a("TierBase=2000||2000||1||2000||2000||N")
    a("LotStep=0.01||0.01||1||0.01||0.01||N")
    a("_MaxSpreadPts=60||60||1||60||60||N")
    a("")
    a("; ===== ORB shared params =====")
    a("_BrokerGMTOffsetHours=0||0||1||0||0||N  ; DEPRECATED post 2026-05-07 (EA uses TimeGMT)")
    a("_ORB_MinRangePts=0||0||1||0||0||N             ; range filter DISABLED")
    a("_ORB_MaxRangePts=999999||999999||1||999999||999999||N")
    a("_ORB_LDN_Enabled=true")
    a("_ORB_LDN_StartHour=4||4||1||4||4||N")
    a("_ORB_NY_Enabled=true")
    a("_ORB_NY_StartHour=10||10||1||10||10||N")
    a("")
    return "\n".join(lines)


def parent_block(s: str, cfg: dict) -> str:
    m = cfg["magic"]
    sl = cfg["sl"]
    rr = cfg["rr"]
    htp = cfg["htp"]
    return (
        f"; ===== ORB_{s} -- {cfg['src']} (magic {m}) =====\n"
        f"_ORB_{s}_Enabled=true\n"
        f"_ORB_{s}_Magic={m}||{m}||1||{m}||{m}||N\n"
        f"_ORB_{s}_Comment=ORB_{s}\n"
        f"_ORB_{s}_RangeMinutes=90||90||1||90||90||N\n"
        f"_ORB_{s}_FixedSL_Pts={sl}||{sl}||1||{sl}||{sl}||N\n"
        f"_ORB_{s}_RR_Ratio={rr}||{rr}||1||{rr}||{rr}||N\n"
        f"_ORB_{s}_HalfTP_Ratio={htp}||{htp}||1||{htp}||{htp}||N\n"
        f"_ORB_{s}_PendingExpireMinutes=240||240||1||240||240||N\n"
        f"_ORB_{s}_DailyTargetPct=0.0||0.0||1||0.0||0.0||N\n"
        f"_ORB_{s}_DailyLossPct=0.0||0.0||1||0.0||0.0||N\n"
    )


def hedge_block(s: str, parent_cfg: dict) -> str:
    sl_mult = hedge["per_stream_sl_mult"][s]
    alpha = hedge["per_stream_partial_fraction"][s]
    pm = hedge["per_stream_profit_mult"][s]
    hm = HEDGE_M[s]
    pm_par = parent_cfg["magic"]
    hsl_pts = int(round(parent_cfg["sl"] * sl_mult))
    hrr = round(pm / sl_mult, 2)
    return (
        f"; ===== HEDGE_{s}r -- SMART-TP: sl_mult={sl_mult}, alpha={alpha}, pm={pm} =====\n"
        f"_HEDGE_{s}_Enabled=true\n"
        f"_HEDGE_{s}_Magic={hm}||{hm}||1||{hm}||{hm}||N\n"
        f"_HEDGE_{s}_Comment=ORB_{s}r\n"
        f"_HEDGE_{s}_ParentMagic={pm_par}||{pm_par}||1||{pm_par}||{pm_par}||N\n"
        f"_HEDGE_{s}_FixedSL_Pts={hsl_pts}||{hsl_pts}||1||{hsl_pts}||{hsl_pts}||N\n"
        f"_HEDGE_{s}_RR_Ratio={hrr}||{hrr}||1||{hrr}||{hrr}||N\n"
        f"_HEDGE_{s}_ExpireMinutes=240||240||1||240||240||N\n"
        f"_HEDGE_{s}_MaxSecondsAfterEntry=1800||1800||1||1800||1800||N\n"
        f"_HEDGE_{s}_PartialFraction={alpha}||{alpha}||0.05||0.5||0.7||N\n"
        f"_HEDGE_{s}_ProfitMult={pm}||{pm}||0.1||1.2||4.0||N\n"
        f"_HEDGE_{s}_SLMult={sl_mult}||{sl_mult}||1||{sl_mult}||{sl_mult}||N\n"
    )


for risk_label in ("6pct", "9pct"):
    out = [header(risk_label)]
    for s, cfg in STREAMS.items():
        out.append(parent_block(s, cfg))
    for s, cfg in STREAMS.items():
        out.append(hedge_block(s, cfg))
    path = ROOT / "configs" / "sets" / f"dt818_pro_v4_{risk_label}_may23_may16.set"
    path.write_text("\n".join(out))
    print(f"Wrote {path.name}: {path.stat().st_size} bytes, {len(path.read_text().splitlines())} lines")
