"""All-streams combined sanity ET — FBO S1 + FBO S2 + FVG S1 + FVG S2 sharing $10k.

Runs two scenarios:
  A) All 4 streams enabled
  B) Only the 3 that passed the WFO gate (FBO S1, FBO S2 M15, FVG S2) — FVG S1 excluded

Reads winner setfiles, runs combined sim continuous Mar 14 -> Apr 25.
"""
from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.fbo_s1 import FBOS1Config
from zgb_sim.fvg import FVGConfig
from zgb_sim.all_streams_combined import simulate_all_streams


SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0


def parse_fbo_s1_setfile() -> FBOS1Config:
    """Parse FBO S1 (M30) winner setfile."""
    text = (ROOT / "configs" / "sets" / "scalp_v1_sim_fbo_s1_spread60_apr25.set").read_text(encoding="utf-8")
    def v(k): return re.search(rf"^{re.escape(k)}=([^|]+)", text, re.MULTILINE).group(1).strip()
    return FBOS1Config(
        risk_pct=float(v("_RiskPct")),
        fractal_bars=int(v("_Bars")),
        take_profit_pts=int(v("_take_profit")),
        stop_loss_pts=int(v("_stop_loss")),
        half_tp_ratio=float(v("_HalfTP1")),
        sma_period=int(v("_EMA_Period1")),
        pending_expire_bars=int(v("_PendingExpireBars")),
        signal_tf_minutes=30,
        comment="FBO_A",
    )


def parse_fbo_s2_m15_setfile() -> FBOS1Config:
    text = (ROOT / "configs" / "sets" / "fbo_s2_m15_sim_spread60_apr25.set").read_text(encoding="utf-8")
    def v(k): return re.search(rf"^{re.escape(k)}=([^|]+)", text, re.MULTILINE).group(1).strip()
    return FBOS1Config(
        risk_pct=float(v("_RiskPct")),
        fractal_bars=int(v("_Bars2")),
        take_profit_pts=int(v("_take_profit2")),
        stop_loss_pts=int(v("_stop_loss2")),
        half_tp_ratio=float(v("_HalfTP2")),
        sma_period=int(v("_EMA_Period2")),
        pending_expire_bars=int(v("_PendingExpireBars")),
        signal_tf_minutes=15,
        comment="FBO_B",
    )


def parse_fvg_s1_setfile() -> FVGConfig:
    text = (ROOT / "configs" / "sets" / "fvg_s1_sim_spread60_apr25.set").read_text(encoding="utf-8")
    def v(k): return re.search(rf"^{re.escape(k)}=([^|]+)", text, re.MULTILINE).group(1).strip()
    return FVGConfig(
        risk_pct=float(v("_RiskPct")),
        min_size_pts=int(v("_FVG_MinSize")),
        max_age_bars=int(v("_FVG_MaxAge")),
        max_zones=int(v("_MaxZones")),
        rr_ratio=float(v("_RR_Ratio")),
        sl_buffer_pts=int(v("_SL_Buffer")),
        pending_expire_bars=int(v("_PendingExpireBars_F1")),
        half_tp_ratio=float(v("_HalfTP_F1")),
        signal_tf_minutes=60,
        comment="FVG_A",
    )


def parse_fvg_s2_setfile() -> FVGConfig:
    text = (ROOT / "configs" / "sets" / "fvg_s2_sim_spread60_apr25.set").read_text(encoding="utf-8")
    def v(k): return re.search(rf"^{re.escape(k)}=([^|]+)", text, re.MULTILINE).group(1).strip()
    return FVGConfig(
        risk_pct=float(v("_RiskPct")),
        min_size_pts=int(v("_FVG_MinSize2")),
        max_age_bars=int(v("_FVG_MaxAge2")),
        max_zones=int(v("_MaxZones2")),
        rr_ratio=float(v("_RR_Ratio2")),
        sl_buffer_pts=int(v("_SL_Buffer2")),
        pending_expire_bars=int(v("_PendingExpireBars_F2")),
        half_tp_ratio=float(v("_HalfTP_F2")),
        signal_tf_minutes=240,
        comment="FVG_B",
    )


def main() -> int:
    fbo_s1 = parse_fbo_s1_setfile()
    fbo_s2 = parse_fbo_s2_m15_setfile()
    fvg_s1 = parse_fvg_s1_setfile()
    fvg_s2 = parse_fvg_s2_setfile()

    print("=" * 72)
    print("  ALL-STREAMS COMBINED SANITY (Mar 14 -> Apr 25, $10k shared)")
    print("=" * 72)
    print(f"\n  FBO S1 (M30): Bars={fbo_s1.fractal_bars} TP={fbo_s1.take_profit_pts} SL={fbo_s1.stop_loss_pts} "
          f"HTP={fbo_s1.half_tp_ratio} SMA={fbo_s1.sma_period} PEB={fbo_s1.pending_expire_bars}")
    print(f"  FBO S2 (M15): Bars={fbo_s2.fractal_bars} TP={fbo_s2.take_profit_pts} SL={fbo_s2.stop_loss_pts} "
          f"HTP={fbo_s2.half_tp_ratio} SMA={fbo_s2.sma_period} PEB={fbo_s2.pending_expire_bars}")
    print(f"  FVG S1 (H1):  MinS={fvg_s1.min_size_pts} MA={fvg_s1.max_age_bars} MZ={fvg_s1.max_zones} "
          f"RR={fvg_s1.rr_ratio} SLB={fvg_s1.sl_buffer_pts} HTP={fvg_s1.half_tp_ratio} PEB={fvg_s1.pending_expire_bars}")
    print(f"  FVG S2 (H4):  MinS={fvg_s2.min_size_pts} MA={fvg_s2.max_age_bars} MZ={fvg_s2.max_zones} "
          f"RR={fvg_s2.rr_ratio} SLB={fvg_s2.sl_buffer_pts} HTP={fvg_s2.half_tp_ratio} PEB={fvg_s2.pending_expire_bars}")

    full_start = datetime(2026, 3, 14, tzinfo=timezone.utc)
    full_end = datetime(2026, 4, 25, tzinfo=timezone.utc)

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )
        print("\n  Loading data...")
        ticks = load_ticks(SYMBOL, full_start, full_end)
        m1 = load_bars(SYMBOL, "M1", full_start, full_end)
        m30 = load_bars(SYMBOL, "M30", full_start, full_end)
        m15 = load_bars(SYMBOL, "M15", full_start, full_end)
        h1 = load_bars(SYMBOL, "H1", full_start, full_end)
        h4 = load_bars(SYMBOL, "H4", full_start, full_end)
        print(f"  ticks={len(ticks):,}  M1={len(m1):,}  M30={len(m30):,}  M15={len(m15):,}  H1={len(h1):,}  H4={len(h4):,}")

        # Scenario A: all 4 streams
        print("\n" + "=" * 72)
        print("  SCENARIO A: ALL 4 STREAMS (FBO S1 + FBO S2 + FVG S1 + FVG S2)")
        print("=" * 72)
        result_a = simulate_all_streams(
            ticks, m30, m15, h1, h4, m1,
            fbo_s1, fbo_s2, fvg_s1, fvg_s2,
            meta, initial_balance=DEPOSIT,
        )
        s = result_a.summary
        print(f"\n  Total NP:    ${result_a.summary.net_profit:+,.2f}  "
              f"({result_a.summary.net_profit / DEPOSIT * 100:+.1f}% ROI)")
        print(f"  Combined DD: {result_a.combined_dd_pct:.2f}%")
        print(f"  Combined PF: {s.profit_factor:.2f}")
        print(f"  Trades: {s.trades}  (TP={s.tp_count} SL={s.sl_count})")
        print(f"\n  Per-stream contribution:")
        for name, ps in result_a.per_stream.items():
            print(f"    {name:<8}  NP=${ps['np']:>+9,.2f}  trades={ps['trades']:>3}  "
                  f"TP={ps['tp']:>3}  SL={ps['sl']:>3}")

        # Scenario B: drop FVG S1 (failed gate)
        print("\n" + "=" * 72)
        print("  SCENARIO B: 3 STREAMS — FVG S1 EXCLUDED (failed gate)")
        print("=" * 72)
        result_b = simulate_all_streams(
            ticks, m30, m15, h1, h4, m1,
            fbo_s1, fbo_s2, None, fvg_s2,
            meta, initial_balance=DEPOSIT,
        )
        s = result_b.summary
        print(f"\n  Total NP:    ${result_b.summary.net_profit:+,.2f}  "
              f"({result_b.summary.net_profit / DEPOSIT * 100:+.1f}% ROI)")
        print(f"  Combined DD: {result_b.combined_dd_pct:.2f}%")
        print(f"  Combined PF: {s.profit_factor:.2f}")
        print(f"  Trades: {s.trades}  (TP={s.tp_count} SL={s.sl_count})")
        print(f"\n  Per-stream contribution:")
        for name, ps in result_b.per_stream.items():
            print(f"    {name:<8}  NP=${ps['np']:>+9,.2f}  trades={ps['trades']:>3}  "
                  f"TP={ps['tp']:>3}  SL={ps['sl']:>3}")

        # Scenario C: only the 2 strongest (FBO S1 + FBO S2 M15)
        print("\n" + "=" * 72)
        print("  SCENARIO C: FBO ONLY (S1 + S2 M15) — for reference")
        print("=" * 72)
        result_c = simulate_all_streams(
            ticks, m30, m15, h1, h4, m1,
            fbo_s1, fbo_s2, None, None,
            meta, initial_balance=DEPOSIT,
        )
        s = result_c.summary
        print(f"\n  Total NP:    ${result_c.summary.net_profit:+,.2f}  "
              f"({result_c.summary.net_profit / DEPOSIT * 100:+.1f}% ROI)")
        print(f"  Combined DD: {result_c.combined_dd_pct:.2f}%")
        print(f"  Combined PF: {s.profit_factor:.2f}")
        print(f"  Trades: {s.trades}  (TP={s.tp_count} SL={s.sl_count})")
        print(f"\n  Per-stream contribution:")
        for name, ps in result_c.per_stream.items():
            print(f"    {name:<8}  NP=${ps['np']:>+9,.2f}  trades={ps['trades']:>3}  "
                  f"TP={ps['tp']:>3}  SL={ps['sl']:>3}")

        print("\n" + "=" * 72)
        print("  COMPARISON SUMMARY")
        print("=" * 72)
        print(f"  {'Scenario':<35}  {'Total NP':>12}  {'DD%':>6}  {'PF':>5}  {'Trades':>7}")
        print(f"  {'-'*72}")
        for label, r in [
            ("A: all 4 streams", result_a),
            ("B: 3 streams (FVG S1 excluded)", result_b),
            ("C: FBO only (S1+S2 M15)", result_c),
        ]:
            print(f"  {label:<35}  ${r.summary.net_profit:>+10,.0f}  "
                  f"{r.combined_dd_pct:>5.1f}  {r.summary.profit_factor:>5.2f}  "
                  f"{r.summary.trades:>7}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
