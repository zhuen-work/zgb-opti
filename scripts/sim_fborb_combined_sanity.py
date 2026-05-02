"""FBORB combined sanity ET — FBO S1 + FBO S2 + ORB on shared $10k.

Reuses simulate_all_streams (FBO+FVG support) by treating ORB as a stand-alone
stream run, then combining results post-hoc on a shared balance via interleaved
deal application. Cleaner approach: write a dedicated 3-stream sim.

For now: run each stream independently then merge deals on a shared balance.
This loses lot-scaling interaction but is fast to build.
"""
from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.fbo_s1 import FBOS1Config
from zgb_sim.fbo_s1_fast import simulate_fast as fbo_simulate
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate


SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0


def parse_fbo_s1() -> FBOS1Config:
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
        signal_tf_minutes=30, comment="FBO_A",
    )


def parse_fbo_s2_m15() -> FBOS1Config:
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
        signal_tf_minutes=15, comment="FBO_B",
    )


def parse_orb() -> ORBConfig:
    text = (ROOT / "configs" / "sets" / "scalp_v2_orb_spread60_apr25.set").read_text(encoding="utf-8")
    def v(k): return re.search(rf"^{re.escape(k)}=([^|]+)", text, re.MULTILINE).group(1).strip()
    return ORBConfig(
        risk_pct=float(v("_RiskPct")),
        range_minutes=int(v("_S1_RangeMinutes")),
        buffer_pts=int(v("_S1_BufferPts")),
        min_range_pts=int(v("_S1_MinRangePts")),
        max_range_pts=int(v("_S1_MaxRangePts")),
        fixed_sl_pts=int(v("_S1_FixedSL_Pts")),
        rr_ratio=float(v("_S1_RR_Ratio")),
        half_tp_ratio=float(v("_S1_HalfTP_Ratio")),
        pending_expire_minutes=int(v("_S1_PendingExpireMinutes")),
        daily_target_pct=float(v("_DailyTargetPct")),
        daily_loss_pct=float(v("_DailyLossPct")),
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13,
        comment="ORB",
    )


def main() -> int:
    base_fbo_s1 = parse_fbo_s1()
    base_fbo_s2 = parse_fbo_s2_m15()
    base_orb = parse_orb()
    # base_orb has risk=1%, daily target=9%, daily loss=6% from WFO.
    # base_fbo* have risk=3% from FBO WFO.
    # Scaling rule: at risk_pct=R%, set ALL streams to R%, scale daily caps Rx
    # from ORB's WFO baseline (9%/6%).

    print("=" * 72)
    print("  FBORB COMBINED SANITY ET — risk sweep")
    print("=" * 72)
    print(f"\n  FBO S1 (M30): Bars={base_fbo_s1.fractal_bars} TP={base_fbo_s1.take_profit_pts} "
          f"SL={base_fbo_s1.stop_loss_pts} HTP={base_fbo_s1.half_tp_ratio} "
          f"SMA={base_fbo_s1.sma_period} PEB={base_fbo_s1.pending_expire_bars}")
    print(f"  FBO S2 (M15): Bars={base_fbo_s2.fractal_bars} TP={base_fbo_s2.take_profit_pts} "
          f"SL={base_fbo_s2.stop_loss_pts} HTP={base_fbo_s2.half_tp_ratio} "
          f"SMA={base_fbo_s2.sma_period} PEB={base_fbo_s2.pending_expire_bars}")
    print(f"  ORB    (M5):  Range={base_orb.range_minutes}min Buf={base_orb.buffer_pts} "
          f"FixSL={base_orb.fixed_sl_pts} RR={base_orb.rr_ratio} HTP={base_orb.half_tp_ratio}")

    full_start = datetime(2026, 3, 14, tzinfo=timezone.utc)
    full_end = datetime(2026, 4, 25, tzinfo=timezone.utc)

    risk_levels = [2.0, 3.0, 4.5]
    summary_rows = []

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
            tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
            volume_min=m["volume_min"], volume_max=m["volume_max"],
            volume_step=m["volume_step"],
        )
        print("\n  Loading data...")
        ticks = load_ticks(SYMBOL, full_start, full_end)
        m1 = load_bars(SYMBOL, "M1", full_start, full_end)
        m5 = load_bars(SYMBOL, "M5", full_start, full_end)
        m15 = load_bars(SYMBOL, "M15", full_start, full_end)
        m30 = load_bars(SYMBOL, "M30", full_start, full_end)

        for risk in risk_levels:
            print("\n" + "=" * 72)
            print(f"  RISK LEVEL: {risk}%")
            print(f"  Daily caps scaled {risk}x from ORB WFO baseline (9%/6%):")
            print(f"    target={9.0 * risk:.1f}%  loss={6.0 * risk:.1f}%")
            print("=" * 72)

            fbo_s1 = FBOS1Config(**{**vars(base_fbo_s1), "risk_pct": risk})
            fbo_s2 = FBOS1Config(**{**vars(base_fbo_s2), "risk_pct": risk})
            orb = ORBConfig(**{**vars(base_orb),
                                "risk_pct": risk,
                                "daily_target_pct": 9.0 * risk,
                                "daily_loss_pct": 6.0 * risk})

            r_s1 = fbo_simulate(ticks, m30, m1, fbo_s1, meta, initial_balance=DEPOSIT)
            r_s2 = fbo_simulate(ticks, m15, m1, fbo_s2, meta, initial_balance=DEPOSIT)
            r_orb = orb_simulate(ticks, m5, m1, orb, meta, initial_balance=DEPOSIT)

            print(f"  FBO S1 :  {r_s1.summary()}")
            print(f"  FBO S2 :  {r_s2.summary()}")
            print(f"  ORB    :  {r_orb.summary()}")

            all_deals = []
            for d in r_s1.deals:
                if d.kind != "entry":
                    all_deals.append((d.ts, "FBO_S1", d.kind, d.pnl))
            for d in r_s2.deals:
                if d.kind != "entry":
                    all_deals.append((d.ts, "FBO_S2", d.kind, d.pnl))
            for d in r_orb.deals:
                if d.kind != "entry":
                    all_deals.append((d.ts, "ORB", d.kind, d.pnl))
            all_deals.sort(key=lambda x: x[0])

            combined_balance = DEPOSIT
            balance_max = DEPOSIT
            dd_abs = 0.0
            for _, _stream, _kind, pnl in all_deals:
                combined_balance += pnl
                if combined_balance > balance_max:
                    balance_max = combined_balance
                cur = balance_max - combined_balance
                if cur > dd_abs:
                    dd_abs = cur
            combined_np = combined_balance - DEPOSIT
            combined_dd_pct = (dd_abs / balance_max * 100.0) if balance_max > 0 else 0.0

            s1_total = sum(p for _, s, _, p in all_deals if s == "FBO_S1")
            s2_total = sum(p for _, s, _, p in all_deals if s == "FBO_S2")
            orb_total = sum(p for _, s, _, p in all_deals if s == "ORB")

            print(f"\n  Combined NP: ${combined_np:+,.2f}  ({combined_np/DEPOSIT*100:+.1f}% ROI)")
            print(f"  Combined DD: {combined_dd_pct:.2f}%   "
                  f"NP/DD: {combined_np / max(combined_dd_pct, 0.5):+.0f}")
            print(f"  Trades: {len(all_deals)}")
            print(f"  Per-stream: FBO_S1=${s1_total:+,.0f}  FBO_S2=${s2_total:+,.0f}  ORB=${orb_total:+,.0f}")

            summary_rows.append({
                "risk": risk,
                "tgt": 9.0 * risk,
                "loss": 6.0 * risk,
                "np": combined_np,
                "dd": combined_dd_pct,
                "trades": len(all_deals),
                "s1": s1_total, "s2": s2_total, "orb": orb_total,
            })

        # Final comparison table
        print("\n" + "=" * 72)
        print("  FBORB RISK-LEVEL COMPARISON (Mar 14 -> Apr 25, $10k)")
        print("=" * 72)
        print(f"  {'Risk':>5}  {'Tgt':>5}  {'Loss':>5}  {'NP':>12}  {'ROI%':>7}  "
              f"{'DD%':>6}  {'NP/DD':>7}  {'Trades':>7}")
        print(f"  {'-'*72}")
        for r in summary_rows:
            print(f"  {r['risk']:>4.1f}%  {r['tgt']:>4.0f}%  {r['loss']:>4.0f}%  "
                  f"${r['np']:>+10,.0f}  "
                  f"{r['np']/DEPOSIT*100:>+6.1f}%  "
                  f"{r['dd']:>5.1f}%  "
                  f"{r['np']/max(r['dd'],0.5):>+7.0f}  "
                  f"{r['trades']:>7}")
        print()
        print(f"  {'Risk':>5}  {'FBO S1':>10}  {'FBO S2':>10}  {'ORB':>10}")
        print(f"  {'-'*45}")
        for r in summary_rows:
            print(f"  {r['risk']:>4.1f}%  ${r['s1']:>+8,.0f}  ${r['s2']:>+8,.0f}  ${r['orb']:>+8,.0f}")

    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
