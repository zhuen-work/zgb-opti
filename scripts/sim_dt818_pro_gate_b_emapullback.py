"""Gate B: combined sanity comparing WFO winner vs smoke winner.

Conditions: 3% risk, 70pt spread, $10k, Feb 14 -> Apr 25 (70 days).
Decision: WFO winner must >= smoke winner in combined NP, otherwise use smoke.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.fbo_s1 import FBOS1Config
from zgb_sim.fbo_s1_fast import simulate_fast as fbo_simulate
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.lsfvg import LSFVGConfig
from zgb_sim.lsfvg_fast import simulate_fast as lsfvg_simulate
from zgb_sim.ema_pullback import EMAPullbackConfig
from zgb_sim.ema_pullback_fast import simulate_fast as ep_simulate

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
RISK = 3.0


def _merge(*deal_lists_with_names):
    flat = []
    for name, deals in deal_lists_with_names:
        for d in deals:
            if d.kind != "entry":
                flat.append((d.ts, name, d.pnl))
    flat.sort(key=lambda t: t[0])
    return flat


def _metrics(deals_flat, deposit=DEPOSIT):
    bal = deposit
    bal_max = deposit
    dd = 0.0
    for _, _s, p in deals_flat:
        bal += p
        if bal > bal_max: bal_max = bal
        cur = bal_max - bal
        if cur > dd: dd = cur
    np_ = bal - deposit
    dd_pct = (dd / bal_max * 100.0) if bal_max > 0 else 0.0
    ndd = (np_ / dd) if dd > 0 else 0.0
    by_stream = {}
    for _, s, p in deals_flat:
        by_stream.setdefault(s, [0.0, 0])
        by_stream[s][0] += p
        by_stream[s][1] += 1
    return dict(np=np_, dd=dd, dd_pct=dd_pct, ndd=ndd,
                trades=len(deals_flat), by_stream=by_stream)


def main() -> int:
    start = datetime(2026, 2, 14, tzinfo=timezone.utc)
    end = datetime(2026, 4, 25, tzinfo=timezone.utc)
    days = (end - start).days

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        ticks = load_ticks(SYMBOL, start, end)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)
        m15 = load_bars(SYMBOL, "M15", start, end)
        m30 = load_bars(SYMBOL, "M30", start, end)

        print("=" * 92)
        print(f"  GATE B: 4-stream baseline | 5-stream w/ smoke | 5-stream w/ WFO winner")
        print(f"  ({days}d, $10k, 3% risk, 70pt)")
        print("=" * 92)

        fbo_s1_cfg = FBOS1Config(
            risk_pct=RISK, fractal_bars=8, take_profit_pts=25_000,
            stop_loss_pts=10_000, half_tp_ratio=0.3, sma_period=10,
            pending_expire_bars=2, signal_tf_minutes=30, comment="FBO_A",
        )
        fbo_s2_cfg = FBOS1Config(
            risk_pct=RISK, fractal_bars=8, take_profit_pts=4_000,
            stop_loss_pts=4_000, half_tp_ratio=0.6, sma_period=50,
            pending_expire_bars=4, signal_tf_minutes=15, comment="FBO_B",
        )
        orb_cfg = ORBConfig(
            risk_pct=RISK, range_minutes=90, buffer_pts=0,
            min_range_pts=200, max_range_pts=5000,
            fixed_sl_pts=350, rr_ratio=2.0, half_tp_ratio=0.0,
            pending_expire_minutes=240,
            daily_target_pct=9.0, daily_loss_pct=6.0,
            ldn_enabled=True, ldn_start_hour=7,
            ny_enabled=True, ny_start_hour=13,
            comment="ORB",
        )
        lsfvg_cfg = LSFVGConfig(
            risk_pct=RISK, signal_tf_minutes=15, lookback_bars=10,
            min_fvg_pts=20, max_fvg_pts=5000, sweep_buffer_pts=30,
            rr_ratio=2.0, half_tp_ratio=0.5, pending_expire_bars=4,
            daily_target_pct=0.0, daily_loss_pct=0.0, comment="LSFVG",
        )
        ep_smoke_cfg = EMAPullbackConfig(
            risk_pct=RISK, signal_tf_minutes=15, ema_period=50,
            lookback_bars=3, pullback_band_pts=100,
            entry_buffer_pts=0, sl_buffer_pts=50,
            rr_ratio=2.0, half_tp_ratio=0.0,
            pending_expire_bars=3,
            daily_target_pct=0.0, daily_loss_pct=0.0,
            comment="EMAPullback",
        )
        ep_wfo_cfg = EMAPullbackConfig(
            risk_pct=RISK, signal_tf_minutes=15, ema_period=50,
            lookback_bars=3, pullback_band_pts=150,
            entry_buffer_pts=0, sl_buffer_pts=30,
            rr_ratio=2.0, half_tp_ratio=0.0,
            pending_expire_bars=3,
            daily_target_pct=0.0, daily_loss_pct=6.0,
            comment="EMAPullback",
        )

        print("\n  Running 4 existing streams...")
        r_s1 = fbo_simulate(ticks, m30, m1, fbo_s1_cfg, meta, initial_balance=DEPOSIT)
        r_s2 = fbo_simulate(ticks, m15, m1, fbo_s2_cfg, meta, initial_balance=DEPOSIT)
        r_orb = orb_simulate(ticks, m5, m1, orb_cfg, meta, initial_balance=DEPOSIT)
        r_lsf = lsfvg_simulate(ticks, m15, m1, lsfvg_cfg, meta, initial_balance=DEPOSIT)
        print("  Running EMAPullback (smoke winner)...")
        r_ep_smoke = ep_simulate(ticks, m15, m1, ep_smoke_cfg, meta, initial_balance=DEPOSIT)
        print("  Running EMAPullback (WFO winner)...")
        r_ep_wfo = ep_simulate(ticks, m15, m1, ep_wfo_cfg, meta, initial_balance=DEPOSIT)

        merged_4 = _merge(("FBO_S1", r_s1.deals), ("FBO_S2", r_s2.deals),
                          ("ORB", r_orb.deals), ("LSFVG", r_lsf.deals))
        m4 = _metrics(merged_4)

        merged_5_smoke = _merge(("FBO_S1", r_s1.deals), ("FBO_S2", r_s2.deals),
                                ("ORB", r_orb.deals), ("LSFVG", r_lsf.deals),
                                ("EMAPullback", r_ep_smoke.deals))
        m5_smoke = _metrics(merged_5_smoke)

        merged_5_wfo = _merge(("FBO_S1", r_s1.deals), ("FBO_S2", r_s2.deals),
                              ("ORB", r_orb.deals), ("LSFVG", r_lsf.deals),
                              ("EMAPullback", r_ep_wfo.deals))
        m5_wfo = _metrics(merged_5_wfo)

        print(f"\n  {'Variant':<22} {'Days':>5} {'NP':>10} {'ROI':>8} {'DD%':>6} "
              f"{'NP/DD':>7} {'Trades':>7} {'Tr/day':>7} {'EP NP':>9}")
        print("-" * 92)
        for label, mm in (("4-stream baseline", m4),
                          ("5-stream (+EP smoke)", m5_smoke),
                          ("5-stream (+EP WFO)", m5_wfo)):
            roi = mm["np"] / DEPOSIT * 100
            ep_np = mm["by_stream"].get("EMAPullback", [0, 0])[0]
            print(f"  {label:<22} {days:>5} {mm['np']:>+10,.0f} {roi:>+7.1f}% "
                  f"{mm['dd_pct']:>5.1f}% {mm['ndd']:>7.2f} "
                  f"{mm['trades']:>7} {mm['trades']/days:>7.2f} "
                  f"${ep_np:>+8,.0f}")

        # Per-stream contribution for WFO variant
        print("\n  Per-stream NP (5-stream WFO variant):")
        for s in ("FBO_S1", "FBO_S2", "ORB", "LSFVG", "EMAPullback"):
            if s in m5_wfo["by_stream"]:
                np_s, tr_s = m5_wfo["by_stream"][s]
                print(f"    {s:<12}  ${np_s:>+9,.0f}  ({tr_s} trades)")

        d_wfo_smoke_np = m5_wfo["np"] - m5_smoke["np"]
        d_wfo_smoke_dd = m5_wfo["dd_pct"] - m5_smoke["dd_pct"]
        d_wfo_4_np = m5_wfo["np"] - m4["np"]
        d_smoke_4_np = m5_smoke["np"] - m4["np"]

        print(f"\n  Delta (5-WFO vs 5-smoke):  NP {d_wfo_smoke_np:+,.0f}  "
              f"DD {d_wfo_smoke_dd:+.1f}pp")
        print(f"  Delta (5-WFO vs 4-baseline): NP {d_wfo_4_np:+,.0f}  "
              f"DD {m5_wfo['dd_pct']-m4['dd_pct']:+.1f}pp  "
              f"NP/DD {m5_wfo['ndd']-m4['ndd']:+.2f}")
        print(f"  Delta (5-smoke vs 4-baseline): NP {d_smoke_4_np:+,.0f}")

        print(f"\n  ===== GATE B DECISION =====")
        if m5_wfo["np"] >= m5_smoke["np"]:
            print(f"  PASS: WFO winner (${m5_wfo['np']:+,.0f}) >= smoke winner (${m5_smoke['np']:+,.0f}) -- USE WFO PARAMS")
        elif m5_wfo["np"] >= m5_smoke["np"] - 1000:
            print(f"  CLOSE: WFO winner within $1k of smoke -- prefer WFO (more robust)")
        else:
            print(f"  REGRESS: WFO winner (${m5_wfo['np']:+,.0f}) < smoke winner (${m5_smoke['np']:+,.0f}) -- USE SMOKE PARAMS")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
