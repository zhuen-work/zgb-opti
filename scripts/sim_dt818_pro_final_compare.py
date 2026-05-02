"""Final May 2 comparison: realistic 4-stream config (current FBO/LSFVG +
new ORB winner) vs prior baseline.

Decision rules applied:
  - ORB: passed P0, use new winner (Range=90, FixSL=800, RR=3.0)
  - FBO_S1, FBO_S2, LSFVG: failed P0, KEEP CURRENT PARAMS
  - EMP: disabled (still no viable winner)
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

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
WFO_BASELINE_RISK = 3.0


def run_combined(risk: float, ticks, m1, m5, m15, m30, meta, orb_new=True):
    fbo_s1 = FBOS1Config(risk_pct=risk, fractal_bars=8, take_profit_pts=25_000,
                          stop_loss_pts=10_000, half_tp_ratio=0.3, sma_period=10,
                          pending_expire_bars=2, signal_tf_minutes=30, comment="FBO_A")
    fbo_s2 = FBOS1Config(risk_pct=risk, fractal_bars=8, take_profit_pts=4_000,
                          stop_loss_pts=4_000, half_tp_ratio=0.6, sma_period=50,
                          pending_expire_bars=4, signal_tf_minutes=15, comment="FBO_B")
    if orb_new:
        # New WFO winner
        orb = ORBConfig(risk_pct=risk, range_minutes=90, buffer_pts=0,
                        min_range_pts=200, max_range_pts=5000,
                        fixed_sl_pts=800, rr_ratio=3.0, half_tp_ratio=0.0,
                        pending_expire_minutes=240,
                        daily_target_pct=9.0 * (risk / WFO_BASELINE_RISK),
                        daily_loss_pct=6.0 * (risk / WFO_BASELINE_RISK),
                        ldn_enabled=True, ldn_start_hour=7,
                        ny_enabled=True, ny_start_hour=13, comment="ORB")
    else:
        # Prior W70 winner
        orb = ORBConfig(risk_pct=risk, range_minutes=90, buffer_pts=0,
                        min_range_pts=200, max_range_pts=5000,
                        fixed_sl_pts=350, rr_ratio=2.0, half_tp_ratio=0.0,
                        pending_expire_minutes=240,
                        daily_target_pct=9.0 * (risk / WFO_BASELINE_RISK),
                        daily_loss_pct=6.0 * (risk / WFO_BASELINE_RISK),
                        ldn_enabled=True, ldn_start_hour=7,
                        ny_enabled=True, ny_start_hour=13, comment="ORB")
    lsfvg = LSFVGConfig(risk_pct=risk, signal_tf_minutes=15, lookback_bars=10,
                         min_fvg_pts=20, max_fvg_pts=5000, sweep_buffer_pts=30,
                         rr_ratio=2.0, half_tp_ratio=0.5, pending_expire_bars=4,
                         daily_target_pct=0.0, daily_loss_pct=0.0, comment="LSFVG")

    r_s1 = fbo_simulate(ticks, m30, m1, fbo_s1, meta, initial_balance=DEPOSIT)
    r_s2 = fbo_simulate(ticks, m15, m1, fbo_s2, meta, initial_balance=DEPOSIT)
    r_orb = orb_simulate(ticks, m5, m1, orb, meta, initial_balance=DEPOSIT)
    r_lsf = lsfvg_simulate(ticks, m15, m1, lsfvg, meta, initial_balance=DEPOSIT)

    deals = []
    for s, r in [("FBO_S1", r_s1), ("FBO_S2", r_s2), ("ORB", r_orb), ("LSFVG", r_lsf)]:
        for d in r.deals:
            if d.kind != "entry":
                deals.append((d.ts, s, d.pnl))
    deals.sort(key=lambda x: x[0])

    bal = DEPOSIT; bal_max = DEPOSIT; dd = 0.0
    for _, _s, p in deals:
        bal += p
        if bal > bal_max: bal_max = bal
        cur = bal_max - bal
        if cur > dd: dd = cur
    np_ = bal - DEPOSIT
    dd_pct = dd / bal_max * 100 if bal_max > 0 else 0
    ndd = np_ / dd if dd > 0 else 0

    by_stream = {}
    for _, s, p in deals:
        by_stream.setdefault(s, [0.0, 0])
        by_stream[s][0] += p
        by_stream[s][1] += 1
    return dict(np=np_, dd=dd, dd_pct=dd_pct, ndd=ndd, trades=len(deals), by=by_stream)


def main() -> int:
    start = datetime(2026, 2, 14, tzinfo=timezone.utc)
    end = datetime(2026, 5, 1, tzinfo=timezone.utc)
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

        print("=" * 100)
        print(f"  FINAL May 2 comparison: 4-stream (FBO/LSFVG current) + ORB new vs prior")
        print(f"  ({days}d, $10k, 70pt, EMP disabled)")
        print("=" * 100)

        for risk in (2.0, 3.0, 4.5):
            print(f"\n  === Risk {risk}% ===")
            old = run_combined(risk, ticks, m1, m5, m15, m30, meta, orb_new=False)
            new = run_combined(risk, ticks, m1, m5, m15, m30, meta, orb_new=True)
            print(f"  {'Variant':<22} {'NP':>10} {'ROI':>7} {'DD%':>6} {'NP/DD':>7} {'Trades':>7}")
            for label, r in (("BEFORE (ORB W70)", old), ("AFTER  (ORB May2)", new)):
                roi = r["np"] / DEPOSIT * 100
                print(f"  {label:<22} {r['np']:>+10,.0f} {roi:>+6.1f}% "
                      f"{r['dd_pct']:>5.1f}% {r['ndd']:>7.2f} {r['trades']:>7}")
            d_np = new["np"] - old["np"]
            d_dd = new["dd_pct"] - old["dd_pct"]
            d_ndd = new["ndd"] - old["ndd"]
            print(f"  Delta:                {d_np:>+10,.0f}        "
                  f"{d_dd:>+5.1f}pp {d_ndd:>+7.2f}")
            print(f"  Per-stream NP (AFTER):")
            for s in ("FBO_S1", "FBO_S2", "ORB", "LSFVG"):
                np_s, tr_s = new["by"].get(s, [0, 0])
                print(f"    {s:<7}  ${np_s:>+9,.0f}  ({tr_s} trades)")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
