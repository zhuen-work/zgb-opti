"""1-week OOS test: Apr 26 -> May 2, 2026 (just past Apr 25 IS).

5-stream combined, 3% risk, 70pt spread, $10k.
Compares to extrapolated IS rate (NP+$37,680 over 70 days = ~$3.7k/week expected).
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


def main() -> int:
    start = datetime(2026, 4, 26, tzinfo=timezone.utc)
    end = datetime(2026, 5, 2, tzinfo=timezone.utc)
    days = (end - start).days

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        print("Loading data Apr 26 -> May 2 (may need to pull from MT5 if not cached)...")
        ticks = load_ticks(SYMBOL, start, end)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)
        m15 = load_bars(SYMBOL, "M15", start, end)
        m30 = load_bars(SYMBOL, "M30", start, end)

        print("=" * 100)
        print(f"  DT818_pro OOS test: Apr 26 -> May 2 ({days}d, $10k, 3% risk, 70pt)")
        print(f"  Reference IS rate (Feb 14->Apr 25 70d): NP +$37,680  =>  ~$3,768/week pro-rated")
        print("=" * 100)

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
        ep_cfg = EMAPullbackConfig(
            risk_pct=RISK, signal_tf_minutes=15, ema_period=50,
            lookback_bars=3, pullback_band_pts=150,
            entry_buffer_pts=0, sl_buffer_pts=30,
            rr_ratio=2.0, half_tp_ratio=0.0, pending_expire_bars=3,
            daily_target_pct=0.0, daily_loss_pct=6.0, comment="EMAPullback",
        )

        print("\n  Running 5 streams standalone on $10k...")
        r_s1 = fbo_simulate(ticks, m30, m1, fbo_s1_cfg, meta, initial_balance=DEPOSIT)
        r_s2 = fbo_simulate(ticks, m15, m1, fbo_s2_cfg, meta, initial_balance=DEPOSIT)
        r_orb = orb_simulate(ticks, m5, m1, orb_cfg, meta, initial_balance=DEPOSIT)
        r_lsf = lsfvg_simulate(ticks, m15, m1, lsfvg_cfg, meta, initial_balance=DEPOSIT)
        r_ep = ep_simulate(ticks, m15, m1, ep_cfg, meta, initial_balance=DEPOSIT)

        print(f"\n  Per-stream standalone:")
        for name, r in [("FBO_S1", r_s1), ("FBO_S2", r_s2),
                        ("ORB   ", r_orb), ("LSFVG ", r_lsf),
                        ("EMP   ", r_ep)]:
            wr = (r.tp_count / r.trades * 100) if r.trades > 0 else 0
            ndd = (r.net_profit / r.max_drawdown) if r.max_drawdown > 0 else 0
            print(f"    {name}: NP=${r.net_profit:>+8,.0f}  DD={r.max_drawdown_pct:>5.1f}%  "
                  f"Tr={r.trades:>3}  TP/SL/O={r.tp_count}/{r.sl_count}/{r.other_count}  "
                  f"PF={r.profit_factor:.2f}  WR={wr:.1f}%  NP/DD={ndd:.2f}")

        # Merge on shared $10k
        all_deals = []
        for d in r_s1.deals:
            if d.kind != "entry": all_deals.append((d.ts, "FBO_S1", d.pnl))
        for d in r_s2.deals:
            if d.kind != "entry": all_deals.append((d.ts, "FBO_S2", d.pnl))
        for d in r_orb.deals:
            if d.kind != "entry": all_deals.append((d.ts, "ORB", d.pnl))
        for d in r_lsf.deals:
            if d.kind != "entry": all_deals.append((d.ts, "LSFVG", d.pnl))
        for d in r_ep.deals:
            if d.kind != "entry": all_deals.append((d.ts, "EMAPullback", d.pnl))
        all_deals.sort(key=lambda x: x[0])

        bal = DEPOSIT
        bal_max = DEPOSIT
        dd_abs = 0.0
        for _, _s, pnl in all_deals:
            bal += pnl
            if bal > bal_max: bal_max = bal
            cur = bal_max - bal
            if cur > dd_abs: dd_abs = cur
        np_combined = bal - DEPOSIT
        dd_pct_combined = (dd_abs / bal_max * 100.0) if bal_max > 0 else 0
        ndd_combined = (np_combined / dd_abs) if dd_abs > 0 else 0

        print(f"\n  Combined ({days}d):")
        print(f"  {'Days':>5} {'NP':>10} {'ROI':>8} {'DD%':>6} {'NP/DD':>7} "
              f"{'Trades':>7} {'Tr/day':>7}")
        print("-" * 70)
        roi = np_combined / DEPOSIT * 100
        print(f"  {days:>5} {np_combined:>+10,.0f} {roi:>+7.1f}% "
              f"{dd_pct_combined:>5.1f}% {ndd_combined:>7.2f} "
              f"{len(all_deals):>7} {len(all_deals)/days:>7.2f}")

        print(f"\n  Per-stream contribution (combined):")
        for s in ("FBO_S1", "FBO_S2", "ORB", "LSFVG", "EMAPullback"):
            np_s = sum(p for _,ss,p in all_deals if ss == s)
            tr_s = sum(1 for _,ss,_ in all_deals if ss == s)
            print(f"    {s:<12}  ${np_s:>+8,.0f}  ({tr_s} trades)")

        # Compare to IS-pro-rated expectation
        is_weekly_rate = 37680 / 70 * 7  # ~$3,768
        is_weekly_dd = 9.7  # IS combined DD% (point estimate)
        actual_weekly = np_combined / days * 7
        print(f"\n  vs IS pro-rated:")
        print(f"    Expected ~$3,768/week (IS NP +$37,680 / 70d × 7), DD ~9.7%")
        print(f"    Actual:  ${actual_weekly:+,.0f}/week, DD {dd_pct_combined:.1f}%")
        print(f"    Ratio:   {actual_weekly/is_weekly_rate*100:+.0f}% of IS rate")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
