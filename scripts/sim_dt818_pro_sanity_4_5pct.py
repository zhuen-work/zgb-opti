"""DT818_pro 4.5% combined sanity ET — with new ORB params (W70 winner).

Streams (all 4.5% risk per trade, sharing $10k):
  FBO_S1 (M30, magic 1000)  — caps disabled
  FBO_S2 (M15, magic 1000)  — caps disabled
  ORB    (M5,  magic 2000)  — Tgt=40.5%, Loss=27% (= 9/6 × 4.5)
                              NEW: Range=90, FixSL=350, RR=2.0, HTP=0.0
  LSFVG  (M15, magic 3000)  — caps disabled
"""
from __future__ import annotations

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
from zgb_sim.lsfvg import LSFVGConfig
from zgb_sim.lsfvg_fast import simulate_fast as lsfvg_simulate

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
RISK = 4.5


def main() -> int:
    start = datetime(2026, 2, 14, tzinfo=timezone.utc)
    end = datetime(2026, 4, 25, tzinfo=timezone.utc)

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

        print("=" * 84)
        print("  DT818_pro 4.5% SANITY (Feb 14 -> Apr 25, $10k, 4.5% per stream, spread=70)")
        print("  ORB updated to W70 winner: Range=90 FixSL=350 RR=2.0 HTP=0.0 Tgt=13.5/Loss=9")
        print("=" * 84)

        fbo_s1 = FBOS1Config(
            risk_pct=RISK, fractal_bars=8, take_profit_pts=25_000,
            stop_loss_pts=10_000, half_tp_ratio=0.3, sma_period=10,
            pending_expire_bars=2, signal_tf_minutes=30, comment="FBO_A",
        )
        fbo_s2 = FBOS1Config(
            risk_pct=RISK, fractal_bars=8, take_profit_pts=4_000,
            stop_loss_pts=4_000, half_tp_ratio=0.6, sma_period=50,
            pending_expire_bars=4, signal_tf_minutes=15, comment="FBO_B",
        )
        orb_cfg = ORBConfig(
            risk_pct=RISK, range_minutes=90, buffer_pts=0,
            min_range_pts=200, max_range_pts=5000,
            fixed_sl_pts=350, rr_ratio=2.0, half_tp_ratio=0.0,
            pending_expire_minutes=240,
            daily_target_pct=13.5, daily_loss_pct=9.0,
            ldn_enabled=True, ldn_start_hour=7,
            ny_enabled=True, ny_start_hour=13,
            comment="ORB",
        )
        lsfvg_cfg = LSFVGConfig(
            risk_pct=RISK, signal_tf_minutes=15, lookback_bars=10,
            min_fvg_pts=20, max_fvg_pts=5000, sweep_buffer_pts=30,
            rr_ratio=2.0, half_tp_ratio=0.5, pending_expire_bars=4,
            daily_target_pct=0.0, daily_loss_pct=0.0,
            comment="LSFVG",
        )

        print("\n  --- Standalone runs (each on its own $10k) ---")
        r_s1 = fbo_simulate(ticks, m30, m1, fbo_s1, meta, initial_balance=DEPOSIT)
        r_s2 = fbo_simulate(ticks, m15, m1, fbo_s2, meta, initial_balance=DEPOSIT)
        r_orb = orb_simulate(ticks, m5, m1, orb_cfg, meta, initial_balance=DEPOSIT)
        r_lsf = lsfvg_simulate(ticks, m15, m1, lsfvg_cfg, meta, initial_balance=DEPOSIT)

        for name, r in [("FBO_S1", r_s1), ("FBO_S2", r_s2),
                        ("ORB   ", r_orb), ("LSFVG ", r_lsf)]:
            wr = (r.tp_count / r.trades * 100) if r.trades > 0 else 0
            ndd = (r.net_profit / r.max_drawdown) if r.max_drawdown > 0 else 0
            print(f"  {name}: NP=${r.net_profit:>+9,.0f}  DD={r.max_drawdown_pct:>5.1f}%  "
                  f"Tr={r.trades:>3}  TP/SL/O={r.tp_count}/{r.sl_count}/{r.other_count}  "
                  f"PF={r.profit_factor:.2f}  WR={wr:.1f}%  NP/DD={ndd:.2f}")

        # Merge deals on shared $10k
        all_deals = []
        for d in r_s1.deals:
            if d.kind != "entry": all_deals.append((d.ts, "FBO_S1", d.pnl))
        for d in r_s2.deals:
            if d.kind != "entry": all_deals.append((d.ts, "FBO_S2", d.pnl))
        for d in r_orb.deals:
            if d.kind != "entry": all_deals.append((d.ts, "ORB", d.pnl))
        for d in r_lsf.deals:
            if d.kind != "entry": all_deals.append((d.ts, "LSFVG", d.pnl))
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

        print("\n  --- COMBINED on shared $10k (deal-merge) ---")
        print(f"  NP=${np_combined:>+10,.0f}  ROI={np_combined/DEPOSIT*100:+.1f}%  "
              f"DD={dd_pct_combined:>5.1f}% (${dd_abs:,.0f})  NP/DD={ndd_combined:.2f}  "
              f"Total trades={len(all_deals)}")
        for stream in ("FBO_S1", "FBO_S2", "ORB", "LSFVG"):
            np_s = sum(p for _,s,p in all_deals if s == stream)
            tr_s = sum(1 for _,s,_ in all_deals if s == stream)
            print(f"    {stream:<7} contrib: ${np_s:>+10,.0f}  ({tr_s} trades)")

        # Comparison vs prior 4.5pct setfile values
        print("\n  --- vs prior 4.5pct sanity (old ORB R60/SL400/RR3.0) ---")
        prior_np = 46_113
        prior_dd_pct = 25.5
        prior_ndd = 2.58
        prior_orb = 26_848
        cur_orb = sum(p for _,s,p in all_deals if s == "ORB")
        print(f"  COMBINED:  NP {np_combined-prior_np:+,.0f}  DD {dd_pct_combined-prior_dd_pct:+.1f}pp  "
              f"NP/DD {ndd_combined-prior_ndd:+.2f}")
        print(f"  ORB only:  NP {cur_orb-prior_orb:+,.0f}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
