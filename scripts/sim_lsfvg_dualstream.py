"""LSFVG dual-stream sanity: M15 + M30 on shared $10k.

Question: does adding an M30 LSFVG stream contribute uncorrelated alpha,
or does it just churn (overlap with M15 signals or fire on the same regime).

Test: run both streams independently on $10k each (for attribution), then
merge deals on a shared $10k account (live behavior). Report:
  - Per-stream NP, DD, trades, PF
  - Combined NP, DD on shared account
  - Pearson correlation of weekly PnL (low = uncorrelated = good)
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.lsfvg import LSFVGConfig
from zgb_sim.lsfvg_fast import simulate_fast


SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0


def main() -> int:
    start = datetime(2026, 2, 1, tzinfo=timezone.utc)
    end = datetime(2026, 4, 25, tzinfo=timezone.utc)

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
            tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
            volume_min=m["volume_min"], volume_max=m["volume_max"],
            volume_step=m["volume_step"],
        )
        ticks = load_ticks(SYMBOL, start, end)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m15 = load_bars(SYMBOL, "M15", start, end)
        m30 = load_bars(SYMBOL, "M30", start, end)

        print("=" * 78)
        print("  LSFVG DUAL-STREAM SANITY (Feb 1 -> Apr 25, $10k, 3% risk per stream)")
        print("=" * 78)

        # M15 stream — production validated
        cfg_a = LSFVGConfig(
            risk_pct=3.0, signal_tf_minutes=15, lookback_bars=10,
            min_fvg_pts=20, max_fvg_pts=5000, sweep_buffer_pts=30,
            rr_ratio=2.0, half_tp_ratio=0.5, pending_expire_bars=4,
            comment="LSFVG_A",
        )
        # M30 stream — same logic, scaled FVG bounds + sweep buf for wider TF
        cfg_b = LSFVGConfig(
            risk_pct=3.0, signal_tf_minutes=30, lookback_bars=10,
            min_fvg_pts=50, max_fvg_pts=8000, sweep_buffer_pts=60,
            rr_ratio=2.0, half_tp_ratio=0.5, pending_expire_bars=4,
            comment="LSFVG_B",
        )

        print(f"\n  Stream A (M15): Lk={cfg_a.lookback_bars} FVG=[{cfg_a.min_fvg_pts},"
              f"{cfg_a.max_fvg_pts}] SwBuf={cfg_a.sweep_buffer_pts} RR={cfg_a.rr_ratio} HTP={cfg_a.half_tp_ratio}")
        print(f"  Stream B (M30): Lk={cfg_b.lookback_bars} FVG=[{cfg_b.min_fvg_pts},"
              f"{cfg_b.max_fvg_pts}] SwBuf={cfg_b.sweep_buffer_pts} RR={cfg_b.rr_ratio} HTP={cfg_b.half_tp_ratio}")

        print("\n  --- Stream A (M15) standalone ---")
        r_a = simulate_fast(ticks, m15, m1, cfg_a, meta, initial_balance=DEPOSIT)
        print(f"  NP=${r_a.net_profit:>+8,.0f}  DD={r_a.max_drawdown_pct:>5.1f}%  "
              f"Trades={r_a.trades:>3}  TP/SL={r_a.tp_count}/{r_a.sl_count}  "
              f"PF={r_a.profit_factor:.2f}  WR={r_a.tp_count/r_a.trades*100:.1f}%")

        print("\n  --- Stream B (M30) standalone ---")
        r_b = simulate_fast(ticks, m30, m1, cfg_b, meta, initial_balance=DEPOSIT)
        wr_b = (r_b.tp_count / r_b.trades * 100) if r_b.trades > 0 else 0
        print(f"  NP=${r_b.net_profit:>+8,.0f}  DD={r_b.max_drawdown_pct:>5.1f}%  "
              f"Trades={r_b.trades:>3}  TP/SL={r_b.tp_count}/{r_b.sl_count}  "
              f"PF={r_b.profit_factor:.2f}  WR={wr_b:.1f}%")

        # ===== Merged deals on shared $10k =====
        all_deals = []
        for d in r_a.deals:
            if d.kind != "entry":
                all_deals.append((d.ts, "A", d.pnl))
        for d in r_b.deals:
            if d.kind != "entry":
                all_deals.append((d.ts, "B", d.pnl))
        all_deals.sort(key=lambda x: x[0])

        balance = DEPOSIT
        balance_max = DEPOSIT
        dd_abs = 0.0
        for _, _s, pnl in all_deals:
            balance += pnl
            if balance > balance_max:
                balance_max = balance
            cur = balance_max - balance
            if cur > dd_abs:
                dd_abs = cur
        combined_np = balance - DEPOSIT
        combined_dd_pct = (dd_abs / balance_max * 100.0) if balance_max > 0 else 0.0
        combined_ndd = (combined_np / dd_abs) if dd_abs > 0 else 0.0

        print("\n  --- COMBINED (shared $10k, deal-merge) ---")
        print(f"  NP=${combined_np:>+8,.0f}  ({combined_np/DEPOSIT*100:+.1f}% ROI)  "
              f"DD={combined_dd_pct:>5.1f}%  NP/DD={combined_ndd:.1f}  "
              f"Total trades={len(all_deals)}")
        print(f"    A contrib: ${sum(p for _,s,p in all_deals if s=='A'):>+8,.0f} "
              f"({sum(1 for _,s,_ in all_deals if s=='A')} trades)")
        print(f"    B contrib: ${sum(p for _,s,p in all_deals if s=='B'):>+8,.0f} "
              f"({sum(1 for _,s,_ in all_deals if s=='B')} trades)")

        # ===== Weekly PnL correlation (low = uncorrelated = good diversification) =====
        if all_deals:
            df = pd.DataFrame([(t, s, p) for t, s, p in all_deals], columns=["ts", "stream", "pnl"])
            df["ts"] = pd.to_datetime(df["ts"])
            df["week"] = df["ts"].dt.to_period("W").astype(str)
            weekly = df.pivot_table(index="week", columns="stream",
                                     values="pnl", aggfunc="sum", fill_value=0.0)
            if "A" in weekly.columns and "B" in weekly.columns and len(weekly) >= 3:
                corr = weekly["A"].corr(weekly["B"])
                print(f"\n  Weekly PnL correlation (A vs B): {corr:+.2f}  "
                      f"(<+0.3 = good diversification)")
            print(f"\n  Weekly PnL breakdown ({len(weekly)} weeks):")
            for week, row in weekly.iterrows():
                a = row.get("A", 0.0)
                b = row.get("B", 0.0)
                print(f"    {week}:  A=${a:>+7,.0f}  B=${b:>+7,.0f}  combined=${a+b:>+7,.0f}")

        # ===== Verdict =====
        print("\n  --- VERDICT ---")
        a_alone_ndd = (r_a.net_profit / r_a.max_drawdown) if r_a.max_drawdown > 0 else 0
        b_alone_ndd = (r_b.net_profit / r_b.max_drawdown) if r_b.max_drawdown > 0 else 0
        print(f"  A alone (M15) NP/DD: {a_alone_ndd:.2f}")
        print(f"  B alone (M30) NP/DD: {b_alone_ndd:.2f}")
        print(f"  Combined NP/DD     : {combined_ndd:.2f}")
        if combined_ndd > a_alone_ndd * 1.10:
            print(f"  -> ADDING M30 IMPROVES risk-adjusted return by "
                  f"{(combined_ndd/a_alone_ndd-1)*100:.0f}%. Worth building dual-stream EA.")
        elif combined_ndd > a_alone_ndd * 0.95:
            print(f"  -> NEUTRAL. M30 doesn't help much but doesn't hurt. "
                  f"Skip dual-stream.")
        else:
            print(f"  -> M30 DEGRADES risk-adjusted return by "
                  f"{(1-combined_ndd/a_alone_ndd)*100:.0f}%. Stay single-stream.")

    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
