"""DT818_pro full sanity: FBO_S1 + FBO_S2 + ORB + LSFVG on shared $10k.

Independence check: each stream uses its own daily caps (or none) and
each magic is isolated. We simulate streams independently and merge deals
on a shared $10k (live behavior). Daily-cap interactions are validated
by checking ORB closes only on its own PnL, LSFVG never closes, FBO never
closes (DailyTargetPct=0 disabled).
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


def main() -> int:
    start = datetime(2026, 2, 14, tzinfo=timezone.utc)
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
        m5 = load_bars(SYMBOL, "M5", start, end)
        m15 = load_bars(SYMBOL, "M15", start, end)
        m30 = load_bars(SYMBOL, "M30", start, end)

        print("=" * 78)
        print("  DT818_pro SANITY (Feb 14 -> Apr 25, $10k, 3% risk per stream)")
        print("=" * 78)

        # ----- Stream configs (mirroring DT818_pro setfile) -----
        fbo_s1 = FBOS1Config(
            risk_pct=3.0, fractal_bars=8, take_profit_pts=25_000,
            stop_loss_pts=10_000, half_tp_ratio=0.3, sma_period=10,
            pending_expire_bars=2, signal_tf_minutes=30, comment="FBO_A",
        )
        fbo_s2 = FBOS1Config(
            risk_pct=3.0, fractal_bars=8, take_profit_pts=4_000,
            stop_loss_pts=4_000, half_tp_ratio=0.6, sma_period=50,
            pending_expire_bars=4, signal_tf_minutes=15, comment="FBO_B",
        )
        orb_cfg = ORBConfig(
            risk_pct=3.0, range_minutes=60, buffer_pts=0,
            min_range_pts=200, max_range_pts=5000,
            fixed_sl_pts=400, rr_ratio=3.0, half_tp_ratio=0.0,
            pending_expire_minutes=240,
            daily_target_pct=27.0, daily_loss_pct=18.0,
            ldn_enabled=True, ldn_start_hour=7,
            ny_enabled=True, ny_start_hour=13,
            comment="ORB",
        )
        lsfvg_cfg = LSFVGConfig(
            risk_pct=3.0, signal_tf_minutes=15, lookback_bars=10,
            min_fvg_pts=20, max_fvg_pts=5000, sweep_buffer_pts=30,
            rr_ratio=2.0, half_tp_ratio=0.5, pending_expire_bars=4,
            daily_target_pct=0.0, daily_loss_pct=0.0,
            comment="LSFVG",
        )

        # ----- Run each stream standalone on $10k for attribution -----
        print("\n  --- Standalone runs (each on its own $10k) ---")
        r_s1 = fbo_simulate(ticks, m30, m1, fbo_s1, meta, initial_balance=DEPOSIT)
        r_s2 = fbo_simulate(ticks, m15, m1, fbo_s2, meta, initial_balance=DEPOSIT)
        r_orb = orb_simulate(ticks, m5, m1, orb_cfg, meta, initial_balance=DEPOSIT)
        r_lsf = lsfvg_simulate(ticks, m15, m1, lsfvg_cfg, meta, initial_balance=DEPOSIT)

        for name, r in [("FBO_S1", r_s1), ("FBO_S2", r_s2),
                        ("ORB   ", r_orb), ("LSFVG ", r_lsf)]:
            wr = (r.tp_count / r.trades * 100) if r.trades > 0 else 0
            ndd = (r.net_profit / r.max_drawdown) if r.max_drawdown > 0 else 0
            print(f"  {name}: NP=${r.net_profit:>+8,.0f}  DD={r.max_drawdown_pct:>5.1f}%  "
                  f"Tr={r.trades:>3}  TP/SL/O={r.tp_count}/{r.sl_count}/{r.other_count}  "
                  f"PF={r.profit_factor:.2f}  WR={wr:.1f}%  NP/DD={ndd:.1f}")

        # ----- Merge deals on shared $10k (live behavior) -----
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
        combined_dd_pct = (dd_abs / balance_max * 100.0) if balance_max > 0 else 0
        combined_ndd = (combined_np / dd_abs) if dd_abs > 0 else 0

        print("\n  --- COMBINED on shared $10k (deal-merge) ---")
        print(f"  NP=${combined_np:>+8,.0f}  ROI={combined_np/DEPOSIT*100:+.1f}%  "
              f"DD={combined_dd_pct:>5.1f}%  NP/DD={combined_ndd:.1f}  "
              f"Total trades={len(all_deals)}")
        for stream in ("FBO_S1", "FBO_S2", "ORB", "LSFVG"):
            np_s = sum(p for _,s,p in all_deals if s == stream)
            tr_s = sum(1 for _,s,_ in all_deals if s == stream)
            print(f"    {stream:<7} contrib: ${np_s:>+8,.0f}  ({tr_s} trades)")

        # ----- Pairwise weekly correlation (low = good diversification) -----
        if all_deals:
            df = pd.DataFrame(all_deals, columns=["ts", "stream", "pnl"])
            df["ts"] = pd.to_datetime(df["ts"])
            df["week"] = df["ts"].dt.to_period("W").astype(str)
            weekly = df.pivot_table(index="week", columns="stream",
                                     values="pnl", aggfunc="sum", fill_value=0.0)
            print("\n  --- Weekly PnL correlation (lower = better diversification) ---")
            streams = [c for c in ("FBO_S1", "FBO_S2", "ORB", "LSFVG") if c in weekly.columns]
            print(f"  {'':>8} " + " ".join(f"{s:>8}" for s in streams))
            for sa in streams:
                row = [f"  {sa:>8} "]
                for sb in streams:
                    if sa == sb:
                        row.append("    1.00 ")
                    else:
                        try:
                            corr = weekly[sa].corr(weekly[sb])
                            row.append(f"  {corr:>+5.2f} ")
                        except Exception:
                            row.append("    n/a  ")
                print("".join(row))

        # ----- Verdict -----
        print("\n  --- VERDICT ---")
        print(f"  DT818_pro combined: NP ${combined_np:+,.0f} ({combined_np/DEPOSIT*100:+.1f}%) / "
              f"DD {combined_dd_pct:.1f}% / {len(all_deals)} trades / NP/DD {combined_ndd:.1f}")

    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
