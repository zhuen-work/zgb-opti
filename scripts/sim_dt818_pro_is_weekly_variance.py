"""IS week-by-week variance check.

Shows that the 'IS pro-rated $3,768/week' is a mean, not a guarantee.
Computes actual per-week combined NP across the IS period (Feb 14 -> Apr 25)
to reveal how lumpy weekly returns really are.
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
from zgb_sim.ema_pullback import EMAPullbackConfig
from zgb_sim.ema_pullback_fast import simulate_fast as ep_simulate

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
RISK = 3.0


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

        fbo_s1 = FBOS1Config(risk_pct=RISK, fractal_bars=8, take_profit_pts=25_000,
                              stop_loss_pts=10_000, half_tp_ratio=0.3, sma_period=10,
                              pending_expire_bars=2, signal_tf_minutes=30, comment="FBO_A")
        fbo_s2 = FBOS1Config(risk_pct=RISK, fractal_bars=8, take_profit_pts=4_000,
                              stop_loss_pts=4_000, half_tp_ratio=0.6, sma_period=50,
                              pending_expire_bars=4, signal_tf_minutes=15, comment="FBO_B")
        orb = ORBConfig(risk_pct=RISK, range_minutes=90, buffer_pts=0,
                        min_range_pts=200, max_range_pts=5000,
                        fixed_sl_pts=350, rr_ratio=2.0, half_tp_ratio=0.0,
                        pending_expire_minutes=240,
                        daily_target_pct=9.0, daily_loss_pct=6.0,
                        ldn_enabled=True, ldn_start_hour=7,
                        ny_enabled=True, ny_start_hour=13, comment="ORB")
        lsfvg = LSFVGConfig(risk_pct=RISK, signal_tf_minutes=15, lookback_bars=10,
                             min_fvg_pts=20, max_fvg_pts=5000, sweep_buffer_pts=30,
                             rr_ratio=2.0, half_tp_ratio=0.5, pending_expire_bars=4,
                             daily_target_pct=0.0, daily_loss_pct=0.0, comment="LSFVG")
        ep = EMAPullbackConfig(risk_pct=RISK, signal_tf_minutes=15, ema_period=50,
                                lookback_bars=3, pullback_band_pts=150,
                                entry_buffer_pts=0, sl_buffer_pts=30,
                                rr_ratio=2.0, half_tp_ratio=0.0, pending_expire_bars=3,
                                daily_target_pct=0.0, daily_loss_pct=6.0, comment="EMP")

        print("Loading 5 streams over IS (Feb 14 -> Apr 25)...")
        r_s1 = fbo_simulate(ticks, m30, m1, fbo_s1, meta, initial_balance=DEPOSIT)
        r_s2 = fbo_simulate(ticks, m15, m1, fbo_s2, meta, initial_balance=DEPOSIT)
        r_orb = orb_simulate(ticks, m5, m1, orb, meta, initial_balance=DEPOSIT)
        r_lsf = lsfvg_simulate(ticks, m15, m1, lsfvg, meta, initial_balance=DEPOSIT)
        r_ep = ep_simulate(ticks, m15, m1, ep, meta, initial_balance=DEPOSIT)

        # Aggregate deals per week per stream
        all_deals = []
        for s, r in [("FBO_S1", r_s1), ("FBO_S2", r_s2), ("ORB", r_orb),
                     ("LSFVG", r_lsf), ("EMP", r_ep)]:
            for d in r.deals:
                if d.kind != "entry":
                    all_deals.append((d.ts, s, d.pnl))
        df = pd.DataFrame(all_deals, columns=["ts", "stream", "pnl"])
        df["ts"] = pd.to_datetime(df["ts"])
        if df["ts"].dt.tz is not None:
            df["ts"] = df["ts"].dt.tz_convert("UTC").dt.tz_localize(None)
        # Use ISO week starting Monday for cleaner buckets
        df["week_start"] = df["ts"].dt.to_period("W-SUN").apply(lambda p: p.start_time.date())

        # Per-stream weekly NP table
        weekly = df.pivot_table(index="week_start", columns="stream", values="pnl",
                                 aggfunc="sum", fill_value=0.0)
        for col in ("FBO_S1", "FBO_S2", "ORB", "LSFVG", "EMP"):
            if col not in weekly.columns:
                weekly[col] = 0.0
        weekly = weekly[["FBO_S1", "FBO_S2", "ORB", "LSFVG", "EMP"]]
        weekly["COMBINED"] = weekly.sum(axis=1)

        print("\n" + "=" * 100)
        print(f"  IS WEEKLY NP per stream (Feb 14 -> Apr 25, 3% risk, 5 standalone $10k)")
        print(f"  Mean weekly NP: ${weekly['COMBINED'].mean():,.0f}  (used as the 'pro-rated' baseline)")
        print(f"  Std weekly NP:  ${weekly['COMBINED'].std():,.0f}")
        print("=" * 100)
        print(f"\n  {'Week start':<12} " + " ".join(f"{c:>10}" for c in weekly.columns))
        for ws, row in weekly.iterrows():
            cells = " ".join(f"${row[c]:>+9,.0f}" for c in weekly.columns)
            print(f"  {str(ws):<12} {cells}")

        print(f"\n  COMBINED weekly stats:")
        print(f"    Min:    ${weekly['COMBINED'].min():>+9,.0f}")
        print(f"    Q25:    ${weekly['COMBINED'].quantile(0.25):>+9,.0f}")
        print(f"    Median: ${weekly['COMBINED'].median():>+9,.0f}")
        print(f"    Mean:   ${weekly['COMBINED'].mean():>+9,.0f}")
        print(f"    Q75:    ${weekly['COMBINED'].quantile(0.75):>+9,.0f}")
        print(f"    Max:    ${weekly['COMBINED'].max():>+9,.0f}")
        n_neg = (weekly["COMBINED"] < 0).sum()
        n_pos = (weekly["COMBINED"] > 0).sum()
        print(f"    Profitable weeks: {n_pos}/{len(weekly)}  Losing weeks: {n_neg}/{len(weekly)}")

        # Per-stream weekly stats
        print(f"\n  Per-stream weekly stats:")
        print(f"  {'Stream':<10} {'Min':>10} {'Med':>10} {'Mean':>10} {'Max':>10} {'Std':>10} {'-wks':>5}")
        for s in ("FBO_S1", "FBO_S2", "ORB", "LSFVG", "EMP", "COMBINED"):
            col = weekly[s]
            print(f"  {s:<10} ${col.min():>+9,.0f} ${col.median():>+9,.0f} "
                  f"${col.mean():>+9,.0f} ${col.max():>+9,.0f} "
                  f"${col.std():>+9,.0f} {(col<0).sum():>5}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
