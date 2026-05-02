"""HourMo (Hour-of-Day Momentum) smoke test — 4 corner combos.

Default conditions: 3% risk, 70pt spread, $10k, Feb 14 -> Apr 25 (70 days).
Trigger hours: 02, 08, 16 UTC (highest-edge hours from 2026 study).

Computes weekly correlation vs ORB, FBO_S2, EMP for diversification check.
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
from zgb_sim.hourmo import HourMoConfig, simulate as hm_simulate
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.fbo_s1 import FBOS1Config
from zgb_sim.fbo_s1_fast import simulate_fast as fbo_simulate
from zgb_sim.ema_pullback import EMAPullbackConfig
from zgb_sim.ema_pullback_fast import simulate_fast as ep_simulate

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
RISK = 3.0


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

        print("=" * 96)
        print(f"  HourMo smoke probe ({days}d, $10k, 3% risk, 70pt)")
        print(f"  Trigger hours UTC: 02, 08, 16  (top edge hours from 2026 study)")
        print("=" * 96)

        # 4 corner combos: (min_signal_pts, sl_buf, RR)
        combos = [
            (400, 50,  1.5),  # tight signal threshold, tight SL, low RR
            (600, 100, 2.0),  # balanced
            (800, 100, 2.0),  # study's p75 threshold
            (600, 150, 2.5),  # mid-signal, wider SL, higher RR
        ]
        results = []
        for min_sig, sl_buf, rr in combos:
            cfg = HourMoConfig(
                risk_pct=RISK, signal_tf_minutes=15,
                trigger_hours_utc=(2, 8, 16),
                min_signal_pts=min_sig,
                entry_buffer_pts=0,
                sl_buffer_pts=sl_buf,
                rr_ratio=rr, half_tp_ratio=0.0,
                pending_expire_bars=2,
                daily_target_pct=0.0, daily_loss_pct=6.0,
                comment="HourMo",
            )
            r = hm_simulate(ticks, m15, m1, cfg, meta, initial_balance=DEPOSIT)
            results.append((min_sig, sl_buf, rr, r))

        print(f"\n  {'MinSig':>6} {'SLBuf':>5} {'RR':>5} {'Days':>5} "
              f"{'NP':>9} {'ROI%':>7} {'DD%':>6} {'NP/DD':>7} "
              f"{'Tr':>4} {'TP':>4} {'SL':>4} {'WR%':>5} {'PF':>5}")
        rows = []
        for min_sig, sl_buf, rr, r in results:
            ndd = (r.net_profit / r.max_drawdown) if r.max_drawdown > 0 else 0
            wr = (r.tp_count / r.trades * 100) if r.trades > 0 else 0
            roi = r.net_profit / DEPOSIT * 100
            print(f"  {min_sig:>6} {sl_buf:>5} {rr:>5.1f} {days:>5} "
                  f"{r.net_profit:>+9,.0f} {roi:>+6.1f}% "
                  f"{r.max_drawdown_pct:>5.1f}% {ndd:>7.2f} "
                  f"{r.trades:>4} {r.tp_count:>4} {r.sl_count:>4} {wr:>4.1f}% "
                  f"{r.profit_factor:>5.2f}")
            rows.append((min_sig, sl_buf, rr, r))

        n_prof = sum(1 for *_, r in rows if r.net_profit > 0)
        print(f"\n  Profitable combos: {n_prof}/{len(rows)}")

        if n_prof > 0:
            best = max(rows, key=lambda t: (t[3].net_profit / t[3].max_drawdown) if t[3].max_drawdown > 0 else 0)
            print(f"\n  Best combo: MinSig={best[0]} SLBuf={best[1]} RR={best[2]}")

            # Run reference streams for correlation
            orb_cfg = ORBConfig(risk_pct=RISK, range_minutes=90, buffer_pts=0,
                                min_range_pts=200, max_range_pts=5000,
                                fixed_sl_pts=350, rr_ratio=2.0, half_tp_ratio=0.0,
                                pending_expire_minutes=240,
                                daily_target_pct=9.0, daily_loss_pct=6.0,
                                ldn_enabled=True, ldn_start_hour=7,
                                ny_enabled=True, ny_start_hour=13, comment="ORB")
            r_orb = orb_simulate(ticks, m5, m1, orb_cfg, meta, initial_balance=DEPOSIT)

            fbo_s2_cfg = FBOS1Config(risk_pct=RISK, fractal_bars=8, take_profit_pts=4_000,
                                      stop_loss_pts=4_000, half_tp_ratio=0.6, sma_period=50,
                                      pending_expire_bars=4, signal_tf_minutes=15, comment="FBO_B")
            r_fbo2 = fbo_simulate(ticks, m15, m1, fbo_s2_cfg, meta, initial_balance=DEPOSIT)

            ep_cfg = EMAPullbackConfig(risk_pct=RISK, signal_tf_minutes=15, ema_period=50,
                                        lookback_bars=3, pullback_band_pts=150,
                                        entry_buffer_pts=0, sl_buffer_pts=30,
                                        rr_ratio=2.0, half_tp_ratio=0.0, pending_expire_bars=3,
                                        daily_target_pct=0.0, daily_loss_pct=6.0, comment="EMP")
            r_ep = ep_simulate(ticks, m15, m1, ep_cfg, meta, initial_balance=DEPOSIT)

            def weekly(deals):
                if not deals: return pd.Series(dtype=float)
                df = pd.DataFrame([(d.ts, d.pnl) for d in deals if d.kind != "entry"],
                                  columns=["ts", "pnl"])
                df["ts"] = pd.to_datetime(df["ts"])
                df["wk"] = df["ts"].dt.to_period("W").astype(str)
                return df.groupby("wk")["pnl"].sum()

            hm_w = weekly(best[3].deals)
            orb_w = weekly(r_orb.deals)
            fbo_w = weekly(r_fbo2.deals)
            ep_w = weekly(r_ep.deals)

            joined = pd.concat([hm_w, orb_w, fbo_w, ep_w], axis=1,
                               keys=["HourMo", "ORB", "FBO_S2", "EMP"]).fillna(0)
            corr = joined.corr()
            print(f"\n  Weekly PnL correlation (HourMo vs existing streams):")
            for s in ("ORB", "FBO_S2", "EMP"):
                c = corr.loc["HourMo", s]
                tag = ("EXCELLENT (negative)" if c < -0.2 else
                       "GOOD (low)" if c < 0.3 else
                       "MODERATE" if c < 0.6 else
                       "HIGH (compounds)")
                print(f"    HourMo vs {s:<6}: {c:+.2f}  -> {tag}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
