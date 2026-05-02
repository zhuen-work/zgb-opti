"""EMA pullback smoke probe — 4 corner combos.

Goal: confirm the strategy generates trades AND produces positive expectancy.
ALSO compute weekly correlation with FBO_S2 to gauge diversification value.
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
from zgb_sim.ema_pullback import EMAPullbackConfig, simulate as ep_simulate
from zgb_sim.fbo_s1 import FBOS1Config
from zgb_sim.fbo_s1_fast import simulate_fast as fbo_simulate

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
        m15 = load_bars(SYMBOL, "M15", start, end)

        days = (end - start).days
        print("=" * 92)
        print(f"  EMAPullback smoke probe (Feb 14 -> Apr 25, $10k, 3% risk, spread=70, {days} days)")
        print("=" * 92)

        # 4 representative combos
        combos = [
            # (ema, lookback, band_pts, sl_buf, rr)
            ( 50, 5,  50, 30, 1.5),  # baseline M15 EMA50
            ( 50, 5,  50, 30, 2.0),  # same trigger, wider RR
            ( 50, 3, 100, 50, 2.0),  # tighter window, wider band
            (100, 5,  50, 30, 2.0),  # slower trend filter
        ]
        results = []
        for ema_p, look, band, sl_buf, rr in combos:
            cfg = EMAPullbackConfig(
                risk_pct=RISK, signal_tf_minutes=15, ema_period=ema_p,
                lookback_bars=look, pullback_band_pts=band,
                entry_buffer_pts=0, sl_buffer_pts=sl_buf,
                rr_ratio=rr, half_tp_ratio=0.0,
                pending_expire_bars=3,
                daily_target_pct=0.0, daily_loss_pct=0.0,
                comment="EMAPullback",
            )
            r = ep_simulate(ticks, m15, m1, cfg, meta, initial_balance=DEPOSIT)
            results.append((ema_p, look, band, sl_buf, rr, r))

        # Display table
        print(f"\n  {'EMA':>4} {'Look':>5} {'Band':>5} {'SLBuf':>6} {'RR':>5} "
              f"{'Days':>5} {'NP':>9} {'ROI%':>7} {'DD%':>6} {'NP/DD':>7} "
              f"{'Tr':>4} {'TP':>4} {'SL':>4} {'WR%':>5} {'PF':>5}")
        rows = []
        for ema_p, look, band, sl_buf, rr, r in results:
            ndd = (r.net_profit / r.max_drawdown) if r.max_drawdown > 0 else 0
            wr = (r.tp_count / r.trades * 100) if r.trades > 0 else 0
            roi = r.net_profit / DEPOSIT * 100
            print(f"  {ema_p:>4} {look:>5} {band:>5} {sl_buf:>6} {rr:>5.1f} "
                  f"{days:>5} {r.net_profit:>+9,.0f} {roi:>+6.1f}% "
                  f"{r.max_drawdown_pct:>5.1f}% {ndd:>7.2f} "
                  f"{r.trades:>4} {r.tp_count:>4} {r.sl_count:>4} {wr:>4.1f}% "
                  f"{r.profit_factor:>5.2f}")
            rows.append((ema_p, look, band, sl_buf, rr, r))

        n_prof = sum(1 for *_, r in rows if r.net_profit > 0)
        print(f"\n  Profitable combos: {n_prof}/{len(rows)}")

        # Diversification check: pick best combo, run FBO_S2 separately,
        # compute weekly PnL correlation. Lower = better diversification.
        if n_prof > 0:
            best = max(rows, key=lambda t: (t[5].net_profit / t[5].max_drawdown) if t[5].max_drawdown > 0 else 0)
            print(f"\n  Best combo: EMA={best[0]} Look={best[1]} Band={best[2]} SLBuf={best[3]} RR={best[4]}")

            # FBO_S2 reference for correlation
            fbo_s2_cfg = FBOS1Config(
                risk_pct=RISK, fractal_bars=8, take_profit_pts=4_000,
                stop_loss_pts=4_000, half_tp_ratio=0.6, sma_period=50,
                pending_expire_bars=4, signal_tf_minutes=15, comment="FBO_B",
            )
            r_fbo = fbo_simulate(ticks, m15, m1, fbo_s2_cfg, meta, initial_balance=DEPOSIT)

            # Weekly PnL correlation
            ep_deals = [(d.ts, d.pnl) for d in best[5].deals if d.kind != "entry"]
            fbo_deals = [(d.ts, d.pnl) for d in r_fbo.deals if d.kind != "entry"]

            def weekly(deals):
                if not deals:
                    return pd.Series(dtype=float)
                df = pd.DataFrame(deals, columns=["ts", "pnl"])
                df["ts"] = pd.to_datetime(df["ts"])
                df["wk"] = df["ts"].dt.to_period("W").astype(str)
                return df.groupby("wk")["pnl"].sum()

            ep_w = weekly(ep_deals)
            fbo_w = weekly(fbo_deals)
            joined = pd.concat([ep_w, fbo_w], axis=1, keys=["EMAPullback", "FBO_S2"]).fillna(0)
            corr = joined.corr().iloc[0, 1] if len(joined) > 1 else float('nan')
            print(f"\n  Weekly PnL correlation EMAPullback vs FBO_S2: {corr:+.2f}")
            if corr < 0.3:
                print(f"    -> Low correlation: GOOD diversification candidate.")
            elif corr < 0.6:
                print(f"    -> Moderate correlation: partial diversification.")
            else:
                print(f"    -> High correlation: would COMPOUND existing FBO trades, not diversify.")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
