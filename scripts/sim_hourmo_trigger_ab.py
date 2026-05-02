"""HourMo trigger-hour A/B: which subset of (02, 08, 16) UTC gives the best
NP-with-low-ORB-correlation tradeoff?

Uses the smoke winner config (MinSig=400, SLBuf=50, RR=1.5) and varies only
the trigger_hours_utc tuple.
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

        # Run ORB once for correlation reference
        orb_cfg = ORBConfig(risk_pct=RISK, range_minutes=90, buffer_pts=0,
                            min_range_pts=200, max_range_pts=5000,
                            fixed_sl_pts=350, rr_ratio=2.0, half_tp_ratio=0.0,
                            pending_expire_minutes=240,
                            daily_target_pct=9.0, daily_loss_pct=6.0,
                            ldn_enabled=True, ldn_start_hour=7,
                            ny_enabled=True, ny_start_hour=13, comment="ORB")
        r_orb = orb_simulate(ticks, m5, m1, orb_cfg, meta, initial_balance=DEPOSIT)

        def weekly(deals):
            df = pd.DataFrame([(d.ts, d.pnl) for d in deals if d.kind != "entry"],
                              columns=["ts", "pnl"])
            if df.empty: return pd.Series(dtype=float)
            df["ts"] = pd.to_datetime(df["ts"])
            df["wk"] = df["ts"].dt.to_period("W").astype(str)
            return df.groupby("wk")["pnl"].sum()

        orb_w = weekly(r_orb.deals)

        print("=" * 96)
        print(f"  HourMo TRIGGER-HOUR A/B  ({days}d, $10k, 3% risk, 70pt)")
        print(f"  Fixed: MinSig=400, SLBuf=50, RR=1.5  (smoke winner)")
        print(f"  Vary:  trigger_hours_utc subset")
        print("=" * 96)

        variants = [
            ("(02, 08, 16) all 3", (2, 8, 16)),
            ("(02) Asia only",      (2,)),
            ("(16) NY-late only",   (16,)),
            ("(02, 16) skip LDN",   (2, 16)),
        ]
        rows = []
        for label, hrs in variants:
            cfg = HourMoConfig(
                risk_pct=RISK, signal_tf_minutes=15,
                trigger_hours_utc=hrs,
                min_signal_pts=400,
                entry_buffer_pts=0,
                sl_buffer_pts=50,
                rr_ratio=1.5, half_tp_ratio=0.0,
                pending_expire_bars=2,
                daily_target_pct=0.0, daily_loss_pct=6.0,
                comment="HourMo",
            )
            r = hm_simulate(ticks, m15, m1, cfg, meta, initial_balance=DEPOSIT)
            hm_w = weekly(r.deals)
            joined = pd.concat([hm_w, orb_w], axis=1, keys=["HM", "ORB"]).fillna(0)
            corr = joined.corr().iloc[0, 1] if len(joined) > 1 else float('nan')
            rows.append((label, hrs, r, corr))

        print(f"\n  {'Variant':<22} {'Hrs':>14} {'NP':>9} {'ROI%':>7} {'DD%':>6} "
              f"{'NP/DD':>7} {'Tr':>4} {'WR%':>5} {'PF':>5} {'Corr_ORB':>9}")
        for label, hrs, r, corr in rows:
            ndd = (r.net_profit / r.max_drawdown) if r.max_drawdown > 0 else 0
            wr = (r.tp_count / r.trades * 100) if r.trades > 0 else 0
            roi = r.net_profit / DEPOSIT * 100
            print(f"  {label:<22} {str(hrs):>14} "
                  f"{r.net_profit:>+9,.0f} {roi:>+6.1f}% "
                  f"{r.max_drawdown_pct:>5.1f}% {ndd:>7.2f} "
                  f"{r.trades:>4} {wr:>4.1f}% {r.profit_factor:>5.2f} "
                  f"{corr:>+9.2f}")

        # Decision: best NP/DD with corr < 0.4 (low diversification threshold)
        good = [r for r in rows if r[3] < 0.4 and r[2].net_profit > 0]
        if good:
            best = max(good, key=lambda r: r[2].net_profit / max(r[2].max_drawdown, 1))
            print(f"\n  WINNER (NP>0, corr<0.4): {best[0]}")
            print(f"    Hours: {best[1]}, NP +${best[2].net_profit:,.0f}, "
                  f"NP/DD {best[2].net_profit/max(best[2].max_drawdown,1):.2f}, "
                  f"corr {best[3]:+.2f}")
        else:
            print(f"\n  NO variant met (NP>0 AND corr<0.4) — HourMo can't escape ORB compounding")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
