"""Quick A/B on BlockFriPM using the prior tiny-WFO winner params."""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone, date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal
from zgb_sim.scalper_v1 import S1Config, SymbolMeta
from zgb_sim.sweep import run_sweep


BASE = dict(
    risk_pct=1.0,
    donchian_bars=20,
    take_profit_pts=200,
    stop_loss_pts=50,
    half_tp_ratio=0.6,
    pending_expire_bars=2,
    start_hour=14,
    end_hour=22,
    daily_target_pct=6.0,
    daily_loss_pct=12.0,
    hedge_mode=False,
)


def main():
    try:
        m = symbol_meta("XAUUSD")
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )
        cfgs = [
            S1Config(block_fri_pm=True,  **BASE),
            S1Config(block_fri_pm=False, **BASE),
        ]
        # Full OOS span to see Friday PM effect clearly
        start = datetime(2026, 3, 7, tzinfo=timezone.utc)
        end   = datetime(2026, 4, 18, tzinfo=timezone.utc)

        print("=" * 72)
        print("  BlockFriPM A/B test (winner params from tiny WFO)")
        print(f"  Base: Donch=20 TP=200 SL=50 HTP=0.6 Tgt=6%  Span: Mar 7 -> Apr 18")
        print("=" * 72)

        df = run_sweep(cfgs, "XAUUSD", start, end, meta,
                       initial_balance=100.0, n_workers=2,
                       window_label="blkfri-ab")

        print(f"\n  {'BlkFri':<7}{'NP':>10}{'Ret%':>8}{'PF':>7}{'DD':>7}{'Trades':>7}{'TP':>5}{'SL':>5}{'Other':>7}")
        for _, r in df.iterrows():
            bf = "Y" if r["block_fri_pm"] else "N"
            print(f"  {bf:<7}{r['net_profit']:>+10.2f}{r['return_pct']:>+7.1f}%"
                  f"{r['profit_factor']:>7.2f}{r['drawdown_pct']:>6.1f}%"
                  f"{int(r['trades']):>7}{int(r['tp']):>5}{int(r['sl']):>5}{int(r['other']):>7}")

        r_on  = df[df.block_fri_pm == True].iloc[0]
        r_off = df[df.block_fri_pm == False].iloc[0]
        diff_np = float(r_off["net_profit"] - r_on["net_profit"])
        diff_tr = int(r_off["trades"] - r_on["trades"])
        print(f"\n  Delta (OFF - ON): NP ${diff_np:+.2f}  Trades {diff_tr:+d}")
        if diff_np > 0:
            print(f"  -> BlockFriPM=false is BETTER by ${diff_np:+.2f}")
        else:
            print(f"  -> BlockFriPM=true is BETTER by ${-diff_np:+.2f} (Friday PM trades net-negative)")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
