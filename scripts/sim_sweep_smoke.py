"""Smoke test for sweep engine.

Step 1: 1 config, 1 worker — should match reference (+$100.72, 372 trades).
Step 2: 8 copies of same config, 8 workers — all results identical, wall time ~= 1 run.
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal
from zgb_sim.scalper_v1 import S1Config, SymbolMeta
from zgb_sim.sweep import run_sweep


REFERENCE_CFG = S1Config(
    risk_pct=1.0, donchian_bars=20,
    take_profit_pts=200, stop_loss_pts=50, half_tp_ratio=0.6,
    pending_expire_bars=2, start_hour=14, end_hour=22,
    daily_target_pct=3.0, daily_loss_pct=6.0,
    block_fri_pm=True, hedge_mode=False,
)


def main():
    try:
        symbol = "XAUUSD"
        start = datetime(2026, 3, 7, tzinfo=timezone.utc)
        end   = datetime(2026, 4, 18, tzinfo=timezone.utc)

        m = symbol_meta(symbol)
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )

        print("=" * 72)
        print("  STEP 1: single-config, 1 worker")
        print("=" * 72)
        t0 = time.time()
        df1 = run_sweep([REFERENCE_CFG], symbol, start, end, meta, n_workers=1,
                        window_label="smoke-1")
        t1 = time.time() - t0
        print(f"\n  Step 1 wall time: {t1:.1f}s")
        print(f"  Result: NP=${df1.iloc[0]['net_profit']:+,.2f}  trades={int(df1.iloc[0]['trades'])}  "
              f"TP={int(df1.iloc[0]['tp'])}  SL={int(df1.iloc[0]['sl'])}  Other={int(df1.iloc[0]['other'])}  "
              f"DD={df1.iloc[0]['drawdown_pct']:.1f}%  PF={df1.iloc[0]['profit_factor']:.2f}")
        print(f"  Expected: NP=+$100.72  trades=372  TP=87  SL=244  Other=41  DD=8.4%")

        # Sanity gate
        np_ok = abs(df1.iloc[0]['net_profit'] - 100.72) < 0.5
        tr_ok = int(df1.iloc[0]['trades']) == 372
        print(f"\n  Step 1 GATE: NP {'OK' if np_ok else 'FAIL'}, trades {'OK' if tr_ok else 'FAIL'}")
        if not (np_ok and tr_ok):
            print("  ABORT: Step 1 does not match reference — not running Step 2.")
            return

        print()
        print("=" * 72)
        print("  STEP 2: 8 copies same config, 8 workers")
        print("=" * 72)
        t0 = time.time()
        df8 = run_sweep([REFERENCE_CFG] * 8, symbol, start, end, meta, n_workers=8,
                        window_label="smoke-8")
        t8 = time.time() - t0
        print(f"\n  Step 2 wall time: {t8:.1f}s  ({t1 / t8:.1f}x speedup expected ~1x)")

        # Validate all results identical
        unique_np = df8["net_profit"].unique()
        unique_trades = df8["trades"].unique()
        print(f"  Unique NPs     : {unique_np}")
        print(f"  Unique trades  : {unique_trades}")
        all_same = len(unique_np) == 1 and len(unique_trades) == 1
        matches_ref = abs(unique_np[0] - df1.iloc[0]['net_profit']) < 0.01
        print(f"\n  Step 2 GATE: all-same {'OK' if all_same else 'FAIL'}, matches-ref {'OK' if matches_ref else 'FAIL'}")

        print()
        print("=" * 72)
        print(f"  Summary:")
        print(f"    1 worker, 1 cfg  : {t1:.1f}s")
        print(f"    8 workers, 8 cfg : {t8:.1f}s")
        print(f"    Speedup (vs. 8x serial) : {8 * t1 / t8:.1f}x")
        print("=" * 72)
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
