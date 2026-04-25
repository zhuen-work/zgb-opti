"""Validate Numba-fast sim against reference slow sim on the same config."""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, load_ticks, load_bars, kill_mt5_terminal
from zgb_sim.scalper_v1 import S1Config, SymbolMeta, simulate
from zgb_sim.scalper_v1_fast import simulate_fast


REF = S1Config(
    risk_pct=1.0, donchian_bars=20,
    take_profit_pts=200, stop_loss_pts=50, half_tp_ratio=0.6,
    pending_expire_bars=2, start_hour=14, end_hour=22,
    daily_target_pct=3.0, daily_loss_pct=6.0,
    block_fri_pm=True, hedge_mode=False,
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

        start = datetime(2026, 3, 7, tzinfo=timezone.utc)
        end = datetime(2026, 4, 18, tzinfo=timezone.utc)

        print("Loading data...")
        t0 = time.time()
        ticks = load_ticks("XAUUSD", start, end)
        m1 = load_bars("XAUUSD", "M1", start, end)
        m5 = load_bars("XAUUSD", "M5", start, end)
        print(f"  Loaded {len(ticks):,} ticks, {len(m1):,} M1, {len(m5):,} M5 in {time.time()-t0:.1f}s")

        # Fast: warmup pass (JIT compile) then timed pass
        print("\n[Fast sim] JIT warmup...")
        t0 = time.time()
        r_fast = simulate_fast(ticks, m5, m1, REF, meta, 100.0)
        t_fast_warm = time.time() - t0
        print(f"  Warmup: {t_fast_warm:.1f}s (includes JIT compile)")
        print(f"  Result: {r_fast.summary()}")

        print("\n[Fast sim] Hot run...")
        t0 = time.time()
        r_fast2 = simulate_fast(ticks, m5, m1, REF, meta, 100.0)
        t_fast_hot = time.time() - t0
        print(f"  Hot:    {t_fast_hot:.1f}s")
        print(f"  Result: {r_fast2.summary()}")

        print("\n[Slow sim] Running reference...")
        t0 = time.time()
        r_slow = simulate(ticks, m5, m1, REF, meta, 100.0)
        t_slow = time.time() - t0
        print(f"  Slow:   {t_slow:.1f}s")
        print(f"  Result: {r_slow.summary()}")

        print("\n=" * 40)
        print("  COMPARISON")
        print("=" * 72)
        print(f"  Slow NP={r_slow.net_profit:+,.2f}  trades={r_slow.trades}  "
              f"TP={r_slow.tp_count}  SL={r_slow.sl_count}  Other={r_slow.other_count}")
        print(f"  Fast NP={r_fast2.net_profit:+,.2f}  trades={r_fast2.trades}  "
              f"TP={r_fast2.tp_count}  SL={r_fast2.sl_count}  Other={r_fast2.other_count}")
        np_diff = abs(r_fast2.net_profit - r_slow.net_profit)
        tr_diff = abs(r_fast2.trades - r_slow.trades)
        print(f"  Diff: NP=${np_diff:.2f}  Trades={tr_diff}")
        print(f"  Speedup (hot): {t_slow / t_fast_hot:.1f}x")

        ok = np_diff < 1.0 and tr_diff <= 5
        print(f"\n  Validation: {'OK' if ok else 'MISMATCH'}")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
