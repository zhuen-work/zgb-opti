"""Validate Numba FBO Stream 1 sim against pure-Python reference.

Reference (from sim_fbo_s1_smoke.py with same params, spread=60, $10k):
  NP=+$2,150.00  ROI=+21.5%  PF=1.51  DD=11.6%  Trades=36  TP/SL/Other=14/22/0
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.fbo_s1 import FBOS1Config
from zgb_sim.fbo_s1_fast import simulate_fast


SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0


def main() -> int:
    cfg = FBOS1Config(
        risk_pct=3.0,
        fractal_bars=8,
        take_profit_pts=25_000,
        stop_loss_pts=5_000,
        half_tp_ratio=0.3,
        sma_period=5,
        pending_expire_bars=2,
        comment="FBO_A",
    )
    start = datetime(2026, 3, 14, tzinfo=timezone.utc)
    end = datetime(2026, 4, 25, tzinfo=timezone.utc)

    print("=" * 72)
    print("  FBO S1 NUMBA smoke test (must match pure-Python reference)")
    print(f"  Reference: NP=+$2,150.00 / Trades=36 / TP=14 / SL=22 / DD=11.6%")
    print(f"  Window: {start.date()} -> {end.date()}, spread=60, $10k deposit")
    print("=" * 72)

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )
        print(f"  Loading data...")
        t0 = time.time()
        ticks = load_ticks(SYMBOL, start, end)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m30 = load_bars(SYMBOL, "M30", start, end)
        print(f"  Data loaded in {time.time()-t0:.1f}s "
              f"(ticks={len(ticks):,}, M1={len(m1):,}, M30={len(m30):,})")

        print("\n  First call (JIT compile + run)...")
        t0 = time.time()
        result = simulate_fast(ticks, m30, m1, cfg, meta, initial_balance=DEPOSIT)
        first_dt = time.time() - t0
        print(f"  First call done in {first_dt:.1f}s")
        print(f"  Result: {result.summary()}")

        print("\n  Second call (cached JIT, pure runtime)...")
        t0 = time.time()
        result2 = simulate_fast(ticks, m30, m1, cfg, meta, initial_balance=DEPOSIT)
        second_dt = time.time() - t0
        print(f"  Second call done in {second_dt:.2f}s")

        # Validation
        ref_np = 2150.00
        ref_trades = 36
        ref_tp = 14
        ref_sl = 22

        print("\n" + "=" * 72)
        print("  VALIDATION")
        print("=" * 72)
        np_diff = abs(result.net_profit - ref_np)
        ok_np = np_diff < 0.01
        ok_trades = result.trades == ref_trades
        ok_tp = result.tp_count == ref_tp
        ok_sl = result.sl_count == ref_sl
        print(f"    NP:     {result.net_profit:+,.2f} vs ref {ref_np:+,.2f}  "
              f"diff={np_diff:.2f}  {'OK' if ok_np else 'MISMATCH'}")
        print(f"    Trades: {result.trades} vs ref {ref_trades}  "
              f"{'OK' if ok_trades else 'MISMATCH'}")
        print(f"    TP:     {result.tp_count} vs ref {ref_tp}  "
              f"{'OK' if ok_tp else 'MISMATCH'}")
        print(f"    SL:     {result.sl_count} vs ref {ref_sl}  "
              f"{'OK' if ok_sl else 'MISMATCH'}")

        all_ok = ok_np and ok_trades and ok_tp and ok_sl
        print()
        if all_ok:
            print("  ALL CHECKS PASSED — Numba sim matches reference.")
        else:
            print("  ** MISMATCH ** — Numba sim diverges from reference.")
        return 0 if all_ok else 1
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    sys.exit(main())
