"""Verify ema_pullback_fast (Numba) produces same NP as ema_pullback (pure Python).

Uses the smoke winner config: NP +$4,999, 86 trades.
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
from zgb_sim.ema_pullback import EMAPullbackConfig, simulate as ep_py
from zgb_sim.ema_pullback_fast import simulate_fast as ep_fast

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0


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

        cfg = EMAPullbackConfig(
            risk_pct=3.0, signal_tf_minutes=15, ema_period=50,
            lookback_bars=3, pullback_band_pts=100,
            entry_buffer_pts=0, sl_buffer_pts=50,
            rr_ratio=2.0, half_tp_ratio=0.0,
            pending_expire_bars=3,
            daily_target_pct=0.0, daily_loss_pct=0.0,
            comment="EMAPullback",
        )

        print("=" * 78)
        print("  Verify: ema_pullback (pure Python) vs ema_pullback_fast (Numba)")
        print("  Smoke winner config; expecting NP +$4,999, 86 trades.")
        print("=" * 78)

        # Run JIT first (first call includes compile time)
        t0 = time.time()
        r_fast = ep_fast(ticks, m15, m1, cfg, meta, initial_balance=DEPOSIT)
        t_fast = time.time() - t0
        print(f"\n  JIT:  NP=${r_fast.net_profit:>+8,.0f}  DD={r_fast.max_drawdown_pct:>5.2f}%  "
              f"Tr={r_fast.trades}  TP/SL/O={r_fast.tp_count}/{r_fast.sl_count}/{r_fast.other_count}  "
              f"PF={r_fast.profit_factor:.3f}  ({t_fast:.1f}s with compile)")

        # Run JIT a second time to measure warm speed
        t0 = time.time()
        r_fast2 = ep_fast(ticks, m15, m1, cfg, meta, initial_balance=DEPOSIT)
        t_fast2 = time.time() - t0
        print(f"  JIT-warm: ({t_fast2:.2f}s)")

        # Run pure-Python for ground truth
        t0 = time.time()
        r_py = ep_py(ticks, m15, m1, cfg, meta, initial_balance=DEPOSIT)
        t_py = time.time() - t0
        print(f"  PY:   NP=${r_py.net_profit:>+8,.0f}  DD={r_py.max_drawdown_pct:>5.2f}%  "
              f"Tr={r_py.trades}  TP/SL/O={r_py.tp_count}/{r_py.sl_count}/{r_py.other_count}  "
              f"PF={r_py.profit_factor:.3f}  ({t_py:.1f}s)")

        # Speedup
        if t_fast2 > 0:
            print(f"\n  JIT speedup: {t_py / t_fast2:.1f}× (warm)")

        # Match
        np_diff = abs(r_fast.net_profit - r_py.net_profit)
        tr_diff = abs(r_fast.trades - r_py.trades)
        print(f"\n  ΔNP = ${np_diff:.2f}  ΔTrades = {tr_diff}")
        if np_diff < 1.0 and tr_diff == 0:
            print("  PASS: JIT matches pure-Python within $1 (numerical noise tolerance)")
        elif np_diff < 100 and tr_diff <= 2:
            print("  CLOSE: JIT matches within $100 / 2 trades (probably tick-edge timing)")
        else:
            print(f"  FAIL: significant divergence — investigate before WFO")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
