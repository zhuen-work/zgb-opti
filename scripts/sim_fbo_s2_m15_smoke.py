"""Smoke test FBO Stream 2-M15 (alternative TF) sim at spread=60."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.fbo_s1 import FBOS1Config
from zgb_sim.fbo_s1_fast import simulate_fast


def main() -> int:
    cfg = FBOS1Config(
        risk_pct=3.0,
        fractal_bars=6,
        take_profit_pts=8_000,
        stop_loss_pts=4_000,
        half_tp_ratio=0.3,
        sma_period=20,
        pending_expire_bars=4,
        signal_tf_minutes=15,        # M15
        comment="FBO_B",
    )
    start = datetime(2026, 3, 14, tzinfo=timezone.utc)
    end = datetime(2026, 4, 25, tzinfo=timezone.utc)

    print("=" * 72)
    print("  FBO S2-M15 smoke test (spread=60, $10k)")
    print(f"  Cfg: TF=M15 Bars={cfg.fractal_bars} TP={cfg.take_profit_pts} "
          f"SL={cfg.stop_loss_pts} HTP={cfg.half_tp_ratio} SMA={cfg.sma_period} "
          f"PEB={cfg.pending_expire_bars}")
    print("=" * 72)

    try:
        m = symbol_meta("XAUUSD")
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )
        ticks = load_ticks("XAUUSD", start, end)
        m1 = load_bars("XAUUSD", "M1", start, end)
        m15 = load_bars("XAUUSD", "M15", start, end)
        print(f"  Loaded ticks={len(ticks):,}  M1={len(m1):,}  M15={len(m15):,}")

        result = simulate_fast(ticks, m15, m1, cfg, meta, initial_balance=10_000.0)
        print(f"\n  Result: {result.summary()}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
