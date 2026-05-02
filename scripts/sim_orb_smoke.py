"""Smoke test ORB sim with default params at spread=60."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig, simulate


def main() -> int:
    cfg = ORBConfig(
        risk_pct=1.0,
        range_minutes=30,
        buffer_pts=30,
        min_range_pts=200,
        max_range_pts=5000,
        fixed_sl_pts=0,
        rr_ratio=2.0,
        half_tp_ratio=0.0,
        pending_expire_minutes=120,
        daily_target_pct=6.0,
        daily_loss_pct=8.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13,
    )
    start = datetime(2026, 3, 14, tzinfo=timezone.utc)
    end = datetime(2026, 4, 25, tzinfo=timezone.utc)

    print("=" * 72)
    print(f"  ORB smoke test (spread=60, $100, 1% risk)")
    print(f"  Cfg: range={cfg.range_minutes}min  buffer={cfg.buffer_pts}pts  "
          f"RR={cfg.rr_ratio}  expire={cfg.pending_expire_minutes}min")
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
        m5 = load_bars("XAUUSD", "M5", start, end)
        print(f"  Loaded ticks={len(ticks):,}  M1={len(m1):,}  M5={len(m5):,}")

        result = simulate(ticks, m5, m1, cfg, meta, initial_balance=10_000.0)
        print(f"\n  Result: {result.summary()}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
