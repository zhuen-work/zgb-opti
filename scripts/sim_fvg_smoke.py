"""Smoke test FVG sim with EA reference params at spread=60."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.fvg import FVGConfig, simulate


def smoke(label: str, cfg: FVGConfig, signal_tf: str):
    start = datetime(2026, 3, 14, tzinfo=timezone.utc)
    end = datetime(2026, 4, 25, tzinfo=timezone.utc)
    m = symbol_meta("XAUUSD")
    meta = SymbolMeta(
        point=m["point"], digits=m["digits"],
        tick_size=m["tick_size"], tick_value=m["tick_value"],
        stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
        volume_max=m["volume_max"], volume_step=m["volume_step"],
    )
    ticks = load_ticks("XAUUSD", start, end)
    m1 = load_bars("XAUUSD", "M1", start, end)
    sig = load_bars("XAUUSD", signal_tf, start, end)
    print(f"\n=== {label} (TF={signal_tf}) ===")
    print(f"  Cfg: MinSize={cfg.min_size_pts} MaxAge={cfg.max_age_bars} "
          f"MaxZones={cfg.max_zones} RR={cfg.rr_ratio} SL_Buffer={cfg.sl_buffer_pts} "
          f"PEB={cfg.pending_expire_bars} HTP={cfg.half_tp_ratio}")
    r = simulate(ticks, sig, m1, cfg, meta, initial_balance=10_000.0)
    print(f"  Result: {r.summary()}")


def main() -> int:
    try:
        # FVG S1 reference (H1, EA defaults)
        cfg_s1 = FVGConfig(
            risk_pct=3.0,
            min_size_pts=1600,
            max_age_bars=150,
            max_zones=3,
            rr_ratio=5.0,
            sl_buffer_pts=40,
            pending_expire_bars=3,
            half_tp_ratio=0.0,
            signal_tf_minutes=60,
            comment="FVG_A",
        )
        smoke("FVG S1 reference", cfg_s1, "H1")

        # FVG S2 reference (H4, EA defaults)
        cfg_s2 = FVGConfig(
            risk_pct=3.0,
            min_size_pts=1800,
            max_age_bars=200,
            max_zones=4,
            rr_ratio=5.0,
            sl_buffer_pts=20,
            pending_expire_bars=1,
            half_tp_ratio=0.0,
            signal_tf_minutes=240,
            comment="FVG_B",
        )
        smoke("FVG S2 reference", cfg_s2, "H4")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
