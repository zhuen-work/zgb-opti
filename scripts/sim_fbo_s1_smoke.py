"""Smoke test FBO Stream 1 simulator with reference setfile params at spread=60."""
from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.fbo_s1 import FBOS1Config, simulate


SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0   # FBO opti convention
DEBUG_BASE = ROOT / "output" / "sim_fbo_s1_smoke" / "smoke"


def main() -> int:
    # Reference params from configs/sets/fbo_v2_3pct_m30xh4xh1_apr11_reopt_10k.set
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

    # 6-week test window
    start = datetime(2026, 3, 14, tzinfo=timezone.utc)
    end = datetime(2026, 4, 25, tzinfo=timezone.utc)

    print("=" * 72)
    print("  FBO Stream 1 smoke test (spread=60, $10k deposit)")
    print(f"  Window: {start.date()} -> {end.date()}")
    print(f"  Cfg: TF=M30 Bars={cfg.fractal_bars} TP={cfg.take_profit_pts} "
          f"SL={cfg.stop_loss_pts} HTP={cfg.half_tp_ratio} SMA={cfg.sma_period} "
          f"PEB={cfg.pending_expire_bars} Risk={cfg.risk_pct}%")
    print("=" * 72)

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )
        print(f"  Meta: {m}")
        print()

        print("  Loading ticks (spread=60 applied at load)...")
        ticks = load_ticks(SYMBOL, start, end)
        print(f"    {len(ticks):,} ticks loaded")
        print("  Loading M1 bars...")
        m1 = load_bars(SYMBOL, "M1", start, end)
        print(f"    {len(m1):,} M1 bars")
        print("  Loading M30 bars...")
        m30 = load_bars(SYMBOL, "M30", start, end)
        print(f"    {len(m30):,} M30 bars")
        print()

        DEBUG_BASE.parent.mkdir(parents=True, exist_ok=True)
        result = simulate(ticks, m30, m1, cfg, meta, initial_balance=DEPOSIT,
                         debug_path=str(DEBUG_BASE))
        print()
        print("  Result:", result.summary())
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
