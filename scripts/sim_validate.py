"""Validate Python simulator against known MT5 Model=4 ET result.

Target: S1 winner params on Mar 7 -> Apr 18 2026
  Donch=20 TP=200 SL=50 HTP=0.6 Tgt=3 Loss=6
  Known MT5 Model=4 result: NP=+$5,457, PF=2.33, DD=6.29%, Trades=998
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Make src importable
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import load_ticks, load_bars, symbol_meta
from zgb_sim.scalper_v1 import S1Config, SymbolMeta, simulate


def main():
    symbol = "XAUUSD"
    start = datetime(2026, 3, 7, tzinfo=timezone.utc)
    end   = datetime(2026, 4, 18, tzinfo=timezone.utc)

    print("Loading symbol metadata...")
    m = symbol_meta(symbol)
    print(f"  {m}")
    meta = SymbolMeta(
        point=m["point"], digits=m["digits"],
        tick_size=m["tick_size"], tick_value=m["tick_value"],
        stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
        volume_max=m["volume_max"], volume_step=m["volume_step"],
    )

    print(f"Loading ticks {start.date()} -> {end.date()}...")
    t0 = time.time()
    ticks = load_ticks(symbol, start, end)
    print(f"  {len(ticks):,} ticks in {time.time()-t0:.1f}s")

    print("Loading M1 bars...")
    m1 = load_bars(symbol, "M1", start, end)
    print(f"  {len(m1):,} M1 bars")

    print("Loading M5 bars...")
    m5 = load_bars(symbol, "M5", start, end)
    print(f"  {len(m5):,} M5 bars")

    cfg = S1Config(
        risk_pct=1.0,
        donchian_bars=20,
        take_profit_pts=200,
        stop_loss_pts=50,
        half_tp_ratio=0.6,
        pending_expire_bars=2,
        start_hour=14,
        end_hour=22,
        daily_target_pct=3.0,
        daily_loss_pct=6.0,
        block_fri_pm=True,
        hedge_mode=False,
    )

    print("\nRunning sim...")
    t0 = time.time()
    r = simulate(ticks, m5, m1, cfg, meta, initial_balance=100.0)
    print(f"  Sim done in {time.time()-t0:.1f}s")

    print("\n--- Sim Result ---")
    print(r.summary())

    print("\n--- Expected (MT5 Model=4) ---")
    print("NP=+5,457.00  PF=2.33  DD=6.29%  Trades=998  TP/SL/Other=393/509/96")


if __name__ == "__main__":
    main()
