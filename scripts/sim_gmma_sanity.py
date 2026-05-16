"""Full-period sanity backtest for a GMMA WFO winner.

Reads output/wfo_gmma_may9/winner.json, runs the winner cfg over the default
test window (Feb 14 -> Apr 25), and prints the result. Use after WFO completes
to validate stability outside the WFO fold boundaries.
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.gmma import GMMAConfig, simulate
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.tick_loader import (load_ticks, load_bars, symbol_meta,
                                  kill_mt5_terminal, SIM_SPREAD_PTS)
from zgb_sim.mt5_accounts import init_account


SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
WINNER_PATH = ROOT / "output" / "wfo_gmma_may9" / "winner.json"
FULL_START = datetime(2026, 2, 14, tzinfo=timezone.utc)
FULL_END = datetime(2026, 4, 25, tzinfo=timezone.utc)


def main():
    if not WINNER_PATH.exists():
        print(f"ERROR: {WINNER_PATH} not found. Run sim_wfo_gmma.py first.")
        return 1
    payload = json.loads(WINNER_PATH.read_text())
    cfg_dict = payload["cfg"]
    cfg = GMMAConfig(**cfg_dict)
    print(f"Winner cfg: {cfg}")
    print(f"P0 pass: {payload['p0_pass']}  slope: {payload['slope']:+.1%}")
    print(f"OOS NPs: {payload['oos_nps']}  total: ${payload['total_np']:+,.0f}")
    print(f"Boundary at edge: {payload['boundary_at_edge']}")

    try:
        init_account("sim")
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        ticks = load_ticks(SYMBOL, FULL_START, FULL_END)
        h1 = load_bars(SYMBOL, "H1", FULL_START, FULL_END)
        h4 = load_bars(SYMBOL, "H4", FULL_START, FULL_END)
        print(f"ticks={len(ticks):,}  H1={len(h1)}  H4={len(h4)}  spread={SIM_SPREAD_PTS}pt")
        r = simulate(ticks, h1, h4, cfg, meta, initial_balance=DEPOSIT)
        print("\n" + "=" * 72)
        print(f"  Full-period sanity ({FULL_START.date()} -> {FULL_END.date()})")
        print("=" * 72)
        print(f"  {r.summary()}")
        if r.balance_curve is not None and not r.balance_curve.empty:
            print(f"  Final balance: ${r.final_balance:,.2f}  "
                  f"Peak: ${(DEPOSIT + r.balance_curve['pnl'].cumsum().max()):,.2f}")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
