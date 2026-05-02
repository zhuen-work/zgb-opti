"""Validate Numba FVG sim matches pure-Python reference.

Pure-Python references (from sim_fvg_smoke.py):
  FVG S1 (H1): NP=+$965.35  PF=1.13  DD=28.1%  Trades=25  TP/SL=5/20
  FVG S2 (H4): NP=+$2,854.65 PF=1.35  DD=21.7%  Trades=26  TP/SL=6/20
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.fvg import FVGConfig
from zgb_sim.fvg_fast import simulate_fast


REFS = [
    ("FVG S1 ref (H1)", "H1", FVGConfig(
        risk_pct=3.0, min_size_pts=1600, max_age_bars=150, max_zones=3,
        rr_ratio=5.0, sl_buffer_pts=40, pending_expire_bars=3,
        half_tp_ratio=0.0, signal_tf_minutes=60, comment="FVG_A",
    ), {"net_profit": 965.35, "trades": 25, "tp_count": 5, "sl_count": 20}),
    ("FVG S2 ref (H4)", "H4", FVGConfig(
        risk_pct=3.0, min_size_pts=1800, max_age_bars=200, max_zones=4,
        rr_ratio=5.0, sl_buffer_pts=20, pending_expire_bars=1,
        half_tp_ratio=0.0, signal_tf_minutes=240, comment="FVG_B",
    ), {"net_profit": 2854.65, "trades": 26, "tp_count": 6, "sl_count": 20}),
]


def main() -> int:
    start = datetime(2026, 3, 14, tzinfo=timezone.utc)
    end = datetime(2026, 4, 25, tzinfo=timezone.utc)

    try:
        m = symbol_meta("XAUUSD")
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )
        print("Loading data once...")
        ticks = load_ticks("XAUUSD", start, end)
        m1 = load_bars("XAUUSD", "M1", start, end)
        print(f"  ticks={len(ticks):,}  M1={len(m1):,}\n")

        all_pass = True
        for label, tf, cfg, ref in REFS:
            sig = load_bars("XAUUSD", tf, start, end)
            print(f"=== {label} ===")
            r = simulate_fast(ticks, sig, m1, cfg, meta, initial_balance=10_000.0)
            print(f"  Result: {r.summary()}")

            np_diff = abs(r.net_profit - ref["net_profit"])
            ok_np = np_diff < 0.01
            ok_tr = r.trades == ref["trades"]
            ok_tp = r.tp_count == ref["tp_count"]
            ok_sl = r.sl_count == ref["sl_count"]
            print(f"  NP:     {r.net_profit:+,.2f} vs ref {ref['net_profit']:+,.2f}  "
                  f"diff={np_diff:.4f}  {'OK' if ok_np else 'MISMATCH'}")
            print(f"  Trades: {r.trades} vs {ref['trades']}  {'OK' if ok_tr else 'MISMATCH'}")
            print(f"  TP:     {r.tp_count} vs {ref['tp_count']}  {'OK' if ok_tp else 'MISMATCH'}")
            print(f"  SL:     {r.sl_count} vs {ref['sl_count']}  {'OK' if ok_sl else 'MISMATCH'}")
            if not (ok_np and ok_tr and ok_tp and ok_sl):
                all_pass = False
            print()

        print("=" * 60)
        if all_pass:
            print("  ALL CHECKS PASSED — Numba FVG matches pure-Python reference.")
            return 0
        else:
            print("  ** MISMATCH ** — Numba FVG diverges from reference.")
            return 1
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    sys.exit(main())
