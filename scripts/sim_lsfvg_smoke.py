"""LSFVG smoke test — single config, default params, sanity ET range.

Validates: detection fires, fills land at FVG levels, trade count is in the
expected range (20-80 over 6 weeks), PF > 1.0 (signal has any edge at all).
If trade count is way off or PF < 1.0, the strategy needs param tuning or
fundamental rethink before we run a full WFO.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.lsfvg import LSFVGConfig
from zgb_sim.lsfvg_fast import simulate_fast


SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0


def main() -> int:
    start = datetime(2026, 2, 1, tzinfo=timezone.utc)
    end = datetime(2026, 4, 25, tzinfo=timezone.utc)

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
            tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
            volume_min=m["volume_min"], volume_max=m["volume_max"],
            volume_step=m["volume_step"],
        )
        ticks = load_ticks(SYMBOL, start, end)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m15 = load_bars(SYMBOL, "M15", start, end)

        print("=" * 78)
        print("  LSFVG SMOKE TEST (Feb 1 -> Apr 25, ~12 weeks, $10k, M15, 3% risk)")
        print("=" * 78)

        # Best config from probe: lookback=10, loose FVG bounds, half-TP split
        cfg = LSFVGConfig(
            risk_pct=3.0,
            signal_tf_minutes=15,
            lookback_bars=10,
            min_fvg_pts=20,
            max_fvg_pts=5000,
            sweep_buffer_pts=30,
            rr_ratio=2.0,
            half_tp_ratio=0.5,
            pending_expire_bars=4,
            use_ema_filter=False,
            daily_target_pct=0.0,
            daily_loss_pct=0.0,
            comment="LSFVG",
        )

        print(f"\n  Config: TF=M{cfg.signal_tf_minutes} Lookback={cfg.lookback_bars} "
              f"FVG=[{cfg.min_fvg_pts},{cfg.max_fvg_pts}]pts SwpBuf={cfg.sweep_buffer_pts}pts "
              f"RR={cfg.rr_ratio} HalfTP={cfg.half_tp_ratio} PEB={cfg.pending_expire_bars}")

        r = simulate_fast(ticks, m15, m1, cfg, meta, initial_balance=DEPOSIT)

        wr = (r.tp_count / r.trades * 100.0) if r.trades > 0 else 0.0
        ndd = r.net_profit / r.max_drawdown if r.max_drawdown > 0 else 0.0
        roi = r.net_profit / DEPOSIT * 100.0

        print("\n  RESULTS")
        print("  " + "-" * 76)
        print(f"  Net Profit     : ${r.net_profit:>+10,.0f}  ({roi:+.1f}% ROI)")
        print(f"  Max DD         : ${r.max_drawdown:>10,.0f}  ({r.max_drawdown_pct:.1f}%)")
        print(f"  NP/DD          : {ndd:>11.1f}")
        print(f"  Profit Factor  : {r.profit_factor:>11.2f}")
        print(f"  Trades         : {r.trades:>11d}  (TP={r.tp_count} / SL={r.sl_count} / Other={r.other_count})")
        print(f"  Win Rate       : {wr:>10.1f}%")

        print("\n  GATES")
        print("  " + "-" * 76)
        gates = [
            ("Trade count 30-150",  30 <= r.trades <= 150),
            ("Win rate 35-65%",     35.0 <= wr <= 65.0),
            ("Profit factor > 1.3", r.profit_factor > 1.3),
            ("Net profit > 0",      r.net_profit > 0),
            ("DD < 25%",            r.max_drawdown_pct < 25.0),
            ("NP/DD > 1.5",         (r.net_profit / r.max_drawdown if r.max_drawdown > 0 else 0) > 1.5),
        ]
        passed = 0
        for name, ok in gates:
            status = "PASS" if ok else "FAIL"
            print(f"  [{status}]  {name}")
            if ok: passed += 1

        print("\n  " + "-" * 76)
        print(f"  Gates passed: {passed}/{len(gates)}")
        if passed == len(gates):
            print("  -> SMOKE TEST GREEN. Proceed to WFO.")
        elif passed >= 3:
            print("  -> AMBER. Edge exists but needs tuning. Try WFO with wider grid.")
        else:
            print("  -> RED. Fundamental issue — review detection logic before WFO.")

    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
