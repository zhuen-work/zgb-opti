"""ORB break-even comparison: no-BE vs BE @ 0.5R / 1.0R / 1.5R / 2.0R.

BE moves SL to entry (or entry + small buffer) once price reaches
`be_trigger_r × original_SL_distance` favorable.

Trade-off:
  - Pro: eliminates risk on trades that have moved meaningfully your way
  - Con: noise pullbacks now stop you out at $0 instead of letting the trade
         continue to TP (especially harmful at low BE triggers)

Tests at 0.5R, 1.0R, 1.5R, 2.0R (no-buffer vs +20pts buffer).
Standalone ORB, $10k, 3% risk. Apr ORB params.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate


SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0


def make_cfg(be_trigger_r: float, be_buffer_pts: int = 0) -> ORBConfig:
    return ORBConfig(
        risk_pct=3.0,
        range_minutes=60, buffer_pts=0,
        min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=400, rr_ratio=3.0, half_tp_ratio=0.0,
        pending_expire_minutes=240,
        daily_target_pct=27.0, daily_loss_pct=18.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13,
        be_trigger_r=be_trigger_r, be_buffer_pts=be_buffer_pts,
        comment="ORB",
    )


def main() -> int:
    start = datetime(2026, 2, 14, tzinfo=timezone.utc)
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
        m5 = load_bars(SYMBOL, "M5", start, end)

        print("=" * 95)
        print("  ORB BREAK-EVEN COMPARISON (Feb 14 -> Apr 25, $10k, 3% risk)")
        print("  Baseline: SL=400pts  RR=3.0  -> TP=1200pts  ->  1R = 400pts favorable")
        print("=" * 95)
        print(f"  {'Variant':<22} {'NP':>10} {'ROI':>7} {'DD':>6} {'NP/DD':>7} {'Tr':>4} "
              f"{'TP':>4} {'SL':>4} {'BE-out':>7} {'WR':>6} {'PF':>6}")
        print("  " + "-" * 93)

        # Build variants
        variants = [
            ("no-BE (baseline)",   0.0,  0),
            ("BE @ 0.5R, buf=0",   0.5,  0),
            ("BE @ 1.0R, buf=0",   1.0,  0),
            ("BE @ 1.5R, buf=0",   1.5,  0),
            ("BE @ 2.0R, buf=0",   2.0,  0),
            ("BE @ 1.0R, buf=20",  1.0, 20),
            ("BE @ 1.0R, buf=50",  1.0, 50),
            ("BE @ 0.5R, buf=20",  0.5, 20),
        ]

        rows = []
        for name, trig, buf in variants:
            cfg = make_cfg(trig, buf)
            r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
            wr = (r.tp_count / r.trades * 100) if r.trades > 0 else 0
            ndd = (r.net_profit / r.max_drawdown) if r.max_drawdown > 0 else 0
            roi = r.net_profit / DEPOSIT * 100
            # "BE-out" = SL hits with PnL ≈ 0 (BE-stopped trades)
            # We approximate by counting SL deals where |pnl| < 5% of typical SL loss.
            typical_sl_loss = DEPOSIT * 0.03  # 3% risk
            be_out = sum(1 for d in r.deals
                         if d.kind == "sl" and abs(d.pnl) < typical_sl_loss * 0.20)
            true_sl = r.sl_count - be_out
            print(f"  {name:<22} ${r.net_profit:>+8,.0f} {roi:>+5.1f}% {r.max_drawdown_pct:>5.1f}% "
                  f"{ndd:>7.2f} {r.trades:>4} {r.tp_count:>4} {true_sl:>4} {be_out:>7} "
                  f"{wr:>5.1f}% {r.profit_factor:>6.2f}")
            rows.append((name, r, be_out, true_sl))

        print("  " + "-" * 93)

        # Delta vs baseline
        baseline_r = rows[0][1]
        print(f"\n  Delta vs baseline (no-BE):")
        for name, r, be_out, true_sl in rows[1:]:
            d_np = r.net_profit - baseline_r.net_profit
            d_dd = r.max_drawdown_pct - baseline_r.max_drawdown_pct
            d_tp = r.tp_count - baseline_r.tp_count
            d_sl_total = r.sl_count - baseline_r.sl_count
            print(f"    {name:<22}  dNP=${d_np:>+7,.0f}  dDD={d_dd:>+5.1f}pp  "
                  f"dTP={d_tp:>+3d}  d(SL+BE-out)={d_sl_total:>+3d}  BE-stops={be_out}")

        # Verdict
        print(f"\n  --- VERDICT ---")
        best = max(rows, key=lambda x: (x[1].net_profit / x[1].max_drawdown
                                         if x[1].max_drawdown > 0 else 0))
        best_name, best_r, _, _ = best
        baseline_ndd = (baseline_r.net_profit / baseline_r.max_drawdown
                        if baseline_r.max_drawdown > 0 else 0)
        best_ndd = (best_r.net_profit / best_r.max_drawdown
                    if best_r.max_drawdown > 0 else 0)
        if best_name == rows[0][0]:
            print(f"  -> NO-BE wins. Baseline NP/DD={baseline_ndd:.2f}. "
                  f"BE doesn't help on this dataset.")
        else:
            improvement = (best_ndd - baseline_ndd) / max(baseline_ndd, 0.01) * 100
            print(f"  -> BEST: {best_name}  (NP/DD {best_ndd:.2f} vs baseline {baseline_ndd:.2f}, "
                  f"+{improvement:.0f}%)")

    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
