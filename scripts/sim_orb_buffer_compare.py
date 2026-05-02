"""ORB buffer comparison: buf=0 (current) vs buf=30 (proposed live fix).

At buf=0, the EA's `if(buyEntry > ask)` strict-inequality check can silently
drop entries when price is at/above range_high at session-end. Buf=30 puts
stops 30 pts away from current price, more reliable in live.

Compares both at the 3% risk level with daily caps, both standalone and
inside the FBORB combined sanity (to see how many ORB trades each captures).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.fbo_s1 import FBOS1Config
from zgb_sim.fbo_s1_fast import simulate_fast as fbo_simulate
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate


SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0


def make_orb(buffer_pts: int, risk_pct: float = 3.0) -> ORBConfig:
    return ORBConfig(
        risk_pct=risk_pct,
        range_minutes=60,
        buffer_pts=buffer_pts,
        min_range_pts=200,
        max_range_pts=5000,
        fixed_sl_pts=400,
        rr_ratio=3.0,
        half_tp_ratio=0.0,
        pending_expire_minutes=240,
        daily_target_pct=9.0 * risk_pct,    # scaled
        daily_loss_pct=6.0 * risk_pct,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13,
        comment="ORB",
    )


def make_fbo_s1(risk_pct=3.0):
    return FBOS1Config(
        risk_pct=risk_pct, fractal_bars=8, take_profit_pts=25_000,
        stop_loss_pts=10_000, half_tp_ratio=0.3, sma_period=10,
        pending_expire_bars=2, signal_tf_minutes=30, comment="FBO_A",
    )


def make_fbo_s2_m15(risk_pct=3.0):
    return FBOS1Config(
        risk_pct=risk_pct, fractal_bars=8, take_profit_pts=4_000,
        stop_loss_pts=4_000, half_tp_ratio=0.6, sma_period=50,
        pending_expire_bars=4, signal_tf_minutes=15, comment="FBO_B",
    )


def main() -> int:
    full_start = datetime(2026, 3, 14, tzinfo=timezone.utc)
    full_end = datetime(2026, 4, 25, tzinfo=timezone.utc)

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
            tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
            volume_min=m["volume_min"], volume_max=m["volume_max"],
            volume_step=m["volume_step"],
        )
        ticks = load_ticks(SYMBOL, full_start, full_end)
        m1 = load_bars(SYMBOL, "M1", full_start, full_end)
        m5 = load_bars(SYMBOL, "M5", full_start, full_end)
        m15 = load_bars(SYMBOL, "M15", full_start, full_end)
        m30 = load_bars(SYMBOL, "M30", full_start, full_end)

        print("=" * 78)
        print("  ORB BUFFER COMPARISON (Mar 14 -> Apr 25, $10k, 3% risk)")
        print("=" * 78)

        results = []
        for buf in (0, 30):
            orb_cfg = make_orb(buf, risk_pct=3.0)
            fbo_s1 = make_fbo_s1(3.0)
            fbo_s2 = make_fbo_s2_m15(3.0)

            print(f"\n--- ORB buffer = {buf} pts ---")
            r_orb = orb_simulate(ticks, m5, m1, orb_cfg, meta, initial_balance=DEPOSIT)
            r_s1 = fbo_simulate(ticks, m30, m1, fbo_s1, meta, initial_balance=DEPOSIT)
            r_s2 = fbo_simulate(ticks, m15, m1, fbo_s2, meta, initial_balance=DEPOSIT)

            # Merge deals onto shared $10k for combined view
            all_deals = []
            for d in r_s1.deals:
                if d.kind != "entry":
                    all_deals.append((d.ts, "FBO_S1", d.pnl))
            for d in r_s2.deals:
                if d.kind != "entry":
                    all_deals.append((d.ts, "FBO_S2", d.pnl))
            for d in r_orb.deals:
                if d.kind != "entry":
                    all_deals.append((d.ts, "ORB", d.pnl))
            all_deals.sort(key=lambda x: x[0])
            balance = DEPOSIT
            balance_max = DEPOSIT
            dd_abs = 0.0
            for _, _stream, pnl in all_deals:
                balance += pnl
                if balance > balance_max:
                    balance_max = balance
                if balance_max - balance > dd_abs:
                    dd_abs = balance_max - balance
            combined_np = balance - DEPOSIT
            combined_dd_pct = (dd_abs / balance_max * 100.0) if balance_max > 0 else 0.0
            orb_pnl = sum(p for _, s, p in all_deals if s == "ORB")
            orb_trades = sum(1 for _, s, _ in all_deals if s == "ORB")

            print(f"  ORB standalone : NP=${r_orb.net_profit:>+9,.0f}  DD={r_orb.max_drawdown_pct:>5.1f}%  Trades={r_orb.trades}  TP/SL/Other={r_orb.tp_count}/{r_orb.sl_count}/{r_orb.other_count}")
            print(f"  FBORB combined : NP=${combined_np:>+9,.0f}  DD={combined_dd_pct:>5.1f}%  Total trades={len(all_deals)}")
            print(f"  ORB contrib    : NP=${orb_pnl:>+9,.0f}  trades={orb_trades}")

            results.append({
                "buf": buf,
                "orb_np": r_orb.net_profit, "orb_dd": r_orb.max_drawdown_pct,
                "orb_trades": r_orb.trades, "orb_tp": r_orb.tp_count, "orb_sl": r_orb.sl_count,
                "combined_np": combined_np, "combined_dd": combined_dd_pct,
                "combined_trades": len(all_deals),
            })

        print("\n" + "=" * 78)
        print("  COMPARISON")
        print("=" * 78)
        print(f"  {'Buffer':>7}  {'ORB NP':>10}  {'ORB DD':>7}  {'ORB Trades':>10}  {'TP/SL':>8}  {'FBORB NP':>10}  {'FBORB DD':>8}")
        print(f"  {'-'*78}")
        for r in results:
            print(f"  {r['buf']:>5}pts  ${r['orb_np']:>+8,.0f}  {r['orb_dd']:>5.1f}%  "
                  f"{r['orb_trades']:>10}  {r['orb_tp']}/{r['orb_sl']:<4}  "
                  f"${r['combined_np']:>+8,.0f}  {r['combined_dd']:>6.1f}%")

        # Delta analysis
        if len(results) == 2:
            r0, r30 = results[0], results[1]
            print(f"\n  Delta (buf=30 vs buf=0):")
            print(f"    ORB trades: {r30['orb_trades'] - r0['orb_trades']:+d} ({r30['orb_trades']} vs {r0['orb_trades']})")
            print(f"    ORB NP: ${r30['orb_np'] - r0['orb_np']:+,.0f}")
            print(f"    ORB DD: {r30['orb_dd'] - r0['orb_dd']:+.1f} pp")
            print(f"    Combined NP: ${r30['combined_np'] - r0['combined_np']:+,.0f}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
