"""DT818_pro risk-level comparison: 2% vs 3% vs 4.5% per stream on shared $10k.

ORB caps scale with risk (18/12 at 2%, 27/18 at 3%, 40.5/27 at 4.5% — same trades-to-trigger).
FBO + LSFVG have caps disabled at all levels.

Reports per-stream NP/DD + combined NP/DD/ROI for each risk level + delta.
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
from zgb_sim.lsfvg import LSFVGConfig
from zgb_sim.lsfvg_fast import simulate_fast as lsfvg_simulate


SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0


def run_at_risk(ticks, m1, m5, m15, m30, meta, risk_pct: float):
    fbo_s1 = FBOS1Config(
        risk_pct=risk_pct, fractal_bars=8, take_profit_pts=25_000,
        stop_loss_pts=10_000, half_tp_ratio=0.3, sma_period=10,
        pending_expire_bars=2, signal_tf_minutes=30, comment="FBO_A",
    )
    fbo_s2 = FBOS1Config(
        risk_pct=risk_pct, fractal_bars=8, take_profit_pts=4_000,
        stop_loss_pts=4_000, half_tp_ratio=0.6, sma_period=50,
        pending_expire_bars=4, signal_tf_minutes=15, comment="FBO_B",
    )
    # ORB caps scale: 9% target / 6% loss × (risk/1) gives cap-trades-to-trigger constant.
    # At 3% risk: 27/18. At 4.5%: 40.5/27.
    orb_cfg = ORBConfig(
        risk_pct=risk_pct, range_minutes=60, buffer_pts=0,
        min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=400, rr_ratio=3.0, half_tp_ratio=0.0,
        pending_expire_minutes=240,
        daily_target_pct=9.0 * risk_pct, daily_loss_pct=6.0 * risk_pct,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13,
        comment="ORB",
    )
    lsfvg_cfg = LSFVGConfig(
        risk_pct=risk_pct, signal_tf_minutes=15, lookback_bars=10,
        min_fvg_pts=20, max_fvg_pts=5000, sweep_buffer_pts=30,
        rr_ratio=2.0, half_tp_ratio=0.5, pending_expire_bars=4,
        daily_target_pct=0.0, daily_loss_pct=0.0,
        comment="LSFVG",
    )

    r_s1 = fbo_simulate(ticks, m30, m1, fbo_s1, meta, initial_balance=DEPOSIT)
    r_s2 = fbo_simulate(ticks, m15, m1, fbo_s2, meta, initial_balance=DEPOSIT)
    r_orb = orb_simulate(ticks, m5, m1, orb_cfg, meta, initial_balance=DEPOSIT)
    r_lsf = lsfvg_simulate(ticks, m15, m1, lsfvg_cfg, meta, initial_balance=DEPOSIT)

    # Merge deals on shared account
    all_deals = []
    for d in r_s1.deals:
        if d.kind != "entry": all_deals.append((d.ts, "FBO_S1", d.pnl))
    for d in r_s2.deals:
        if d.kind != "entry": all_deals.append((d.ts, "FBO_S2", d.pnl))
    for d in r_orb.deals:
        if d.kind != "entry": all_deals.append((d.ts, "ORB", d.pnl))
    for d in r_lsf.deals:
        if d.kind != "entry": all_deals.append((d.ts, "LSFVG", d.pnl))
    all_deals.sort(key=lambda x: x[0])

    balance = DEPOSIT
    balance_max = DEPOSIT
    dd_abs = 0.0
    for _, _s, pnl in all_deals:
        balance += pnl
        if balance > balance_max:
            balance_max = balance
        cur = balance_max - balance
        if cur > dd_abs:
            dd_abs = cur

    return {
        "risk": risk_pct,
        "streams": {"FBO_S1": r_s1, "FBO_S2": r_s2, "ORB": r_orb, "LSFVG": r_lsf},
        "combined_np": balance - DEPOSIT,
        "combined_dd_pct": (dd_abs / balance_max * 100) if balance_max > 0 else 0,
        "combined_dd_abs": dd_abs,
        "total_trades": len(all_deals),
        "all_deals": all_deals,
    }


def stream_summary(name, r):
    wr = (r.tp_count / r.trades * 100) if r.trades > 0 else 0
    ndd = (r.net_profit / r.max_drawdown) if r.max_drawdown > 0 else 0
    return (f"  {name:<7} NP=${r.net_profit:>+9,.0f}  DD={r.max_drawdown_pct:>5.1f}%  "
            f"Tr={r.trades:>3}  TP/SL={r.tp_count}/{r.sl_count}  "
            f"PF={r.profit_factor:.2f}  WR={wr:.1f}%  NP/DD={ndd:.1f}")


def main() -> int:
    start = datetime(2026, 2, 14, tzinfo=timezone.utc)
    end = datetime(2026, 4, 24, 23, 0, tzinfo=timezone.utc)  # shrunk to fit cache (Apr 24 23:56 max)

    try:
        # Known Vantage XAUUSD meta (fallback if MT5 terminal not live)
        try:
            m = symbol_meta(SYMBOL)
            meta = SymbolMeta(
                point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                volume_min=m["volume_min"], volume_max=m["volume_max"],
                volume_step=m["volume_step"],
            )
        except RuntimeError:
            print("  [warn] MT5 symbol_meta unavailable, using hardcoded XAUUSD constants")
            meta = SymbolMeta(
                point=0.01, digits=2, tick_size=0.01, tick_value=1.0,
                stops_level_pts=0, volume_min=0.01, volume_max=500.0,
                volume_step=0.01,
            )
        ticks = load_ticks(SYMBOL, start, end)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)
        m15 = load_bars(SYMBOL, "M15", start, end)
        m30 = load_bars(SYMBOL, "M30", start, end)

        print("=" * 96)
        print("  DT818_pro RISK COMPARISON: 2% vs 3% vs 4.5% (Feb 14 -> Apr 25, $10k)")
        print("  ORB caps scale: 2%->18/12, 3%->27/18, 4.5%->40.5/27 (constant trades-to-trigger)")
        print("=" * 96)

        results = {}
        for risk in (2.0, 3.0, 4.5):
            print(f"\n  ====== RISK = {risk:.1f}% ======")
            print(f"  ORB caps: target={9.0*risk:.1f}%  loss={6.0*risk:.1f}%")
            res = run_at_risk(ticks, m1, m5, m15, m30, meta, risk)
            print()
            for name, r in res["streams"].items():
                print(stream_summary(name, r))
            ndd_combined = (res["combined_np"] / res["combined_dd_abs"]
                            if res["combined_dd_abs"] > 0 else 0)
            print(f"\n  COMBINED on shared $10k: "
                  f"NP=${res['combined_np']:>+9,.0f}  ROI={res['combined_np']/DEPOSIT*100:+.1f}%  "
                  f"DD={res['combined_dd_pct']:.1f}%  NP/DD={ndd_combined:.1f}  "
                  f"Tr={res['total_trades']}")
            results[risk] = res

        res_2, res_3, res_4_5 = results[2.0], results[3.0], results[4.5]

        # Side-by-side comparison
        print("\n" + "=" * 96)
        print("  SIDE-BY-SIDE COMPARISON")
        print("=" * 96)
        print(f"  {'Stream':<8} {'2% NP':>9} {'2% DD':>6}  {'3% NP':>9} {'3% DD':>6}  "
              f"{'4.5% NP':>9} {'4.5% DD':>7}  {'2->3 scale':>10} {'2->4.5 scale':>12}")
        print("  " + "-" * 92)
        for name in ("FBO_S1", "FBO_S2", "ORB", "LSFVG"):
            r2 = res_2["streams"][name]
            r3 = res_3["streams"][name]
            r4 = res_4_5["streams"][name]
            scale_23 = (r3.net_profit / r2.net_profit) if r2.net_profit != 0 else 0
            scale_24 = (r4.net_profit / r2.net_profit) if r2.net_profit != 0 else 0
            print(f"  {name:<8} ${r2.net_profit:>+7,.0f} {r2.max_drawdown_pct:>4.1f}%  "
                  f"${r3.net_profit:>+7,.0f} {r3.max_drawdown_pct:>4.1f}%  "
                  f"${r4.net_profit:>+7,.0f} {r4.max_drawdown_pct:>5.1f}%  "
                  f"{scale_23:>9.2f}x {scale_24:>11.2f}x")

        print("  " + "-" * 92)
        sc23 = res_3["combined_np"] / res_2["combined_np"]
        sc24 = res_4_5["combined_np"] / res_2["combined_np"]
        print(f"  {'COMBINED':<8} ${res_2['combined_np']:>+7,.0f} {res_2['combined_dd_pct']:>4.1f}%  "
              f"${res_3['combined_np']:>+7,.0f} {res_3['combined_dd_pct']:>4.1f}%  "
              f"${res_4_5['combined_np']:>+7,.0f} {res_4_5['combined_dd_pct']:>5.1f}%  "
              f"{sc23:>9.2f}x {sc24:>11.2f}x")

        ndd2 = res_2["combined_np"] / res_2["combined_dd_abs"] if res_2["combined_dd_abs"] > 0 else 0
        ndd3 = res_3["combined_np"] / res_3["combined_dd_abs"] if res_3["combined_dd_abs"] > 0 else 0
        ndd4 = res_4_5["combined_np"] / res_4_5["combined_dd_abs"] if res_4_5["combined_dd_abs"] > 0 else 0
        roi2 = res_2["combined_np"] / DEPOSIT * 100
        roi3 = res_3["combined_np"] / DEPOSIT * 100
        roi4 = res_4_5["combined_np"] / DEPOSIT * 100

        print(f"\n  Combined ROI    : 2% = {roi2:.1f}%   |  3% = {roi3:.1f}%   |  4.5% = {roi4:.1f}%")
        print(f"  Combined DD     : 2% = {res_2['combined_dd_pct']:.1f}%    |  3% = {res_3['combined_dd_pct']:.1f}%    |  4.5% = {res_4_5['combined_dd_pct']:.1f}%")
        print(f"  Combined NP/DD$ : 2% = {ndd2:.2f}    |  3% = {ndd3:.2f}    |  4.5% = {ndd4:.2f}")
        print(f"  ROI / DD%       : 2% = {roi2/res_2['combined_dd_pct']:.2f}    |  3% = {roi3/res_3['combined_dd_pct']:.2f}    |  4.5% = {roi4/res_4_5['combined_dd_pct']:.2f}")

        # Verdict — pick the highest ROI/DD% (capital efficiency)
        print(f"\n  --- VERDICT ---")
        ratios = [(2.0, roi2/res_2['combined_dd_pct']),
                  (3.0, roi3/res_3['combined_dd_pct']),
                  (4.5, roi4/res_4_5['combined_dd_pct'])]
        best = max(ratios, key=lambda x: x[1])
        print(f"  Most capital-efficient by ROI/DD%: {best[0]:.1f}% risk (ratio {best[1]:.2f})")
        print(f"  Highest absolute NP: 4.5% at ${res_4_5['combined_np']:+,.0f}")
        print(f"  Lowest DD          : 2.0% at {res_2['combined_dd_pct']:.1f}%")

    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
