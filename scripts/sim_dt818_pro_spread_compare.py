"""DT818_pro spread sweep: compare combined results at spread 60/70/80 pts."""
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
SPREADS = [60, 70, 80]


def run_one(spread, ticks, m1, m5, m15, m30, meta):
    fbo_s1 = FBOS1Config(
        risk_pct=3.0, fractal_bars=8, take_profit_pts=25_000,
        stop_loss_pts=10_000, half_tp_ratio=0.3, sma_period=10,
        pending_expire_bars=2, signal_tf_minutes=30, comment="FBO_A",
    )
    fbo_s2 = FBOS1Config(
        risk_pct=3.0, fractal_bars=8, take_profit_pts=4_000,
        stop_loss_pts=4_000, half_tp_ratio=0.6, sma_period=50,
        pending_expire_bars=4, signal_tf_minutes=15, comment="FBO_B",
    )
    orb_cfg = ORBConfig(
        risk_pct=3.0, range_minutes=60, buffer_pts=0,
        min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=400, rr_ratio=3.0, half_tp_ratio=0.0,
        pending_expire_minutes=240,
        daily_target_pct=27.0, daily_loss_pct=18.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13,
        comment="ORB",
    )
    lsfvg_cfg = LSFVGConfig(
        risk_pct=3.0, signal_tf_minutes=15, lookback_bars=10,
        min_fvg_pts=20, max_fvg_pts=5000, sweep_buffer_pts=30,
        rr_ratio=2.0, half_tp_ratio=0.5, pending_expire_bars=4,
        daily_target_pct=0.0, daily_loss_pct=0.0,
        comment="LSFVG",
    )

    r_s1 = fbo_simulate(ticks, m30, m1, fbo_s1, meta, initial_balance=DEPOSIT)
    r_s2 = fbo_simulate(ticks, m15, m1, fbo_s2, meta, initial_balance=DEPOSIT)
    r_orb = orb_simulate(ticks, m5, m1, orb_cfg, meta, initial_balance=DEPOSIT)
    r_lsf = lsfvg_simulate(ticks, m15, m1, lsfvg_cfg, meta, initial_balance=DEPOSIT)

    # Merge deals on shared $10k
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

    bal = DEPOSIT
    bal_max = DEPOSIT
    dd = 0.0
    for _, _s, p in all_deals:
        bal += p
        if bal > bal_max: bal_max = bal
        cur = bal_max - bal
        if cur > dd: dd = cur
    np_ = bal - DEPOSIT
    dd_pct = dd / bal_max * 100.0 if bal_max > 0 else 0
    ndd = np_ / dd if dd > 0 else 0

    by_stream = {}
    for _, s, p in all_deals:
        by_stream.setdefault(s, [0.0, 0])
        by_stream[s][0] += p
        by_stream[s][1] += 1

    return dict(
        spread=spread, np=np_, dd=dd, dd_pct=dd_pct, ndd=ndd,
        trades=len(all_deals), by_stream=by_stream,
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
        # Load bars once (spread doesn't affect bars)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)
        m15 = load_bars(SYMBOL, "M15", start, end)
        m30 = load_bars(SYMBOL, "M30", start, end)

        print("=" * 84)
        print("  DT818_pro spread sweep  (Feb 14 -> Apr 25, $10k, 3% risk per stream)")
        print("=" * 84)

        results = []
        for sp in SPREADS:
            print(f"\n  Loading ticks @ spread={sp}...")
            ticks = load_ticks(SYMBOL, start, end, spread_pts=sp)
            r = run_one(sp, ticks, m1, m5, m15, m30, meta)
            results.append(r)

        # Header
        print("\n" + "-" * 84)
        print(f"  {'Spread':>7} {'NP':>11} {'ROI%':>8} {'DD$':>10} {'DD%':>6} "
              f"{'NP/DD':>7} {'Trades':>7}")
        print("-" * 84)
        for r in results:
            print(f"  {r['spread']:>5}pt {r['np']:>+11,.0f} "
                  f"{r['np']/DEPOSIT*100:>+7.1f}% "
                  f"{r['dd']:>+10,.0f} {r['dd_pct']:>5.1f}% "
                  f"{r['ndd']:>7.2f} {r['trades']:>7}")

        print("\n  Per-stream NP contribution:")
        print(f"  {'Spread':>7} " + " ".join(f"{s:>10}" for s in ("FBO_S1","FBO_S2","ORB","LSFVG")))
        for r in results:
            row = [f"  {r['spread']:>5}pt"]
            for s in ("FBO_S1","FBO_S2","ORB","LSFVG"):
                if s in r["by_stream"]:
                    n, _t = r["by_stream"][s]
                    row.append(f"{n:>+10,.0f}")
                else:
                    row.append(f"{'-':>10}")
            print(" ".join(row))

        # Deltas vs 60
        base = results[0]
        print("\n  Deltas vs 60pt baseline:")
        for r in results[1:]:
            d_np = r["np"] - base["np"]
            d_dd = r["dd_pct"] - base["dd_pct"]
            d_ndd = r["ndd"] - base["ndd"]
            d_tr = r["trades"] - base["trades"]
            print(f"    {r['spread']}pt: NP {d_np:+,.0f}  "
                  f"DD {d_dd:+.1f}pp  NP/DD {d_ndd:+.2f}  Trades {d_tr:+d}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
