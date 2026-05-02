"""3-week per-stream breakdown for DT818_pro.

Three consecutive weeks (Sun-Sat in UTC):
  W-2:  Apr 12 -> Apr 19  (in IS, week ending Apr 18)
  W-1:  Apr 19 -> Apr 26  (in IS, week ending Apr 25)
  OOS:  Apr 26 -> May  2  (just outside IS, week ending May 2)

Conditions: 3% risk, 70pt spread, $10k, 5-stream standalone-on-$10k each
then deal-merge on shared $10k for combined.
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
from zgb_sim.ema_pullback import EMAPullbackConfig
from zgb_sim.ema_pullback_fast import simulate_fast as ep_simulate

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
RISK = 3.0


def make_configs():
    fbo_s1 = FBOS1Config(risk_pct=RISK, fractal_bars=8, take_profit_pts=25_000,
                          stop_loss_pts=10_000, half_tp_ratio=0.3, sma_period=10,
                          pending_expire_bars=2, signal_tf_minutes=30, comment="FBO_A")
    fbo_s2 = FBOS1Config(risk_pct=RISK, fractal_bars=8, take_profit_pts=4_000,
                          stop_loss_pts=4_000, half_tp_ratio=0.6, sma_period=50,
                          pending_expire_bars=4, signal_tf_minutes=15, comment="FBO_B")
    orb = ORBConfig(risk_pct=RISK, range_minutes=90, buffer_pts=0,
                    min_range_pts=200, max_range_pts=5000,
                    fixed_sl_pts=350, rr_ratio=2.0, half_tp_ratio=0.0,
                    pending_expire_minutes=240,
                    daily_target_pct=9.0, daily_loss_pct=6.0,
                    ldn_enabled=True, ldn_start_hour=7,
                    ny_enabled=True, ny_start_hour=13, comment="ORB")
    lsfvg = LSFVGConfig(risk_pct=RISK, signal_tf_minutes=15, lookback_bars=10,
                         min_fvg_pts=20, max_fvg_pts=5000, sweep_buffer_pts=30,
                         rr_ratio=2.0, half_tp_ratio=0.5, pending_expire_bars=4,
                         daily_target_pct=0.0, daily_loss_pct=0.0, comment="LSFVG")
    ep = EMAPullbackConfig(risk_pct=RISK, signal_tf_minutes=15, ema_period=50,
                            lookback_bars=3, pullback_band_pts=150,
                            entry_buffer_pts=0, sl_buffer_pts=30,
                            rr_ratio=2.0, half_tp_ratio=0.0, pending_expire_bars=3,
                            daily_target_pct=0.0, daily_loss_pct=6.0, comment="EMP")
    return fbo_s1, fbo_s2, orb, lsfvg, ep


def run_week(label: str, start: datetime, end: datetime, meta: SymbolMeta) -> dict:
    days = (end - start).days
    print(f"\n  Loading data {label} ({start.date()} -> {end.date()})...")
    ticks = load_ticks(SYMBOL, start, end)
    m1 = load_bars(SYMBOL, "M1", start, end)
    m5 = load_bars(SYMBOL, "M5", start, end)
    m15 = load_bars(SYMBOL, "M15", start, end)
    m30 = load_bars(SYMBOL, "M30", start, end)

    fbo_s1, fbo_s2, orb, lsfvg, ep = make_configs()
    r_s1 = fbo_simulate(ticks, m30, m1, fbo_s1, meta, initial_balance=DEPOSIT)
    r_s2 = fbo_simulate(ticks, m15, m1, fbo_s2, meta, initial_balance=DEPOSIT)
    r_orb = orb_simulate(ticks, m5, m1, orb, meta, initial_balance=DEPOSIT)
    r_lsf = lsfvg_simulate(ticks, m15, m1, lsfvg, meta, initial_balance=DEPOSIT)
    r_ep = ep_simulate(ticks, m15, m1, ep, meta, initial_balance=DEPOSIT)

    streams = {"FBO_S1": r_s1, "FBO_S2": r_s2, "ORB": r_orb, "LSFVG": r_lsf, "EMP": r_ep}

    # Combined deal-merge
    all_deals = []
    for s, r in streams.items():
        for d in r.deals:
            if d.kind != "entry":
                all_deals.append((d.ts, s, d.pnl))
    all_deals.sort(key=lambda x: x[0])

    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    for _, _s, p in all_deals:
        bal += p
        if bal > bal_max: bal_max = bal
        cur = bal_max - bal
        if cur > dd_abs: dd_abs = cur
    combined_np = bal - DEPOSIT
    combined_dd_pct = (dd_abs / bal_max * 100) if bal_max > 0 else 0
    combined_ndd = combined_np / dd_abs if dd_abs > 0 else 0

    return dict(
        label=label, days=days, streams=streams, all_deals=all_deals,
        combined_np=combined_np, combined_dd_pct=combined_dd_pct, combined_ndd=combined_ndd,
    )


def main() -> int:
    weeks = [
        ("W-2 (Apr 12-19)", datetime(2026, 4, 12, tzinfo=timezone.utc),
                            datetime(2026, 4, 19, tzinfo=timezone.utc)),
        ("W-1 (Apr 19-26)", datetime(2026, 4, 19, tzinfo=timezone.utc),
                            datetime(2026, 4, 26, tzinfo=timezone.utc)),
        ("OOS (Apr 26-May 2)", datetime(2026, 4, 26, tzinfo=timezone.utc),
                                datetime(2026, 5,  2, tzinfo=timezone.utc)),
    ]

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])

        print("=" * 100)
        print(f"  DT818_pro 3-week per-stream breakdown ($10k, 3% risk, 70pt)")
        print(f"  Reference IS rate: ~$3,768/week (NP +$37,680 over 70d)")
        print("=" * 100)

        results = []
        for label, start, end in weeks:
            r = run_week(label, start, end, meta)
            results.append(r)

        # Per-stream cross-week table
        print(f"\n  {'Stream':<10} ", end="")
        for r in results:
            print(f"{r['label']:>22}", end="")
        print()
        print("-" * (10 + 22 * len(results)))
        for s in ("FBO_S1", "FBO_S2", "ORB", "LSFVG", "EMP"):
            print(f"  {s:<10} ", end="")
            for r in results:
                sr = r["streams"][s]
                wr = (sr.tp_count / sr.trades * 100) if sr.trades > 0 else 0
                cell = f"${sr.net_profit:>+6,.0f} {sr.trades:>2}t {wr:>4.0f}%"
                print(f"{cell:>22}", end="")
            print()

        # Combined row
        print(f"  {'COMBINED':<10} ", end="")
        for r in results:
            cell = f"${r['combined_np']:>+6,.0f} DD{r['combined_dd_pct']:>4.1f}%"
            print(f"{cell:>22}", end="")
        print()

        # Trade counts
        print(f"\n  {'Stream':<10} ", end="")
        for r in results:
            print(f"{r['label']:>22}", end="")
        print()
        for s in ("FBO_S1", "FBO_S2", "ORB", "LSFVG", "EMP"):
            print(f"  {s:<10} ", end="")
            for r in results:
                sr = r["streams"][s]
                cell = f"TP/SL/O={sr.tp_count}/{sr.sl_count}/{sr.other_count}"
                print(f"{cell:>22}", end="")
            print()

        # Vs IS pro-rated per-week summary
        print(f"\n  vs IS pro-rated ($3,768/week expected, DD ~9.7%):")
        print(f"  {'Week':<22} {'NP':>9} {'ROI':>7} {'DD%':>6} {'NP/DD':>7} {'%-of-IS':>9} {'Tr':>4}")
        for r in results:
            roi = r["combined_np"] / DEPOSIT * 100
            weekly_pace = r["combined_np"] / r["days"] * 7
            ratio = weekly_pace / 3768 * 100
            print(f"  {r['label']:<22} ${r['combined_np']:>+8,.0f} {roi:>+6.1f}% "
                  f"{r['combined_dd_pct']:>5.1f}% {r['combined_ndd']:>+7.2f} "
                  f"{ratio:>+8.0f}% {len(r['all_deals']):>4}")

        # Trend per stream
        print(f"\n  3-week per-stream NP trend:")
        for s in ("FBO_S1", "FBO_S2", "ORB", "LSFVG", "EMP"):
            nps = [r["streams"][s].net_profit for r in results]
            arrow = "->".join(f"${v:+.0f}" for v in nps)
            total = sum(nps)
            print(f"    {s:<10}: {arrow}   total: ${total:+,.0f}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
