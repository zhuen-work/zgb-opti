"""DT818_pro combined sanity at 2% / 3% / 4.5% — with new ORB W70 winner.

ORB params: Range=90, FixSL=350, RR=2.0, HTP=0.0
ORB caps scale by (R / 3.0) since W70 WFO baseline = 3% risk:
  2.0% risk -> Tgt=6.0%, Loss=4.0%
  3.0% risk -> Tgt=9.0%, Loss=6.0%
  4.5% risk -> Tgt=13.5%, Loss=9.0%
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
WFO_BASELINE_RISK = 3.0  # ORB W70 WFO ran at 3% risk

RISK_LEVELS = [
    # (risk_pct, prior_combined_np_for_compare)
    (2.0, 19_169),   # prior 2pct combined NP from old setfile header
    (3.0, 29_366),   # prior 3pct combined NP from old setfile header
    (4.5, 46_113),   # prior 4.5pct combined NP from old setfile header
    (6.0, 0),        # 6pct: no prior baseline (new setfile)
]


def run_one(risk, ticks, m1, m5, m15, m30, meta):
    orb_tgt = 9.0 * (risk / WFO_BASELINE_RISK)
    orb_loss = 6.0 * (risk / WFO_BASELINE_RISK)
    emp_loss = 6.0 * (risk / WFO_BASELINE_RISK)  # EMP loss cap scales same way

    fbo_s1 = FBOS1Config(
        risk_pct=risk, fractal_bars=8, take_profit_pts=25_000,
        stop_loss_pts=10_000, half_tp_ratio=0.3, sma_period=10,
        pending_expire_bars=2, signal_tf_minutes=30, comment="FBO_A",
    )
    fbo_s2 = FBOS1Config(
        risk_pct=risk, fractal_bars=8, take_profit_pts=4_000,
        stop_loss_pts=4_000, half_tp_ratio=0.6, sma_period=50,
        pending_expire_bars=4, signal_tf_minutes=15, comment="FBO_B",
    )
    orb_cfg = ORBConfig(
        risk_pct=risk, range_minutes=90, buffer_pts=0,
        min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=350, rr_ratio=2.0, half_tp_ratio=0.0,
        pending_expire_minutes=240,
        daily_target_pct=orb_tgt, daily_loss_pct=orb_loss,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13,
        comment="ORB",
    )
    lsfvg_cfg = LSFVGConfig(
        risk_pct=risk, signal_tf_minutes=15, lookback_bars=10,
        min_fvg_pts=20, max_fvg_pts=5000, sweep_buffer_pts=30,
        rr_ratio=2.0, half_tp_ratio=0.5, pending_expire_bars=4,
        daily_target_pct=0.0, daily_loss_pct=0.0,
        comment="LSFVG",
    )
    ep_cfg = EMAPullbackConfig(
        risk_pct=risk, signal_tf_minutes=15, ema_period=50,
        lookback_bars=3, pullback_band_pts=150,
        entry_buffer_pts=0, sl_buffer_pts=30,
        rr_ratio=2.0, half_tp_ratio=0.0,
        pending_expire_bars=3,
        daily_target_pct=0.0, daily_loss_pct=emp_loss,
        comment="EMAPullback",
    )

    r_s1 = fbo_simulate(ticks, m30, m1, fbo_s1, meta, initial_balance=DEPOSIT)
    r_s2 = fbo_simulate(ticks, m15, m1, fbo_s2, meta, initial_balance=DEPOSIT)
    r_orb = orb_simulate(ticks, m5, m1, orb_cfg, meta, initial_balance=DEPOSIT)
    r_lsf = lsfvg_simulate(ticks, m15, m1, lsfvg_cfg, meta, initial_balance=DEPOSIT)
    r_ep = ep_simulate(ticks, m15, m1, ep_cfg, meta, initial_balance=DEPOSIT)

    deals = []
    for d in r_s1.deals:
        if d.kind != "entry": deals.append((d.ts, "FBO_S1", d.pnl))
    for d in r_s2.deals:
        if d.kind != "entry": deals.append((d.ts, "FBO_S2", d.pnl))
    for d in r_orb.deals:
        if d.kind != "entry": deals.append((d.ts, "ORB", d.pnl))
    for d in r_lsf.deals:
        if d.kind != "entry": deals.append((d.ts, "LSFVG", d.pnl))
    for d in r_ep.deals:
        if d.kind != "entry": deals.append((d.ts, "EMAPullback", d.pnl))
    deals.sort(key=lambda x: x[0])

    bal = DEPOSIT
    bal_max = DEPOSIT
    dd_abs = 0.0
    for _, _s, pnl in deals:
        bal += pnl
        if bal > bal_max: bal_max = bal
        cur = bal_max - bal
        if cur > dd_abs: dd_abs = cur
    np_ = bal - DEPOSIT
    dd_pct = (dd_abs / bal_max * 100.0) if bal_max > 0 else 0
    ndd = (np_ / dd_abs) if dd_abs > 0 else 0

    by_stream = {}
    for _, s, p in deals:
        by_stream.setdefault(s, [0.0, 0])
        by_stream[s][0] += p
        by_stream[s][1] += 1

    return dict(
        risk=risk, orb_tgt=orb_tgt, orb_loss=orb_loss,
        np=np_, dd=dd_abs, dd_pct=dd_pct, ndd=ndd,
        trades=len(deals), by_stream=by_stream,
        standalone_orb_dd=r_orb.max_drawdown_pct,
        standalone_orb_pf=r_orb.profit_factor,
        standalone_orb_tr=r_orb.trades,
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
        m15 = load_bars(SYMBOL, "M15", start, end)
        m30 = load_bars(SYMBOL, "M30", start, end)

        print("=" * 92)
        print("  DT818_pro combined sanity — 2% / 3% / 4.5%  (Feb 14 -> Apr 25, $10k, spread=70)")
        print("  ORB W70: Range=90 FixSL=350 RR=2.0 HTP=0.0  caps scaled by R/3.0")
        print("=" * 92)

        results = []
        for risk, prior in RISK_LEVELS:
            r = run_one(risk, ticks, m1, m5, m15, m30, meta)
            r["prior_np"] = prior
            results.append(r)

        # Summary table
        days = (end - start).days
        print(f"\n  {'Risk':>5} {'ORB caps':>11} {'Days':>5} {'NP':>11} {'ROI':>8} "
              f"{'DD%':>6} {'NP/DD':>7} {'Trades':>7} {'Tr/day':>7} "
              f"{'EP NP':>9} {'vs prior':>9}")
        print("-" * 100)
        for r in results:
            cap_label = f"{r['orb_tgt']:.1f}/{r['orb_loss']:.1f}"
            roi = r["np"] / DEPOSIT * 100
            ep_np = r["by_stream"].get("EMAPullback", [0, 0])[0]
            delta = r["np"] - r["prior_np"]
            print(f"  {r['risk']:>4}% {cap_label:>11} {days:>5} {r['np']:>+11,.0f} "
                  f"{roi:>+7.1f}% {r['dd_pct']:>5.1f}% {r['ndd']:>7.2f} "
                  f"{r['trades']:>7} {r['trades']/days:>7.2f} "
                  f"${ep_np:>+8,.0f} {delta:>+9,.0f}")

        # Per-stream table
        print(f"\n  Per-stream NP contribution (combined):")
        print(f"  {'Risk':>5} " + " ".join(f"{s:>11}" for s in ("FBO_S1","FBO_S2","ORB","LSFVG","EMAPullback")))
        for r in results:
            row = [f"  {r['risk']:>4}%"]
            for s in ("FBO_S1","FBO_S2","ORB","LSFVG","EMAPullback"):
                if s in r["by_stream"]:
                    n, t = r["by_stream"][s]
                    row.append(f"{n:>+11,.0f}")
                else:
                    row.append(f"{'-':>11}")
            print(" ".join(row))

        # Standalone ORB diagnostics
        print(f"\n  Standalone ORB diagnostics:")
        print(f"  {'Risk':>5} {'StdAln DD':>10} {'StdAln PF':>10} {'StdAln Tr':>10}")
        for r in results:
            print(f"  {r['risk']:>4}% {r['standalone_orb_dd']:>9.1f}% "
                  f"{r['standalone_orb_pf']:>10.2f} {r['standalone_orb_tr']:>10}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
