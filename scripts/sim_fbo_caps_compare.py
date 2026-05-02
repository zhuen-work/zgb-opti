"""Post-hoc test: would FBO joint daily caps help vs uncapped?

Approximation: run FBO_S1 + FBO_S2 uncapped; merge deals chronologically;
walk day-by-day tracking joint realized PnL on the SHARED $10k account.
When joint PnL crosses target or -loss, drop all subsequent FBO deals for
that day (approximates the EA's joint magic-1000 cap behavior).

Only realized PnL is visible post-hoc, so this UNDER-counts cap firings
relative to the EA (which also reads unrealized). Treat as directional.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

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


def apply_joint_fbo_caps(s1_deals, s2_deals, target_pct, loss_pct, balance):
    """Walk merged FBO_S1+S2 deals day-by-day; drop deals after joint cap fires.
    Returns filtered (s1_deals, s2_deals).
    """
    if target_pct <= 0 and loss_pct <= 0:
        return s1_deals, s2_deals

    target_money = balance * target_pct / 100.0 if target_pct > 0 else None
    loss_money = balance * loss_pct / 100.0 if loss_pct > 0 else None

    # Tag each deal with its source so we can split back at the end
    tagged = []
    for d in s1_deals:
        tagged.append(("S1", d))
    for d in s2_deals:
        tagged.append(("S2", d))
    tagged.sort(key=lambda x: x[1].ts)

    keep_s1, keep_s2 = [], []
    cur_day = None
    day_pnl = 0.0
    locked = False

    for src, d in tagged:
        day = pd.Timestamp(d.ts).date()
        if day != cur_day:
            cur_day = day
            day_pnl = 0.0
            locked = False
        if locked:
            continue
        # Always keep the deal (cap fires AFTER the deal that crosses the line —
        # so the triggering deal is included; only subsequent ones are dropped)
        if src == "S1":
            keep_s1.append(d)
        else:
            keep_s2.append(d)
        if d.kind != "entry":
            day_pnl += d.pnl
            if target_money is not None and day_pnl >= target_money:
                locked = True
            elif loss_money is not None and day_pnl <= -loss_money:
                locked = True
    return keep_s1, keep_s2


def _merge(*deal_lists_with_names):
    flat = []
    for name, deals in deal_lists_with_names:
        for d in deals:
            if d.kind != "entry":
                flat.append((d.ts, name, d.pnl))
    flat.sort(key=lambda t: t[0])
    return flat


def _metrics(deals_flat, deposit=DEPOSIT):
    bal = deposit
    bal_max = deposit
    dd = 0.0
    for _, _s, p in deals_flat:
        bal += p
        if bal > bal_max: bal_max = bal
        cur = bal_max - bal
        if cur > dd: dd = cur
    np_ = bal - deposit
    dd_pct = (dd / bal_max * 100.0) if bal_max > 0 else 0.0
    ndd = (np_ / dd) if dd > 0 else 0.0
    by_stream = {}
    for _, s, p in deals_flat:
        by_stream.setdefault(s, [0.0, 0])
        by_stream[s][0] += p
        by_stream[s][1] += 1
    return dict(np=np_, dd=dd, dd_pct=dd_pct, ndd=ndd,
                trades=len(deals_flat), by_stream=by_stream)


def main() -> int:
    start = datetime(2026, 2, 14, tzinfo=timezone.utc)
    end = datetime(2026, 4, 25, tzinfo=timezone.utc)
    days = (end - start).days

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        ticks = load_ticks(SYMBOL, start, end)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)
        m15 = load_bars(SYMBOL, "M15", start, end)
        m30 = load_bars(SYMBOL, "M30", start, end)

        print("=" * 100)
        print(f"  FBO joint-cap probe ({days}d, $10k, 3% risk, 70pt) — POST-HOC approximation")
        print(f"  Joint cap = sum(FBO_S1 + FBO_S2 realized) day-by-day; drop subsequent FBO deals after trigger.")
        print(f"  NOTE: post-hoc sees only realized PnL; EA also reads unrealized -- triggers under-counted.")
        print("=" * 100)

        # Run all 5 streams once, then probe FBO cap variants
        fbo_s1_cfg = FBOS1Config(
            risk_pct=RISK, fractal_bars=8, take_profit_pts=25_000,
            stop_loss_pts=10_000, half_tp_ratio=0.3, sma_period=10,
            pending_expire_bars=2, signal_tf_minutes=30, comment="FBO_A",
        )
        fbo_s2_cfg = FBOS1Config(
            risk_pct=RISK, fractal_bars=8, take_profit_pts=4_000,
            stop_loss_pts=4_000, half_tp_ratio=0.6, sma_period=50,
            pending_expire_bars=4, signal_tf_minutes=15, comment="FBO_B",
        )
        orb_cfg = ORBConfig(
            risk_pct=RISK, range_minutes=90, buffer_pts=0,
            min_range_pts=200, max_range_pts=5000,
            fixed_sl_pts=350, rr_ratio=2.0, half_tp_ratio=0.0,
            pending_expire_minutes=240,
            daily_target_pct=9.0, daily_loss_pct=6.0,
            ldn_enabled=True, ldn_start_hour=7,
            ny_enabled=True, ny_start_hour=13,
            comment="ORB",
        )
        lsfvg_cfg = LSFVGConfig(
            risk_pct=RISK, signal_tf_minutes=15, lookback_bars=10,
            min_fvg_pts=20, max_fvg_pts=5000, sweep_buffer_pts=30,
            rr_ratio=2.0, half_tp_ratio=0.5, pending_expire_bars=4,
            daily_target_pct=0.0, daily_loss_pct=0.0, comment="LSFVG",
        )
        ep_cfg = EMAPullbackConfig(
            risk_pct=RISK, signal_tf_minutes=15, ema_period=50,
            lookback_bars=3, pullback_band_pts=150,
            entry_buffer_pts=0, sl_buffer_pts=30,
            rr_ratio=2.0, half_tp_ratio=0.0, pending_expire_bars=3,
            daily_target_pct=0.0, daily_loss_pct=6.0, comment="EMAPullback",
        )

        print("\n  Running 5 streams (uncapped FBO)...")
        r_s1 = fbo_simulate(ticks, m30, m1, fbo_s1_cfg, meta, initial_balance=DEPOSIT)
        r_s2 = fbo_simulate(ticks, m15, m1, fbo_s2_cfg, meta, initial_balance=DEPOSIT)
        r_orb = orb_simulate(ticks, m5, m1, orb_cfg, meta, initial_balance=DEPOSIT)
        r_lsf = lsfvg_simulate(ticks, m15, m1, lsfvg_cfg, meta, initial_balance=DEPOSIT)
        r_ep = ep_simulate(ticks, m15, m1, ep_cfg, meta, initial_balance=DEPOSIT)

        # Probe FBO joint cap settings
        cap_settings = [
            (0.0, 0.0, "0 / 0 (current — uncapped)"),
            (15.0, 10.0, "15 / 10 (loose)"),
            (12.0, 8.0,  "12 / 8  (medium)"),
            (9.0,  6.0,  "9 / 6   (matches ORB/EMP)"),
            (6.0,  4.0,  "6 / 4   (tight)"),
        ]

        print(f"\n  {'FBO caps':<26} {'Days':>5} {'Combined NP':>12} {'ROI':>8} "
              f"{'DD%':>6} {'NP/DD':>7} {'Trades':>7} {'Tr/day':>7} "
              f"{'FBO_S1':>9} {'FBO_S2':>9}")
        print("-" * 100)

        results = []
        for tgt, loss, label in cap_settings:
            s1_filt, s2_filt = apply_joint_fbo_caps(r_s1.deals, r_s2.deals, tgt, loss, DEPOSIT)
            merged = _merge(("FBO_S1", s1_filt), ("FBO_S2", s2_filt),
                           ("ORB", r_orb.deals), ("LSFVG", r_lsf.deals),
                           ("EMAPullback", r_ep.deals))
            mm = _metrics(merged)
            roi = mm["np"] / DEPOSIT * 100
            s1_np = mm["by_stream"].get("FBO_S1", [0, 0])[0]
            s2_np = mm["by_stream"].get("FBO_S2", [0, 0])[0]
            print(f"  {label:<26} {days:>5} {mm['np']:>+12,.0f} {roi:>+7.1f}% "
                  f"{mm['dd_pct']:>5.1f}% {mm['ndd']:>7.2f} "
                  f"{mm['trades']:>7} {mm['trades']/days:>7.2f} "
                  f"${s1_np:>+8,.0f} ${s2_np:>+8,.0f}")
            results.append((tgt, loss, label, mm))

        # Compare each capped variant vs baseline (no caps)
        baseline = results[0][3]
        print(f"\n  Delta vs uncapped baseline (NP +${baseline['np']:,.0f} / DD {baseline['dd_pct']:.1f}% / NP/DD {baseline['ndd']:.2f}):")
        for tgt, loss, label, mm in results[1:]:
            d_np = mm["np"] - baseline["np"]
            d_dd = mm["dd_pct"] - baseline["dd_pct"]
            d_ndd = mm["ndd"] - baseline["ndd"]
            print(f"    {label:<26}  NP {d_np:+,.0f}  DD {d_dd:+.1f}pp  NP/DD {d_ndd:+.2f}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
