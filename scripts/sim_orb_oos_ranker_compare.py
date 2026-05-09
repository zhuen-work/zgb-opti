"""Compare CURRENT vs ALTERNATIVE ranker top-3 on May 4 -> today OOS window.

CURRENT (production):
  S1: R=90 SL=500 RR=4.0 HTP=0.25  (rank 1 by prof_count 4/4)
  S2: R=90 SL=400 RR=4.0 HTP=0.0   (rank 2)
  S3: R=90 SL=350 RR=4.0 HTP=0.5   (rank 3)

ALTERNATIVE (sanity-check action #2: drop rank-1, promote next):
  S1: R=90 SL=400 RR=4.0 HTP=0.0   (was S2, IS NP $12,576)
  S2: R=90 SL=350 RR=4.0 HTP=0.5   (was S3, IS NP $11,946)
  S3: R=90 SL=400 RR=4.0 HTP=0.5   (was rank 4, IS NP $10,300)

OOS window: 2026-05-04 (Mon, post-IS) -> today.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import importlib.util
_spec = importlib.util.spec_from_file_location("oos_today", ROOT / "scripts" / "sim_orb_oos_today.py")
oos = importlib.util.module_from_spec(_spec)
sys.modules["oos_today"] = oos
_spec.loader.exec_module(oos)

from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.tick_loader import kill_mt5_terminal

DEPOSIT = 10_000.0
SPREAD = 23
START = datetime(2026, 5, 4, tzinfo=timezone.utc)
END = datetime.now(timezone.utc)

# (label, range, SL, RR, HTP)
CURRENT = [
    ("S1", 90, 500, 4.0, 0.25),
    ("S2", 90, 400, 4.0, 0.0),
    ("S3", 90, 350, 4.0, 0.5),
]
ALTERNATIVE = [
    ("S1_alt", 90, 400, 4.0, 0.0),    # was rank 2
    ("S2_alt", 90, 350, 4.0, 0.5),    # was rank 3
    ("S3_alt", 90, 400, 4.0, 0.5),    # was rank 4
]


def make_cfg(label, rng, sl, rr, htp, risk_pct):
    return ORBConfig(
        risk_pct=risk_pct, range_minutes=rng, buffer_pts=0,
        min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=sl, rr_ratio=rr, half_tp_ratio=htp,
        pending_expire_minutes=240, daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True,  ny_start_hour=13, comment=label,
    )


def aggregate(deals):
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0; gp = gl = 0.0
    for _, _s, p in sorted(deals, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p > 0: gp += p
        elif p < 0: gl += p
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    pf = (gp / abs(gl)) if gl < 0 else float("inf")
    ndd = (np_ / dd_abs) if dd_abs > 0 else 0
    return np_, dd_pct, pf, ndd


def run_portfolio(streams, ticks, m1, m5, meta, total_risk):
    per_stream = total_risk / len(streams)
    deals = []; per_s = {}
    for label, rng, sl, rr, htp in streams:
        cfg = make_cfg(label, rng, sl, rr, htp, per_stream)
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        np_s = 0.0; tr_s = 0
        for d in r.deals:
            if d.kind != "entry":
                deals.append((d.ts, label, d.pnl))
                np_s += d.pnl; tr_s += 1
        per_s[label] = (np_s, tr_s)
    return aggregate(deals), per_s, len(deals)


def main():
    days = (END - START).days + 1
    print("=" * 100)
    print(f"  OOS RANKER COMPARE  |  {START.date()} -> {END.strftime('%Y-%m-%d %H:%M UTC')}  ({days}d)")
    print(f"  Spread {SPREAD}pt, $10k, total risk split N=3")
    print("  CURRENT:    S1=500/4.0/0.25  S2=400/4.0/0.0  S3=350/4.0/0.5")
    print("  ALTERNATIVE: S1=400/4.0/0.0   S2=350/4.0/0.5  S3=400/4.0/0.5  (rank-1 dropped, rank-4 added)")
    print("=" * 100)

    sym, m_dict = oos.fetch_meta(None)
    meta = SymbolMeta(point=m_dict["point"], digits=m_dict["digits"], tick_size=m_dict["tick_size"],
                      tick_value=m_dict["tick_value"], stops_level_pts=m_dict["stops_level"],
                      volume_min=m_dict["volume_min"], volume_max=m_dict["volume_max"],
                      volume_step=m_dict["volume_step"])
    sym, ticks, m1, m5 = oos.fetch_window(None, START, END, SPREAD)
    print(f"  Symbol={sym}  Ticks={len(ticks):,}  M1={len(m1):,}  M5={len(m5):,}")

    print(f"\n  {'Variant':<14} {'Risk':<5} {'NP':>10} {'NP-haircut':>12} {'ROI':>7} {'DD%':>6} "
          f"{'NP/DD$':>7} {'Trades':>7}  Per-stream NP")
    try:
        for total_risk in (3.0, 4.5, 6.0):
            for variant_name, streams in [("CURRENT", CURRENT), ("ALTERNATIVE", ALTERNATIVE)]:
                (np_, dd, pf, ndd), per_s, n_trades = run_portfolio(streams, ticks, m1, m5, meta, total_risk)
                np_haircut = np_ * 0.94
                roi = np_ / DEPOSIT * 100
                ps_str = " ".join(f"{lab[:7]}:${ps:+,.0f}({tr})"
                                   for lab, (ps, tr) in per_s.items())
                print(f"  {variant_name:<14} {total_risk:>4.1f}% ${np_:>+8,.0f} ${np_haircut:>+10,.0f} "
                      f"{roi:>+6.1f}% {dd:>5.1f}% {ndd:>7.2f} {n_trades:>7}  {ps_str}")
            # Delta line
            print()
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
