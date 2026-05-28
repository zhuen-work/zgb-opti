"""Phase C OOS validation for the per-stream hedge winners.

Window: May 4 (Mon) -> today (post-May-2-WFO IS).
Streams: S1/S2/S3 with their per-stream IS-winning hedge configs (from
sim_orb_s1_hedge_sweep.py). Compares baseline vs +hedge for each.

Per the plan: pass = hedge NP >= 0 AND DD doesn't increase >2pp on OOS window.
2 trading days is very short — interpret as "didn't blow up" not "validated."

Uses XAUUSD direct-pull (sim account 18912087) since cache stops at May 1.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate

# Reuse helpers from the OOS-today script (direct MT5 pull, sim account)
import importlib.util
_spec = importlib.util.spec_from_file_location("oos_today", ROOT / "scripts" / "sim_orb_oos_today.py")
oos_today = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(oos_today)

# Reuse the hedge simulator from the sweep script
_spec2 = importlib.util.spec_from_file_location("hedge_sweep", ROOT / "scripts" / "sim_orb_s1_hedge_sweep.py")
hedge_sweep = importlib.util.module_from_spec(_spec2); _spec2.loader.exec_module(hedge_sweep)

DEPOSIT = 10_000.0
SPREAD = 30  # per feedback_default_test_conditions.md (all live = 30pt 2026-05-16)
START = datetime(2026, 5, 4, tzinfo=timezone.utc)
END = datetime.now(timezone.utc)

# Per-stream IS winners (from sim_orb_s1_hedge_sweep.py 1% sizing run)
WINNERS = {
    "S1": dict(buf=200, h_sl=800, h_rr=3.0, exp=30),
    "S2": dict(buf=0,   h_sl=500, h_rr=3.0, exp=120),
    "S3": dict(buf=50,  h_sl=800, h_rr=3.0, exp=30),
}


def main() -> int:
    days = (END - START).days + 1
    print("=" * 100)
    print(f"  HEDGE OOS VALIDATION  |  {START.date()} -> {END.strftime('%Y-%m-%d %H:%M UTC')}  ({days}d)")
    print(f"  Spread {SPREAD}pt, $10k, S1/S2/S3 baseline at 3% risk, hedge at 1% risk")
    print(f"  IS winners (from May 2 sweep on Feb 14 -> May 1):")
    for s, w in WINNERS.items():
        print(f"    {s}: buf={w['buf']} h_sl={w['h_sl']} h_rr={w['h_rr']} exp={w['exp']}min")
    print("=" * 100)

    # Direct-pull (cache doesn't cover May 4-5; sim account is XAUUSD)
    sym, m_dict = oos_today.fetch_meta(None)
    meta = SymbolMeta(point=m_dict["point"], digits=m_dict["digits"],
                      tick_size=m_dict["tick_size"], tick_value=m_dict["tick_value"],
                      stops_level_pts=m_dict["stops_level"],
                      volume_min=m_dict["volume_min"], volume_max=m_dict["volume_max"],
                      volume_step=m_dict["volume_step"])
    sym, ticks, m1, m5 = oos_today.fetch_window(None, START, END, SPREAD)
    print(f"  Symbol: {sym}  Ticks: {len(ticks):,}  M1: {len(m1):,}  M5: {len(m5):,}")

    ts_ns = ticks["ts"].dt.tz_convert("UTC").dt.tz_localize(None).astype("datetime64[ns]").astype("int64").to_numpy()
    ticks_arr = {
        "ts_ns": ts_ns,
        "bid": ticks["bid"].to_numpy(dtype=np.float64),
        "ask": ticks["ask"].to_numpy(dtype=np.float64),
    }

    print(f"\n  {'Stream':<6} {'Variant':<10} {'NP':>9} {'DD%':>6} {'NP/DD$':>7} {'PF':>5} {'trades':>6}  hedge_NP  h_n  h_WR")
    summary = []
    for s, w in WINNERS.items():
        base_deals, sl_events = hedge_sweep.run_baseline(s, ticks, m1, m5, meta)
        b_np, b_dd, b_pf, b_n = hedge_sweep.aggregate_balance_curve(base_deals)
        b_dd_abs = b_dd / 100 * (DEPOSIT + b_np)
        b_ndd = (b_np / b_dd_abs) if b_dd_abs > 0 else 0
        h_deals = hedge_sweep.simulate_hedges(sl_events, ticks_arr, w["buf"], w["h_sl"], w["h_rr"], w["exp"])
        merged = base_deals + h_deals
        h_np_total, h_dd, h_pf, h_n_total = hedge_sweep.aggregate_balance_curve(merged)
        h_dd_abs = h_dd / 100 * (DEPOSIT + h_np_total)
        h_ndd = (h_np_total / h_dd_abs) if h_dd_abs > 0 else 0
        h_only_np = sum(p for _, p in h_deals)
        h_only_n = len(h_deals)
        h_wins = sum(1 for _, p in h_deals if p > 0)
        h_wr = (h_wins / h_only_n * 100) if h_only_n else 0
        print(f"  {s:<6} {'baseline':<10} ${b_np:>+7,.0f} {b_dd:>5.2f}% {b_ndd:>7.2f} {b_pf:>5.2f} {b_n:>6}")
        print(f"  {s:<6} {'+hedge':<10} ${h_np_total:>+7,.0f} {h_dd:>5.2f}% {h_ndd:>7.2f} {h_pf:>5.2f} {h_n_total:>6}  "
              f"${h_only_np:>+6,.0f}  {h_only_n:>3}  {h_wr:>3.0f}%")
        d_np = h_np_total - b_np
        d_dd = h_dd - b_dd
        d_ndd = h_ndd - b_ndd
        # Decision per plan
        passed = (h_only_np >= 0) and (d_dd <= 2.0)
        verdict = "PASS" if passed else "FAIL"
        print(f"  {s:<6} {'delta':<10} ${d_np:>+7,.0f} {d_dd:>+5.1f}p {d_ndd:>+7.2f}                          "
              f"{verdict}  (hedge NP {'>=' if h_only_np >= 0 else '<'} 0 AND dDD {'<=' if d_dd <= 2 else '>'} 2pp)")
        summary.append((s, b_ndd, h_ndd, h_only_np, d_dd, passed))
        print()

    print("=" * 100)
    print(f"  OOS VERDICT  ({days} trading days — short, treat as smoke test)")
    print("=" * 100)
    print(f"  {'Stream':<6} {'IS NP/DD$':>10} {'OOS base':>10} {'OOS +hdg':>10} {'hedge NP':>9} {'dDD':>6}  {'Verdict':<8}")
    is_ndd = {"S1": 9.45, "S2": 4.63, "S3": 4.53}  # from IS sweep
    for s, b, h, hpnl, ddd, passed in summary:
        print(f"  {s:<6} {is_ndd[s]:>10.2f} {b:>10.2f} {h:>10.2f} ${hpnl:>+7,.0f} {ddd:>+5.1f}p  "
              f"{'PASS' if passed else 'FAIL':<8}")

    n_pass = sum(1 for *_, p in summary if p)
    print(f"\n  {n_pass}/3 streams passed.")
    if n_pass >= 2:
        print(f"  -> PROMOTE to per-stream WFO (per Phase D rule: >=2 streams pass)")
    elif n_pass == 1:
        print(f"  -> STREAM-SPECIFIC: only run WFO for the passing stream(s)")
    else:
        print(f"  -> REJECT: no streams survived OOS, log as overfit")
    return 0


if __name__ == "__main__":
    sys.exit(main())
