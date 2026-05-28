"""Today's OOS with per-stream HEDGE applied — for comparison vs the live hedge EA.

Pulls today's ticks (XAUUSD, sim account), runs the 3-stream parent portfolio
+ per-stream hedge (from output/wfo_hedge_per_stream_may2/), reports baseline +
+hedge side-by-side for each risk level.

Hedge cfg: per-stream (sim_wfo_hedge.py May 2 winners):
  S1: buf=350 SL=500 RR=4.0 exp=30
  S2: buf=100 SL=500 RR=4.0 exp=120
  S3: buf=100 SL=500 RR=4.0 exp=120
Hedge sized at per-stream-risk (mirrors parent allocation).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import importlib.util
_spec_oos = importlib.util.spec_from_file_location("oos_today", ROOT / "scripts" / "sim_orb_oos_today.py")
oos = importlib.util.module_from_spec(_spec_oos); sys.modules["oos_today"] = oos
_spec_oos.loader.exec_module(oos)

_spec_hg = importlib.util.spec_from_file_location("wfo_hedge", ROOT / "scripts" / "sim_wfo_hedge.py")
hg = importlib.util.module_from_spec(_spec_hg); sys.modules["wfo_hedge"] = hg
_spec_hg.loader.exec_module(hg)

from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.tick_loader import kill_mt5_terminal

DEPOSIT = 10_000.0
SPREAD = 30  # per feedback_default_test_conditions.md (all live = 30pt 2026-05-16)
HEDGE_DIR = ROOT / "output" / "wfo_hedge_per_stream_may2"

# OOS window: today only (00:00 UTC -> now)
_now = datetime.now(timezone.utc)
START = datetime(_now.year, _now.month, _now.day, tzinfo=timezone.utc)
END = _now


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


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", choices=["sim", "live"], default="sim",
                    help="MT5 account for tick data. 'live' uses XAUUSD.sc to match EA's symbol.")
    args = ap.parse_args()
    days = (END - START).days + 1
    print("=" * 100)
    print(f"  ORB+HEDGE OOS today  |  {START.date()} -> {END.strftime('%Y-%m-%d %H:%M UTC')}  ({days}d)")
    print(f"  Spread {SPREAD}pt, $10k, hedge from {HEDGE_DIR.name}, account={args.account}")
    print("=" * 100)

    # Per-stream parent rows from May 2 WFO
    rows = [
        {"label": "S1", "range_minutes": 90, "fixed_sl_pts": 500, "rr_ratio": 4.0,
         "half_tp_ratio": 0.25},
        {"label": "S2", "range_minutes": 90, "fixed_sl_pts": 400, "rr_ratio": 4.0,
         "half_tp_ratio": 0.0},
        {"label": "S3", "range_minutes": 90, "fixed_sl_pts": 350, "rr_ratio": 4.0,
         "half_tp_ratio": 0.5},
    ]
    hedge_cfgs = {}
    for s in ("S1", "S2", "S3"):
        h = json.loads((HEDGE_DIR / f"{s}.json").read_text())
        hedge_cfgs[s] = hg.HedgeCfg(buf=h["buffer_pts"], h_sl=h["fixed_sl_pts"],
                                     h_rr=h["rr_ratio"], exp=h["expire_minutes"])
        print(f"  Hedge {s}: buf={h['buffer_pts']} h_sl={h['fixed_sl_pts']} "
              f"h_rr={h['rr_ratio']} exp={h['expire_minutes']}min")
    print()

    sym, m_dict = oos.fetch_meta(None, account=args.account)
    meta = SymbolMeta(point=m_dict["point"], digits=m_dict["digits"],
                      tick_size=m_dict["tick_size"], tick_value=m_dict["tick_value"],
                      stops_level_pts=m_dict["stops_level"],
                      volume_min=m_dict["volume_min"], volume_max=m_dict["volume_max"],
                      volume_step=m_dict["volume_step"])
    sym, ticks, m1, m5 = oos.fetch_window(None, START, END, SPREAD, account=args.account)
    t_arr = hg.ts_arr_from_ticks(ticks)
    print(f"  Account={args.account}  Symbol={sym}  Ticks={len(ticks):,}  M1={len(m1):,}  M5={len(m5):,}")

    print(f"\n  {'TotalRisk':<10} {'Variant':<8} {'NP':>10} {'NP-haircut':>12} "
          f"{'ROI':>7} {'DD%':>6} {'NP/DD$':>7} {'Trades':>7}  Per-stream")
    try:
        for total_risk in (3.0, 4.5, 6.0):
            per_stream = total_risk / 3
            hg.HEDGE_RISK_PCT = per_stream  # mirror per-stream allocation
            # Build parent configs
            cfgs = []
            for r in rows:
                # NOTE: ldn=7/ny=13 are BROKER hours; with Vantage at UTC+3
                # these are REAL UTC 04/10 (NOT real LDN/NY). See
                # reference_vantage_broker_time.md.
                cfg = ORBConfig(
                    risk_pct=per_stream, range_minutes=r["range_minutes"],
                    buffer_pts=0, min_range_pts=200, max_range_pts=5000,
                    fixed_sl_pts=r["fixed_sl_pts"], rr_ratio=r["rr_ratio"],
                    half_tp_ratio=r["half_tp_ratio"], pending_expire_minutes=240,
                    daily_target_pct=0.0, daily_loss_pct=0.0,
                    ldn_enabled=True, ldn_start_hour=7,
                    ny_enabled=True, ny_start_hour=13, comment=r["label"],
                )
                cfgs.append((r["label"], cfg))

            base_deals = []; per_s_base = {}
            sl_events_per_stream = {}
            for lab, cfg in cfgs:
                r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
                np_s = 0.0; tr_s = 0
                sl_events = []
                for d in r.deals:
                    if d.kind == "entry":
                        continue
                    base_deals.append((d.ts, lab, d.pnl))
                    np_s += d.pnl; tr_s += 1
                    if d.kind == "sl":
                        sl_events.append({"ts_ns": pd.Timestamp(d.ts).value,
                                          "direction": int(d.direction),
                                          "sl_price": float(d.price),
                                          "lots": float(d.lots)})
                per_s_base[lab] = (np_s, tr_s)
                sl_events_per_stream[lab] = sl_events
            np_b, dd_b, pf_b, ndd_b = aggregate(base_deals)
            ps_str = " ".join(f"{s}:${ps:+,.0f}({tr})"
                                for s, (ps, tr) in per_s_base.items())
            print(f"  {total_risk:>5.1f}%    {'base':<8} ${np_b:>+8,.0f} ${np_b*0.94:>+10,.0f} "
                  f"{np_b/DEPOSIT*100:>+6.1f}% {dd_b:>5.1f}% {ndd_b:>7.2f} {len(base_deals):>7}  {ps_str}")

            # +hedge
            hedge_deals = []; per_h = {}
            for lab in ("S1", "S2", "S3"):
                h_d = hg.simulate_hedges(sl_events_per_stream[lab], t_arr, hedge_cfgs[lab])
                h_pnl = sum(p for _, p in h_d)
                h_n = len(h_d); h_w = sum(1 for _, p in h_d if p > 0)
                per_h[lab] = (h_pnl, h_n, h_w)
                for ts, p in h_d:
                    hedge_deals.append((pd.Timestamp(ts), f"{lab}h", p))
            merged = base_deals + hedge_deals
            np_w, dd_w, pf_w, ndd_w = aggregate(merged)
            ps_w_str = " ".join(f"{s}:${ps:+,.0f}+h${hp:+,.0f}({hn})"
                                  for s, (ps, _) in per_s_base.items()
                                  for hp, hn, hw in [per_h[s]])
            print(f"  {total_risk:>5.1f}%    {'+hedge':<8} ${np_w:>+8,.0f} ${np_w*0.94:>+10,.0f} "
                  f"{np_w/DEPOSIT*100:>+6.1f}% {dd_w:>5.1f}% {ndd_w:>7.2f} {len(merged):>7}  {ps_w_str}")
            d_np = np_w - np_b; d_dd = dd_w - dd_b; d_ndd = ndd_w - ndd_b
            print(f"          delta:               ${d_np:>+8,.0f}                "
                  f"{d_dd:>+5.1f}p {d_ndd:>+7.2f}")
            print()
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
