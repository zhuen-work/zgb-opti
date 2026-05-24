"""Combined LIMIT + STOP-extension hedge test.

Hypothesis: LIMIT (current) and STOP-ext catch different parent-SL events
(retracement-then-continue vs direct-continuation). Combined firing should
be additive with minimal overlap.

Setup:
  LIMIT      = current winner (per-stream sl_mult/alpha/profit_mult)
  STOP-ext   = ext_pts=100, tp_mult=3.0, sl_mult=1.0, f1=1800

Lots: each hedge gets HALF the risk-equalized lot count (so total hedge
risk == single hedge risk, just split across two mechanisms).

Compares per-window OOS NP across all 4 may23 windows.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.wfo_helpers import WINDOWS_MAY23 as WINDOWS, to_utc

from sim_wfo_hedge_retry import (STREAM_CFGS, make_stream_cfg, slice_window,
                                  ts_arr_from_ticks, SYMBOL, DEPOSIT, SPREAD,
                                  POINT, CONTRACT, PARENT_RISK_PROD)
from sim_wfo_hedge_reverse import (ReverseHedgeCfg, simulate_reverse_hedges,
                                    StopExtensionCfg, simulate_stop_extension_hedges,
                                    tag_session_regimes)


# Per-stream LIMIT winner (from prior WFO)
LIMIT_PER_STREAM = {
    "S1": dict(sl_mult=1.0, alpha=0.5, pm=3.0),
    "S2": dict(sl_mult=1.2, alpha=0.5, pm=3.5),
    "S3": dict(sl_mult=1.0, alpha=0.5, pm=3.0),
    "S4": dict(sl_mult=1.0, alpha=0.5, pm=3.5),
    "S5": dict(sl_mult=1.0, alpha=0.5, pm=3.0),
    "S6": dict(sl_mult=1.2, alpha=0.5, pm=3.5),
}

# STOP-ext winner from geometry sweep (single config across all streams)
STOPEXT_CFG = StopExtensionCfg(exp_min=240, f1_sec=1800, ext_pts=100, tp_mult=3.0, sl_mult=1.0)


def extract_sl_events_fmt(deals):
    open_pos = []
    out = []
    for d in deals:
        ts_ns = pd.Timestamp(d.ts).value
        if "entry" in str(d.kind).lower():
            open_pos.append({"ts_ns": ts_ns, "direction": int(d.direction),
                              "entry_price": float(d.price), "lots": float(d.lots)})
            continue
        m = -1
        for i, op in enumerate(open_pos):
            if op["direction"] == int(d.direction):
                m = i; break
        if m < 0: continue
        op = open_pos.pop(m)
        if d.pnl < 0:
            out.append({"ts_ns": ts_ns, "direction": op["direction"],
                        "entry_price": op["entry_price"],
                        "entry_ts_ns": op["ts_ns"], "lots": op["lots"]})
    return out


def run_window(ticks, m1, m5, meta, regime, lots_scale_limit=1.0, lots_scale_stopext=1.0):
    """Run parents + both hedges for one window. Returns (per_stream_nps, totals)."""
    ticks_arr = ts_arr_from_ticks(ticks)
    parent_total = 0
    limit_total = 0; limit_n = 0; limit_wins = 0
    stopext_total = 0; stopext_n = 0; stopext_wins = 0
    per_s = {}
    for s in ("S1","S2","S3","S4","S5","S6"):
        cfg = make_stream_cfg(s, PARENT_RISK_PROD)
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        sl_ev = extract_sl_events_fmt(r.deals)
        # Adjust lots before passing to hedge sims by scaling sl_ev["lots"]
        sl_ev_limit = [{**ev, "lots": ev["lots"] * lots_scale_limit} for ev in sl_ev]
        sl_ev_stopext = [{**ev, "lots": ev["lots"] * lots_scale_stopext} for ev in sl_ev]

        p = LIMIT_PER_STREAM[s]
        lcfg = ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off",
                                sl_mult=p["sl_mult"], partial_fraction=p["alpha"],
                                profit_mult=p["pm"],
                                fractal_confirm=False, fractal_width=5,
                                tier_count=1, tier_spacing=0.0)
        l_hp = simulate_reverse_hedges(sl_ev_limit, ticks_arr, STREAM_CFGS[s], lcfg, regime)
        l_np = sum(p for _, p in l_hp); l_n = len(l_hp); l_w = sum(1 for _, p in l_hp if p > 0)

        s_hp = simulate_stop_extension_hedges(sl_ev_stopext, ticks_arr, STREAM_CFGS[s], STOPEXT_CFG)
        s_np = sum(p for _, p in s_hp); s_n = len(s_hp); s_w = sum(1 for _, p in s_hp if p > 0)

        parent_np = sum(d.pnl for d in r.deals if "entry" not in str(d.kind).lower())
        parent_total += parent_np
        limit_total += l_np; limit_n += l_n; limit_wins += l_w
        stopext_total += s_np; stopext_n += s_n; stopext_wins += s_w
        per_s[s] = dict(parent_np=parent_np, limit_np=l_np, limit_n=l_n,
                         stopext_np=s_np, stopext_n=s_n)
    return per_s, dict(parent=parent_total, limit=limit_total, limit_n=limit_n, limit_wins=limit_wins,
                        stopext=stopext_total, stopext_n=stopext_n, stopext_wins=stopext_wins)


def main():
    print(f"=== Combined LIMIT + STOP-ext test on may23 windows ===")
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        full_start = to_utc(WINDOWS[0][1])
        full_end = to_utc(WINDOWS[-1][4])
        ticks_full = load_ticks(SYMBOL, full_start, full_end, spread_pts=SPREAD)
        m1_full = load_bars(SYMBOL, "M1", full_start, full_end)
        m5_full = load_bars(SYMBOL, "M5", full_start, full_end)
    finally:
        kill_mt5_terminal()

    # === Run 4 OOS windows with each mode ===
    modes = [
        ("A: full LIMIT, no STOP-ext",  1.0, 0.0),
        ("B: no LIMIT, full STOP-ext",  0.0, 1.0),
        ("C: half LIMIT + half STOP-ext", 0.5, 0.5),
        ("D: full LIMIT + full STOP-ext (additive risk)", 1.0, 1.0),
    ]

    results = {}
    for label, lscale, sscale in modes:
        print(f"\n--- {label} ---")
        per_w = {}
        for w_label, _, _, oos_s, oos_e in WINDOWS:
            ticks = slice_window(ticks_full, "ts", to_utc(oos_s), to_utc(oos_e))
            m1 = slice_window(m1_full, "ts", to_utc(oos_s), to_utc(oos_e))
            m5 = slice_window(m5_full, "ts", to_utc(oos_s), to_utc(oos_e))
            regime = tag_session_regimes(ticks, m1)
            per_s, tot = run_window(ticks, m1, m5, meta, regime,
                                     lots_scale_limit=lscale, lots_scale_stopext=sscale)
            total_hedge = tot["limit"] + tot["stopext"]
            per_w[w_label] = dict(parent=tot["parent"], limit=tot["limit"], stopext=tot["stopext"],
                                   total_hedge=total_hedge, combined=tot["parent"] + total_hedge,
                                   limit_n=tot["limit_n"], stopext_n=tot["stopext_n"])
            print(f"  {w_label}_OOS  parent=${tot['parent']:+,.0f}  "
                  f"L=${tot['limit']:+,.0f}(n={tot['limit_n']})  "
                  f"SE=${tot['stopext']:+,.0f}(n={tot['stopext_n']})  "
                  f"hedge_total=${total_hedge:+,.0f}")
        agg_parent = sum(v["parent"] for v in per_w.values())
        agg_limit = sum(v["limit"] for v in per_w.values())
        agg_stopext = sum(v["stopext"] for v in per_w.values())
        agg_combined = sum(v["combined"] for v in per_w.values())
        print(f"  TOTAL OOS  parent=${agg_parent:+,.0f}  L=${agg_limit:+,.0f}  SE=${agg_stopext:+,.0f}  combined=${agg_combined:+,.0f}")
        results[label] = dict(per_w=per_w, agg_parent=agg_parent, agg_limit=agg_limit,
                               agg_stopext=agg_stopext, agg_combined=agg_combined)

    # === Side-by-side ===
    print(f"\n=== SIDE-BY-SIDE (OOS totals across 4 windows) ===")
    print(f"  {'Mode':<48}  {'parent':>10}  {'L_hedge':>10}  {'SE_hedge':>10}  {'combined':>10}")
    for label, _, _ in modes:
        r = results[label]
        print(f"  {label:<48}  ${r['agg_parent']:>+9,.0f}  ${r['agg_limit']:>+9,.0f}  ${r['agg_stopext']:>+9,.0f}  ${r['agg_combined']:>+9,.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
