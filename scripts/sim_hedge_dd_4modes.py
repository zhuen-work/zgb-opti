"""DD + NP/DD$ comparison for the 4 hedge modes (full Feb 14 -> May 23 period).

Mode A: full LIMIT only         (current)
Mode B: full STOP-ext only      (new)
Mode C: half LIMIT + half STOP-ext
Mode D: full LIMIT + full STOP-ext (additive)

Each mode runs the same 6-stream parent portfolio + designated hedge layer(s).
Deals are merged chronologically; equity curve walked; DD computed properly.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from sim_wfo_hedge_retry import (STREAM_CFGS, make_stream_cfg, ts_arr_from_ticks,
                                  SYMBOL, DEPOSIT, SPREAD, POINT, CONTRACT,
                                  PARENT_RISK_PROD)
from sim_wfo_hedge_reverse import (ReverseHedgeCfg, simulate_reverse_hedges,
                                    StopExtensionCfg, simulate_stop_extension_hedges,
                                    tag_session_regimes)


SIM_START = datetime(2026, 2, 14, tzinfo=timezone.utc)
SIM_END   = datetime(2026, 5, 23, tzinfo=timezone.utc)
HAIRCUT_NP = 0.94

LIMIT_PER_STREAM = {
    "S1": dict(sl_mult=1.0, alpha=0.5, pm=3.0),
    "S2": dict(sl_mult=1.2, alpha=0.5, pm=3.5),
    "S3": dict(sl_mult=1.0, alpha=0.5, pm=3.0),
    "S4": dict(sl_mult=1.0, alpha=0.5, pm=3.5),
    "S5": dict(sl_mult=1.0, alpha=0.5, pm=3.0),
    "S6": dict(sl_mult=1.2, alpha=0.5, pm=3.5),
}
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


def walk_equity(deals):
    """Sort deals by ts, walk equity, return (np, dd_abs, dd_pct, pf, n, wr)."""
    deals = sorted(deals, key=lambda x: x[0])
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gains = losses = 0.0; wins = 0
    for ts, p in deals:
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p >= 0: gains += p; wins += 1
        else: losses += -p
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    pf = gains / losses if losses > 0 else float("inf")
    n = len(deals); wr = wins / n * 100 if n else 0
    ndd = np_ / dd_abs if dd_abs > 0 else 0
    return dict(np=np_, dd_abs=dd_abs, dd_pct=dd_pct, ndd=ndd, pf=pf, n=n, wr=wr,
                np_hc=np_ * HAIRCUT_NP, ndd_hc=(np_ * HAIRCUT_NP / dd_abs if dd_abs > 0 else 0))


def main():
    print(f"=== DD + NP/DD$ for 4 hedge modes | {SIM_START.date()} -> {SIM_END.date()} ===")
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        ticks = load_ticks(SYMBOL, SIM_START, SIM_END, spread_pts=SPREAD)
        m1 = load_bars(SYMBOL, "M1", SIM_START, SIM_END)
        m5 = load_bars(SYMBOL, "M5", SIM_START, SIM_END)
    finally:
        kill_mt5_terminal()
    regime = tag_session_regimes(ticks, m1)
    ticks_arr = ts_arr_from_ticks(ticks)

    # Cache per-stream parent deals + SL events ONCE
    parent_deals_by_stream = {}
    sl_events_by_stream = {}
    for s in ("S1","S2","S3","S4","S5","S6"):
        cfg = make_stream_cfg(s, PARENT_RISK_PROD)
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        parent_deals_by_stream[s] = [(pd.Timestamp(d.ts).value, d.pnl)
                                       for d in r.deals if "entry" not in str(d.kind).lower()]
        sl_events_by_stream[s] = extract_sl_events_fmt(r.deals)

    # Compute hedge deals once per mechanism (lots from real parent lots)
    limit_deals_by_stream = {}
    stopext_deals_by_stream = {}
    for s in ("S1","S2","S3","S4","S5","S6"):
        p = LIMIT_PER_STREAM[s]
        lcfg = ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off",
                                sl_mult=p["sl_mult"], partial_fraction=p["alpha"],
                                profit_mult=p["pm"],
                                fractal_confirm=False, fractal_width=5,
                                tier_count=1, tier_spacing=0.0)
        limit_deals_by_stream[s] = simulate_reverse_hedges(
            sl_events_by_stream[s], ticks_arr, STREAM_CFGS[s], lcfg, regime)
        stopext_deals_by_stream[s] = simulate_stop_extension_hedges(
            sl_events_by_stream[s], ticks_arr, STREAM_CFGS[s], STOPEXT_CFG)

    # Parent portfolio (same in all modes)
    parent_all = []
    for s in ("S1","S2","S3","S4","S5","S6"):
        parent_all.extend(parent_deals_by_stream[s])

    # Mode A: parents + full LIMIT
    a_deals = list(parent_all)
    for s in ("S1","S2","S3","S4","S5","S6"):
        a_deals.extend(limit_deals_by_stream[s])

    # Mode B: parents + full STOP-ext
    b_deals = list(parent_all)
    for s in ("S1","S2","S3","S4","S5","S6"):
        b_deals.extend(stopext_deals_by_stream[s])

    # Mode C: parents + 0.5 LIMIT + 0.5 STOP-ext (scale PnL by 0.5 each)
    c_deals = list(parent_all)
    for s in ("S1","S2","S3","S4","S5","S6"):
        c_deals.extend((ts, p * 0.5) for ts, p in limit_deals_by_stream[s])
        c_deals.extend((ts, p * 0.5) for ts, p in stopext_deals_by_stream[s])

    # Mode D: parents + full LIMIT + full STOP-ext
    d_deals = list(parent_all)
    for s in ("S1","S2","S3","S4","S5","S6"):
        d_deals.extend(limit_deals_by_stream[s])
        d_deals.extend(stopext_deals_by_stream[s])

    # Parents-only baseline (Mode 0)
    p0_deals = parent_all
    p0 = walk_equity(p0_deals)

    a = walk_equity(a_deals)
    b = walk_equity(b_deals)
    c = walk_equity(c_deals)
    d = walk_equity(d_deals)

    print(f"\n=== Full-period portfolio metrics (parents+hedges, $10k base, 1.5%/stream, 30pt) ===\n")
    print(f"  {'Mode':<42} {'NP':>10} {'DD$':>10} {'DD%':>6} {'NP/DD$':>7} {'PF':>5} {'WR%':>5} {'Trades':>7} {'NP_hc':>10} {'NP/DD$_hc':>9}")
    rows = [
        ("0: Parents only (current v5 deploy)",    p0),
        ("A: + full LIMIT (old v4/v5 default)",     a),
        ("B: + full STOP-ext (NEW)",                b),
        ("C: + half LIMIT + half STOP-ext",         c),
        ("D: + full LIMIT + full STOP-ext",         d),
    ]
    for label, x in rows:
        print(f"  {label:<42} ${x['np']:>+9,.0f} ${x['dd_abs']:>9,.0f} {x['dd_pct']:>5.2f}% "
              f"{x['ndd']:>7.2f} {x['pf']:>5.2f} {x['wr']:>5.1f} {x['n']:>7} "
              f"${x['np_hc']:>+9,.0f} {x['ndd_hc']:>9.2f}")

    print(f"\n=== Delta vs parents-only (Mode 0) ===")
    print(f"  {'Mode':<42} {'NP delta':>10} {'DD$ delta':>10} {'DD% delta':>10} {'NP/DD$_hc delta':>16}")
    for label, x in rows[1:]:
        dnp = x['np'] - p0['np']
        ddd = x['dd_abs'] - p0['dd_abs']
        ddp = x['dd_pct'] - p0['dd_pct']
        dndd = x['ndd_hc'] - p0['ndd_hc']
        print(f"  {label:<42} ${dnp:>+9,.0f} ${ddd:>+9,.0f} {ddp:>+9.2f}pp {dndd:>+15.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
