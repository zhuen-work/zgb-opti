"""Next-week projection comparison: hedge OFF (current v5) vs hedge ON.

Sims the same 14-week period (Feb 14 -> May 23) with:
  A) Parents only (v5 current deployment)
  B) Parents + reverse-hedge layer (using WFO winner params)

For each: computes weekly NP distribution, applies compound-rate scaling
to live balance with combined haircut. Side-by-side next-week NP forecast.
"""
from __future__ import annotations

import json
import sys
import statistics
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
                                  SYMBOL, DEPOSIT, SPREAD, POINT)
from sim_wfo_hedge_reverse import (ReverseHedgeCfg, simulate_reverse_hedges,
                                    tag_session_regimes)

SIM_START = datetime(2026, 2, 14, tzinfo=timezone.utc)
SIM_END   = datetime(2026, 5, 23, tzinfo=timezone.utc)
PER_STREAM_RISK = 1.5
HAIRCUT_NP = 0.94

# Hedge winner per-stream (from output/wfo_hedge_reverse_may23/winner.json)
HEDGE_WINNER = {
    "S1": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.0, partial_fraction=0.5, profit_mult=3.0),
    "S2": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.2, partial_fraction=0.5, profit_mult=3.5),
    "S3": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.0, partial_fraction=0.5, profit_mult=3.0),
    "S4": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.0, partial_fraction=0.5, profit_mult=3.5),
    "S5": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.0, partial_fraction=0.5, profit_mult=3.0),
    "S6": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.2, partial_fraction=0.5, profit_mult=3.5),
}


def extract_sl_events(deals):
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


def slope_to_decay(s):
    if s >= -10: return 0.90
    if s >= -30: return 0.80
    if s >= -50: return 0.75
    return 0.65


def per_stream_slope(weekly):
    n = len(weekly)
    if n < 4: return 0
    half = n // 2
    a, b = np.mean(weekly[:half]), np.mean(weekly[half:])
    return float((b - a) / abs(a) * 100) if abs(a) > 1e-9 else 0


def compound_weekly_mean(weekly_nps, live_balance, sim_deposit, combined_haircut):
    total_np = sum(weekly_nps)
    sim_weeks = len(weekly_nps)
    if total_np <= -sim_deposit or sim_weeks == 0: return 0
    rate = (1 + total_np / sim_deposit) ** (1 / sim_weeks) - 1
    return rate * live_balance * combined_haircut


def dist(weekly_nps, scale_to_mean):
    """Anchor on a target mean, derive p10/p90/etc using actual sim's distribution shape."""
    if not weekly_nps or abs(np.mean(weekly_nps)) < 1e-9:
        return dict(mean=0, median=0, p10=0, p90=0, worst=0, best=0, std=0, green_prob=0)
    sim_mean = float(np.mean(weekly_nps))
    factor = scale_to_mean / sim_mean
    out = dict(
        mean=scale_to_mean,
        median=float(np.median(weekly_nps)) * factor,
        p10=float(np.percentile(weekly_nps, 10)) * factor,
        p90=float(np.percentile(weekly_nps, 90)) * factor,
        worst=float(min(weekly_nps)) * factor,
        best=float(max(weekly_nps)) * factor,
        std=float(np.std(weekly_nps)) * abs(factor),
        green_prob=sum(1 for w in weekly_nps if w > 0) / len(weekly_nps),
    )
    return out


def main():
    existing = json.loads((ROOT / "output" / "forward_projection.json").read_text())
    live_balance = existing["baseline_balance"]
    print(f"=== Next-week projection: hedge OFF vs hedge ON ===")
    print(f"=== sim {SIM_START.date()} -> {SIM_END.date()} | risk={PER_STREAM_RISK}%/stream | {SPREAD}pt | baseline ${live_balance:,.0f} ===")

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

    # Per-stream sim once; extract parent NPs + SL events
    parent_deals = {}
    sl_events = {}
    for s in ("S1","S2","S3","S4","S5","S6"):
        cfg = make_stream_cfg(s, PER_STREAM_RISK)
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        parent_deals[s] = [(d.ts, d.pnl) for d in r.deals if "entry" not in str(d.kind).lower()]
        sl_events[s] = extract_sl_events(r.deals)
        print(f"  {s}: parent_deals={len(parent_deals[s])} sl_events={len(sl_events[s])}")

    # Hedge sims
    hedge_deals = {}
    for s in ("S1","S2","S3","S4","S5","S6"):
        hp = simulate_reverse_hedges(sl_events[s], ticks_arr, STREAM_CFGS[s],
                                       HEDGE_WINNER[s], regime)
        # Convert ts_ns + pnl to ts + pnl
        hedge_deals[s] = [(pd.Timestamp(ts, tz="UTC"), pnl) for ts, pnl in hp]
        print(f"  {s}: hedge_deals={len(hedge_deals[s])}")

    # Weekly NPs — A (parents only)
    weekly_a = {}
    for s in ("S1","S2","S3","S4","S5","S6"):
        for ts, p in parent_deals[s]:
            wk = pd.Timestamp(ts).to_period("W").start_time
            weekly_a[wk] = weekly_a.get(wk, 0) + p
    weekly_a_sorted = [v for _, v in sorted(weekly_a.items())]

    # Weekly NPs — B (parents + hedge)
    weekly_b = dict(weekly_a)
    for s in ("S1","S2","S3","S4","S5","S6"):
        for ts, p in hedge_deals[s]:
            wk = pd.Timestamp(ts).to_period("W").start_time
            weekly_b[wk] = weekly_b.get(wk, 0) + p
    weekly_b_sorted = [v for _, v in sorted(weekly_b.items())]

    # Slopes
    slope_a = float(np.mean([per_stream_slope([w for _, w in
        sorted({pd.Timestamp(ts).to_period('W').start_time:
                sum(p for t,p in parent_deals[s] if pd.Timestamp(t).to_period('W').start_time
                    == pd.Timestamp(ts).to_period('W').start_time)
                for ts,_ in parent_deals[s]}.items())])
        for s in ("S1","S2","S3","S4","S5","S6")]))
    # Simpler portfolio-level slope
    slope_a_port = per_stream_slope(weekly_a_sorted)
    slope_b_port = per_stream_slope(weekly_b_sorted)
    decay_a = slope_to_decay(slope_a_port)
    decay_b = slope_to_decay(slope_b_port)
    combined_a = HAIRCUT_NP * decay_a
    combined_b = HAIRCUT_NP * decay_b
    print(f"\n  Parent-only portfolio slope: {slope_a_port:+.1f}%  -> decay={decay_a}  combined={combined_a:.3f}")
    print(f"  Parent+hedge portfolio slope: {slope_b_port:+.1f}%  -> decay={decay_b}  combined={combined_b:.3f}")

    # Compute mean weekly NP via compound rate, then distribution
    mean_a = compound_weekly_mean(weekly_a_sorted, live_balance, DEPOSIT, combined_a)
    mean_b = compound_weekly_mean(weekly_b_sorted, live_balance, DEPOSIT, combined_b)
    dist_a = dist(weekly_a_sorted, mean_a)
    dist_b = dist(weekly_b_sorted, mean_b)

    print(f"\n=== NEXT-WEEK PROJECTION (live balance ${live_balance:,.0f}, compound-rate scaled, haircut applied) ===\n")
    rows = [
        ("Metric", "A: hedge OFF (v5 current)", "B: hedge ON (decay-flagged)", "Delta (B - A)"),
        ("decay_factor", f"{decay_a}", f"{decay_b}", f"{decay_b - decay_a:+.2f}"),
        ("portfolio slope", f"{slope_a_port:+.1f}%", f"{slope_b_port:+.1f}%", f"{slope_b_port-slope_a_port:+.1f}pp"),
        ("weekly mean_np", f"${dist_a['mean']:+,.0f}", f"${dist_b['mean']:+,.0f}", f"${dist_b['mean']-dist_a['mean']:+,.0f}"),
        ("weekly median",  f"${dist_a['median']:+,.0f}", f"${dist_b['median']:+,.0f}", f"${dist_b['median']-dist_a['median']:+,.0f}"),
        ("weekly p10",     f"${dist_a['p10']:+,.0f}", f"${dist_b['p10']:+,.0f}", f"${dist_b['p10']-dist_a['p10']:+,.0f}"),
        ("weekly p90",     f"${dist_a['p90']:+,.0f}", f"${dist_b['p90']:+,.0f}", f"${dist_b['p90']-dist_a['p90']:+,.0f}"),
        ("weekly worst",   f"${dist_a['worst']:+,.0f}", f"${dist_b['worst']:+,.0f}", f"${dist_b['worst']-dist_a['worst']:+,.0f}"),
        ("weekly best",    f"${dist_a['best']:+,.0f}", f"${dist_b['best']:+,.0f}", f"${dist_b['best']-dist_a['best']:+,.0f}"),
        ("weekly std",     f"${dist_a['std']:,.0f}", f"${dist_b['std']:,.0f}", f"${dist_b['std']-dist_a['std']:+,.0f}"),
        ("green_week_prob",f"{dist_a['green_prob']:.2f}", f"{dist_b['green_prob']:.2f}", f"{dist_b['green_prob']-dist_a['green_prob']:+.2f}"),
        ("ROI %/week",     f"{dist_a['mean']/live_balance*100:.2f}%", f"{dist_b['mean']/live_balance*100:.2f}%",
                            f"{(dist_b['mean']-dist_a['mean'])/live_balance*100:+.2f}pp"),
    ]
    w0, w1, w2, w3 = (max(len(r[i]) for r in rows) for i in range(4))
    for r in rows:
        print(f"  {r[0]:<{w0}}  {r[1]:>{w1}}  {r[2]:>{w2}}  {r[3]:>{w3}}")

    # Raw-sim breakdown for reference
    parent_total = sum(weekly_a_sorted)
    hedge_only_total = sum(weekly_b_sorted) - parent_total
    print(f"\n=== Raw sim totals (14-week period, $10k base, before scaling) ===")
    print(f"  Parents only        NP=${parent_total:+,.0f}")
    print(f"  +Hedge contribution NP=${hedge_only_total:+,.0f}")
    print(f"  Parents + hedge     NP=${parent_total + hedge_only_total:+,.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
