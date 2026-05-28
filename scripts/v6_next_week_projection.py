"""v6 next-week projection (vs deployed v5).

Same methodology as update_v5_projection.py (compound-rate scaled to live
balance, ratio-anchored distribution, live haircut). Adds STOP-ext hedge
P&L to the parent stream.

Reports side-by-side with current v5 deployed values from forward_projection.json.
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
                                  SYMBOL, DEPOSIT, SPREAD, POINT, CONTRACT,
                                  PARENT_RISK_PROD)
from sim_wfo_hedge_reverse import (StopExtensionCfg, simulate_stop_extension_hedges,
                                    tag_session_regimes)

SIM_START = datetime(2026, 2, 14, tzinfo=timezone.utc)
SIM_END   = datetime(2026, 5, 23, tzinfo=timezone.utc)
HAIRCUT_NP = 0.94
PER_STREAM_RISK = 1.5   # 9pct setfile (matches deployed v5 projection)
STOPEXT_CFG = StopExtensionCfg(exp_min=240, f1_sec=1800, ext_pts=100, tp_mult=3.0, sl_mult=1.0)


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


def main():
    existing = json.loads((ROOT / "output" / "forward_projection.json").read_text())
    live_balance = existing["baseline_balance"]
    v5_weekly = existing.get("weekly_live", {})
    v5_daily  = existing.get("daily_live", {})
    v5_monthly = existing.get("monthly_live", {})
    print(f"=== v6 next-week projection ===")
    print(f"sim {SIM_START.date()} -> {SIM_END.date()}, baseline ${live_balance:,.0f}, {PER_STREAM_RISK}%/stream, 30pt")

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

    # Parents + STOP-ext hedge
    all_deals = []
    parent_total = 0; hedge_total = 0; hedge_n = 0; hedge_wins = 0
    for s in ("S1","S2","S3","S4","S5","S6"):
        cfg = make_stream_cfg(s, PER_STREAM_RISK)
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        for d in r.deals:
            if "entry" in str(d.kind).lower(): continue
            ts_ns = pd.Timestamp(d.ts).value
            all_deals.append((ts_ns, d.pnl))
            parent_total += d.pnl
        sl_ev = extract_sl_events(r.deals)
        hp = simulate_stop_extension_hedges(sl_ev, ticks_arr, STREAM_CFGS[s], STOPEXT_CFG)
        for ts, pnl in hp:
            all_deals.append((ts, pnl))
            hedge_total += pnl; hedge_n += 1
            if pnl > 0: hedge_wins += 1

    # Aggregate to weekly NPs
    week_buckets = {}
    for ts, p in all_deals:
        wk = pd.Timestamp(ts).to_period("W").start_time
        week_buckets[wk] = week_buckets.get(wk, 0) + p
    weekly_nps = [v for _, v in sorted(week_buckets.items())]
    n_weeks = len(weekly_nps)

    # Compound rate from sim totals
    portfolio_np = sum(weekly_nps)
    sim_weeks = (SIM_END - SIM_START).days / 7
    compound_rate = (1 + portfolio_np / DEPOSIT) ** (1 / sim_weeks) - 1 if portfolio_np > -DEPOSIT else 0

    # Slope + decay
    slope = per_stream_slope(weekly_nps)
    decay = slope_to_decay(slope)
    combined = HAIRCUT_NP * decay

    # Live mean = compound rate × live balance × combined haircut
    smean = compound_rate * live_balance * combined

    # Distribution: scale weekly distribution by (smean / sim_mean) to anchor on smean
    sim_mean = float(np.mean(weekly_nps))
    factor = smean / sim_mean if abs(sim_mean) > 1e-9 else 0
    smedian = float(np.median(weekly_nps)) * factor
    sp10 = float(np.percentile(weekly_nps, 10)) * factor
    sp90 = float(np.percentile(weekly_nps, 90)) * factor
    sworst = float(min(weekly_nps)) * factor
    sbest = float(max(weekly_nps)) * factor
    sstd = float(np.std(weekly_nps)) * abs(factor)
    green_prob = sum(1 for w in weekly_nps if w > 0) / n_weeks

    monthly_compound_roi = ((1 + compound_rate * combined) ** 4 - 1) * 100

    # Print v6 projection
    print(f"\n  Raw sim totals (${DEPOSIT:,.0f} base, {n_weeks} weeks):")
    print(f"    Parent NP:   ${parent_total:+,.0f}")
    print(f"    Hedge NP:    ${hedge_total:+,.0f}  ({hedge_n} fills, {hedge_wins/max(hedge_n,1)*100:.1f}% WR)")
    print(f"    Combined:    ${portfolio_np:+,.0f}")
    print(f"    Compound weekly rate (sim): {compound_rate*100:.2f}%/wk")
    print(f"    Avg OOS slope: {slope:+.2f}%  ->  decay={decay}  combined_haircut={combined:.3f}")

    # Side-by-side vs v5 deployed
    print(f"\n  === Side-by-side: v5 deployed vs v6 projected (live ${live_balance:,.0f}) ===\n")
    print(f"  {'Metric':<32} {'v5 deployed':>14} {'v6 projected':>14} {'Delta':>12}")
    rows = [
        ("weekly mean NP",       v5_weekly.get('mean_np', 0),   smean),
        ("weekly median NP",     v5_weekly.get('median_np', 0), smedian),
        ("weekly p10 NP",        v5_weekly.get('p10_np', 0),    sp10),
        ("weekly p90 NP",        v5_weekly.get('p90_np', 0),    sp90),
        ("weekly worst NP",      v5_weekly.get('worst_np', 0),  sworst),
        ("weekly best NP",       v5_weekly.get('best_np', 0),   sbest),
        ("weekly std",           v5_weekly.get('std_np', 0),    sstd),
        ("green week prob",      v5_weekly.get('green_week_prob', 0), green_prob),
        ("ROI %/week",           v5_weekly.get('expected_roi_pct', 0), smean/live_balance*100),
        ("daily mean NP",        v5_daily.get('mean_np', 0),    smean/5),
        ("monthly compound ROI%", v5_monthly.get('compound_mean_roi_pct', 0), monthly_compound_roi),
        ("target_balance_floor",  live_balance + v5_weekly.get('p10_np', 0), live_balance + sp10),
        ("target_balance_mean",   live_balance + v5_weekly.get('mean_np', 0), live_balance + smean),
        ("target_balance_stretch", live_balance + v5_weekly.get('p90_np', 0), live_balance + sp90),
    ]
    for label, v5_val, v6_val in rows:
        delta = v6_val - v5_val
        if "prob" in label:
            print(f"  {label:<32} {v5_val:>14.2f} {v6_val:>14.2f} {delta:>+12.2f}")
        elif "%" in label:
            print(f"  {label:<32} {v5_val:>13.2f}% {v6_val:>13.2f}% {delta:>+11.2f}pp")
        else:
            print(f"  {label:<32} ${v5_val:>+12,.0f} ${v6_val:>+12,.0f} ${delta:>+11,.0f}")

    print(f"\n  === Hedge contribution (v6 only) ===")
    hedge_share_pct = hedge_total / portfolio_np * 100 if portfolio_np > 0 else 0
    weekly_hedge_share = smean * hedge_total / portfolio_np if portfolio_np > 0 else 0
    print(f"    Parent NP share:  ${smean - weekly_hedge_share:+,.0f}/wk  ({100-hedge_share_pct:.1f}%)")
    print(f"    Hedge NP share:   ${weekly_hedge_share:+,.0f}/wk  ({hedge_share_pct:.1f}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
