"""v7 weekly live projection (View C: decay-adjusted live haircut).

Runs the v7 setfile across the full historical window (Feb 14 -> May 23, ~14 weeks)
on $10k @ 9%/total risk, deal-merges all 6 streams, buckets PnL per ISO week,
applies live haircut (NP * 0.94, PF - 0.25), computes OOS decay slope across
weeks, and projects forward.

View C = haircut * (1 + slope_factor), per feedback_forward_projection_view_c.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from statistics import mean, median

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, load_ticks, load_bars, kill_mt5_terminal
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast

# Reuse the comparison script's setfile parser + cfg builder.
sys.path.insert(0, str(ROOT / "scripts"))
from compare_v6_v7_portfolio import (parse_setfile, build_cfg_for_stream,
                                      get_bool, get_float, get_int)

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
SPREAD = 30
HAIRCUT_NP = 0.94
HAIRCUT_PF = 0.25

# Match v6's published Feb 14 -> May 23 window
START = datetime(2026, 2, 14, tzinfo=timezone.utc)
END   = datetime(2026, 5, 23, tzinfo=timezone.utc)

_DEFAULT_V7 = ROOT / "configs/sets/dt818_pro_v7_9pct_may30_may23.set"

# Default setfile path can be overridden via CLI arg.
import argparse
_ap = argparse.ArgumentParser(add_help=False)
_ap.add_argument("--setfile", default=str(_DEFAULT_V7))
_ap.add_argument("--label", default="v7")
_args, _ = _ap.parse_known_args()
SETFILE_PATH = Path(_args.setfile)
LABEL = _args.label


def iso_week_start(ts: datetime) -> datetime:
    """Return Monday 00:00 UTC of the ISO week containing ts."""
    monday = ts - timedelta(days=ts.weekday())
    return monday.replace(hour=0, minute=0, second=0, microsecond=0)


def main() -> int:
    from zgb_sim.mt5_accounts import init_account
    init_account("sim")
    m = symbol_meta(SYMBOL)
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])
    print(f"loading {SYMBOL} ticks/M5/M1 {START.date()} -> {END.date()} spread={SPREAD}pt...")
    ticks = load_ticks(SYMBOL, START, END, spread_pts=SPREAD)
    m1 = load_bars(SYMBOL, "M1", START, END)
    m5 = load_bars(SYMBOL, "M5", START, END)
    print(f"  {len(ticks):,} ticks, {len(m5):,} M5 bars")

    d = parse_setfile(SETFILE_PATH)
    risk = get_float(d, "_RiskPct", 1.5)
    streams = []
    for sn in range(1, 7):
        if not get_bool(d, f"_ORB_S{sn}_Enabled"):
            continue
        streams.append((sn, build_cfg_for_stream(d, sn, risk)))
    print(f"  v7 setfile loaded: {len(streams)} streams @ {risk}% each = {risk*len(streams)}% total")

    # Run each stream
    deals = []
    for sn, cfg in streams:
        r = simulate_fast(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        for de in r.deals:
            if de.kind == "entry":
                continue
            deals.append((de.ts, sn, de.pnl, de.kind))
    deals.sort(key=lambda x: x[0])
    print(f"  collected {len(deals):,} deals across all 6 streams")

    # Deal-merge on shared balance to get portfolio NP/DD
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gains = 0.0; losses = 0.0; wins = 0; trades = 0
    weekly_pnl = {}  # iso_week_start -> sum of PnLs in that week
    for ts, _sn, pnl, _k in deals:
        bal += pnl
        if bal > bal_max:
            bal_max = bal
        cur_dd = bal_max - bal
        if cur_dd > dd_abs:
            dd_abs = cur_dd
        if pnl >= 0:
            gains += pnl; wins += 1
        else:
            losses += -pnl
        trades += 1
        # Bucket per ISO week
        wk_start = iso_week_start(ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts)
        if wk_start.tzinfo is None:
            wk_start = wk_start.replace(tzinfo=timezone.utc)
        weekly_pnl.setdefault(wk_start, 0.0)
        weekly_pnl[wk_start] += pnl

    np_total = bal - DEPOSIT
    pf = gains / losses if losses > 0 else float("inf")
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0.0
    wr = wins / trades * 100 if trades > 0 else 0.0

    # Live haircuts
    np_hc = np_total * HAIRCUT_NP
    pf_hc = pf - HAIRCUT_PF if pf != float("inf") else pf

    # Weekly distribution (drop partial-end weeks below 3 trading days if any)
    weeks_sorted = sorted(weekly_pnl.items())
    weekly_np_list = [v for _k, v in weeks_sorted]
    weekly_np_list_hc = [v * HAIRCUT_NP for v in weekly_np_list]
    n_weeks = len(weekly_np_list)

    # OOS decay slope across weeks: (last 4 weeks mean / first 4 weeks mean) - 1
    if n_weeks >= 8:
        first_4_mean = mean(weekly_np_list_hc[:4])
        last_4_mean = mean(weekly_np_list_hc[-4:])
        slope = (last_4_mean - first_4_mean) / max(abs(first_4_mean), 1.0)
    else:
        first_4_mean = last_4_mean = mean(weekly_np_list_hc)
        slope = 0.0

    # View C decay factor: clamp slope to [-0.5, +0.5] then apply as multiplier
    decay_factor = max(-0.5, min(0.5, slope))
    weekly_mean_hc = mean(weekly_np_list_hc) if weekly_np_list_hc else 0.0
    weekly_median_hc = median(weekly_np_list_hc) if weekly_np_list_hc else 0.0
    weekly_mean_view_c = weekly_mean_hc * (1.0 + decay_factor)

    # Percentiles
    def pct(arr, p):
        if not arr: return 0.0
        s = sorted(arr)
        k = (len(s) - 1) * p / 100.0
        f = int(k); c = min(f + 1, len(s) - 1)
        return s[f] + (s[c] - s[f]) * (k - f)

    p10 = pct(weekly_np_list_hc, 10)
    p25 = pct(weekly_np_list_hc, 25)
    p75 = pct(weekly_np_list_hc, 75)
    p90 = pct(weekly_np_list_hc, 90)
    worst_week = min(weekly_np_list_hc) if weekly_np_list_hc else 0.0
    best_week = max(weekly_np_list_hc) if weekly_np_list_hc else 0.0
    green_weeks = sum(1 for w in weekly_np_list_hc if w > 0)
    green_prob = green_weeks / n_weeks if n_weeks else 0.0

    # Monthly compound ROI (assume 4.33 weeks/month, multiplicative weekly growth)
    if weekly_mean_view_c > 0:
        weekly_roi = weekly_mean_view_c / DEPOSIT
        monthly_roi = ((1 + weekly_roi) ** 4.33 - 1) * 100
    else:
        monthly_roi = 0.0

    kill_mt5_terminal()

    print()
    print("=" * 80)
    print(f"  {LABEL.upper()} WEEKLY LIVE PROJECTION  --  Feb 14 -> May 23 ({n_weeks} weeks)")
    print(f"  Setfile: {SETFILE_PATH.name}  ·  9% total risk  ·  $10k deposit  ·  30pt spread")
    print("=" * 80)
    print()
    print(f"  Total raw sim:  NP=${np_total:>12,.2f}  DD=${dd_abs:,.2f} ({dd_pct:.2f}%)  "
          f"PF={pf:.3f}  WR={wr:.1f}%  Trades={trades:,}")
    print(f"  Live haircut:   NP_hc=${np_hc:>12,.2f}  PF_hc={pf_hc:.3f}  "
          f"(NP × {HAIRCUT_NP}, PF − {HAIRCUT_PF})")
    print()
    print(f"  --- Weekly distribution (haircut applied, n={n_weeks}) ---")
    print(f"  Mean      ${weekly_mean_hc:>+10,.2f}     Median  ${weekly_median_hc:>+10,.2f}")
    print(f"  p10       ${p10:>+10,.2f}     p25     ${p25:>+10,.2f}")
    print(f"  p75       ${p75:>+10,.2f}     p90     ${p90:>+10,.2f}")
    print(f"  Worst week${worst_week:>+10,.2f}     Best    ${best_week:>+10,.2f}")
    print(f"  Green-week prob: {green_prob:.2%}  ({green_weeks}/{n_weeks} weeks > 0)")
    print()
    print(f"  --- OOS decay (first 4 weeks vs last 4 weeks) ---")
    print(f"  First 4 weeks mean (hc): ${first_4_mean:>+10,.2f}")
    print(f"  Last  4 weeks mean (hc): ${last_4_mean:>+10,.2f}")
    print(f"  Slope (relative):        {slope:>+.4f}   →  decay_factor={decay_factor:+.4f}")
    print()
    print(f"  === VIEW C (decay-adjusted forward weekly projection) ===")
    print(f"  Forward weekly mean:  ${weekly_mean_view_c:>+10,.2f}   "
          f"(= haircut_mean × (1 + decay_factor))")
    print(f"  Monthly compound ROI: {monthly_roi:>+.2f}%   "
          f"(compounding on $10k baseline)")
    print()
    print(f"  --- Per-week PnL (haircut) ---")
    for wk, pnl_raw in weeks_sorted:
        pnl_hc = pnl_raw * HAIRCUT_NP
        marker = "+" if pnl_hc > 0 else ("=" if pnl_hc == 0 else "-")
        print(f"    [{marker}] {wk.date()}  ${pnl_hc:>+10,.2f}")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
