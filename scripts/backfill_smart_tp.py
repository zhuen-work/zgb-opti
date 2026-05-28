"""Multi-day backfill: smart-TP what-if across the past N trading days.

For each day, pulls live parent SL events + actual hedge NP, sims smart-TP
on top of that day's ticks, and reports per-day delta. Aggregates the week.

Usage:
  python scripts/backfill_smart_tp.py             # default last 7 days
  python scripts/backfill_smart_tp.py --days 14
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from sim_orb_oos_today import fetch_window
from sim_wfo_hedge_reverse import simulate_reverse_hedges, ReverseHedgeCfg
from sim_wfo_hedge_retry import ts_arr_from_ticks
from zgb_sim.mt5_accounts import init_account
from zgb_sim.tick_loader import kill_mt5_terminal

SETFILE = ROOT / "configs" / "sets" / "dt818_pro_v3_9pct_may16_may9.set"

# Smart-TP R2 winner config (from WFO post-R7 smart-TP redesign sweep)
# Replaces setfile's default α=0.5/pm=1.2 with per-stream optimized α/pm.
# WFO R2: NP/DD$ 15.90 (+9.1% vs R7) with NP $100,646 / DD 5.72% / PF 1.68.
R2_CONFIG = {
    "S1": {"sl_mult": 1.0, "partial_fraction": 0.5, "profit_mult": 2.5},
    "S2": {"sl_mult": 1.0, "partial_fraction": 0.5, "profit_mult": 3.0},
    "S3": {"sl_mult": 1.2, "partial_fraction": 0.5, "profit_mult": 3.5},
    "S4": {"sl_mult": 1.0, "partial_fraction": 0.5, "profit_mult": 3.0},
    "S5": {"sl_mult": 1.0, "partial_fraction": 0.5, "profit_mult": 3.0},
    "S6": {"sl_mult": 1.0, "partial_fraction": 0.5, "profit_mult": 3.0},
}

PARENT_MAGICS = {1111: "S1", 2222: "S2", 3333: "S3",
                  4444: "S4", 5555: "S5", 6666: "S6"}
HEDGE_MAGICS  = {8111: "S1", 8222: "S2", 8333: "S3",
                  8444: "S4", 8555: "S5", 8666: "S6"}
# Old v2.1_h hedge magics (pre-2026-05-18) — for days where retry-hedge was live
OLD_HEDGE_MAGICS = {7111: "S1", 7222: "S2", 7333: "S3",
                     7444: "S4", 7555: "S5", 7666: "S6"}
SPREAD_LIVE = 30


def parse_setfile(path: Path) -> dict:
    text = path.read_text()
    out = {}
    for i in range(1, 7):
        s = f"S{i}"
        def g(key):
            m = re.search(rf"_ORB_{s}_{key}=([^|]+)\|\|", text)
            return m.group(1).strip()
        def gh(key):
            m = re.search(rf"_HEDGE_{s}_{key}=([^|]+)\|\|", text)
            return m.group(1).strip()
        out[s] = {
            "fixed_sl_pts": int(g("FixedSL_Pts")),
            "rr_ratio": float(g("RR_Ratio")),
            "sl_mult": float(gh("SLMult")),
            "partial_fraction": float(gh("PartialFraction")),
            "profit_mult": float(gh("ProfitMult")),
        }
    return out


def fetch_live_day(date: datetime, mt5) -> tuple[dict, dict, dict, dict]:
    """Returns:
      sl_events_per_stream      — for smart-TP sim input
      parent_np_per_stream      — live actual parent P&L
      hedge_np_per_stream       — live actual hedge P&L (v3 reverse: 8xxx)
      old_retry_np_per_stream   — live actual old retry-hedge P&L (v2.1_h: 7xxx)
    """
    since = datetime(date.year, date.month, date.day, 0, 0, tzinfo=timezone.utc)
    end = since + timedelta(days=1, hours=6)
    deals = mt5.history_deals_get(since, end) or ()

    entries = {}
    parent_np = {s: 0.0 for s in PARENT_MAGICS.values()}
    hedge_np = {s: 0.0 for s in HEDGE_MAGICS.values()}
    old_retry_np = {s: 0.0 for s in OLD_HEDGE_MAGICS.values()}
    for d in deals:
        m = int(d.magic)
        if m in PARENT_MAGICS or m in HEDGE_MAGICS or m in OLD_HEDGE_MAGICS:
            if d.entry == 0:
                entries[d.position_id] = d
            else:
                if m in PARENT_MAGICS:
                    parent_np[PARENT_MAGICS[m]] += d.profit
                elif m in HEDGE_MAGICS:
                    hedge_np[HEDGE_MAGICS[m]] += d.profit
                else:
                    old_retry_np[OLD_HEDGE_MAGICS[m]] += d.profit

    sl_events = {s: [] for s in PARENT_MAGICS.values()}
    for d in deals:
        m = int(d.magic)
        if m not in PARENT_MAGICS or d.entry != 1:
            continue
        if not str(d.comment or "").startswith("[sl"):
            continue
        ent = entries.get(d.position_id)
        if ent is None:
            continue
        direction = +1 if int(ent.type) == 0 else -1
        entry_ts_ns = pd.Timestamp(
            datetime.fromtimestamp(ent.time_msc / 1000, tz=timezone.utc)).value
        sl_ts_ns = pd.Timestamp(
            datetime.fromtimestamp(d.time_msc / 1000, tz=timezone.utc)).value
        sl_events[PARENT_MAGICS[m]].append({
            "ts_ns": sl_ts_ns,
            "entry_ts_ns": entry_ts_ns,
            "direction": direction,
            "entry_price": float(ent.price),
            "sl_price": float(d.price),
            "lots": float(ent.volume),
        })
    return sl_events, parent_np, hedge_np, old_retry_np


def smart_tp_for_day(date: datetime, streams: dict) -> dict:
    """Run smart-TP backfill for one day. Manages own MT5 lifecycle."""
    # Step 1: init MT5 for live, fetch SL events + live actual NPs
    import MetaTrader5 as mt5
    init_account("live")
    try:
        sl_events, parent_np, hedge_np, old_retry_np = fetch_live_day(date, mt5)
    finally:
        mt5.shutdown()
        kill_mt5_terminal()
    n_sl = sum(len(ev) for ev in sl_events.values())
    parent_total = sum(parent_np.values())
    hedge_total = sum(hedge_np.values())
    old_retry_total = sum(old_retry_np.values())
    live_actual_total = parent_total + hedge_total + old_retry_total

    if n_sl == 0:
        return {"date": date.date(), "n_sl": 0, "parent": parent_total,
                "old_hedge": hedge_total, "old_retry": old_retry_total,
                "smart_hedge": 0.0, "smart_trades": 0,
                "live_actual": live_actual_total, "what_if": live_actual_total,
                "delta": 0.0, "per_stream": None}

    # Step 2: fetch that day's ticks (fetch_window manages its own MT5)
    start = datetime(date.year, date.month, date.day, tzinfo=timezone.utc)
    end_window = start + timedelta(days=1)
    sym, ticks, _, _ = fetch_window(None, start, end_window, SPREAD_LIVE, account="live")
    ticks_arr = ts_arr_from_ticks(ticks)
    regime_by_session = {}

    smart_total = 0.0
    smart_trades = 0
    per_stream = {}
    for s, sc in streams.items():
        evs = sl_events[s]
        if not evs:
            per_stream[s] = (0.0, 0)
            continue
        hcfg = ReverseHedgeCfg(
            exp_min=240, f1_sec=1800, regime_gate="off",
            sl_mult=sc["sl_mult"],
            partial_fraction=sc["partial_fraction"],
            profit_mult=sc["profit_mult"],
        )
        stream_cfg = {"fixed_sl_pts": sc["fixed_sl_pts"]}
        h_deals = simulate_reverse_hedges(evs, ticks_arr, stream_cfg, hcfg, regime_by_session)
        s_np = sum(p for _, p in h_deals)
        s_n = len(h_deals)
        per_stream[s] = (s_np, s_n)
        smart_total += s_np
        smart_trades += s_n

    # what-if: replace OLD hedge contribution with smart-TP hedge.
    # parent stays the same; live old hedge (or old retry) is removed; smart-TP added.
    what_if = parent_total + smart_total
    delta = what_if - live_actual_total
    return {
        "date": date.date(), "n_sl": n_sl,
        "parent": parent_total,
        "old_hedge": hedge_total,
        "old_retry": old_retry_total,
        "smart_hedge": smart_total, "smart_trades": smart_trades,
        "live_actual": live_actual_total, "what_if": what_if, "delta": delta,
        "per_stream": per_stream,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7,
                     help="Number of calendar days to look back (skipping weekends)")
    ap.add_argument("--r2", action="store_true",
                     help="Use WFO R2 winner per-stream alpha/pm (overrides setfile)")
    args = ap.parse_args()

    streams = parse_setfile(SETFILE)
    if args.r2:
        for s, override in R2_CONFIG.items():
            streams[s].update(override)
        print(f"  [USING R2 WINNER CONFIG] per-stream alpha/pm override applied")
        for s, sc in streams.items():
            print(f"    {s}: sl_mult={sc['sl_mult']:.2f} alpha={sc['partial_fraction']:.2f} pm={sc['profit_mult']:.2f}")
    today = datetime.now(timezone.utc)

    # Build trading-day list (skip Sat/Sun)
    dates = []
    cur = today - timedelta(days=args.days - 1)
    while cur.date() <= today.date():
        if cur.weekday() < 5:  # Mon-Fri
            dates.append(cur)
        cur += timedelta(days=1)

    print("=" * 110)
    print(f"  SMART-TP BACKFILL  ({len(dates)} trading days: {dates[0].date()} -> {dates[-1].date()})")
    print(f"  Setfile: {SETFILE.name}  (alpha=0.5, pm=1.2 across all streams; sl_mult per setfile)")
    print("=" * 110)

    rows = []
    for d in dates:
        print(f"\n  ===== {d.date()} ({d.strftime('%a')}) =====")
        try:
            row = smart_tp_for_day(d, streams)
        except Exception as e:
            print(f"  SKIP: {type(e).__name__}: {e}")
            continue
        rows.append(row)
        if row["n_sl"] == 0:
            print(f"    No parent SLs this day.  Live actual: ${row['live_actual']:>+10,.0f}  (no smart-TP impact)")
            continue
        print(f"    Parent SLs: {row['n_sl']}    "
              f"LiveParent ${row['parent']:>+9,.0f}    "
              f"LiveOldHedge ${row['old_hedge']:>+9,.0f}    "
              f"LiveOldRetry ${row['old_retry']:>+8,.0f}")
        print(f"    SmartTPHedge ${row['smart_hedge']:>+9,.0f} ({row['smart_trades']} legs)")
        print(f"    Live actual total: ${row['live_actual']:>+10,.0f}    "
              f"What-if smart-TP:  ${row['what_if']:>+10,.0f}    "
              f"Delta: ${row['delta']:>+10,.0f}")

    # Aggregate table
    print("\n" + "=" * 110)
    print(f"  SUMMARY ({len(rows)} days)")
    print("=" * 110)
    print(f"  {'Date':<11} {'DOW':<4} {'SLs':>4} {'LiveParent':>11} {'OldHedge':>10} "
          f"{'OldRetry':>9} {'SmartTP':>10} {'LiveTotal':>11} {'WhatIf':>11} {'Delta':>11}")
    tot_parent = tot_oldh = tot_oldr = tot_smart = tot_live = tot_what = tot_delta = 0
    tot_sls = 0
    for r in rows:
        print(f"  {str(r['date']):<11} {r['date'].strftime('%a'):<4} {r['n_sl']:>4} "
              f"${r['parent']:>+9,.0f} ${r['old_hedge']:>+8,.0f} "
              f"${r['old_retry']:>+7,.0f} ${r['smart_hedge']:>+8,.0f} "
              f"${r['live_actual']:>+9,.0f} ${r['what_if']:>+9,.0f} ${r['delta']:>+9,.0f}")
        tot_parent += r["parent"]; tot_oldh += r["old_hedge"]; tot_oldr += r["old_retry"]
        tot_smart += r["smart_hedge"]; tot_live += r["live_actual"]
        tot_what += r["what_if"]; tot_delta += r["delta"]; tot_sls += r["n_sl"]
    print(f"  {'TOTAL':<11} {'':<4} {tot_sls:>4} "
          f"${tot_parent:>+9,.0f} ${tot_oldh:>+8,.0f} ${tot_oldr:>+7,.0f} "
          f"${tot_smart:>+8,.0f} ${tot_live:>+9,.0f} ${tot_what:>+9,.0f} ${tot_delta:>+9,.0f}")

    print()
    print(f"  Week-to-date live actual:    ${tot_live:>+12,.0f}")
    print(f"  Week-to-date with smart-TP:  ${tot_what:>+12,.0f}")
    print(f"  Week-to-date net delta:      ${tot_delta:>+12,.0f}")
    print()
    print(f"  Days favorable to smart-TP:  {sum(1 for r in rows if r['delta'] > 0)} / {len(rows)}")
    print(f"  Days unfavorable to smart-TP: {sum(1 for r in rows if r['delta'] < 0)} / {len(rows)}")
    print(f"  Days neutral (no SLs):       {sum(1 for r in rows if r['n_sl'] == 0)} / {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
