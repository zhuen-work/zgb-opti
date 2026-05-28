"""Faithful realized-only GLOBAL daily-loss-cap sim (matches v8 EA behavior).

Improves on sweep_global_daily_cap.py's "drop later deals" approximation by
modeling the v8 EA's actual on-lock action:
  - TRIGGER: realized (booked, closed-deal) cumulative daily P&L crosses
    -lossPct * day_start_balance  (REALIZED-ONLY, like the EA).
  - ON LOCK at time T:
      * parent positions still OPEN (entry<=T<exit) are CLOSED AT MARKET at T
        (marked to the tick mid-price at T) — NOT dropped, NOT run to exit.
      * parent positions not yet ENTERED (entry>T) are DROPPED (pendings cancelled).
      * hedges with exit_ts>T that day are dropped (short-lived; exit~entry).
  - This captures the key question the drop-approximation can't: does locking
    early FORFEIT in-flight winners, or preserve them? (Decides if 0.5% is real.)

Parents are fully entry/exit paired; hedges expose only exit times so they're
treated as point events at exit (short STOP-ext trades, exit ~= entry).

Run:
  python scripts/sim_daily_cap_realized_faithful.py --setfile D:/v7/dt818_pro_v7_9pct_may30_may23.set
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

os.environ.setdefault("ZGB_SPREAD_PTS_OVERRIDE", "30")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast
from zgb_sim.tick_loader import symbol_meta, load_ticks, load_bars, kill_mt5_terminal
from zgb_sim.wfo_helpers import WINDOWS_MAY23
from sim_wfo_hedge_reverse import StopExtensionCfg, simulate_stop_extension_hedges
from sim_orb_oos_today_hedge_v6 import extract_sl_events, ts_arr_from_ticks
from compare_v6_v7_portfolio import (parse_setfile, build_cfg_for_stream,
                                      get_float, get_int)
import pandas as pd

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
TARGET_LOCKED = 0.0
# Circuit-breaker range: only fire on genuine disaster days (loose caps).
LOSS_GRID = [0.0] + [round(3.0 + x * 0.5, 1) for x in range(0, 13)]  # 0(off) + 3.0..9.0 step 0.5


def _to_utc(d):
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def pair_parent_positions(deals, ppv):
    """FIFO-pair entry/exit deals -> position records with entry/exit ts+price."""
    open_pos = []
    out = []
    for d in deals:
        ts_ns = pd.Timestamp(d.ts).value
        if d.kind == "entry":
            open_pos.append({"ts": ts_ns, "dir": int(d.direction),
                             "lots": float(d.lots), "price": float(d.price)})
            continue
        # exit: match FIFO by direction
        idx = next((i for i, op in enumerate(open_pos) if op["dir"] == int(d.direction)), -1)
        if idx < 0:
            continue
        op = open_pos.pop(idx)
        dirsign = 1.0 if op["dir"] in (0, 1) and op["dir"] != 1 else (1.0 if op["dir"] == 0 else -1.0)
        # robust dir mapping: 0=buy(long,+1), 1=sell(short,-1)
        dirsign = 1.0 if int(op["dir"]) == 0 else -1.0
        out.append({
            "entry_ts": op["ts"], "exit_ts": ts_ns,
            "entry_price": op["price"], "dirsign": dirsign,
            "lots": op["lots"], "pnl": float(d.pnl),
        })
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--setfile", default="D:/v7/dt818_pro_v7_9pct_may30_may23.set")
    args = ap.parse_args()
    setfile = Path(args.setfile)

    d = parse_setfile(setfile)
    risk = get_float(d, "_RiskPct", 1.5)
    hedge_cfg = StopExtensionCfg(
        exp_min=get_int(d, "_HEDGE_S1_ExpireMinutes", 240),
        f1_sec=get_int(d, "_HEDGE_S1_MaxSecondsAfterEntry", 1800),
        ext_pts=get_int(d, "_HEDGE_S1_ExtPts", 100),
        tp_mult=get_float(d, "_HEDGE_S1_TPMult", 3.0),
        sl_mult=get_float(d, "_HEDGE_S1_SLMult", 1.0),
    )
    m = symbol_meta(SYMBOL)
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])
    ppv = meta.tick_value / meta.tick_size  # $ per price-unit per lot
    cfgs = [build_cfg_for_stream(d, sn, risk) for sn in range(1, 7)]

    parents = []     # position records
    hedges = []      # (exit_ts_ns, pnl)
    price_ts = []    # for global mid-price lookup
    price_mid = []
    print(f"[BOOT] faithful realized cap sim — {setfile.name} risk={risk}%/stream, spread=30")
    for label, _, _, oos_s, oos_e in WINDOWS_MAY23:
        ts, te = _to_utc(oos_s), _to_utc(oos_e)
        ticks = load_ticks(SYMBOL, ts, te)
        m1 = load_bars(SYMBOL, "M1", ts, te)
        m5 = load_bars(SYMBOL, "M5", ts, te)
        ticks_arr = ts_arr_from_ticks(ticks)
        price_ts.append(ticks_arr["ts_ns"])
        price_mid.append((ticks_arr["bid"] + ticks_arr["ask"]) / 2.0)
        for sn, cfg in enumerate(cfgs, start=1):
            r = simulate_fast(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
            parents.extend(pair_parent_positions(r.deals, ppv))
            _, sl_events = extract_sl_events(r.deals)
            sm = {"magic": sn, "risk_pct": risk, "fixed_sl_pts": cfg.fixed_sl_pts,
                  "rr_ratio": cfg.rr_ratio, "half_tp_ratio": cfg.half_tp_ratio,
                  "range_minutes": cfg.range_minutes,
                  "pending_expire_minutes": cfg.pending_expire_minutes}
            for tsn, p in simulate_stop_extension_hedges(sl_events, ticks_arr, sm, hedge_cfg):
                hedges.append((int(tsn), float(p)))
        print(f"  [{label}] parents={len(parents)} hedges={len(hedges)}", flush=True)
    kill_mt5_terminal()

    all_ts = np.concatenate(price_ts)
    all_mid = np.concatenate(price_mid)
    order = np.argsort(all_ts)
    all_ts = all_ts[order]; all_mid = all_mid[order]

    def price_at(ts_ns):
        i = int(np.searchsorted(all_ts, ts_ns))
        i = min(max(i, 0), len(all_ts) - 1)
        return float(all_mid[i])

    def day_of(ts_ns):
        return datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc).date()

    # group events by day
    from collections import defaultdict
    days = sorted(set([day_of(p["entry_ts"]) for p in parents] +
                      [day_of(h[0]) for h in hedges]))
    parents_by_day = defaultdict(list)
    for p in parents:
        parents_by_day[day_of(p["entry_ts"])].append(p)
    hedges_by_day = defaultdict(list)
    for h in hedges:
        hedges_by_day[day_of(h[0])].append(h)

    def run_cap(loss_pct):
        bal = DEPOSIT
        stream = []  # (ts, pnl) capped
        days_capped = 0
        for day in days:
            day_start_bal = bal
            # build chronological exit events for the day: parents (exit_ts, pnl, rec) + hedges
            evs = [(p["exit_ts"], p["pnl"], "P", p) for p in parents_by_day[day]]
            evs += [(h[0], h[1], "H", None) for h in hedges_by_day[day]]
            evs.sort(key=lambda x: x[0])
            booked = 0.0
            locked = False
            t_lock = None
            day_deals = []
            for ts_ns, pnl, kind, rec in evs:
                if locked:
                    continue
                day_deals.append((ts_ns, pnl))
                booked += pnl
                if loss_pct > 0 and booked <= -day_start_bal * loss_pct / 100.0:
                    locked = True
                    t_lock = ts_ns
                    days_capped += 1
            if locked:
                # close parents open at t_lock (entry<=t_lock<exit) at market
                for p in parents_by_day[day]:
                    if p["entry_ts"] <= t_lock < p["exit_ts"]:
                        mtm = (price_at(t_lock) - p["entry_price"]) * p["dirsign"] * p["lots"] * ppv
                        day_deals.append((t_lock, mtm))
                # parents entered after t_lock: dropped (pendings cancelled) -> nothing
                # hedges after t_lock: dropped (already excluded by locked skip)
            for ts_ns, pnl in day_deals:
                bal += pnl
                stream.append((ts_ns, pnl))
        return stream, days_capped

    def metrics(stream):
        b = DEPOSIT; peak = DEPOSIT; dd = 0.0; g = l = 0.0
        for _, p in sorted(stream, key=lambda x: x[0]):
            b += p
            if b > peak: peak = b
            if (peak - b) > dd: dd = peak - b
            if p >= 0: g += p
            else: l += -p
        np_ = b - DEPOSIT
        return {"np": np_, "dd": dd, "dd_pct": (dd / peak * 100 if peak > 0 else 0),
                "ndd": (np_ / dd if dd > 0 else 0), "pf": (g / l if l > 0 else float("inf"))}

    base, _ = run_cap(0.0)
    bm = metrics(base)
    print(f"\n  FAITHFUL realized-only cap (mark-open-at-lock) — {setfile.name}")
    print(f"  BASELINE: NP=${bm['np']:>+9,.0f}  DD=${bm['dd']:>7,.0f} ({bm['dd_pct']:.2f}%)  "
          f"NDD={bm['ndd']:.2f}  PF={bm['pf']:.2f}\n")
    print(f"  {'loss%':>7}  {'NP':>10}  {'DD$':>9}  {'DD%':>7}  {'NDD':>6}  {'PF':>5}  {'dayCap':>7}  {'ΔNDD':>7}")
    print("  " + "-" * 78)
    best = None
    for lp in LOSS_GRID:
        st, nc = run_cap(lp)
        mt = metrics(st)
        dndd = mt["ndd"] - bm["ndd"]
        tag = "  (base)" if lp == 0.0 else ""
        print(f"  {lp:>6.1f}%  ${mt['np']:>+9,.0f}  ${mt['dd']:>8,.0f}  {mt['dd_pct']:>6.2f}%  "
              f"{mt['ndd']:>6.2f}  {mt['pf']:>5.2f}  {nc:>7d}  {dndd:>+7.2f}{tag}")
        if lp > 0 and (best is None or mt["ndd"] > best[1]["ndd"]):
            best = (lp, mt, dndd, nc)
    print("  " + "=" * 78)
    if best:
        print(f"  Best LOSS (faithful): {best[0]:.1f}% -> NDD={best[1]['ndd']:.2f} ({best[2]:+.2f}), "
              f"NP=${best[1]['np']:+,.0f}, DD%={best[1]['dd_pct']:.2f}%, {best[3]} days capped")
    print("  Compares vs sweep_global_daily_cap.py (drop-approx) — divergence at tight caps")
    print("  reveals whether locking forfeits in-flight winners (the 0.5% question).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
