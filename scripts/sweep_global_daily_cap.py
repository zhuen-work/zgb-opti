"""Sweep the v8 GLOBAL daily target/loss cap over the deployed v7 9pct params.

Sources parent + hedge params from D:\\v7\\dt818_pro_v7_9pct_may30_may23.set
(the LIVE setfile), runs parent + STOP-ext hedge across the 4 may23 WFO OOS
windows at 9% risk / spread=30, merges all deals into one equity curve, then
overlays a portfolio-level daily cap for each candidate target% (and loss%)
and reports NP / DD$ / DD% / NDD / PF vs the uncapped baseline.

MODEL NOTE (approximation): the cap here fires on REALIZED P&L at deal-close
times — once a day's cumulative closed P&L crosses the threshold, all later
deals that day are dropped. The live v8 EA fires intraday on realized+unrealized
(MTM), so live locks SOONER and slightly more often. Treat this as a screening
sweep to find the ballpark target%, not a tick-exact backtest.

Run:
  python scripts/sweep_global_daily_cap.py
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone, date
from pathlib import Path

os.environ.setdefault("ZGB_SPREAD_PTS_OVERRIDE", "30")  # live-match

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
                                      get_bool, get_float, get_int)

import argparse as _argparse
_ap = _argparse.ArgumentParser()
_ap.add_argument("--setfile", default="D:/v7/dt818_pro_v7_9pct_may30_may23.set",
                 help="Setfile to source parent+hedge params + _RiskPct from.")
_ARGS, _ = _ap.parse_known_args()
SETFILE = Path(_ARGS.setfile)
SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0

# DAILY TARGET is LOCKED OFF (decision 2026-05-28): a profit target truncates
# the trend edge and degraded NDD at every level on both 9pct and 6pct. Only
# the daily LOSS cap is swept. Loss grid is % of day-start balance; the optimal
# scales ~linearly with risk (9pct -> ~4%, 6pct -> ~2.7%).
TARGET_LOCKED = 0.0
LOSS_GRID = [round(x * 0.5, 1) for x in range(0, 21)]  # 0..10 step 0.5


def _to_utc(d):
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def collect_deals():
    """Run parent+hedge across all OOS windows; return merged [(ts_ns, pnl)] sorted."""
    d = parse_setfile(SETFILE)
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
    cfgs = [build_cfg_for_stream(d, sn, risk) for sn in range(1, 7)]

    all_deals = []
    for label, _, _, oos_s, oos_e in WINDOWS_MAY23:
        ts, te = _to_utc(oos_s), _to_utc(oos_e)
        ticks = load_ticks(SYMBOL, ts, te)
        m1 = load_bars(SYMBOL, "M1", ts, te)
        m5 = load_bars(SYMBOL, "M5", ts, te)
        ticks_arr = ts_arr_from_ticks(ticks)
        for sn, cfg in enumerate(cfgs, start=1):
            r = simulate_fast(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
            parent_pnls, sl_events = extract_sl_events(r.deals)
            for tsn, p in parent_pnls:
                all_deals.append((int(tsn), float(p)))
            sm = {"magic": sn, "risk_pct": risk, "fixed_sl_pts": cfg.fixed_sl_pts,
                  "rr_ratio": cfg.rr_ratio, "half_tp_ratio": cfg.half_tp_ratio,
                  "range_minutes": cfg.range_minutes,
                  "pending_expire_minutes": cfg.pending_expire_minutes}
            hpn = simulate_stop_extension_hedges(sl_events, ticks_arr, sm, hedge_cfg)
            for tsn, p in hpn:
                all_deals.append((int(tsn), float(p)))
        print(f"  [{label}] {oos_s}->{oos_e}  cumulative deals={len(all_deals)}", flush=True)
    kill_mt5_terminal()
    all_deals.sort(key=lambda x: x[0])
    return all_deals


def apply_cap(deals, target_pct, loss_pct):
    """Overlay a per-day realized-P&L cap. Returns (kept_deals, days_capped)."""
    bal = DEPOSIT
    kept = []
    cur_day = None
    day_start_bal = bal
    day_cum = 0.0
    locked = False
    days_capped = 0
    for ts_ns, pnl in deals:
        day = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc).date()
        if day != cur_day:
            cur_day = day
            day_start_bal = bal
            day_cum = 0.0
            locked = False
        if locked:
            continue
        kept.append((ts_ns, pnl))
        bal += pnl
        day_cum += pnl
        if target_pct > 0 and day_cum >= day_start_bal * target_pct / 100.0:
            locked = True; days_capped += 1
        elif loss_pct > 0 and day_cum <= -day_start_bal * loss_pct / 100.0:
            locked = True; days_capped += 1
    return kept, days_capped


def metrics(deals):
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gains = losses = 0.0
    for _, p in deals:
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p >= 0: gains += p
        else: losses += -p
    np_ = bal - DEPOSIT
    pf = gains / losses if losses > 0 else float("inf")
    ndd = np_ / dd_abs if dd_abs > 0 else 0.0
    dd_pct = (dd_abs / max(bal_max, DEPOSIT) * 100.0) if bal_max > 0 else 0.0
    return {"np": np_, "dd": dd_abs, "dd_pct": dd_pct, "ndd": ndd, "pf": pf}


def main():
    print("=" * 100)
    print("  GLOBAL daily-cap sweep — v7 9pct deployed params, may23 OOS (4 windows), 9% risk, spread=30")
    print(f"  Source: {SETFILE}")
    print("=" * 100)
    deals = collect_deals()
    base = metrics(deals)
    print(f"\n  BASELINE (no cap):  NP=${base['np']:>+9,.0f}  DD=${base['dd']:>7,.0f} "
          f"({base['dd_pct']:>5.2f}%)  NDD={base['ndd']:>5.2f}  PF={base['pf']:.2f}\n")

    # --- DAILY LOSS sweep (target LOCKED OFF) ---
    print(f"  DAILY LOSS sweep  (target LOCKED OFF at {TARGET_LOCKED:.1f}%):")
    print(f"  {'loss%':>8}  {'NP':>10}  {'DD$':>9}  {'DD%':>7}  {'NDD':>6}  {'PF':>5}  {'dayCap':>7}  {'ΔNDD':>7}")
    print("  " + "-" * 80)
    best_l = None
    for l in LOSS_GRID:
        kept, nc = apply_cap(deals, TARGET_LOCKED, l)
        ml = metrics(kept)
        dndd = ml["ndd"] - base["ndd"]
        tag = "  (base)" if l == 0.0 else ""
        print(f"  {l:>7.1f}%  ${ml['np']:>+9,.0f}  ${ml['dd']:>8,.0f}  {ml['dd_pct']:>6.2f}%  "
              f"{ml['ndd']:>6.2f}  {ml['pf']:>5.2f}  {nc:>7d}  {dndd:>+7.2f}{tag}")
        if l > 0.0 and (best_l is None or ml["ndd"] > best_l[1]["ndd"]):
            best_l = (l, ml, dndd, nc)

    print("\n  " + "=" * 80)
    print(f"  Baseline NDD={base['ndd']:.2f}, NP=${base['np']:+,.0f}, DD%={base['dd_pct']:.2f}%")
    if best_l:
        print(f"  Best LOSS: {best_l[0]:.1f}% -> NDD={best_l[1]['ndd']:.2f} ({best_l[2]:+.2f}), "
              f"NP=${best_l[1]['np']:+,.0f}, DD%={best_l[1]['dd_pct']:.2f}%, {best_l[3]} days capped")
        print(f"  -> set _GlobalDailyLossPct={best_l[0]:.1f}  _GlobalDailyTargetPct=0.0")
    print("  " + "=" * 80)
    print("  NOTE: realized-P&L approximation (caps fire at deal-close, not intraday MTM).")
    print("        Live v8 locks sooner on unrealized — treat as screening, not exact.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
