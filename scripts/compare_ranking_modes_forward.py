"""Compare 6-stream portfolio results across ranking modes on live forward ticks.

Modes compared:
  A. PARENT-ONLY HYBRID  (currently deployed)  — sp30 top-3 + sp60 top-3 (parent-only)
  B. HEDGED HYBRID                              — sp30 top-3 + sp60 top-3 (hedge-rescored, has duplicates)
  C. TOP-6 HEDGED sp30                          — hedge-rescored sp30 single source
  D. TOP-6 HEDGED sp60                          — hedge-rescored sp60 single source
  E. TOP-6 PARENT sp30                          — parent-only sp30 single source (legacy)

Forward window: 2026-05-23 (may23 WFO completion) -> now (live).
This is the ONLY truly clean forward data — no WFO peeked at it.

Risk: 9% total (1.5%/stream × 6). Live spread: 30pt.
"""
from __future__ import annotations
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast
from zgb_sim.tick_loader import kill_mt5_terminal
from sim_orb_oos_today import fetch_window, fetch_meta
from sim_wfo_hedge_reverse import StopExtensionCfg, simulate_stop_extension_hedges
from sim_orb_oos_today_hedge_v6 import extract_sl_events, ts_arr_from_ticks

DEPOSIT = 10_000.0
LIVE_SPREAD = 30
RISK_PCT = 1.5

V7 = dict(fractal_confirm=True, fractal_width=5,
          sma_cross_exit=True, sma_cross_fast=8, sma_cross_slow=21,
          sma_cross_atr_gate=0.0)
HCFG = StopExtensionCfg(exp_min=240, f1_sec=1800, ext_pts=100, tp_mult=3.0, sl_mult=1.0)


def cfg_from_row(row) -> ORBConfig:
    return ORBConfig(
        risk_pct=RISK_PCT,
        range_minutes=int(row["range_minutes"]), buffer_pts=0,
        min_range_pts=0, max_range_pts=999_999,
        fixed_sl_pts=int(row["fixed_sl_pts"]),
        rr_ratio=float(row["rr_ratio"]),
        half_tp_ratio=round(float(row["half_tp_ratio"]), 2),
        pending_expire_minutes=int(row["pending_expire_minutes"]),
        daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13,
        **V7, comment="ORB",
    )


def run_portfolio(cfgs, ticks, m1, m5, meta, ticks_arr):
    deals = []
    for sn, cfg in enumerate(cfgs, start=1):
        r = simulate_fast(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        parent_pnls, sl_events = extract_sl_events(r.deals)
        for ts, p in parent_pnls:
            deals.append((ts, sn, p))
        sm = {"magic": sn, "risk_pct": RISK_PCT,
              "fixed_sl_pts": cfg.fixed_sl_pts, "rr_ratio": cfg.rr_ratio,
              "half_tp_ratio": cfg.half_tp_ratio,
              "range_minutes": cfg.range_minutes,
              "pending_expire_minutes": cfg.pending_expire_minutes}
        hpn = simulate_stop_extension_hedges(sl_events, ticks_arr, sm, HCFG)
        for ts_ns, p in hpn:
            deals.append((int(ts_ns), sn, p))
    deals.sort(key=lambda x: x[0])
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gains = losses = 0.0; wins = 0
    for _, _sn, p in deals:
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p >= 0: gains += p; wins += 1
        else: losses += -p
    np_ = bal - DEPOSIT
    pf = gains / losses if losses > 0 else float("inf")
    ndd = np_ / dd_abs if dd_abs > 0 else 0.0
    dd_pct = (dd_abs / max(bal_max, DEPOSIT) * 100.0) if bal_max > 0 else 0.0
    return {"np": np_, "dd": dd_abs, "dd_pct": dd_pct, "ndd": ndd, "pf": pf,
            "trades": len(deals), "wins": wins}


def main():
    sp30_p = pd.read_csv(ROOT/"output/wfo_orb_v2_may23_spread30/oos_rank.csv")
    sp60_p = pd.read_csv(ROOT/"output/wfo_orb_v2_may23/oos_rank.csv")
    sp30_h = pd.read_csv(ROOT/"output/wfo_orb_v2_may23_spread30/oos_rank_hedged.csv")
    sp60_h = pd.read_csv(ROOT/"output/wfo_orb_v2_may23/oos_rank_hedged.csv")

    modes = {
        "A. PARENT-ONLY HYBRID":   [cfg_from_row(sp30_p.iloc[i]) for i in range(3)] +
                                    [cfg_from_row(sp60_p.iloc[i]) for i in range(3)],
        "B. HEDGED HYBRID":        [cfg_from_row(sp30_h.iloc[i]) for i in range(3)] +
                                    [cfg_from_row(sp60_h.iloc[i]) for i in range(3)],
        "C. TOP-6 HEDGED sp30":    [cfg_from_row(sp30_h.iloc[i]) for i in range(6)],
        "D. TOP-6 HEDGED sp60":    [cfg_from_row(sp60_h.iloc[i]) for i in range(6)],
        "E. TOP-6 PARENT sp30":    [cfg_from_row(sp30_p.iloc[i]) for i in range(6)],
    }

    ts = datetime(2026, 5, 23, tzinfo=timezone.utc)
    te = datetime.now(timezone.utc)

    sym, m = fetch_meta(None, account="live")
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])
    sym, ticks, m1, m5 = fetch_window(sym, ts, te, LIVE_SPREAD, account="live")
    ticks_arr = ts_arr_from_ticks(ticks)
    print(f"\n[WINDOW] {ts.date()} -> {te.strftime('%Y-%m-%d %H:%M UTC')}  "
          f"({(te-ts).days} days, {len(ticks):,} ticks)\n")

    results = {}
    for name, cfgs in modes.items():
        r = run_portfolio(cfgs, ticks, m1, m5, meta, ticks_arr)
        results[name] = r
        print(f"  [{name}]  NP=${r['np']:>+8,.0f}  DD=${r['dd']:>7,.0f} ({r['dd_pct']:>5.2f}%)  "
              f"NDD={r['ndd']:>6.2f}  PF={r['pf']:>5.2f}  trades={r['trades']:>3}  wins={r['wins']:>3}")
    kill_mt5_terminal()

    print()
    print("=" * 110)
    print("  RESULTS SUMMARY — 6-stream portfolio on live forward ticks (parent + STOP-ext hedge)")
    print("=" * 110)
    print(f"  {'Mode':<26}  {'NP':>10}  {'DD $':>9}  {'DD %':>7}  {'NDD':>6}  {'PF':>5}  {'Trades':>7}  {'Wins':>6}")
    print(f"  {'-' * 96}")
    for name, r in results.items():
        pf = f"{r['pf']:.2f}" if r['pf'] != float('inf') else "inf"
        print(f"  {name:<26}  ${r['np']:>+9,.0f}  ${r['dd']:>+8,.0f}  {r['dd_pct']:>6.2f}%  "
              f"{r['ndd']:>6.2f}  {pf:>5}  {r['trades']:>7d}  {r['wins']:>6d}")
    print()

    # Live haircut convention: NP × 0.94, PF − 0.25
    print(f"  After live haircut (NP × 0.94, PF − 0.25):")
    print(f"  {'Mode':<26}  {'NP_hc':>10}  {'PF_hc':>5}  {'NDD_hc':>7}")
    print(f"  {'-' * 65}")
    for name, r in results.items():
        np_hc = r['np'] * 0.94
        pf_hc = max(r['pf'] - 0.25, 0.0)
        ndd_hc = np_hc / r['dd'] if r['dd'] > 0 else 0
        pf_hc_str = f"{pf_hc:.2f}" if pf_hc != float('inf') else "inf"
        print(f"  {name:<26}  ${np_hc:>+9,.0f}  {pf_hc_str:>5}  {ndd_hc:>7.2f}")
    print("=" * 110)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
