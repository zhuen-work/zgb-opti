"""Per-stream A/B: hedge as pending LIMIT (reversal bet) vs pending STOP (continuation bet).

For each S1/S2/S3:
  - Run baseline (no hedge)
  - Sweep LIMIT hedge grid → best by NP/DD$
  - Sweep STOP hedge grid → best by NP/DD$
  - Report top 5 of each + side-by-side winner comparison

Window: Feb 14 -> May 1 (76d, $10k, 23pt). Same grid as sim_orb_s1_hedge_sweep.py.
Hedge sized at 1% (mirrors per-stream production allocation).
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import importlib.util
_spec = importlib.util.spec_from_file_location("wfo_hedge", ROOT / "scripts" / "sim_wfo_hedge.py")
hg = importlib.util.module_from_spec(_spec)
sys.modules["wfo_hedge"] = hg
_spec.loader.exec_module(hg)

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
SPREAD = 23
START = datetime(2026, 2, 14, tzinfo=timezone.utc)
END = datetime(2026, 5, 1, tzinfo=timezone.utc)

STREAM_CFGS = hg.STREAM_CFGS

# Same sweep grid as sim_orb_s1_hedge_sweep.py v2
BUFFERS    = [0, 50, 100, 200, 350]
HEDGE_SLS  = [300, 500, 700, 900]
HEDGE_RRS  = [2.0, 3.0, 4.0]
EXPIRES    = [30, 120]
HEDGE_RISK_PCT = 1.0


def aggregate(deals):
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0; gp = gl = 0.0
    for _, p in sorted(deals, key=lambda x: x[0]):
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


def make_cfg(stream, risk_pct):
    sc = STREAM_CFGS[stream]
    return ORBConfig(
        risk_pct=risk_pct, range_minutes=sc["range_minutes"],
        buffer_pts=0, min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=sc["fixed_sl_pts"], rr_ratio=sc["rr_ratio"],
        half_tp_ratio=sc["half_tp_ratio"], pending_expire_minutes=240,
        daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True,  ny_start_hour=13, comment=stream,
    )


def sweep_one_stream(stream, ticks, m1, m5, meta, ticks_arr):
    print("\n" + "=" * 110)
    sc = STREAM_CFGS[stream]
    print(f"  STREAM {stream}  (parent: Range={sc['range_minutes']} SL={sc['fixed_sl_pts']} "
          f"RR={sc['rr_ratio']} HTP={sc['half_tp_ratio']})")
    print("=" * 110)
    cfg = make_cfg(stream, 3.0)
    r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
    base_deals = []; sl_events = []
    for d in r.deals:
        if d.kind == "entry": continue
        base_deals.append((pd.Timestamp(d.ts).value, d.pnl))
        if d.kind == "sl":
            sl_events.append({"ts_ns": pd.Timestamp(d.ts).value,
                              "direction": int(d.direction),
                              "sl_price": float(d.price),
                              "lots": float(d.lots)})
    b_np, b_dd, b_pf, b_ndd = aggregate(base_deals)
    print(f"  Baseline: NP=${b_np:+,.0f} DD={b_dd:.2f}% PF={b_pf:.2f} NP/DD$={b_ndd:.2f} "
          f"trades={len(base_deals)} SL_events={len(sl_events)}")

    hg.HEDGE_RISK_PCT = HEDGE_RISK_PCT
    grid = [hg.HedgeCfg(b, hs, hr, e) for b in BUFFERS for hs in HEDGE_SLS
            for hr in HEDGE_RRS for e in EXPIRES]
    results = {}
    for mode in ("limit", "stop"):
        cells = []
        for hc in grid:
            h_deals = hg.simulate_hedges(sl_events, ticks_arr, hc, order_type=mode)
            merged = base_deals + h_deals
            np_, dd, pf, ndd = aggregate(merged)
            h_pnl = sum(p for _, p in h_deals)
            h_n = len(h_deals)
            h_w = sum(1 for _, p in h_deals if p > 0)
            wr = (h_w / h_n * 100) if h_n else 0
            cells.append({"buf": hc.buf, "h_sl": hc.h_sl, "h_rr": hc.h_rr, "exp": hc.exp,
                          "np": np_, "dd": dd, "pf": pf, "ndd": ndd,
                          "h_np": h_pnl, "h_n": h_n, "h_wr": wr, "d_ndd": ndd - b_ndd})
        cells.sort(key=lambda c: c["ndd"], reverse=True)
        results[mode] = cells
        print(f"\n  Top 5 {mode.upper()} (vs baseline {b_ndd:.2f}):")
        print(f"  {'rank':>4} {'buf':>4} {'h_sl':>5} {'h_rr':>5} {'exp':>4} "
              f"{'NP':>10} {'DD%':>6} {'NP/DD$':>7} {'dNP/DD$':>8} {'h_NP':>9} {'h_n':>4} {'h_WR':>5}")
        for i, c in enumerate(cells[:5], 1):
            print(f"  #{i:<3} {c['buf']:>4} {c['h_sl']:>5} {c['h_rr']:>5.1f} {c['exp']:>4} "
                  f"${c['np']:>+8,.0f} {c['dd']:>5.2f}% {c['ndd']:>7.2f} {c['d_ndd']:>+7.2f} "
                  f"${c['h_np']:>+7,.0f} {c['h_n']:>4} {c['h_wr']:>4.0f}%")
    return {"baseline": (b_np, b_dd, b_ndd, b_pf, len(base_deals)),
            "limit_top": results["limit"][0],
            "stop_top":  results["stop"][0]}


def main():
    print("=" * 110)
    print(f"  HEDGE A/B  |  LIMIT (reversal) vs STOP (continuation)  |  {START.date()} -> {END.date()} (76d, $10k, {SPREAD}pt)")
    print(f"  Grid: buf={BUFFERS} h_sl={HEDGE_SLS} h_rr={HEDGE_RRS} exp={EXPIRES} = "
          f"{len(BUFFERS)*len(HEDGE_SLS)*len(HEDGE_RRS)*len(EXPIRES)} cells × 2 modes × 3 streams")
    print(f"  Hedge sized at {HEDGE_RISK_PCT}% / deposit")
    print("=" * 110)
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        m1 = load_bars(SYMBOL, "M1", START, END)
        m5 = load_bars(SYMBOL, "M5", START, END)
        ticks = load_ticks(SYMBOL, START, END, spread_pts=SPREAD)
        ticks_arr = hg.ts_arr_from_ticks(ticks)
        print(f"\n  Loaded ticks={len(ticks):,} M1={len(m1):,} M5={len(m5):,}")

        results = {}
        for s in ("S1", "S2", "S3"):
            results[s] = sweep_one_stream(s, ticks, m1, m5, meta, ticks_arr)

        # Side-by-side
        print("\n" + "=" * 110)
        print("  CROSS-STREAM SUMMARY: LIMIT vs STOP best per stream")
        print("=" * 110)
        print(f"  {'Stream':<6} {'Base NP/DD$':>12} | {'LIMIT NP/DD$':>12} {'cfg':<25} {'h_WR':>5} | "
              f"{'STOP NP/DD$':>12} {'cfg':<25} {'h_WR':>5} | Winner")
        for s in ("S1", "S2", "S3"):
            base = results[s]["baseline"]
            l = results[s]["limit_top"]; st = results[s]["stop_top"]
            l_cfg = f"{l['buf']}/{l['h_sl']}/{l['h_rr']}/{l['exp']}"
            s_cfg = f"{st['buf']}/{st['h_sl']}/{st['h_rr']}/{st['exp']}"
            winner = "LIMIT" if l["ndd"] > st["ndd"] else "STOP"
            if max(l["ndd"], st["ndd"]) <= base[2]: winner = "neither (baseline best)"
            print(f"  {s:<6} {base[2]:>12.2f} | {l['ndd']:>12.2f} {l_cfg:<25} {l['h_wr']:>4.0f}% | "
                  f"{st['ndd']:>12.2f} {s_cfg:<25} {st['h_wr']:>4.0f}% | {winner}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
