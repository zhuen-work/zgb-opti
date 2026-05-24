"""Retry-hedge WFO on MAY23 windows + 3-way next-week comparison.

Phase 1: Per-stream retry-hedge WFO. Uses current STREAM_CFGS (top-6 from v5
expire-extend WFO with fractal_confirm=true). Sweeps (exp_min, f1_sec, tp_mult)
per stream, ranks with P0 decay-filter, picks winner.

Phase 2: 3-way comparison on May 2-23 (3 weeks live-match window, 30pt spread):
  A) Parents only (current v5 deployment)
  B) Parents + retry-hedge with WFO winners
  C) Parents + reverse-hedge with prior WFO winners (already known)

Markers: [BOOT], [GRID], [WIN], [PHASE2], [DONE].
"""
from __future__ import annotations

import json
import sys
import time
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
from zgb_sim.wfo_helpers import (WINDOWS_MAY23 as WINDOWS, rank_with_p0,
                                  print_phase_d_with_p0, select_winner_with_p0,
                                  check_winner_boundaries, print_boundary_check, to_utc)
from sim_wfo_hedge_retry import (STREAM_CFGS, make_stream_cfg, slice_window,
                                  aggregate, ts_arr_from_ticks,
                                  run_baseline_window, simulate_retry_hedges,
                                  RetryHedgeCfg, SYMBOL, DEPOSIT, SPREAD, POINT,
                                  CONTRACT, PARENT_RISK_SWEEP, PARENT_RISK_PROD)
from sim_wfo_hedge_reverse import (ReverseHedgeCfg, simulate_reverse_hedges,
                                    tag_session_regimes)

# --- Phase 1 grid (focused for May23) ---
EXPIRES_MIN  = [60, 240, 480, 720]      # 4
F1_CUTOFFS   = [0, 1800, 3600]           # 3 (0 = disabled)
BUFFERS_PTS  = [0]                        # 1 (fixed at exact entry)
TP_MULTS     = [0.5, 0.75, 1.0, 1.5]      # 4
# = 48 cells per stream x 6 streams x 8 folds = 2304 sims, ~10-15 min

OUT_DIR = ROOT / "output" / "wfo_hedge_retry_may23"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Reverse hedge winner (for comparison C)
REVERSE_WINNER = {
    "S1": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.0, partial_fraction=0.5, profit_mult=3.0),
    "S2": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.2, partial_fraction=0.5, profit_mult=3.5),
    "S3": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.0, partial_fraction=0.5, profit_mult=3.0),
    "S4": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.0, partial_fraction=0.5, profit_mult=3.5),
    "S5": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.0, partial_fraction=0.5, profit_mult=3.0),
    "S6": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.2, partial_fraction=0.5, profit_mult=3.5),
}


def phase1_wfo(full_ticks, full_m1, full_m5, meta):
    """Run per-stream retry-hedge WFO, return {stream: winner_cfg}."""
    grid = [RetryHedgeCfg(e, f, b, t) for e in EXPIRES_MIN for f in F1_CUTOFFS
            for b in BUFFERS_PTS for t in TP_MULTS]
    print(f"[GRID] {len(grid)} retry cells per stream  x  6 streams  x  8 folds  = {len(grid)*6*8} sims", flush=True)

    winners = {}
    for stream in ("S1","S2","S3","S4","S5","S6"):
        sc = STREAM_CFGS[stream]
        print(f"\n[STREAM {stream}] parent SL={sc['fixed_sl_pts']} RR={sc['rr_ratio']} HTP={sc['half_tp_ratio']} Exp={sc.get('pending_expire_minutes',240)}", flush=True)
        is_per, oos_per = {}, {}
        t0 = time.time()
        for label, is_s, is_e, oos_s, oos_e in WINDOWS:
            for tag, (s, e), bucket in [("IS", (to_utc(is_s), to_utc(is_e)), is_per),
                                         ("OOS", (to_utc(oos_s), to_utc(oos_e)), oos_per)]:
                ticks = slice_window(full_ticks, "ts", s, e)
                m1 = slice_window(full_m1, "ts", s, e)
                m5 = slice_window(full_m5, "ts", s, e)
                base_deals, sl_ev = run_baseline_window(stream, ticks, m1, m5, meta, PARENT_RISK_SWEEP)
                t_arr = ts_arr_from_ticks(ticks)
                rows = []
                for hc in grid:
                    h_deals = simulate_retry_hedges(sl_ev, t_arr, STREAM_CFGS[stream], hc)
                    merged = base_deals + h_deals
                    np_, dd, pf = aggregate(merged)
                    rows.append({"exp": hc.exp_min, "f1": hc.f1_sec, "buf": hc.buf_pts,
                                 "tp_mult": hc.tp_mult,
                                 "net_profit": np_, "drawdown_pct": dd, "profit_factor": pf,
                                 "trades": len(merged), "error": None, "recovery_factor": np_/max(dd, 0.5),
                                 "n_sl": len(sl_ev), "h_n": len(h_deals)})
                bucket[label] = pd.DataFrame(rows)
        ranked = rank_with_p0(grid, oos_per, WINDOWS, decay_threshold=-0.25,
                               grid_configs=grid, is_per_window=is_per)
        winner = select_winner_with_p0(ranked) or ranked[0]
        wcfg = winner["cfg"]
        print(f"  [WIN {stream}] exp={wcfg.exp_min}min f1={wcfg.f1_sec}s tp_mult={wcfg.tp_mult}  (elapsed {time.time()-t0:.0f}s)", flush=True)
        winners[stream] = wcfg
        (OUT_DIR / f"{stream}.json").write_text(json.dumps({
            "exp_min": wcfg.exp_min, "f1_sec": wcfg.f1_sec,
            "buf_pts": wcfg.buf_pts, "tp_mult": wcfg.tp_mult,
        }, indent=2))
    return winners


def extract_sl_events_for_reverse(deals):
    """Reverse-hedge needs entry_price + entry_ts_ns + lots, not sl_price."""
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


def phase2_three_way(retry_winners, full_ticks, full_m1, full_m5, meta):
    """3-way next-week comparison on May 2-23 window."""
    start = to_utc(datetime(2026, 5, 2).date())
    end = to_utc(datetime(2026, 5, 23).date())
    print(f"\n[PHASE2] 3-way compare {start.date()} -> {end.date()} @ {PARENT_RISK_PROD}%/stream", flush=True)
    ticks = slice_window(full_ticks, "ts", start, end)
    m1 = slice_window(full_m1, "ts", start, end)
    m5 = slice_window(full_m5, "ts", start, end)
    t_arr = ts_arr_from_ticks(ticks)
    regime = tag_session_regimes(ticks, m1)

    parent_deals = {}
    sl_ev_retry = {}      # retry needs sl_price (from run_baseline_window)
    sl_ev_reverse = {}    # reverse needs entry_price + entry_ts_ns
    for s in ("S1","S2","S3","S4","S5","S6"):
        deals, sl_ev_r = run_baseline_window(s, ticks, m1, m5, meta, PARENT_RISK_PROD)
        parent_deals[s] = deals
        sl_ev_retry[s] = sl_ev_r
        # Also build the reverse-format events (need entry_price + lots from full deals)
        # We can recover that by running orb_simulate again, but easier to refactor:
        # the deals from run_baseline_window are (ts_ns, pnl) pairs without entry data.
        # Re-run sim to extract for reverse hedge.
        cfg = make_stream_cfg(s, PARENT_RISK_PROD)
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        sl_ev_reverse[s] = extract_sl_events_for_reverse(r.deals)

    # Variant A: parents only
    a_deals = []
    for s in ("S1","S2","S3","S4","S5","S6"):
        a_deals.extend(parent_deals[s])

    # Variant B: parents + retry-hedge
    b_deals = list(a_deals)
    b_retry_contrib = {}
    for s in ("S1","S2","S3","S4","S5","S6"):
        hp = simulate_retry_hedges(sl_ev_retry[s], t_arr, STREAM_CFGS[s], retry_winners[s])
        b_deals.extend(hp)
        b_retry_contrib[s] = (sum(p for _, p in hp), len(hp),
                               sum(1 for _, p in hp if p > 0))

    # Variant C: parents + reverse-hedge
    c_deals = list(a_deals)
    c_reverse_contrib = {}
    for s in ("S1","S2","S3","S4","S5","S6"):
        hp = simulate_reverse_hedges(sl_ev_reverse[s], t_arr, STREAM_CFGS[s],
                                       REVERSE_WINNER[s], regime)
        c_deals.extend(hp)
        c_reverse_contrib[s] = (sum(p for _, p in hp), len(hp),
                                  sum(1 for _, p in hp if p > 0))

    def agg(deals):
        np_, dd, pf = aggregate(deals)
        return np_, dd, pf

    np_a, dd_a, pf_a = agg(a_deals)
    np_b, dd_b, pf_b = agg(b_deals)
    np_c, dd_c, pf_c = agg(c_deals)

    print(f"\n  === 3-WAY {start.date()} -> {end.date()} (parent risk {PARENT_RISK_PROD}%/stream, {SPREAD}pt) ===\n")
    print(f"  {'Variant':<22} {'NP':>10} {'DD%':>6} {'PF':>5} {'Trades':>7}")
    print(f"  {'A: Parents only':<22} ${np_a:>+8,.0f} {dd_a:>5.2f}% {pf_a:>5.2f} {len(a_deals):>7}")
    print(f"  {'B: + Retry hedge':<22} ${np_b:>+8,.0f} {dd_b:>5.2f}% {pf_b:>5.2f} {len(b_deals):>7}")
    print(f"  {'C: + Reverse hedge':<22} ${np_c:>+8,.0f} {dd_c:>5.2f}% {pf_c:>5.2f} {len(c_deals):>7}")
    print(f"\n  Delta vs A:  Retry  NP={np_b-np_a:+8,.0f}  DD={dd_b-dd_a:+.2f}pp")
    print(f"  Delta vs A:  Reverse NP={np_c-np_a:+8,.0f}  DD={dd_c-dd_a:+.2f}pp")

    print(f"\n  Per-stream retry contribution:")
    print(f"  {'Stream':<6} retry_NP    retry_n  retry_WR%")
    for s in ("S1","S2","S3","S4","S5","S6"):
        rp, rn, rw = b_retry_contrib[s]
        wr = rw / rn * 100 if rn else 0
        print(f"  {s:<6}  ${rp:>+7,.0f}    {rn:>4}     {wr:>5.1f}%")
    print(f"\n  Per-stream reverse contribution (for comparison):")
    print(f"  {'Stream':<6} reverse_NP  reverse_n  reverse_WR%")
    for s in ("S1","S2","S3","S4","S5","S6"):
        rp, rn, rw = c_reverse_contrib[s]
        wr = rw / rn * 100 if rn else 0
        print(f"  {s:<6}  ${rp:>+7,.0f}    {rn:>4}     {wr:>5.1f}%")

    return {"A": (np_a, dd_a, pf_a, len(a_deals)),
            "B": (np_b, dd_b, pf_b, len(b_deals), b_retry_contrib),
            "C": (np_c, dd_c, pf_c, len(c_deals), c_reverse_contrib)}


def main():
    print(f"[BOOT] Retry-hedge WFO + 3-way compare on MAY23 windows", flush=True)
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        full_start = to_utc(WINDOWS[0][1])
        full_end = to_utc(WINDOWS[-1][4])
        ticks = load_ticks(SYMBOL, full_start, full_end, spread_pts=SPREAD)
        m1 = load_bars(SYMBOL, "M1", full_start, full_end)
        m5 = load_bars(SYMBOL, "M5", full_start, full_end)
    finally:
        kill_mt5_terminal()

    winners = phase1_wfo(ticks, m1, m5, meta)
    summary = phase2_three_way(winners, ticks, m1, m5, meta)
    print(f"\n[DONE] Winners persisted to {OUT_DIR}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
