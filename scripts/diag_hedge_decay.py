"""Decay diagnostic for the v5 reverse-hedge WFO.

Hedge WFO winner showed slope=-103.5% (OOS NPs collapse W1 -> W4).
This script breaks down per (window, stream):
  - parent trades fired
  - parent SL events (hedge opportunities)
  - hedge fired (after F1 filter)
  - hedge NP
  - hedge WR

Output: a per-week-per-stream table so we can see WHICH streams decay
and WHEN, plus correlation between parent_SL_count and hedge_NP.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.wfo_helpers import WINDOWS_MAY23 as WINDOWS, to_utc

from sim_wfo_hedge_retry import (STREAM_CFGS, make_stream_cfg, slice_window,
                                  ts_arr_from_ticks, SYMBOL, DEPOSIT, SPREAD,
                                  POINT, PARENT_RISK_SWEEP)
from sim_wfo_hedge_reverse import (ReverseHedgeCfg, simulate_reverse_hedges,
                                    tag_session_regimes)


# Winner from output/wfo_hedge_reverse_may23/winner.json
HEDGE_WINNER = {
    "S1": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.0, partial_fraction=0.5, profit_mult=3.0),
    "S2": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.2, partial_fraction=0.5, profit_mult=3.5),
    "S3": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.0, partial_fraction=0.5, profit_mult=3.0),
    "S4": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.0, partial_fraction=0.5, profit_mult=3.5),
    "S5": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.0, partial_fraction=0.5, profit_mult=3.0),
    "S6": ReverseHedgeCfg(exp_min=240, f1_sec=1800, regime_gate="off", sl_mult=1.2, partial_fraction=0.5, profit_mult=3.5),
}


def extract_sl_events(deals):
    """Walk a SimResult.deals list and pair each entry with its SL/TP/other exit.
    Returns list of dicts with ts_ns, direction, entry_price, entry_ts_ns, exit_kind, exit_pnl.
    Only returns events where exit_kind=='SL' (the hedge opportunities).
    """
    open_pos = []
    sl_events = []
    for d in deals:
        ts_ns = pd.Timestamp(d.ts).value
        if d.kind == "entry":
            open_pos.append({"ts_ns": ts_ns, "direction": int(d.direction),
                              "entry_price": float(d.price), "lots": float(d.lots)})
            continue
        # find matching FIFO open position (same direction)
        match = -1
        for i, op in enumerate(open_pos):
            if op["direction"] == int(d.direction):
                match = i; break
        if match < 0:
            continue
        op = open_pos.pop(match)
        if d.kind == "SL":
            sl_events.append({
                "ts_ns": ts_ns,
                "direction": op["direction"],
                "entry_price": op["entry_price"],
                "entry_ts_ns": op["ts_ns"],
            })
    return sl_events


def main():
    print(f"=== Hedge decay diagnostic | may23 windows ===")
    rows = []
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])

        # Walk both IS and OOS for each window
        all_folds = []
        for w_label, is_s, is_e, oos_s, oos_e in WINDOWS:
            all_folds.append((f"{w_label}_IS", is_s, is_e))
            all_folds.append((f"{w_label}_OOS", oos_s, oos_e))

        # Load the full span ONCE
        full_start = to_utc(WINDOWS[0][1])
        full_end = to_utc(WINDOWS[-1][4])
        ticks_full = load_ticks(SYMBOL, full_start, full_end, spread_pts=SPREAD)
        m1_full = load_bars(SYMBOL, "M1", full_start, full_end)
        m5_full = load_bars(SYMBOL, "M5", full_start, full_end)

        for fold_label, s, e in all_folds:
            ts_start = to_utc(s); ts_end = to_utc(e)
            ticks_w = slice_window(ticks_full, "ts", ts_start, ts_end)
            m1_w    = slice_window(m1_full,    "ts", ts_start, ts_end)
            m5_w    = slice_window(m5_full,    "ts", ts_start, ts_end)
            if ticks_w.empty:
                print(f"  {fold_label}: empty window, skipping")
                continue
            regime_by_session = tag_session_regimes(ticks_w, m1_w)
            ticks_arr = ts_arr_from_ticks(ticks_w)

            for stream in ("S1","S2","S3","S4","S5","S6"):
                parent_cfg = make_stream_cfg(stream, PARENT_RISK_SWEEP)
                r = orb_simulate(ticks_w, m5_w, m1_w, parent_cfg, meta, initial_balance=DEPOSIT)
                entries = sum(1 for d in r.deals if d.kind == "entry")
                sl_events = extract_sl_events(r.deals)

                hcfg = HEDGE_WINNER[stream]
                hedge_pnls = simulate_reverse_hedges(
                    sl_events, ticks_arr,
                    STREAM_CFGS[stream], hcfg, regime_by_session,
                )
                # F1 filter post-fact (simulate_reverse_hedges already applies it via hcfg.f1_sec)
                hedge_n = len(hedge_pnls)
                hedge_np = sum(p for _, p in hedge_pnls)
                hedge_wins = sum(1 for _, p in hedge_pnls if p > 0)
                hedge_wr = (hedge_wins / hedge_n * 100) if hedge_n else 0.0
                parent_np = sum(d.pnl for d in r.deals if d.kind != "entry")
                rows.append({
                    "fold": fold_label, "stream": stream,
                    "parent_entries": entries,
                    "parent_sls": len(sl_events),
                    "parent_np": round(parent_np, 0),
                    "hedge_fired": hedge_n,
                    "hedge_np": round(hedge_np, 0),
                    "hedge_wr": round(hedge_wr, 1),
                    "fire_rate_pct": round(hedge_n / max(len(sl_events), 1) * 100, 1),
                })
            print(f"  {fold_label}: done ({len(rows)} rows so far)")
    finally:
        kill_mt5_terminal()

    df = pd.DataFrame(rows)
    OUT = ROOT / "output" / "wfo_hedge_reverse_may23" / "decay_diag.csv"
    df.to_csv(OUT, index=False)
    print(f"\nWrote: {OUT}\n")

    # Aggregate by fold (OOS only)
    print("\n=== Per-fold OOS aggregate ===")
    print(f"{'fold':<10} {'parent_SLs':>10} {'hedge_fired':>11} {'fire_rate%':>10} {'hedge_NP':>10} {'hedge_WR%':>9}")
    for w_label, _, _, _, _ in WINDOWS:
        sub = df[df["fold"] == f"{w_label}_OOS"]
        if sub.empty: continue
        psls = int(sub["parent_sls"].sum())
        hf   = int(sub["hedge_fired"].sum())
        fr   = hf / max(psls, 1) * 100
        hnp  = sub["hedge_np"].sum()
        # WR aggregated across streams (weighted by fire count)
        wins_total = sum((r["hedge_wr"]/100 * r["hedge_fired"]) for _, r in sub.iterrows())
        wr   = wins_total / max(hf, 1) * 100
        print(f"{w_label+'_OOS':<10} {psls:>10} {hf:>11} {fr:>9.1f}% ${hnp:>+9,.0f} {wr:>8.1f}%")

    # Per-stream slope across OOS folds
    print("\n=== Per-stream OOS hedge NP trend ===")
    print(f"{'stream':<6} {'W1_OOS':>10} {'W2_OOS':>10} {'W3_OOS':>10} {'W4_OOS':>10} {'total':>10}")
    for s in ("S1","S2","S3","S4","S5","S6"):
        vals = []
        for w_label, _, _, _, _ in WINDOWS:
            v = df[(df["fold"] == f"{w_label}_OOS") & (df["stream"] == s)]
            vals.append(v.iloc[0]["hedge_np"] if not v.empty else 0)
        print(f"{s:<6} ${vals[0]:>+9,.0f} ${vals[1]:>+9,.0f} ${vals[2]:>+9,.0f} ${vals[3]:>+9,.0f} ${sum(vals):>+9,.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
