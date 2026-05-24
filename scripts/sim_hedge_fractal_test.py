"""Focused A/B test: hedge-fractal-filter OFF vs ON (width=5).

Uses the current hedge WFO winner (per-stream sl_mult/alpha/profit_mult,
global exp=240 f1=1800) against the new V5 parents, sims both with
fractal_confirm=False (current) and fractal_confirm=True (new).

Reports per-window OOS NP + slope to see if the fractal filter halts
the decay observed in the may23 WFO (slope -103%, W4 OOS negative).
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.fractals import confirmed_fractals
from zgb_sim.wfo_helpers import WINDOWS_MAY23 as WINDOWS, to_utc

from sim_wfo_hedge_retry import (STREAM_CFGS, make_stream_cfg, slice_window,
                                  ts_arr_from_ticks, SYMBOL, DEPOSIT, SPREAD,
                                  POINT, CONTRACT, PARENT_RISK_PROD)
from sim_wfo_hedge_reverse import (ReverseHedgeCfg, simulate_reverse_hedges,
                                    tag_session_regimes)


HEDGE_WINNER_BASE = {
    "S1": dict(sl_mult=1.0, partial_fraction=0.5, profit_mult=3.0),
    "S2": dict(sl_mult=1.2, partial_fraction=0.5, profit_mult=3.5),
    "S3": dict(sl_mult=1.0, partial_fraction=0.5, profit_mult=3.0),
    "S4": dict(sl_mult=1.0, partial_fraction=0.5, profit_mult=3.5),
    "S5": dict(sl_mult=1.0, partial_fraction=0.5, profit_mult=3.0),
    "S6": dict(sl_mult=1.2, partial_fraction=0.5, profit_mult=3.5),
}


def make_hcfg(stream: str, fractal_on: bool) -> ReverseHedgeCfg:
    p = HEDGE_WINNER_BASE[stream]
    return ReverseHedgeCfg(
        exp_min=240, f1_sec=1800, regime_gate="off",
        sl_mult=p["sl_mult"], partial_fraction=p["partial_fraction"],
        profit_mult=p["profit_mult"],
        fractal_confirm=fractal_on, fractal_width=5,
    )


def extract_sl_events_v2(deals):
    """Pair entry deals with their SL/TP exits using kind & pnl heuristics."""
    open_pos = []
    sl_events = []
    for d in deals:
        ts_ns = pd.Timestamp(d.ts).value
        kind = str(d.kind).lower()
        if "entry" in kind:
            open_pos.append({"ts_ns": ts_ns, "direction": int(d.direction),
                              "entry_price": float(d.price), "lots": float(d.lots)})
            continue
        # FIFO match by direction
        match = -1
        for i, op in enumerate(open_pos):
            if op["direction"] == int(d.direction):
                match = i; break
        if match < 0:
            continue
        op = open_pos.pop(match)
        # Treat any LOSING exit as a hedge opportunity (kind may be "sl", "stop", "exit", etc.)
        if d.pnl < 0:
            sl_events.append({
                "ts_ns": ts_ns,
                "direction": op["direction"],
                "entry_price": op["entry_price"],
                "entry_ts_ns": op["ts_ns"],
                "lots": op["lots"],
            })
    return sl_events


def run_one(fractal_on: bool, fold_label, ticks, m1, m5, meta, regime_by_session, fractal_cache):
    ticks_arr = ts_arr_from_ticks(ticks)
    rows = []
    portfolio_pnls = []
    for stream in ("S1","S2","S3","S4","S5","S6"):
        parent_cfg = make_stream_cfg(stream, PARENT_RISK_PROD)
        r = orb_simulate(ticks, m5, m1, parent_cfg, meta, initial_balance=DEPOSIT)
        sl_events = extract_sl_events_v2(r.deals)
        parent_np = sum(d.pnl for d in r.deals if str(d.kind).lower() != "entry")

        hcfg = make_hcfg(stream, fractal_on)
        hedge_pnls = simulate_reverse_hedges(
            sl_events, ticks_arr, STREAM_CFGS[stream],
            hcfg, regime_by_session, fractal_cache=fractal_cache,
        )
        hedge_n = len(hedge_pnls)
        hedge_np = sum(p for _, p in hedge_pnls)
        hedge_wins = sum(1 for _, p in hedge_pnls if p > 0)
        hedge_wr = (hedge_wins / hedge_n * 100) if hedge_n else 0.0
        rows.append({
            "fold": fold_label, "stream": stream,
            "fractal": "ON_w5" if fractal_on else "OFF",
            "parent_sls": len(sl_events),
            "parent_np": round(parent_np, 0),
            "hedge_fired": hedge_n,
            "hedge_np": round(hedge_np, 0),
            "hedge_wr": round(hedge_wr, 1),
        })
        portfolio_pnls.extend(hedge_pnls)
    return rows


def main():
    print(f"=== Hedge fractal A/B test on may23 windows ===")
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])

        full_start = to_utc(WINDOWS[0][1])
        full_end = to_utc(WINDOWS[-1][4])
        ticks_full = load_ticks(SYMBOL, full_start, full_end, spread_pts=SPREAD)
        m1_full = load_bars(SYMBOL, "M1", full_start, full_end)
        m5_full = load_bars(SYMBOL, "M5", full_start, full_end)

        all_rows = []
        for w_label, is_s, is_e, oos_s, oos_e in WINDOWS:
            for fold_label, s, e in (("IS", is_s, is_e), ("OOS", oos_s, oos_e)):
                ts_start = to_utc(s); ts_end = to_utc(e)
                ticks_w = slice_window(ticks_full, "ts", ts_start, ts_end)
                m1_w    = slice_window(m1_full,    "ts", ts_start, ts_end)
                m5_w    = slice_window(m5_full,    "ts", ts_start, ts_end)
                regime_by_session = tag_session_regimes(ticks_w, m1_w)
                fractal_cache = confirmed_fractals(m5_w, width=5)
                lab = f"{w_label}_{fold_label}"
                all_rows.extend(run_one(False, lab, ticks_w, m1_w, m5_w, meta, regime_by_session, fractal_cache))
                all_rows.extend(run_one(True,  lab, ticks_w, m1_w, m5_w, meta, regime_by_session, fractal_cache))
                print(f"  {lab}: done")
    finally:
        kill_mt5_terminal()

    df = pd.DataFrame(all_rows)
    OUT = ROOT / "output" / "wfo_hedge_reverse_may23" / "fractal_ab.csv"
    df.to_csv(OUT, index=False)
    print(f"\nWrote: {OUT}")

    # Per-fold OOS aggregates
    print(f"\n=== OOS aggregates (per window) ===")
    print(f"{'fold':<10} {'mode':<6} {'fired':>6} {'NP':>10} {'WR%':>6}")
    for w_label, _, _, _, _ in WINDOWS:
        for mode in ("OFF", "ON_w5"):
            sub = df[(df["fold"] == f"{w_label}_OOS") & (df["fractal"] == mode)]
            fired = int(sub["hedge_fired"].sum())
            np_  = sub["hedge_np"].sum()
            wins_total = sum(r["hedge_wr"]/100 * r["hedge_fired"] for _, r in sub.iterrows())
            wr = wins_total / max(fired, 1) * 100
            print(f"{w_label+'_OOS':<10} {mode:<6} {fired:>6} ${np_:>+9,.0f} {wr:>5.1f}%")

    # Per-stream OOS NP trend by mode
    print(f"\n=== Per-stream OOS hedge NP trend (OFF vs ON) ===")
    for s in ("S1","S2","S3","S4","S5","S6"):
        print(f"\n  {s}:")
        for mode in ("OFF", "ON_w5"):
            vals = []
            for w_label, _, _, _, _ in WINDOWS:
                v = df[(df["fold"] == f"{w_label}_OOS") & (df["stream"] == s) & (df["fractal"] == mode)]
                vals.append(v.iloc[0]["hedge_np"] if not v.empty else 0)
            tot = sum(vals)
            print(f"    {mode:<6}  W1=${vals[0]:>+7,.0f}  W2=${vals[1]:>+7,.0f}  W3=${vals[2]:>+7,.0f}  W4=${vals[3]:>+7,.0f}  total=${tot:>+8,.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
