"""Combo sweep: alpha x F1 to try to fix hedge decay.

Locked:
  regime_gate = off  (per user 2026-05-24)
  Per-stream sl_mult + profit_mult = WFO winner (no change)
  expire_minutes = 240

Swept:
  partial_fraction (alpha): {0.5, 0.6, 0.7}
  f1_sec:                   {600, 1800}  (10 min, 30 min)

= 6 combos x 6 streams x 8 folds (4 IS + 4 OOS) = 288 sims. ~4-7 min.

Goal: find a (alpha, F1) combo whose W3/W4 OOS hedge NP is positive,
i.e., the decay is broken.
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
from zgb_sim.wfo_helpers import WINDOWS_MAY23 as WINDOWS, to_utc

from sim_wfo_hedge_retry import (STREAM_CFGS, make_stream_cfg, slice_window,
                                  ts_arr_from_ticks, SYMBOL, DEPOSIT, SPREAD,
                                  PARENT_RISK_PROD)
from sim_wfo_hedge_reverse import (ReverseHedgeCfg, simulate_reverse_hedges,
                                    tag_session_regimes)


# Per-stream WFO winner sl_mult + profit_mult (locked)
WIN_BASE = {
    "S1": dict(sl_mult=1.0, profit_mult=3.0),
    "S2": dict(sl_mult=1.2, profit_mult=3.5),
    "S3": dict(sl_mult=1.0, profit_mult=3.0),
    "S4": dict(sl_mult=1.0, profit_mult=3.5),
    "S5": dict(sl_mult=1.0, profit_mult=3.0),
    "S6": dict(sl_mult=1.2, profit_mult=3.5),
}

ALPHAS = [0.5, 0.6, 0.7]
F1S = [600, 1800]


def make_hcfg(stream: str, alpha: float, f1_sec: int) -> ReverseHedgeCfg:
    p = WIN_BASE[stream]
    # Validity check
    pm = p["profit_mult"]
    if alpha <= 1.0 / (pm + 1.0):
        # Invalid combo for this stream's pm; skip (return None to signal)
        return None
    return ReverseHedgeCfg(
        exp_min=240, f1_sec=f1_sec, regime_gate="off",
        sl_mult=p["sl_mult"], partial_fraction=alpha, profit_mult=pm,
        fractal_confirm=False, fractal_width=5,
    )


def extract_sl_events_v2(deals):
    open_pos = []
    sl_events = []
    for d in deals:
        ts_ns = pd.Timestamp(d.ts).value
        if "entry" in str(d.kind).lower():
            open_pos.append({"ts_ns": ts_ns, "direction": int(d.direction),
                              "entry_price": float(d.price), "lots": float(d.lots)})
            continue
        match = -1
        for i, op in enumerate(open_pos):
            if op["direction"] == int(d.direction):
                match = i; break
        if match < 0:
            continue
        op = open_pos.pop(match)
        if d.pnl < 0:
            sl_events.append({
                "ts_ns": ts_ns,
                "direction": op["direction"],
                "entry_price": op["entry_price"],
                "entry_ts_ns": op["ts_ns"],
                "lots": op["lots"],
            })
    return sl_events


def main():
    print(f"=== Hedge alpha x F1 combo sweep (regime=off) on may23 windows ===")
    all_rows = []
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

        for w_label, is_s, is_e, oos_s, oos_e in WINDOWS:
            for fold_label, s, e in (("IS", is_s, is_e), ("OOS", oos_s, oos_e)):
                ts_start = to_utc(s); ts_end = to_utc(e)
                ticks_w = slice_window(ticks_full, "ts", ts_start, ts_end)
                m1_w    = slice_window(m1_full,    "ts", ts_start, ts_end)
                m5_w    = slice_window(m5_full,    "ts", ts_start, ts_end)
                regime_by_session = tag_session_regimes(ticks_w, m1_w)
                ticks_arr = ts_arr_from_ticks(ticks_w)

                # Cache parent sims per stream once per window
                stream_sls = {}
                for stream in ("S1","S2","S3","S4","S5","S6"):
                    pcfg = make_stream_cfg(stream, PARENT_RISK_PROD)
                    r = orb_simulate(ticks_w, m5_w, m1_w, pcfg, meta, initial_balance=DEPOSIT)
                    stream_sls[stream] = extract_sl_events_v2(r.deals)

                for alpha in ALPHAS:
                    for f1 in F1S:
                        for stream in ("S1","S2","S3","S4","S5","S6"):
                            hcfg = make_hcfg(stream, alpha, f1)
                            if hcfg is None:
                                continue   # invalid alpha/pm combo for this stream
                            hpnls = simulate_reverse_hedges(
                                stream_sls[stream], ticks_arr,
                                STREAM_CFGS[stream], hcfg, regime_by_session,
                            )
                            n = len(hpnls)
                            np_  = sum(p for _, p in hpnls)
                            wins = sum(1 for _, p in hpnls if p > 0)
                            wr = (wins / n * 100) if n else 0
                            all_rows.append({
                                "fold": f"{w_label}_{fold_label}",
                                "stream": stream,
                                "alpha": alpha, "f1": f1,
                                "fired": n, "np": round(np_, 0), "wr": round(wr, 1),
                            })
                print(f"  {w_label}_{fold_label}: done")
    finally:
        kill_mt5_terminal()

    df = pd.DataFrame(all_rows)
    OUT = ROOT / "output" / "wfo_hedge_reverse_may23" / "alpha_f1_sweep.csv"
    df.to_csv(OUT, index=False)
    print(f"\nWrote: {OUT}")

    # OOS aggregate per (alpha, f1)
    print(f"\n=== OOS aggregates per (alpha, F1) ===")
    print(f"{'alpha':>5} {'F1':>5} | {'W1_NP':>9} {'W2_NP':>9} {'W3_NP':>9} {'W4_NP':>9} | {'total':>10} {'totWR%':>7}")
    for alpha in ALPHAS:
        for f1 in F1S:
            row = [f"{alpha:.1f}", str(f1)]
            tot_np = 0
            tot_n = 0
            tot_wins = 0
            for w_label, _, _, _, _ in WINDOWS:
                sub = df[(df["fold"] == f"{w_label}_OOS") &
                         (df["alpha"] == alpha) & (df["f1"] == f1)]
                np_w = sub["np"].sum()
                n_w  = sub["fired"].sum()
                wins_w = sum(r["wr"]/100 * r["fired"] for _, r in sub.iterrows())
                tot_np += np_w; tot_n += n_w; tot_wins += wins_w
                row.append(f"${np_w:>+8,.0f}")
            wr = (tot_wins / max(tot_n, 1)) * 100
            row.append(f"${tot_np:>+9,.0f}")
            row.append(f"{wr:>6.1f}%")
            print(" ".join(row))

    # Find winning combo
    best = max(((a, f, sum(df[(df["fold"].str.endswith("_OOS")) & (df["alpha"]==a) & (df["f1"]==f)]["np"]))
                for a in ALPHAS for f in F1S),
               key=lambda x: x[2])
    print(f"\n  BEST (alpha={best[0]}, F1={best[1]}): total OOS NP = ${best[2]:+,.0f}")
    base_total = sum(df[(df["fold"].str.endswith("_OOS")) & (df["alpha"]==0.5) & (df["f1"]==1800)]["np"])
    print(f"  Baseline (alpha=0.5, F1=1800): total OOS NP = ${base_total:+,.0f}")
    print(f"  Improvement: ${best[2] - base_total:+,.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
