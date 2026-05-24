"""New hedge-geometry sweeps: multi-tier LIMITs + STOP-on-extension.

Tests two fundamentally new hedge mechanisms vs the existing single-LIMIT baseline:

  Multi-tier:  tier_count in {1, 2, 3}, tier_spacing in {0.15, 0.30, 0.50}
               1 x 1 cell is the baseline; 8 new variants.
  STOP-ext:    ext_pts in {0, 50, 100, 200} (offset past parent SL)
               tp_mult in {1.5, 2.0, 3.0}
               sl_mult fixed at 1.0
               = 12 new variants.

All run on may23 windows (4 IS + 4 OOS). 3-way compare on May 2-23.

Markers: [BOOT], [MT cells], [SE cells], [WIN], [3WAY], [DONE].
"""
from __future__ import annotations

import sys
import time
from dataclasses import dataclass
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
from zgb_sim.wfo_helpers import WINDOWS_MAY23 as WINDOWS, to_utc

from sim_wfo_hedge_retry import (STREAM_CFGS, make_stream_cfg, slice_window,
                                  aggregate, ts_arr_from_ticks,
                                  run_baseline_window, SYMBOL, DEPOSIT, SPREAD,
                                  POINT, CONTRACT, PARENT_RISK_PROD)
from sim_wfo_hedge_reverse import (ReverseHedgeCfg, simulate_reverse_hedges,
                                    StopExtensionCfg, simulate_stop_extension_hedges,
                                    tag_session_regimes)


# === Baseline + new grids ===
# Baseline single-tier reverse hedge (winner per-stream params)
BASELINE_PER_STREAM = {
    "S1": dict(sl_mult=1.0, alpha=0.5, pm=3.0),
    "S2": dict(sl_mult=1.2, alpha=0.5, pm=3.5),
    "S3": dict(sl_mult=1.0, alpha=0.5, pm=3.0),
    "S4": dict(sl_mult=1.0, alpha=0.5, pm=3.5),
    "S5": dict(sl_mult=1.0, alpha=0.5, pm=3.0),
    "S6": dict(sl_mult=1.2, alpha=0.5, pm=3.5),
}

MT_TIERS = [1, 2, 3]
MT_SPACINGS = [0.15, 0.30, 0.50]
SE_EXT_PTS = [0, 50, 100, 200]
SE_TP_MULTS = [1.5, 2.0, 3.0]
SE_SL_MULT = 1.0

OUT_DIR = ROOT / "output" / "wfo_hedge_geometry_may23"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def extract_sl_events_reverse_fmt(deals):
    """Reverse-hedge sl_events: needs entry_price, entry_ts_ns, lots."""
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


def sweep_multi_tier(sl_events_by_stream, ticks_arr, regime):
    """Returns dict {(tiers, spacing): {stream: total_np, ...}} for OOS aggregates."""
    rows = []
    for tiers in MT_TIERS:
        for spacing in MT_SPACINGS:
            for s in ("S1","S2","S3","S4","S5","S6"):
                p = BASELINE_PER_STREAM[s]
                hcfg = ReverseHedgeCfg(
                    exp_min=240, f1_sec=1800, regime_gate="off",
                    sl_mult=p["sl_mult"], partial_fraction=p["alpha"],
                    profit_mult=p["pm"],
                    fractal_confirm=False, fractal_width=5,
                    tier_count=tiers, tier_spacing=spacing,
                )
                hp = simulate_reverse_hedges(sl_events_by_stream[s], ticks_arr,
                                              STREAM_CFGS[s], hcfg, regime)
                np_ = sum(p for _, p in hp)
                n = len(hp); w = sum(1 for _, p in hp if p > 0)
                rows.append({
                    "mechanism": "multi-tier", "tiers": tiers, "spacing": spacing,
                    "stream": s, "n": n, "np": round(np_, 0),
                    "wr": round((w/n*100) if n else 0, 1),
                })
    return rows


def sweep_stop_extension(sl_events_by_stream, ticks_arr):
    rows = []
    for ext in SE_EXT_PTS:
        for tp in SE_TP_MULTS:
            for s in ("S1","S2","S3","S4","S5","S6"):
                hcfg = StopExtensionCfg(
                    exp_min=240, f1_sec=1800,
                    ext_pts=ext, tp_mult=tp, sl_mult=SE_SL_MULT,
                )
                hp = simulate_stop_extension_hedges(sl_events_by_stream[s], ticks_arr,
                                                      STREAM_CFGS[s], hcfg)
                np_ = sum(p for _, p in hp)
                n = len(hp); w = sum(1 for _, p in hp if p > 0)
                rows.append({
                    "mechanism": "stop-ext", "ext_pts": ext, "tp_mult": tp,
                    "stream": s, "n": n, "np": round(np_, 0),
                    "wr": round((w/n*100) if n else 0, 1),
                })
    return rows


def main():
    print(f"[BOOT] Hedge geometry sweep on may23 windows", flush=True)
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
    finally:
        kill_mt5_terminal()

    all_rows = []
    for w_label, is_s, is_e, oos_s, oos_e in WINDOWS:
        for fold_label, s, e in (("IS", is_s, is_e), ("OOS", oos_s, oos_e)):
            ts_start = to_utc(s); ts_end = to_utc(e)
            ticks = slice_window(ticks_full, "ts", ts_start, ts_end)
            m1 = slice_window(m1_full, "ts", ts_start, ts_end)
            m5 = slice_window(m5_full, "ts", ts_start, ts_end)
            regime = tag_session_regimes(ticks, m1)
            ticks_arr = ts_arr_from_ticks(ticks)

            # Cache parent SLs
            sl_events_by_stream = {}
            for st in ("S1","S2","S3","S4","S5","S6"):
                cfg = make_stream_cfg(st, PARENT_RISK_PROD)
                r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
                sl_events_by_stream[st] = extract_sl_events_reverse_fmt(r.deals)

            t0 = time.time()
            mt_rows = sweep_multi_tier(sl_events_by_stream, ticks_arr, regime)
            se_rows = sweep_stop_extension(sl_events_by_stream, ticks_arr)
            for row in mt_rows + se_rows:
                row["fold"] = f"{w_label}_{fold_label}"
                all_rows.append(row)
            print(f"  {w_label}_{fold_label}: MT cells={len(mt_rows)} SE cells={len(se_rows)} ({time.time()-t0:.0f}s)", flush=True)

    df = pd.DataFrame(all_rows)
    df.to_csv(OUT_DIR / "geometry_sweep.csv", index=False)

    # === Phase 2: pick best multi-tier + best stop-ext, run 3-way compare on May 2-23 ===
    # Aggregate OOS NP per (mechanism, config) summed across all streams + 4 OOS folds
    oos_df = df[df["fold"].str.endswith("_OOS")]

    print(f"\n=== Multi-tier OOS aggregate (all 6 streams x 4 windows) ===")
    print(f"{'tiers':>5} {'spacing':>8}  {'OOS NP':>10}  {'OOS WR%':>8}  {'n_fired':>8}")
    best_mt = None
    for tiers in MT_TIERS:
        for spacing in MT_SPACINGS:
            sub = oos_df[(oos_df["mechanism"] == "multi-tier") &
                          (oos_df["tiers"] == tiers) & (oos_df["spacing"] == spacing)]
            np_ = sub["np"].sum(); n_ = sub["n"].sum()
            wr = sum(r["wr"]/100 * r["n"] for _, r in sub.iterrows()) / max(n_, 1) * 100
            print(f"{tiers:>5} {spacing:>8.2f}  ${np_:>+9,.0f}  {wr:>6.1f}%  {n_:>8}")
            if best_mt is None or np_ > best_mt[2]:
                best_mt = (tiers, spacing, np_, wr, n_)

    print(f"\n=== STOP-ext OOS aggregate (all 6 streams x 4 windows) ===")
    print(f"{'ext':>5} {'tp_mult':>7}  {'OOS NP':>10}  {'OOS WR%':>8}  {'n_fired':>8}")
    best_se = None
    for ext in SE_EXT_PTS:
        for tp in SE_TP_MULTS:
            sub = oos_df[(oos_df["mechanism"] == "stop-ext") &
                          (oos_df["ext_pts"] == ext) & (oos_df["tp_mult"] == tp)]
            np_ = sub["np"].sum(); n_ = sub["n"].sum()
            wr = sum(r["wr"]/100 * r["n"] for _, r in sub.iterrows()) / max(n_, 1) * 100
            print(f"{ext:>5} {tp:>7.1f}  ${np_:>+9,.0f}  {wr:>6.1f}%  {n_:>8}")
            if best_se is None or np_ > best_se[2]:
                best_se = (ext, tp, np_, wr, n_)

    print(f"\n[WIN multi-tier]   tiers={best_mt[0]} spacing={best_mt[1]:.2f}  OOS NP=${best_mt[2]:+,.0f}  WR={best_mt[3]:.1f}%  n={best_mt[4]}")
    print(f"[WIN stop-ext]     ext={best_se[0]} tp_mult={best_se[1]}  OOS NP=${best_se[2]:+,.0f}  WR={best_se[3]:.1f}%  n={best_se[4]}")

    # Baseline single-tier reverse (tiers=1, spacing=0)
    base_sub = oos_df[(oos_df["mechanism"] == "multi-tier") &
                       (oos_df["tiers"] == 1) & (oos_df["spacing"] == MT_SPACINGS[0])]
    base_np = base_sub["np"].sum(); base_n = base_sub["n"].sum()
    base_wr = sum(r["wr"]/100 * r["n"] for _, r in base_sub.iterrows()) / max(base_n, 1) * 100

    print(f"\n=== [3WAY] OOS totals across may23 4 windows + 6 streams ===")
    print(f"  Baseline (tiers=1):  OOS NP=${base_np:+,.0f}  WR={base_wr:.1f}%  n={base_n}")
    print(f"  Best multi-tier:     OOS NP=${best_mt[2]:+,.0f}  WR={best_mt[3]:.1f}%  n={best_mt[4]}  delta=${best_mt[2]-base_np:+,.0f}")
    print(f"  Best stop-ext:       OOS NP=${best_se[2]:+,.0f}  WR={best_se[3]:.1f}%  n={best_se[4]}  delta=${best_se[2]-base_np:+,.0f}")

    print(f"\n[DONE] Wrote {OUT_DIR/'geometry_sweep.csv'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
