"""WFO for GMMA + RSI pullback strategy on XAUUSD.

Multi-TF: H4 trend bias, H1 GMMA+RSI entry. Default 55pt synthetic spread
(SIM_SPREAD_PTS), 3% risk, $10k deposit. 4-fold WFO using WINDOWS_MAY9.

Grid (216 cells × 4 windows = 864 sims):
  entry_mode:       rsi_cross_50, tag_plus_side, rsi_os_reversal (3)
  sl_pts:           400, 600, 900, 1200                          (4)
  rr_ratio:         1.5, 2.0, 3.0                                (3)
  lookback_bars:    3, 6, 12                                     (3)
  h4_trend_required:True, False                                  (2)
  rsi_period:       14                                           (fixed)

Phases A → B → C → D mirror sim_wfo_orb.py.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import (symbol_meta, kill_mt5_terminal, load_ticks,
                                  load_bars, SIM_SPREAD_PTS)
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.gmma import GMMAConfig, simulate, ENTRY_MODE_RSI_CROSS, \
    ENTRY_MODE_TAG_SIDE, ENTRY_MODE_RSI_OS
from zgb_sim.sweep_gmma import run_sweep
from zgb_sim.wfo_helpers import (WINDOWS_MAY9, rank_with_p0,
                                  print_phase_d_with_p0, select_winner_with_p0,
                                  check_winner_boundaries, print_boundary_check)


SYMBOL = "XAUUSD"
RISK_PCT = 3.0
DEPOSIT = 10_000.0
N_WORKERS = 6
WINDOWS = WINDOWS_MAY9
PREWARM_START = date(2026, 2, 19)
PREWARM_END = date(2026, 5, 9)
OUT_DIR = ROOT / "output" / "wfo_gmma_may9"

ENTRY_MODES = (ENTRY_MODE_RSI_CROSS, ENTRY_MODE_TAG_SIDE, ENTRY_MODE_RSI_OS)


def build_grid(tiny: bool = False) -> list[GMMAConfig]:
    if tiny:
        return [GMMAConfig(risk_pct=RISK_PCT, entry_mode=ENTRY_MODE_TAG_SIDE,
                           sl_pts=600, rr_ratio=2.0, lookback_bars=6,
                           h4_trend_required=True, comment="GMMA")]
    grid = []
    for mode in ENTRY_MODES:
        for sl in (400, 600, 900, 1200):
            for rr in (1.5, 2.0, 3.0):
                for lb in (3, 6, 12):
                    for h4 in (True, False):
                        grid.append(GMMAConfig(
                            risk_pct=RISK_PCT,
                            entry_mode=mode,
                            sl_pts=sl,
                            rr_ratio=rr,
                            lookback_bars=lb,
                            h4_trend_required=h4,
                            rsi_period=14,
                            comment="GMMA",
                        ))
    return grid


def _to_utc(d): return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _param_key(row):
    return (
        str(row["entry_mode"]),
        int(row["sl_pts"]),
        round(float(row["rr_ratio"]), 2),
        int(row["lookback_bars"]),
        bool(row["h4_trend_required"]),
    )


def run_is_phase(configs, meta):
    per = {}
    for label, is_s, is_e, _, _ in WINDOWS:
        cache = OUT_DIR / f"is_{label}.parquet"
        df = run_sweep(configs, SYMBOL, _to_utc(is_s), _to_utc(is_e),
                       meta, initial_balance=DEPOSIT, n_workers=N_WORKERS,
                       cache_path=cache, window_label=f"IS-{label}")
        per[label] = df
    return per


def print_top5(df, label):
    p = df[(df["net_profit"] > 0) & (df["trades"] >= 5) & df["error"].isna()]
    top = p.sort_values("recovery_factor", ascending=False).head(5)
    print(f"\n  {label}: top-5 by RF (of {len(p)} profitable, {len(df)} total):")
    for _, r in top.iterrows():
        print(f"    NP=${r['net_profit']:>+8,.0f}  DD={r['drawdown_pct']:>4.1f}%  "
              f"Tr={int(r['trades']):>3}  RF={r['recovery_factor']:>5.0f}  "
              f"mode={r['entry_mode']:<16} SL={int(r['sl_pts'])} RR={r['rr_ratio']} "
              f"LB={int(r['lookback_bars'])} H4={r['h4_trend_required']}")


def select_robust(per_window, top_n=30, max_candidates=15):
    counts = {}
    for label, df in per_window.items():
        prof = df[(df["net_profit"] > 0) & (df["trades"] >= 5) & df["error"].isna()]
        top = prof.sort_values("recovery_factor", ascending=False).head(top_n)
        for _, row in top.iterrows():
            k = _param_key(row)
            c = counts.setdefault(k, {"count": 0, "windows": [], "total_rf": 0.0,
                                      "total_np": 0.0, "sample_row": row})
            c["count"] += 1
            c["windows"].append(label)
            c["total_rf"] += float(row["recovery_factor"])
            c["total_np"] += float(row["net_profit"])

    robust = [(k, info) for k, info in counts.items() if info["count"] >= 2]
    print(f"\n  Robust params (top-{top_n} of 2+ windows): {len(robust)}")
    if not robust:
        print("  No robust. Falling back to top-RF combined.")
        combined = list(counts.items())
        combined.sort(key=lambda x: -x[1]["total_rf"])
        robust = combined[: max_candidates * 2]

    robust.sort(key=lambda x: -x[1]["total_rf"])
    seen = set()
    unique = []
    for k, info in robust:
        np_key = round(info["total_np"])
        if np_key in seen: continue
        seen.add(np_key)
        unique.append((k, info))
        if len(unique) >= max_candidates: break

    cands = []
    for k, info in unique:
        r = info["sample_row"]
        cands.append(GMMAConfig(
            risk_pct=RISK_PCT,
            entry_mode=str(r["entry_mode"]),
            sl_pts=int(r["sl_pts"]),
            rr_ratio=float(r["rr_ratio"]),
            lookback_bars=int(r["lookback_bars"]),
            h4_trend_required=bool(r["h4_trend_required"]),
            rsi_period=int(r["rsi_period"]) if "rsi_period" in r else 14,
            comment="GMMA",
        ))
        print(f"    #{len(cands)} mode={k[0]} SL={k[1]} RR={k[2]} LB={k[3]} "
              f"H4={k[4]}  windows={info['windows']}")
    return cands


def run_oos_phase(candidates, meta):
    per = {}
    for label, _, _, oos_s, oos_e in WINDOWS:
        cache = OUT_DIR / f"oos_{label}.parquet"
        df = run_sweep(candidates, SYMBOL, _to_utc(oos_s), _to_utc(oos_e),
                       meta, initial_balance=DEPOSIT,
                       n_workers=min(N_WORKERS, len(candidates)),
                       cache_path=cache, window_label=f"OOS-{label}")
        per[label] = df
    return per


def sanity_full(cfg: GMMAConfig, meta: SymbolMeta):
    print("\n" + "=" * 72)
    print(f"  SANITY full-period (Feb 14 -> May 9, $10k, 3% risk, {SIM_SPREAD_PTS}pt friction)")
    print("=" * 72)
    full_start = _to_utc(date(2026, 2, 14))
    full_end = _to_utc(date(2026, 5, 9))
    ticks = load_ticks(SYMBOL, full_start, full_end)
    h1 = load_bars(SYMBOL, "H1", full_start, full_end)
    h4 = load_bars(SYMBOL, "H4", full_start, full_end)
    r = simulate(ticks, h1, h4, cfg, meta, initial_balance=DEPOSIT)
    print(f"\n  Sanity: {r.summary()}")
    return r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiny", action="store_true",
                    help="Use 1-cell smoke grid for end-to-end testing")
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    configs = build_grid(tiny=args.tiny)

    print("=" * 84)
    print(f"  WFO GMMA+RSI (XAUUSD)  {len(configs)} configs  4 windows")
    print(f"  {SIM_SPREAD_PTS}pt friction, {RISK_PCT}% risk, ${DEPOSIT:,.0f}")
    print(f"  out_dir: {OUT_DIR}")
    print("=" * 84)

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        print(f"  Workers: {N_WORKERS}")

        print("\n  Pre-warming (sim account, ticks + H1 + H4)...")
        from zgb_sim.mt5_accounts import init_account
        init_account("sim")
        t0 = time.time()
        _ = load_ticks(SYMBOL, _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        _ = load_bars(SYMBOL, "H1", _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        _ = load_bars(SYMBOL, "H4", _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        print(f"  Pre-warm done in {time.time()-t0:.1f}s")

        print("\n=== PHASE A: IS Sweep ===")
        is_per = run_is_phase(configs, meta)
        for label, df in is_per.items():
            print_top5(df, f"IS-{label}")

        print("\n=== PHASE B: Robust ===")
        candidates = select_robust(is_per)
        if not candidates:
            print("No candidates."); return

        print("\n=== PHASE C: OOS ===")
        oos_per = run_oos_phase(candidates, meta)
        for label, df in oos_per.items():
            print(f"\n  OOS-{label}:")
            for i, r in df.iterrows():
                print(f"    #{i+1} NP={r['net_profit']:+,.2f} ({r['return_pct']:+.1f}%)  "
                      f"PF={r['profit_factor']:.2f}  DD={r['drawdown_pct']:.1f}%  "
                      f"Tr={int(r['trades'])}")

        ranked = rank_with_p0(candidates, oos_per, WINDOWS, decay_threshold=-0.25,
                                grid_configs=configs, is_per_window=is_per)
        print_phase_d_with_p0(ranked, "GMMA+RSI")

        winner_row = select_winner_with_p0(ranked)
        if winner_row is None:
            print("\n  WARNING: 0 candidates passed P0. Using top-ranked fallback.")
            winner_row = ranked[0]
        winner = winner_row["cfg"]
        print(f"\n  WINNER: mode={winner.entry_mode}  SL={winner.sl_pts} RR={winner.rr_ratio} "
              f"LB={winner.lookback_bars} H4={winner.h4_trend_required}  "
              f"slope={winner_row['slope']:+.1%}  P0={'PASS' if winner_row['p0_pass'] else 'FAIL'}")
        boundary_rows = check_winner_boundaries(winner, configs)
        boundary_at_edge = print_boundary_check(boundary_rows)

        winner_payload = {
            "cfg": asdict(winner),
            "p0_pass": winner_row["p0_pass"],
            "slope": winner_row["slope"],
            "oos_nps": winner_row["oos_nps"],
            "total_np": winner_row["total_np"],
            "plateau_score": winner_row.get("plateau_score"),
            "n_neighbors": winner_row.get("n_neighbors", 0),
            "boundary_at_edge": boundary_at_edge,
            "boundary_rows": boundary_rows,
        }
        (OUT_DIR / "winner.json").write_text(
            json.dumps(winner_payload, indent=2, default=str), encoding="utf-8")
        print(f"  Winner persisted: {OUT_DIR / 'winner.json'}")

        sanity_full(winner, meta)
        print("\n=== Done ===")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
