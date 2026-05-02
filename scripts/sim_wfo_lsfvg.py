"""WFO for LSFVG at 70pt spread, 3% risk, $10k.

Current live params (anchor):
  lookback_bars=10, min_fvg_pts=20, max_fvg_pts=5000, sweep_buffer_pts=30,
  RR=2.0, HalfTP=0.5, expire=4 bars, sweep_window_bars=5

Grid (~324 combos × 4 windows = 1,296 sims):
  lookback_bars:       8, 10, 15                  (3)
  min_fvg_pts:         15, 20, 30                 (3)
  sweep_buffer_pts:    20, 30, 50                 (3)
  rr_ratio:            1.5, 2.0, 2.5              (3)
  half_tp_ratio:       0.0, 0.3, 0.5, 0.7         (4)
  -> 3*3*3*3*4 = 324
"""
from __future__ import annotations

import argparse
import sys
import time
import json
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.lsfvg import LSFVGConfig
from zgb_sim.lsfvg_fast import simulate_fast as lsfvg_simulate
from zgb_sim.sweep_lsfvg import run_sweep
from zgb_sim.wfo_helpers import WINDOWS_MAY2 as WINDOWS, rank_with_p0, print_phase_d_with_p0, select_winner_with_p0


SYMBOL = "XAUUSD"
RISK_PCT = 3.0
DEPOSIT = 10_000.0
N_WORKERS = 6
SIGNAL_TF = "M15"
COMMENT = "LSFVG"
PENDING_EXPIRE_BARS = 4

PREWARM_START = date(2026, 2, 12)
PREWARM_END   = date(2026, 5,  1)

OUT_DIR = ROOT / "output" / "wfo_lsfvg_may2"


def build_grid(tiny=False) -> list[LSFVGConfig]:
    if tiny:
        return [LSFVGConfig(
            risk_pct=RISK_PCT, signal_tf_minutes=15, lookback_bars=10,
            min_fvg_pts=20, max_fvg_pts=5000, sweep_buffer_pts=30,
            rr_ratio=2.0, half_tp_ratio=0.5, pending_expire_bars=PENDING_EXPIRE_BARS,
            daily_target_pct=0.0, daily_loss_pct=0.0, comment=COMMENT,
        )]
    grid = []
    for lookback in (8, 10, 15):
        for min_fvg in (15, 20, 30):
            for sweep_buf in (20, 30, 50):
                for rr in (1.5, 2.0, 2.5):
                    for htp in (0.0, 0.3, 0.5, 0.7):
                        grid.append(LSFVGConfig(
                            risk_pct=RISK_PCT,
                            signal_tf_minutes=15,
                            lookback_bars=lookback,
                            min_fvg_pts=min_fvg,
                            max_fvg_pts=5000,
                            sweep_buffer_pts=sweep_buf,
                            rr_ratio=rr,
                            half_tp_ratio=htp,
                            pending_expire_bars=PENDING_EXPIRE_BARS,
                            daily_target_pct=0.0, daily_loss_pct=0.0,
                            comment=COMMENT,
                        ))
    return grid


def _to_utc(d): return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _param_key(row):
    return (
        int(row["lookback_bars"]), int(row["min_fvg_pts"]),
        int(row["sweep_buffer_pts"]), round(float(row["rr_ratio"]), 2),
        round(float(row["half_tp_ratio"]), 2),
    )


def run_is_phase(configs, meta):
    per = {}
    for label, is_s, is_e, _, _ in WINDOWS:
        cache = OUT_DIR / f"is_{label}.parquet"
        df = run_sweep(configs, SYMBOL, _to_utc(is_s), _to_utc(is_e), meta,
                       initial_balance=DEPOSIT, n_workers=N_WORKERS,
                       cache_path=cache, window_label=f"IS-{label}", signal_tf=SIGNAL_TF)
        per[label] = df
    return per


def print_top5(df, label):
    p = df[(df["net_profit"] > 0) & (df["trades"] >= 3) & df["error"].isna()]
    top = p.sort_values("recovery_factor", ascending=False).head(5)
    print(f"\n  {label}: top-5 by RF (of {len(p)} profitable):")
    for _, r in top.iterrows():
        print(f"    NP=${r['net_profit']:>+8,.0f}  DD={r['drawdown_pct']:>4.1f}%  "
              f"Tr={int(r['trades']):>3}  RF={r['recovery_factor']:>5.0f}  "
              f"Look={int(r['lookback_bars'])} MinFVG={int(r['min_fvg_pts'])} "
              f"SwpBuf={int(r['sweep_buffer_pts'])} RR={r['rr_ratio']} HTP={r['half_tp_ratio']}")


def select_robust(per_window, top_n=20, max_candidates=12):
    counts = {}
    for label, df in per_window.items():
        prof = df[(df["net_profit"] > 0) & (df["trades"] >= 3) & df["error"].isna()]
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
        if np_key in seen:
            continue
        seen.add(np_key)
        unique.append((k, info))
        if len(unique) >= max_candidates:
            break

    cands = []
    for k, info in unique:
        r = info["sample_row"]
        cands.append(LSFVGConfig(
            risk_pct=RISK_PCT, signal_tf_minutes=15,
            lookback_bars=int(r["lookback_bars"]),
            min_fvg_pts=int(r["min_fvg_pts"]),
            max_fvg_pts=5000,
            sweep_buffer_pts=int(r["sweep_buffer_pts"]),
            rr_ratio=float(r["rr_ratio"]),
            half_tp_ratio=round(float(r["half_tp_ratio"]), 2),
            pending_expire_bars=PENDING_EXPIRE_BARS,
            daily_target_pct=0.0, daily_loss_pct=0.0,
            comment=COMMENT,
        ))
        print(f"    #{len(cands)} Look={k[0]} MinFVG={k[1]} SwpBuf={k[2]} "
              f"RR={k[3]} HTP={k[4]}  windows={info['windows']}")
    return cands


def run_oos_phase(candidates, meta):
    per = {}
    for label, _, _, oos_s, oos_e in WINDOWS:
        cache = OUT_DIR / f"oos_{label}.parquet"
        df = run_sweep(candidates, SYMBOL, _to_utc(oos_s), _to_utc(oos_e), meta,
                       initial_balance=DEPOSIT,
                       n_workers=min(N_WORKERS, len(candidates)),
                       cache_path=cache, window_label=f"OOS-{label}", signal_tf=SIGNAL_TF)
        per[label] = df
    return per


def sanity_et(cfg: LSFVGConfig, meta: SymbolMeta):
    print("\n" + "=" * 72)
    print("  SANITY ET LSFVG (Mar 14 -> May 1, $10k, 3% risk, spread=70)")
    print("=" * 72)
    full_start = _to_utc(date(2026, 3, 14))
    full_end = _to_utc(date(2026, 5, 1))
    ticks = load_ticks(SYMBOL, full_start, full_end)
    m1 = load_bars(SYMBOL, "M1", full_start, full_end)
    m15 = load_bars(SYMBOL, "M15", full_start, full_end)
    r = lsfvg_simulate(ticks, m15, m1, cfg, meta, initial_balance=DEPOSIT)
    days = (full_end - full_start).days
    print(f"  Sanity ({days}d): NP=${r.net_profit:+,.0f}  ROI={r.net_profit/DEPOSIT*100:+.1f}%  "
          f"DD={r.max_drawdown_pct:.1f}%  Trades={r.trades}  PF={r.profit_factor:.2f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiny", action="store_true")
    args = ap.parse_args()
    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        configs = build_grid(tiny=args.tiny)
        print("=" * 84)
        print(f"  PYSIM WFO LSFVG (3% risk, $10k, spread=70) -- {len(configs)} combos")
        print("=" * 84)

        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        print(f"  Workers: {N_WORKERS}  Deposit: ${DEPOSIT:,.0f}  Risk: {RISK_PCT}%")

        print("\n  Pre-warming...")
        t0 = time.time()
        _ = load_ticks(SYMBOL, _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        _ = load_bars(SYMBOL, "M1", _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        _ = load_bars(SYMBOL, SIGNAL_TF, _to_utc(PREWARM_START), _to_utc(PREWARM_END))
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
                      f"PF={r['profit_factor']:.2f}  DD={r['drawdown_pct']:.1f}%  Tr={int(r['trades'])}")

        ranked = rank_with_p0(candidates, oos_per, WINDOWS, decay_threshold=-0.25)
        print_phase_d_with_p0(ranked, "LSFVG")

        winner_row = select_winner_with_p0(ranked)
        if winner_row is None:
            winner_row = ranked[0]
        winner = winner_row["cfg"]
        print(f"\n  WINNER: Look={winner.lookback_bars} MinFVG={winner.min_fvg_pts} "
              f"SwpBuf={winner.sweep_buffer_pts} RR={winner.rr_ratio} HTP={winner.half_tp_ratio}  "
              f"slope={winner_row['slope']:+.1%}  P0={'PASS' if winner_row['p0_pass'] else 'FAIL'}")

        winner_path = OUT_DIR / "winner.json"
        winner_path.write_text(json.dumps({
            "cfg": asdict(winner), "p0_pass": winner_row["p0_pass"],
            "slope": winner_row["slope"], "oos_nps": winner_row["oos_nps"],
            "total_np": winner_row["total_np"],
        }, indent=2, default=str), encoding="utf-8")
        print(f"  Winner persisted: {winner_path}")
        sanity_et(winner, meta)
        print("\n=== Done ===")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
