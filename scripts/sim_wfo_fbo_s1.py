"""WFO for FBO_S1 (M30 fractal) at 70pt spread, 3% risk, $10k.

Current live params (anchor):
  TF=30 (M30), TP=25,000, SL=10,000, Bars=8, SMA=10, HalfTP=0.3, expire=2 bars

Grid (~720 combos, 4 windows = 2,880 sims):
  take_profit_pts:   15000, 20000, 25000, 30000          (4)
  stop_loss_pts:     7500, 10000, 12500                  (3)
  fractal_bars:      6, 8, 10                            (3)
  sma_period:        5, 10, 15                           (3)
  half_tp_ratio:     0.0, 0.3, 0.5                       (3)
  pending_expire_bars: 2                                  (1, fixed)
  -> 4*3*3*3*3 = 324 combos
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
from zgb_sim.fbo_s1 import FBOS1Config
from zgb_sim.fbo_s1_fast import simulate_fast as fbo_simulate
from zgb_sim.sweep_fbo import run_sweep
from zgb_sim.wfo_helpers import WINDOWS_MAY2 as WINDOWS, rank_with_p0, print_phase_d_with_p0, select_winner_with_p0


SYMBOL = "XAUUSD"
RISK_PCT = 3.0
DEPOSIT = 10_000.0
N_WORKERS = 6
SIGNAL_TF = "M30"
COMMENT = "FBO_A"
PENDING_EXPIRE_BARS = 2

PREWARM_START = date(2026, 2, 12)
PREWARM_END   = date(2026, 5,  1)

OUT_DIR = ROOT / "output" / "wfo_fbo_s1_may2"


def build_grid(tiny=False) -> list[FBOS1Config]:
    if tiny:
        return [FBOS1Config(
            risk_pct=RISK_PCT, fractal_bars=8, take_profit_pts=25_000,
            stop_loss_pts=10_000, half_tp_ratio=0.3, sma_period=10,
            pending_expire_bars=PENDING_EXPIRE_BARS, signal_tf_minutes=30, comment=COMMENT,
        )]
    grid = []
    for tp in (15000, 20000, 25000, 30000):
        for sl in (7500, 10000, 12500):
            for bars in (6, 8, 10):
                for sma in (5, 10, 15):
                    for htp in (0.0, 0.3, 0.5):
                        grid.append(FBOS1Config(
                            risk_pct=RISK_PCT,
                            fractal_bars=bars,
                            take_profit_pts=tp,
                            stop_loss_pts=sl,
                            half_tp_ratio=htp,
                            sma_period=sma,
                            pending_expire_bars=PENDING_EXPIRE_BARS,
                            signal_tf_minutes=30,
                            comment=COMMENT,
                        ))
    return grid


def _to_utc(d): return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _param_key(row):
    return (
        int(row["take_profit_pts"]), int(row["stop_loss_pts"]),
        int(row["fractal_bars"]), int(row["sma_period"]),
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
    p = df[(df["net_profit"] > 0) & (df["trades"] >= 5) & df["error"].isna()]
    top = p.sort_values("recovery_factor", ascending=False).head(5)
    print(f"\n  {label}: top-5 by RF (of {len(p)} profitable):")
    for _, r in top.iterrows():
        print(f"    NP=${r['net_profit']:>+8,.0f}  DD={r['drawdown_pct']:>4.1f}%  "
              f"Tr={int(r['trades']):>3}  RF={r['recovery_factor']:>5.0f}  "
              f"TP={int(r['take_profit_pts'])} SL={int(r['stop_loss_pts'])} "
              f"Bars={int(r['fractal_bars'])} SMA={int(r['sma_period'])} HTP={r['half_tp_ratio']}")


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
        if np_key in seen:
            continue
        seen.add(np_key)
        unique.append((k, info))
        if len(unique) >= max_candidates:
            break

    cands = []
    for k, info in unique:
        r = info["sample_row"]
        cands.append(FBOS1Config(
            risk_pct=RISK_PCT, signal_tf_minutes=30,
            take_profit_pts=int(r["take_profit_pts"]),
            stop_loss_pts=int(r["stop_loss_pts"]),
            fractal_bars=int(r["fractal_bars"]),
            sma_period=int(r["sma_period"]),
            half_tp_ratio=round(float(r["half_tp_ratio"]), 2),
            pending_expire_bars=PENDING_EXPIRE_BARS,
            comment=COMMENT,
        ))
        print(f"    #{len(cands)} TP={k[0]} SL={k[1]} Bars={k[2]} SMA={k[3]} HTP={k[4]}  windows={info['windows']}")
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


def sanity_et(cfg: FBOS1Config, meta: SymbolMeta):
    print("\n" + "=" * 72)
    print("  SANITY ET FBO_S1 (continuous Mar 14 -> May 2, $10k, 3% risk, spread=70)")
    print("=" * 72)
    full_start = _to_utc(date(2026, 3, 14))
    full_end = _to_utc(date(2026, 5, 1))
    ticks = load_ticks(SYMBOL, full_start, full_end)
    m1 = load_bars(SYMBOL, "M1", full_start, full_end)
    m30 = load_bars(SYMBOL, "M30", full_start, full_end)
    r = fbo_simulate(ticks, m30, m1, cfg, meta, initial_balance=DEPOSIT)
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
        print(f"  PYSIM WFO FBO_S1 (3% risk, $10k, spread=70) -- {len(configs)} combos")
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
        print_phase_d_with_p0(ranked, "FBO_S1")

        winner_row = select_winner_with_p0(ranked)
        if winner_row is None:
            print(f"\n  WARNING: 0 candidates passed P0. FBO_S1 archetype showing decay.")
            winner_row = ranked[0]
        winner = winner_row["cfg"]
        print(f"\n  WINNER: TP={winner.take_profit_pts} SL={winner.stop_loss_pts} "
              f"Bars={winner.fractal_bars} SMA={winner.sma_period} HTP={winner.half_tp_ratio}  "
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
