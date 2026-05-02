"""WFO for EMAPullback at 70pt spread, 3% risk, $10k.

Smoke winner anchor: EMA=50, Look=3, Band=100, SLBuf=50, RR=2.0 -> NP +$4,999, NP/DD 1.86.
This WFO finds robust params around that point.

Grid (768 combos):
  ema_period:       21, 34, 50, 100        (4)
  lookback_bars:    3, 5                    (2)
  pullback_band_pts: 50, 100, 150           (3)
  sl_buffer_pts:    30, 50, 100             (3)
  rr_ratio:         1.5, 2.0, 2.5           (3)
  daily_target_pct: 0, 9                    (2) — 0 disables to test caps-off
  daily_loss_pct:   0, 6                    (2)
  -> 4*2*3*3*3*2*2 = 864
Window: Feb 14 -> Apr 25, 3 IS/OOS folds.
"""
from __future__ import annotations

import argparse
import sys
import time
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.ema_pullback import EMAPullbackConfig
from zgb_sim.ema_pullback_fast import simulate_fast as ep_simulate
from zgb_sim.sweep_ema_pullback import run_sweep


SYMBOL = "XAUUSD"
RISK_PCT = 3.0
DEPOSIT = 10_000.0
N_WORKERS = 6
SIGNAL_TF = "M15"

WINDOWS = [
    ("W1", date(2026, 2, 14), date(2026, 3, 14), date(2026, 3, 14), date(2026, 3, 28)),
    ("W2", date(2026, 2, 28), date(2026, 3, 28), date(2026, 3, 28), date(2026, 4, 11)),
    ("W3", date(2026, 3, 14), date(2026, 4, 11), date(2026, 4, 11), date(2026, 4, 25)),
]
PREWARM_START = date(2026, 2, 12)
PREWARM_END   = date(2026, 4, 25)

OUT_DIR = ROOT / "output" / "sim_wfo_ema_pullback_spread70"


def build_grid(tiny=False) -> list[EMAPullbackConfig]:
    if tiny:
        return [EMAPullbackConfig(
            risk_pct=RISK_PCT, signal_tf_minutes=15, ema_period=50,
            lookback_bars=3, pullback_band_pts=100,
            entry_buffer_pts=0, sl_buffer_pts=50,
            rr_ratio=2.0, half_tp_ratio=0.0,
            pending_expire_bars=3,
            daily_target_pct=0.0, daily_loss_pct=0.0,
            comment="EMAPullback",
        )]
    grid = []
    for ema_p in (21, 34, 50, 100):
        for look in (3, 5):
            for band in (50, 100, 150):
                for sl_buf in (30, 50, 100):
                    for rr in (1.5, 2.0, 2.5):
                        for tgt in (0.0, 9.0):
                            for loss in (0.0, 6.0):
                                grid.append(EMAPullbackConfig(
                                    risk_pct=RISK_PCT,
                                    signal_tf_minutes=15,
                                    ema_period=ema_p,
                                    lookback_bars=look,
                                    pullback_band_pts=band,
                                    entry_buffer_pts=0,
                                    sl_buffer_pts=sl_buf,
                                    rr_ratio=rr,
                                    half_tp_ratio=0.0,
                                    pending_expire_bars=3,
                                    daily_target_pct=tgt,
                                    daily_loss_pct=loss,
                                    comment="EMAPullback",
                                ))
    return grid


def _to_utc(d): return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _param_key(row):
    return (
        int(row["ema_period"]), int(row["lookback_bars"]),
        int(row["pullback_band_pts"]), int(row["sl_buffer_pts"]),
        round(float(row["rr_ratio"]), 2),
        round(float(row["daily_target_pct"]), 2),
        round(float(row["daily_loss_pct"]), 2),
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
              f"EMA={int(r['ema_period'])} Look={int(r['lookback_bars'])} "
              f"Band={int(r['pullback_band_pts'])} SL={int(r['sl_buffer_pts'])} "
              f"RR={r['rr_ratio']} Tgt={r['daily_target_pct']} Loss={r['daily_loss_pct']}")


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
        cands.append(EMAPullbackConfig(
            risk_pct=RISK_PCT, signal_tf_minutes=15,
            ema_period=int(r["ema_period"]),
            lookback_bars=int(r["lookback_bars"]),
            pullback_band_pts=int(r["pullback_band_pts"]),
            entry_buffer_pts=0,
            sl_buffer_pts=int(r["sl_buffer_pts"]),
            rr_ratio=float(r["rr_ratio"]),
            half_tp_ratio=0.0,
            pending_expire_bars=3,
            daily_target_pct=float(r["daily_target_pct"]),
            daily_loss_pct=float(r["daily_loss_pct"]),
            comment="EMAPullback",
        ))
        print(f"    #{len(cands)} EMA={k[0]} Look={k[1]} Band={k[2]} SL={k[3]} "
              f"RR={k[4]} Tgt={k[5]} Loss={k[6]}  windows={info['windows']}")
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


def rank_oos(candidates, oos_per_window):
    rows = []
    for i, cfg in enumerate(candidates):
        total_np = 0.0
        dds = []
        prof_count = 0
        for label, _, _, _, _ in WINDOWS:
            r = oos_per_window[label].iloc[i]
            total_np += float(r["net_profit"])
            dds.append(float(r["drawdown_pct"]))
            if r["net_profit"] > 0: prof_count += 1
        avg_dd = sum(dds) / len(dds) if dds else 0.5
        np_dd = total_np / max(avg_dd, 0.5)
        rows.append({"cfg": cfg, "total_np": total_np, "prof_count": prof_count,
                     "avg_dd": avg_dd, "np_dd_ratio": np_dd})
    rows.sort(key=lambda x: (x["prof_count"], x["np_dd_ratio"]), reverse=True)
    return rows


def sanity_et(cfg: EMAPullbackConfig, meta: SymbolMeta):
    print("\n" + "=" * 78)
    print("  SANITY ET (continuous Mar 14 -> Apr 25, $10k, 3% risk, spread=70)")
    print("=" * 78)
    full_start = _to_utc(date(2026, 3, 14))
    full_end = _to_utc(date(2026, 4, 25))
    ticks = load_ticks(SYMBOL, full_start, full_end)
    m1 = load_bars(SYMBOL, "M1", full_start, full_end)
    m15 = load_bars(SYMBOL, "M15", full_start, full_end)
    r = ep_simulate(ticks, m15, m1, cfg, meta, initial_balance=DEPOSIT)
    days = (full_end - full_start).days
    print(f"\n  Sanity ({days}d): NP=${r.net_profit:+,.0f}  ROI={r.net_profit/DEPOSIT*100:+.1f}%  "
          f"DD={r.max_drawdown_pct:.1f}%  Trades={r.trades}  PF={r.profit_factor:.2f}  "
          f"TP/SL/O={r.tp_count}/{r.sl_count}/{r.other_count}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiny", action="store_true")
    args = ap.parse_args()
    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        print("=" * 84)
        configs = build_grid(tiny=args.tiny)
        print(f"  PYSIM WFO EMAPullback (3% risk, $10k, spread=70) -- {len(configs)} combos")
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

        print("\n=== PHASE D: Final Ranking ===")
        ranked = rank_oos(candidates, oos_per)
        for rank, row in enumerate(ranked, 1):
            cfg = row["cfg"]
            print(f"  #{rank}  NP=${row['total_np']:+,.0f}  AvgDD={row['avg_dd']:.1f}%  "
                  f"NP/DD={row['np_dd_ratio']:+.0f}  Prof={row['prof_count']}/3  "
                  f"EMA={cfg.ema_period} Look={cfg.lookback_bars} "
                  f"Band={cfg.pullback_band_pts} SL={cfg.sl_buffer_pts} "
                  f"RR={cfg.rr_ratio} Tgt={cfg.daily_target_pct} Loss={cfg.daily_loss_pct}")

        winner = ranked[0]["cfg"]
        sanity_et(winner, meta)
        print("\n=== Done ===")
        print(f"\n  WINNER: EMA={winner.ema_period} Look={winner.lookback_bars} "
              f"Band={winner.pullback_band_pts} SL={winner.sl_buffer_pts} "
              f"RR={winner.rr_ratio} Tgt={winner.daily_target_pct} Loss={winner.daily_loss_pct}")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
