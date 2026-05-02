"""WFO for LSFVG (Liquidity Sweep + FVG) at $10k / 3% risk, ending Apr 25.

Same Phase A/B/C/D structure as FBO/ORB WFO scripts, adjusted for LSFVG's
sparse trade frequency (~10-15 trades per 4-week window).

Phase A: IS sweep on 3 rolling windows (4w IS / 2w OOS, ending 2026-04-25).
Phase B: top-30 robust filter, dedupe by total NP, max 12 candidates.
Phase C: OOS validation on those 12.
Phase D: rank by (prof_count desc, NP/AvgDD desc), write winner setfile.

Filtered to >= 5 trades per IS window (instead of ORB's 10) — LSFVG is rare.
"""
from __future__ import annotations

import argparse
import sys
import time
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.lsfvg import LSFVGConfig
from zgb_sim.lsfvg_fast import simulate_fast
from zgb_sim.sweep_lsfvg import run_sweep


SYMBOL = "XAUUSD"
RISK_PCT = 3.0
DEPOSIT = 10_000.0
N_WORKERS = 4

WINDOWS = [
    ("W1", date(2026, 2, 14), date(2026, 3, 14), date(2026, 3, 14), date(2026, 3, 28)),
    ("W2", date(2026, 2, 28), date(2026, 3, 28), date(2026, 3, 28), date(2026, 4, 11)),
    ("W3", date(2026, 3, 14), date(2026, 4, 11), date(2026, 4, 11), date(2026, 4, 25)),
]

PREWARM_START = date(2026, 2, 1)
PREWARM_END   = date(2026, 4, 25)

OUT_DIR = ROOT / "output" / "sim_wfo_lsfvg_apr25"
SET_OUT = ROOT / "configs" / "sets" / "lsfvg_3pct_apr25_reopt_may9.set"

MIN_TRADES_IS = 5    # Each IS window must have >= 5 trades to be considered
MIN_TRADES_OOS = 1   # OOS window can be 0-trade (sparse strategy)


def build_config_grid(tiny: bool = False) -> list[LSFVGConfig]:
    """Grid centered on the smoke-test winner: lookback=10, FVG=[20,5000], RR=2, HTP=0.5.

    Sweep around it:
      lookback : 8, 10, 15, 20         (4)
      min_fvg  : 20, 50, 100           (3)
      max_fvg  : 3000, 5000, 8000      (3)
      sweep_buf: 10, 30, 60            (3)
      rr       : 1.5, 2.0, 3.0         (3)
      htp      : 0.0, 0.3, 0.5         (3)
      peb      : 3, 5                  (2)
    Total: 4*3*3*3*3*3*2 = 1944. Too many. Trim:
      lookback : 8, 10, 15             (3)
      min_fvg  : 20, 50                (2)
      max_fvg  : 3000, 5000            (2)
      sweep_buf: 30, 60                (2)
      rr       : 1.5, 2.0, 3.0         (3)
      htp      : 0.0, 0.5              (2)
      peb      : 4                     (1)
    Total: 3*2*2*2*3*2*1 = 144. ~3-5min sim each → ~4 hr total per window.
    Manageable.
    """
    if tiny:
        return [LSFVGConfig(
            risk_pct=RISK_PCT, signal_tf_minutes=15, lookback_bars=10,
            min_fvg_pts=20, max_fvg_pts=5000, sweep_buffer_pts=30,
            rr_ratio=2.0, half_tp_ratio=0.5, pending_expire_bars=4,
        )]

    lookback_vals = (8, 10, 15)
    min_fvg_vals = (20, 50)
    max_fvg_vals = (3000, 5000)
    sweep_buf_vals = (30, 60)
    rr_vals = (1.5, 2.0, 3.0)
    htp_vals = (0.0, 0.5)

    grid = []
    for lk in lookback_vals:
        for mn in min_fvg_vals:
            for mx in max_fvg_vals:
                for sb in sweep_buf_vals:
                    for rr in rr_vals:
                        for htp in htp_vals:
                            grid.append(LSFVGConfig(
                                risk_pct=RISK_PCT,
                                signal_tf_minutes=15,
                                lookback_bars=lk,
                                min_fvg_pts=mn,
                                max_fvg_pts=mx,
                                sweep_buffer_pts=sb,
                                rr_ratio=rr,
                                half_tp_ratio=htp,
                                pending_expire_bars=4,
                                comment="LSFVG",
                            ))
    return grid


def _to_utc(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _param_key(row) -> tuple:
    return (
        int(row["lookback_bars"]),
        int(row["min_fvg_pts"]),
        int(row["max_fvg_pts"]),
        int(row["sweep_buffer_pts"]),
        round(float(row["rr_ratio"]), 2),
        round(float(row["half_tp_ratio"]), 2),
        int(row["pending_expire_bars"]),
    )


def run_is_phase(configs, meta) -> dict[str, pd.DataFrame]:
    per_window = {}
    for win_label, is_start, is_end, _, _ in WINDOWS:
        cache = OUT_DIR / f"is_{win_label}.parquet"
        df = run_sweep(
            configs, SYMBOL,
            _to_utc(is_start), _to_utc(is_end),
            meta, initial_balance=DEPOSIT,
            n_workers=N_WORKERS,
            cache_path=cache,
            window_label=f"IS-{win_label}",
            signal_tf="M15",
        )
        per_window[win_label] = df
    return per_window


def print_top5(df: pd.DataFrame, label: str):
    profitable = df[(df["net_profit"] > 0) & (df["trades"] >= MIN_TRADES_IS)
                    & df["error"].isna()]
    top = profitable.sort_values("recovery_factor", ascending=False).head(5)
    print(f"\n  {label}: top-5 by RF (of {len(profitable)} profitable, "
          f"{len(df)} total):")
    print(f"    {'NP':>9} {'PF':>5} {'DD%':>5} {'Tr':>3} {'RF':>6} | "
          f"{'Lk':>3} {'MnFVG':>5} {'MxFVG':>5} {'SwBuf':>5} {'RR':>4} {'HTP':>4}")
    for _, r in top.iterrows():
        print(f"    {r['net_profit']:>+9,.0f} {r['profit_factor']:>5.2f} "
              f"{r['drawdown_pct']:>4.1f}% {int(r['trades']):>3} "
              f"{r['recovery_factor']:>6.0f} | "
              f"{int(r['lookback_bars']):>3} {int(r['min_fvg_pts']):>5} "
              f"{int(r['max_fvg_pts']):>5} {int(r['sweep_buffer_pts']):>5} "
              f"{r['rr_ratio']:>4.1f} {r['half_tp_ratio']:>4.1f}")


def select_robust(per_window, top_n: int = 30, max_candidates: int = 12) -> list[LSFVGConfig]:
    counts = {}
    for win_label, df in per_window.items():
        profitable = df[(df["net_profit"] > 0) & (df["trades"] >= MIN_TRADES_IS)
                        & df["error"].isna()]
        top = profitable.sort_values("recovery_factor", ascending=False).head(top_n)
        for _, row in top.iterrows():
            k = _param_key(row)
            c = counts.setdefault(k, {"count": 0, "windows": [], "total_rf": 0.0,
                                      "total_np": 0.0, "sample_row": row})
            c["count"] += 1
            c["windows"].append(win_label)
            c["total_rf"] += float(row["recovery_factor"])
            c["total_np"] += float(row["net_profit"])

    robust = [(k, info) for k, info in counts.items() if info["count"] >= 2]
    print(f"\n  Robust params (in top-{top_n} of 2+ windows): {len(robust)}")

    if not robust:
        print("  No robust params! Falling back to top-RF combined.")
        combined = list(counts.items())
        combined.sort(key=lambda x: -x[1]["total_rf"])
        robust = combined[: max_candidates * 2]

    robust.sort(key=lambda x: -x[1]["total_rf"])
    seen_np = set()
    unique = []
    for k, info in robust:
        np_key = round(info["total_np"])
        if np_key in seen_np:
            continue
        seen_np.add(np_key)
        unique.append((k, info))
        if len(unique) >= max_candidates:
            break

    candidates = []
    for k, info in unique:
        r = info["sample_row"]
        candidates.append(LSFVGConfig(
            risk_pct=RISK_PCT,
            signal_tf_minutes=15,
            lookback_bars=int(r["lookback_bars"]),
            min_fvg_pts=int(r["min_fvg_pts"]),
            max_fvg_pts=int(r["max_fvg_pts"]),
            sweep_buffer_pts=int(r["sweep_buffer_pts"]),
            rr_ratio=round(float(r["rr_ratio"]), 2),
            half_tp_ratio=round(float(r["half_tp_ratio"]), 2),
            pending_expire_bars=int(r["pending_expire_bars"]),
            comment="LSFVG",
        ))
        print(f"    #{len(candidates)} Lk={k[0]} FVG=[{k[1]},{k[2]}] SwBuf={k[3]} "
              f"RR={k[4]} HTP={k[5]}  "
              f"(windows={info['windows']}  total_NP=${info['total_np']:+,.0f})")
    return candidates


def run_oos_phase(candidates, meta) -> dict[str, pd.DataFrame]:
    per_window = {}
    for win_label, _, _, oos_start, oos_end in WINDOWS:
        cache = OUT_DIR / f"oos_{win_label}.parquet"
        df = run_sweep(
            candidates, SYMBOL,
            _to_utc(oos_start), _to_utc(oos_end),
            meta, initial_balance=DEPOSIT,
            n_workers=min(N_WORKERS, len(candidates)),
            cache_path=cache,
            window_label=f"OOS-{win_label}",
            signal_tf="M15",
        )
        per_window[win_label] = df
    return per_window


def rank_oos(candidates, oos_per_window):
    rows = []
    for i, cfg in enumerate(candidates):
        total_np = 0.0
        total_tr = 0
        dds = []
        prof_count = 0
        per_win = []
        for win_label, _, _, _, _ in WINDOWS:
            r = oos_per_window[win_label].iloc[i]
            per_win.append((win_label, r))
            total_np += float(r["net_profit"])
            total_tr += int(r["trades"])
            dds.append(float(r["drawdown_pct"]))
            if r["net_profit"] > 0:
                prof_count += 1
        avg_dd = sum(dds) / len(dds) if dds else 0.5
        np_dd_ratio = total_np / max(avg_dd, 0.5)
        rows.append({
            "cfg": cfg, "total_np": total_np, "total_tr": total_tr,
            "all_prof": prof_count == len(WINDOWS), "prof_count": prof_count,
            "per_win": per_win, "avg_dd": avg_dd, "np_dd_ratio": np_dd_ratio,
        })
    rows.sort(key=lambda x: (x["prof_count"], x["np_dd_ratio"]), reverse=True)
    return rows


def write_setfile(cfg: LSFVGConfig, path: Path, oos_summary: str):
    c = asdict(cfg)
    header = (
        f"; LSFVG (Liquidity Sweep + Fair Value Gap) - WFO winner ending Apr 25, 3% risk\n"
        f"; Next WFO: May 9 2026\n"
        f";\n"
        f"; {oos_summary}\n"
        f";\n"
    )
    lines = [
        f"_BaseMagic=3000||3000||1||3000||3000||N",
        f"_CapitalProtectionAmount=0.0||0.0||1||0.0||0.0||N",
        f"_RiskPct={c['risk_pct']}||{c['risk_pct']}||1||{c['risk_pct']}||{c['risk_pct']}||N",
        f"_LotMode=1||1||1||1||1||N",
        f"TierBase=2000||2000||1||2000||2000||N",
        f"LotStep=0.01||0.01||1||0.01||0.01||N",
        f"_OrderComment=LSFVG",
        f"_SignalTF={c['signal_tf_minutes']}||{c['signal_tf_minutes']}||1||{c['signal_tf_minutes']}||{c['signal_tf_minutes']}||N",
        f"_LookbackBars={c['lookback_bars']}||{c['lookback_bars']}||1||{c['lookback_bars']}||{c['lookback_bars']}||N",
        f"_MinFVGPts={c['min_fvg_pts']}||{c['min_fvg_pts']}||1||{c['min_fvg_pts']}||{c['min_fvg_pts']}||N",
        f"_MaxFVGPts={c['max_fvg_pts']}||{c['max_fvg_pts']}||1||{c['max_fvg_pts']}||{c['max_fvg_pts']}||N",
        f"_SweepBufferPts={c['sweep_buffer_pts']}||{c['sweep_buffer_pts']}||1||{c['sweep_buffer_pts']}||{c['sweep_buffer_pts']}||N",
        f"_RR_Ratio={c['rr_ratio']}||{c['rr_ratio']}||1||{c['rr_ratio']}||{c['rr_ratio']}||N",
        f"_HalfTP_Ratio={c['half_tp_ratio']}||{c['half_tp_ratio']}||1||{c['half_tp_ratio']}||{c['half_tp_ratio']}||N",
        f"_PendingExpireBars={c['pending_expire_bars']}||{c['pending_expire_bars']}||1||{c['pending_expire_bars']}||{c['pending_expire_bars']}||N",
        f"_UseEMAFilter=false",
        f"_EMA_Period=50||50||1||50||50||N",
        f"_DailyTargetPct=0.0||0.0||1||0.0||0.0||N",
        f"_DailyLossPct=0.0||0.0||1||0.0||0.0||N",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(header + "\n".join(lines) + "\n", encoding="utf-8")


def sanity_et(cfg: LSFVGConfig, meta: SymbolMeta) -> str:
    print("\n" + "=" * 72)
    print("  SANITY ET: winner on full Feb 14 -> Apr 25 continuous")
    print("=" * 72)
    full_start = _to_utc(date(2026, 2, 14))
    full_end = _to_utc(date(2026, 4, 25))
    ticks = load_ticks(SYMBOL, full_start, full_end)
    m1 = load_bars(SYMBOL, "M1", full_start, full_end)
    m15 = load_bars(SYMBOL, "M15", full_start, full_end)
    r = simulate_fast(ticks, m15, m1, cfg, meta, initial_balance=DEPOSIT)
    wr = (r.tp_count / r.trades * 100.0) if r.trades > 0 else 0.0
    summary = (f"Sanity ET (Feb 14 -> Apr 25, $10k): NP=${r.net_profit:+,.0f} "
               f"({r.net_profit/DEPOSIT*100:+.1f}% ROI) / DD {r.max_drawdown_pct:.1f}% "
               f"/ {r.trades} trades / WR {wr:.1f}% / PF {r.profit_factor:.2f}")
    print(f"\n  {summary}")
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiny", action="store_true")
    args = ap.parse_args()

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        print("=" * 72)
        mode = "TINY (1 cfg)" if args.tiny else "EXPLORATORY (144 combos)"
        print(f"  PYSIM WFO LSFVG (Apr 25)   [{mode}]")
        print("=" * 72)

        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )
        print(f"  Workers: {N_WORKERS}, Deposit: ${DEPOSIT:,.0f}, Risk: {RISK_PCT}%")

        print("\n  Pre-warming caches...")
        t0 = time.time()
        _ = load_ticks(SYMBOL, _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        _ = load_bars(SYMBOL, "M1", _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        _ = load_bars(SYMBOL, "M15", _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        print(f"  Pre-warm done in {time.time()-t0:.1f}s")

        configs = build_config_grid(tiny=args.tiny)
        print(f"\n  Config grid: {len(configs)} combos × 3 IS windows = {len(configs)*3} sims")

        print("\n" + "=" * 72)
        print("  PHASE A: IS Sweep")
        print("=" * 72)
        is_per_window = run_is_phase(configs, meta)
        for win_label, df in is_per_window.items():
            print_top5(df, f"IS-{win_label}")

        print("\n" + "=" * 72)
        print("  PHASE B: Cross-Window Robust Selection")
        print("=" * 72)
        candidates = select_robust(is_per_window, top_n=30, max_candidates=12)
        if not candidates:
            print("  No candidates. Aborting.")
            return

        print("\n" + "=" * 72)
        print("  PHASE C: OOS Validation")
        print("=" * 72)
        oos_per_window = run_oos_phase(candidates, meta)
        for win_label, df in oos_per_window.items():
            print(f"\n  OOS-{win_label}:")
            for i, r in df.iterrows():
                print(f"    #{i+1} NP=${r['net_profit']:+,.0f} ({r['return_pct']:+.1f}%) "
                      f"PF={r['profit_factor']:.2f} DD={r['drawdown_pct']:.1f}% "
                      f"Tr={int(r['trades'])}")

        print("\n" + "=" * 72)
        print("  PHASE D: FINAL RANKING (ProfCount, then NP/AvgDD)")
        print("=" * 72)
        ranked = rank_oos(candidates, oos_per_window)
        print(f"\n  {'Rank':<5}{'TotalOOSNP':>13}{'AvgDD%':>8}"
              f"{'NP/DD':>8}{'Prof':>6}  Params")
        for rank, row in enumerate(ranked, 1):
            cfg = row["cfg"]
            prof_str = f"{row['prof_count']}/{len(WINDOWS)}"
            print(f"  {rank:<5}{row['total_np']:>+13,.0f}{row['avg_dd']:>7.1f}%"
                  f"{row['np_dd_ratio']:>8.0f}{prof_str:>6}  "
                  f"Lk={cfg.lookback_bars} FVG=[{cfg.min_fvg_pts},{cfg.max_fvg_pts}] "
                  f"SwBuf={cfg.sweep_buffer_pts} RR={cfg.rr_ratio} HTP={cfg.half_tp_ratio}")

        winner = ranked[0]["cfg"]
        winner_total_np = ranked[0]["total_np"]
        print(f"\n  WINNER: Lk={winner.lookback_bars} "
              f"FVG=[{winner.min_fvg_pts},{winner.max_fvg_pts}] "
              f"SwBuf={winner.sweep_buffer_pts} RR={winner.rr_ratio} "
              f"HTP={winner.half_tp_ratio}")
        print(f"          OOS total: ${winner_total_np:+,.0f}")

        sanity_summary = sanity_et(winner, meta)
        write_setfile(winner, SET_OUT, sanity_summary)
        print(f"\n  Setfile written: {SET_OUT}")

        print("\n" + "=" * 72)
        print("  Done.")
        print("=" * 72)
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
