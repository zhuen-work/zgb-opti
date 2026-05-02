"""WFO for FVG Stream 1 only (FBO_FVG_v2 EA, H1 timeframe), at spread=60.

Mechanics: FVG limit orders at fair-value-gap zones (mean-reversion).
Different from FBO: limit orders + multi-zone + per-bar re-place.

Grid: 288 combos. ETA ~10 min Numba + 6 workers.
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
from zgb_sim.fvg import FVGConfig, simulate
from zgb_sim.sweep_fvg import run_sweep


SYMBOL = "XAUUSD"
RISK_PCT = 3.0
DEPOSIT = 10_000.0
N_WORKERS = 6   # Per memory rule: cap at 6.

WINDOWS = [
    ("W1", date(2026, 2, 14), date(2026, 3, 14), date(2026, 3, 14), date(2026, 3, 28)),
    ("W2", date(2026, 2, 28), date(2026, 3, 28), date(2026, 3, 28), date(2026, 4, 11)),
    ("W3", date(2026, 3, 14), date(2026, 4, 11), date(2026, 4, 11), date(2026, 4, 25)),
]
PREWARM_START = date(2026, 2, 12)
PREWARM_END   = date(2026, 4, 25)

OUT_DIR = ROOT / "output" / "sim_wfo_fvg_s1_spread60_apr25"
SET_OUT = ROOT / "configs" / "sets" / "fvg_s1_sim_spread60_apr25.set"
SIGNAL_TF = "H1"
SIGNAL_TF_MINUTES = 60


def build_config_grid(tiny: bool = False) -> list[FVGConfig]:
    """FVG S1 (H1) exploratory grid at spread=60.

    Grid 288: MinSize 1000/1500/2000, MaxAge 100/200, MaxZones 1/3,
    RR 3/5/7, SL_Buffer 20/40, HalfTP 0/0.3, PEB 2/4.
    """
    if tiny:
        min_size = (1500,); max_age = (150,); max_zones = (3,)
        rr = (5.0,); sl_buf = (40,); htp = (0.0,); peb = (3,)
    else:
        min_size = (1000, 1500, 2000)        # 3
        max_age = (100, 200)                 # 2
        max_zones = (1, 3)                   # 2
        rr = (3.0, 5.0, 7.0)                 # 3
        sl_buf = (20, 40)                    # 2
        htp = (0.0, 0.3)                     # 2
        peb = (2, 4)                         # 2
    grid: list[FVGConfig] = []
    for ms in min_size:
        for ma in max_age:
            for mz in max_zones:
                for r in rr:
                    for slb in sl_buf:
                        for h in htp:
                            for p in peb:
                                grid.append(FVGConfig(
                                    risk_pct=RISK_PCT,
                                    min_size_pts=ms,
                                    max_age_bars=ma,
                                    max_zones=mz,
                                    rr_ratio=r,
                                    sl_buffer_pts=slb,
                                    half_tp_ratio=h,
                                    pending_expire_bars=p,
                                    signal_tf_minutes=SIGNAL_TF_MINUTES,
                                    comment="FVG_A",
                                ))
    return grid


def _to_utc(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _param_key(row) -> tuple:
    return (
        int(row["min_size_pts"]),
        int(row["max_age_bars"]),
        int(row["max_zones"]),
        round(float(row["rr_ratio"]), 2),
        int(row["sl_buffer_pts"]),
        round(float(row["half_tp_ratio"]), 2),
        int(row["pending_expire_bars"]),
    )


def run_is_phase(configs, meta) -> dict[str, pd.DataFrame]:
    per = {}
    for label, is_s, is_e, _, _ in WINDOWS:
        cache = OUT_DIR / f"is_{label}.parquet"
        df = run_sweep(configs, SYMBOL, _to_utc(is_s), _to_utc(is_e),
                       meta, initial_balance=DEPOSIT, n_workers=N_WORKERS,
                       cache_path=cache, window_label=f"IS-{label}",
                       signal_tf=SIGNAL_TF)
        per[label] = df
    return per


def print_top5(df: pd.DataFrame, label: str):
    profitable = df[(df["net_profit"] > 0) & (df["trades"] >= 5) & df["error"].isna()]
    top = profitable.sort_values("recovery_factor", ascending=False).head(5)
    print(f"\n  {label}: top-5 by RF (of {len(profitable)} profitable):")
    print(f"    {'NP':>9}  {'ROI%':>6}  {'PF':>5}  {'DD':>5}  {'Tr':>3}  {'RF':>6}  "
          f"MinS  MA  MZ  RR  SLB  HTP  PEB")
    for _, r in top.iterrows():
        print(f"    {r['net_profit']:>+9,.0f}  {r['return_pct']:>+5.1f}%  "
              f"{r['profit_factor']:>5.2f}  {r['drawdown_pct']:>4.1f}%  "
              f"{int(r['trades']):>3}  {r['recovery_factor']:>6.0f}  "
              f"{int(r['min_size_pts']):>4}  {int(r['max_age_bars']):>2}  "
              f"{int(r['max_zones']):>2}  {r['rr_ratio']:>3.0f}  "
              f"{int(r['sl_buffer_pts']):>3}  {r['half_tp_ratio']:>3.1f}  "
              f"{int(r['pending_expire_bars']):>2}")


def select_robust(per_window, top_n=30, max_candidates=15) -> list[FVGConfig]:
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

    cands = []
    for k, info in unique:
        r = info["sample_row"]
        cands.append(FVGConfig(
            risk_pct=RISK_PCT,
            min_size_pts=int(r["min_size_pts"]),
            max_age_bars=int(r["max_age_bars"]),
            max_zones=int(r["max_zones"]),
            rr_ratio=float(r["rr_ratio"]),
            sl_buffer_pts=int(r["sl_buffer_pts"]),
            half_tp_ratio=round(float(r["half_tp_ratio"]), 2),
            pending_expire_bars=int(r["pending_expire_bars"]),
            signal_tf_minutes=SIGNAL_TF_MINUTES,
            comment="FVG_A",
        ))
        print(f"    #{len(cands)} MinS={k[0]} MA={k[1]} MZ={k[2]} RR={k[3]} "
              f"SLB={k[4]} HTP={k[5]} PEB={k[6]}  "
              f"(windows={info['windows']}  total_NP=${info['total_np']:+,.0f})")
    return cands


def run_oos_phase(candidates, meta) -> dict[str, pd.DataFrame]:
    per = {}
    for label, _, _, oos_s, oos_e in WINDOWS:
        cache = OUT_DIR / f"oos_{label}.parquet"
        df = run_sweep(candidates, SYMBOL, _to_utc(oos_s), _to_utc(oos_e),
                       meta, initial_balance=DEPOSIT,
                       n_workers=min(N_WORKERS, len(candidates)),
                       cache_path=cache, window_label=f"OOS-{label}",
                       signal_tf=SIGNAL_TF)
        per[label] = df
    return per


def rank_oos(candidates, oos_per_window):
    rows = []
    for i, cfg in enumerate(candidates):
        total_np = 0.0
        dds = []
        prof_count = 0
        per_win = []
        for label, _, _, _, _ in WINDOWS:
            r = oos_per_window[label].iloc[i]
            per_win.append((label, r))
            total_np += float(r["net_profit"])
            dds.append(float(r["drawdown_pct"]))
            if r["net_profit"] > 0:
                prof_count += 1
        avg_dd = sum(dds) / len(dds) if dds else 0.5
        np_dd = total_np / max(avg_dd, 0.5)
        rows.append({
            "cfg": cfg, "total_np": total_np,
            "all_prof": prof_count == len(WINDOWS), "prof_count": prof_count,
            "per_win": per_win, "avg_dd": avg_dd, "np_dd_ratio": np_dd,
        })
    rows.sort(key=lambda x: (x["prof_count"], x["np_dd_ratio"]), reverse=True)
    return rows


def write_setfile(cfg: FVGConfig, path: Path):
    """Write FVG S1 winner setfile (FBO + FVG S2 disabled)."""
    c = asdict(cfg)
    lines = [
        f"_BaseMagic=1000||1000||1||1000||1000||N",
        f"_CapitalProtectionAmount=0.0||0.0||1||0.0||0.0||N",
        f"_RiskPct={c['risk_pct']}||{c['risk_pct']}||1||{c['risk_pct']}||{c['risk_pct']}||N",
        f"_LotMode=1||1||1||1||1||N",
        f"TierBase=2000||2000||1||2000||2000||N",
        f"LotStep=0.01||0.01||1||0.01||0.01||N",
        f"_PendingExpireBars=2||2||1||2||2||N",
        f"_FBO1=0||0||1||0||0||N",
        f"_FBO2=0||0||1||0||0||N",
        f"_FVG1=1||1||1||1||1||N",
        f"_OrderComment4=FVG_A",
        f"_FVG_TF=16385||16385||1||16385||16385||N",   # H1
        f"_FVG_MinSize={c['min_size_pts']}||{c['min_size_pts']}||1||{c['min_size_pts']}||{c['min_size_pts']}||N",
        f"_FVG_MaxAge={c['max_age_bars']}||{c['max_age_bars']}||1||{c['max_age_bars']}||{c['max_age_bars']}||N",
        f"_MaxZones={c['max_zones']}||{c['max_zones']}||1||{c['max_zones']}||{c['max_zones']}||N",
        f"_RR_Ratio={c['rr_ratio']}||{c['rr_ratio']}||1||{c['rr_ratio']}||{c['rr_ratio']}||N",
        f"_SL_Buffer={c['sl_buffer_pts']}||{c['sl_buffer_pts']}||1||{c['sl_buffer_pts']}||{c['sl_buffer_pts']}||N",
        f"_PendingExpireBars_F1={c['pending_expire_bars']}||{c['pending_expire_bars']}||1||{c['pending_expire_bars']}||{c['pending_expire_bars']}||N",
        f"_HalfTP_F1={c['half_tp_ratio']}||{c['half_tp_ratio']}||1||{c['half_tp_ratio']}||{c['half_tp_ratio']}||N",
        f"_FVG2=0||0||1||0||0||N",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def sanity_et(cfg: FVGConfig, meta: SymbolMeta):
    print("\n" + "=" * 72)
    print("  SANITY ET: winner on full Mar 14 -> Apr 25 (continuous)")
    print("=" * 72)
    full_start = _to_utc(date(2026, 3, 14))
    full_end = _to_utc(date(2026, 4, 25))
    ticks = load_ticks(SYMBOL, full_start, full_end)
    m1 = load_bars(SYMBOL, "M1", full_start, full_end)
    sig = load_bars(SYMBOL, SIGNAL_TF, full_start, full_end)
    debug_base = OUT_DIR / "winner_sanity"
    r = simulate(ticks, sig, m1, cfg, meta, initial_balance=DEPOSIT,
                 debug_path=str(debug_base))
    print(f"\n  Winner sanity ET: {r.summary()}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiny", action="store_true")
    args = ap.parse_args()
    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        print("=" * 72)
        mode = "TINY" if args.tiny else "FVG S1 H1 (288 combos)"
        print(f"  PYSIM WFO FVG Stream 1 (H1, Apr 25, spread=60)   [{mode}]")
        print("=" * 72)

        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )
        print(f"  Workers: {N_WORKERS}, Deposit: ${DEPOSIT:,.0f}, Risk: {RISK_PCT}%")

        print(f"\n  Pre-warming tick/M1/{SIGNAL_TF} cache for full range...")
        t0 = time.time()
        _ = load_ticks(SYMBOL, _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        _ = load_bars(SYMBOL, "M1", _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        _ = load_bars(SYMBOL, SIGNAL_TF, _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        print(f"  Pre-warm done in {time.time()-t0:.1f}s")

        configs = build_config_grid(tiny=args.tiny)
        print(f"\n  Config grid: {len(configs)} combos")

        print("\n" + "=" * 72)
        print("  PHASE A: IS Sweep")
        print("=" * 72)
        is_per = run_is_phase(configs, meta)
        for label, df in is_per.items():
            print_top5(df, f"IS-{label}")

        print("\n" + "=" * 72)
        print("  PHASE B: Cross-Window Robust Selection")
        print("=" * 72)
        candidates = select_robust(is_per, top_n=30, max_candidates=15)
        if not candidates:
            print("  No candidates. Aborting.")
            return

        print("\n" + "=" * 72)
        print("  PHASE C: OOS Validation")
        print("=" * 72)
        oos_per = run_oos_phase(candidates, meta)
        for label, df in oos_per.items():
            print(f"\n  OOS-{label} results:")
            for i, r in df.iterrows():
                print(f"    #{i+1} NP={r['net_profit']:+,.2f} "
                      f"({r['return_pct']:+.1f}%)  PF={r['profit_factor']:.2f}  "
                      f"DD={r['drawdown_pct']:.1f}%  Tr={int(r['trades'])}")

        print("\n" + "=" * 72)
        print("  PHASE D: FINAL RANKING (by ProfCount, then NP/AvgDD ratio)")
        print("=" * 72)
        ranked = rank_oos(candidates, oos_per)
        print(f"\n  {'Rank':<5}{'Total OOS NP':>15}{'ROI%':>9}{'AvgDD':>8}"
              f"{'NP/DD':>8}{'Prof':>6}  Params")
        for rank, row in enumerate(ranked, 1):
            cfg = row["cfg"]
            total_ret = row["total_np"] / (DEPOSIT * len(WINDOWS)) * 100.0
            prof_str = f"{row['prof_count']}/{len(WINDOWS)}"
            print(f"  {rank:<5}{row['total_np']:>+14,.2f}{total_ret:>+7.1f}%"
                  f"{row['avg_dd']:>7.1f}%{row['np_dd_ratio']:>8.0f}{prof_str:>6}  "
                  f"MinS={cfg.min_size_pts} MA={cfg.max_age_bars} MZ={cfg.max_zones} "
                  f"RR={cfg.rr_ratio} SLB={cfg.sl_buffer_pts} HTP={cfg.half_tp_ratio} "
                  f"PEB={cfg.pending_expire_bars}")

        winner = ranked[0]["cfg"]
        winner_total = ranked[0]["total_np"]
        winner_ret = winner_total / (DEPOSIT * len(WINDOWS)) * 100.0
        print(f"\n  WINNER: MinS={winner.min_size_pts} MA={winner.max_age_bars} "
              f"MZ={winner.max_zones} RR={winner.rr_ratio} "
              f"SLB={winner.sl_buffer_pts} HTP={winner.half_tp_ratio} "
              f"PEB={winner.pending_expire_bars}")
        print(f"  OOS total: ${winner_total:+,.2f} ({winner_ret:+.1f}% avg/window)")
        write_setfile(winner, SET_OUT)
        print(f"  Setfile written: {SET_OUT}")

        sanity_et(winner, meta)
        print("\n" + "=" * 72)
        print("  Done.")
        print("=" * 72)
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
