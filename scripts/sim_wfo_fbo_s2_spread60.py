"""WFO for FBO Stream 2 only (FBO_FVG_v2 EA, H4 timeframe), at spread=60.

Mirrors S1 WFO; differs in:
  - signal TF = H4 (240 min) instead of M30
  - SMA periods scale longer (10/25/50)
  - PEB swept in H4-bar units (1 = 4hr, 2 = 8hr, 3 = 12hr)

Grid: 729 combos. ETA ~30 min with Numba + 6 workers.
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
from zgb_sim.fbo_s1 import FBOS1Config, simulate
from zgb_sim.sweep_fbo import run_sweep


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

OUT_DIR = ROOT / "output" / "sim_wfo_fbo_s2_spread60_apr25"
SET_OUT = ROOT / "configs" / "sets" / "fbo_s2_sim_spread60_apr25.set"
SIGNAL_TF = "H4"
SIGNAL_TF_MINUTES = 240


def build_config_grid(tiny: bool = False) -> list[FBOS1Config]:
    """S2 (H4) exploratory grid at spread=60.

    Reference (S2 in fbo_v2_3pct_m30xh4xh1_apr11_reopt_10k.set):
      Bars=4, TP=12000, SL=15000, HTP=0.8, SMA=25, PEB=2.

    Grid 729: Bars 4/6/8, TP 10/12/15k, SL 10/12.5/15k, HTP 0/0.3/0.8,
    SMA 10/25/50, PEB 1/2/3.
    """
    if tiny:
        bars_vals = (4,)
        tp_vals = (12_000,)
        sl_vals = (15_000,)
        htp_vals = (0.8,)
        sma_vals = (25,)
        peb_vals = (2,)
    else:
        bars_vals = (4, 6, 8)                  # 3
        tp_vals = (10_000, 12_000, 15_000)     # 3
        sl_vals = (10_000, 12_500, 15_000)     # 3
        htp_vals = (0.0, 0.3, 0.8)             # 3 — keep 0.8 (S2 ref)
        sma_vals = (10, 25, 50)                # 3 — longer for H4
        peb_vals = (1, 2, 3)                   # 3 — H4 PEB units
    grid: list[FBOS1Config] = []
    for bars in bars_vals:
        for tp in tp_vals:
            for sl in sl_vals:
                for htp in htp_vals:
                    for sma in sma_vals:
                        for peb in peb_vals:
                            grid.append(FBOS1Config(
                                risk_pct=RISK_PCT,
                                fractal_bars=bars,
                                take_profit_pts=tp,
                                stop_loss_pts=sl,
                                half_tp_ratio=htp,
                                sma_period=sma,
                                pending_expire_bars=peb,
                                signal_tf_minutes=SIGNAL_TF_MINUTES,
                                comment="FBO_B",
                            ))
    return grid


def _to_utc(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _param_key(row) -> tuple:
    return (
        int(row["fractal_bars"]),
        int(row["take_profit_pts"]),
        int(row["stop_loss_pts"]),
        round(float(row["half_tp_ratio"]), 2),
        int(row["sma_period"]),
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
            signal_tf=SIGNAL_TF,
        )
        per_window[win_label] = df
    return per_window


def print_top5(df: pd.DataFrame, label: str):
    profitable = df[(df["net_profit"] > 0) & (df["trades"] >= 5) & df["error"].isna()]
    top = profitable.sort_values("recovery_factor", ascending=False).head(5)
    print(f"\n  {label}: top-5 by Recovery Factor (of {len(profitable)} profitable):")
    print(f"    {'NP':>10}  {'ROI%':>7}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  {'RF':>8}  Bars  TP   SL    HTP  SMA")
    for _, r in top.iterrows():
        print(f"    {r['net_profit']:>+10,.0f}  {r['return_pct']:>+6.1f}%  "
              f"{r['profit_factor']:>6.2f}  "
              f"{r['drawdown_pct']:>5.1f}%  {int(r['trades']):>4}  {r['recovery_factor']:>8.0f}  "
              f"{int(r['fractal_bars']):>4}  {int(r['take_profit_pts']):>5}  "
              f"{int(r['stop_loss_pts']):>4}  {r['half_tp_ratio']:>4.1f}  "
              f"{int(r['sma_period']):>3}")


def select_robust(per_window, top_n: int = 30, max_candidates: int = 15) -> list[FBOS1Config]:
    counts = {}
    for win_label, df in per_window.items():
        profitable = df[(df["net_profit"] > 0) & (df["trades"] >= 5) & df["error"].isna()]
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
        candidates.append(FBOS1Config(
            risk_pct=RISK_PCT,
            fractal_bars=int(r["fractal_bars"]),
            take_profit_pts=int(r["take_profit_pts"]),
            stop_loss_pts=int(r["stop_loss_pts"]),
            half_tp_ratio=round(float(r["half_tp_ratio"]), 2),
            sma_period=int(r["sma_period"]),
            pending_expire_bars=int(r["pending_expire_bars"]),
            comment="FBO_A",
        ))
        print(f"    #{len(candidates)} Bars={k[0]} TP={k[1]} SL={k[2]} HTP={k[3]} "
              f"SMA={k[4]} PEB={k[5]}  "
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
            signal_tf=SIGNAL_TF,
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


def write_setfile(cfg: FBOS1Config, path: Path):
    """Write FBO Stream 2 winner setfile (S1+S3+FVG disabled)."""
    c = asdict(cfg)
    lines = [
        f"_BaseMagic=1000||1000||1||1000||1000||N",
        f"_CapitalProtectionAmount=0.0||0.0||1||0.0||0.0||N",
        f"_RiskPct={c['risk_pct']}||{c['risk_pct']}||1||{c['risk_pct']}||{c['risk_pct']}||N",
        f"_LotMode=1||1||1||1||1||N",
        f"TierBase=2000||2000||1||2000||2000||N",
        f"LotStep=0.01||0.01||1||0.01||0.01||N",
        f"_PendingExpireBars={c['pending_expire_bars']}||{c['pending_expire_bars']}||1||{c['pending_expire_bars']}||{c['pending_expire_bars']}||N",
        f"_FBO1=0||0||1||0||0||N",
        f"_FBO2=1||1||1||1||1||N",
        f"_OrderComment2=FBO_B",
        f"_time_frame2=16388||16388||1||16388||16388||N",   # H4
        f"_take_profit2={c['take_profit_pts']}||{c['take_profit_pts']}||1||{c['take_profit_pts']}||{c['take_profit_pts']}||N",
        f"_stop_loss2={c['stop_loss_pts']}||{c['stop_loss_pts']}||1||{c['stop_loss_pts']}||{c['stop_loss_pts']}||N",
        f"_Bars2={c['fractal_bars']}||{c['fractal_bars']}||1||{c['fractal_bars']}||{c['fractal_bars']}||N",
        f"_EMA_Period2={c['sma_period']}||{c['sma_period']}||1||{c['sma_period']}||{c['sma_period']}||N",
        f"_HalfTP2={c['half_tp_ratio']}||{c['half_tp_ratio']}||1||{c['half_tp_ratio']}||{c['half_tp_ratio']}||N",
        f"_FBO3=0||0||1||0||0||N",
        f"_FVG1=0||0||1||0||0||N",
        f"_FVG2=0||0||1||0||0||N",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def sanity_et(cfg: FBOS1Config, meta: SymbolMeta):
    print("\n" + "=" * 72)
    print("  SANITY ET: winner on full OOS span Mar 14 -> Apr 25 (continuous)")
    print("=" * 72)
    full_start = _to_utc(date(2026, 3, 14))
    full_end = _to_utc(date(2026, 4, 25))
    ticks = load_ticks(SYMBOL, full_start, full_end)
    m1 = load_bars(SYMBOL, "M1", full_start, full_end)
    h4 = load_bars(SYMBOL, SIGNAL_TF, full_start, full_end)
    debug_base = OUT_DIR / "winner_sanity"
    r = simulate(ticks, h4, m1, cfg, meta, initial_balance=DEPOSIT,
                 debug_path=str(debug_base))
    print(f"\n  Winner sanity ET: {r.summary()}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiny", action="store_true", help="1-config sanity run")
    args = ap.parse_args()

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        print("=" * 72)
        mode = "TINY (1 cfg)" if args.tiny else "S2 H4 (729 combos)"
        print(f"  PYSIM WFO FBO Stream 2 (H4, Apr 25, spread=60) — sim IS+OOS   [{mode}]")
        print("=" * 72)

        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )
        print(f"  Meta: {m}")
        print(f"  Workers: {N_WORKERS}, Deposit: ${DEPOSIT:,.0f}, Risk: {RISK_PCT}%")

        print(f"\n  Pre-warming tick/M1/{SIGNAL_TF} cache for full range...")
        t0 = time.time()
        _ = load_ticks(SYMBOL, _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        _ = load_bars(SYMBOL, "M1", _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        _ = load_bars(SYMBOL, SIGNAL_TF, _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        print(f"  Pre-warm done in {time.time()-t0:.1f}s")

        configs = build_config_grid(tiny=args.tiny)
        print(f"\n  Config grid: {len(configs)} combos")

        # Phase A
        print("\n" + "=" * 72)
        print("  PHASE A: IS Sweep")
        print("=" * 72)
        is_per_window = run_is_phase(configs, meta)
        for win_label, df in is_per_window.items():
            print_top5(df, f"IS-{win_label}")

        # Phase B
        print("\n" + "=" * 72)
        print("  PHASE B: Cross-Window Robust Selection")
        print("=" * 72)
        candidates = select_robust(is_per_window, top_n=30, max_candidates=15)
        if not candidates:
            print("  No candidates. Aborting.")
            return

        # Phase C
        print("\n" + "=" * 72)
        print("  PHASE C: OOS Validation")
        print("=" * 72)
        oos_per_window = run_oos_phase(candidates, meta)
        for win_label, df in oos_per_window.items():
            print(f"\n  OOS-{win_label} results:")
            for i, r in df.iterrows():
                print(f"    #{i+1} NP={r['net_profit']:+,.2f} ({r['return_pct']:+.1f}%)  "
                      f"PF={r['profit_factor']:.2f}  DD={r['drawdown_pct']:.1f}%  "
                      f"Tr={int(r['trades'])}")

        # Phase D
        print("\n" + "=" * 72)
        print("  PHASE D: FINAL RANKING (by ProfCount, then NP/AvgDD ratio)")
        print("=" * 72)
        ranked = rank_oos(candidates, oos_per_window)
        print(f"\n  {'Rank':<5}{'Total OOS NP':>15}{'ROI%':>9}{'AvgDD':>8}"
              f"{'NP/DD':>8}{'Prof':>6}  Params")
        for rank, row in enumerate(ranked, 1):
            cfg = row["cfg"]
            total_ret_pct = row["total_np"] / (DEPOSIT * len(WINDOWS)) * 100.0
            prof_str = f"{row['prof_count']}/{len(WINDOWS)}"
            print(f"  {rank:<5}{row['total_np']:>+14,.2f}{total_ret_pct:>+7.1f}%"
                  f"{row['avg_dd']:>7.1f}%{row['np_dd_ratio']:>8.0f}{prof_str:>6}  "
                  f"Bars={cfg.fractal_bars} TP={cfg.take_profit_pts} SL={cfg.stop_loss_pts} "
                  f"HTP={cfg.half_tp_ratio} SMA={cfg.sma_period} "
                  f"PEB={cfg.pending_expire_bars}")

        winner = ranked[0]["cfg"]
        winner_total_np = ranked[0]["total_np"]
        winner_ret_pct = winner_total_np / (DEPOSIT * len(WINDOWS)) * 100.0
        print(f"\n  WINNER: Bars={winner.fractal_bars} TP={winner.take_profit_pts} "
              f"SL={winner.stop_loss_pts} HTP={winner.half_tp_ratio} "
              f"SMA={winner.sma_period}")
        print(f"          OOS total: ${winner_total_np:+,.2f} "
              f"({winner_ret_pct:+.1f}% avg/window, ${DEPOSIT:,.0f} deposit)")
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
