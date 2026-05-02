"""WFO for Scalper_v2 ORB stream only, at spread=60.

Conditions: $100 deposit, 1% risk (Scalper convention).
Same Phase A/B/C/D structure.
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
from zgb_sim.orb import ORBConfig, simulate
from zgb_sim.sweep_orb import run_sweep


SYMBOL = "XAUUSD"
RISK_PCT = 1.0
DEPOSIT = 10_000.0   # Standardized to match FBO_v3 for fair comparison
N_WORKERS = 6
SIGNAL_TF = "M5"

WINDOWS = [
    ("W1", date(2026, 2, 14), date(2026, 3, 14), date(2026, 3, 14), date(2026, 3, 28)),
    ("W2", date(2026, 2, 28), date(2026, 3, 28), date(2026, 3, 28), date(2026, 4, 11)),
    ("W3", date(2026, 3, 14), date(2026, 4, 11), date(2026, 4, 11), date(2026, 4, 25)),
]
PREWARM_START = date(2026, 2, 12)
PREWARM_END   = date(2026, 4, 25)

OUT_DIR = ROOT / "output" / "sim_wfo_orb_spread60_apr25"
SET_OUT = ROOT / "configs" / "sets" / "scalp_v2_orb_spread60_apr25.set"


def build_config_grid(tiny=False) -> list[ORBConfig]:
    if tiny:
        return [ORBConfig()]
    grid = []
    # Wider grid centered on previous winner (Range=60, Buf=0, FixSL=400,
    # RR=3.0, HTP=0.0, Tgt=9, Loss=6). Tests if extensions beat baseline.
    for range_min in (30, 45, 60, 90):              # 4 — extend higher
        for buffer_pts in (0, 30):                   # 2
            for fixed_sl in (200, 300, 400, 500, 600, 800):  # 6 — extend higher
                for rr in (2.0, 3.0, 4.0):           # 3 — add RR=4
                    for htp in (0.0, 0.3):           # 2
                        for tgt in (4.0, 6.0, 9.0, 12.0):    # 4 — add 12
                            for loss in (4.0, 6.0, 9.0, 12.0):  # 4 — add 4 + 12
                                grid.append(ORBConfig(
                                    risk_pct=RISK_PCT,
                                    range_minutes=range_min,
                                    buffer_pts=buffer_pts,
                                    min_range_pts=200,
                                    max_range_pts=5000,
                                    fixed_sl_pts=fixed_sl,
                                    rr_ratio=rr,
                                    half_tp_ratio=htp,
                                    pending_expire_minutes=120,
                                    daily_target_pct=tgt,
                                    daily_loss_pct=loss,
                                    ldn_enabled=True, ldn_start_hour=7,
                                    ny_enabled=True, ny_start_hour=13,
                                    comment="ORB",
                                ))
    return grid


def _to_utc(d): return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _param_key(row):
    return (
        int(row["range_minutes"]), int(row["buffer_pts"]),
        int(row["fixed_sl_pts"]), round(float(row["rr_ratio"]), 2),
        round(float(row["half_tp_ratio"]), 2),
        round(float(row["daily_target_pct"]), 2),
        round(float(row["daily_loss_pct"]), 2),
    )


def run_is_phase(configs, meta):
    per = {}
    for label, is_s, is_e, _, _ in WINDOWS:
        cache = OUT_DIR / f"is_{label}.parquet"
        df = run_sweep(configs, SYMBOL, _to_utc(is_s), _to_utc(is_e),
                       meta, initial_balance=DEPOSIT, n_workers=N_WORKERS,
                       cache_path=cache, window_label=f"IS-{label}", signal_tf=SIGNAL_TF)
        per[label] = df
    return per


def print_top5(df, label):
    p = df[(df["net_profit"] > 0) & (df["trades"] >= 5) & df["error"].isna()]
    top = p.sort_values("recovery_factor", ascending=False).head(5)
    print(f"\n  {label}: top-5 by RF (of {len(p)} profitable):")
    for _, r in top.iterrows():
        print(f"    NP=${r['net_profit']:>+7.0f}  DD={r['drawdown_pct']:>4.1f}%  "
              f"Tr={int(r['trades']):>3}  RF={r['recovery_factor']:>5.0f}  "
              f"Range={int(r['range_minutes'])}min Buf={int(r['buffer_pts'])} "
              f"FixSL={int(r['fixed_sl_pts'])} RR={r['rr_ratio']} "
              f"HTP={r['half_tp_ratio']} Tgt={r['daily_target_pct']}% Loss={r['daily_loss_pct']}%")


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
        cands.append(ORBConfig(
            risk_pct=RISK_PCT,
            range_minutes=int(r["range_minutes"]),
            buffer_pts=int(r["buffer_pts"]),
            min_range_pts=200, max_range_pts=5000,
            fixed_sl_pts=int(r["fixed_sl_pts"]),
            rr_ratio=float(r["rr_ratio"]),
            half_tp_ratio=round(float(r["half_tp_ratio"]), 2),
            pending_expire_minutes=120,
            daily_target_pct=float(r["daily_target_pct"]),
            daily_loss_pct=float(r["daily_loss_pct"]),
            ldn_enabled=True, ldn_start_hour=7,
            ny_enabled=True, ny_start_hour=13,
            comment="ORB",
        ))
        print(f"    #{len(cands)} Range={k[0]} Buf={k[1]} FixSL={k[2]} RR={k[3]} "
              f"HTP={k[4]} Tgt={k[5]}% Loss={k[6]}%  windows={info['windows']}")
    return cands


def run_oos_phase(candidates, meta):
    per = {}
    for label, _, _, oos_s, oos_e in WINDOWS:
        cache = OUT_DIR / f"oos_{label}.parquet"
        df = run_sweep(candidates, SYMBOL, _to_utc(oos_s), _to_utc(oos_e),
                       meta, initial_balance=DEPOSIT,
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


def write_setfile(cfg: ORBConfig, path: Path):
    c = asdict(cfg)
    lines = [
        f"_BaseMagic=3000||3000||1||3000||3000||N",
        f"_RiskPct={c['risk_pct']}||{c['risk_pct']}||1||{c['risk_pct']}||{c['risk_pct']}||N",
        f"_LotMode=1||1||1||1||1||N",
        f"_DailyTargetPct={c['daily_target_pct']}||{c['daily_target_pct']}||1||{c['daily_target_pct']}||{c['daily_target_pct']}||N",
        f"_DailyLossPct={c['daily_loss_pct']}||{c['daily_loss_pct']}||1||{c['daily_loss_pct']}||{c['daily_loss_pct']}||N",
        f"_S1_Enabled=true",
        f"_S1_Comment=ORB",
        f"_S1_RangeMinutes={c['range_minutes']}||{c['range_minutes']}||1||{c['range_minutes']}||{c['range_minutes']}||N",
        f"_S1_BufferPts={c['buffer_pts']}||{c['buffer_pts']}||1||{c['buffer_pts']}||{c['buffer_pts']}||N",
        f"_S1_MinRangePts={c['min_range_pts']}||{c['min_range_pts']}||1||{c['min_range_pts']}||{c['min_range_pts']}||N",
        f"_S1_MaxRangePts={c['max_range_pts']}||{c['max_range_pts']}||1||{c['max_range_pts']}||{c['max_range_pts']}||N",
        f"_S1_FixedSL_Pts={c['fixed_sl_pts']}||{c['fixed_sl_pts']}||1||{c['fixed_sl_pts']}||{c['fixed_sl_pts']}||N",
        f"_S1_RR_Ratio={c['rr_ratio']}||{c['rr_ratio']}||1||{c['rr_ratio']}||{c['rr_ratio']}||N",
        f"_S1_HalfTP_Ratio={c['half_tp_ratio']}||{c['half_tp_ratio']}||1||{c['half_tp_ratio']}||{c['half_tp_ratio']}||N",
        f"_S1_PendingExpireMinutes={c['pending_expire_minutes']}||{c['pending_expire_minutes']}||1||{c['pending_expire_minutes']}||{c['pending_expire_minutes']}||N",
        f"_S2_Enabled=false",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def sanity_et(cfg: ORBConfig, meta: SymbolMeta):
    print("\n" + "=" * 72)
    print("  SANITY ET (continuous Mar 14 -> Apr 25)")
    print("=" * 72)
    full_start = _to_utc(date(2026, 3, 14))
    full_end = _to_utc(date(2026, 4, 25))
    ticks = load_ticks(SYMBOL, full_start, full_end)
    m1 = load_bars(SYMBOL, "M1", full_start, full_end)
    m5 = load_bars(SYMBOL, "M5", full_start, full_end)
    r = simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
    print(f"\n  Sanity: {r.summary()}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiny", action="store_true")
    args = ap.parse_args()
    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        print("=" * 72)
        configs = build_config_grid(tiny=args.tiny)
        print(f"  PYSIM WFO ORB (Apr 25, spread=60) — {len(configs)} combos")
        print("=" * 72)

        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        print(f"  Workers: {N_WORKERS}  Deposit: ${DEPOSIT}  Risk: {RISK_PCT}%")

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
                  f"Range={cfg.range_minutes} Buf={cfg.buffer_pts} FixSL={cfg.fixed_sl_pts} "
                  f"RR={cfg.rr_ratio} HTP={cfg.half_tp_ratio} "
                  f"Tgt={cfg.daily_target_pct}% Loss={cfg.daily_loss_pct}%")

        winner = ranked[0]["cfg"]
        write_setfile(winner, SET_OUT)
        print(f"\n  Setfile: {SET_OUT}")
        sanity_et(winner, meta)
        print("\n=== Done ===")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
