"""V7 parent WFO — same grid as v6's V2 fractal WFO but with SMA(8,21) cross-exit
LOCKED ON for every config, plus fractal_confirm=True, fractal_width=5 locked.

Grid: range x sl x rr x htp x expire = 2 x 3 x 4 x 2 x 1 = 48 configs per window.
4 windows total = 192 sims. ETA ~5-7 min.

The cross-exit feature (sma_cross_exit=True, periods 8/21) is the V5-final winner.
This WFO finds the best parent dims to PAIR with that cross-exit configuration.

Prints marker lines for Monitor: [BOOT], [GRID], [WIN], [OOS], [RANK], [DONE].
"""
from __future__ import annotations

import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.sweep_orb import run_sweep
from zgb_sim.wfo_helpers import WINDOWS_MAY23 as WINDOWS, rank_with_p0

SYMBOL = "XAUUSD"
RISK_PCT = 6.0
DEPOSIT = 10_000.0
N_WORKERS = 6
SIGNAL_TF = "M5"
OUT_DIR = ROOT / "output" / "wfo_orb_v7_smacross_may23"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Locked V5-winner cross-exit periods.
SMA_FAST = 8
SMA_SLOW = 21


def _to_utc(d):
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def build_grid() -> list[ORBConfig]:
    """Wider grid matching v6 deployment baseline + SMA cross-exit ON.
    Range {60,90,120} × SL {400,550,700} × RR {2.0..4.0/0.5} × HTP {0..0.6/0.2} × Exp {240,480,720}
    = 3×3×5×4×3 = 540 configs/window."""
    grid = []
    for range_min in (60, 90, 120):
        for fixed_sl in (400, 550, 700):
            for rr in (2.0, 2.5, 3.0, 3.5, 4.0):
                for htp in (0.0, 0.2, 0.4, 0.6):
                    for expire_min in (240, 480, 720):
                        grid.append(ORBConfig(
                            risk_pct=RISK_PCT,
                            range_minutes=range_min,
                            buffer_pts=0,
                            min_range_pts=0,
                            max_range_pts=999_999,
                            fixed_sl_pts=fixed_sl,
                            rr_ratio=rr,
                            half_tp_ratio=htp,
                            pending_expire_minutes=expire_min,
                            daily_target_pct=0.0,
                            daily_loss_pct=0.0,
                            ldn_enabled=True, ldn_start_hour=7,
                            ny_enabled=True, ny_start_hour=13,
                            fractal_confirm=True,
                            fractal_width=5,
                            sma_cross_exit=True,
                            sma_cross_fast=SMA_FAST,
                            sma_cross_slow=SMA_SLOW,
                            comment="ORB",
                        ))
    return grid


def _param_key(row):
    return (
        int(row["range_minutes"]),
        int(row["fixed_sl_pts"]),
        round(float(row["rr_ratio"]), 2),
        round(float(row["half_tp_ratio"]), 2),
        int(row["pending_expire_minutes"]),
    )


def select_robust(per_window, top_n=20, max_candidates=20):
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
    if not robust:
        combined = list(counts.items())
        combined.sort(key=lambda x: -x[1]["total_rf"])
        robust = combined[: max_candidates * 2]
    robust.sort(key=lambda x: -x[1]["total_rf"])
    seen = set()
    unique = []
    for k, info in robust:
        if k in seen:
            continue
        seen.add(k)
        unique.append((k, info))
        if len(unique) >= max_candidates:
            break

    cands = []
    for k, info in unique:
        r = info["sample_row"]
        cands.append(ORBConfig(
            risk_pct=RISK_PCT,
            range_minutes=int(r["range_minutes"]),
            buffer_pts=0,
            min_range_pts=0, max_range_pts=999_999,
            fixed_sl_pts=int(r["fixed_sl_pts"]),
            rr_ratio=float(r["rr_ratio"]),
            half_tp_ratio=round(float(r["half_tp_ratio"]), 2),
            pending_expire_minutes=int(r["pending_expire_minutes"]),
            daily_target_pct=0.0, daily_loss_pct=0.0,
            ldn_enabled=True, ldn_start_hour=7,
            ny_enabled=True, ny_start_hour=13,
            fractal_confirm=True, fractal_width=5,
            sma_cross_exit=True,
            sma_cross_fast=SMA_FAST,
            sma_cross_slow=SMA_SLOW,
            comment="ORB",
        ))
    return cands


def fmt_cfg(c: ORBConfig) -> str:
    return (f"Range={c.range_minutes:>3} SL={c.fixed_sl_pts:>4} "
            f"RR={c.rr_ratio:<3} HTP={c.half_tp_ratio:<3} "
            f"Exp={c.pending_expire_minutes:>3} V2_w5 SMA{SMA_FAST}/{SMA_SLOW}")


def main():
    print(f"[BOOT] V7 SMA-cross WFO on {WINDOWS[0][0]}..{WINDOWS[-1][0]}  "
          f"SMA periods locked ({SMA_FAST},{SMA_SLOW})", flush=True)
    m = symbol_meta(SYMBOL)
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])

    configs = build_grid()
    print(f"[GRID] {len(configs)} configs per window ({len(WINDOWS)} windows)", flush=True)

    # === Phase 1: IS sweep per window ===
    is_per = {}
    for label, is_s, is_e, _, _ in WINDOWS:
        cache = OUT_DIR / f"p1_is_{label}.parquet"
        t0 = time.time()
        df = run_sweep(configs, SYMBOL, _to_utc(is_s), _to_utc(is_e),
                       meta, initial_balance=DEPOSIT, n_workers=N_WORKERS,
                       cache_path=cache, window_label=f"IS-{label}", signal_tf=SIGNAL_TF)
        is_per[label] = df
        elapsed = time.time() - t0
        print(f"[WIN] IS-{label} done in {elapsed:.0f}s ({len(df)} rows)", flush=True)

    # === Robust candidate selection ===
    cands = select_robust(is_per, top_n=20, max_candidates=20)
    print(f"[RANK] {len(cands)} robust candidates selected", flush=True)
    for i, c in enumerate(cands[:10]):
        print(f"  cand#{i+1} {fmt_cfg(c)}", flush=True)

    # === Phase 1: OOS validation per window ===
    oos_per = {}
    for label, _, _, oos_s, oos_e in WINDOWS:
        cache = OUT_DIR / f"p1_oos_{label}.parquet"
        t0 = time.time()
        df = run_sweep(cands, SYMBOL, _to_utc(oos_s), _to_utc(oos_e),
                       meta, initial_balance=DEPOSIT,
                       n_workers=min(N_WORKERS, len(cands)),
                       cache_path=cache, window_label=f"OOS-{label}", signal_tf=SIGNAL_TF)
        oos_per[label] = df
        elapsed = time.time() - t0
        print(f"[OOS] OOS-{label} done in {elapsed:.0f}s", flush=True)

    # === Rank with p0 ===
    ranked = rank_with_p0(cands, oos_per, WINDOWS, decay_threshold=-0.25,
                           grid_configs=configs, is_per_window=is_per)

    print(f"[RANK] Final OOS rank top-10:", flush=True)
    for i, info in enumerate(ranked[:10]):
        c = info["cfg"]
        np_dd = info.get("np_dd_ratio", float("nan"))
        prof = info.get("prof_count", 0)
        total_np = info.get("total_np", 0)
        print(f"  rank#{i+1} prof={prof}/4 total_np=${total_np:>+8,.0f} "
              f"NP/DD$={np_dd:>5.2f}  {fmt_cfg(c)}", flush=True)

    # Write summary CSV
    rows = []
    for i, info in enumerate(ranked):
        c = info["cfg"]
        rows.append({
            "rank": i + 1,
            "prof_count": info.get("prof_count", 0),
            "total_np": info.get("total_np", 0),
            "np_dd_ratio": info.get("np_dd_ratio", 0),
            "range_minutes": c.range_minutes,
            "fixed_sl_pts": c.fixed_sl_pts,
            "rr_ratio": c.rr_ratio,
            "half_tp_ratio": c.half_tp_ratio,
            "pending_expire_minutes": c.pending_expire_minutes,
            "fractal_confirm": c.fractal_confirm,
            "fractal_width": c.fractal_width,
            "sma_cross_exit": c.sma_cross_exit,
            "sma_cross_fast": c.sma_cross_fast,
            "sma_cross_slow": c.sma_cross_slow,
        })
    pd.DataFrame(rows).to_csv(OUT_DIR / "oos_rank.csv", index=False)
    print(f"[DONE] Wrote {OUT_DIR / 'oos_rank.csv'}", flush=True)
    kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
