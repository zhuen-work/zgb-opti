"""Compare V2-on vs V2-off ranks from the focused WFO.

Loads the 144-config IS parquets, splits by fractal_confirm flag, selects
top-N robust candidates from each subset (V2-off and V2-on), runs OOS on
both candidate lists, prints a side-by-side ranking.

V2-on OOS is already cached from the main WFO run.
V2-off OOS will be computed fresh here.
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.sweep_orb import run_sweep
from zgb_sim.wfo_helpers import WINDOWS_MAY23 as WINDOWS

SYMBOL = "XAUUSD"
RISK_PCT = 6.0
DEPOSIT = 10_000.0
N_WORKERS = 6
SIGNAL_TF = "M5"
OUT_DIR = ROOT / "output" / "wfo_orb_v2_may23"
TOP_N = 10


def _to_utc(d):
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def select_robust_from_subset(is_per_window, subset_filter, top_n=30, max_candidates=10):
    """Pick robust candidates from a filtered subset of IS results."""
    counts = {}
    for label, df in is_per_window.items():
        sub = df[subset_filter(df)]
        prof = sub[(sub["net_profit"] > 0) & (sub["trades"] >= 5) & sub["error"].isna()]
        top = prof.sort_values("recovery_factor", ascending=False).head(top_n)
        for _, row in top.iterrows():
            k = (int(row["range_minutes"]), int(row["fixed_sl_pts"]),
                 round(float(row["rr_ratio"]), 2),
                 round(float(row["half_tp_ratio"]), 2),
                 int(row["pending_expire_minutes"]),
                 bool(row["fractal_confirm"]), int(row["fractal_width"]))
            c = counts.setdefault(k, {"count": 0, "total_rf": 0.0, "sample_row": row})
            c["count"] += 1
            c["total_rf"] += float(row["recovery_factor"])
    robust = [(k, info) for k, info in counts.items() if info["count"] >= 2]
    if not robust:
        combined = sorted(counts.items(), key=lambda x: -x[1]["total_rf"])
        robust = combined[:max_candidates * 2]
    robust.sort(key=lambda x: -x[1]["total_rf"])
    return [info["sample_row"] for _, info in robust[:max_candidates]]


def row_to_cfg(r) -> ORBConfig:
    return ORBConfig(
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
        fractal_confirm=bool(r["fractal_confirm"]),
        fractal_width=int(r["fractal_width"]),
        comment="ORB",
    )


def aggregate_oos(cands, oos_per):
    rows = []
    for i, cfg in enumerate(cands):
        prof = 0; total_np = 0.0; dds = []
        for label, _, _, _, _ in WINDOWS:
            r = oos_per[label].iloc[i]
            total_np += float(r["net_profit"])
            dds.append(float(r["drawdown_pct"]))
            if r["net_profit"] > 0: prof += 1
        avg_dd = sum(dds) / len(dds) if dds else 0.5
        rows.append({
            "cfg": cfg, "prof_count": prof, "total_np": total_np,
            "avg_dd_pct": avg_dd,
            "np_dd_ratio": total_np / max(avg_dd, 0.5),
        })
    rows.sort(key=lambda x: (x["prof_count"], x["np_dd_ratio"]), reverse=True)
    return rows


def fmt_cfg(c: ORBConfig) -> str:
    fc = f"V2_w{c.fractal_width}" if c.fractal_confirm else "OFF  "
    return (f"Range={c.range_minutes:>3} SL={c.fixed_sl_pts:>4} "
            f"RR={c.rr_ratio:<3} HTP={c.half_tp_ratio:<3} {fc}")


def main():
    is_per = {label: pd.read_parquet(OUT_DIR / f"p1_is_{label}.parquet")
              for label, _, _, _, _ in WINDOWS}

    # V2-OFF candidates: from baseline subset only
    off_rows = select_robust_from_subset(is_per,
                                           lambda df: ~df["fractal_confirm"],
                                           top_n=30, max_candidates=TOP_N)
    off_cands = [row_to_cfg(r) for r in off_rows]
    # V2-ON candidates: from V2 subset (width 3 or 5)
    on_rows = select_robust_from_subset(is_per,
                                          lambda df: df["fractal_confirm"],
                                          top_n=30, max_candidates=TOP_N)
    on_cands = [row_to_cfg(r) for r in on_rows]

    print(f"V2-OFF candidates: {len(off_cands)}; V2-ON candidates: {len(on_cands)}")

    # Run OOS for V2-OFF (fresh)
    m = symbol_meta(SYMBOL)
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])

    off_oos = {}
    for label, _, _, oos_s, oos_e in WINDOWS:
        cache = OUT_DIR / f"compare_oos_off_{label}.parquet"
        df = run_sweep(off_cands, SYMBOL, _to_utc(oos_s), _to_utc(oos_e),
                       meta, initial_balance=DEPOSIT,
                       n_workers=min(N_WORKERS, len(off_cands)),
                       cache_path=cache, window_label=f"OFF-{label}", signal_tf=SIGNAL_TF)
        off_oos[label] = df

    # Run OOS for V2-ON (uses cached p1_oos parquets if candidates match — else recomputes)
    on_oos = {}
    for label, _, _, oos_s, oos_e in WINDOWS:
        cache = OUT_DIR / f"compare_oos_on_{label}.parquet"
        df = run_sweep(on_cands, SYMBOL, _to_utc(oos_s), _to_utc(oos_e),
                       meta, initial_balance=DEPOSIT,
                       n_workers=min(N_WORKERS, len(on_cands)),
                       cache_path=cache, window_label=f"ON-{label}", signal_tf=SIGNAL_TF)
        on_oos[label] = df

    kill_mt5_terminal()

    off_ranked = aggregate_oos(off_cands, off_oos)
    on_ranked = aggregate_oos(on_cands, on_oos)

    print("\n" + "=" * 88)
    print(f"  V2-OFF (baseline) top-{TOP_N} OOS — best of {len(off_cands)} robust candidates")
    print("=" * 88)
    for i, info in enumerate(off_ranked):
        c = info["cfg"]
        print(f"  #{i+1} prof={info['prof_count']}/4  total_np=${info['total_np']:>+8,.0f}  "
              f"NP/DD%={info['np_dd_ratio']:>7.1f}  {fmt_cfg(c)}")

    print("\n" + "=" * 88)
    print(f"  V2-ON top-{TOP_N} OOS — best of {len(on_cands)} robust candidates")
    print("=" * 88)
    for i, info in enumerate(on_ranked):
        c = info["cfg"]
        print(f"  #{i+1} prof={info['prof_count']}/4  total_np=${info['total_np']:>+8,.0f}  "
              f"NP/DD%={info['np_dd_ratio']:>7.1f}  {fmt_cfg(c)}")

    if off_ranked and on_ranked:
        print("\n" + "=" * 88)
        print("  HEAD-TO-HEAD")
        print("=" * 88)
        off1 = off_ranked[0]; on1 = on_ranked[0]
        d_np = on1["total_np"] - off1["total_np"]
        d_pf = (on1["total_np"] / max(off1["total_np"], 1)) if off1["total_np"] > 0 else 0
        print(f"  Best baseline: {fmt_cfg(off1['cfg'])}")
        print(f"    prof={off1['prof_count']}/4  total_np=${off1['total_np']:>+8,.0f}  NP/DD%={off1['np_dd_ratio']:.1f}")
        print(f"  Best V2_on:    {fmt_cfg(on1['cfg'])}")
        print(f"    prof={on1['prof_count']}/4  total_np=${on1['total_np']:>+8,.0f}  NP/DD%={on1['np_dd_ratio']:.1f}")
        print(f"  Delta:         NP +${d_np:>+,.0f} ({d_pf:.2f}x)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
