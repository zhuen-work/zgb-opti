"""WFO for ORB on NAS100.r — Vantage mini-index CFD.

Conditions: $10k deposit, 3% risk, default per-symbol sim spread (250pt for NAS100.r).
Pending expire = 240 min (matches gold setfile convention).

Grid is scaled ~3x from gold WFO because:
  - NAS LDN-90min median range = 4,062pt (gold = 1,678pt; ratio ~2.4x)
  - NAS NY-90min median range = 6,100pt (gold = 2,013pt; ratio ~3.0x)
  - Daily range NAS = 44,025pt (gold = 10,190pt; ratio ~4.3x)

So pt-denominated SL/range live in different bands. RR/HTP semantics carry over.

Phase 1 grid: 840 combos × 4 windows = 3,360 sims:
  range_minutes:    45, 60, 90, 105, 120, 150               (6)
  buffer_pts:       0                                        (1, fixed)
  fixed_sl_pts:     800, 1200, 1500, 2000, 2500, 3000, 3500  (7)
  rr_ratio:         1.0, 1.5, 2.0, 3.0, 4.0                  (5, extended down per 2026-05-10 boundary check)
  half_tp_ratio:    0.0, 0.25, 0.5, 0.75                     (4)
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
from zgb_sim.orb import ORBConfig, simulate
from zgb_sim.sweep_orb import run_sweep


SYMBOL = "NAS100.r"
RISK_PCT = 3.0
DEPOSIT = 10_000.0
N_WORKERS = 6
SIGNAL_TF = "M5"
PENDING_EXPIRE_MIN = 240

# NAS-specific range filter (3x gold's 200/5000)
MIN_RANGE_PTS = 600
MAX_RANGE_PTS = 15000

from zgb_sim.wfo_helpers import (WINDOWS_MAY9 as WINDOWS, rank_with_p0,
                                  print_phase_d_with_p0, select_winner_with_p0,
                                  check_winner_boundaries, print_boundary_check)

PREWARM_START = date(2026, 2, 19)   # 2-day pad before W1 IS start (Feb 21)
PREWARM_END   = date(2026, 5,  9)


def session_flags(session: str):
    return (session in ("ldn", "both"), session in ("ny", "both"))


DATE_TAG = "may9"

def out_dir_for(session: str) -> Path:
    if session == "both":
        return ROOT / "output" / f"wfo_orb_nas_{DATE_TAG}"
    return ROOT / "output" / f"wfo_orb_nas_{session}_{DATE_TAG}"


def build_entry_grid(session: str, tiny=False) -> list[ORBConfig]:
    ldn_on, ny_on = session_flags(session)
    if tiny:
        return [ORBConfig(
            risk_pct=RISK_PCT, range_minutes=60, buffer_pts=0,
            min_range_pts=MIN_RANGE_PTS, max_range_pts=MAX_RANGE_PTS,
            fixed_sl_pts=1500, rr_ratio=3.0, half_tp_ratio=0.0,
            pending_expire_minutes=PENDING_EXPIRE_MIN,
            daily_target_pct=0.0, daily_loss_pct=0.0,
            ldn_enabled=ldn_on, ldn_start_hour=7,
            ny_enabled=ny_on, ny_start_hour=13,
            comment="ORB",
        )]
    grid = []
    for range_min in (45, 60, 90, 105, 120, 150):                  # 6
        for fixed_sl in (800, 1200, 1500, 2000, 2500, 3000, 3500): # 7  (~3x gold)
            for rr in (1.0, 1.5, 2.0, 3.0, 4.0):                   # 5 (RR=1.0/1.5 added 2026-05-10)
                for htp in (0.0, 0.25, 0.5, 0.75):                 # 4
                    grid.append(ORBConfig(
                        risk_pct=RISK_PCT,
                        range_minutes=range_min,
                        buffer_pts=0,
                        min_range_pts=MIN_RANGE_PTS,
                        max_range_pts=MAX_RANGE_PTS,
                        fixed_sl_pts=fixed_sl,
                        rr_ratio=rr,
                        half_tp_ratio=htp,
                        pending_expire_minutes=PENDING_EXPIRE_MIN,
                        daily_target_pct=0.0,
                        daily_loss_pct=0.0,
                        ldn_enabled=ldn_on, ldn_start_hour=7,
                        ny_enabled=ny_on, ny_start_hour=13,
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


def run_is_phase(configs, meta, out_dir: Path, cache_prefix: str = ""):
    per = {}
    for label, is_s, is_e, _, _ in WINDOWS:
        cache = out_dir / f"{cache_prefix}is_{label}.parquet"
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
        print(f"    NP=${r['net_profit']:>+8,.0f}  DD={r['drawdown_pct']:>4.1f}%  "
              f"Tr={int(r['trades']):>3}  RF={r['recovery_factor']:>5.0f}  "
              f"Range={int(r['range_minutes'])} FixSL={int(r['fixed_sl_pts'])} "
              f"RR={r['rr_ratio']} HTP={r['half_tp_ratio']}")


def select_robust(per_window, session: str = "both", top_n=30, max_candidates=15):
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

    ldn_on, ny_on = session_flags(session)
    cands = []
    for k, info in unique:
        r = info["sample_row"]
        cands.append(ORBConfig(
            risk_pct=RISK_PCT,
            range_minutes=int(r["range_minutes"]),
            buffer_pts=0,
            min_range_pts=MIN_RANGE_PTS, max_range_pts=MAX_RANGE_PTS,
            fixed_sl_pts=int(r["fixed_sl_pts"]),
            rr_ratio=float(r["rr_ratio"]),
            half_tp_ratio=round(float(r["half_tp_ratio"]), 2),
            pending_expire_minutes=PENDING_EXPIRE_MIN,
            daily_target_pct=float(r["daily_target_pct"]),
            daily_loss_pct=float(r["daily_loss_pct"]),
            ldn_enabled=ldn_on, ldn_start_hour=7,
            ny_enabled=ny_on, ny_start_hour=13,
            comment="ORB",
        ))
        print(f"    #{len(cands)} Range={k[0]} FixSL={k[2]} RR={k[3]} HTP={k[4]}  "
              f"windows={info['windows']}")
    return cands


def run_oos_phase(candidates, meta, out_dir: Path, cache_prefix: str = ""):
    per = {}
    for label, _, _, oos_s, oos_e in WINDOWS:
        cache = out_dir / f"{cache_prefix}oos_{label}.parquet"
        df = run_sweep(candidates, SYMBOL, _to_utc(oos_s), _to_utc(oos_e),
                       meta, initial_balance=DEPOSIT,
                       n_workers=min(N_WORKERS, len(candidates)),
                       cache_path=cache, window_label=f"OOS-{label}", signal_tf=SIGNAL_TF)
        per[label] = df
    return per


def sanity_et(cfg: ORBConfig, meta: SymbolMeta, session_label: str = "ORB"):
    print("\n" + "=" * 72)
    from zgb_sim.tick_loader import SYMBOL_DEFAULT_SPREAD_PTS
    sp = SYMBOL_DEFAULT_SPREAD_PTS.get(SYMBOL, "?")
    print(f"  SANITY ET {session_label} (Apr 1 -> May 9, $10k, 3% risk, {sp}pt friction)")
    print("=" * 72)
    full_start = _to_utc(date(2026, 4, 1))
    full_end = _to_utc(date(2026, 5, 9))
    ticks = load_ticks(SYMBOL, full_start, full_end)
    m1 = load_bars(SYMBOL, "M1", full_start, full_end)
    m5 = load_bars(SYMBOL, "M5", full_start, full_end)
    r = simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
    print(f"\n  Sanity: {r.summary()}")


def main():
    import json
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiny", action="store_true",
                    help="single-config smoke test instead of full grid")
    ap.add_argument("--session", choices=("ldn", "ny", "both"), default="both")
    args = ap.parse_args()

    out_dir = out_dir_for(args.session)
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_prefix = "p1_"

    try:
        from zgb_sim.tick_loader import SYMBOL_DEFAULT_SPREAD_PTS
        sim_spread = SYMBOL_DEFAULT_SPREAD_PTS.get(SYMBOL)

        configs = build_entry_grid(args.session, tiny=args.tiny)

        print("=" * 84)
        print(f"  WFO ORB NAS100.r session={args.session}  Phase 1 (entry sweep, caps=0)")
        print(f"  {sim_spread}pt friction, 3% risk, $10k -- {len(configs)} combos")
        print(f"  out_dir: {out_dir}")
        print("=" * 84)

        # Pull bars once (NAS100.r M1/M5 not yet cached). Connect to sim acct first.
        from zgb_sim.mt5_accounts import init_account
        init_account("sim")

        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        print(f"  Workers: {N_WORKERS}  Deposit: ${DEPOSIT:,.0f}  Risk: {RISK_PCT}%")
        print(f"  Symbol meta: tick_value=${m['tick_value']} volume_min={m['volume_min']}")

        print("\n  Pre-warming ticks + bars...")
        t0 = time.time()
        _ = load_ticks(SYMBOL, _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        _ = load_bars(SYMBOL, "M1", _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        _ = load_bars(SYMBOL, SIGNAL_TF, _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        print(f"  Pre-warm done in {time.time()-t0:.1f}s")

        print("\n=== PHASE A: IS Sweep ===")
        is_per = run_is_phase(configs, meta, out_dir, cache_prefix)
        for label, df in is_per.items():
            print_top5(df, f"IS-{label}")

        print("\n=== PHASE B: Robust ===")
        candidates = select_robust(is_per, session=args.session)
        if not candidates:
            print("No candidates."); return

        print("\n=== PHASE C: OOS ===")
        oos_per = run_oos_phase(candidates, meta, out_dir, cache_prefix)
        for label, df in oos_per.items():
            print(f"\n  OOS-{label}:")
            for i, r in df.iterrows():
                print(f"    #{i+1} NP={r['net_profit']:+,.2f} ({r['return_pct']:+.1f}%)  "
                      f"PF={r['profit_factor']:.2f}  DD={r['drawdown_pct']:.1f}%  Tr={int(r['trades'])}")

        ranked = rank_with_p0(candidates, oos_per, WINDOWS, decay_threshold=-0.25,
                                grid_configs=configs, is_per_window=is_per)
        rank_label = f"NAS100.r ORB_{args.session.upper()} P1"
        print_phase_d_with_p0(ranked, rank_label)

        winner_row = select_winner_with_p0(ranked)
        if winner_row is None:
            print(f"\n  WARNING: 0 candidates passed P0. Using fallback (top by ranking).")
            winner_row = ranked[0]
        winner = winner_row["cfg"]
        print(f"\n  WINNER: Range={winner.range_minutes} FixSL={winner.fixed_sl_pts} "
              f"RR={winner.rr_ratio} HTP={winner.half_tp_ratio}  "
              f"slope={winner_row['slope']:+.1%}  P0={'PASS' if winner_row['p0_pass'] else 'FAIL'}")
        boundary_rows = check_winner_boundaries(winner, configs)
        boundary_at_edge = print_boundary_check(boundary_rows)

        winner_path = out_dir / "winner_p1.json"
        winner_payload = {
            "cfg": asdict(winner),
            "session": args.session,
            "phase": 1,
            "p0_pass": winner_row["p0_pass"],
            "slope": winner_row["slope"],
            "oos_nps": winner_row["oos_nps"],
            "total_np": winner_row["total_np"],
            "plateau_score": winner_row.get("plateau_score"),
            "n_neighbors": winner_row.get("n_neighbors", 0),
            "boundary_at_edge": boundary_at_edge,
            "boundary_rows": boundary_rows,
        }
        winner_path.write_text(json.dumps(winner_payload, indent=2, default=str), encoding="utf-8")
        print(f"  Winner persisted: {winner_path}")
        sanity_et(winner, meta, session_label=rank_label)
        print("\n=== Done ===")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
