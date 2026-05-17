"""WFO for DT818_pro ORB stream. Uses SIM_SPREAD_PTS default (currently 55pt friction).

Conditions: $10k deposit, 3% risk (DT818_pro convention), 70pt spread.
Pending expire = 240 min (matches live setfile).
4-fold WFO: IS 4w / OOS 2w each. Phases A (IS sweep) → B (robust) → C (OOS) → D (rank).

Grid expanded 2026-05-03 in response to boundary check + live-config calibration.
Range expansion: range_minutes added {105, 120, 150} (prior winner was at MAX).
FixSL expansion: added 400 to make live ORB config (Range=60/SL=400/RR=3.0)
a reachable grid point for proper comparison.

**Two-phase approach** (2026-05-03): Phase 1 sweeps entry/exit params with daily
caps DISABLED (matches live which runs uncapped). Phase 2 (separate script) tunes
caps against Phase 1 winner. Caps and entry mechanics are weakly coupled, so this
saves ~9x compute vs sweeping the cartesian product.

Phase 1 grid: 1,125 combos × 4 windows = 4,500 sims at 60pt friction, 6% risk:
  range_minutes:    60, 90, 120                              (3, 30-step from 60)
  buffer_pts:       0                                        (1, fixed)
  min_range_pts:    0                                        (range filter DISABLED to match live)
  max_range_pts:    999999                                   (range filter DISABLED to match live)
  fixed_sl_pts:     400, 550, 700, 850, 1000                 (5, 150-step from 400)
  rr_ratio:         2.0, 2.5, 3.0, 3.5, 4.0                  (5, 0.5-step from 2.0)
  half_tp_ratio:    0.0, 0.2, 0.4, 0.6, 0.8                  (5, 0.2-step from 0.0)
  pending_expire_minutes: 120, 240, 360                      (3, 120-step from 120)
  daily_target_pct: 0 (Phase 2 cap sweep DISABLED 2026-05-15)
  daily_loss_pct:   0 (Phase 2 cap sweep DISABLED 2026-05-15)
  risk_pct:         6.0 (bumped 3->6 on 2026-05-15)
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


SYMBOL = "XAUUSD"
RISK_PCT = 6.0   # bumped 3->6 on 2026-05-15 per user; matches sweep risk closer to total live risk envelope
DEPOSIT = 10_000.0
N_WORKERS = 6
SIGNAL_TF = "M5"
PENDING_EXPIRE_MIN = 240  # legacy default; sweep dim added 2026-05-15 (see grid below)

# !!! BROKER-TIME GOTCHA — read reference_vantage_broker_time.md !!!
# MT5 (Vantage) returns timestamps in BROKER LOCAL TIME (currently UTC+3, EEST/DST).
# Python labels them as UTC but they're actually broker-time epoch.
# Below `ldn_start_hour=7` selects ticks where ts.hour == 7 in the broker-time
# tick stream = REAL UTC 04:00 (Asian-quiet hour, NOT actual LDN session).
# Similarly `ny_start_hour=13` = broker hour 13 = REAL UTC 10:00 (mid-LDN, not NY).
# The live EA uses MQL5 TimeGMT() which IS real UTC, so live trades the actual
# LDN (real UTC 07:00) and NY (real UTC 13:00) sessions — DIFFERENT from what
# this WFO optimizes. To make the WFO match live, set ldn_start_hour=10 and
# ny_start_hour=16 (broker labels for real UTC 07/13). See sim_wfo_orb_realutc.py
# for that variant. Verified 2026-05-11.

from zgb_sim.wfo_helpers import (WINDOWS_MAY9, WINDOWS_MAY16, WINDOWS_MAY23,
                                  rank_with_p0,
                                  print_phase_d_with_p0, select_winner_with_p0,
                                  check_winner_boundaries, print_boundary_check)

# Default WFO window set. Override via env: ZGB_WFO_WINDOWS={may9|may16|may23}.
# Each MAY{N} windows set is 1 week rolled forward from MAY{N-7}.
import os as _os
_WFO_WIN_TAG = _os.environ.get("ZGB_WFO_WINDOWS", "may9").lower()
if _WFO_WIN_TAG == "may23":
    WINDOWS = WINDOWS_MAY23
    PREWARM_START = date(2026, 3,  5)  # MAY23 W1 IS starts Mar 7, 2-day pad
    PREWARM_END   = date(2026, 5, 23)
elif _WFO_WIN_TAG == "may16":
    WINDOWS = WINDOWS_MAY16
    PREWARM_START = date(2026, 2, 26)
    PREWARM_END   = date(2026, 5, 16)
else:
    WINDOWS = WINDOWS_MAY9
    PREWARM_START = date(2026, 2, 19)  # MAY9 W1 IS starts Feb 21, give 2-day pad
    PREWARM_END   = date(2026, 5,  9)


def session_flags(session: str):
    """Return (ldn_enabled, ny_enabled) for given session selector."""
    return (session in ("ldn", "both"), session in ("ny", "both"))


DATE_TAG = _WFO_WIN_TAG  # may9 or may16 depending on ZGB_WFO_WINDOWS

def out_dir_for(session: str) -> Path:
    if session == "both":
        return ROOT / "output" / f"wfo_orb_{DATE_TAG}"
    return ROOT / "output" / f"wfo_orb_{session}_{DATE_TAG}"


def build_entry_grid(session: str, tiny=False) -> list[ORBConfig]:
    """Phase 1: entry params (range/SL/RR/HTP) with caps=0/0."""
    ldn_on, ny_on = session_flags(session)
    if tiny:
        return [ORBConfig(
            risk_pct=RISK_PCT, range_minutes=60, buffer_pts=0,
            min_range_pts=200, max_range_pts=5000,
            fixed_sl_pts=400, rr_ratio=3.0, half_tp_ratio=0.0,
            pending_expire_minutes=PENDING_EXPIRE_MIN,
            daily_target_pct=0.0, daily_loss_pct=0.0,
            ldn_enabled=ldn_on, ldn_start_hour=7,
            ny_enabled=ny_on, ny_start_hour=13,
            comment="ORB",
        )]
    grid = []
    for range_min in (60, 90, 120):                            # 3 (30-step 60-120, per user 2026-05-16)
        for fixed_sl in (400, 550, 700, 850, 1000):            # 5 (150-step 400-1000, per user 2026-05-16)
            for rr in (2.0, 2.5, 3.0, 3.5, 4.0):               # 5 (0.5-step 2.0-4.0, per user 2026-05-16)
                for htp in (0.0, 0.2, 0.4, 0.6, 0.8):          # 5 (0.2-step 0.0-0.8)
                    for expire_min in (120, 240, 360):         # 3 (120-step 120-360, per user 2026-05-16)
                        grid.append(ORBConfig(
                            risk_pct=RISK_PCT,
                            range_minutes=range_min,
                            buffer_pts=0,
                            min_range_pts=0,         # range filter DISABLED to match live (was 200)
                            max_range_pts=999999,    # range filter DISABLED to match live (was 5000)
                            fixed_sl_pts=fixed_sl,
                            rr_ratio=rr,
                            half_tp_ratio=htp,
                            pending_expire_minutes=expire_min,
                            daily_target_pct=0.0,
                            daily_loss_pct=0.0,
                            ldn_enabled=ldn_on, ldn_start_hour=7,
                            ny_enabled=ny_on, ny_start_hour=13,
                            comment="ORB",
                        ))
    return grid


def build_cap_grid(p1_winner_cfg: dict, session: str) -> list[ORBConfig]:
    """Phase 2: cap sweep (5x5=25) with entry params fixed from P1 winner."""
    ldn_on, ny_on = session_flags(session)
    grid = []
    for tgt in (0.0, 5.0, 9.0, 15.0, 27.0):              # 5
        for loss in (0.0, 4.0, 6.0, 10.0, 18.0):         # 5
            grid.append(ORBConfig(
                risk_pct=RISK_PCT,
                range_minutes=int(p1_winner_cfg["range_minutes"]),
                buffer_pts=int(p1_winner_cfg.get("buffer_pts", 0)),
                min_range_pts=int(p1_winner_cfg.get("min_range_pts", 200)),
                max_range_pts=int(p1_winner_cfg.get("max_range_pts", 5000)),
                fixed_sl_pts=int(p1_winner_cfg["fixed_sl_pts"]),
                rr_ratio=float(p1_winner_cfg["rr_ratio"]),
                half_tp_ratio=float(p1_winner_cfg["half_tp_ratio"]),
                pending_expire_minutes=PENDING_EXPIRE_MIN,
                daily_target_pct=tgt,
                daily_loss_pct=loss,
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
              f"RR={r['rr_ratio']} HTP={r['half_tp_ratio']} "
              f"Tgt={r['daily_target_pct']}% Loss={r['daily_loss_pct']}%")


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
            min_range_pts=200, max_range_pts=5000,
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
        print(f"    #{len(cands)} Range={k[0]} FixSL={k[2]} RR={k[3]} HTP={k[4]} "
              f"Tgt={k[5]}% Loss={k[6]}%  windows={info['windows']}")
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
        "; DT818_pro ORB-only WFO @ default spread (55pt friction), 3% risk, $10k",
        "; Reopt date: 2026-05-01  Next reopt: 2026-05-09",
        "; IS 4w / OOS 2w × 3 windows on Feb 14 -> Apr 25, 2026",
        ";",
        "_ORB_Magic=2000||2000||1||2000||2000||N",
        "_ORB_Comment=ORB",
        "_ORB_Enabled=true",
        f"_ORB_RangeMinutes={c['range_minutes']}||{c['range_minutes']}||1||{c['range_minutes']}||{c['range_minutes']}||N",
        f"_ORB_BufferPts={c['buffer_pts']}||{c['buffer_pts']}||1||{c['buffer_pts']}||{c['buffer_pts']}||N",
        f"_ORB_MinRangePts={c['min_range_pts']}||{c['min_range_pts']}||1||{c['min_range_pts']}||{c['min_range_pts']}||N",
        f"_ORB_MaxRangePts={c['max_range_pts']}||{c['max_range_pts']}||1||{c['max_range_pts']}||{c['max_range_pts']}||N",
        f"_ORB_FixedSL_Pts={c['fixed_sl_pts']}||{c['fixed_sl_pts']}||1||{c['fixed_sl_pts']}||{c['fixed_sl_pts']}||N",
        f"_ORB_RR_Ratio={c['rr_ratio']}||{c['rr_ratio']}||1||{c['rr_ratio']}||{c['rr_ratio']}||N",
        f"_ORB_HalfTP_Ratio={c['half_tp_ratio']}||{c['half_tp_ratio']}||1||{c['half_tp_ratio']}||{c['half_tp_ratio']}||N",
        f"_ORB_PendingExpireMinutes={c['pending_expire_minutes']}||{c['pending_expire_minutes']}||1||{c['pending_expire_minutes']}||{c['pending_expire_minutes']}||N",
        f"_ORB_DailyTargetPct={c['daily_target_pct']}||{c['daily_target_pct']}||1||{c['daily_target_pct']}||{c['daily_target_pct']}||N",
        f"_ORB_DailyLossPct={c['daily_loss_pct']}||{c['daily_loss_pct']}||1||{c['daily_loss_pct']}||{c['daily_loss_pct']}||N",
        "_ORB_LDN_Enabled=true",
        "_ORB_LDN_StartHour=7||7||1||7||7||N",
        "_ORB_NY_Enabled=true",
        "_ORB_NY_StartHour=13||13||1||13||13||N",
        "_BrokerGMTOffsetHours=3||3||1||3||3||N",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def sanity_et(cfg: ORBConfig, meta: SymbolMeta, session_label: str = "ORB"):
    print("\n" + "=" * 72)
    from zgb_sim.tick_loader import SIM_SPREAD_PTS
    print(f"  SANITY ET {session_label} (Mar 14 -> May 2, $10k, 3% risk, {SIM_SPREAD_PTS}pt friction)")
    print("=" * 72)
    full_start = _to_utc(date(2026, 3, 14))
    full_end = _to_utc(date(2026, 5, 1))
    ticks = load_ticks(SYMBOL, full_start, full_end)
    m1 = load_bars(SYMBOL, "M1", full_start, full_end)
    m5 = load_bars(SYMBOL, "M5", full_start, full_end)
    r = simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
    print(f"\n  Sanity: {r.summary()}")


def main():
    import json
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiny", action="store_true")
    ap.add_argument("--session", choices=("ldn", "ny", "both"), default="both",
                    help="Which session(s) enabled in this WFO sweep")
    ap.add_argument("--phase", type=int, choices=(1, 2), default=1,
                    help="1 = entry sweep with caps=0; 2 = cap sweep with entry from P1 winner")
    args = ap.parse_args()

    out_dir = out_dir_for(args.session)
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_prefix = f"p{args.phase}_"

    try:
        from zgb_sim.tick_loader import SIM_SPREAD_PTS

        # Build phase-specific grid
        if args.phase == 1:
            configs = build_entry_grid(args.session, tiny=args.tiny)
            phase_label = "P1 (entry sweep, caps=0)"
            winner_filename = "winner_p1.json"
        else:
            p1_path = out_dir / "winner_p1.json"
            if not p1_path.exists():
                print(f"ERROR: phase 2 needs {p1_path} from a prior phase 1 run.")
                return 1
            p1 = json.loads(p1_path.read_text())
            configs = build_cap_grid(p1["cfg"], args.session)
            phase_label = "P2 (cap sweep, entry fixed from P1)"
            winner_filename = "winner.json"

        print("=" * 84)
        print(f"  WFO ORB session={args.session} phase={args.phase}  {phase_label}")
        print(f"  {SIM_SPREAD_PTS}pt friction, {RISK_PCT}% risk, $10k -- {len(configs)} combos")
        print(f"  out_dir: {out_dir}")
        print("=" * 84)

        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        print(f"  Workers: {N_WORKERS}  Deposit: ${DEPOSIT:,.0f}  Risk: {RISK_PCT}%")

        print("\n  Pre-warming (ensuring sim account selected for XAUUSD)...")
        from zgb_sim.mt5_accounts import init_account
        init_account("sim")  # connect to 18912087 / XAUUSD before tick/bar fetches
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
        rank_label = f"ORB_{args.session.upper()} {phase_label}"
        print_phase_d_with_p0(ranked, rank_label)

        winner_row = select_winner_with_p0(ranked)
        if winner_row is None:
            print(f"\n  WARNING: 0 candidates passed P0. Using fallback (top by ranking).")
            winner_row = ranked[0]
        winner = winner_row["cfg"]
        print(f"\n  WINNER: Range={winner.range_minutes} FixSL={winner.fixed_sl_pts} "
              f"RR={winner.rr_ratio} HTP={winner.half_tp_ratio} "
              f"Tgt={winner.daily_target_pct}% Loss={winner.daily_loss_pct}%  "
              f"slope={winner_row['slope']:+.1%}  P0={'PASS' if winner_row['p0_pass'] else 'FAIL'}")
        boundary_rows = check_winner_boundaries(winner, configs)
        boundary_at_edge = print_boundary_check(boundary_rows)

        winner_path = out_dir / winner_filename
        winner_payload = {
            "cfg": asdict(winner),
            "session": args.session,
            "phase": args.phase,
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

        # Auto-refresh dashboard projection (View C + Weekly Projection cards)
        # so the dashboard always reflects the latest WFO winner without a
        # manual save_forward_projection.py run. Fails-open: dashboard push
        # errors don't fail the WFO.
        try:
            print("\n=== Refreshing dashboard projection ===")
            import subprocess as _sp
            r = _sp.run(
                [sys.executable, "-u", str(ROOT / "scripts" / "save_forward_projection.py")],
                capture_output=True, text=True, timeout=600,
            )
            tail = (r.stdout or "").splitlines()[-3:]
            for line in tail:
                print(f"  {line}")
            if r.returncode != 0:
                print(f"  [warn] save_forward_projection exit {r.returncode}: {r.stderr[:200] if r.stderr else ''}")
        except Exception as e:
            print(f"  [warn] dashboard projection refresh skipped: {type(e).__name__}: {e}")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
