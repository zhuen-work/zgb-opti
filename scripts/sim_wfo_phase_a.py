"""Full WFO using the Python simulator (no MT5 tester involvement).

Flow:
  1. Build config grid (576 combos).
  2. For each IS window: parallel sweep -> top-20 by recovery factor.
  3. Cross-window robust filter -> top-5 unique candidates.
  4. OOS validation on each OOS window.
  5. Rank by total OOS NP, write winner setfile.
  6. Sanity ET of winner on full span with debug logging.

Windows: 3 x (4w IS / 2w OOS) ending 2026-04-18.
"""
from __future__ import annotations

import argparse
import itertools
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone, date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import S1Config, SymbolMeta, simulate
from zgb_sim.sweep import run_sweep


SYMBOL = "XAUUSD"
RISK_PCT = 1.0
DEPOSIT = 100.0
N_WORKERS = 8

# 3-window WFO ending 2026-04-25 (rolling 2w shift, 4w IS / 2w OOS)
WINDOWS = [
    ("W1", date(2026, 2, 14), date(2026, 3, 14), date(2026, 3, 14), date(2026, 3, 28)),
    ("W2", date(2026, 2, 28), date(2026, 3, 28), date(2026, 3, 28), date(2026, 4, 11)),
    ("W3", date(2026, 3, 14), date(2026, 4, 11), date(2026, 4, 11), date(2026, 4, 25)),
]

# Pre-warm range (pad for load_bars buffer)
PREWARM_START = date(2026, 2, 12)
PREWARM_END   = date(2026, 4, 25)

OUT_DIR = ROOT / "output" / "sim_wfo_phase_a_apr25"
SET_OUT = ROOT / "configs" / "sets" / "scalp_v1_sim_phase_a_apr25.set"


def build_config_grid(tiny: bool = False, hedge: bool = False) -> list[S1Config]:
    """Phase A: extend sweep into wider Donch, wider TP, tighter SL,
    DailyLoss, PendingExpireBars dimensions. HTP fixed at 0.6 (proven winner)."""
    if tiny:
        donch_vals = (30, 35)
        tp_vals = (150, 300)
        sl_vals = (50,)
        htp_vals = (0.6,)
        tgt_vals = (9.0,)
        loss_vals = (12.0,)
        peb_vals = (2,)
    else:
        donch_vals = (25, 30, 35, 40)        # was 15-30; explore wider
        tp_vals = (150, 200, 300)             # was up to 250; add 300
        sl_vals = (40, 50)                     # was 50-100; add tighter 40
        htp_vals = (0.6,)                      # fixed at proven winner
        tgt_vals = (6.0, 9.0, 12.0)            # was 3-9; add 12
        loss_vals = (8.0, 12.0, 18.0)          # NEW: was fixed 12
        peb_vals = (1, 2, 3)                   # NEW: was fixed 2
    grid = []
    for donch in donch_vals:
        for tp in tp_vals:
            for sl in sl_vals:
                for htp in htp_vals:
                    for tgt in tgt_vals:
                        for loss in loss_vals:
                            for peb in peb_vals:
                                grid.append(S1Config(
                                    risk_pct=RISK_PCT,
                                    donchian_bars=donch,
                                    take_profit_pts=tp,
                                    stop_loss_pts=sl,
                                    half_tp_ratio=htp,
                                    pending_expire_bars=peb,
                                    start_hour=14,
                                    end_hour=22,
                                    daily_target_pct=tgt,
                                    daily_loss_pct=loss,
                                    block_fri_pm=True,
                                    hedge_mode=hedge,
                                ))
    return grid


def _to_utc(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _param_key(row) -> tuple:
    """Key used for cross-window robust matching (Phase A: includes peb + loss)."""
    return (
        int(row["donchian_bars"]),
        int(row["take_profit_pts"]),
        int(row["stop_loss_pts"]),
        round(float(row["half_tp_ratio"]), 2),
        round(float(row["daily_target_pct"]), 2),
        round(float(row["daily_loss_pct"]), 2),
        int(row["pending_expire_bars"]),
    )


def run_is_phase(configs: list[S1Config], meta: SymbolMeta) -> dict[str, pd.DataFrame]:
    """Run IS sweep per window. Returns {window_label: DataFrame}."""
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
        )
        per_window[win_label] = df
    return per_window


def print_top5(df: pd.DataFrame, label: str):
    profitable = df[(df["net_profit"] > 0) & (df["trades"] >= 10) & df["error"].isna()]
    top = profitable.sort_values("recovery_factor", ascending=False).head(5)
    print(f"\n  {label}: top-5 by Recovery Factor (of {len(profitable)} profitable):")
    print(f"    {'NP':>10}  {'ROI%':>7}  {'PF':>6}  {'DD':>6}  {'Tr':>4}  {'RF':>8}  Donch  TP  SL  HTP  Tgt  Loss  PEB")
    for _, r in top.iterrows():
        print(f"    {r['net_profit']:>+10,.2f}  {r['return_pct']:>+6.1f}%  "
              f"{r['profit_factor']:>6.2f}  "
              f"{r['drawdown_pct']:>5.1f}%  {int(r['trades']):>4}  {r['recovery_factor']:>8.1f}  "
              f"{int(r['donchian_bars']):>5}  {int(r['take_profit_pts']):>3}  "
              f"{int(r['stop_loss_pts']):>3}  {r['half_tp_ratio']:>4.1f}  "
              f"{int(r['daily_target_pct']):>3}  {int(r['daily_loss_pct']):>4}  {int(r['pending_expire_bars']):>3}")


def select_robust(per_window: dict[str, pd.DataFrame], top_n: int = 20) -> list[S1Config]:
    """Params appearing in top-N of >=2 windows. Dedupe by total NP, take top-5."""
    counts = {}
    for win_label, df in per_window.items():
        profitable = df[(df["net_profit"] > 0) & (df["trades"] >= 10) & df["error"].isna()]
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
        # Fallback: take top-10 from each window's top-N combined
        print("  No robust params! Falling back to top-RF from all windows.")
        combined = list(counts.items())
        combined.sort(key=lambda x: -x[1]["total_rf"])
        robust = combined[:10]

    robust.sort(key=lambda x: -x[1]["total_rf"])
    seen_np = set()
    unique = []
    for k, info in robust:
        np_key = round(info["total_np"])
        if np_key in seen_np:
            continue
        seen_np.add(np_key)
        unique.append((k, info))
        if len(unique) >= 5:
            break

    # Build S1Config from sample_row
    candidates = []
    for k, info in unique:
        r = info["sample_row"]
        candidates.append(S1Config(
            risk_pct=RISK_PCT,
            donchian_bars=int(r["donchian_bars"]),
            take_profit_pts=int(r["take_profit_pts"]),
            stop_loss_pts=int(r["stop_loss_pts"]),
            half_tp_ratio=round(float(r["half_tp_ratio"]), 2),
            pending_expire_bars=int(r["pending_expire_bars"]),
            start_hour=14,
            end_hour=22,
            daily_target_pct=float(r["daily_target_pct"]),
            daily_loss_pct=float(r["daily_loss_pct"]),
            block_fri_pm=True,
            hedge_mode=bool(r["hedge_mode"]),
        ))
        print(f"    #{len(candidates)} Donch={k[0]} TP={k[1]} SL={k[2]} HTP={k[3]} "
              f"Tgt={k[4]} Loss={k[5]} PEB={k[6]}  "
              f"(windows={info['windows']}  total_NP={info['total_np']:+,.0f})")
    return candidates


def run_oos_phase(candidates: list[S1Config], meta: SymbolMeta) -> dict[str, pd.DataFrame]:
    """Run OOS sweep per window with candidates. Returns {window_label: DataFrame}."""
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
        )
        per_window[win_label] = df
    return per_window


def rank_oos(candidates: list[S1Config], oos_per_window: dict[str, pd.DataFrame]):
    """Compute per-candidate OOS totals, return ranked list."""
    rows = []
    for i, cfg in enumerate(candidates):
        total_np = 0.0
        total_tr = 0
        all_prof = True
        per_win = []
        for win_label, _, _, _, _ in WINDOWS:
            r = oos_per_window[win_label].iloc[i]  # preserved order
            per_win.append((win_label, r))
            total_np += float(r["net_profit"])
            total_tr += int(r["trades"])
            if r["net_profit"] <= 0:
                all_prof = False
        rows.append({
            "cfg": cfg, "total_np": total_np, "total_tr": total_tr,
            "all_prof": all_prof, "per_win": per_win,
        })
    rows.sort(key=lambda x: (x["all_prof"], x["total_np"]), reverse=True)
    return rows


def write_setfile(cfg: S1Config, path: Path):
    c = asdict(cfg)
    lines = [
        f"_Magic=2000||2000||1||2000||2000||N",
        f"_EntryTF=5||5||1||5||5||N",
        f"_BlockFriPM={'true' if c['block_fri_pm'] else 'false'}",
        f"_S1_Enabled=true",
        f"_S1_Comment=DT818_S1",
        f"_S1_RiskPct={c['risk_pct']}||{c['risk_pct']}||1||{c['risk_pct']}||{c['risk_pct']}||N",
        f"_S1_DonchianBars={c['donchian_bars']}||{c['donchian_bars']}||1||{c['donchian_bars']}||{c['donchian_bars']}||N",
        f"_S1_TakeProfit={c['take_profit_pts']}||{c['take_profit_pts']}||1||{c['take_profit_pts']}||{c['take_profit_pts']}||N",
        f"_S1_StopLoss={c['stop_loss_pts']}||{c['stop_loss_pts']}||1||{c['stop_loss_pts']}||{c['stop_loss_pts']}||N",
        f"_S1_HalfTP_Ratio={c['half_tp_ratio']}||{c['half_tp_ratio']}||1||{c['half_tp_ratio']}||{c['half_tp_ratio']}||N",
        f"_S1_PendingExpireBars={c['pending_expire_bars']}||{c['pending_expire_bars']}||1||{c['pending_expire_bars']}||{c['pending_expire_bars']}||N",
        f"_S1_TradeStartHour={c['start_hour']}||{c['start_hour']}||1||{c['start_hour']}||{c['start_hour']}||N",
        f"_S1_TradeEndHour={c['end_hour']}||{c['end_hour']}||1||{c['end_hour']}||{c['end_hour']}||N",
        f"_S1_DailyTargetPct={c['daily_target_pct']}||{c['daily_target_pct']}||1||{c['daily_target_pct']}||{c['daily_target_pct']}||N",
        f"_S1_DailyLossPct={c['daily_loss_pct']}||{c['daily_loss_pct']}||1||{c['daily_loss_pct']}||{c['daily_loss_pct']}||N",
        f"_S1_DailyMaxWins=0||0||1||0||0||N",
        f"_S1_DailyMaxLosses=0||0||1||0||0||N",
        f"_S1_HedgeMode={'true' if c['hedge_mode'] else 'false'}",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def sanity_et(cfg: S1Config, meta: SymbolMeta):
    """Full-span re-run of winner across all OOS windows."""
    print("\n" + "=" * 72)
    print("  SANITY ET: winner on full OOS span Mar 14 -> Apr 25")
    print("=" * 72)
    full_start = _to_utc(date(2026, 3, 14))
    full_end = _to_utc(date(2026, 4, 25))
    ticks = load_ticks(SYMBOL, full_start, full_end)
    m1 = load_bars(SYMBOL, "M1", full_start, full_end)
    m5 = load_bars(SYMBOL, "M5", full_start, full_end)
    debug_base = OUT_DIR / "winner_sanity"
    r = simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT,
                 debug_path=str(debug_base))
    print(f"\n  Winner sanity ET: {r.summary()}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiny", action="store_true",
                    help="Small grid (16 configs) for end-to-end sanity (~15 min)")
    ap.add_argument("--hedge", action="store_true",
                    help="Run with hedge_mode=True (mirror SellLimit/BuyLimit at each breakout)")
    ap.add_argument("--out-dir", default=None,
                    help="Override output dir (caches + setfile)")
    args = ap.parse_args()

    global OUT_DIR, SET_OUT
    if args.out_dir:
        OUT_DIR = Path(args.out_dir)
    if args.tiny:
        OUT_DIR = ROOT / "output" / "sim_wfo_phase_a_tiny"
        SET_OUT = ROOT / "configs" / "sets" / "scalp_v1_sim_phase_a_tiny.set"

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        print("=" * 72)
        mode = "TINY (sanity)" if args.tiny else "FULL (576 combos)"
        print(f"  PYSIM WFO (Apr 18) — 3 windows, sim for IS + OOS   [{mode}]")
        print("=" * 72)

        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )
        print(f"  Meta: {m}")
        print(f"  Workers: {N_WORKERS}, Deposit: ${DEPOSIT}, Risk: {RISK_PCT}%")

        # Pre-warm tick/bar cache in parent so workers hit cache cleanly
        print("\n  Pre-warming tick/bar cache for full range...")
        t0 = time.time()
        _ = load_ticks(SYMBOL, _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        _ = load_bars(SYMBOL, "M1", _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        _ = load_bars(SYMBOL, "M5", _to_utc(PREWARM_START), _to_utc(PREWARM_END))
        print(f"  Pre-warm done in {time.time()-t0:.1f}s")

        # Build grid
        configs = build_config_grid(tiny=args.tiny, hedge=args.hedge)
        hedge_label = " HEDGE=ON" if args.hedge else ""
        print(f"\n  Config grid: {len(configs)} combos{hedge_label}")

        # Phase A: IS sweep per window
        print("\n" + "=" * 72)
        print("  PHASE A: IS Sweep")
        print("=" * 72)
        is_per_window = run_is_phase(configs, meta)
        for win_label, df in is_per_window.items():
            print_top5(df, f"IS-{win_label}")

        # Phase B: Cross-window robust filter
        print("\n" + "=" * 72)
        print("  PHASE B: Cross-Window Robust Selection")
        print("=" * 72)
        candidates = select_robust(is_per_window, top_n=20)
        if not candidates:
            print("  No candidates. Aborting.")
            return

        # Phase C: OOS validation
        print("\n" + "=" * 72)
        print("  PHASE C: OOS Validation (pysim, strict real ticks)")
        print("=" * 72)
        oos_per_window = run_oos_phase(candidates, meta)
        for win_label, df in oos_per_window.items():
            print(f"\n  OOS-{win_label} results:")
            for i, r in df.iterrows():
                print(f"    #{i+1} NP={r['net_profit']:+,.2f} ({r['return_pct']:+.1f}%)  "
                      f"PF={r['profit_factor']:.2f}  "
                      f"DD={r['drawdown_pct']:.1f}%  Tr={int(r['trades'])}")

        # Phase D: Final ranking
        print("\n" + "=" * 72)
        print("  PHASE D: FINAL RANKING (by total OOS NP)")
        print("=" * 72)
        ranked = rank_oos(candidates, oos_per_window)
        print(f"\n  {'Rank':<5}{'Total OOS NP':>15}{'ROI%':>9}{'All Prof':>10}  Params")
        for rank, row in enumerate(ranked, 1):
            cfg = row["cfg"]
            flag = "Y" if row["all_prof"] else "N"
            # Return % measured against initial deposit per OOS run
            # (each OOS window starts from DEPOSIT, 3 independent runs)
            total_ret_pct = row["total_np"] / (DEPOSIT * len(WINDOWS)) * 100.0
            print(f"  {rank:<5}{row['total_np']:>+15,.2f}{total_ret_pct:>+7.1f}%{flag:>10}  "
                  f"Donch={cfg.donchian_bars} TP={cfg.take_profit_pts} SL={cfg.stop_loss_pts} "
                  f"HTP={cfg.half_tp_ratio} Tgt={cfg.daily_target_pct} "
                  f"Loss={cfg.daily_loss_pct} PEB={cfg.pending_expire_bars}")

        winner = ranked[0]["cfg"]
        winner_total_np = ranked[0]["total_np"]
        winner_ret_pct = winner_total_np / (DEPOSIT * len(WINDOWS)) * 100.0
        print(f"\n  WINNER: Donch={winner.donchian_bars} TP={winner.take_profit_pts} "
              f"SL={winner.stop_loss_pts} HTP={winner.half_tp_ratio} "
              f"Tgt={winner.daily_target_pct}")
        print(f"          OOS total: ${winner_total_np:+,.2f} ({winner_ret_pct:+.1f}% avg/window, "
              f"${DEPOSIT:.0f} starting deposit, each window independent)")
        write_setfile(winner, SET_OUT)
        print(f"  Setfile written: {SET_OUT}")

        # Phase E: Sanity ET
        sanity_et(winner, meta)

        print("\n" + "=" * 72)
        print("  Done.")
        print("=" * 72)
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
