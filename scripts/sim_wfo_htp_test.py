"""HTP-only sweep: take top W1 base configs (HTP=0) and re-run each at
HTP in {0.0, 0.3, 0.5, 0.7, 0.9} on the same W1 window.

Goal: decide whether HTP is worth adding to the full WFO grid.

Single window (W1 IS = 2026-02-14 -> 2026-03-14), spread 60 pts.
Reads the existing W1 parquet to pick base configs deterministically.
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal
from zgb_sim.scalper_v1 import S1Config, SymbolMeta
from zgb_sim.sweep import run_sweep


SYMBOL = "XAUUSD"
RISK_PCT = 1.0
DEPOSIT = 100.0
N_WORKERS = 8

WIN_START = date(2026, 2, 14)
WIN_END   = date(2026, 3, 14)

W1_PARQUET = ROOT / "output" / "sim_wfo_spread60_deep_apr25" / "is_W1.parquet"
OUT_DIR = ROOT / "output" / "sim_wfo_htp_test_apr25"
OUT_DIR.mkdir(parents=True, exist_ok=True)

HTP_VALUES = (0.0, 0.3, 0.5, 0.7, 0.9)
N_BASES = 12


def load_top_bases(n: int) -> list[dict]:
    """Pick top-N base configs by NP/DD from existing W1 parquet. Deduped on
    the (donch, tp, sl, tgt, loss) key (HTP and PEB held to 0/2)."""
    df = pd.read_parquet(W1_PARQUET)
    df = df[(df["error"].isna() | (df["error"] == "")) &
            (df["trades"] >= 10) &
            (df["net_profit"] > 0)].copy()
    df["np_dd"] = df["net_profit"] / df["drawdown_pct"].clip(lower=0.5)
    df = df.sort_values("np_dd", ascending=False)
    seen = set()
    bases = []
    for _, r in df.iterrows():
        k = (int(r["donchian_bars"]), int(r["take_profit_pts"]),
             int(r["stop_loss_pts"]), round(float(r["daily_target_pct"]), 2),
             round(float(r["daily_loss_pct"]), 2))
        if k in seen:
            continue
        seen.add(k)
        bases.append({
            "donchian_bars": int(r["donchian_bars"]),
            "take_profit_pts": int(r["take_profit_pts"]),
            "stop_loss_pts": int(r["stop_loss_pts"]),
            "daily_target_pct": float(r["daily_target_pct"]),
            "daily_loss_pct": float(r["daily_loss_pct"]),
            "base_np": float(r["net_profit"]),
            "base_dd": float(r["drawdown_pct"]),
            "base_trades": int(r["trades"]),
            "base_np_dd": float(r["np_dd"]),
        })
        if len(bases) >= n:
            break
    return bases


def build_grid(bases: list[dict]) -> list[S1Config]:
    grid = []
    for b in bases:
        for htp in HTP_VALUES:
            grid.append(S1Config(
                risk_pct=RISK_PCT,
                donchian_bars=b["donchian_bars"],
                take_profit_pts=b["take_profit_pts"],
                stop_loss_pts=b["stop_loss_pts"],
                half_tp_ratio=htp,
                pending_expire_bars=2,
                start_hour=14,
                end_hour=22,
                daily_target_pct=b["daily_target_pct"],
                daily_loss_pct=b["daily_loss_pct"],
                block_fri_pm=True,
                hedge_mode=False,
            ))
    return grid


def main() -> int:
    bases = load_top_bases(N_BASES)
    print(f"Loaded {len(bases)} base configs from W1 parquet")
    print(f"\nBase configs (HTP=0 on W1):")
    print(f"  {'#':>2} {'Donch':>5} {'TP':>5} {'SL':>4} {'Tgt':>4} {'Loss':>4} {'NP':>8} {'DD%':>6} {'Tr':>4} {'NP/DD':>7}")
    for i, b in enumerate(bases, 1):
        print(f"  {i:>2} {b['donchian_bars']:>5} {b['take_profit_pts']:>5} "
              f"{b['stop_loss_pts']:>4} {int(b['daily_target_pct']):>4} "
              f"{int(b['daily_loss_pct']):>4} {b['base_np']:>+8.2f} "
              f"{b['base_dd']:>5.2f}% {b['base_trades']:>4} {b['base_np_dd']:>7.2f}")

    grid = build_grid(bases)
    print(f"\nGrid: {len(bases)} bases x {len(HTP_VALUES)} HTP = {len(grid)} configs")

    m = symbol_meta(SYMBOL)
    meta = SymbolMeta(
        point=m["point"], digits=m["digits"],
        tick_size=m["tick_size"], tick_value=m["tick_value"],
        stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
        volume_max=m["volume_max"], volume_step=m["volume_step"],
    )
    cache = OUT_DIR / "htp_sweep.parquet"

    df = run_sweep(
        grid, SYMBOL,
        datetime(WIN_START.year, WIN_START.month, WIN_START.day, tzinfo=timezone.utc),
        datetime(WIN_END.year, WIN_END.month, WIN_END.day, tzinfo=timezone.utc),
        meta, initial_balance=DEPOSIT,
        n_workers=N_WORKERS,
        cache_path=cache,
        window_label="HTP-W1",
    )

    df = df[(df["error"].isna() | (df["error"] == ""))].copy()
    df["np_dd"] = df["net_profit"] / df["drawdown_pct"].clip(lower=0.5)

    print("\n" + "=" * 100)
    print("HTP SWEEP RESULTS — per base, all HTP values")
    print("=" * 100)
    for i, b in enumerate(bases, 1):
        sub = df[
            (df["donchian_bars"] == b["donchian_bars"]) &
            (df["take_profit_pts"] == b["take_profit_pts"]) &
            (df["stop_loss_pts"] == b["stop_loss_pts"]) &
            (abs(df["daily_target_pct"] - b["daily_target_pct"]) < 0.01) &
            (abs(df["daily_loss_pct"] - b["daily_loss_pct"]) < 0.01)
        ].sort_values("half_tp_ratio")
        if sub.empty:
            continue
        hdr = (f"\n#{i} Donch={b['donchian_bars']} TP={b['take_profit_pts']} "
               f"SL={b['stop_loss_pts']} Tgt={int(b['daily_target_pct'])} "
               f"Loss={int(b['daily_loss_pct'])}")
        print(hdr)
        print(f"    {'HTP':>4}  {'NP':>9}  {'DD%':>6}  {'Tr':>4}  {'TP':>3}  {'SL':>3}  {'NP/DD':>7}  {'d_vs_HTP0':>11}")
        base_np_dd = None
        for _, r in sub.iterrows():
            if r["half_tp_ratio"] == 0.0:
                base_np_dd = r["np_dd"]
                break
        for _, r in sub.iterrows():
            delta = (r["np_dd"] - base_np_dd) if base_np_dd is not None else 0.0
            mark = "  <-- best" if r["np_dd"] == sub["np_dd"].max() else ""
            print(f"    {r['half_tp_ratio']:>4.1f}  {r['net_profit']:>+9.2f}  "
                  f"{r['drawdown_pct']:>5.2f}%  {int(r['trades']):>4}  "
                  f"{int(r['tp']):>3}  {int(r['sl']):>3}  {r['np_dd']:>7.2f}  "
                  f"{delta:>+11.2f}{mark}")

    print("\n" + "=" * 100)
    print("VERDICT — count of bases where HTP > 0 beats HTP = 0 on NP/DD")
    print("=" * 100)
    htp_wins = 0
    htp_loses = 0
    htp_ties = 0
    deltas = []
    for b in bases:
        sub = df[
            (df["donchian_bars"] == b["donchian_bars"]) &
            (df["take_profit_pts"] == b["take_profit_pts"]) &
            (df["stop_loss_pts"] == b["stop_loss_pts"]) &
            (abs(df["daily_target_pct"] - b["daily_target_pct"]) < 0.01) &
            (abs(df["daily_loss_pct"] - b["daily_loss_pct"]) < 0.01)
        ]
        if sub.empty:
            continue
        base_row = sub[sub["half_tp_ratio"] == 0.0]
        nonbase = sub[sub["half_tp_ratio"] > 0.0]
        if base_row.empty or nonbase.empty:
            continue
        base_np_dd = float(base_row.iloc[0]["np_dd"])
        best_htp = nonbase.loc[nonbase["np_dd"].idxmax()]
        delta = float(best_htp["np_dd"]) - base_np_dd
        deltas.append(delta)
        if delta > 0.5:
            htp_wins += 1
        elif delta < -0.5:
            htp_loses += 1
        else:
            htp_ties += 1

    total = htp_wins + htp_loses + htp_ties
    print(f"  Bases tested: {total}")
    print(f"  HTP>0 wins (>+0.5 NP/DD):   {htp_wins}")
    print(f"  HTP=0 wins (>+0.5 NP/DD):   {htp_loses}")
    print(f"  Ties (within +-0.5 NP/DD):  {htp_ties}")
    if deltas:
        avg = sum(deltas) / len(deltas)
        print(f"  Mean delta NP/DD (best HTP>0 vs HTP=0): {avg:+.2f}")
    print()
    if htp_wins > total / 2:
        print("  -> HTP IS worth sweeping in full WFO.")
    elif htp_loses > total / 2:
        print("  -> HTP is NOT worth sweeping — keep HTP=0 in WFO grid.")
    else:
        print("  -> HTP is marginal — probably not worth tripling grid size.")

    kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
