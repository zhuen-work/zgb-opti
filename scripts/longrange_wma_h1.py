"""12-month H1 WMA-cross test. Splits the year into 4 quarters for OOS-style
consistency check + full-year summary. Integrity-checked.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from zgb_sim.tick_loader import load_bars, symbol_meta
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.wma_cross import WMACrossConfig, simulate_wma_cross


SYMBOL = "XAUUSD"
OUTDIR = Path("output/wma_cross_longrange")


def to_utc(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def run(start: datetime, end: datetime, meta: SymbolMeta, balance: float, label: str):
    warmup_days = 20  # H1 needs 200h ≈ 8d; 20d gives safety buffer
    bars = load_bars(SYMBOL, "H1", start - timedelta(days=warmup_days), end)
    if hasattr(bars["ts"].dt, "tz") and bars["ts"].dt.tz is None:
        bars["ts"] = bars["ts"].dt.tz_localize("UTC")
    cfg = WMACrossConfig(risk_pct=1.0, fast_period=50, slow_period=200,
                          sl_buffer_pts=100, rr_ratio=0.0, max_spread_pts=70)
    res = simulate_wma_cross(bars, cfg, meta, initial_balance=balance,
                              window_start_ts=pd.Timestamp(start))
    # Build deals df
    rows = []
    open_entry = None
    for d in res.deals:
        if d.kind == "entry":
            open_entry = d
        elif open_entry is not None:
            rows.append({
                "entry_ts": open_entry.ts, "exit_ts": d.ts,
                "dir": open_entry.direction, "lots": open_entry.lots,
                "entry_px": open_entry.price, "exit_px": d.price,
                "exit_kind": d.kind, "pnl": d.pnl,
                "dur_h": (d.ts - open_entry.ts).total_seconds() / 3600.0,
            })
            open_entry = None
    df = pd.DataFrame(rows)
    if not df.empty:
        df["balance_after"] = balance + df["pnl"].cumsum()
        OUTDIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUTDIR / f"deals_{label}.csv", index=False)
    # Integrity
    issues = []
    if not df.empty:
        if ((df["exit_kind"] == "sl") & (df["pnl"] > 0.5)).any():
            issues.append("mislabeled SL with +pnl")
        if (df["lots"] > 50).any():
            issues.append("whale trade (lots>50)")
        if len(df) >= 5 and df["pnl"].abs().max() / df["pnl"].abs().sum() > 0.7:
            issues.append(f"single trade is {df['pnl'].abs().max()/df['pnl'].abs().sum()*100:.0f}% of |pnl|")
    return res, df, issues


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-05-23")
    ap.add_argument("--end", default="2026-05-23")
    ap.add_argument("--balance", type=float, default=10_000.0)
    args = ap.parse_args()

    m = symbol_meta(SYMBOL)
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])

    start = to_utc(args.start); end = to_utc(args.end)
    total_days = (end - start).days
    print(f"WMA H1/cross long-range: {SYMBOL} {args.start} -> {args.end} ({total_days}d, ${args.balance:,.0f})")
    print("=" * 110)

    # 4 quarters
    quarters = []
    q_start = start
    q_len_days = total_days // 4
    for i in range(4):
        q_end = q_start + timedelta(days=q_len_days) if i < 3 else end
        quarters.append((f"Q{i+1}", q_start, q_end))
        q_start = q_end

    print(f"\nQUARTERLY (each independent, fresh ${args.balance:,.0f} balance):")
    print(f"{'q':>4} {'window':>26} | {'NP':>10} {'ROI':>7} {'PF':>5} {'DD%':>5} "
          f"{'Trd':>4} {'win':>4} {'lose':>4} {'avgWin':>7} {'avgLose':>8}")
    print("-" * 110)
    q_results = []
    for qlbl, qs, qe in quarters:
        res, df, issues = run(qs, qe, meta, args.balance, f"{qlbl}_{qs.date()}_{qe.date()}")
        if not df.empty:
            wins = df[df["pnl"] > 0]; losses = df[df["pnl"] <= 0]
            avg_win = wins["pnl"].mean() if len(wins) > 0 else 0
            avg_lose = losses["pnl"].mean() if len(losses) > 0 else 0
        else:
            wins = losses = pd.DataFrame()
            avg_win = avg_lose = 0
        roi = res.net_profit / args.balance * 100.0
        print(f"{qlbl:>4} {f'{qs.date()} -> {qe.date()}':>26} | "
              f"{res.net_profit:>+10,.0f} {roi:>+6.1f}% {res.profit_factor:>5.2f} "
              f"{res.max_drawdown_pct:>4.1f}% {res.trades:>4} {len(wins):>4} {len(losses):>4} "
              f"{avg_win:>+7,.0f} {avg_lose:>+8,.0f}")
        q_results.append({"q": qlbl, "np": res.net_profit, "trades": res.trades, "issues": issues})

    # Full-year run (compounding through the whole year)
    print(f"\nFULL YEAR (compounding):")
    res, df, issues = run(start, end, meta, args.balance, f"full_{start.date()}_{end.date()}")
    if not df.empty:
        wins = df[df["pnl"] > 0]; losses = df[df["pnl"] <= 0]
        wr = len(wins) / len(df) * 100
        avg_win = wins["pnl"].mean() if len(wins) > 0 else 0
        avg_lose = losses["pnl"].mean() if len(losses) > 0 else 0
        roi = res.net_profit / args.balance * 100.0
        print(f"  trades={res.trades}  WR={wr:.1f}%  avgWin=${avg_win:+,.0f}  avgLose=${avg_lose:+,.0f}")
        print(f"  NP=${res.net_profit:+,.0f}  ROI={roi:+.1f}%  PF={res.profit_factor:.2f}  "
              f"DD={res.max_drawdown_pct:.1f}%")

    # Summary
    pos_q = sum(1 for r in q_results if r["np"] > 0)
    sum_np = sum(r["np"] for r in q_results)
    print(f"\nSUMMARY:")
    print(f"  Quarters profitable: {pos_q}/{len(q_results)}")
    print(f"  Sum quarter NP (independent $10k each): ${sum_np:+,.0f}")
    print(f"  Full-year NP (compounded): ${res.net_profit:+,.0f}")
    print(f"  Full-year trades: {res.trades}")

    print(f"\nINTEGRITY:")
    all_issues = []
    for r in q_results:
        if r["issues"]:
            all_issues.append(f"  {r['q']}: " + "; ".join(r["issues"]))
    if issues:
        all_issues.append(f"  full-year: " + "; ".join(issues))
    if all_issues:
        print("\n".join(all_issues))
    else:
        print("  [OK] No mislabels, no whale trades, no single-trade-dominated quarters")


if __name__ == "__main__":
    main()
