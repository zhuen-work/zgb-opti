"""Sanity for WMA Golden Cross. Runs (H1, H4) x (exit-on-cross, rr=2.0) matrix
on 3 windows. Includes deal dump for top winners/losers for spot-check.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from zgb_sim.tick_loader import load_bars, symbol_meta
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.wma_cross import WMACrossConfig, simulate_wma_cross, _wma


SYMBOL = "XAUUSD"
OUTDIR = Path("output/wma_cross_debug")


def to_utc(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def run_cell(symbol: str, tf: str, exit_mode: str, start: datetime, end: datetime,
             meta: SymbolMeta, balance: float, warmup_days: int):
    bars = load_bars(symbol, tf, start - timedelta(days=warmup_days), end)
    if hasattr(bars["ts"].dt, "tz") and bars["ts"].dt.tz is None:
        bars["ts"] = bars["ts"].dt.tz_localize("UTC")
    rr = 2.0 if exit_mode == "rr2" else 0.0
    cfg = WMACrossConfig(risk_pct=1.0, fast_period=50, slow_period=200,
                          sl_buffer_pts=100, rr_ratio=rr, max_spread_pts=70)
    window_start = pd.Timestamp(start)
    res = simulate_wma_cross(bars, cfg, meta, initial_balance=balance,
                              window_start_ts=window_start)
    return res, bars


def dump_deals(res, label: str, out_path: Path, meta: SymbolMeta):
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
                "px_move_pts": (d.price - open_entry.price) * open_entry.direction / meta.point,
            })
            open_entry = None
    if not rows:
        return None
    df = pd.DataFrame(rows)
    df["balance_after"] = res.initial_balance + df["pnl"].cumsum()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return df


def integrity_check(df: pd.DataFrame, label: str) -> list[str]:
    """Return list of integrity issues. Empty list = clean."""
    issues = []
    if df is None or df.empty:
        return issues
    # SL with positive PnL (mislabel) or TP with negative PnL
    bad_sl = df[(df["exit_kind"] == "sl") & (df["pnl"] > 0.5)]
    bad_tp = df[(df["exit_kind"] == "tp") & (df["pnl"] < -0.5)]
    if len(bad_sl) > 0:
        issues.append(f"[{label}] {len(bad_sl)} 'sl' deals with positive pnl (mislabel)")
    if len(bad_tp) > 0:
        issues.append(f"[{label}] {len(bad_tp)} 'tp' deals with negative pnl (mislabel)")
    # Suspiciously large lots
    big_lots = df[df["lots"] > 50]
    if len(big_lots) > 0:
        issues.append(f"[{label}] {len(big_lots)} deals with lots > 50 (suspicious sizing)")
    # Suspiciously short hold (entry+exit same bar with non-trivial pnl)
    instant = df[(df["dur_h"] < 0.01) & (df["pnl"].abs() > df["pnl"].abs().median())]
    if len(instant) > 0:
        issues.append(f"[{label}] {len(instant)} deals with <0.6min duration + above-median |pnl|")
    # PnL dominated by a single trade
    if len(df) >= 5:
        top_abs = df["pnl"].abs().max()
        total_abs = df["pnl"].abs().sum()
        if total_abs > 0 and top_abs / total_abs > 0.7:
            issues.append(f"[{label}] single trade contributes {top_abs/total_abs*100:.0f}% of total |pnl|")
    return issues


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--balance", type=float, default=10_000.0)
    args = ap.parse_args()

    OUTDIR.mkdir(parents=True, exist_ok=True)
    m = symbol_meta(SYMBOL)
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])

    print(f"WMA Golden Cross sanity: {SYMBOL}  bal=${args.balance:,.0f}  risk=1%/trade")
    print(f"  TFs: H1, H4   exit_mode: rr2 + cross-only   SL: 200-WMA ± 100pt buffer")
    print("=" * 110)

    windows = [
        ("Apr 1-25",  to_utc("2026-04-01"), to_utc("2026-04-25")),
        ("May 1-22",  to_utc("2026-05-01"), to_utc("2026-05-22")),
        ("Mar 1-31",  to_utc("2026-03-01"), to_utc("2026-03-31")),
    ]
    tfs = ["M5", "M15"]
    exits = ["cross", "rr2"]

    rows = []
    all_issues = []
    for wlbl, ws, we in windows:
        for tf in tfs:
            # Warmup: M5 200*5min ≈ 17h → 3d; M15 200*15min ≈ 50h → 7d (generous buffer)
            warmup = 5 if tf == "M5" else 10
            for ex in exits:
                lbl = f"{tf}/{ex}/{wlbl}"
                print(f"\n[{lbl}] loading bars (warmup={warmup}d)...", flush=True)
                res, bars = run_cell(SYMBOL, tf, ex, ws, we, meta, args.balance, warmup)
                df = dump_deals(res, lbl, OUTDIR / f"deals_{tf}_{ex}_{wlbl.replace(' ','_').replace('-','_')}.csv", meta)

                # Integrity check
                issues = integrity_check(df, lbl)
                all_issues.extend(issues)

                roi = res.net_profit / args.balance * 100.0
                rows.append({"win": wlbl, "tf": tf, "exit": ex,
                              "trades": res.trades, "tp": res.tp_count,
                              "sl": res.sl_count, "other": res.other_count,
                              "np": res.net_profit, "roi": roi, "pf": res.profit_factor,
                              "dd": res.max_drawdown_pct, "n_deals": (len(df) if df is not None else 0)})
                print(f"  trades={res.trades} TP={res.tp_count} SL={res.sl_count} other={res.other_count} "
                      f"NP=${res.net_profit:+,.0f} ROI={roi:+.1f}% PF={res.profit_factor:.2f} DD={res.max_drawdown_pct:.1f}%",
                      flush=True)

    # Final table
    print(f"\n\nRESULTS MATRIX:")
    print(f"{'window':<10} {'tf':>3} {'exit':>6} | {'NP':>9} {'ROI':>7} {'PF':>5} {'DD%':>5} "
          f"{'Trd':>4} {'TP':>3} {'SL':>3} {'Othr':>4}")
    print("-" * 90)
    for r in rows:
        print(f"{r['win']:<10} {r['tf']:>3} {r['exit']:>6} | {r['np']:>+9,.0f} "
              f"{r['roi']:>+6.1f}% {r['pf']:>5.2f} {r['dd']:>4.1f}% "
              f"{r['trades']:>4} {r['tp']:>3} {r['sl']:>3} {r['other']:>4}")

    print(f"\n\nINTEGRITY CHECK:")
    if all_issues:
        print(f"  [WARN] {len(all_issues)} issues found:")
        for iss in all_issues:
            print(f"    - {iss}")
    else:
        print(f"  [OK] All {len(rows)} cells passed integrity checks (no mislabels, no whale trades, no instant SLs)")

    # Profitability rollup per config
    print(f"\n\nPER-CONFIG ACROSS-WINDOW SUMMARY:")
    for tf in tfs:
        for ex in exits:
            sub = [r for r in rows if r["tf"] == tf and r["exit"] == ex]
            pos = sum(1 for r in sub if r["np"] > 0)
            print(f"  {tf}/{ex}: {pos}/{len(sub)} profitable  "
                  f"sumNP=${sum(r['np'] for r in sub):+,.0f}  "
                  f"avgTrd={np.mean([r['trades'] for r in sub]):.1f}")


if __name__ == "__main__":
    main()
