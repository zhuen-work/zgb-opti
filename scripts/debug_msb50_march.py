"""Debug dump for MSB50_v1 rank4 on March 2026 — verify the +600% NP isn't a bug.

Outputs:
  - output/msb50_debug/deals_march.csv      — every deal with full context
  - output/msb50_debug/balance_march.csv    — equity curve
  - console: balance progression by week, top-5 winners, top-5 losers,
             and manual spot-check of 3 sample trades against the M5 chart.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from zgb_sim.tick_loader import load_ticks, load_bars, symbol_meta
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.msb50 import (
    MSB50Config, simulate_msb50, detect_pivots,
    RANGE_IMPULSE, MSB_DONCHIAN,
)


SYMBOL = "XAUUSD"
OUTDIR = Path("output/msb50_debug")


def to_utc(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def main():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    start = to_utc("2026-03-01"); end = to_utc("2026-03-31")
    BAL = 10_000.0

    print(f"MSB50_v1 DEBUG: rank4 (pn=3 tol=20 slb=50 rr=2.0 nd=10) on {start.date()} -> {end.date()}")
    print("=" * 100, flush=True)

    m = symbol_meta(SYMBOL)
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])
    print("  Loading data...", flush=True)
    ticks = load_ticks(SYMBOL, start, end, spread_pts=60)
    m5 = load_bars(SYMBOL, "M5", start, end)
    print(f"  ticks={len(ticks):,}  m5={len(m5):,}", flush=True)

    cfg = MSB50Config(
        risk_pct=1.0, pivot_n=3,
        range_mode=RANGE_IMPULSE, msb_mode=MSB_DONCHIAN,
        tol_pts=20, n_donch=10, sl_buffer_pts=50, rr_ratio=2.0,
        max_spread_pts=70, arm_timeout_bars=24,
    )

    print("  Running sim...", flush=True)
    res = simulate_msb50(ticks, m5, cfg, meta, initial_balance=BAL)
    print(f"  Done. NP={res.net_profit:+,.0f}  PF={res.profit_factor:.2f}  "
          f"DD%={res.max_drawdown_pct:.1f}  Trd={res.trades}", flush=True)

    # ---- Build deals DataFrame ----
    # Pair entries with their immediate exit (sl/tp/other)
    rows = []
    open_entry = None
    for d in res.deals:
        if d.kind == "entry":
            open_entry = d
        elif open_entry is not None:
            sl_or_tp = d.kind
            rows.append({
                "entry_ts": open_entry.ts, "exit_ts": d.ts,
                "dir": open_entry.direction, "lots": open_entry.lots,
                "entry_px": open_entry.price, "exit_px": d.price,
                "exit_kind": sl_or_tp, "pnl": d.pnl,
                "dur_min": (d.ts - open_entry.ts).total_seconds() / 60.0,
                "px_move_pts": abs(d.price - open_entry.price) / meta.point,
            })
            open_entry = None
    df = pd.DataFrame(rows)
    df["balance_after"] = BAL + df["pnl"].cumsum()
    df["risk_pct_check"] = (df["lots"] * 50 * meta.tick_value /
                             df["balance_after"].shift(1).fillna(BAL)) * 100  # for SL_pts=50 approx
    out_deals = OUTDIR / "deals_march.csv"
    df.to_csv(out_deals, index=False)
    print(f"  Wrote {out_deals} ({len(df)} closed trades)", flush=True)

    # ---- Balance curve ----
    bc = res.balance_curve
    if not bc.empty:
        bc.to_csv(OUTDIR / "balance_march.csv", index=False)

    # ---- Weekly summary ----
    print(f"\n  WEEKLY BALANCE PROGRESSION:")
    df["week"] = pd.to_datetime(df["entry_ts"]).dt.to_period("W")
    weekly = df.groupby("week").agg(trades=("pnl", "count"),
                                     wins=("pnl", lambda s: (s > 0).sum()),
                                     pnl=("pnl", "sum"),
                                     bal_end=("balance_after", "last")).reset_index()
    print(f"  {'week':<25} {'trd':>4} {'win':>4} {'pnl':>12} {'bal_end':>12}")
    for _, r in weekly.iterrows():
        print(f"  {str(r['week']):<25} {r['trades']:>4} {r['wins']:>4} "
              f"{r['pnl']:>+12,.0f} {r['bal_end']:>+12,.0f}")

    # ---- Win/loss stats ----
    wins = df[df["pnl"] > 0]
    losses = df[df["pnl"] < 0]
    print(f"\n  WIN/LOSS STATS:")
    print(f"  Wins:   n={len(wins):>3}  avg={wins['pnl'].mean():+,.0f}  "
          f"max={wins['pnl'].max():+,.0f}  med={wins['pnl'].median():+,.0f}")
    print(f"  Losses: n={len(losses):>3}  avg={losses['pnl'].mean():+,.0f}  "
          f"min={losses['pnl'].min():+,.0f}  med={losses['pnl'].median():+,.0f}")
    print(f"  Win rate: {len(wins) / len(df) * 100:.1f}%")
    print(f"  Avg lots (early 10): {df['lots'].iloc[:10].mean():.2f}  "
          f"(late 10): {df['lots'].iloc[-10:].mean():.2f}  "
          f"<- if late >> early, COMPOUNDING is driving NP")

    # ---- Top 5 winners + losers ----
    print(f"\n  TOP 5 WINNERS:")
    cols = ["entry_ts", "exit_ts", "dir", "lots", "entry_px", "exit_px", "exit_kind", "pnl", "dur_min", "px_move_pts"]
    print(wins.nlargest(5, "pnl")[cols].to_string(index=False))

    print(f"\n  TOP 5 LOSERS:")
    print(losses.nsmallest(5, "pnl")[cols].to_string(index=False))

    # ---- Spot-check 3 sample trades against M5 chart ----
    print(f"\n  SPOT-CHECK 3 SAMPLE TRADES:")
    sample_idxs = [0, len(df) // 2, len(df) - 1] if len(df) >= 3 else list(range(len(df)))
    m5_ts_np = m5["ts"].dt.tz_convert("UTC").dt.tz_localize(None).values.astype("datetime64[ns]")
    b_high = m5["high"].values.astype(np.float64)
    b_low = m5["low"].values.astype(np.float64)
    b_close = m5["close"].values.astype(np.float64)

    # Pre-compute pivots for cross-reference
    piv_idx, piv_kind = detect_pivots(b_high, b_low, cfg.pivot_n)

    for si in sample_idxs:
        r = df.iloc[si]
        entry_ts = pd.Timestamp(r["entry_ts"]).tz_convert("UTC").tz_localize(None) if pd.Timestamp(r["entry_ts"]).tzinfo else pd.Timestamp(r["entry_ts"])
        # Find M5 bar at/just-before entry
        bi = int(np.searchsorted(m5_ts_np, entry_ts.to_datetime64(), side="right") - 1)
        print(f"\n  --- Trade #{si} ({'BUY' if r['dir']==1 else 'SELL'}) ---")
        print(f"    Entry: {entry_ts}  px={r['entry_px']:.2f}  lots={r['lots']:.2f}")
        print(f"    Exit:  {r['exit_ts']}  px={r['exit_px']:.2f}  {r['exit_kind'].upper()}  pnl=${r['pnl']:+,.2f}")
        print(f"    Dur={r['dur_min']:.0f}min  Move={r['px_move_pts']:.0f}pt")

        # Show 6 bars before + 4 bars after entry
        lo, hi = max(0, bi - 6), min(len(m5), bi + 5)
        print(f"    M5 bars around entry (bar idx {bi}):")
        for j in range(lo, hi):
            marker = "<-- entry bar" if j == bi else ""
            # Was there a pivot here?
            piv_marker = ""
            for k, p_idx in enumerate(piv_idx):
                if p_idx == j:
                    piv_marker = f" [PIVOT {'H' if piv_kind[k]==1 else 'L'}]"
                    break
            ts_j = pd.Timestamp(m5_ts_np[j])
            print(f"      {ts_j} h={b_high[j]:.2f} l={b_low[j]:.2f} c={b_close[j]:.2f}{piv_marker} {marker}")

    print(f"\n  Outputs written to: {OUTDIR.resolve()}")


if __name__ == "__main__":
    main()
