"""Diagnostic: verify whether V1 fractal-trail uses pre-entry fractals (index-start bug).

Runs baseline vs V1_trail_w5 for S1 only over 1 week (Apr 1-8 2026).
Dumps exit deals with entry_price, exit_price, orig_sl, kind.
Checks: for any V1 SL-hit BUY deal, is exit_price < entry_price?
If yes => pre-entry fractal ratcheted SL above entry, masking stop-outs as near-breakeven.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig, simulate
from sim_wfo_hedge_retry import make_stream_cfg

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
# Use 1 week to keep diagnostic fast
START = datetime(2026, 4, 1, tzinfo=timezone.utc)
END = datetime(2026, 4, 8, tzinfo=timezone.utc)
SPREAD = 30
PER_STREAM_RISK = 1.0
STREAM = "S1"


def run_and_dump(flags: dict, label: str, meta, ticks, m5, m1):
    base = make_stream_cfg(STREAM, PER_STREAM_RISK)
    cfg = base
    for k, v in flags.items():
        setattr(cfg, k, v)

    r = simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)

    rows = []
    entry_map = {}  # direction -> entry_price (last seen)
    for d in r.deals:
        if d.kind == "entry":
            entry_map[d.direction] = (d.price, None)  # store entry price
        elif d.kind in ("sl", "tp", "other"):
            ep, _ = entry_map.get(d.direction, (None, None))
            rows.append({
                "ts": d.ts, "kind": d.kind, "dir": d.direction,
                "exit_price": d.price, "pnl": d.pnl, "entry_price": ep,
            })

    df = pd.DataFrame(rows)
    if df.empty:
        print(f"  [{label}] No exits.")
        return df
    df["above_entry"] = df.apply(
        lambda r: (r["exit_price"] >= r["entry_price"]) if r["dir"] == 1
        else (r["exit_price"] <= r["entry_price"]), axis=1
    )
    wins = (df["pnl"] > 0).sum()
    total = len(df)
    sl_hits = (df["kind"] == "sl").sum()
    print(f"\n  [{label}] trades={total} wins={wins} WR={wins/total*100:.0f}% sl_hits={sl_hits}")
    print(f"  NP={df['pnl'].sum():+.1f}  MaxPnl={df['pnl'].max():.1f}  MinPnl={df['pnl'].min():.1f}")
    # Check for exits at prices that seem suspicious
    if "entry_price" in df.columns and df["entry_price"].notna().any():
        buy_sl = df[(df["kind"] == "sl") & (df["dir"] == 1)]
        sell_sl = df[(df["kind"] == "sl") & (df["dir"] == -1)]
        if not buy_sl.empty:
            buy_above = (buy_sl["exit_price"] >= buy_sl["entry_price"]).sum()
            print(f"  BUY SL exits above-or-at entry: {buy_above}/{len(buy_sl)}")
            if buy_above > 0:
                print("  *** SUSPICIOUS: BUY SL hit at price >= entry (pre-entry fractal may have ratcheted SL above entry)")
                print(buy_sl[["ts","exit_price","entry_price","pnl"]].to_string())
        if not sell_sl.empty:
            sell_above = (sell_sl["exit_price"] <= sell_sl["entry_price"]).sum()
            print(f"  SELL SL exits below-or-at entry: {sell_above}/{len(sell_sl)}")
            if sell_above > 0:
                print("  *** SUSPICIOUS: SELL SL hit at price <= entry (pre-entry fractal may have ratcheted SL below entry)")
    return df


def main():
    print(f"=== V1 trail bug diagnostic | {START.date()} -> {END.date()} | S1 only ===")
    m = symbol_meta(SYMBOL)
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])
    ticks = load_ticks(SYMBOL, START, END, spread_pts=SPREAD)
    m5 = load_bars(SYMBOL, "M5", START, END)
    m1 = load_bars(SYMBOL, "M1", START, END)

    try:
        baseline_df = run_and_dump({}, "baseline", meta, ticks, m5, m1)
        v1_df = run_and_dump({"fractal_trail": True, "fractal_width": 5}, "V1_trail_w5", meta, ticks, m5, m1)

        print("\n=== SUMMARY ===")
        if not v1_df.empty and not baseline_df.empty:
            b_wr = (baseline_df["pnl"] > 0).mean() * 100
            v_wr = (v1_df["pnl"] > 0).mean() * 100
            print(f"Baseline WR={b_wr:.0f}%  V1 WR={v_wr:.0f}%")
            if v_wr > 80 and (v_wr - b_wr) > 15:
                print("VERDICT: Likely BUG — V1 WR suspiciously high vs baseline.")
            else:
                print("VERDICT: WR gap within plausible range; may be real edge or regime.")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
