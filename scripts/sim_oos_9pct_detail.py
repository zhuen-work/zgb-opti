"""One-shot sim @ 9% with per-trade dump for live-vs-sim comparison.

Reads live setfile (dt818_pro_v3_9pct_may16_may9.set), runs each stream against
today's ticks, and prints trade-by-trade results per stream so they can be
diffed against live deal log from live_check.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import kill_mt5_terminal
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from sim_orb_oos_today import fetch_window, fetch_meta, row_to_cfg

LIVE_SETFILE = ROOT / "configs" / "sets" / "dt818_pro_v3_9pct_may16_may9.set"
DEPOSIT = 10_000.0
SPREAD_LIVE = 30  # per feedback_default_test_conditions.md (all live = 30pt 2026-05-16)
TOTAL_RISK = 9.0
PER_STREAM = TOTAL_RISK / 6


def parse_setfile(path: Path):
    text = path.read_text()
    rows = []
    for i in range(1, 7):
        def _get(key):
            m = re.search(rf"_ORB_S{i}_{key}=([^|]+)\|\|", text)
            return m.group(1).strip()
        rows.append({
            "range_minutes": int(_get("RangeMinutes")),
            "fixed_sl_pts": int(_get("FixedSL_Pts")),
            "rr_ratio": float(_get("RR_Ratio")),
            "half_tp_ratio": float(_get("HalfTP_Ratio")),
            "daily_target_pct": 999.0,
            "daily_loss_pct": 999.0,
        })
    return rows


def main():
    now = datetime.now(timezone.utc)
    start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)

    rows = parse_setfile(LIVE_SETFILE)
    labels = ["S1", "S2", "S3", "S4", "S5", "S6"]
    sources = ["MAY9 R1", "MAY9 R2", "MAY9 R3", "MAY16 R2", "MAY16 R3", "MAY16 R4"]

    print(f"v3 9% OOS detail | {start.date()} -> {now.strftime('%H:%M UTC')}")
    print(f"Setfile: {LIVE_SETFILE.name}  Spread: {SPREAD_LIVE}pt  Deposit: ${DEPOSIT:,.0f}")
    for lbl, src, r in zip(labels, sources, rows):
        print(f"  {lbl} ({src}): Range={r['range_minutes']} SL={r['fixed_sl_pts']} "
              f"RR={r['rr_ratio']} HTP={r['half_tp_ratio']}")

    try:
        sym, m = fetch_meta(None, account="sim")
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        sym, ticks, m1, m5 = fetch_window(sym, start, now, SPREAD_LIVE, account="sim")
        print(f"\nTicks: {len(ticks):,}  M1: {len(m1):,}  M5: {len(m5):,}")

        print(f"\n{'='*108}")
        print(f"PER-STREAM TRADE DETAIL @ {TOTAL_RISK:.0f}% total ({PER_STREAM:.2f}%/stream) on ${DEPOSIT:,.0f}")
        print(f"{'='*108}")
        totals = {}
        all_trades = []
        for i, lbl in enumerate(labels):
            cfg = row_to_cfg(rows[i], lbl, PER_STREAM)
            r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
            net = 0.0; wins = 0; losses = 0
            trades = []
            for d in r.deals:
                if d.kind == "entry":
                    continue
                trades.append(d)
                net += d.pnl
                if d.pnl > 0: wins += 1
                else: losses += 1
                all_trades.append((d.ts, lbl, d.kind, d.direction, d.lots, d.price, d.pnl))
            totals[lbl] = (net, wins, losses, len(trades))
            print(f"\n[{lbl}] {sources[i]}  SL={rows[i]['fixed_sl_pts']}pt  RR={rows[i]['rr_ratio']}  HTP={rows[i]['half_tp_ratio']}"
                  f"   Net=${net:+,.2f}  W/L={wins}/{losses}  ({len(trades)} trades)")
            print(f"  {'Time (UTC)':<19} {'Side':<5} {'Kind':<5} {'Lots':>5} {'Price':>10} {'PnL$':>11}")
            for d in trades:
                side = "BUY" if d.direction == 1 else "SELL"
                print(f"  {d.ts.strftime('%Y-%m-%d %H:%M:%S'):<19} {side:<5} {d.kind:<5} "
                      f"{d.lots:>5.3f} {d.price:>10.2f} {d.pnl:>+11,.2f}")

        print(f"\n{'='*108}")
        print("SUMMARY")
        print(f"{'='*108}")
        print(f"  {'Stream':<8} {'Source':<10} {'Trades':>7} {'W/L':>5} {'Net $':>11}")
        grand = 0.0
        for lbl, src in zip(labels, sources):
            net, w, l, t = totals[lbl]
            print(f"  {lbl:<8} {src:<10} {t:>7} {w}/{l:<3} {net:>+11,.2f}")
            grand += net
        print(f"  {'TOTAL':<8} {'':<10} {sum(t[3] for t in totals.values()):>7} {'':<5} {grand:>+11,.2f}")
        print(f"  ROI on ${DEPOSIT:,.0f}: {grand/DEPOSIT*100:+.2f}%")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
