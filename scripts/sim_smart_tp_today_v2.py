"""Proper smart-TP what-if: use TODAY's actual live parent SL events from MT5
history, then simulate ONLY the smart-TP hedge layer on top of today's ticks.

Removes the sim-vs-live parent misalignment of sim_smart_tp_today.py — the
hedge is evaluated against the EXACT same tick path live experienced.

Output:
  - For each live parent SL event (n=36 today): direction, entry, lots,
    smart-TP hedge outcome (stage1 px/pnl, stage2 px/pnl)
  - Per-stream aggregate: live parent NP (actual), live old-hedge NP (actual),
    smart-TP hedge NP (what-if)
  - Portfolio totals + delta vs live actual
"""
from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from sim_orb_oos_today import fetch_window
from sim_wfo_hedge_reverse import simulate_reverse_hedges, ReverseHedgeCfg
from sim_wfo_hedge_retry import ts_arr_from_ticks
from zgb_sim.mt5_accounts import init_account
from zgb_sim.tick_loader import kill_mt5_terminal

SETFILE = ROOT / "configs" / "sets" / "dt818_pro_v3_9pct_may16_may9.set"
PARENT_MAGICS = {1111: "S1", 2222: "S2", 3333: "S3",
                  4444: "S4", 5555: "S5", 6666: "S6"}
HEDGE_MAGICS  = {8111: "S1", 8222: "S2", 8333: "S3",
                  8444: "S4", 8555: "S5", 8666: "S6"}
SPREAD_LIVE = 30


def parse_setfile(path: Path) -> dict:
    text = path.read_text()
    out = {}
    for i in range(1, 7):
        s = f"S{i}"
        def g(key):
            m = re.search(rf"_ORB_{s}_{key}=([^|]+)\|\|", text)
            return m.group(1).strip()
        def gh(key):
            m = re.search(rf"_HEDGE_{s}_{key}=([^|]+)\|\|", text)
            return m.group(1).strip()
        out[s] = {
            "fixed_sl_pts": int(g("FixedSL_Pts")),
            "rr_ratio": float(g("RR_Ratio")),
            "sl_mult": float(gh("SLMult")),
            "partial_fraction": float(gh("PartialFraction")),
            "profit_mult": float(gh("ProfitMult")),
        }
    return out


def fetch_live_parent_events(date: datetime) -> tuple[list, dict, dict]:
    """Pull today's PARENT entry+exit pairs from live MT5 history.

    Returns (sl_events_per_stream, parent_np_per_stream, hedge_np_per_stream).
    sl_events format matches simulate_reverse_hedges contract:
      [{ts_ns, entry_ts_ns, direction, entry_price, lots, stream}]
    """
    import MetaTrader5 as mt5
    init_account("live")
    try:
        # Broker-time labeled bounds — covers entire trading day with margin
        since = datetime(date.year, date.month, date.day, 0, 0, tzinfo=timezone.utc)
        end   = datetime(date.year, date.month, date.day + 1, 6, 0, tzinfo=timezone.utc)
        deals = mt5.history_deals_get(since, end) or ()

        # Pair entries with exits via position_id
        entries = {}   # position_id -> entry_deal
        parent_np = {s: 0.0 for s in PARENT_MAGICS.values()}
        hedge_np = {s: 0.0 for s in HEDGE_MAGICS.values()}
        for d in deals:
            mag = int(d.magic)
            if mag in PARENT_MAGICS or mag in HEDGE_MAGICS:
                if d.entry == 0:
                    entries[d.position_id] = d
                else:
                    if mag in PARENT_MAGICS:
                        parent_np[PARENT_MAGICS[mag]] += d.profit
                    else:
                        hedge_np[HEDGE_MAGICS[mag]] += d.profit

        # Build SL events from parent exits with [sl ... comment
        sl_events_by_stream = {s: [] for s in PARENT_MAGICS.values()}
        for d in deals:
            mag = int(d.magic)
            if mag not in PARENT_MAGICS or d.entry != 1:
                continue
            if not str(d.comment or "").startswith("[sl"):
                continue
            entry_d = entries.get(d.position_id)
            if entry_d is None:
                continue
            # MT5 deal d.type for ENTRY: 0=BUY, 1=SELL -> position dir = +1/-1
            # For EXIT (entry==1) deal type is opposite of position dir.
            direction = +1 if int(entry_d.type) == 0 else -1
            # broker-time-labeled-UTC nanoseconds (matches tick timeline)
            entry_ts_ns = pd.Timestamp(
                datetime.fromtimestamp(entry_d.time_msc / 1000, tz=timezone.utc)
            ).value
            sl_ts_ns = pd.Timestamp(
                datetime.fromtimestamp(d.time_msc / 1000, tz=timezone.utc)
            ).value
            stream = PARENT_MAGICS[mag]
            sl_events_by_stream[stream].append({
                "ts_ns": sl_ts_ns,
                "entry_ts_ns": entry_ts_ns,
                "direction": direction,
                "entry_price": float(entry_d.price),
                "sl_price": float(d.price),
                "lots": float(entry_d.volume),
            })
        return sl_events_by_stream, parent_np, hedge_np
    finally:
        mt5.shutdown()
        kill_mt5_terminal()


def main() -> int:
    streams = parse_setfile(SETFILE)
    today = datetime.now(timezone.utc)

    print("=" * 100)
    print(f"  PROPER SMART-TP WHAT-IF | {today.date()}")
    print(f"  Method: live parent SL events × today's ticks × smart-TP hedge sim")
    print(f"  Setfile: {SETFILE.name}")
    print("=" * 100)

    # Fetch ticks first via LIVE account (XAUUSD.sc — matches the symbol the EA
    # traded today, and avoids a sim->live MT5 account switch which sometimes
    # fails when only one account has persistent credentials).
    start = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
    end = today
    sym, ticks, _, _ = fetch_window(None, start, end, SPREAD_LIVE, account="live")

    # Pull parent SLs from LIVE history (same account, no switch needed)
    sl_events_by_stream, live_parent_np, live_hedge_np = fetch_live_parent_events(today)
    n_sl = sum(len(ev) for ev in sl_events_by_stream.values())
    print(f"  Live parent SL events fetched: {n_sl}")
    for s, evs in sl_events_by_stream.items():
        lots_list = [f"{ev['lots']:.2f}" for ev in evs]
        print(f"    {s}: {len(evs)} SLs, lots={lots_list}")
    if n_sl == 0:
        print("  No live SLs today — nothing to compare.")
        return 0
    print(f"\n  Ticks: {len(ticks):,} from {ticks.ts.min()} -> {ticks.ts.max()} ({sym})")
    ticks_arr = ts_arr_from_ticks(ticks)
    regime_by_session = {}  # gate=off

    # Run smart-TP hedge sim per stream
    print(f"\n  {'Stream':<6} {'SLs':>4} {'LiveParent$':>12} {'LiveOldHedge$':>13} "
          f"{'SmartTPHedge$':>14} {'SmartTPTrades':>14} {'Delta(smart-old)$':>16}")
    total_parent = total_old_hedge = total_smart_hedge = total_smart_trades = 0.0
    smart_by_stream = {}
    for s, sc in streams.items():
        evs = sl_events_by_stream[s]
        hcfg = ReverseHedgeCfg(
            exp_min=240, f1_sec=1800, regime_gate="off",
            sl_mult=sc["sl_mult"],
            partial_fraction=sc["partial_fraction"],
            profit_mult=sc["profit_mult"],
        )
        stream_cfg = {"fixed_sl_pts": sc["fixed_sl_pts"]}
        h_deals = simulate_reverse_hedges(evs, ticks_arr, stream_cfg, hcfg, regime_by_session)
        smart_np = sum(p for _, p in h_deals)
        smart_n = len(h_deals)
        smart_by_stream[s] = (smart_np, smart_n)
        delta = smart_np - live_hedge_np[s]
        print(f"  {s:<6} {len(evs):>4} ${live_parent_np[s]:>+10,.0f} "
              f"${live_hedge_np[s]:>+11,.0f} ${smart_np:>+12,.0f} {smart_n:>14} "
              f"${delta:>+14,.0f}")
        total_parent += live_parent_np[s]
        total_old_hedge += live_hedge_np[s]
        total_smart_hedge += smart_np
        total_smart_trades += smart_n
    total_delta = total_smart_hedge - total_old_hedge
    print(f"  {'TOTAL':<6} {n_sl:>4} ${total_parent:>+10,.0f} "
          f"${total_old_hedge:>+11,.0f} ${total_smart_hedge:>+12,.0f} {int(total_smart_trades):>14} "
          f"${total_delta:>+14,.0f}")

    print()
    print("=" * 100)
    print(f"  PORTFOLIO RESULT")
    print("=" * 100)
    actual = total_parent + total_old_hedge
    whatif = total_parent + total_smart_hedge
    print(f"  Live ACTUAL (parent + old single-tp hedge):  ${actual:>+12,.0f}")
    print(f"  What-if (parent + smart-TP hedge):           ${whatif:>+12,.0f}")
    print(f"  Net delta if smart-TP had been deployed:     ${whatif - actual:>+12,.0f}")
    print()

    # Per-event detail
    print("=" * 100)
    print(f"  PER-EVENT DETAIL  (live parent SL -> smart-TP hedge outcome)")
    print("=" * 100)
    for s, sc in streams.items():
        evs = sl_events_by_stream[s]
        if not evs:
            continue
        hcfg = ReverseHedgeCfg(
            exp_min=240, f1_sec=1800, regime_gate="off",
            sl_mult=sc["sl_mult"],
            partial_fraction=sc["partial_fraction"],
            profit_mult=sc["profit_mult"],
        )
        stream_cfg = {"fixed_sl_pts": sc["fixed_sl_pts"]}
        # Run per-event to see which fired/missed
        for ev in sorted(evs, key=lambda e: e["ts_ns"]):
            single = simulate_reverse_hedges([ev], ticks_arr, stream_cfg, hcfg, regime_by_session)
            entry_t = pd.Timestamp(ev["entry_ts_ns"]).strftime("%H:%M")
            sl_t = pd.Timestamp(ev["ts_ns"]).strftime("%H:%M")
            dir_s = "BUY" if ev["direction"] == 1 else "SELL"
            if not single:
                print(f"  {s} {dir_s} entry@{entry_t} SL@{sl_t} lots={ev['lots']:.2f} "
                      f"entry_px={ev['entry_price']:.2f} -> smart-TP: NO FILL (price never returned to entry)")
            else:
                pnl_sum = sum(p for _, p in single)
                if len(single) == 2:
                    p1 = single[0][1]; p2 = single[1][1]
                    print(f"  {s} {dir_s} entry@{entry_t} SL@{sl_t} lots={ev['lots']:.2f} "
                          f"entry_px={ev['entry_price']:.2f} -> S1 ${p1:>+8,.0f}  S2 ${p2:>+8,.0f}  "
                          f"sum ${pnl_sum:>+8,.0f}")
                else:
                    print(f"  {s} {dir_s} entry@{entry_t} SL@{sl_t} lots={ev['lots']:.2f} -> "
                          f"only {len(single)} leg(s) settled, sum ${pnl_sum:>+,.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
