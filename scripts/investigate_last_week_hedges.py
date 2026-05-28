"""Investigate why last week's S2-S6 hedges had 0 fires (only S1 fired).

Pulls all deals + orders for last week's window. Tabulates:
  - Every distinct magic that produced deals (with entry comment + count)
  - Pendings placed by each magic (whether they expired vs filled)
  - First/last seen timestamp per magic
  - SL events on parents — were hedge pendings placed in response?

Run: python scripts/investigate_last_week_hedges.py
"""
from __future__ import annotations

import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import MetaTrader5 as mt5
from zgb_sim.mt5_accounts import init_account

SYMBOL = "XAUUSD.sc"

# v6/v7 hedge magic mapping (per current setfile)
HEDGE_MAGICS = {1: 8111, 2: 9222, 3: 10333, 4: 11444, 5: 12555, 6: 13666}
# Alternative v6 spec from EA comments (8111-8666)
ALT_HEDGE_MAGICS = {1: 8111, 2: 8222, 3: 8333, 4: 8444, 5: 8555, 6: 8666}
PARENT_MAGICS = {1: 1111, 2: 2222, 3: 3333, 4: 4444, 5: 5555, 6: 6666}


def main():
    init_account("live")
    try:
        # Last week: Mon May 18 → Mon May 25
        last_mon = datetime(2026, 5, 18, tzinfo=timezone.utc)
        this_mon = datetime(2026, 5, 25, tzinfo=timezone.utc)

        # 1) Distinct magics + entry comments in last week's deals
        deals = mt5.history_deals_get(last_mon, this_mon)
        if deals is None:
            print("No deals returned for last week.")
            return 1
        per_magic = defaultdict(lambda: {"in_n": 0, "out_n": 0, "comments_in": defaultdict(int),
                                          "comments_out": defaultdict(int), "out_pnl": 0.0,
                                          "first_in": None, "last_in": None})
        for d in deals:
            if d.symbol != SYMBOL:
                continue
            rec = per_magic[d.magic]
            cm = (d.comment or "(empty)")[:40]
            if d.entry == mt5.DEAL_ENTRY_IN:
                rec["in_n"] += 1
                rec["comments_in"][cm] += 1
                ts = datetime.fromtimestamp(d.time, tz=timezone.utc)
                if rec["first_in"] is None or ts < rec["first_in"]:
                    rec["first_in"] = ts
                if rec["last_in"] is None or ts > rec["last_in"]:
                    rec["last_in"] = ts
            elif d.entry == mt5.DEAL_ENTRY_OUT:
                rec["out_n"] += 1
                rec["comments_out"][cm] += 1
                rec["out_pnl"] += d.profit + d.commission + d.swap

        print("=" * 100)
        print(f"  LAST WEEK ({last_mon.date()} -> {this_mon.date()}) — magics with deals")
        print("=" * 100)
        for mg in sorted(per_magic.keys()):
            r = per_magic[mg]
            label = "PARENT" if mg in PARENT_MAGICS.values() else ("HEDGE-v7" if mg in HEDGE_MAGICS.values() else ("HEDGE-v6alt" if mg in ALT_HEDGE_MAGICS.values() else "OTHER"))
            print(f"\n  magic={mg:<6}  [{label}]  IN={r['in_n']:>3}  OUT={r['out_n']:>3}  OUT-PnL=${r['out_pnl']:>+9,.0f}")
            if r["first_in"]:
                print(f"    first IN: {r['first_in'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
                print(f"    last  IN: {r['last_in'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
            for c, n in sorted(r["comments_in"].items(), key=lambda x: -x[1])[:3]:
                print(f"    IN  comment ({n:>3}x): {c}")

        # 2) Were any pending HEDGE STOP orders placed last week? (orders_get returns ACTIVE orders only,
        # but history_orders_get returns historical orders incl. expired/cancelled)
        print()
        print("=" * 100)
        print(f"  PENDING-ORDER HISTORY for hedge magics (last week)")
        print("=" * 100)
        orders = mt5.history_orders_get(last_mon, this_mon)
        if orders is None:
            print("  (no history_orders_get results)")
        else:
            hedge_magics_all = set(HEDGE_MAGICS.values()) | set(ALT_HEDGE_MAGICS.values())
            hedge_orders = [o for o in orders if o.symbol == SYMBOL and o.magic in hedge_magics_all]
            print(f"  Total hedge orders found: {len(hedge_orders)}")
            per_mg = defaultdict(lambda: {"n": 0, "states": defaultdict(int)})
            for o in hedge_orders:
                per_mg[o.magic]["n"] += 1
                state_name = {
                    mt5.ORDER_STATE_FILLED: "FILLED",
                    mt5.ORDER_STATE_CANCELED: "CANCELED",
                    mt5.ORDER_STATE_EXPIRED: "EXPIRED",
                    mt5.ORDER_STATE_REJECTED: "REJECTED",
                    mt5.ORDER_STATE_PARTIAL: "PARTIAL",
                }.get(o.state, f"state_{o.state}")
                per_mg[o.magic]["states"][state_name] += 1
            for mg in sorted(per_mg.keys()):
                pm = per_mg[mg]
                states_str = ", ".join(f"{k}={v}" for k, v in sorted(pm["states"].items()))
                print(f"  magic={mg:<6}  total_orders={pm['n']:>3}  states: {states_str}")
            zero_fire = [mg for mg in HEDGE_MAGICS.values() if mg not in per_mg]
            if zero_fire:
                print(f"\n  >>> ZERO orders placed for magics: {zero_fire}")

        # 3) Cross-reference: parent SL events on S2-S6 last week — were hedges supposed to fire?
        print()
        print("=" * 100)
        print(f"  PARENT-SL events last week (S2-S6) — would the v7 hedge logic have fired?")
        print("=" * 100)
        deals = mt5.history_deals_get(last_mon, this_mon)
        parent_sls = defaultdict(int)
        for d in deals:
            if d.symbol != SYMBOL: continue
            if d.entry != mt5.DEAL_ENTRY_OUT: continue
            if d.magic not in PARENT_MAGICS.values(): continue
            if d.profit + d.commission + d.swap >= 0: continue  # losses only
            parent_sls[d.magic] += 1
        print(f"  S1 (1111): {parent_sls[1111]} parent SLs (S1 hedge 8111 fired 22x)")
        print(f"  S2 (2222): {parent_sls[2222]} parent SLs (S2 hedge 9222 fired 0x)")
        print(f"  S3 (3333): {parent_sls[3333]} parent SLs (S3 hedge 10333 fired 0x)")
        print(f"  S4 (4444): {parent_sls[4444]} parent SLs (S4 hedge 11444 fired 0x)")
        print(f"  S5 (5555): {parent_sls[5555]} parent SLs (S5 hedge 12555 fired 0x)")
        print(f"  S6 (6666): {parent_sls[6666]} parent SLs (S6 hedge 13666 fired 0x)")

    finally:
        mt5.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
