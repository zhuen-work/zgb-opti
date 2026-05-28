"""For each parent SL today, simulate the EA's hedge precondition checks.

Answers: would the EA's ProcessHedgeStream actually have placed hedges?

For each parent SL deal:
  1. Find IN deal (entry) for the same position_id
  2. Compute (sl_time - in_time) - F1 filter check (default cutoff 3600s)
  3. Resolve original order via DEAL_ORDER on the IN deal
  4. Verify orig_type is BUY_STOP or SELL_STOP
  5. Report whether all preconditions pass
"""
from __future__ import annotations
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import MetaTrader5 as mt5
from zgb_sim.mt5_accounts import init_account
from zgb_sim.tick_loader import kill_mt5_terminal

PARENT_MAGICS = {1111: "S1", 2222: "S2", 3333: "S3", 4444: "S4", 5555: "S5", 6666: "S6"}
HEDGE_F1_DEFAULT = 3600  # EA default _HEDGE_Sn_MaxSecondsAfterEntry


def main():
    spec = init_account("live")
    try:
        now = datetime.now(timezone.utc)
        frm = now - timedelta(hours=30)

        deals = mt5.history_deals_get(frm, now)
        if deals is None:
            print(f"history_deals_get None: {mt5.last_error()}")
            return 1
        orders = mt5.history_orders_get(frm, now)
        if orders is None:
            print(f"history_orders_get None: {mt5.last_error()}")
            return 1

        order_by_ticket = {int(o.ticket): o for o in orders}

        # Find parent SL exit deals
        sl_deals = []
        for d in deals:
            if int(d.magic) not in PARENT_MAGICS:
                continue
            if d.entry != mt5.DEAL_ENTRY_OUT:
                continue
            if not str(d.comment).startswith("[sl"):
                continue
            sl_deals.append(d)

        print(f"Parent SL exits in last 30h: {len(sl_deals)}")
        print(f"{'Time UTC':<19} {'Strm':<4} {'Mg':<5} {'Side':<5} {'Vol':<5} {'Exit$':<11} "
              f"{'dt(s)':>6} {'F1pass':<6} {'OrderType':<12} {'OrigEntry':<10} {'Hedge?':<8}")

        would_fire = 0
        f1_fail = 0
        no_in = 0
        type_bad = 0
        for d in sl_deals:
            sl_time = datetime.fromtimestamp(d.time, tz=timezone.utc)
            stream = PARENT_MAGICS[int(d.magic)]
            side = "SELL" if d.type == 1 else "BUY"

            # Find IN deal for this position_id
            in_d = None
            for d2 in deals:
                if d2.position_id == d.position_id and d2.entry == mt5.DEAL_ENTRY_IN:
                    in_d = d2; break
            if in_d is None:
                no_in += 1
                print(f"{sl_time.strftime('%Y-%m-%d %H:%M:%S')} {stream:<4} {d.magic:<5} {side:<5} "
                      f"{d.volume:<5.2f} {d.profit:<+11,.0f} {'?':>6} {'?':<6} {'NO_IN_DEAL':<12}")
                continue
            in_time = datetime.fromtimestamp(in_d.time, tz=timezone.utc)
            dt_s = (sl_time - in_time).total_seconds()
            f1_pass = dt_s <= HEDGE_F1_DEFAULT

            # Resolve original order via DEAL_ORDER on IN deal
            in_order_ticket = int(in_d.order)
            orig_order = order_by_ticket.get(in_order_ticket)
            if orig_order is None:
                print(f"{sl_time.strftime('%Y-%m-%d %H:%M:%S')} {stream:<4} {d.magic:<5} {side:<5} "
                      f"{d.volume:<5.2f} {d.profit:<+11,.0f} {dt_s:>6.0f} {str(f1_pass):<6} {'?(no_ord)':<12}")
                continue
            type_str = {2:"BUY_LIMIT", 3:"SELL_LIMIT", 4:"BUY_STOP", 5:"SELL_STOP"}.get(orig_order.type, f"T{orig_order.type}")
            is_stop = orig_order.type in (4, 5)
            if not is_stop: type_bad += 1
            if not f1_pass: f1_fail += 1

            hedge_ok = f1_pass and is_stop
            if hedge_ok: would_fire += 1

            print(f"{sl_time.strftime('%Y-%m-%d %H:%M:%S')} {stream:<4} {d.magic:<5} {side:<5} "
                  f"{d.volume:<5.2f} {d.profit:<+11,.0f} {dt_s:>6.0f} {str(f1_pass):<6} {type_str:<12} "
                  f"{orig_order.price_open:<10.2f} {'YES' if hedge_ok else 'no':<8}")

        print(f"\n{'='*70}")
        print(f"SUMMARY: {len(sl_deals)} parent SLs in window")
        print(f"  Hedge would-fire:    {would_fire}")
        print(f"  Blocked by F1:       {f1_fail}")
        print(f"  Wrong order type:    {type_bad}")
        print(f"  Missing IN deal:     {no_in}")
        print(f"\nExpected hedge orders today (per EA logic, ignoring spread/stops-level/trade-through): {would_fire}")
        print(f"Actual hedge orders placed:                                                              0")
    finally:
        mt5.shutdown()
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
