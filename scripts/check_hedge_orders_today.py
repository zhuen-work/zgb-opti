"""Pull today's ORDERS (incl pending/expired) for hedge magics 8111-8666.

Distinguishes:
  - Never placed:    no orders for that magic today
  - Placed & expired: order with state=EXPIRED
  - Placed & filled:  order with state=FILLED (would show in deals)
  - Placed & rejected: order with state=REJECTED/CANCELED
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

HEDGE_MAGICS = {8111, 8222, 8333, 8444, 8555, 8666}
PARENT_MAGICS = {1111, 2222, 3333, 4444, 5555, 6666}


def main():
    spec = init_account("live")
    try:
        now = datetime.now(timezone.utc)
        # Pull last ~24h of broker-time history
        frm = now - timedelta(hours=30)
        # MT5 history_orders_get expects broker-time; pass naive epoch.
        orders = mt5.history_orders_get(frm, now)
        if orders is None:
            print(f"history_orders_get None: {mt5.last_error()}")
            return 1
        print(f"Pulled {len(orders)} orders in last 30h (broker time)")

        hedge_orders = [o for o in orders if int(o.magic) in HEDGE_MAGICS]
        parent_orders = [o for o in orders if int(o.magic) in PARENT_MAGICS]

        print(f"\nHEDGE orders (magics 8xxx): {len(hedge_orders)}")
        print(f"PARENT orders (magics 1xxx-6xxx): {len(parent_orders)}")

        # ORDER_STATE enum: 0=STARTED 1=PLACED 2=CANCELED 3=PARTIAL 4=FILLED 5=REJECTED 6=EXPIRED 7=REQUEST_ADD 8=REQUEST_MODIFY 9=REQUEST_CANCEL
        state_names = {0:"STARTED", 1:"PLACED", 2:"CANCELED", 3:"PARTIAL", 4:"FILLED", 5:"REJECTED", 6:"EXPIRED", 7:"REQ_ADD", 8:"REQ_MODIFY", 9:"REQ_CANCEL"}
        type_names = {0:"BUY", 1:"SELL", 2:"BUY_LIMIT", 3:"SELL_LIMIT", 4:"BUY_STOP", 5:"SELL_STOP", 6:"BUY_STOP_LIMIT", 7:"SELL_STOP_LIMIT", 8:"CLOSE_BY"}

        if hedge_orders:
            print(f"\n{'Ticket':<14} {'Time':<19} {'Magic':<6} {'Type':<12} {'State':<10} {'Price':<10} {'Vol':<6} {'Comment':<20}")
            for o in sorted(hedge_orders, key=lambda x: x.time_setup):
                tsetup = datetime.fromtimestamp(o.time_setup, tz=timezone.utc)
                print(f"{o.ticket:<14} {tsetup.strftime('%Y-%m-%d %H:%M:%S')} "
                      f"{o.magic:<6} {type_names.get(o.type, str(o.type)):<12} "
                      f"{state_names.get(o.state, str(o.state)):<10} "
                      f"{o.price_open:<10.2f} {o.volume_initial:<6.2f} {o.comment[:20]}")
        else:
            print("\nNO HEDGE ORDERS FOUND in last 30h.")
            print("This means EA never even attempted to place hedge orders.")
            print("Diagnoses (v6 STOP-on-extension era, since 2026-05-24):")
            print("  - EA not loaded with v6 (might still be earlier version on VPS)")
            print("  - All hedges in setfile disabled (_HEDGE_S*_Enabled=false)")
            print("  - F1 filter rejected all attempts (would show in Experts log)")
            print("  - STOP placement sanity-checks failed (stops level, already-triggered)")
            print("  - No parent SL events in window (nothing to hedge)")
            print("  - OrderSend() failed with error (would show in Experts log)")

        # Show parent SL count for context
        sl_orders = [o for o in parent_orders if o.state == 4 and o.type in (4, 5)]
        print(f"\nParent STOP orders (BUY_STOP=4 / SELL_STOP=5) filled today: {len(sl_orders)}")
    finally:
        mt5.shutdown()
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
