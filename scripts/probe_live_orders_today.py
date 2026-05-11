"""Pull every ORDER (not just deal) for today across v2 magics.

A pending order shows up in history_orders_get even if it was never filled
(state = expired/canceled/etc). This will tell us if the LDN session pendings
were placed, expired, rejected, or never created.
"""
from __future__ import annotations
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.mt5_accounts import init_account
from zgb_sim.tick_loader import kill_mt5_terminal
import MetaTrader5 as mt5

MAGIC_TO_LABEL = {1111: "S1", 2222: "S2", 3333: "S3",
                   4444: "S4", 5555: "S5", 6666: "S6"}

ORDER_STATE = {
    mt5.ORDER_STATE_STARTED: "STARTED",
    mt5.ORDER_STATE_PLACED: "PLACED",
    mt5.ORDER_STATE_CANCELED: "CANCELED",
    mt5.ORDER_STATE_PARTIAL: "PARTIAL",
    mt5.ORDER_STATE_FILLED: "FILLED",
    mt5.ORDER_STATE_REJECTED: "REJECTED",
    mt5.ORDER_STATE_EXPIRED: "EXPIRED",
    mt5.ORDER_STATE_REQUEST_ADD: "REQ_ADD",
    mt5.ORDER_STATE_REQUEST_MODIFY: "REQ_MOD",
    mt5.ORDER_STATE_REQUEST_CANCEL: "REQ_CAN",
}
ORDER_TYPE = {
    mt5.ORDER_TYPE_BUY: "BUY",
    mt5.ORDER_TYPE_SELL: "SELL",
    mt5.ORDER_TYPE_BUY_LIMIT: "BUY_LIMIT",
    mt5.ORDER_TYPE_SELL_LIMIT: "SELL_LIMIT",
    mt5.ORDER_TYPE_BUY_STOP: "BUY_STOP",
    mt5.ORDER_TYPE_SELL_STOP: "SELL_STOP",
    mt5.ORDER_TYPE_BUY_STOP_LIMIT: "BUY_SL",
    mt5.ORDER_TYPE_SELL_STOP_LIMIT: "SELL_SL",
}

# Use a wider window: Sun 00:00 UTC to now (covers EA load on Sunday + all of Mon)
end = datetime.now(timezone.utc)
start = end.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)


def main():
    spec = init_account("live")
    try:
        orders = mt5.history_orders_get(start, end) or ()
        by_mag = defaultdict(list)
        for o in orders:
            if o.magic not in MAGIC_TO_LABEL:
                continue
            by_mag[o.magic].append(o)

        print(f"\n{'='*120}")
        print(f"  v2 ORDERS history  |  {start.isoformat()}  ->  {end.isoformat()}")
        print(f"{'='*120}\n")

        for magic, lbl in MAGIC_TO_LABEL.items():
            ords = by_mag.get(magic, [])
            print(f"--- {lbl} (magic {magic}) — {len(ords)} orders ---")
            if not ords:
                print(f"  ** NO ORDERS PLACED for {lbl} in this window **")
                continue
            # Sort by time_setup
            ords.sort(key=lambda o: o.time_setup)
            print(f"  {'Setup time (UTC)':<22} {'Type':<10} {'State':<10} {'Vol':>6} "
                  f"{'PriceOpen':>9} {'SL':>9} {'TP':>9} {'Expire':<22} {'Comment':<15}")
            for o in ords:
                t_setup = datetime.fromtimestamp(o.time_setup, tz=timezone.utc)
                t_exp = datetime.fromtimestamp(o.time_expiration, tz=timezone.utc) if o.time_expiration else None
                print(f"  {str(t_setup):<22} {ORDER_TYPE.get(o.type,'?'):<10} "
                      f"{ORDER_STATE.get(o.state,'?'):<10} "
                      f"{o.volume_initial:>6.2f} {o.price_open:>9.2f} "
                      f"{o.sl:>9.2f} {o.tp:>9.2f} "
                      f"{(str(t_exp) if t_exp else '-'):<22} {o.comment:<15}")
            print()

        # Now also look at the ldn session window specifically
        print(f"\n{'='*120}")
        print(f"  ORDERS placed during LDN range / pre-fire window (07:00–08:36 UTC Mon)")
        print(f"{'='*120}\n")
        ldn_start = end.replace(hour=7, minute=0, second=0, microsecond=0)
        ldn_fire = end.replace(hour=8, minute=40, second=0, microsecond=0)
        for magic, lbl in MAGIC_TO_LABEL.items():
            for o in by_mag.get(magic, []):
                t_setup = datetime.fromtimestamp(o.time_setup, tz=timezone.utc)
                if ldn_start <= t_setup <= ldn_fire:
                    state = ORDER_STATE.get(o.state, '?')
                    print(f"  {lbl}/{ORDER_TYPE.get(o.type,'?'):<10} setup={t_setup.strftime('%H:%M:%S')} "
                          f"state={state} entry={o.price_open:.2f} sl={o.sl:.2f} tp={o.tp:.2f}")

    finally:
        mt5.shutdown()
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
