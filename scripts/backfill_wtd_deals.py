"""Backfill week-to-date deals to dt818-console dashboard.

The dashboard's /api/today returns wtd_deals from its own SQLite deals table.
live_check.py only ever pushes deals since the last check (incremental window),
so the deals table is missing prior days of the current week — making the
dashboard's WTD P&L and per-stream bars wrong until live_check has run every
day of the week.

This script pulls the full Monday-00:00-UTC -> now window of closed deals from
MT5 and re-publishes them via /ingest/deals (which is INSERT OR IGNORE so
re-publishing is idempotent — see worker INSERT OR IGNORE INTO deals).

Run once on Monday morning before market open, or any time the dashboard's WTD
view looks short. Subsequent live_check runs append today's deals.
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
from zgb_sim.cf_publish import publish_deals

# Same mapping as live_check.py
STREAM_NAMES: dict[int, str] = {
    1111: "ORB_S1", 2222: "ORB_S2", 3333: "ORB_S3",
    4444: "ORB_S4", 5555: "ORB_S5", 6666: "ORB_S6",
    8111: "ORB_S1r", 8222: "ORB_S2r", 8333: "ORB_S3r",
    8444: "ORB_S4r", 8555: "ORB_S5r", 8666: "ORB_S6r",
}
TRACKED_MAGICS = set(STREAM_NAMES)


def main() -> int:
    spec = init_account("live")
    try:
        now = datetime.now(timezone.utc)
        # Monday 00:00 UTC of this week
        dow = now.weekday()  # 0=Mon
        week_mon = (now - timedelta(days=dow)).replace(hour=0, minute=0, second=0, microsecond=0)
        print(f"Backfill window: {week_mon.isoformat()} -> {now.isoformat()} ({(now - week_mon).days}d)")

        deals = mt5.history_deals_get(week_mon, now)
        if deals is None:
            print(f"history_deals_get None: {mt5.last_error()}")
            return 1

        # Filter to our tracked magics + closed deals (DEAL_ENTRY_OUT = a position close).
        closed = [d for d in deals
                  if int(d.magic) in TRACKED_MAGICS
                  and d.entry == mt5.DEAL_ENTRY_OUT]
        print(f"Found {len(closed)} closed deals across {len(TRACKED_MAGICS)} tracked magics in WTD window.")

        if not closed:
            print("Nothing to backfill.")
            return 0

        # Account snapshot for the same payload shape live_check uses.
        ai = mt5.account_info()
        if ai is None:
            print(f"account_info None: {mt5.last_error()}")
            return 1

        positions = mt5.positions_get() or []
        account_snap = {
            "ts": now.isoformat(),
            "balance": float(ai.balance), "equity": float(ai.equity),
            "margin": float(ai.margin), "margin_free": float(ai.margin_free),
            "open_positions": len(positions),
            "unrealized": sum(float(p.profit) for p in positions),
        }

        # Chunk in batches of 200 to avoid hitting any payload limits.
        BATCH = 200
        total_pushed = 0
        for i in range(0, len(closed), BATCH):
            batch = closed[i:i + BATCH]
            payloads = [{
                "deal_id": int(d.ticket),
                "ts": datetime.fromtimestamp(d.time_msc / 1000, tz=timezone.utc).isoformat(),
                "magic": int(d.magic),
                "stream": STREAM_NAMES.get(int(d.magic), f"m{d.magic}"),
                "symbol": d.symbol,
                "side": "buy" if d.type == 0 else "sell",
                "volume": float(d.volume),
                "price": float(d.price),
                "sl": None, "tp": None,
                "profit": float(d.profit),
                "comment": d.comment or None,
                "position_id": int(d.position_id),
                "balance_after": float(ai.balance),  # rough — actual would need running tally
            } for d in batch]
            ok = publish_deals(payloads, account=account_snap if i == 0 else None)
            print(f"  batch {i // BATCH + 1}: pushed {len(payloads)} deals -> {'OK' if ok else 'FAIL'}")
            if ok:
                total_pushed += len(payloads)

        net = sum(float(d.profit) for d in closed)
        print(f"\nBackfill complete: {total_pushed}/{len(closed)} deals pushed, net=${net:+,.2f}")
        return 0 if total_pushed == len(closed) else 2
    finally:
        mt5.shutdown()
        kill_mt5_terminal()


if __name__ == "__main__":
    sys.exit(main())
