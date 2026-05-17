"""Push synthetic v3 data to dt818-console for end-to-end render check.

Pushes (in order):
  1. forward_projection.json snapshot (updates setfile card + projection numbers)
  2. ~25 closed parent deals + ~12 hedge deals across this week + today
  3. 3 open positions

Synthetic IDs sit in the 99_000_000+ range so they're trivially removable:
  cd C:\\Projects\\dt818-console
  npx wrangler d1 execute dt818-console-db --remote --command "DELETE FROM deals WHERE deal_id >= 99000000; DELETE FROM positions_snapshot WHERE ticket >= 99000000"

Usage:
  python scripts/populate_dummy_dashboard.py          # push dummy data
  python scripts/populate_dummy_dashboard.py --clear  # tells you the SQL to wipe
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.cf_publish import (publish_deals, publish_positions, publish_projection,
                                 publish_alert)

ACCOUNT_BALANCE = 149_335.30
SYMBOL = "XAUUSD.sc"

# v3 magic structure
PARENT_MAGICS = {1111: "ORB_S1", 2222: "ORB_S2", 3333: "ORB_S3",
                 4444: "ORB_S4", 5555: "ORB_S5", 6666: "ORB_S6"}
HEDGE_MAGICS  = {8111: "ORB_S1r", 8222: "ORB_S2r", 8333: "ORB_S3r",
                 8444: "ORB_S4r", 8555: "ORB_S5r", 8666: "ORB_S6r"}
HEDGE_PARENT_OF = {8111: 1111, 8222: 2222, 8333: 3333, 8444: 4444, 8555: 5555, 8666: 6666}
HEDGE_R_RATIO   = {8111: 5.0, 8222: 6.0, 8333: 6.8, 8444: 6.0, 8555: 6.0, 8666: 6.0}


def stream_for(magic: int) -> str:
    return PARENT_MAGICS.get(magic) or HEDGE_MAGICS.get(magic) or f"m{magic}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--clear", action="store_true", help="Hint how to remove dummy data")
    args = ap.parse_args()

    if args.clear:
        print("To wipe synthetic data, run in PowerShell:")
        print('  cd C:\\Projects\\dt818-console')
        print('  npx wrangler d1 execute dt818-console-db --remote --command "DELETE FROM deals WHERE deal_id >= 99000000; DELETE FROM positions_snapshot WHERE ticket >= 99000000"')
        return 0

    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=4, minute=0, second=0, microsecond=0)  # LDN open broker time

    # ===== 1. Push forward_projection.json (updates setfile card on console) =====
    proj_path = ROOT / "output" / "forward_projection.json"
    proj = json.loads(proj_path.read_text())
    print(f"[1/3] Pushing projection (setfile: {proj['setfile']}, ea: {proj.get('ea', '?')})...")
    ok = publish_projection(proj)
    print(f"      {'OK' if ok else 'FAIL'}")

    # ===== 2. Today's parent + hedge deals =====
    # Parent: 5 trades, mix of wins/losses
    today_parent = [
        mk_deal(99_000_001, today_start + timedelta(hours=2, minutes=15),
                magic=2222, side="buy",  vol=2.20, price=3398.20, profit=+3_120.00,
                sl=3392.70, tp=3417.45, comment="ORB_S2_long_TP1"),
        mk_deal(99_000_002, today_start + timedelta(hours=3, minutes=42),
                magic=4444, side="sell", vol=2.20, price=3402.85, profit=+2_480.00,
                sl=3408.35, tp=3380.85, comment="ORB_S4_short_TP"),
        mk_deal(99_000_003, today_start + timedelta(hours=4, minutes=10),
                magic=6666, side="buy",  vol=2.20, price=3401.10, profit= -2_240.00,
                sl=3395.60, tp=3417.60, comment="ORB_S6_long_SL"),
        mk_deal(99_000_004, today_start + timedelta(hours=6, minutes=30),
                magic=2222, side="buy",  vol=2.20, price=3404.55, profit=+1_490.00,
                sl=3399.05, tp=3423.80, comment="ORB_S2_long_TP1"),
        mk_deal(99_000_005, today_start + timedelta(hours=7, minutes=18),
                magic=4444, side="sell", vol=2.20, price=3408.70, profit=+1_490.00,
                sl=3414.20, tp=3386.70, comment="ORB_S4_short_TP1"),
    ]
    # Today's hedges: 3 fires (S6 parent SL'd at 4:10 -> hedge S6r fired; +1 win, +1 SL, +1 small)
    today_hedge = [
        mk_deal(99_000_011, today_start + timedelta(hours=4, minutes=42),
                magic=8666, side="sell", vol=2.20, price=3401.10, profit=+13_440.00,
                sl=3406.60, tp=3368.10, comment="ORB_S6r_short_TP"),
        mk_deal(99_000_012, today_start + timedelta(hours=5, minutes=58),
                magic=8333, side="buy",  vol=2.75, price=3395.20, profit= -2_240.00,
                sl=3390.20, tp=3429.20, comment="ORB_S3r_long_SL"),
    ]

    today_deals = today_parent + today_hedge

    # ===== 3. Earlier-week parent + hedge (Mon-Fri this week) =====
    base = today_start - timedelta(days=4)
    earlier_parent = []
    earlier_hedge = []
    parent_seed = [
        # (magic, day_offset, hour, side, vol, price, profit)
        (1111, 0,  3, "buy",  2.20, 3389.20, +3_980), (1111, 1,  4, "sell", 2.20, 3395.10, +2_540),
        (1111, 2,  5, "buy",  2.20, 3392.40, -2_240),(1111, 3,  3, "sell", 2.20, 3401.30, +3_220),
        (2222, 0,  4, "buy",  2.20, 3388.50, +2_280), (2222, 1,  5, "sell", 2.20, 3396.80, +4_120),
        (2222, 2,  6, "sell", 2.20, 3393.20, +3_180), (2222, 3,  4, "buy",  2.20, 3402.00, -1_980),
        (2222, 3,  7, "sell", 2.20, 3406.40, +2_640),
        (3333, 0,  5, "sell", 2.75, 3390.10, -1_580), (3333, 1,  3, "buy",  2.75, 3394.00, +1_820),
        (3333, 2,  7, "buy",  2.75, 3398.30, -1_660),
        (4444, 1,  4, "sell", 2.20, 3397.20, +2_920), (4444, 2,  3, "buy",  2.20, 3393.40, +3_240),
        (4444, 3,  5, "sell", 2.20, 3402.80, +1_960),
        (5555, 0,  6, "buy",  2.20, 3391.10, +2_780), (5555, 2,  4, "sell", 2.20, 3399.60, +1_840),
        (5555, 3,  6, "buy",  2.20, 3405.80, +2_980),
        (6666, 1,  7, "sell", 2.20, 3398.20, +1_840), (6666, 3,  4, "buy",  2.20, 3403.50, +1_580),
    ]
    deal_id = 99_001_000
    for magic, doff, hour, side, vol, price, profit in parent_seed:
        ts = base + timedelta(days=doff, hours=hour, minutes=(deal_id % 60))
        if ts >= today_start:
            continue
        sl_off = 6.0 if side == "buy" else -6.0
        tp_off = (20.0 if profit > 0 else 18.0) * (1 if side == "buy" else -1)
        earlier_parent.append(mk_deal(
            deal_id, ts, magic=magic, side=side, vol=vol, price=price, profit=float(profit),
            sl=price - sl_off, tp=price + tp_off,
            comment=f"{stream_for(magic)}_{'long' if side == 'buy' else 'short'}",
        ))
        deal_id += 1

    # Hedges fire ~50% of parent SLs (parent SL = negative profit). Generate 10 hedge fires this week.
    hedge_seed = [
        # (parent_magic, day_offset, hour, side, vol, profit) -- side is opposite of original parent
        (8111, 2,  6, "sell", 2.20, +11_200),   # S1 parent SL'd Wed → reverse short TP
        (8222, 3,  5, "buy",  2.20, -2_240),    # S2 parent SL'd Thu morning → hedge long SL
        (8333, 0,  6, "buy",  2.75, +18_550),   # S3 parent SL'd Mon → reverse long TP (R=6.8!)
        (8333, 2,  8, "buy",  2.75, -2_240),    # S3 hedge SL
        (8444, 0,  5, "buy",  2.20,   +650),    # tiny gain
        (8555, 1,  6, "sell", 2.20, +13_440),   # S5 reverse short big TP
        (8666, 0,  7, "sell", 2.20, -2_240),    # S6 hedge SL
        (8222, 0,  5, "buy",  2.20, +13_440),   # S2 reverse long TP
        (8444, 3,  6, "buy",  2.20, +13_440),   # S4 reverse long TP
        (8111, 3,  4, "sell", 2.20, -2_240),    # S1 hedge SL
    ]
    deal_id = 99_002_000
    for h_magic, doff, hour, side, vol, profit in hedge_seed:
        ts = base + timedelta(days=doff, hours=hour, minutes=(deal_id % 60))
        if ts >= today_start:
            continue
        price = 3390 + (deal_id % 20)
        sl_off = 5.5 if side == "buy" else -5.5
        # TP relative to parent SL distance × tp_mult (R-ratio)
        r = HEDGE_R_RATIO[h_magic]
        tp_off = (5.5 * r) * (1 if side == "buy" else -1)
        earlier_hedge.append(mk_deal(
            deal_id, ts, magic=h_magic, side=side, vol=vol, price=price, profit=float(profit),
            sl=price - sl_off, tp=price + tp_off,
            comment=f"{stream_for(h_magic)}_{'long' if side == 'buy' else 'short'}",
        ))
        deal_id += 1

    all_deals = today_deals + earlier_parent + earlier_hedge

    parent_today_np = sum(d["profit"] for d in today_parent)
    hedge_today_np  = sum(d["profit"] for d in today_hedge)
    parent_wtd_np   = sum(d["profit"] for d in today_parent + earlier_parent)
    hedge_wtd_np    = sum(d["profit"] for d in today_hedge + earlier_hedge)

    print(f"\n[2/3] Pushing {len(all_deals)} dummy deals:")
    print(f"      Today:   parent {len(today_parent)} (${parent_today_np:+,.0f})  +  "
          f"hedge {len(today_hedge)} (${hedge_today_np:+,.0f})  =  ${parent_today_np+hedge_today_np:+,.0f}")
    print(f"      Week:    parent {len(today_parent)+len(earlier_parent)} (${parent_wtd_np:+,.0f})  +  "
          f"hedge {len(today_hedge)+len(earlier_hedge)} (${hedge_wtd_np:+,.0f})  =  "
          f"${parent_wtd_np+hedge_wtd_np:+,.0f}")
    print(f"      Hedge share of total: "
          f"{hedge_wtd_np / max(abs(parent_wtd_np+hedge_wtd_np), 1) * 100:+.1f}%")

    account_snap = {
        "ts": now.isoformat(),
        "balance": ACCOUNT_BALANCE,
        "equity": ACCOUNT_BALANCE + 95.0,
        "margin": 1_240.0,
        "margin_free": ACCOUNT_BALANCE - 1_240.0,
        "open_positions": 3,
        "unrealized": 95.0,
    }
    ok = publish_deals(all_deals, account=account_snap)
    print(f"      deals push: {'OK' if ok else 'FAIL'}")

    # ===== Open positions =====
    positions = [
        mk_position(99_900_001, magic=1111, side="buy",  vol=2.20, price=3408.30,
                    sl=3401.30, tp=3425.80, unrealized=+340.00, comment="ORB_S1_long"),
        mk_position(99_900_002, magic=4444, side="sell", vol=2.20, price=3411.50,
                    sl=3417.00, tp=3389.50, unrealized= -80.00, comment="ORB_S4_short"),
        mk_position(99_900_003, magic=6666, side="buy",  vol=2.20, price=3409.80,
                    sl=3404.30, tp=3426.30, unrealized=+95.00, comment="ORB_S6_long"),
    ]
    print(f"\n[3/3] Pushing {len(positions)} open positions...")
    ok = publish_positions(positions, account=account_snap, snapshot_id="dummy_v3")
    print(f"      positions push: {'OK' if ok else 'FAIL'}")

    # Optional: info alert noting this is dummy data
    publish_alert("info", "v3_dummy_data_test",
                  f"v3 reverse-hedge dummy data pushed: parent ${parent_wtd_np:+,.0f} + "
                  f"hedge ${hedge_wtd_np:+,.0f}",
                  context={"setfile": proj["setfile"]})

    print(f"\n[DONE] Dashboard should populate in ~60s.")
    print(f"  Verify at https://dt818-console.pages.dev/")
    print(f"  Setfile card should now read:  {proj['setfile']}")
    print(f"  6 parent rows (1111-6666) + 6 hedge rows (8111-8666) expected")
    print(f"  Weekly projection card:        mean $27,056 / p10 $9,200 / p90 $42,000")
    print(f"\nTo wipe: python scripts/populate_dummy_dashboard.py --clear")
    return 0


def mk_deal(deal_id: int, ts: datetime, magic: int, side: str, vol: float,
            price: float, profit: float, sl: float, tp: float, comment: str) -> dict:
    return {
        "deal_id": deal_id,
        "ts": ts.astimezone(timezone.utc).isoformat(),
        "magic": magic, "stream": stream_for(magic),
        "symbol": SYMBOL, "side": side, "volume": vol, "price": price,
        "sl": sl, "tp": tp, "profit": profit, "comment": comment,
        "position_id": deal_id, "balance_after": ACCOUNT_BALANCE,
        "is_hedge": magic >= 8000,
        "parent_magic": HEDGE_PARENT_OF.get(magic),
    }


def mk_position(ticket: int, magic: int, side: str, vol: float, price: float,
                 sl: float, tp: float, unrealized: float, comment: str) -> dict:
    CS = 100  # XAUUSD.sc contract size
    sign = 1 if side == "buy" else -1
    return {
        "ticket": ticket, "magic": magic, "stream": stream_for(magic),
        "symbol": SYMBOL, "side": side, "volume": vol, "price_open": price,
        "sl": sl, "tp": tp, "unrealized": unrealized,
        "sl_usd": sign * (sl - price) * CS * vol,
        "tp_usd": sign * (tp - price) * CS * vol,
        "comment": comment,
        "ts_open": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
        "is_hedge": magic >= 8000,
    }


if __name__ == "__main__":
    sys.exit(main())
