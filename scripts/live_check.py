"""Daily live-account pulse for the trading account.

Workflow:
  1. Init Vantage MT5 (launches terminal64 if not running)
  2. Read last check timestamp; default to 24h ago
  3. Pull closed deals since last check (filtered to symbol + relevant magics)
  4. Aggregate per-magic stream: trades, wins, losses, P&L
  5. Read current account balance/equity
  6. Append events to journal file; update last_check timestamp
  7. Print summary; close MT5 cleanly

Usage:
  python scripts/live_check.py [--since YYYY-MM-DD] [--symbol XAUUSD.sc]

Triggered by the user typing "/live-check" in chat.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

# MT5 Files dir (sandbox accessible to read EAs but useful for our journal too)
MT5_FILES_DIR = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400/MQL5/Files")
JOURNAL = MT5_FILES_DIR / "dt818_pro_journal.jsonl"
LAST_CHECK = MT5_FILES_DIR / "last_check.txt"

DEFAULT_SYMBOL = "XAUUSD"
# Production streams only. FBO (1000), LSFVG (3000), EMP (4000), legacy ORB (2000/2100)
# were removed from the EA before May 4 2026 — any deals in their magics are historical
# and will fall through to the "m<magic>" fallback labelling.
STREAM_NAMES = {1111: "ORB_S1", 2222: "ORB_S2", 3333: "ORB_S3",
                4444: "ORB_S4", 5555: "ORB_S5", 6666: "ORB_S6",
                5111: "HEDGE_S1", 5222: "HEDGE_S2", 5333: "HEDGE_S3"}
# Active v2 stream-source mapping. Update at every Sat reopt:
#   S1-3 = PREVIOUS-week WFO  |  S4-6 = CURRENT-week WFO
# Last rotation: 2026-05-09 (S1-3 = MAY2 R1-3 / S4-6 = MAY9 R1-3)
STREAM_SOURCE = {1111: "MAY2 R1 (prev)", 2222: "MAY2 R2 (prev)", 3333: "MAY2 R3 (prev)",
                 4444: "MAY9 R1 (curr)", 5555: "MAY9 R2 (curr)", 6666: "MAY9 R3 (curr)"}
# XAUUSD.sc reports trade_contract_size=1.0 in symbol_info but realized P&L
# reconciles only with 100 oz/lot. Verified via order history 2026-05-03.
CONTRACT_SIZE = 100


def read_last_check() -> datetime:
    if LAST_CHECK.exists():
        ts = LAST_CHECK.read_text().strip()
        try:
            return datetime.fromisoformat(ts).astimezone(timezone.utc)
        except Exception:
            pass
    return datetime.now(timezone.utc) - timedelta(hours=24)


def write_last_check(ts: datetime) -> None:
    MT5_FILES_DIR.mkdir(parents=True, exist_ok=True)
    LAST_CHECK.write_text(ts.isoformat())


def append_journal(events: list[dict]) -> None:
    if not events:
        return
    MT5_FILES_DIR.mkdir(parents=True, exist_ok=True)
    with JOURNAL.open("a", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, default=str) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=None,
                    help="ISO datetime; default = last_check.txt or 24h ago")
    ap.add_argument("--symbol", default=DEFAULT_SYMBOL)
    ap.add_argument("--no-update-marker", action="store_true",
                    help="Do not advance last_check.txt (for ad-hoc queries)")
    ap.add_argument("--history-days", type=int, default=7,
                    help="Also show per-stream summary over the last N days (default 7); 0 to disable")
    args = ap.parse_args()

    if args.since:
        start = datetime.fromisoformat(args.since).astimezone(timezone.utc)
    else:
        start = read_last_check()
    end = datetime.now(timezone.utc)

    import MetaTrader5 as mt5
    from zgb_sim.tick_loader import kill_mt5_terminal
    from zgb_sim.mt5_accounts import init_account

    try:
        spec = init_account("live")
    except Exception as e:
        print(f"MT5 init failed: {e}")
        return 1
    # Live-check always pulls from the live trading account's symbol
    args.symbol = spec.symbol
    try:
        ti = mt5.terminal_info()
        ai = mt5.account_info()
        if ai is None:
            print(f"No account info (last_error={mt5.last_error()})")
            return 1

        print("=" * 78)
        print(f"  LIVE CHECK — {ai.company} acct {ai.login}  ({ti.name} build {ti.build})")
        print(f"  Window: {start.isoformat()}  ->  {end.isoformat()}")
        print("=" * 78)
        print(f"  Balance: ${ai.balance:,.2f}   Equity: ${ai.equity:,.2f}   "
              f"Margin: ${ai.margin:,.2f}   Free: ${ai.margin_free:,.2f}")
        print(f"  v2 stream-source mapping (S1-3=prev week, S4-6=curr week):")
        for mag in (1111, 2222, 3333, 4444, 5555, 6666):
            print(f"    {STREAM_NAMES[mag]:<7} ({mag}) -> {STREAM_SOURCE.get(mag, '?')}")
        print("=" * 78)

        def aggregate(window_start, window_end, collect_events: bool):
            deals = mt5.history_deals_get(window_start, window_end) or ()
            by_magic: dict[int, dict] = {}
            events: list[dict] = []
            for d in deals:
                if d.symbol != args.symbol:
                    continue
                mag = int(d.magic)
                if collect_events:
                    stream_name = STREAM_NAMES.get(mag, f"magic_{mag}")
                    entry = "in" if d.entry == 0 else ("out" if d.entry == 1 else f"e{d.entry}")
                    events.append({
                        "ts": datetime.fromtimestamp(d.time_msc / 1000, tz=timezone.utc).isoformat(),
                        "stream": stream_name, "magic": mag, "deal": int(d.ticket),
                        "kind": entry, "side": "buy" if d.type == 0 else ("sell" if d.type == 1 else f"t{d.type}"),
                        "price": d.price, "volume": d.volume, "profit": d.profit,
                        "comment": d.comment,
                    })
                agg = by_magic.setdefault(mag, {"trades": 0, "wins": 0, "losses": 0,
                                                  "gross_profit": 0.0, "gross_loss": 0.0, "net": 0.0})
                if d.entry == 1:
                    agg["trades"] += 1
                    if d.profit > 0:
                        agg["wins"] += 1
                        agg["gross_profit"] += d.profit
                    elif d.profit < 0:
                        agg["losses"] += 1
                        agg["gross_loss"] += d.profit
                    agg["net"] += d.profit
            return by_magic, events

        def print_summary(by_magic: dict, label: str):
            total_trades = sum(a["trades"] for a in by_magic.values())
            print(f"\n  {label}: {total_trades} closed trades on {args.symbol}")
            if not by_magic:
                print(f"  No deals in window.")
                return
            print(f"  {'Stream':<8} {'Magic':>5} {'Trades':>6} {'W/L':>6} {'Gross+':>10} {'Gross-':>10} {'Net':>10}")
            grand_net = 0.0
            for mag in sorted(by_magic):
                a = by_magic[mag]
                stream = STREAM_NAMES.get(mag, f"m{mag}")
                wl = f"{a['wins']}/{a['losses']}"
                pf = a["gross_profit"] / abs(a["gross_loss"]) if a["gross_loss"] < 0 else float("inf")
                print(f"  {stream:<8} {mag:>5} {a['trades']:>6} {wl:>6} "
                      f"${a['gross_profit']:>+8,.0f} ${a['gross_loss']:>+8,.0f} "
                      f"${a['net']:>+8,.0f}  PF={pf:.2f}")
                grand_net += a["net"]
            print(f"  TOTAL net P&L: ${grand_net:+,.2f}")

        by_magic, events_to_journal = aggregate(start, end, collect_events=True)
        print_summary(by_magic, "Daily window (since last check)")

        if args.history_days > 0:
            hist_start = end - timedelta(days=args.history_days)
            hist_by_magic, _ = aggregate(hist_start, end, collect_events=False)
            print(f"\n  --- History: last {args.history_days} days "
                  f"({hist_start.date()} -> {end.date()}) ---")
            print_summary(hist_by_magic, f"Last {args.history_days}d")

        # Open positions snapshot — full table with SL/TP in $, magic, comments
        mt5.symbol_select(args.symbol, True)
        tick = mt5.symbol_info_tick(args.symbol)
        positions = mt5.positions_get(symbol=args.symbol) or ()
        if positions:
            print(f"\n  [OPEN POSITIONS]  {len(positions)} on {args.symbol}  "
                  f"(bid={tick.bid:.2f} ask={tick.ask:.2f})")
            print(f"  {'Ticket':>10} {'Magic':>5} {'Stream':<7} {'Side':<4} {'Vol':>5} "
                  f"{'Open':>9} {'SL':>9} {'SL$':>8} {'TP':>9} {'TP$':>8} {'Unr$':>8} {'Comment':<28}")
            tot_unr = tot_sl = tot_tp = 0.0
            for p in sorted(positions, key=lambda x: x.time):
                sn = STREAM_NAMES.get(int(p.magic), f"m{p.magic}")
                side = "BUY" if p.type == 0 else "SELL"
                sign = 1 if p.type == 0 else -1
                sl_usd = sign * (p.sl - p.price_open) * CONTRACT_SIZE * p.volume if p.sl > 0 else 0.0
                tp_usd = sign * (p.tp - p.price_open) * CONTRACT_SIZE * p.volume if p.tp > 0 else 0.0
                print(f"  {p.ticket:>10} {p.magic:>5} {sn:<7} {side:<4} {p.volume:>5.2f} "
                      f"{p.price_open:>9.2f} {p.sl:>9.2f} ${sl_usd:>+6,.0f} "
                      f"{p.tp:>9.2f} ${tp_usd:>+6,.0f} ${p.profit:>+6,.0f} "
                      f"{(p.comment or '')[:28]:<28}")
                tot_unr += p.profit; tot_sl += sl_usd; tot_tp += tp_usd
            eq = ai.equity if ai.equity else 1
            print(f"\n  TOTALS  unrealized=${tot_unr:+,.2f}  "
                  f"if-all-SL=${tot_sl:+,.2f} ({tot_sl/eq*100:+.2f}% eq)  "
                  f"if-all-TP=${tot_tp:+,.2f} ({tot_tp/eq*100:+.2f}% eq)")

            # Entry-side comments for the open positions
            open_pids = {p.identifier for p in positions}
            ent_lookback = end - timedelta(days=max(args.history_days, 30))
            ent_deals = mt5.history_deals_get(ent_lookback, end) or ()
            ent = [d for d in ent_deals
                   if d.symbol == args.symbol and d.entry == 0 and d.position_id in open_pids]
            if ent:
                print(f"\n  [ENTRY-SIDE COMMENTS for open positions]")
                print(f"  {'PosID':>10} {'Magic':>5} {'Stream':<7} {'Side':<4} "
                      f"{'EntryPx':>9} {'EntryComment':<32}")
                for d in sorted(ent, key=lambda x: x.time_msc):
                    sn = STREAM_NAMES.get(int(d.magic), f"m{d.magic}")
                    side = "BUY" if d.type == 0 else "SELL"
                    print(f"  {d.position_id:>10} {d.magic:>5} {sn:<7} {side:<4} "
                          f"{d.price:>9.2f} {(d.comment or '')[:32]:<32}")

        # Closed-trade detail rows for the daily window (with magic + comments)
        win_deals = mt5.history_deals_get(start, end) or ()
        win_closed = [d for d in win_deals if d.symbol == args.symbol and d.entry == 1]
        if win_closed:
            print(f"\n  [CLOSED TRADES detail]  daily window -- {len(win_closed)}")
            print(f"  {'Closed (UTC)':<17} {'PosID':>10} {'Magic':>5} {'Stream':<7} "
                  f"{'Side':<4} {'Vol':>5} {'Exit':>9} {'P&L$':>10} {'ExitComment':<28}")
            for d in sorted(win_closed, key=lambda x: x.time_msc):
                sn = STREAM_NAMES.get(int(d.magic), f"m{d.magic}")
                side = "BUY" if d.type == 0 else "SELL"
                t = datetime.fromtimestamp(d.time_msc / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                print(f"  {t:<17} {d.position_id:>10} {d.magic:>5} {sn:<7} "
                      f"{side:<4} {d.volume:>5.2f} {d.price:>9.2f} ${d.profit:>+8,.2f} "
                      f"{(d.comment or '')[:28]:<28}")

        # Persist
        append_journal(events_to_journal)
        if not args.no_update_marker:
            write_last_check(end)
            print(f"\n  Last-check marker advanced to {end.isoformat()}")
        else:
            print(f"\n  (last-check marker unchanged; ad-hoc query)")
        print(f"  Journal: {JOURNAL}")
    finally:
        mt5.shutdown()
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
