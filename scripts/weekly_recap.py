"""Weekly recap of the live trading account.

Workflow:
  1. Init Vantage MT5 (launches terminal64 if not running)
  2. Pull deals for the recap window (default last 7 days)
  3. Group by day + by stream; report per-day P&L, win rate, trade count
  4. Weekly stream totals + win rate
  5. Best/worst day; balance change over window
  6. Append a calibration row to output/live_calibration_log.csv
  7. Close MT5 cleanly

Usage:
  python scripts/weekly_recap.py [--days 7] [--symbol XAUUSD.sc]

Triggered by user typing "/weekly-recap" in chat.

Designed to read-only — does NOT advance the daily last_check.txt marker
(that belongs to /live-check).
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

CALIB_LOG = ROOT / "output" / "live_calibration_log.csv"
DEFAULT_SYMBOL = "XAUUSD.sc"
STREAM_NAMES = {1000: "FBO", 2000: "ORB", 3000: "LSFVG", 4000: "EMP"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7,
                    help="Recap window length in days (default 7)")
    ap.add_argument("--symbol", default=DEFAULT_SYMBOL)
    args = ap.parse_args()

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=args.days)

    import MetaTrader5 as mt5
    from zgb_sim.tick_loader import kill_mt5_terminal

    if not mt5.initialize():
        print(f"MT5 init failed: {mt5.last_error()}")
        kill_mt5_terminal()
        return 1
    try:
        ai = mt5.account_info()
        ti = mt5.terminal_info()
        if ai is None:
            print(f"No account info (last_error={mt5.last_error()})")
            return 1

        print("=" * 84)
        print(f"  WEEKLY RECAP — {ai.company} acct {ai.login}  ({ti.name} build {ti.build})")
        print(f"  Window: {start.date()} -> {end.date()}  ({args.days}d)   Symbol: {args.symbol}")
        print("=" * 84)
        print(f"  Current balance: ${ai.balance:,.2f}   Equity: ${ai.equity:,.2f}")

        deals = mt5.history_deals_get(start, end) or ()
        # Filter to symbol; keep only EXIT deals (carry realized P&L)
        exits = [d for d in deals if d.symbol == args.symbol and d.entry == 1]

        if not exits:
            print(f"\n  No closed trades in window.")
            return 0

        # Group by date
        by_day: dict[str, dict] = defaultdict(lambda: {
            "trades": 0, "wins": 0, "losses": 0, "net": 0.0,
            "by_stream": defaultdict(lambda: {"trades": 0, "wins": 0, "losses": 0, "net": 0.0})
        })
        # Also track weekly per-stream
        weekly_stream: dict[int, dict] = defaultdict(lambda: {
            "trades": 0, "wins": 0, "losses": 0, "gross_profit": 0.0, "gross_loss": 0.0, "net": 0.0
        })

        for d in exits:
            ts = datetime.fromtimestamp(d.time_msc / 1000, tz=timezone.utc)
            day_key = ts.date().isoformat()
            mag = int(d.magic)
            day = by_day[day_key]
            day["trades"] += 1
            day["net"] += d.profit
            ds = day["by_stream"][mag]
            ds["trades"] += 1
            ds["net"] += d.profit
            if d.profit > 0:
                day["wins"] += 1; ds["wins"] += 1
            elif d.profit < 0:
                day["losses"] += 1; ds["losses"] += 1
            ws = weekly_stream[mag]
            ws["trades"] += 1
            ws["net"] += d.profit
            if d.profit > 0:
                ws["wins"] += 1; ws["gross_profit"] += d.profit
            elif d.profit < 0:
                ws["losses"] += 1; ws["gross_loss"] += d.profit

        # Per-day breakdown
        print(f"\n  --- PER-DAY P&L ---")
        print(f"  {'Date':<12} {'Trades':>6} {'W/L':>6} {'Net P&L':>12}  Streams")
        days_sorted = sorted(by_day.keys())
        for dkey in days_sorted:
            day = by_day[dkey]
            wl = f"{day['wins']}/{day['losses']}"
            stream_breakdown = " │ ".join(
                f"{STREAM_NAMES.get(m,'m'+str(m))}:{s['trades']}@${s['net']:+,.0f}"
                for m, s in day["by_stream"].items() if s["trades"] > 0
            )
            print(f"  {dkey:<12} {day['trades']:>6} {wl:>6} ${day['net']:>+10,.2f}  {stream_breakdown}")

        # Weekly stream totals
        print(f"\n  --- WEEKLY PER-STREAM ---")
        print(f"  {'Stream':<8} {'Magic':>5} {'Trades':>6} {'W/L':>6} {'WR%':>5} "
              f"{'Gross+':>10} {'Gross-':>10} {'Net':>11} {'PF':>6}")
        grand_net = 0.0
        for mag in sorted(weekly_stream):
            s = weekly_stream[mag]
            stream = STREAM_NAMES.get(mag, f"m{mag}")
            wl = f"{s['wins']}/{s['losses']}"
            wr = (s["wins"] / max(s["trades"], 1)) * 100
            pf = s["gross_profit"] / abs(s["gross_loss"]) if s["gross_loss"] < 0 else float("inf")
            print(f"  {stream:<8} {mag:>5} {s['trades']:>6} {wl:>6} {wr:>4.0f}% "
                  f"${s['gross_profit']:>+8,.0f} ${s['gross_loss']:>+8,.0f} "
                  f"${s['net']:>+9,.0f} {pf:>6.2f}")
            grand_net += s["net"]
        print(f"  {'TOTAL':<8} {'':>5} {sum(s['trades'] for s in weekly_stream.values()):>6}"
              f" {'':>6} {'':>5} {'':>10} {'':>10} ${grand_net:>+9,.0f}")

        # Best / worst day
        if len(days_sorted) > 0:
            best = max(days_sorted, key=lambda k: by_day[k]["net"])
            worst = min(days_sorted, key=lambda k: by_day[k]["net"])
            n_trading = sum(1 for k in days_sorted if by_day[k]["trades"] > 0)
            print(f"\n  --- HIGHLIGHTS ---")
            print(f"  Trading days:    {n_trading}/{args.days}")
            print(f"  Best day:        {best}  (${by_day[best]['net']:+,.2f})")
            print(f"  Worst day:       {worst}  (${by_day[worst]['net']:+,.2f})")
            print(f"  Window net P&L:  ${grand_net:+,.2f}")
            if ai.balance > 0:
                roi_pct = grand_net / (ai.balance - grand_net) * 100  # ROI on starting balance
                print(f"  ROI:             {roi_pct:+.2f}%")

        # Append calibration row (just live numbers; sim cross-check is a future addition)
        CALIB_LOG.parent.mkdir(parents=True, exist_ok=True)
        is_new = not CALIB_LOG.exists()
        with CALIB_LOG.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if is_new:
                w.writerow(["recap_date", "window_start", "window_end", "trades",
                            "wins", "losses", "net_live", "balance_end", "by_stream_json"])
            import json
            stream_json = json.dumps({STREAM_NAMES.get(m, f"m{m}"): s for m, s in weekly_stream.items()},
                                      default=str)
            w.writerow([end.date().isoformat(), start.date().isoformat(),
                        end.date().isoformat(),
                        sum(s["trades"] for s in weekly_stream.values()),
                        sum(s["wins"] for s in weekly_stream.values()),
                        sum(s["losses"] for s in weekly_stream.values()),
                        f"{grand_net:.2f}", f"{ai.balance:.2f}", stream_json])
        print(f"\n  Calibration log row appended → {CALIB_LOG}")
    finally:
        mt5.shutdown()
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
