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
PROJECTION_PATH = ROOT / "output" / "forward_projection.json"
DEFAULT_SYMBOL = "XAUUSD.sc"
STREAM_NAMES = {1000: "FBO", 2000: "ORB", 3000: "LSFVG", 4000: "EMP",
                1111: "ORB_S1", 2222: "ORB_S2", 3333: "ORB_S3",
                4444: "ORB_S4", 5555: "ORB_S5", 6666: "ORB_S6"}


def print_projection_vs_actual_weekly(week_np: float, current_balance: float, days: int) -> None:
    """Compare actual week NP vs View C projection from output/forward_projection.json."""
    if not PROJECTION_PATH.exists():
        return
    try:
        import json as _json
        proj = _json.loads(PROJECTION_PATH.read_text())
    except Exception:
        return

    base_bal = float(proj.get("baseline_balance", current_balance))
    scale = current_balance / base_bal if base_bal > 0 else 1.0
    wk = proj.get("weekly_live", {})
    tol = proj.get("tolerance", {})

    # If window is not 7 days, prorate the projection.
    prorate = days / 7.0 if days > 0 else 1.0
    weekly_mean = wk.get("mean_np", 0.0) * scale * prorate
    weekly_p10 = wk.get("p10_np", 0.0) * scale * prorate
    weekly_p90 = wk.get("p90_np", 0.0) * scale * prorate
    weekly_worst = wk.get("worst_np", 0.0) * scale * prorate
    week_red_trip = tol.get("single_week_red_usd", -50_000.0) * scale * prorate
    green_prob = wk.get("green_week_prob", 0.0)

    def status(actual, p10, p90, mean):
        if actual >= p90: return "ABOVE p90 (top 10%)"
        if actual >= mean: return "above mean"
        if actual >= p10: return "in expected range"
        return "BELOW p10 (bottom 10%)"

    print("\n" + "=" * 84)
    print(f"  WEEKLY PROJECTION vs ACTUAL (View C, setfile: {proj.get('setfile', '?')})")
    print(f"  Method: decay {proj.get('decay_factor', 0):.2f} x live haircut {proj.get('live_haircut_np', 0):.2f}"
          f" = combined {proj.get('combined_haircut', 0):.3f}    avg slope {proj.get('avg_oos_slope_pct', 0):+.1f}%")
    if abs(scale - 1.0) > 0.01:
        print(f"  Balance-scaled to ${current_balance:,.0f} (proj baseline ${base_bal:,.0f}, x{scale:.2f})")
    if abs(prorate - 1.0) > 0.01:
        print(f"  Window prorated to {days}d (projection is per 7d, x{prorate:.2f})")
    print("-" * 84)
    print(f"  Actual {days}d NP:        ${week_np:>+12,.0f}   ROI: {week_np / max(current_balance,1) * 100:+.2f}% on ${current_balance:,.0f}")
    print(f"  Expected mean:          ${weekly_mean:>+12,.0f}")
    print(f"  Expected range (p10-p90):  ${weekly_p10:>+12,.0f}  to  ${weekly_p90:+,.0f}")
    print(f"  Worst sim week (post-haircut): ${weekly_worst:+,.0f}")
    print(f"  Green-week probability: {green_prob*100:.0f}%")
    print(f"  Status: {status(week_np, weekly_p10, weekly_p90, weekly_mean)}")
    if week_np <= week_red_trip:
        print(f"  !! Investigation trigger breached: ${week_np:+,.0f} <= ${week_red_trip:+,.0f}")
    print("=" * 84)


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

        # BROKER-TZ FIX: history_deals_get reads datetime args as broker
        # wall-clock (Vantage = UTC+3 summer / UTC+2 winter, auto-detected).
        # Pass shifted bounds + filter strictly to broker-as-epoch range.
        # See feedback_no_unverified_account_claims.md.
        from zgb_sim.mt5_accounts import get_broker_offset
        broker_off = get_broker_offset(args.symbol)
        mt5_start = start + broker_off
        mt5_end = max(end, datetime.now(timezone.utc)) + broker_off
        s_epoch = int(mt5_start.timestamp())
        e_epoch = int(mt5_end.timestamp())
        raw = mt5.history_deals_get(mt5_start, mt5_end) or ()
        deals = [d for d in raw if s_epoch <= d.time <= e_epoch]
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

        # Projection comparison (View C — read output/forward_projection.json)
        prod_magics = {1111, 2222, 3333, 4444, 5555, 6666}
        prod_week_np = sum(s["net"] for m, s in weekly_stream.items() if m in prod_magics)
        print_projection_vs_actual_weekly(prod_week_np, ai.balance, args.days)

        # Push to dt818-console (fails-open if .env unconfigured).
        try:
            from zgb_sim.cf_publish import publish_weekly_recap
            by_stream_dict = {STREAM_NAMES.get(m, f"m{m}"): {
                "trades": s["trades"], "wins": s["wins"], "losses": s["losses"],
                "gross_profit": s["gross_profit"], "gross_loss": s["gross_loss"],
                "net": s["net"],
            } for m, s in weekly_stream.items()}
            publish_weekly_recap(
                week_ending=end.date().isoformat(), days=args.days,
                net_pnl=float(grand_net), balance_end=float(ai.balance),
                trades=sum(s["trades"] for s in weekly_stream.values()),
                wins=sum(s["wins"] for s in weekly_stream.values()),
                losses=sum(s["losses"] for s in weekly_stream.values()),
                by_stream=by_stream_dict,
            )
        except Exception as e:
            print(f"  [cf_publish] skipped: {type(e).__name__}: {e}")

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
