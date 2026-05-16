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


def print_projection_vs_actual(today_np: float, wtd_np: float, current_balance: float) -> None:
    """Read output/forward_projection.json and print actual vs View C expectation."""
    if not PROJECTION_PATH.exists():
        return
    try:
        proj = json.loads(PROJECTION_PATH.read_text())
    except Exception:
        return

    # Scale projection from its baseline_balance to today's balance.
    base_bal = float(proj.get("baseline_balance", current_balance))
    scale = current_balance / base_bal if base_bal > 0 else 1.0
    wk = proj.get("weekly_live", {})
    dly = proj.get("daily_live", {})
    tol = proj.get("tolerance", {})

    daily_mean = dly.get("mean_np", 0.0) * scale
    daily_p10 = dly.get("p10_np", 0.0) * scale
    daily_p90 = dly.get("p90_np", 0.0) * scale
    daily_red_trip = tol.get("daily_outlier_red_usd", -25_000.0) * scale
    weekly_mean = wk.get("mean_np", 0.0) * scale
    weekly_p10 = wk.get("p10_np", 0.0) * scale
    weekly_p90 = wk.get("p90_np", 0.0) * scale
    week_red_trip = tol.get("single_week_red_usd", -50_000.0) * scale

    def status(actual, p10, p90, mean):
        if actual >= p90:
            return f"ABOVE p90 (best 10%)"
        if actual >= mean:
            return f"above mean"
        if actual >= p10:
            return f"in expected range"
        return f"BELOW p10 (worst 10%)"

    def trip(actual, trip_threshold):
        if trip_threshold < 0 and actual <= trip_threshold:
            return "  !! TRIGGER: investigation threshold breached"
        return ""

    print("\n" + "=" * 78)
    print(f"  PROJECTION vs ACTUAL  (View C, setfile: {proj.get('setfile', '?')})")
    print(f"  Decay {proj.get('decay_factor', 0):.2f} x live_haircut {proj.get('live_haircut_np', 0):.2f}"
          f" = combined {proj.get('combined_haircut', 0):.3f}    "
          f"avg slope {proj.get('avg_oos_slope_pct', 0):+.1f}%")
    if abs(scale - 1.0) > 0.01:
        print(f"  Balance-scaled to current ${current_balance:,.0f} (proj baseline ${base_bal:,.0f}, x{scale:.2f})")
    print("-" * 78)
    print(f"  {'Metric':<22} {'Actual':>12} {'Expected mean':>15} {'p10':>12} {'p90':>12}  Status")
    print(f"  {'TODAY (daily window)':<22} ${today_np:>+10,.0f}  ${daily_mean:>+13,.0f}"
          f"  ${daily_p10:>+10,.0f}  ${daily_p90:>+10,.0f}  {status(today_np, daily_p10, daily_p90, daily_mean)}")
    print(f"  {'WEEK-to-date (7d)':<22} ${wtd_np:>+10,.0f}  ${weekly_mean:>+13,.0f}"
          f"  ${weekly_p10:>+10,.0f}  ${weekly_p90:>+10,.0f}  {status(wtd_np, weekly_p10, weekly_p90, weekly_mean)}")
    print()
    if today_np <= daily_red_trip:
        print(f"  !! DAILY outlier: ${today_np:+,.0f} <= trigger ${daily_red_trip:+,.0f} -- investigate today's fills.")
    if wtd_np <= week_red_trip:
        print(f"  !! WEEKLY outlier: ${wtd_np:+,.0f} <= trigger ${week_red_trip:+,.0f} -- pause + re-eval setfile.")
    print("=" * 78)

# MT5 Files dir (sandbox accessible to read EAs but useful for our journal too)
MT5_FILES_DIR = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400/MQL5/Files")
JOURNAL = MT5_FILES_DIR / "dt818_pro_journal.jsonl"
LAST_CHECK = MT5_FILES_DIR / "last_check.txt"

DEFAULT_SYMBOL = "XAUUSD"
PROJECTION_PATH = ROOT / "output" / "forward_projection.json"
# Production streams only. FBO (1000), LSFVG (3000), EMP (4000), legacy ORB (2000/2100)
# were removed from the EA before May 4 2026 — any deals in their magics are historical
# and will fall through to the "m<magic>" fallback labelling.
STREAM_NAMES = {1111: "ORB_S1", 2222: "ORB_S2", 3333: "ORB_S3",
                4444: "ORB_S4", 5555: "ORB_S5", 6666: "ORB_S6",
                7111: "ORB_S1h", 7222: "ORB_S2h", 7333: "ORB_S3h",
                7444: "ORB_S4h", 7555: "ORB_S5h", 7666: "ORB_S6h",
                5111: "HEDGE_S1", 5222: "HEDGE_S2", 5333: "HEDGE_S3"}
# Active v2 stream-source mapping. Update at every Sat reopt:
#   S1-3 = PREVIOUS-week WFO  |  S4-6 = CURRENT-week WFO
# Last rotation: 2026-05-16 (S1-3 = MAY9 R1-3 / S4-6 = MAY16 R2/R3/R4, dup-skipped)
STREAM_SOURCE = {1111: "MAY9 R1 (prev)", 2222: "MAY9 R2 (prev)", 3333: "MAY9 R3 (prev)",
                 4444: "MAY16 R2 (curr)", 5555: "MAY16 R3 (curr)", 6666: "MAY16 R4 (curr)",
                 7111: "retry S1 tp=1.0", 7222: "retry S2 tp=0.75", 7333: "retry S3 tp=0.75",
                 7444: "retry S4 tp=1.0", 7555: "retry S5 tp=0.75", 7666: "retry S6 tp=0.75"}
# XAUUSD.sc reports trade_contract_size=1.0 in symbol_info but realized P&L
# reconciles only with 100 oz/lot. Verified via order history 2026-05-03.
CONTRACT_SIZE = 100


def read_last_check() -> tuple[datetime, float | None]:
    """Returns (ts, prior_balance). prior_balance is None for legacy text-only markers."""
    if LAST_CHECK.exists():
        raw = LAST_CHECK.read_text().strip()
        # Try JSON first (new format with balance)
        try:
            import json
            obj = json.loads(raw)
            ts = datetime.fromisoformat(obj["ts"]).astimezone(timezone.utc)
            return ts, float(obj.get("balance")) if obj.get("balance") is not None else None
        except Exception:
            pass
        # Legacy text-only marker
        try:
            return datetime.fromisoformat(raw).astimezone(timezone.utc), None
        except Exception:
            pass
    return datetime.now(timezone.utc) - timedelta(hours=24), None


def write_last_check(ts: datetime, balance: float | None = None) -> None:
    """Write marker with timestamp + (optional) balance for reconciliation on next run."""
    import json
    MT5_FILES_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"ts": ts.isoformat(), "balance": balance}
    LAST_CHECK.write_text(json.dumps(payload))


def reconcile_balance(prior_balance: float | None, current_balance: float,
                        deals, symbol: str) -> dict:
    """Compute balance reconciliation.

    Returns dict with:
      prior, current, delta_actual, delta_expected, gap, reconciled, deal_breakdown
    """
    # Sum all balance-affecting items from the deal window. Include EVERY symbol
    # and EVERY magic — this is account-level reconciliation, not strategy-level.
    type_names = {0: "BUY", 1: "SELL", 2: "BALANCE", 3: "CREDIT", 4: "CHARGE",
                   5: "CORRECTION", 6: "BONUS", 7: "COMMISSION",
                   8: "COMMISSION_DAILY", 9: "COMMISSION_MONTHLY",
                   10: "AGENT_DAILY", 11: "AGENT_MONTHLY", 12: "INTEREST",
                   13: "BUY_CANCELED", 14: "SELL_CANCELED", 15: "DIVIDEND",
                   16: "DIVIDEND_FRANKED", 17: "TAX"}
    breakdown = {}
    sum_profit = 0.0; sum_comm = 0.0; sum_swap = 0.0; sum_balance_ops = 0.0
    for d in deals:
        tn = type_names.get(d.type, f"UNK_{d.type}")
        breakdown.setdefault(tn, {"n": 0, "profit": 0.0, "commission": 0.0, "swap": 0.0})
        breakdown[tn]["n"] += 1
        breakdown[tn]["profit"] += d.profit
        breakdown[tn]["commission"] += d.commission
        breakdown[tn]["swap"] += d.swap
        sum_comm += d.commission
        sum_swap += d.swap
        if d.type in (2, 3, 4, 6):  # BALANCE, CREDIT, CHARGE, BONUS
            sum_balance_ops += d.profit
        else:
            sum_profit += d.profit
    delta_expected = sum_profit + sum_comm + sum_swap + sum_balance_ops
    delta_actual = current_balance - prior_balance if prior_balance is not None else None
    gap = (delta_actual - delta_expected) if delta_actual is not None else None
    return {
        "prior": prior_balance,
        "current": current_balance,
        "delta_actual": delta_actual,
        "delta_expected": delta_expected,
        "sum_profit": sum_profit, "sum_commission": sum_comm,
        "sum_swap": sum_swap, "sum_balance_ops": sum_balance_ops,
        "gap": gap,
        "reconciled": (gap is not None and abs(gap) < 1.0),
        "breakdown": breakdown,
    }


def print_reconciliation(rec: dict) -> None:
    """Print reconciliation gate at top of output. ALWAYS runs before any trade summary."""
    print()
    print("=" * 78)
    print("  BALANCE RECONCILIATION  (must reconcile before trusting downstream P&L)")
    print("=" * 78)
    if rec["prior"] is None:
        print("  [!] No prior balance in marker file (legacy text marker or first run).")
        print("      Cannot reconcile this run — will store balance for next run.")
        print(f"      Current balance: ${rec['current']:,.2f}")
        return
    print(f"  Prior balance:    ${rec['prior']:,.2f}")
    print(f"  Current balance:  ${rec['current']:,.2f}")
    print(f"  Actual delta:     ${rec['delta_actual']:>+12,.2f}")
    print(f"  Sum from deals (profit + comm + swap + balance ops):")
    print(f"    profit only:    ${rec['sum_profit']:>+12,.2f}")
    print(f"    commission:     ${rec['sum_commission']:>+12,.2f}")
    print(f"    swap:           ${rec['sum_swap']:>+12,.2f}")
    print(f"    balance ops:    ${rec['sum_balance_ops']:>+12,.2f}  "
          f"(deposits/withdrawals/credits/bonuses)")
    print(f"  Expected delta:   ${rec['delta_expected']:>+12,.2f}")
    print(f"  GAP (actual - expected): ${rec['gap']:>+12,.2f}")
    if rec["reconciled"]:
        print(f"  >>> RECONCILED  (gap within $1 tolerance)")
    else:
        print(f"  *** UNRECONCILED — GAP = ${rec['gap']:+,.2f} ***")
        print(f"  *** Cause unknown. Possible: deposit/withdrawal not surfaced as deal,")
        print(f"  *** broker correction, or marker-balance staleness. INVESTIGATE before")
        print(f"  *** trusting any P&L claim downstream.")
        print(f"  Per-deal-type breakdown for diagnosis:")
        for t, v in sorted(rec["breakdown"].items()):
            print(f"    {t:<22} n={v['n']:>3}  profit={v['profit']:>+10,.2f}  "
                  f"comm={v['commission']:>+8,.2f}  swap={v['swap']:>+7,.2f}")
    print("=" * 78)


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
        start, prior_balance = read_last_check()
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

        # ===== RECONCILIATION GATE =====
        # Pulls all deals (every magic, every type) for the window and confirms
        # that balance change == sum of deal-level effects. If not, halt downstream
        # claims. Saved per feedback_no_unverified_account_claims.md (2026-05-15).
        #
        # CRITICAL: history_deals_get treats datetime args as BROKER TIME, not
        # real UTC. Vantage broker = UTC+3. We must convert real-UTC bounds to
        # broker-labeled datetimes before passing in, otherwise we cut off
        # ~3h of recent trades. (Bug discovered 2026-05-15: API silently returned
        # 0 May 15 deals because window_end was 07:24 UTC = 04:24 broker, before
        # the May 15 trades at 08:30+ broker.)
        from zgb_sim.mt5_accounts import get_broker_offset
        broker_offset = get_broker_offset(spec.symbol)  # auto-detect (DST-safe)
        # Pass start/end shifted forward by broker_offset, AND extend end to
        # NOW + broker_offset to catch all just-completed deals.
        recon_start = start + broker_offset
        recon_end_real = max(end, datetime.now(timezone.utc))
        recon_end = recon_end_real + broker_offset
        recon_deals = mt5.history_deals_get(recon_start, recon_end) or ()
        # Filter out deals that fall outside the REAL-UTC window we asked about.
        # MT5 returns deals with d.time as broker-time-as-epoch; convert each to
        # real-UTC and drop those outside [start, recon_end_real].
        filtered = []
        start_broker_epoch = int((start + broker_offset).timestamp())
        end_broker_epoch = int(recon_end.timestamp())
        for d in recon_deals:
            if start_broker_epoch <= d.time <= end_broker_epoch:
                filtered.append(d)
        recon_deals = filtered
        rec = reconcile_balance(prior_balance, ai.balance, recon_deals, args.symbol)
        print_reconciliation(rec)
        print(f"  v2.1_h stream-source mapping (parent S1-6 + hedge S1h-S6h):")
        for mag in (1111, 2222, 3333, 4444, 5555, 6666,
                    7111, 7222, 7333, 7444, 7555, 7666):
            name = STREAM_NAMES.get(mag, f"m{mag}")
            print(f"    {name:<8} ({mag}) -> {STREAM_SOURCE.get(mag, '?')}")
        print("=" * 78)

        def aggregate(window_start, window_end, collect_events: bool):
            # BROKER-TZ FIX: history_deals_get treats datetimes as broker time
            # (Vantage UTC+3). Shift bounds forward by broker_offset and filter
            # returned deals against broker-shifted bounds. Without this fix the
            # window cut off the most recent ~3h of trades. See
            # feedback_no_unverified_account_claims.md (2026-05-15 incident).
            broker_off = broker_offset  # auto-detected at recon gate above
            ws_b = window_start + broker_off
            we_b = max(window_end, datetime.now(timezone.utc)) + broker_off
            ws_b_epoch = int(ws_b.timestamp())
            we_b_epoch = int(we_b.timestamp())
            raw_deals = mt5.history_deals_get(ws_b, we_b) or ()
            # Filter strictly to the requested window (in broker-time-as-epoch)
            deals = [d for d in raw_deals if ws_b_epoch <= d.time <= we_b_epoch]
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

            # Entry-side comments for the open positions (broker-tz fix applied)
            open_pids = {p.identifier for p in positions}
            ent_lookback = end - timedelta(days=max(args.history_days, 30))
            broker_off = broker_offset  # auto-detected at recon gate above
            ent_deals = mt5.history_deals_get(ent_lookback + broker_off,
                                                end + broker_off) or ()
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

        # Closed-trade detail rows for the daily window (broker-tz fix applied)
        broker_off = broker_offset  # auto-detected at recon gate above
        win_start_b = start + broker_off
        win_end_b = max(end, datetime.now(timezone.utc)) + broker_off
        win_start_epoch = int(win_start_b.timestamp())
        win_end_epoch = int(win_end_b.timestamp())
        win_deals_raw = mt5.history_deals_get(win_start_b, win_end_b) or ()
        win_deals = [d for d in win_deals_raw
                       if win_start_epoch <= d.time <= win_end_epoch]
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

        # Projection comparison (View C — read output/forward_projection.json)
        # Restrict to the 6 live ORB-stream magics so other historical magics
        # don't pollute the comparison.
        prod_magics = {1111, 2222, 3333, 4444, 5555, 6666}
        today_np = sum(a["net"] for m, a in by_magic.items() if m in prod_magics)
        wtd_start = end - timedelta(days=7)
        wtd_by_magic, _ = aggregate(wtd_start, end, collect_events=False)
        wtd_np = sum(a["net"] for m, a in wtd_by_magic.items() if m in prod_magics)

        # Push to dt818-console (fails-open if .env unconfigured).
        try:
            from zgb_sim.cf_publish import publish_deals, publish_positions
            now_iso = datetime.now(timezone.utc).isoformat()
            account_snap = {
                "ts": now_iso, "balance": float(ai.balance), "equity": float(ai.equity),
                "margin": float(ai.margin), "margin_free": float(ai.margin_free),
                "open_positions": len(positions), "unrealized": sum(float(p.profit) for p in positions),
            }
            deal_payloads = [{
                "deal_id": int(d.ticket),
                "ts": datetime.fromtimestamp(d.time_msc / 1000, tz=timezone.utc).isoformat(),
                "magic": int(d.magic), "stream": STREAM_NAMES.get(int(d.magic), f"m{d.magic}"),
                "symbol": d.symbol, "side": "buy" if d.type == 0 else "sell",
                "volume": float(d.volume), "price": float(d.price),
                "sl": None, "tp": None, "profit": float(d.profit),
                "comment": d.comment or None, "position_id": int(d.position_id),
                "balance_after": float(ai.balance),
            } for d in win_closed] if win_closed else []
            if deal_payloads:
                publish_deals(deal_payloads, account=account_snap)
            # Open positions snapshot
            position_payloads = [{
                "ticket": int(p.ticket), "magic": int(p.magic),
                "stream": STREAM_NAMES.get(int(p.magic), f"m{p.magic}"),
                "symbol": args.symbol, "side": "buy" if p.type == 0 else "sell",
                "volume": float(p.volume), "price_open": float(p.price_open),
                "sl": float(p.sl) if p.sl > 0 else None,
                "tp": float(p.tp) if p.tp > 0 else None,
                "unrealized": float(p.profit),
                "sl_usd": (1 if p.type == 0 else -1) * (float(p.sl) - float(p.price_open)) * CONTRACT_SIZE * float(p.volume) if p.sl > 0 else None,
                "tp_usd": (1 if p.type == 0 else -1) * (float(p.tp) - float(p.price_open)) * CONTRACT_SIZE * float(p.volume) if p.tp > 0 else None,
                "comment": (p.comment or None),
                "ts_open": datetime.fromtimestamp(p.time, tz=timezone.utc).isoformat(),
            } for p in positions]
            publish_positions(position_payloads, account=account_snap)
        except Exception as e:
            print(f"  [cf_publish] skipped: {type(e).__name__}: {e}")
        print_projection_vs_actual(today_np, wtd_np, ai.balance)

        # Persist
        append_journal(events_to_journal)
        if not args.no_update_marker:
            write_last_check(end, ai.balance)
            print(f"\n  Last-check marker advanced to {end.isoformat()} "
                  f"(balance ${ai.balance:,.2f} stored for next-run reconciliation)")
        else:
            print(f"\n  (last-check marker unchanged; ad-hoc query)")
        print(f"  Journal: {JOURNAL}")
    finally:
        mt5.shutdown()
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
