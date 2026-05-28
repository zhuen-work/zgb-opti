"""Counterfactual: replay THIS WEEK's live deals with a 4% GLOBAL daily loss cap.

Pulls actual closed deals from the live account (parents 1111-6666 + ALL hedge
magics: the configured 8111-8666 AND the buggy +7000 set 9222/10333/11444/
12555/13666 that fired since 05-26), reconstructs the chronological equity, and
overlays the v8 _GlobalDailyLossPct logic: once a broker-day's cumulative P&L
crosses -lossPct * day_start_balance, halt the rest of that day (drop later deals).

Compares actual (no cap) vs with-cap: NP, DD$, DD%, per-day, days capped.

MODEL NOTE: realized-P&L approximation — the cap fires at deal-CLOSE times, so
a day can overshoot to wherever the crossing deal lands before halting. The live
v8 EA fires intraday on realized+unrealized (MTM), so it would lock SOONER and
cap the day tighter. This backfill is therefore a CONSERVATIVE estimate of the
cap's benefit (real protection >= shown here).

Run:
  python scripts/backfill_daily_loss_cap.py
  python scripts/backfill_daily_loss_cap.py --since 2026-05-25 --loss-pct 4.0
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta, date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import MetaTrader5 as mt5
from zgb_sim.mt5_accounts import init_account
from zgb_sim.tick_loader import kill_mt5_terminal

PARENT_MAGICS = {1111, 2222, 3333, 4444, 5555, 6666}
HEDGE_MAGICS = {8111, 8222, 8333, 8444, 8555, 8666,        # configured
                9222, 10333, 11444, 12555, 13666}          # buggy +7000 set (live since 05-26)
EA_MAGICS = PARENT_MAGICS | HEDGE_MAGICS


def pull_deals(since: datetime):
    init_account("live")
    bal_now = mt5.account_info().balance
    frm = since - timedelta(hours=6)
    to = datetime.now(timezone.utc) + timedelta(hours=6)
    deals = mt5.history_deals_get(frm, to)
    rows = []
    for d in (deals or []):
        if int(d.magic) not in EA_MAGICS:
            continue
        if int(d.entry) != mt5.DEAL_ENTRY_OUT:
            continue
        ts = datetime.fromtimestamp(d.time, tz=timezone.utc)
        if ts < since:
            continue
        net = float(d.profit) + float(d.commission) + float(d.swap)
        rows.append((ts, int(d.magic), net))
    kill_mt5_terminal()
    rows.sort(key=lambda x: x[0])
    return rows, bal_now


def equity_metrics(deals, start_bal):
    """deals: [(ts, magic, pnl)] chronological. Returns NP, DD$, DD%, PF."""
    bal = start_bal; peak = start_bal; dd_abs = 0.0
    gains = losses = 0.0
    for _, _m, p in deals:
        bal += p
        if bal > peak: peak = bal
        if (peak - bal) > dd_abs: dd_abs = peak - bal
        if p >= 0: gains += p
        else: losses += -p
    np_ = bal - start_bal
    pf = gains / losses if losses > 0 else float("inf")
    dd_pct = (dd_abs / peak * 100.0) if peak > 0 else 0.0
    return {"np": np_, "dd": dd_abs, "dd_pct": dd_pct, "pf": pf, "end_bal": bal}


def apply_cap(deals, start_bal, loss_pct):
    """Drop deals after a day's cumulative P&L crosses -loss_pct*day_start_bal.
    Returns (kept_deals, capped_days:set, per_day_info)."""
    bal = start_bal
    kept = []
    cur_day = None
    day_start_bal = bal
    day_cum = 0.0
    locked = False
    capped_days = set()
    per_day = defaultdict(lambda: {"actual": 0.0, "capped": 0.0, "locked": False, "start_bal": 0.0})
    # first pass for actual per-day totals
    bal2 = start_bal
    d2 = None
    for ts, m, p in deals:
        day = ts.date()
        if day != d2:
            d2 = day; per_day[day]["start_bal"] = bal2
        per_day[day]["actual"] += p
        bal2 += p
    # cap pass
    for ts, m, p in deals:
        day = ts.date()
        if day != cur_day:
            cur_day = day
            day_start_bal = bal
            day_cum = 0.0
            locked = False
        if locked:
            continue
        kept.append((ts, m, p))
        bal += p
        day_cum += p
        per_day[day]["capped"] += p
        if loss_pct > 0 and day_cum <= -day_start_bal * loss_pct / 100.0:
            locked = True
            capped_days.add(day)
            per_day[day]["locked"] = True
    return kept, capped_days, per_day


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-05-25", help="ISO date (broker week start)")
    ap.add_argument("--loss-pct", type=float, default=4.0)
    args = ap.parse_args()
    since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)

    deals, bal_now = pull_deals(since)
    if not deals:
        print("no EA deals in window")
        return 1
    total = sum(p for _, _m, p in deals)
    start_bal = bal_now - total  # back out week-start balance

    print("=" * 96)
    print(f"  DAILY LOSS CAP BACKFILL — live acct, {since.date()} -> now  ({len(deals)} closed EA deals)")
    print(f"  Reconstructed week-start balance: ${start_bal:,.2f}  (current ${bal_now:,.2f})")
    print(f"  Cap: GlobalDailyLossPct = {args.loss_pct:.1f}%  (realized-P&L approximation)")
    print("=" * 96)

    base = equity_metrics(deals, start_bal)
    kept, capped_days, per_day = apply_cap(deals, start_bal, args.loss_pct)
    capped = equity_metrics(kept, start_bal)

    print(f"\n  {'Scenario':<22} {'NP':>11} {'DD$':>10} {'DD%':>8} {'PF':>6} {'Deals':>7} {'EndBal':>12}")
    print("  " + "-" * 80)
    print(f"  {'ACTUAL (no cap)':<22} ${base['np']:>+10,.0f} ${base['dd']:>9,.0f} {base['dd_pct']:>7.2f}% "
          f"{base['pf']:>6.2f} {len(deals):>7} ${base['end_bal']:>11,.0f}")
    print(f"  {f'WITH {args.loss_pct:g}% loss cap':<22} ${capped['np']:>+10,.0f} ${capped['dd']:>9,.0f} {capped['dd_pct']:>7.2f}% "
          f"{capped['pf']:>6.2f} {len(kept):>7} ${capped['end_bal']:>11,.0f}")
    print(f"  {'Delta':<22} ${capped['np']-base['np']:>+10,.0f} ${capped['dd']-base['dd']:>+9,.0f} "
          f"{capped['dd_pct']-base['dd_pct']:>+7.2f}% {'':>6} {len(kept)-len(deals):>7}")

    print(f"\n  Per-day breakdown:")
    print(f"  {'Date':<12} {'DayStartBal':>13} {'Actual P&L':>12} {'Capped P&L':>12} {'Locked?':>9} {'4% trip':>10}")
    print("  " + "-" * 74)
    for day in sorted(per_day):
        info = per_day[day]
        trip = -info["start_bal"] * args.loss_pct / 100.0
        lk = "LOCKED" if info["locked"] else ""
        print(f"  {str(day):<12} ${info['start_bal']:>12,.0f} ${info['actual']:>+11,.0f} "
              f"${info['capped']:>+11,.0f} {lk:>9} ${trip:>+9,.0f}")

    print(f"\n  Days capped: {len(capped_days)}  {sorted(str(d) for d in capped_days)}")
    print("=" * 96)
    print("  NOTE: realized approximation — live v8 fires on MTM intraday, so it would lock")
    print("        SOONER and cap each bad day TIGHTER. This is a conservative lower bound.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
