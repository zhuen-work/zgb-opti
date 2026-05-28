"""Compute slippage-only friction for specified date(s).

For each parent SL exit:
  expected_$_loss = lots × SL_distance_pts × $1/pt   (XAUUSD: 1 lot × 1 pt = $1)
  actual_$_loss   = |deal.profit|
  slippage_pct    = (actual / expected - 1) × 100

This isolates BROKER execution quality from HTP fire-rate variance. The
'apparent friction' (live vs sim_scaled) mixes both; slippage-only is what
triggers calibration retune per project_live_vs_sim_calibration_log.md
(5-day rolling mean > 10%).

Usage:
  python scripts/compute_slippage_friction.py 2026-05-14 2026-05-15 2026-05-18
  python scripts/compute_slippage_friction.py --publish 2026-05-18   # also push to dashboard
"""
from __future__ import annotations
import sys
from datetime import datetime, timezone, timedelta, date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import MetaTrader5 as mt5
from zgb_sim.mt5_accounts import init_account
from zgb_sim.tick_loader import kill_mt5_terminal

PARENT_MAGICS = {1111: "S1", 2222: "S2", 3333: "S3",
                  4444: "S4", 5555: "S5", 6666: "S6"}
POINT = 0.01
PT_VALUE = 1.0  # XAUUSD on Vantage: 1 lot × 1 pt = $1


def compute_for_date(target_date: date):
    # Pull broker history wide enough to cover any timezone wobble.
    frm = datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc) - timedelta(hours=6)
    to  = datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=30)

    deals = mt5.history_deals_get(frm, to)
    if deals is None:
        print(f"  history_deals_get None: {mt5.last_error()}")
        return None
    orders = mt5.history_orders_get(frm, to)
    if orders is None:
        print(f"  history_orders_get None: {mt5.last_error()}")
        return None
    order_by_ticket = {int(o.ticket): o for o in orders}

    # Filter to parent SL exit deals on target_date (broker calendar date).
    sl_deals = []
    for d in deals:
        if int(d.magic) not in PARENT_MAGICS: continue
        if d.entry != mt5.DEAL_ENTRY_OUT: continue
        if not str(d.comment).startswith("[sl"): continue
        ts = datetime.fromtimestamp(d.time, tz=timezone.utc)
        if ts.date() != target_date: continue
        sl_deals.append((d, ts))

    if not sl_deals:
        print(f"  no parent SL deals found for {target_date}")
        return None

    total_expected = 0.0
    total_actual = 0.0
    per_stream: dict[int, dict] = {m: {"n": 0, "exp": 0.0, "act": 0.0}
                                    for m in PARENT_MAGICS}
    rows = []

    for d, ts in sl_deals:
        in_d = None
        for d2 in deals:
            if d2.position_id == d.position_id and d2.entry == mt5.DEAL_ENTRY_IN:
                in_d = d2; break
        if in_d is None: continue
        orig_order = order_by_ticket.get(int(in_d.order))
        if orig_order is None: continue

        orig_entry = float(orig_order.price_open)
        orig_sl = float(orig_order.sl)
        sl_dist_pts = abs(orig_entry - orig_sl) / POINT
        lots = float(d.volume)
        expected = lots * sl_dist_pts * PT_VALUE
        actual = abs(float(d.profit))
        slip_pct = (actual / expected - 1.0) * 100 if expected > 0 else 0.0

        total_expected += expected
        total_actual += actual
        ps = per_stream[int(d.magic)]
        ps["n"] += 1
        ps["exp"] += expected
        ps["act"] += actual
        rows.append({
            "ts": ts, "stream": PARENT_MAGICS[int(d.magic)], "magic": int(d.magic),
            "lots": lots, "sl_dist_pts": sl_dist_pts,
            "expected": expected, "actual": actual, "slip_pct": slip_pct,
        })

    portfolio_slip = (total_actual / total_expected - 1.0) * 100 if total_expected > 0 else 0.0

    print(f"\n=== {target_date} — {len(rows)} parent SL exits ===")
    print(f"  Total expected (all-SL clean): ${total_expected:>10,.2f}")
    print(f"  Total actual   (live deals):   ${total_actual:>10,.2f}")
    print(f"  Portfolio slippage friction:   {portfolio_slip:+.2f}%")
    print(f"\n  Per-stream:")
    for m in sorted(per_stream):
        ps = per_stream[m]
        if ps["n"] == 0: continue
        slip = (ps["act"] / ps["exp"] - 1.0) * 100 if ps["exp"] > 0 else 0.0
        print(f"    {PARENT_MAGICS[m]} ({m}): n={ps['n']}  "
              f"exp=${ps['exp']:>8,.0f}  act=${ps['act']:>8,.0f}  slip={slip:+.2f}%")
    return {
        "date": target_date.isoformat(),
        "trades": len(rows),
        "expected": total_expected,
        "actual": total_actual,
        "slippage_pct": portfolio_slip,
    }


def main():
    args = sys.argv[1:]
    do_publish = False
    if "--publish" in args:
        do_publish = True
        args = [a for a in args if a != "--publish"]
    dates = args if args else [datetime.now(timezone.utc).date().isoformat()]

    init_account("live")
    results = []
    try:
        for ds in dates:
            td = date.fromisoformat(ds)
            r = compute_for_date(td)
            if r: results.append(r)

        if do_publish and results:
            from zgb_sim.cf_publish import publish_slippage, publish_alert
            print(f"\n  Publishing slippage to dashboard for {len(results)} date(s)...")
            for r in results:
                ok = publish_slippage(
                    date=r["date"],
                    slippage_pct=r["slippage_pct"],
                    sl_trades=r["trades"],
                    expected_loss_usd=r["expected"],
                    actual_loss_usd=r["actual"],
                    notes=f"compute_slippage_friction {r['trades']} SLs",
                )
                print(f"    {r['date']}: {'OK' if ok else 'FAIL'}  "
                      f"slip={r['slippage_pct']:+.2f}%  n={r['trades']}")

            # Calibration retune trigger: 5-day rolling mean of slippage_pct > 10%
            # → fire warn alert (project_live_vs_sim_calibration_log.md rule).
            # Uses the LAST 5 results passed to this invocation. For correct
            # rolling-window evaluation, pass at least 5 recent loser dates.
            if len(results) >= 5:
                last5 = results[-5:]
                mean5 = sum(r["slippage_pct"] for r in last5) / 5.0
                print(f"\n  5-day rolling slippage mean: {mean5:+.2f}% "
                      f"(threshold +10.00%)")
                if mean5 > 10.0:
                    dates_str = ", ".join(r["date"] for r in last5)
                    publish_alert("warn", "slippage_retune",
                        f"5-day rolling slippage mean {mean5:+.2f}% > +10% trigger "
                        f"({dates_str}). Retune calibration: bump NP haircut from "
                        f"5-8% to 10-12%, PF haircut from 0.25 to 0.30.",
                        context={"mean_5d_pct": mean5, "trigger_pct": 10.0,
                                  "dates": [r["date"] for r in last5],
                                  "per_day_pct": [r["slippage_pct"] for r in last5]})
                    print(f"  >> ALERT FIRED: slippage_retune (warn)")
                else:
                    print(f"  -> within band, no alert")

        if len(results) > 1:
            print(f"\n{'='*60}")
            print("ROLLING SUMMARY (slippage-only friction)")
            print(f"{'='*60}")
            print(f"  {'Date':<12} {'Trades':>7} {'Expected $':>12} {'Actual $':>12} {'Slip %':>9}")
            for r in results:
                print(f"  {r['date']:<12} {r['trades']:>7} ${r['expected']:>11,.0f} "
                      f"${r['actual']:>11,.0f} {r['slippage_pct']:>+8.2f}%")
            mean = sum(r["slippage_pct"] for r in results) / len(results)
            print(f"\n  {len(results)}-day mean slippage: {mean:+.2f}%")
            print(f"  Retune trigger threshold: +10.00%")
            print(f"  Verdict: {'RETUNE INDICATED' if mean > 10.0 else 'within calibration band'}")
    finally:
        mt5.shutdown()
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
