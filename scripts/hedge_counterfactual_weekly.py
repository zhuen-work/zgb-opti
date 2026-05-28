"""Parent-only vs parent+hedge counterfactual for this week + last week (LIVE).

Pulls closed deals from MT5 live account for each week's window, splits by
magic (parents 1111-6666 vs hedges 8111/9222-13666), reports per-stream
parent NP, hedge NP, combined NP, and hedge contribution %.

Run: python scripts/hedge_counterfactual_weekly.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import MetaTrader5 as mt5
from zgb_sim.mt5_accounts import init_account

SYMBOL = "XAUUSD.sc"

# v7 setfile magics
PARENT_MAGICS = {1: 1111, 2: 2222, 3: 3333, 4: 4444, 5: 5555, 6: 6666}
HEDGE_MAGICS  = {1: 8111, 2: 9222, 3: 10333, 4: 11444, 5: 12555, 6: 13666}


def pull_per_stream(start: datetime, end: datetime) -> dict:
    deals = mt5.history_deals_get(start, end)
    if deals is None:
        return {"streams": {}, "n_deals": 0, "parent_total": 0.0, "hedge_total": 0.0}
    streams = {sn: {"parent_n": 0, "parent_pnl": 0.0, "parent_wins": 0,
                    "hedge_n": 0,  "hedge_pnl": 0.0,  "hedge_wins": 0}
               for sn in range(1, 7)}
    n_deals = 0
    for d in deals:
        if d.symbol != SYMBOL: continue
        if d.entry != mt5.DEAL_ENTRY_OUT: continue
        n_deals += 1
        pnl = d.profit + d.commission + d.swap
        for sn, mg in PARENT_MAGICS.items():
            if d.magic == mg:
                streams[sn]["parent_n"] += 1
                streams[sn]["parent_pnl"] += pnl
                if pnl > 0: streams[sn]["parent_wins"] += 1
                break
        for sn, mg in HEDGE_MAGICS.items():
            if d.magic == mg:
                streams[sn]["hedge_n"] += 1
                streams[sn]["hedge_pnl"] += pnl
                if pnl > 0: streams[sn]["hedge_wins"] += 1
                break
    parent_total = sum(s["parent_pnl"] for s in streams.values())
    hedge_total = sum(s["hedge_pnl"] for s in streams.values())
    return {"streams": streams, "n_deals": n_deals,
            "parent_total": parent_total, "hedge_total": hedge_total}


def print_block(label: str, start: datetime, end: datetime, data: dict):
    streams = data["streams"]
    parent_total = data["parent_total"]
    hedge_total = data["hedge_total"]
    combined = parent_total + hedge_total
    print()
    print("=" * 95)
    print(f"  {label}: {start.strftime('%Y-%m-%d')} -> {end.strftime('%Y-%m-%d')} "
          f"({(end-start).days}d, {data['n_deals']} closed deals)")
    print("=" * 95)
    print(f"  {'Stream':<7} {'P trades':>8} {'P W/L':>9} {'Parent NP':>12} | "
          f"{'H trades':>8} {'H W/L':>9} {'Hedge NP':>12} | {'Combined':>12}")
    print("  " + "-" * 91)
    for sn in range(1, 7):
        s = streams[sn]
        wl_p = f"{s['parent_wins']}/{s['parent_n']-s['parent_wins']}" if s['parent_n'] else "-"
        wl_h = f"{s['hedge_wins']}/{s['hedge_n']-s['hedge_wins']}"   if s['hedge_n']  else "-"
        combined_s = s["parent_pnl"] + s["hedge_pnl"]
        print(f"  S{sn:<6} {s['parent_n']:>8} {wl_p:>9} ${s['parent_pnl']:>+10,.0f} | "
              f"{s['hedge_n']:>8} {wl_h:>9} ${s['hedge_pnl']:>+10,.0f} | ${combined_s:>+10,.0f}")
    print("  " + "-" * 91)
    print(f"  {'TOTAL':<7} {sum(s['parent_n'] for s in streams.values()):>8} {'':>9} "
          f"${parent_total:>+10,.0f} | "
          f"{sum(s['hedge_n'] for s in streams.values()):>8} {'':>9} "
          f"${hedge_total:>+10,.0f} | ${combined:>+10,.0f}")
    print()
    delta = combined - parent_total
    if abs(parent_total) > 0.01:
        delta_pct = hedge_total / abs(parent_total) * 100
    else:
        delta_pct = 0.0
    if hedge_total > 0:
        verdict = f"HEDGE HELPED  (+${hedge_total:+,.0f})"
    elif hedge_total < 0:
        verdict = f"HEDGE HURT    (${hedge_total:+,.0f})"
    else:
        verdict = "HEDGE NEUTRAL"
    print(f"  --- COUNTERFACTUAL ---")
    print(f"  Parent-only total: ${parent_total:>+10,.0f}")
    print(f"  Hedge       total: ${hedge_total:>+10,.0f}  ({delta_pct:+.1f}% of |parent|)")
    print(f"  Combined    total: ${combined:>+10,.0f}")
    print(f"  -> {verdict}")


def main():
    init_account("live")
    try:
        # Current week (Mon 00:00 UTC to now)
        now = datetime.now(timezone.utc)
        this_mon = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        this_sun_end = this_mon + timedelta(days=7)
        last_mon = this_mon - timedelta(days=7)

        # Cap "this week" at now (so we don't query future ticks)
        this_end = min(this_sun_end, now)

        data_this = pull_per_stream(this_mon, this_end)
        data_last = pull_per_stream(last_mon, this_mon)
    finally:
        mt5.shutdown()

    print_block("THIS WEEK (live)", this_mon, this_end, data_this)
    print_block("LAST WEEK (live)", last_mon, this_mon, data_last)

    # Two-week summary
    print()
    print("=" * 95)
    print("  TWO-WEEK SUMMARY (last week + this week)")
    print("=" * 95)
    p_2w = data_this["parent_total"] + data_last["parent_total"]
    h_2w = data_this["hedge_total"] + data_last["hedge_total"]
    c_2w = p_2w + h_2w
    print(f"  Parent only       : ${p_2w:>+12,.0f}")
    print(f"  Hedge contribution: ${h_2w:>+12,.0f}  ({h_2w/max(abs(p_2w),1)*100:+.1f}% of |parent|)")
    print(f"  Combined          : ${c_2w:>+12,.0f}")
    if h_2w > 0:
        print(f"  -> Hedge helped overall (+${h_2w:+,.0f})")
    else:
        print(f"  -> Hedge hurt overall  ({h_2w:+,.0f})")
    print("=" * 95)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
