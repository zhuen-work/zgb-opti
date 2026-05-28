"""Hedge ON vs OFF comparison for May 3 -> May 9 (last OOS week).

Uses MAY9 parent winners + per-stream hedge winners.
Production sizing: 1% parent + 1% hedge per stream. 3 spreads.
"""
from __future__ import annotations

import json
import sys
from datetime import date, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.wfo_helpers import to_utc

from sim_wfo_hedge import (STREAM_CFGS, HedgeCfg, run_baseline_window,
                            simulate_hedges, ts_arr_from_ticks, slice_window,
                            aggregate, DEPOSIT, SYMBOL, PARENT_RISK_PROD,
                            HEDGE_RISK_PCT)

SPREADS = [30, 55, 70]  # per feedback_default_test_conditions.md (all live = 30pt 2026-05-16)
HEDGE_DIR = ROOT / "output" / "wfo_hedge_per_stream_may9"
START_DATE = date(2026, 5, 3)
END_DATE   = date(2026, 5, 9)


def load_winners() -> dict[str, HedgeCfg]:
    out = {}
    for s in ("S1", "S2", "S3"):
        d = json.loads((HEDGE_DIR / f"{s}.json").read_text())
        out[s] = HedgeCfg(buf=int(d["buffer_pts"]), h_sl=int(d["fixed_sl_pts"]),
                          h_rr=float(d["rr_ratio"]), exp=int(d["expire_minutes"]))
    return out


def portfolio_sim(full_ticks, full_m1, full_m5, meta, winners, with_hedge: bool):
    start = to_utc(START_DATE); end = to_utc(END_DATE)
    ticks = slice_window(full_ticks, "ts", start, end)
    m1 = slice_window(full_m1, "ts", start, end)
    m5 = slice_window(full_m5, "ts", start, end)
    t_arr = ts_arr_from_ticks(ticks) if with_hedge else None

    all_deals = []; per_stream = {}; per_hedge = {}
    for s in STREAM_CFGS:
        deals, sl_ev = run_baseline_window(s, ticks, m1, m5, meta, PARENT_RISK_PROD)
        per_stream[s] = (sum(p for _, p in deals), len(deals))
        if with_hedge:
            h_deals = simulate_hedges(sl_ev, t_arr, winners[s])
            per_hedge[s] = (sum(p for _, p in h_deals), len(h_deals),
                            sum(1 for _, p in h_deals if p > 0))
            all_deals.extend(deals + h_deals)
        else:
            all_deals.extend(deals)
    np_, dd, pf = aggregate(all_deals)
    ndd = (np_ / (dd / 100 * (DEPOSIT + np_))) if dd > 0 else 0.0
    return np_, dd, ndd, pf, len(all_deals), per_stream, per_hedge


def main() -> int:
    print("=" * 100)
    print(f"  HEDGE ON vs OFF  |  {START_DATE} -> {END_DATE} ({(END_DATE-START_DATE).days}d)")
    print(f"  $10k, per stream: parent {PARENT_RISK_PROD}%, hedge {HEDGE_RISK_PCT}%")
    print("=" * 100)
    winners = load_winners()
    print(f"\n  Hedge winners (from {HEDGE_DIR.name}):")
    for s, w in winners.items():
        print(f"    {s}: buf={w.buf} h_sl={w.h_sl} h_rr={w.h_rr} exp={w.exp}min")

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        start = to_utc(START_DATE); end = to_utc(END_DATE)
        full_m1 = load_bars(SYMBOL, "M1", start, end)
        full_m5 = load_bars(SYMBOL, "M5", start, end)

        rows = []
        for sp in SPREADS:
            print(f"\n  -- spread {sp}pt --")
            full_ticks = load_ticks(SYMBOL, start, end, spread_pts=sp)
            no_h = portfolio_sim(full_ticks, full_m1, full_m5, meta, winners, with_hedge=False)
            wi_h = portfolio_sim(full_ticks, full_m1, full_m5, meta, winners, with_hedge=True)
            rows.append((sp, no_h, wi_h))
            print(f"    no-hedge: NP=${no_h[0]:+,.0f}  DD={no_h[1]:.2f}%  NP/DD$={no_h[2]:.2f}  "
                  f"PF={no_h[3]:.2f}  T={no_h[4]}")
            print(f"    +hedge:   NP=${wi_h[0]:+,.0f}  DD={wi_h[1]:.2f}%  NP/DD$={wi_h[2]:.2f}  "
                  f"PF={wi_h[3]:.2f}  T={wi_h[4]}")
            print(f"    per-stream (with hedge):")
            for s in STREAM_CFGS:
                pp, pn = wi_h[5][s]; hp, hn, hw = wi_h[6][s]
                wr = (hw/hn*100) if hn else 0
                print(f"      {s}: parent NP=${pp:+,.0f} ({pn} tr) | "
                      f"hedge NP=${hp:+,.0f} ({hn} tr, {wr:.0f}% W)")

        print("\n" + "=" * 100)
        print(f"  SUMMARY -- {START_DATE} -> {END_DATE}")
        print("=" * 100)
        print(f"  {'Spread':>6} | {'Variant':<8} | {'NP':>9} | {'DD%':>6} | {'NP/DD$':>7} | "
              f"{'PF':>5} | {'Trades':>6}")
        print(f"  {'-'*6}-+-{'-'*8}-+-{'-'*9}-+-{'-'*6}-+-{'-'*7}-+-{'-'*5}-+-{'-'*6}")
        for sp, no_h, wi_h in rows:
            print(f"  {sp:>4}pt | {'no-hedge':<8} | ${no_h[0]:>+7,.0f} | "
                  f"{no_h[1]:>5.2f}% | {no_h[2]:>7.2f} | {no_h[3]:>5.2f} | {no_h[4]:>6}")
            print(f"  {sp:>4}pt | {'+hedge':<8} | ${wi_h[0]:>+7,.0f} | "
                  f"{wi_h[1]:>5.2f}% | {wi_h[2]:>7.2f} | {wi_h[3]:>5.2f} | {wi_h[4]:>6}")
            d_np = wi_h[0] - no_h[0]; d_dd = wi_h[1] - no_h[1]
            d_ndd = wi_h[2] - no_h[2]
            ndd_pct = (d_ndd / no_h[2] * 100) if no_h[2] != 0 else 0
            print(f"  {sp:>4}pt | {'delta':<8} | ${d_np:>+7,.0f} | "
                  f"{d_dd:>+5.1f}p | {d_ndd:>+7.2f} ({ndd_pct:+.0f}%)")
            print(f"  {'-'*6}-+-{'-'*8}-+-{'-'*9}-+-{'-'*6}-+-{'-'*7}-+-{'-'*5}-+-{'-'*6}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
