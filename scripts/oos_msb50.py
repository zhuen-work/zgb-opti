"""OOS validation for MSB50_v1. Tests the top-5 IS winners on a different
window to check for curve-fit.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from zgb_sim.tick_loader import load_ticks, load_bars, symbol_meta
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.msb50 import MSB50Config, simulate_msb50, RANGE_IMPULSE, MSB_DONCHIAN


SYMBOL = "XAUUSD"


# Top 5 from Apr 1-25 sweep (impulse + donchian dominates).
TOP_CONFIGS = [
    # (label,                                              pn, tol, slb, rr,  nd)
    ("rank1: pn=2 tol=5  slb=200 rr=3.0 nd=10",            2,  5,   200, 3.0, 10),
    ("rank2: pn=2 tol=20 slb=200 rr=3.0 nd=10",            2,  20,  200, 3.0, 10),
    ("rank3: pn=2 tol=50 slb=200 rr=3.0 nd=10",            2,  50,  200, 3.0, 10),
    ("rank4: pn=3 tol=20 slb=50  rr=2.0 nd=10",            3,  20,  50,  2.0, 10),
    ("rank5: pn=3 tol=50 slb=50  rr=2.0 nd=10",            3,  50,  50,  2.0, 10),
]


def to_utc(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def run_window(label: str, start: datetime, end: datetime, spread: int, balance: float):
    m = symbol_meta(SYMBOL)
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])
    print(f"\n{label}: {start.date()} -> {end.date()} (spread={spread}pt)")
    print("-" * 110)
    ticks = load_ticks(SYMBOL, start, end, spread_pts=spread)
    m5 = load_bars(SYMBOL, "M5", start, end)
    print(f"  ticks={len(ticks):,}  m5={len(m5):,}")

    print(f"{'config':<48} | {'NP':>9} {'ROI':>7} {'PF':>5} {'DD%':>5} "
          f"{'Trd':>4} {'TP':>3} {'SL':>3} {'NP/DD':>6}")
    print("-" * 110)
    out_rows = []
    for lbl, pn, tol, slb, rr, nd in TOP_CONFIGS:
        cfg = MSB50Config(
            risk_pct=1.0, pivot_n=pn,
            range_mode=RANGE_IMPULSE, msb_mode=MSB_DONCHIAN,
            tol_pts=tol, n_donch=nd, sl_buffer_pts=slb, rr_ratio=rr,
            max_spread_pts=70, arm_timeout_bars=24,
        )
        res = simulate_msb50(ticks, m5, cfg, meta, initial_balance=balance)
        roi = res.net_profit / balance * 100.0
        np_dd = res.net_profit / res.max_drawdown if res.max_drawdown > 0 else 0.0
        print(f"{lbl:<48} | {res.net_profit:>+9,.0f} {roi:>+6.1f}% {res.profit_factor:>5.2f} "
              f"{res.max_drawdown_pct:>4.1f}% {res.trades:>4} {res.tp_count:>3} "
              f"{res.sl_count:>3} {np_dd:>6.2f}", flush=True)
        out_rows.append({"label": lbl, "np": res.net_profit, "trades": res.trades})
    return out_rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spread", type=int, default=60)
    ap.add_argument("--balance", type=float, default=10_000.0)
    args = ap.parse_args()

    print(f"MSB50_v1 OOS: top 5 IS winners on 3 windows (IS + 2 OOS)")
    print("=" * 110)

    windows = [
        ("IS    (Apr 1-25)", to_utc("2026-04-01"), to_utc("2026-04-25")),
        ("OOS-A (May 1-22)", to_utc("2026-05-01"), to_utc("2026-05-22")),
        ("OOS-B (Mar 1-31)", to_utc("2026-03-01"), to_utc("2026-03-31")),
    ]
    all_results = {}
    for lbl, s, e in windows:
        rows = run_window(lbl, s, e, args.spread, args.balance)
        all_results[lbl] = rows

    # Cross-window summary
    print(f"\n\nCROSS-WINDOW NP SUMMARY:")
    print(f"{'config':<48} | " + " ".join(f"{lbl:>16}" for lbl, _, _ in windows) + " | profitable")
    print("-" * 130)
    for i, (lbl, _, _, _, _, _) in enumerate(TOP_CONFIGS):
        row_str = f"{lbl:<48} | "
        n_pos = 0
        for w_lbl, _, _ in windows:
            np_val = all_results[w_lbl][i]["np"]
            row_str += f"{np_val:>+16,.0f} "
            if np_val > 0: n_pos += 1
        row_str += f"| {n_pos}/{len(windows)}"
        print(row_str)


if __name__ == "__main__":
    main()
