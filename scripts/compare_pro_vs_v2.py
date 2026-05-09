"""Pro (3 streams MAY9) vs Pro_v2 (6 streams MAY9+MAY2) portfolio comparison.

Same total risk (3%) across both:
  - pro: 3 streams (MAY9 R1/R2/R3) at 1.0% each
  - v2:  6 streams (MAY9 R1/R2/R3 + MAY2 R1/R2/R3) at 0.5% each

Window: Feb 21 -> May 9 (77d, matches WFO span). 3 spreads (23/55/70pt).
"""
from __future__ import annotations

import sys
from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.wfo_helpers import WINDOWS_MAY9 as WINDOWS, to_utc

from sim_wfo_hedge import slice_window, aggregate, DEPOSIT, SYMBOL

SPREADS = [23, 55, 70]
TOTAL_RISKS = [6.0]   # total portfolio risk %; per-stream = total/n_streams
START_DATE = date(2026, 2, 21)
END_DATE   = date(2026, 5, 9)

# MAY9 top-3 (this week's WFO)
MAY9_CFGS = [
    dict(range_minutes=90, fixed_sl_pts=650, rr_ratio=4.0, half_tp_ratio=0.25),  # R1
    dict(range_minutes=90, fixed_sl_pts=350, rr_ratio=4.0, half_tp_ratio=0.5),   # R2
    dict(range_minutes=90, fixed_sl_pts=400, rr_ratio=4.0, half_tp_ratio=0.5),   # R3
]
# MAY2 top-3 (last week's WFO)
MAY2_CFGS = [
    dict(range_minutes=90, fixed_sl_pts=500, rr_ratio=4.0, half_tp_ratio=0.25),  # R1
    dict(range_minutes=90, fixed_sl_pts=400, rr_ratio=4.0, half_tp_ratio=0.0),   # R2
    dict(range_minutes=90, fixed_sl_pts=350, rr_ratio=4.0, half_tp_ratio=0.5),   # R3
]
# MAY2 top-6 (last week's WFO, full top-6)
MAY2_TOP6_CFGS = [
    dict(range_minutes=90, fixed_sl_pts=500, rr_ratio=4.0, half_tp_ratio=0.25),  # R1
    dict(range_minutes=90, fixed_sl_pts=400, rr_ratio=4.0, half_tp_ratio=0.0),   # R2
    dict(range_minutes=90, fixed_sl_pts=350, rr_ratio=4.0, half_tp_ratio=0.5),   # R3
    dict(range_minutes=90, fixed_sl_pts=400, rr_ratio=4.0, half_tp_ratio=0.5),   # R4
    dict(range_minutes=90, fixed_sl_pts=350, rr_ratio=3.0, half_tp_ratio=0.75),  # R5
    dict(range_minutes=90, fixed_sl_pts=350, rr_ratio=4.0, half_tp_ratio=0.25),  # R6
]


def make_cfg(d: dict, risk_pct: float, comment: str) -> ORBConfig:
    return ORBConfig(
        risk_pct=risk_pct,
        range_minutes=d["range_minutes"],
        buffer_pts=0, min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=d["fixed_sl_pts"],
        rr_ratio=d["rr_ratio"], half_tp_ratio=d["half_tp_ratio"],
        pending_expire_minutes=240,
        daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True,  ny_start_hour=13,
        comment=comment,
    )


def run_stream(cfg_dict: dict, risk: float, name: str, ticks, m1, m5, meta):
    cfg = make_cfg(cfg_dict, risk, name)
    res = orb_simulate(ticks, m5, m1, cfg, meta, DEPOSIT)
    deals = [(d.ts, d.pnl) for d in res.deals if d.kind != "entry"]
    return deals


def portfolio(ticks, m1, m5, meta, cfgs: list[dict], risk_per_stream: float, prefix: str):
    all_deals = []
    per = []
    for i, c in enumerate(cfgs, 1):
        name = f"{prefix}_{i}"
        deals = run_stream(c, risk_per_stream, name, ticks, m1, m5, meta)
        per.append((name, sum(p for _, p in deals), len(deals)))
        all_deals.extend(deals)
    np_, dd, pf = aggregate(all_deals)
    ndd = (np_ / (dd / 100 * (DEPOSIT + np_))) if dd > 0 else 0.0
    return np_, dd, ndd, pf, len(all_deals), per


def main() -> int:
    print("=" * 100)
    print(f"  PRO (3-stream MAY9) vs V2 (6-stream MAY9+MAY2) vs V3 (6-stream MAY2 top-6)")
    print(f"  3 spreads x {len(TOTAL_RISKS)} risk level(s)")
    print(f"  {START_DATE} -> {END_DATE} ({(END_DATE-START_DATE).days}d, $10k)")
    print("=" * 100)
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        start = to_utc(START_DATE); end = to_utc(END_DATE)
        full_m1 = load_bars(SYMBOL, "M1", start, end)
        full_m5 = load_bars(SYMBOL, "M5", start, end)

        all_rows = []  # list of (total_risk, sp, pro, v2, v3)
        for total in TOTAL_RISKS:
            pro_per = total / 3.0
            v2_per  = total / 6.0
            v3_per  = total / 6.0
            print(f"\n{'='*100}")
            print(f"  TOTAL RISK = {total}%   pro: {pro_per}% per stream  |  v2/v3: {v2_per}% per stream")
            print(f"{'='*100}")
            for sp in SPREADS:
                print(f"\n  -- spread {sp}pt --")
                full_ticks = load_ticks(SYMBOL, start, end, spread_pts=sp)
                ticks = slice_window(full_ticks, "ts", start, end)
                m1 = slice_window(full_m1, "ts", start, end)
                m5 = slice_window(full_m5, "ts", start, end)

                pro = portfolio(ticks, m1, m5, meta, MAY9_CFGS, pro_per, "MAY9")
                v2  = portfolio(ticks, m1, m5, meta, MAY9_CFGS + MAY2_CFGS, v2_per, "v2")
                v3  = portfolio(ticks, m1, m5, meta, MAY2_TOP6_CFGS, v3_per, "v3")
                all_rows.append((total, sp, pro, v2, v3))
                print(f"    pro (3 MAY9):    NP=${pro[0]:+,.0f}  DD={pro[1]:.2f}%  NP/DD$={pro[2]:.2f}  "
                      f"PF={pro[3]:.2f}  T={pro[4]}")
                print(f"    v2  (3+3 mix):   NP=${v2[0]:+,.0f}  DD={v2[1]:.2f}%  NP/DD$={v2[2]:.2f}  "
                      f"PF={v2[3]:.2f}  T={v2[4]}")
                print(f"    v3  (6 MAY2):    NP=${v3[0]:+,.0f}  DD={v3[1]:.2f}%  NP/DD$={v3[2]:.2f}  "
                      f"PF={v3[3]:.2f}  T={v3[4]}")

        print("\n" + "=" * 100)
        print("  SUMMARY")
        print("=" * 100)
        print(f"  {'Risk':>4} | {'Spread':>6} | {'EA':<8} | {'NP':>10} | {'DD%':>6} | "
              f"{'NP/DD$':>7} | {'PF':>5} | {'Trades':>6}")
        print(f"  {'-'*4}-+-{'-'*6}-+-{'-'*8}-+-{'-'*10}-+-{'-'*6}-+-{'-'*7}-+-{'-'*5}-+-{'-'*6}")
        prev_risk = None
        for total, sp, pro, v2, v3 in all_rows:
            if prev_risk is not None and prev_risk != total:
                print(f"  {'='*4}-+-{'='*6}-+-{'='*8}-+-{'='*10}-+-{'='*6}-+-{'='*7}-+-{'='*5}-+-{'='*6}")
            prev_risk = total
            for label, x in [("pro", pro), ("v2", v2), ("v3", v3)]:
                print(f"  {total:>3}% | {sp:>4}pt | {label:<8} | ${x[0]:>+8,.0f} | "
                      f"{x[1]:>5.2f}% | {x[2]:>7.2f} | {x[3]:>5.2f} | {x[4]:>6}")
            print(f"  {'-'*4}-+-{'-'*6}-+-{'-'*8}-+-{'-'*10}-+-{'-'*6}-+-{'-'*7}-+-{'-'*5}-+-{'-'*6}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
