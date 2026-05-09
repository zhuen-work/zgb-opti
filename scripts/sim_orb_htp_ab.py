"""A/B test: production top-3 (HTP allowed) vs HTP-free re-ranked top-3.

Variant A: Top-3 selected from FULL grid (HTP in {0, 0.25, 0.5, 0.75}).
Variant B: Top-3 RE-RANKED from HTP=0-only subset of the same WFO data.

Both variants use the same ranker (rank_with_p0 from wfo_helpers) on the same
IS/OOS windows. This is a fair head-to-head: "what would the WFO have picked
if HTP wasn't an option" vs "what the WFO actually picked."

Window: Feb 14 -> May 1 (76d), $10k, 23/35/55pt spreads, total risk 3/4.5/6%.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.wfo_helpers import WINDOWS_MAY2 as WINDOWS, rank_with_p0

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
WFO_DIR = ROOT / "output" / "wfo_orb_may2"
START = datetime(2026, 2, 14, tzinfo=timezone.utc)
END = datetime(2026, 5, 1, tzinfo=timezone.utc)
SPREADS = (23, 35, 55)


def row_to_cfg(row, comment: str, risk_pct: float) -> ORBConfig:
    return ORBConfig(
        risk_pct=risk_pct,
        range_minutes=int(row["range_minutes"]),
        buffer_pts=0,
        min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=int(row["fixed_sl_pts"]),
        rr_ratio=float(row["rr_ratio"]),
        half_tp_ratio=round(float(row["half_tp_ratio"]), 2),
        pending_expire_minutes=240,
        daily_target_pct=float(row["daily_target_pct"]),
        daily_loss_pct=float(row["daily_loss_pct"]),
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True,  ny_start_hour=13,
        comment=comment,
    )


def aggregate(deals):
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gp = gl = 0.0
    for _, _s, p in sorted(deals, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p > 0: gp += p
        elif p < 0: gl += p
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    pf = (gp / abs(gl)) if gl < 0 else float("inf")
    return dict(np=np_, dd_pct=dd_pct, dd_abs=dd_abs, pf=pf,
                ndd=(np_ / dd_abs if dd_abs > 0 else 0))


def rerank_with_filter(filter_fn, oos_per, is_per, top_n=3):
    """Re-rank WFO results using rank_with_p0 on a filtered candidate set."""
    cands_df = oos_per["W1"]
    grid_df = is_per["W1"]
    if filter_fn is not None:
        cands_df = cands_df[cands_df.apply(filter_fn, axis=1)].reset_index(drop=True)
        grid_df  = grid_df[grid_df.apply(filter_fn, axis=1)].reset_index(drop=True)
        # Filter the per-window dicts too
        oos_filt = {k: v[v.apply(filter_fn, axis=1)].reset_index(drop=True) for k, v in oos_per.items()}
        is_filt  = {k: v[v.apply(filter_fn, axis=1)].reset_index(drop=True) for k, v in is_per.items()}
    else:
        oos_filt, is_filt = oos_per, is_per
    cands = [row_to_cfg(r, "ORB", 3.0) for _, r in cands_df.iterrows()]
    grid  = [row_to_cfg(r, "ORB", 3.0) for _, r in grid_df.iterrows()]
    ranked = rank_with_p0(cands, oos_filt, WINDOWS, decay_threshold=-0.25,
                          grid_configs=grid, is_per_window=is_filt)
    rows = []
    for i in range(top_n):
        c = ranked[i]["cfg"]
        rows.append({"range_minutes": c.range_minutes, "fixed_sl_pts": c.fixed_sl_pts,
                     "rr_ratio": c.rr_ratio, "half_tp_ratio": c.half_tp_ratio,
                     "daily_target_pct": c.daily_target_pct, "daily_loss_pct": c.daily_loss_pct})
    return rows


def run_portfolio(rows, ticks, m1, m5, meta, total_risk: float):
    per_stream = total_risk / 3
    cfgs = [(tag, row_to_cfg(row, tag, per_stream)) for tag, row in zip(("S1","S2","S3"), rows)]
    deals = []; per_s = {}
    for label, cfg in cfgs:
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        np_s = 0.0; tr_s = 0
        for d in r.deals:
            if d.kind != "entry":
                deals.append((d.ts, label, d.pnl))
                np_s += d.pnl; tr_s += 1
        per_s[label] = (np_s, tr_s)
    agg = aggregate(deals)
    agg["per_stream"] = per_s
    agg["trades"] = len(deals)
    return agg


def main() -> int:
    is_per = {label: pd.read_parquet(WFO_DIR / f"is_{label}.parquet") for label, _, _, _, _ in WINDOWS}
    oos_per = {label: pd.read_parquet(WFO_DIR / f"oos_{label}.parquet") for label, _, _, _, _ in WINDOWS}

    rows_A = rerank_with_filter(None, oos_per, is_per)  # production: full grid
    rows_B = rerank_with_filter(lambda r: r["half_tp_ratio"] == 0.0, oos_per, is_per)

    days = (END - START).days
    print("=" * 110)
    print(f"  HTP A/B (proper re-rank)  |  {START.date()} -> {END.date()} ({days}d, $10k)")
    print(f"  Variant A: top-3 from FULL grid (HTP in 0/0.25/0.5/0.75)")
    print(f"    S1: Range={rows_A[0]['range_minutes']} SL={rows_A[0]['fixed_sl_pts']} RR={rows_A[0]['rr_ratio']} HTP={rows_A[0]['half_tp_ratio']}")
    print(f"    S2: Range={rows_A[1]['range_minutes']} SL={rows_A[1]['fixed_sl_pts']} RR={rows_A[1]['rr_ratio']} HTP={rows_A[1]['half_tp_ratio']}")
    print(f"    S3: Range={rows_A[2]['range_minutes']} SL={rows_A[2]['fixed_sl_pts']} RR={rows_A[2]['rr_ratio']} HTP={rows_A[2]['half_tp_ratio']}")
    print(f"  Variant B: top-3 RE-RANKED from HTP=0 subset (126 candidates per window)")
    print(f"    S1: Range={rows_B[0]['range_minutes']} SL={rows_B[0]['fixed_sl_pts']} RR={rows_B[0]['rr_ratio']} HTP={rows_B[0]['half_tp_ratio']}")
    print(f"    S2: Range={rows_B[1]['range_minutes']} SL={rows_B[1]['fixed_sl_pts']} RR={rows_B[1]['rr_ratio']} HTP={rows_B[1]['half_tp_ratio']}")
    print(f"    S3: Range={rows_B[2]['range_minutes']} SL={rows_B[2]['fixed_sl_pts']} RR={rows_B[2]['rr_ratio']} HTP={rows_B[2]['half_tp_ratio']}")
    print("=" * 110)

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        m1 = load_bars(SYMBOL, "M1", START, END)
        m5 = load_bars(SYMBOL, "M5", START, END)

        for sp in SPREADS:
            ticks = load_ticks(SYMBOL, START, END, spread_pts=sp)
            print(f"\n  --- Spread {sp}pt ---")
            print(f"  {'Risk':<5} {'Variant':<10} {'NP':>10} {'DD%':>6} {'NP/DD$':>7} {'PF':>5} {'Trades':>6}  "
                  f"{'S1':>16} {'S2':>16} {'S3':>16}")
            for risk in (3.0, 4.5, 6.0):
                a = run_portfolio(rows_A, ticks, m1, m5, meta, risk)
                b = run_portfolio(rows_B, ticks, m1, m5, meta, risk)
                for tag, agg in (("A:full", a), ("B:noHTP", b)):
                    ps = agg["per_stream"]
                    s_str = lambda s: f"${ps[s][0]:>+7,.0f}({ps[s][1]:>3})"
                    print(f"  {risk:>4}% {tag:<10} ${agg['np']:>+8,.0f} {agg['dd_pct']:>5.1f}% "
                          f"{agg['ndd']:>7.2f} {agg['pf']:>5.2f} {agg['trades']:>6}  "
                          f"{s_str('S1'):>16} {s_str('S2'):>16} {s_str('S3'):>16}")
                d_np = b["np"] - a["np"]; d_dd = b["dd_pct"] - a["dd_pct"]
                d_ndd = b["ndd"] - a["ndd"]; d_pf = b["pf"] - a["pf"]
                print(f"        d(B-A)     ${d_np:>+8,.0f} {d_dd:>+5.1f}p {d_ndd:>+7.2f} {d_pf:>+5.2f}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
