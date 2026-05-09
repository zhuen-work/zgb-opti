"""Compare rank-1 vs rank-1+2 vs rank-1+2+3 portfolio (May 2 both-sessions WFO).

Extracts the top 3 candidates from the cached IS/OOS parquets in
output/wfo_orb_may2/, then runs:
  - Baseline:  rank 1 alone (current setfile)
  - Test 1+2:  rank 1 + rank 2 dual-stream (deal-merge)
  - Test 1+2+3: rank 1 + rank 2 + rank 3 tri-stream (deal-merge)

All streams have both LDN+NY sessions enabled. Reports NP/DD$ at 23/35/55 pt.
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
RISK = 3.0
WFO_DIR = ROOT / "output" / "wfo_orb_may2"


def row_to_cfg(row, comment: str, risk_pct: float = RISK) -> ORBConfig:
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


def aggregate(deals: list[tuple]) -> dict:
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    for _, _s, p in sorted(deals, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    ndd = np_ / dd_abs if dd_abs > 0 else 0
    return dict(np=np_, dd_pct=dd_pct, dd_abs=dd_abs, ndd=ndd)


def main() -> int:
    # Load cached IS + OOS parquets
    is_per = {}
    oos_per = {}
    for label, _, _, _, _ in WINDOWS:
        is_per[label]  = pd.read_parquet(WFO_DIR / f"is_{label}.parquet")
        oos_per[label] = pd.read_parquet(WFO_DIR / f"oos_{label}.parquet")

    # The OOS parquets contain Phase B candidates (~15 robust configs).
    cands_df = oos_per["W1"].copy()
    candidates = [row_to_cfg(r, "ORB") for _, r in cands_df.iterrows()]
    # The IS parquets contain the full sweep grid (504 configs) — needed for plateau lookup.
    full_grid_df = is_per["W1"].copy()
    full_grid = [row_to_cfg(r, "ORB") for _, r in full_grid_df.iterrows()]

    # Re-rank with P0 + plateau using full cached data
    ranked = rank_with_p0(candidates, oos_per, WINDOWS, decay_threshold=-0.25,
                          grid_configs=full_grid, is_per_window=is_per)

    print("=" * 100)
    print("  Re-ranked top 5 from May 2 both-sessions WFO")
    print("=" * 100)
    print(f"  {'Rank':<5} {'P0':>4} {'Prof':>5} {'NP':>9} {'NP/DD':>8} {'Slope':>8} {'Plat$':>9} "
          f"Range/SL/RR/HTP/Tgt/Loss")
    for i, r in enumerate(ranked[:5], 1):
        cfg = r["cfg"]
        p0 = "PASS" if r["p0_pass"] else "FAIL"
        cfg_str = (f"{cfg.range_minutes}/{cfg.fixed_sl_pts}/{cfg.rr_ratio}/"
                   f"{cfg.half_tp_ratio}/{cfg.daily_target_pct}/{cfg.daily_loss_pct}")
        plat = r.get("plateau_score") or 0
        print(f"  {i:<5} {p0:>4} {r['prof_count']}/{len(r['oos_nps'])} "
              f"${r['total_np']:>+8,.0f} {r['np_dd_ratio']:>+8.0f} "
              f"{r['slope']:>+7.1%} ${plat:>+8,.0f}  {cfg_str}")

    def rank_to_cfg(rank_idx: int, comment: str, risk_pct: float) -> ORBConfig:
        c = ranked[rank_idx]["cfg"]
        return row_to_cfg({
            "range_minutes": c.range_minutes,
            "fixed_sl_pts":  c.fixed_sl_pts,
            "rr_ratio":      c.rr_ratio,
            "half_tp_ratio": c.half_tp_ratio,
            "daily_target_pct": c.daily_target_pct,
            "daily_loss_pct":   c.daily_loss_pct,
        }, comment, risk_pct=risk_pct)

    # Run portfolio sims
    start = datetime(2026, 2, 14, tzinfo=timezone.utc)
    end = datetime(2026, 5, 1, tzinfo=timezone.utc)
    days = (end - start).days

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)

        print("\n" + "=" * 100)
        print(f"  PORTFOLIO sanity: rank1 vs +2 vs +2+3 vs +2+3+4 ({days}d, $10k)")
        print(f"  FAIR risk allocation: total {RISK}% per setup split equally across N streams.")
        print(f"  (1×3.00%   2×1.50%   3×1.00%   4×0.75%)")
        print("=" * 100)

        def run_portfolio(cfgs_with_labels, ticks):
            """Run multi-stream portfolio sim, return aggregated metrics + per-stream."""
            deals = []
            per_stream = {}
            for label, cfg in cfgs_with_labels:
                r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
                stream_np = 0.0
                stream_tr = 0
                for d in r.deals:
                    if d.kind != "entry":
                        deals.append((d.ts, label, d.pnl))
                        stream_np += d.pnl
                        stream_tr += 1
                per_stream[label] = (stream_np, stream_tr)
            agg = aggregate(deals)
            agg["per_stream"] = per_stream
            agg["trades"] = len(deals)
            return agg

        # Fair-risk cfgs: each stream gets RISK / N
        rank1_full = rank_to_cfg(0, "ORB", risk_pct=RISK)
        # 2-stream
        rank1_half = rank_to_cfg(0, "S1", risk_pct=RISK / 2)
        rank2_half = rank_to_cfg(1, "S2", risk_pct=RISK / 2)
        # 3-stream
        rank1_third = rank_to_cfg(0, "S1", risk_pct=RISK / 3)
        rank2_third = rank_to_cfg(1, "S2", risk_pct=RISK / 3)
        rank3_third = rank_to_cfg(2, "S3", risk_pct=RISK / 3)
        # 4-stream
        rank1_q = rank_to_cfg(0, "S1", risk_pct=RISK / 4)
        rank2_q = rank_to_cfg(1, "S2", risk_pct=RISK / 4)
        rank3_q = rank_to_cfg(2, "S3", risk_pct=RISK / 4)
        rank4_q = rank_to_cfg(3, "S4", risk_pct=RISK / 4)

        for sp in (23, 35, 55):
            ticks = load_ticks(SYMBOL, start, end, spread_pts=sp)
            print(f"\n  --- Spread {sp}pt ---")
            print(f"  {'Variant':<16} {'NP':>10} {'DD%':>6} {'NP/DD$':>7} {'Trades':>7}  Per-stream NP")

            agg_b    = run_portfolio([("ORB", rank1_full)], ticks)
            agg_12   = run_portfolio([("S1", rank1_half), ("S2", rank2_half)], ticks)
            agg_123  = run_portfolio([("S1", rank1_third), ("S2", rank2_third), ("S3", rank3_third)], ticks)
            agg_1234 = run_portfolio([("S1", rank1_q), ("S2", rank2_q), ("S3", rank3_q), ("S4", rank4_q)], ticks)

            for label, agg in [("Rank1 only", agg_b),
                                ("Rank1+2", agg_12),
                                ("Rank1+2+3", agg_123),
                                ("Rank1+2+3+4", agg_1234)]:
                ps_str = "  ".join(f"{s}:${np_s:+,.0f}({tr_s})"
                                    for s, (np_s, tr_s) in agg["per_stream"].items())
                print(f"  {label:<16} ${agg['np']:>+8,.0f} "
                      f"{agg['dd_pct']:>5.1f}% {agg['ndd']:>7.2f} "
                      f"{agg['trades']:>7}  {ps_str}")
            print(f"  {'delta vs R1':<16} "
                  f"R1+2:NP{agg_12['np']-agg_b['np']:+,.0f}/NPDD{agg_12['ndd']-agg_b['ndd']:+.2f}  "
                  f"R1+2+3:NP{agg_123['np']-agg_b['np']:+,.0f}/NPDD{agg_123['ndd']-agg_b['ndd']:+.2f}  "
                  f"R1+2+3+4:NP{agg_1234['np']-agg_b['np']:+,.0f}/NPDD{agg_1234['ndd']-agg_b['ndd']:+.2f}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
