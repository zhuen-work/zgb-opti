"""Compare 3-stream rank portfolio at 3% / 4.5% / 6% total risk.

Each level: rank1 + rank2 + rank3, each at total/3 risk_pct.
Reports NP, DD, NP/DD$, ROI, trades across 23 / 35 / 55 pt spreads.

With --hedge-cfg-json: also runs +hedge variant side-by-side at each risk level.
Hedge sized at per_stream_risk (mirrors parent allocation).
"""
from __future__ import annotations
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

# Register sim_wfo_hedge module for HedgeCfg dataclass + simulate_hedges helper
import importlib.util
_hg_spec = importlib.util.spec_from_file_location("wfo_hedge", ROOT / "scripts" / "sim_wfo_hedge.py")
hg = importlib.util.module_from_spec(_hg_spec)
sys.modules["wfo_hedge"] = hg
_hg_spec.loader.exec_module(hg)

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.wfo_helpers import WINDOWS_MAY2 as WINDOWS, rank_with_p0

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
WFO_DIR = ROOT / "output" / "wfo_orb_may2"


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


def run_with_hedge(cfgs, ticks, m1, m5, meta, hedge_for_stream, per_stream_risk):
    """Run 3-stream portfolio + hedge per stream. hedge_for_stream(label) -> cfg dict."""
    hg.HEDGE_RISK_PCT = per_stream_risk  # mirror per-stream allocation
    t_arr = hg.ts_arr_from_ticks(ticks)
    deals = []; per_s = {}; per_h = {}
    for label, cfg in cfgs:
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        np_s = 0.0; tr_s = 0
        sl_events = []
        for d in r.deals:
            if d.kind == "entry":
                continue
            deals.append((d.ts, label, d.pnl))
            np_s += d.pnl; tr_s += 1
            if d.kind == "sl":
                sl_events.append({"ts_ns": pd.Timestamp(d.ts).value,
                                  "direction": int(d.direction),
                                  "sl_price": float(d.price),
                                  "lots": float(d.lots)})
        per_s[label] = (np_s, tr_s)
        hcfg = hedge_for_stream(label)
        h_deals = hg.simulate_hedges(sl_events, t_arr,
                                      hg.HedgeCfg(buf=hcfg["buffer_pts"],
                                                   h_sl=hcfg["fixed_sl_pts"],
                                                   h_rr=hcfg["rr_ratio"],
                                                   exp=hcfg["expire_minutes"]))
        h_pnl = 0.0; h_wins = 0
        for ts, p in h_deals:
            deals.append((pd.Timestamp(ts), f"{label}h", p))
            h_pnl += p
            if p > 0: h_wins += 1
        per_h[label] = (h_pnl, len(h_deals), h_wins)
    return deals, per_s, per_h


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hedge-cfg-json", type=Path, default=None,
                    help="Single global hedge cfg (winner.json from sim_wfo_hedge_global.py)")
    ap.add_argument("--hedge-per-stream-dir", type=Path, default=None,
                    help="Dir with per-stream {S1,S2,S3}.json (from sim_wfo_hedge.py)")
    args = ap.parse_args()
    # hedge_for_stream(stream_name) -> dict | None
    if args.hedge_per_stream_dir:
        per_stream_hedge = {}
        for s in ("S1", "S2", "S3"):
            p = args.hedge_per_stream_dir / f"{s}.json"
            per_stream_hedge[s] = json.loads(p.read_text())
            h = per_stream_hedge[s]
            print(f"  Hedge {s}: buf={h['buffer_pts']} h_sl={h['fixed_sl_pts']} "
                  f"h_rr={h['rr_ratio']} exp={h['expire_minutes']}min")
        def hedge_for_stream(s): return per_stream_hedge[s]
    elif args.hedge_cfg_json:
        h = json.loads(args.hedge_cfg_json.read_text())
        print(f"  Hedge cfg (global): buf={h['buffer_pts']} h_sl={h['fixed_sl_pts']} "
              f"h_rr={h['rr_ratio']} exp={h['expire_minutes']}min")
        def hedge_for_stream(s): return h
    else:
        def hedge_for_stream(s): return None
    hedge_active = (args.hedge_cfg_json or args.hedge_per_stream_dir)
    is_per = {label: pd.read_parquet(WFO_DIR / f"is_{label}.parquet") for label, _, _, _, _ in WINDOWS}
    oos_per = {label: pd.read_parquet(WFO_DIR / f"oos_{label}.parquet") for label, _, _, _, _ in WINDOWS}

    cands_df = oos_per["W1"].copy()
    candidates = [row_to_cfg(r, "ORB", 3.0) for _, r in cands_df.iterrows()]
    full_grid = [row_to_cfg(r, "ORB", 3.0) for _, r in is_per["W1"].iterrows()]

    ranked = rank_with_p0(candidates, oos_per, WINDOWS, decay_threshold=-0.25,
                          grid_configs=full_grid, is_per_window=is_per)

    def rank_to_row(rank_idx: int):
        c = ranked[rank_idx]["cfg"]
        return {"range_minutes": c.range_minutes, "fixed_sl_pts": c.fixed_sl_pts,
                "rr_ratio": c.rr_ratio, "half_tp_ratio": c.half_tp_ratio,
                "daily_target_pct": c.daily_target_pct, "daily_loss_pct": c.daily_loss_pct}

    r1_row = rank_to_row(0)
    r2_row = rank_to_row(1)
    r3_row = rank_to_row(2)

    print("=" * 100)
    print(f"  Top 3 ranks from May 2 WFO:")
    for i, r in enumerate([r1_row, r2_row, r3_row], 1):
        print(f"    Rank {i}: Range={r['range_minutes']} SL={r['fixed_sl_pts']} "
              f"RR={r['rr_ratio']} HTP={r['half_tp_ratio']}")
    print("=" * 100)

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

        print(f"\n  3-stream rank portfolio (S1+S2+S3) — risk-level comparison")
        print(f"  ({days}d, $10k, total per-setup risk = N × per-stream)")

        for sp in (23, 35, 55):
            ticks = load_ticks(SYMBOL, start, end, spread_pts=sp)
            print(f"\n  --- Spread {sp}pt ---")
            print(f"  {'Total Risk':<10} {'PerStream':<10} {'NP':>10} {'NP-haircut':>12} "
                  f"{'ROI':>7} {'DD%':>6} {'NP/DD$':>7} {'Trades':>7}  Per-stream NP")

            for total_risk in (3.0, 4.5, 6.0):
                per_stream = total_risk / 3
                cfgs = [
                    ("S1", row_to_cfg(r1_row, "S1", per_stream)),
                    ("S2", row_to_cfg(r2_row, "S2", per_stream)),
                    ("S3", row_to_cfg(r3_row, "S3", per_stream)),
                ]
                # Baseline (no hedge)
                deals_b = []; per_s_b = {}
                for label, cfg in cfgs:
                    r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
                    np_s = 0.0; tr_s = 0
                    for d in r.deals:
                        if d.kind != "entry":
                            deals_b.append((d.ts, label, d.pnl))
                            np_s += d.pnl; tr_s += 1
                    per_s_b[label] = (np_s, tr_s)
                agg_b = aggregate(deals_b)
                ps_str_b = " ".join(f"{s}:${np_s:+,.0f}" for s, (np_s, tr_s) in per_s_b.items())
                print(f"  {total_risk:>5.1f}% base {per_stream:>5.2f}%   "
                      f"${agg_b['np']:>+8,.0f} ${agg_b['np']*0.94:>+10,.0f} {agg_b['np']/DEPOSIT*100:>+6.1f}% "
                      f"{agg_b['dd_pct']:>5.1f}% {agg_b['ndd']:>7.2f} {len(deals_b):>7}  {ps_str_b}")

                if hedge_active:
                    deals_w, per_s_w, per_h_w = run_with_hedge(cfgs, ticks, m1, m5, meta,
                                                                hedge_for_stream, per_stream)
                    agg_w = aggregate(deals_w)
                    ps_str_w = " ".join(f"{s}:${ps:+,.0f}+h${ph:+,.0f}({hn})"
                                         for s, (ps, _) in per_s_w.items()
                                         for ph, hn, hw in [per_h_w[s]])
                    d_np = agg_w["np"] - agg_b["np"]; d_dd = agg_w["dd_pct"] - agg_b["dd_pct"]
                    d_ndd = agg_w["ndd"] - agg_b["ndd"]
                    print(f"  {total_risk:>5.1f}% +hdg {per_stream:>5.2f}%   "
                          f"${agg_w['np']:>+8,.0f} ${agg_w['np']*0.94:>+10,.0f} {agg_w['np']/DEPOSIT*100:>+6.1f}% "
                          f"{agg_w['dd_pct']:>5.1f}% {agg_w['ndd']:>7.2f} {len(deals_w):>7}  {ps_str_w}")
                    print(f"          delta:                ${d_np:>+8,.0f}                "
                          f"{d_dd:>+5.1f}p {d_ndd:>+7.2f}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
