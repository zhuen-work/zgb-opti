"""Global hedge WFO — finds the SINGLE hedge config that maximizes joint
3-stream portfolio NP/DD$ across W1-W4, ranked via P0+plateau+prof_count.

Use this AFTER the parent ORB WFO completes:
  1. Run scripts/sim_wfo_orb.py --session both --phase 1
  2. Run scripts/extract_top_n.py to confirm top-3 parents (or just trust the ranker)
  3. Run THIS script: it pulls top-3 parents from the WFO output and optimizes
     a single global hedge config that fires on each parent's SL events.

Output: output/wfo_hedge_global_<wfo_dir_basename>/winner.json with the global
hedge cfg (buf, h_sl, h_rr, exp). Feed to extract_top_n.py via --hedge-cfg-json.

Sizing: parent at 1% (production sizing), hedge at 1% (matches per-stream
allocation). Hedge is independent risk — total max-stop exposure is 2% per stream
when both parent and hedge stop on same setup.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

# Register sim_wfo_hedge module so its @dataclass can introspect (Python 3.14 quirk)
import importlib.util
_hg_spec = importlib.util.spec_from_file_location("wfo_hedge", ROOT / "scripts" / "sim_wfo_hedge.py")
hg = importlib.util.module_from_spec(_hg_spec)
sys.modules["wfo_hedge"] = hg
_hg_spec.loader.exec_module(hg)

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.wfo_helpers import (WINDOWS_MAY2 as WINDOWS, rank_with_p0,
                                  print_phase_d_with_p0, select_winner_with_p0,
                                  check_winner_boundaries, print_boundary_check, to_utc)

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
SPREAD = 30  # per feedback_default_test_conditions.md (all live = 30pt 2026-05-16)
PARENT_RISK = 1.0       # production per-stream allocation (3% setfile / 3 streams)
HEDGE_RISK_PCT = 1.0    # mirrors parent per-stream allocation

# Hedge sweep grid (matches sim_wfo_hedge.py for consistency)
BUFFERS    = [0, 50, 100, 200, 350]
HEDGE_SLS  = [300, 500, 700, 900]
HEDGE_RRS  = [2.0, 3.0, 4.0]
EXPIRES    = [30, 120]


def load_top3_from_wfo(wfo_dir: Path) -> list[dict]:
    """Re-rank the WFO output and return top-3 parent configs as dicts."""
    is_per = {}
    oos_per = {}
    for label, _, _, _, _ in WINDOWS:
        is_p = wfo_dir / f"is_{label}.parquet"
        oos_p = wfo_dir / f"oos_{label}.parquet"
        if not is_p.exists():
            is_p = wfo_dir / f"p1_is_{label}.parquet"
        if not oos_p.exists():
            oos_p = wfo_dir / f"p1_oos_{label}.parquet"
        if not is_p.exists() or not oos_p.exists():
            raise FileNotFoundError(f"Missing WFO parquets for {label} in {wfo_dir}")
        is_per[label] = pd.read_parquet(is_p)
        oos_per[label] = pd.read_parquet(oos_p)

    def row_to_cfg(row):
        return ORBConfig(
            risk_pct=PARENT_RISK,
            range_minutes=int(row["range_minutes"]),
            buffer_pts=0, min_range_pts=200, max_range_pts=5000,
            fixed_sl_pts=int(row["fixed_sl_pts"]),
            rr_ratio=float(row["rr_ratio"]),
            half_tp_ratio=round(float(row["half_tp_ratio"]), 2),
            pending_expire_minutes=240,
            daily_target_pct=float(row.get("daily_target_pct", 0.0)),
            daily_loss_pct=float(row.get("daily_loss_pct", 0.0)),
            ldn_enabled=True, ldn_start_hour=7,
            ny_enabled=True,  ny_start_hour=13,
            comment="ORB",
        )

    cands = [row_to_cfg(r) for _, r in oos_per["W1"].iterrows()]
    grid = [row_to_cfg(r) for _, r in is_per["W1"].iterrows()]
    ranked = rank_with_p0(cands, oos_per, WINDOWS, decay_threshold=-0.25,
                          grid_configs=grid, is_per_window=is_per)
    return [{"range_minutes": r["cfg"].range_minutes,
             "fixed_sl_pts": r["cfg"].fixed_sl_pts,
             "rr_ratio": r["cfg"].rr_ratio,
             "half_tp_ratio": r["cfg"].half_tp_ratio} for r in ranked[:3]]


def make_parent_cfg(parent_dict, risk_pct: float) -> ORBConfig:
    return ORBConfig(
        risk_pct=risk_pct,
        range_minutes=parent_dict["range_minutes"],
        buffer_pts=0, min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=parent_dict["fixed_sl_pts"],
        rr_ratio=parent_dict["rr_ratio"],
        half_tp_ratio=parent_dict["half_tp_ratio"],
        pending_expire_minutes=240,
        daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True,  ny_start_hour=13,
        comment="ORB",
    )


def run_parent_window(parent_cfg, ticks, m1, m5, meta):
    """Run one parent on a window. Returns (deals, sl_events)."""
    from zgb_sim.orb_fast import simulate_fast as orb_simulate
    r = orb_simulate(ticks, m5, m1, parent_cfg, meta, initial_balance=DEPOSIT)
    deals = []
    sl_events = []
    for d in r.deals:
        if d.kind == "entry":
            continue
        ts_ns = pd.Timestamp(d.ts).value
        deals.append((ts_ns, d.pnl))
        if d.kind == "sl":
            sl_events.append({"ts_ns": ts_ns, "direction": int(d.direction),
                              "sl_price": float(d.price), "lots": float(d.lots)})
    return deals, sl_events


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wfo-dir", type=Path, default=ROOT / "output" / "wfo_orb_may2",
                    help="Parent ORB WFO output dir")
    ap.add_argument("--out-dir", type=Path, default=None,
                    help="Where to write winner.json (default: output/wfo_hedge_global_<wfo_basename>)")
    args = ap.parse_args()

    out_dir = args.out_dir or (ROOT / "output" / f"wfo_hedge_global_{args.wfo_dir.name.replace('wfo_orb_', '')}")
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 110)
    print(f"  GLOBAL HEDGE WFO  |  parent WFO: {args.wfo_dir}  |  out: {out_dir}")
    print(f"  Parent risk: {PARENT_RISK}%/stream, hedge risk: {HEDGE_RISK_PCT}%/hedge")
    print(f"  Grid: buf={BUFFERS} h_sl={HEDGE_SLS} h_rr={HEDGE_RRS} exp={EXPIRES}  "
          f"= {len(BUFFERS)*len(HEDGE_SLS)*len(HEDGE_RRS)*len(EXPIRES)} cells × 4 windows × IS+OOS")
    print("=" * 110)

    # Pull top-3 parents from WFO
    parents = load_top3_from_wfo(args.wfo_dir)
    for i, p in enumerate(parents, 1):
        print(f"  Parent S{i}: Range={p['range_minutes']} SL={p['fixed_sl_pts']} "
              f"RR={p['rr_ratio']} HTP={p['half_tp_ratio']}")

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        start = to_utc(WINDOWS[0][1]); end = to_utc(WINDOWS[-1][4])
        full_m1 = load_bars(SYMBOL, "M1", start, end)
        full_m5 = load_bars(SYMBOL, "M5", start, end)
        full_ticks = load_ticks(SYMBOL, start, end, spread_pts=SPREAD)
        print(f"\n  Full data {start.date()}->{end.date()}: ticks={len(full_ticks):,} "
              f"M1={len(full_m1):,} M5={len(full_m5):,}")

        grid = [hg.HedgeCfg(b, hs, hr, e) for b in BUFFERS for hs in HEDGE_SLS
                for hr in HEDGE_RRS for e in EXPIRES]
        n_cells = len(grid)

        # Patch the hedge module's HEDGE_RISK_PCT for this run
        hg.HEDGE_RISK_PCT = HEDGE_RISK_PCT

        is_per_w: dict[str, pd.DataFrame] = {}
        oos_per_w: dict[str, pd.DataFrame] = {}
        t_start = time.time()

        for label, is_s, is_e, oos_s, oos_e in WINDOWS:
            for tag, (s, e), bucket in [("IS", (to_utc(is_s), to_utc(is_e)), is_per_w),
                                         ("OOS", (to_utc(oos_s), to_utc(oos_e)), oos_per_w)]:
                ticks = hg.slice_window(full_ticks, "ts", s, e)
                m1 = hg.slice_window(full_m1, "ts", s, e)
                m5 = hg.slice_window(full_m5, "ts", s, e)
                t_arr = hg.ts_arr_from_ticks(ticks)

                # Run all 3 parents once on this window — collect deals + SL events per parent
                per_parent_deals = []
                per_parent_sl = []
                for p in parents:
                    pcfg = make_parent_cfg(p, PARENT_RISK)
                    pdeals, psl = run_parent_window(pcfg, ticks, m1, m5, meta)
                    per_parent_deals.append(pdeals)
                    per_parent_sl.append(psl)

                rows = []
                for hc in grid:
                    # Combine all parents + hedge per parent → portfolio deals
                    all_deals = []
                    for pdeals, psl in zip(per_parent_deals, per_parent_sl):
                        all_deals.extend(pdeals)
                        h_deals = hg.simulate_hedges(psl, t_arr, hc)
                        all_deals.extend(h_deals)
                    np_, dd, pf = hg.aggregate(all_deals)
                    rows.append({"buf": hc.buf, "h_sl": hc.h_sl, "h_rr": hc.h_rr, "exp": hc.exp,
                                 "net_profit": np_, "drawdown_pct": dd, "profit_factor": pf})
                df = pd.DataFrame(rows)
                bucket[label] = df
                df.to_parquet(out_dir / f"{tag.lower()}_{label}.parquet", index=False)
                base_np = sum(p for pd_ in per_parent_deals for _, p in pd_)
                print(f"  {label} {tag} {s.date()}->{e.date()}  base portfolio NP=${base_np:+,.0f}  "
                      f"hedge cells={n_cells}  [{time.time()-t_start:.1f}s]")

        # Rank
        ranked = rank_with_p0(grid, oos_per_w, WINDOWS, decay_threshold=-0.25,
                              grid_configs=grid, is_per_window=is_per_w)
        print_phase_d_with_p0(ranked[:10], "global hedge", decay_threshold=-0.25)
        winner = select_winner_with_p0(ranked) or ranked[0]
        flagged = check_winner_boundaries(winner["cfg"], grid)
        print_boundary_check(flagged)

        wcfg = winner["cfg"]
        result = {
            "buffer_pts": wcfg.buf,
            "fixed_sl_pts": wcfg.h_sl,
            "rr_ratio": wcfg.h_rr,
            "expire_minutes": wcfg.exp,
            "risk_pct": HEDGE_RISK_PCT,
            "parent_risk_pct": PARENT_RISK,
            "wfo_dir": str(args.wfo_dir),
            "spread_pts": SPREAD,
            "windows": [w[0] for w in WINDOWS],
            "p0_pass": winner["p0_pass"],
            "prof_count": winner["prof_count"],
            "total_np": winner["total_np"],
            "avg_dd": winner["avg_dd"],
            "np_dd_ratio": winner["np_dd_ratio"],
            "oos_nps": winner["oos_nps"],
            "slope": winner["slope"],
            "boundary_flags": [{"dim": f["dim"], "value": f["value"], "at_boundary": f["at_boundary"]}
                                for f in flagged if f["at_boundary"]],
        }
        winner_path = out_dir / "winner.json"
        winner_path.write_text(json.dumps(result, indent=2, default=str))
        print(f"\n  WINNER for global hedge: buf={wcfg.buf} h_sl={wcfg.h_sl} "
              f"h_rr={wcfg.h_rr} exp={wcfg.exp}")
        print(f"  Saved to {winner_path}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
