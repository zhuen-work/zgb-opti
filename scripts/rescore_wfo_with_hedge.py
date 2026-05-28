"""Re-rank a parent-only WFO's top-N candidates by parent+hedge composite NP/DD$.

Why this exists:
  The standard WFO (sim_wfo_orb_v2_fractal_parametric.py) ranks candidates by
  parent NP/DD$ ONLY. In production each parent has a STOP-on-extension hedge
  attached (v6/v7), and hedge income can shift which parent picks are actually
  best. This script re-runs the top-N OOS candidates with hedge sim layered
  in, then re-ranks by composite NP/DD$ using the same prof_count + decay logic.

Output:
  <wfo_dir>/oos_rank_hedged.csv  (same columns as oos_rank.csv, re-ordered)

Methodology notes:
  - Uses same OOS windows as the parent WFO (from WINDOWS_BY_KEY)
  - Uses same WFO spread (parsed from --wfo-dir name, e.g. "_spread30")
  - Uses default v6/v7 deployment hedge: STOP-ext ExtPts=100 TPMult=3.0 SLMult=1.0
  - Parent sim includes V2 fractal-confirm + SMA(8,21) cross-exit (v7 deployment)
  - Re-ranking applies rank_with_p0 with prof_count primary tiebreak (matches WFO)

Run:
  $env:ZGB_SPREAD_PTS_OVERRIDE = "30"   # not strictly needed, script sets it
  python scripts/rescore_wfo_with_hedge.py --wfo-dir output/wfo_orb_v2_may23_spread30 --window-key may23
  python scripts/rescore_wfo_with_hedge.py --wfo-dir output/wfo_orb_v2_may23 --window-key may23  # 60pt default
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast
from zgb_sim.tick_loader import symbol_meta, load_ticks, load_bars, kill_mt5_terminal
from zgb_sim.wfo_helpers import (WINDOWS_MAY2, WINDOWS_MAY9, WINDOWS_MAY16,
                                  WINDOWS_MAY23, rank_with_p0)
from sim_wfo_hedge_reverse import StopExtensionCfg, simulate_stop_extension_hedges
from sim_orb_oos_today_hedge_v6 import extract_sl_events, ts_arr_from_ticks

WINDOWS_BY_KEY = {
    "may2":  WINDOWS_MAY2,
    "may9":  WINDOWS_MAY9,
    "may16": WINDOWS_MAY16,
    "may23": WINDOWS_MAY23,
}

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
RISK_PCT = 6.0

V7_PARENT = dict(sma_cross_exit=True, sma_cross_fast=8, sma_cross_slow=21,
                  sma_cross_atr_gate=0.0)

HCFG = StopExtensionCfg(exp_min=240, f1_sec=1800, ext_pts=100,
                         tp_mult=3.0, sl_mult=1.0)


def _to_utc(d):
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def parse_spread_from_dir(wfo_dir: Path) -> int:
    m = re.search(r"_spread(\d+)", wfo_dir.name)
    if m:
        return int(m.group(1))
    return 60  # legacy may23 dir without suffix


def cfg_from_row(row) -> ORBConfig:
    return ORBConfig(
        risk_pct=RISK_PCT,
        range_minutes=int(row["range_minutes"]), buffer_pts=0,
        min_range_pts=0, max_range_pts=999_999,
        fixed_sl_pts=int(row["fixed_sl_pts"]),
        rr_ratio=float(row["rr_ratio"]),
        half_tp_ratio=round(float(row["half_tp_ratio"]), 2),
        pending_expire_minutes=int(row["pending_expire_minutes"]),
        daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13,
        fractal_confirm=bool(row.get("fractal_confirm", True)),
        fractal_width=int(row.get("fractal_width", 5)),
        **V7_PARENT, comment="ORB",
    )


def run_window_composite(cfgs: list[ORBConfig], ticks: pd.DataFrame,
                          m1: pd.DataFrame, m5: pd.DataFrame,
                          meta: SymbolMeta, ticks_arr: dict) -> list[dict]:
    """For each cfg, run parent + hedge on this window and return per-cfg metrics."""
    results = []
    for sn, cfg in enumerate(cfgs, start=1):
        r = simulate_fast(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        parent_pnls, sl_events = extract_sl_events(r.deals)
        deals = [(ts, p) for ts, p in parent_pnls]
        sm = {"magic": sn, "risk_pct": RISK_PCT,
              "fixed_sl_pts": cfg.fixed_sl_pts, "rr_ratio": cfg.rr_ratio,
              "half_tp_ratio": cfg.half_tp_ratio,
              "range_minutes": cfg.range_minutes,
              "pending_expire_minutes": cfg.pending_expire_minutes}
        hpn = simulate_stop_extension_hedges(sl_events, ticks_arr, sm, HCFG)
        for ts_ns, p in hpn:
            deals.append((int(ts_ns), p))
        deals.sort(key=lambda x: x[0])
        bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
        for _, p in deals:
            bal += p
            if bal > bal_max: bal_max = bal
            if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        np_ = bal - DEPOSIT
        dd_pct = (dd_abs / max(bal_max, DEPOSIT) * 100.0) if bal_max > 0 else 0.0
        results.append({
            "net_profit": np_,
            "drawdown": dd_abs,
            "drawdown_pct": dd_pct,
            "trades": len(deals),
            "error": np.nan,
        })
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wfo-dir", required=True,
                     help="Directory of parent-only WFO (e.g. output/wfo_orb_v2_may23_spread30)")
    ap.add_argument("--window-key", required=True, choices=list(WINDOWS_BY_KEY))
    ap.add_argument("--top-n", type=int, default=15,
                     help="Number of top parent-rank candidates to rescore (default 15)")
    args = ap.parse_args()

    wfo_dir = Path(args.wfo_dir)
    if not wfo_dir.is_absolute():
        wfo_dir = ROOT / wfo_dir
    rank_csv = wfo_dir / "oos_rank.csv"
    if not rank_csv.exists():
        print(f"[ERROR] missing {rank_csv}")
        return 1

    spread = parse_spread_from_dir(wfo_dir)
    os.environ["ZGB_SPREAD_PTS_OVERRIDE"] = str(spread)

    WINDOWS = WINDOWS_BY_KEY[args.window_key]
    print(f"[BOOT] Rescoring {wfo_dir.name} at spread={spread}pt across "
          f"{len(WINDOWS)} OOS windows", flush=True)

    df_rank = pd.read_csv(rank_csv).head(args.top_n)
    print(f"[CANDS] Loaded top-{len(df_rank)} parent-rank candidates", flush=True)
    cfgs = [cfg_from_row(r) for _, r in df_rank.iterrows()]

    m = symbol_meta(SYMBOL)
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])

    oos_per_window = {}
    for label, _, _, oos_s, oos_e in WINDOWS:
        t0 = time.time()
        ts = _to_utc(oos_s); te = _to_utc(oos_e)
        ticks = load_ticks(SYMBOL, ts, te)
        m1 = load_bars(SYMBOL, "M1", ts, te)
        m5 = load_bars(SYMBOL, "M5", ts, te)
        ticks_arr = ts_arr_from_ticks(ticks)
        rows = run_window_composite(cfgs, ticks, m1, m5, meta, ticks_arr)
        oos_per_window[label] = pd.DataFrame(rows)
        nps = [r["net_profit"] for r in rows]
        print(f"[OOS-{label}] {oos_s}->{oos_e} done in {time.time()-t0:.0f}s "
              f"(NP range ${min(nps):+,.0f}..${max(nps):+,.0f})", flush=True)

    ranked = rank_with_p0(cfgs, oos_per_window, WINDOWS, decay_threshold=-0.25,
                           grid_configs=None, is_per_window=None)

    print(f"\n[HEDGED-RANK] Composite NP/DD$ top-10 (parent+hedge):", flush=True)
    for i, info in enumerate(ranked[:10]):
        c = info["cfg"]; np_dd = info.get("np_dd_ratio", 0); prof = info.get("prof_count", 0)
        total_np = info.get("total_np", 0)
        print(f"  rank#{i+1} prof={prof}/4 total_np=${total_np:>+8,.0f} "
              f"NP/DD$={np_dd:>5.0f}  R={c.range_minutes:>3} SL={c.fixed_sl_pts:>4} "
              f"RR={c.rr_ratio:<3} HTP={c.half_tp_ratio:<3} "
              f"V2_w{c.fractal_width if c.fractal_confirm else '0'}", flush=True)

    parent_lookup = {}
    for _, r in df_rank.iterrows():
        key = (int(r["range_minutes"]), int(r["fixed_sl_pts"]),
               round(float(r["rr_ratio"]), 2), round(float(r["half_tp_ratio"]), 2),
               int(r["pending_expire_minutes"]),
               bool(r.get("fractal_confirm", True)),
               int(r.get("fractal_width", 5)))
        parent_lookup[key] = int(r["rank"])

    rows = []
    for i, info in enumerate(ranked):
        c = info["cfg"]
        key = (c.range_minutes, c.fixed_sl_pts, round(c.rr_ratio, 2),
               round(c.half_tp_ratio, 2), c.pending_expire_minutes,
               c.fractal_confirm, c.fractal_width)
        rows.append({
            "rank": i + 1,
            "parent_rank": parent_lookup.get(key, -1),
            "prof_count": info.get("prof_count", 0),
            "total_np": info.get("total_np", 0),
            "np_dd_ratio": info.get("np_dd_ratio", 0),
            "range_minutes": c.range_minutes,
            "fixed_sl_pts": c.fixed_sl_pts,
            "rr_ratio": c.rr_ratio,
            "half_tp_ratio": c.half_tp_ratio,
            "pending_expire_minutes": c.pending_expire_minutes,
            "fractal_confirm": c.fractal_confirm,
            "fractal_width": c.fractal_width,
        })
    out = wfo_dir / "oos_rank_hedged.csv"
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"\n[DONE] Wrote {out}", flush=True)
    print(f"       Parent-rank -> hedged-rank shifts in top-6:", flush=True)
    for r in rows[:6]:
        shift = r["parent_rank"] - r["rank"]
        marker = "  " if shift == 0 else (f"+{shift}" if shift > 0 else str(shift))
        print(f"       hedged #{r['rank']:>2}  was parent #{r['parent_rank']:>2}  "
              f"({marker})  R{r['range_minutes']} SL{r['fixed_sl_pts']} "
              f"RR{r['rr_ratio']} HTP{r['half_tp_ratio']}", flush=True)
    kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
