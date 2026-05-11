"""Print per-trade TP/SL counts per stream for today's OOS sim @ 9% risk.

Mirrors sim_orb_oos_today.py setup but emits exit-kind breakdown.
"""
from __future__ import annotations
import sys
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from sim_orb_oos_today import (PREV_WFO_DIR, CURR_WFO_DIR, SPREAD_LIVE, DEPOSIT,
                                fetch_meta, fetch_window, row_to_cfg)
from zgb_sim.wfo_helpers import WINDOWS_MAY2, WINDOWS_MAY9, rank_with_p0
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast as orb_simulate
import pandas as pd

end = datetime.now(timezone.utc)
start = datetime(end.year, end.month, end.day, tzinfo=timezone.utc)


def load_top3(wfo_dir, windows):
    def _read(label):
        for prefix in ("", "p1_"):
            p_is = wfo_dir / f"{prefix}is_{label}.parquet"
            p_oos = wfo_dir / f"{prefix}oos_{label}.parquet"
            if p_is.exists() and p_oos.exists():
                return pd.read_parquet(p_is), pd.read_parquet(p_oos)
        raise FileNotFoundError(label)
    is_per, oos_per = {}, {}
    for label, _, _, _, _ in windows:
        is_per[label], oos_per[label] = _read(label)
    cands = [row_to_cfg(r, "ORB", 3.0) for _, r in oos_per["W1"].iterrows()]
    grid = [row_to_cfg(r, "ORB", 3.0) for _, r in is_per["W1"].iterrows()]
    ranked = rank_with_p0(cands, oos_per, windows, decay_threshold=-0.25,
                          grid_configs=grid, is_per_window=is_per)
    return [ranked[i]["cfg"] for i in range(3)]


prev = load_top3(PREV_WFO_DIR, WINDOWS_MAY2)
curr = load_top3(CURR_WFO_DIR, WINDOWS_MAY9)
labels = ["S1", "S2", "S3", "S4", "S5", "S6"]
cfgs_base = prev + curr

sym_used, m = fetch_meta(None, account="live")
meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                  tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                  volume_min=m["volume_min"], volume_max=m["volume_max"],
                  volume_step=m["volume_step"])
sym_used, ticks, m1, m5 = fetch_window(sym_used, start, end, SPREAD_LIVE, account="live")

print(f"\n=== Sim per-stream exit-kind breakdown @ 9% risk (today, {start.date()}) ===")
print(f"  Window: {start} -> {end}")
print(f"  Symbol: {sym_used}  Ticks: {len(ticks):,}\n")

per_stream_risk = 9.0 / 6
totals = defaultdict(lambda: {"tp": 0, "sl": 0, "tp_pnl": 0.0, "sl_pnl": 0.0,
                                "other": 0, "other_pnl": 0.0})
print(f"  {'Stream':<6} {'Cfg':<22} {'TP':>4} {'SL':>4} {'Other':>5} "
      f"{'TP_PnL':>9} {'SL_PnL':>9} {'Other_PnL':>9} {'Net':>9}")
print(f"  {'-'*6} {'-'*22} {'-'*4} {'-'*4} {'-'*5} {'-'*9} {'-'*9} {'-'*9} {'-'*9}")
for lbl, base_cfg in zip(labels, cfgs_base):
    cfg = row_to_cfg({"range_minutes": base_cfg.range_minutes,
                      "fixed_sl_pts": base_cfg.fixed_sl_pts,
                      "rr_ratio": base_cfg.rr_ratio,
                      "half_tp_ratio": base_cfg.half_tp_ratio,
                      "daily_target_pct": 0.0, "daily_loss_pct": 0.0},
                     lbl, per_stream_risk)
    r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
    cfg_desc = f"SL={cfg.fixed_sl_pts} HTP={cfg.half_tp_ratio}"
    counts = totals[lbl]
    for d in r.deals:
        if d.kind == "entry":
            continue
        if d.kind == "tp" or d.kind == "htp":
            counts["tp"] += 1; counts["tp_pnl"] += d.pnl
        elif d.kind == "sl":
            counts["sl"] += 1; counts["sl_pnl"] += d.pnl
        else:
            counts["other"] += 1; counts["other_pnl"] += d.pnl
    c = counts
    net = c["tp_pnl"] + c["sl_pnl"] + c["other_pnl"]
    print(f"  {lbl:<6} {cfg_desc:<22} {c['tp']:>4} {c['sl']:>4} {c['other']:>5} "
          f"${c['tp_pnl']:>+7,.0f} ${c['sl_pnl']:>+7,.0f} ${c['other_pnl']:>+7,.0f} "
          f"${net:>+7,.0f}")

print()
total_tp = sum(t["tp"] for t in totals.values())
total_sl = sum(t["sl"] for t in totals.values())
total_other = sum(t["other"] for t in totals.values())
print(f"  Totals: TP={total_tp}  SL={total_sl}  Other={total_other}  "
      f"All={total_tp + total_sl + total_other}")
