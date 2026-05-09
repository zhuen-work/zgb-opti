"""Compare per-stream hedge winners vs single global hedge config.

Three variants on Feb 14 -> May 1 (76d, $10k, parent 1%/stream, hedge 1%):
  A. per-stream:  S1=350/500/4.0/30, S2=100/500/4.0/120, S3=100/500/4.0/120
  B. global S2/S3 winner: 100/500/4.0/120 applied to all 3 streams
  C. global S1 winner:   350/500/4.0/30  applied to all 3 streams
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.wfo_helpers import WINDOWS_MAY2 as WINDOWS, to_utc

import importlib.util
_spec = importlib.util.spec_from_file_location("wfo_hedge", ROOT / "scripts" / "sim_wfo_hedge.py")
hg = importlib.util.module_from_spec(_spec)
sys.modules["wfo_hedge"] = hg  # required so @dataclass can introspect the module
_spec.loader.exec_module(hg)

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
SPREAD = 23
PARENT_RISK = 1.0  # production sizing

PER_STREAM = {
    "S1": hg.HedgeCfg(buf=350, h_sl=500, h_rr=4.0, exp=30),
    "S2": hg.HedgeCfg(buf=100, h_sl=500, h_rr=4.0, exp=120),
    "S3": hg.HedgeCfg(buf=100, h_sl=500, h_rr=4.0, exp=120),
}
GLOBAL_S2S3 = hg.HedgeCfg(buf=100, h_sl=500, h_rr=4.0, exp=120)
GLOBAL_S1   = hg.HedgeCfg(buf=350, h_sl=500, h_rr=4.0, exp=30)


def run_variant(name, hedge_for, full_ticks, full_m1, full_m5, meta):
    start = to_utc(WINDOWS[0][1]); end = to_utc(WINDOWS[-1][4])
    ticks = hg.slice_window(full_ticks, "ts", start, end)
    m1 = hg.slice_window(full_m1, "ts", start, end)
    m5 = hg.slice_window(full_m5, "ts", start, end)
    t_arr = hg.ts_arr_from_ticks(ticks)
    all_deals = []; per_s = {}; per_h = {}
    for s in ("S1", "S2", "S3"):
        deals, sl_ev = hg.run_baseline_window(s, ticks, m1, m5, meta, PARENT_RISK)
        h_deals = hg.simulate_hedges(sl_ev, t_arr, hedge_for(s)) if hedge_for else []
        all_deals.extend(deals + h_deals)
        per_s[s] = sum(p for _, p in deals)
        per_h[s] = (sum(p for _, p in h_deals), len(h_deals),
                    sum(1 for _, p in h_deals if p > 0))
    np_, dd, pf = hg.aggregate(all_deals)
    ndd = (np_ / (dd/100 * (DEPOSIT + np_))) if dd > 0 else 0
    return dict(name=name, np=np_, dd=dd, pf=pf, ndd=ndd, n=len(all_deals),
                per_s=per_s, per_h=per_h)


def main():
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
        print(f"Data loaded.  ticks={len(full_ticks):,} M1={len(full_m1):,} M5={len(full_m5):,}")

        variants = []
        # A. No hedge (control)
        variants.append(run_variant("no-hedge", None, full_ticks, full_m1, full_m5, meta))
        # B. Per-stream WFO winners
        variants.append(run_variant("per-stream", lambda s: PER_STREAM[s], full_ticks, full_m1, full_m5, meta))
        # C. Global S2/S3 (100/500/4.0/120)
        variants.append(run_variant("global S2/S3 (100/500/4.0/120)",
                                    lambda s: GLOBAL_S2S3, full_ticks, full_m1, full_m5, meta))
        # D. Global S1 (350/500/4.0/30)
        variants.append(run_variant("global S1 (350/500/4.0/30)",
                                    lambda s: GLOBAL_S1, full_ticks, full_m1, full_m5, meta))

        print("\n" + "=" * 110)
        print(f"  HEDGE: PER-STREAM vs GLOBAL  ({(end-start).days}d, $10k, parent 1%/stream, hedge 1%)")
        print("=" * 110)
        print(f"  {'Variant':<35} {'NP':>10} {'DD%':>6} {'NP/DD$':>7} {'PF':>5} {'Trades':>7}")
        for v in variants:
            print(f"  {v['name']:<35} ${v['np']:>+8,.0f} {v['dd']:>5.2f}% {v['ndd']:>7.2f} {v['pf']:>5.2f} {v['n']:>7}")
        print()
        print(f"  Per-stream contribution:")
        print(f"  {'Variant':<35}  S1_par/hedge(WR)         S2_par/hedge(WR)         S3_par/hedge(WR)")
        for v in variants:
            cells = []
            for s in ("S1", "S2", "S3"):
                pp = v["per_s"][s]
                hp, hn, hw = v["per_h"][s]
                wr = (hw / hn * 100) if hn else 0
                cells.append(f"${pp:+,.0f}/${hp:+,.0f}({wr:.0f}%)")
            print(f"  {v['name']:<35}  {cells[0]:<24} {cells[1]:<24} {cells[2]:<24}")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
