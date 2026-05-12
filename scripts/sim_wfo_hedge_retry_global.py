"""Retry-hedge WFO with GLOBAL exp_min + f1_sec, PER-STREAM tp_mult.

Approach:
  1. Per (stream, exp, f1, tp_mult) cell, run retry sim per window (IS+OOS).
  2. For each global (exp, f1) candidate, pick each stream's best tp_mult by
     summing its IS NPs across W1-W4.
  3. With those per-stream tp_mults, build the portfolio's OOS NP per window.
  4. Rank global (exp, f1) candidates by rank_with_p0 (P0 + plateau + decay).
  5. Output: 1 global (exp, f1) winner + 6 per-stream tp_mult choices.

Compared to sim_wfo_hedge_retry.py (per-stream independent):
  - exp_min and f1_sec are now ONE value across all 6 hedge sub-streams
  - tp_mult stays per-stream (each stream gets its own optimal target ratio)
"""
from __future__ import annotations

import sys
import time
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.wfo_helpers import (WINDOWS_MAY9 as WINDOWS, rank_with_p0,
                                  print_phase_d_with_p0, select_winner_with_p0,
                                  check_winner_boundaries, print_boundary_check, to_utc)

from sim_wfo_hedge_retry import (STREAM_CFGS, RetryHedgeCfg, make_stream_cfg,
                                  slice_window, aggregate, run_baseline_window,
                                  ts_arr_from_ticks, simulate_retry_hedges,
                                  SYMBOL, DEPOSIT, SPREAD, POINT, CONTRACT,
                                  PARENT_RISK_SWEEP, PARENT_RISK_PROD)

# Grid (smaller than full per-stream sweep since global exp/f1 + per-stream tp_mult)
EXPIRES_MIN     = [60, 120, 240, 480, 720, 1440, 2880]   # 7 global candidates
F1_CUTOFFS_SEC  = [0]                                     # F1 DISABLED per user request 2026-05-13
TP_MULTS        = [0.25, 0.5, 0.75, 1.0]                 # 4 per-stream
# Global (exp, f1) combos = 7 * 4 = 28
# Per-stream per-window inner sims = 7 * 4 * 4 * 6 streams = 672 per window
# Per-window total cells = 28 * (4 tp_mults) * 6 streams = 4032


def main() -> int:
    print("=" * 110)
    print("  RETRY-HEDGE WFO (GLOBAL exp_min/f1_sec, PER-STREAM tp_mult)")
    print(f"  Grid: exp({len(EXPIRES_MIN)}) * f1({len(F1_CUTOFFS_SEC)}) global "
          f"× tp_mult({len(TP_MULTS)}) per stream × 6 streams")
    print(f"  Per-stream inner sims per window: "
          f"{len(EXPIRES_MIN)*len(F1_CUTOFFS_SEC)*len(TP_MULTS)*6}")
    print("=" * 110)
    try:
        from zgb_sim.mt5_accounts import init_account
        init_account("sim")
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        start = to_utc(WINDOWS[0][1])
        end = to_utc(WINDOWS[-1][4])
        full_m1 = load_bars(SYMBOL, "M1", start, end)
        full_m5 = load_bars(SYMBOL, "M5", start, end)
        full_ticks = load_ticks(SYMBOL, start, end, spread_pts=SPREAD)
        print(f"\n  Full data {start.date()}->{end.date()}: ticks={len(full_ticks):,} "
              f"M1={len(full_m1):,} M5={len(full_m5):,}")

        # For each window, for each stream, store:
        #   results[window_label][stream][(exp, f1, tp_mult)] = (hedge_deals, baseline_deals)
        # Memory-efficient: store baseline_deals once per (window, stream), hedge_deals per cell.

        t_start = time.time()
        is_results = {}   # is_results[label][stream] = {(exp, f1, tp_mult): hedge_deals_list}
        oos_results = {}
        is_baselines = {} # is_baselines[label][stream] = baseline_deals_list
        oos_baselines = {}

        all_streams = list(STREAM_CFGS.keys())

        for label, is_s, is_e, oos_s, oos_e in WINDOWS:
            for tag, (s, e), results, baselines in [
                ("IS", (to_utc(is_s), to_utc(is_e)), is_results, is_baselines),
                ("OOS", (to_utc(oos_s), to_utc(oos_e)), oos_results, oos_baselines),
            ]:
                ticks = slice_window(full_ticks, "ts", s, e)
                m1 = slice_window(full_m1, "ts", s, e)
                m5 = slice_window(full_m5, "ts", s, e)
                t_arr = ts_arr_from_ticks(ticks)
                results[label] = {}
                baselines[label] = {}
                for stream in all_streams:
                    base_deals, sl_ev = run_baseline_window(
                        stream, ticks, m1, m5, meta, PARENT_RISK_SWEEP
                    )
                    baselines[label][stream] = base_deals
                    results[label][stream] = {}
                    for exp_min in EXPIRES_MIN:
                        for f1 in F1_CUTOFFS_SEC:
                            for tp_mult in TP_MULTS:
                                hcfg = RetryHedgeCfg(exp_min=exp_min, f1_sec=f1,
                                                     buf_pts=0, tp_mult=tp_mult)
                                h_deals = simulate_retry_hedges(
                                    sl_ev, t_arr, STREAM_CFGS[stream], hcfg
                                )
                                results[label][stream][(exp_min, f1, tp_mult)] = h_deals
                print(f"  {label} {tag} {s.date()}->{e.date()} done  "
                      f"streams={len(all_streams)}  [{time.time()-t_start:.0f}s]")

        # Build per-(exp, f1) grid: for each global candidate, pick per-stream best tp_mult
        # by maximizing stream-only NP across the 4 IS windows.
        print("\n  Selecting per-stream tp_mult for each global (exp, f1) candidate...")
        is_per = {label: [] for label, *_ in WINDOWS}
        oos_per = {label: [] for label, *_ in WINDOWS}
        grid_cfgs = []
        global_combos = [(e, f) for e in EXPIRES_MIN for f in F1_CUTOFFS_SEC]

        # For each global (exp, f1), choose best tp_mult per stream based on
        # SUM of IS NPs across all 4 windows. Then compute portfolio per-window.
        per_global_tp_choice = {}  # {(exp, f1): {stream: best_tp_mult}}
        for (exp_min, f1) in global_combos:
            tp_choice = {}
            for stream in all_streams:
                best_tp = None; best_total_np = -1e18
                for tp_mult in TP_MULTS:
                    total_np = 0.0
                    for label, *_ in WINDOWS:
                        base_deals = is_baselines[label][stream]
                        h_deals = is_results[label][stream][(exp_min, f1, tp_mult)]
                        np_, _, _ = aggregate(base_deals + h_deals)
                        total_np += np_
                    if total_np > best_total_np:
                        best_total_np = total_np
                        best_tp = tp_mult
                tp_choice[stream] = best_tp
            per_global_tp_choice[(exp_min, f1)] = tp_choice

            # Compute portfolio per window IS + OOS using these tp_mults
            cfg_label = f"exp={exp_min}_f1={f1}"
            grid_cfgs.append({"exp_min": exp_min, "f1_sec": f1, "tp_choice": tp_choice,
                              "label": cfg_label})

        # Build is_per_window / oos_per_window DataFrames as expected by rank_with_p0
        rows_is_by_label = {label: [] for label, *_ in WINDOWS}
        rows_oos_by_label = {label: [] for label, *_ in WINDOWS}
        for cfg in grid_cfgs:
            exp_min = cfg["exp_min"]; f1 = cfg["f1_sec"]; tp_choice = cfg["tp_choice"]
            for label, *_ in WINDOWS:
                portfolio_is = []; portfolio_oos = []
                for stream in all_streams:
                    tp = tp_choice[stream]
                    portfolio_is.extend(is_baselines[label][stream])
                    portfolio_is.extend(is_results[label][stream][(exp_min, f1, tp)])
                    portfolio_oos.extend(oos_baselines[label][stream])
                    portfolio_oos.extend(oos_results[label][stream][(exp_min, f1, tp)])
                np_is, dd_is, pf_is = aggregate(portfolio_is)
                np_oos, dd_oos, pf_oos = aggregate(portfolio_oos)
                rows_is_by_label[label].append({
                    "exp_min": exp_min, "f1_sec": f1,
                    "net_profit": np_is, "drawdown_pct": dd_is, "profit_factor": pf_is,
                })
                rows_oos_by_label[label].append({
                    "exp_min": exp_min, "f1_sec": f1,
                    "net_profit": np_oos, "drawdown_pct": dd_oos, "profit_factor": pf_oos,
                })

        is_per = {label: pd.DataFrame(rows_is_by_label[label]) for label, *_ in WINDOWS}
        oos_per = {label: pd.DataFrame(rows_oos_by_label[label]) for label, *_ in WINDOWS}

        # Rank using P0 framework. cfg is a simple namespace
        @dataclass(frozen=True)
        class GlobalCfg:
            exp_min: int
            f1_sec: int

        grid_for_rank = [GlobalCfg(exp_min=c["exp_min"], f1_sec=c["f1_sec"])
                         for c in grid_cfgs]

        ranked = rank_with_p0(grid_for_rank, oos_per, WINDOWS, decay_threshold=-0.25,
                              grid_configs=grid_for_rank, is_per_window=is_per)
        print_phase_d_with_p0(ranked[:10], f"global retry-hedge", decay_threshold=-0.25)
        winner = select_winner_with_p0(ranked) or ranked[0]
        flagged = check_winner_boundaries(winner["cfg"], grid_for_rank)
        print_boundary_check(flagged)

        # Resolve winner's per-stream tp_choice
        w_exp = winner["cfg"].exp_min; w_f1 = winner["cfg"].f1_sec
        w_tp_choice = per_global_tp_choice[(w_exp, w_f1)]
        print(f"\n  WINNER: exp_min={w_exp} f1_sec={w_f1}")
        print(f"  Per-stream tp_mult:")
        for stream in all_streams:
            print(f"    {stream}: tp_mult={w_tp_choice[stream]}")

        # Save winner JSON
        out_dir = ROOT / "output" / "wfo_hedge_retry_global_may9"
        out_dir.mkdir(parents=True, exist_ok=True)
        wj = {"expire_minutes": int(w_exp),
              "max_seconds_after_entry": int(w_f1),
              "buffer_pts": 0,
              "per_stream_tp_mult": {s: float(w_tp_choice[s]) for s in all_streams}}
        (out_dir / "winner.json").write_text(json.dumps(wj, indent=2))
        print(f"  Persisted: {out_dir / 'winner.json'}")

        # Portfolio compare at production sizing (1.5% per stream = 9% total)
        print("\n" + "=" * 110)
        print(f"  PORTFOLIO COMPARISON  Feb 21 -> May 9 (77d, $10k, 1.5% per stream)")
        print("=" * 110)
        # Re-run baselines at PARENT_RISK_PROD
        ticks_full = slice_window(full_ticks, "ts", start, end)
        m1_full = slice_window(full_m1, "ts", start, end)
        m5_full = slice_window(full_m5, "ts", start, end)
        t_arr_full = ts_arr_from_ticks(ticks_full)
        all_base = []; all_w = []; per_stream_summary = {}
        for stream in all_streams:
            base_deals, sl_ev = run_baseline_window(
                stream, ticks_full, m1_full, m5_full, meta, PARENT_RISK_PROD
            )
            tp = w_tp_choice[stream]
            hcfg = RetryHedgeCfg(exp_min=w_exp, f1_sec=w_f1, buf_pts=0, tp_mult=tp)
            h_deals = simulate_retry_hedges(sl_ev, t_arr_full, STREAM_CFGS[stream], hcfg)
            all_base.extend(base_deals)
            all_w.extend(base_deals + h_deals)
            wr = sum(1 for _, p in h_deals if p > 0) / len(h_deals) * 100 if h_deals else 0
            per_stream_summary[stream] = {
                "parent_np": sum(p for _, p in base_deals),
                "parent_n": len(base_deals),
                "hedge_np": sum(p for _, p in h_deals),
                "hedge_n": len(h_deals),
                "hedge_wr": wr,
                "tp_mult": tp,
            }
        np_b, dd_b, pf_b = aggregate(all_base)
        ndd_b = (np_b / (dd_b/100 * (DEPOSIT + np_b))) if dd_b > 0 else 0
        np_w, dd_w, pf_w = aggregate(all_w)
        ndd_w = (np_w / (dd_w/100 * (DEPOSIT + np_w))) if dd_w > 0 else 0

        print(f"\n  {'Variant':<14} {'NP':>10} {'DD%':>6} {'NP/DD$':>7} {'PF':>5} {'Trades':>7}")
        print(f"  {'no-retry':<14} ${np_b:>+8,.0f} {dd_b:>5.2f}% {ndd_b:>7.2f} {pf_b:>5.2f} {len(all_base):>7}")
        print(f"  {'+retry':<14} ${np_w:>+8,.0f} {dd_w:>5.2f}% {ndd_w:>7.2f} {pf_w:>5.2f} {len(all_w):>7}")
        d_np = np_w - np_b; d_dd = dd_w - dd_b; d_ndd = ndd_w - ndd_b
        print(f"  {'delta':<14} ${d_np:>+8,.0f} {d_dd:>+5.1f}p {d_ndd:>+7.2f}")

        print(f"\n  Per-stream retry contribution (global exp={w_exp}, f1={w_f1}):")
        print(f"  {'Stream':<6}  {'tp_mult':<7} {'parent_NP':>10} {'parent_n':>8} {'retry_NP':>10} {'retry_n':>8} {'retry_W':>7}")
        for s in all_streams:
            ps = per_stream_summary[s]
            print(f"  {s:<6}  {ps['tp_mult']:<7} ${ps['parent_np']:>+8,.0f} {ps['parent_n']:>8} "
                  f"${ps['hedge_np']:>+8,.0f} {ps['hedge_n']:>8} {ps['hedge_wr']:>6.0f}%")

    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
