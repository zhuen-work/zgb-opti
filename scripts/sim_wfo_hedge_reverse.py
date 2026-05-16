"""Reverse-hedge WFO — sim for DT818_pro_v3.mq5.

Hedge logic: when parent SL hits, place an OPPOSITE-direction LIMIT pending at
the parent's ORIGINAL entry price, with mirrored SL/TP geometry.
  Parent BUY  SL → SELL_LIMIT at entry, hedge SL above, TP below
  Parent SELL SL → BUY_LIMIT  at entry, hedge SL below, TP above

Joint optimization (global exp_min + F1 + regime_gate, per-stream tp_mult):
  Dim                    Grid                       Type
  tp_mult                [0.5, 0.75, 1.0, 1.25, 1.5]   PER-STREAM
  expire_minutes         [240, 480, 720, 1440]         GLOBAL
  max_seconds_after_entry F1  [0, 1800, 3600, 7200]     GLOBAL
  regime_gate            [off, TIGHT_NORMAL, TIGHT]    GLOBAL

  Cells per stream = 5 tp_mults × 48 globals = 240
  Globals (exp × F1 × regime) = 4 × 4 × 3 = 48
  Total stream sims per window = 240 × 6 = 1440

For each global candidate, pick each stream's best tp_mult by IS NP sum,
then rank globals by portfolio NP/DD$ with rank_with_p0.

Usage:
  python scripts/sim_wfo_hedge_reverse.py --windows may9   # MAY9 retro
  python scripts/sim_wfo_hedge_reverse.py --windows may16  # Sat 2026-05-16 reopt
"""
from __future__ import annotations

import sys
import time
import json
import argparse
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
from zgb_sim.wfo_helpers import (WINDOWS_MAY9, WINDOWS_MAY16, rank_with_p0,
                                  print_phase_d_with_p0, select_winner_with_p0,
                                  check_winner_boundaries, print_boundary_check, to_utc)
from zgb_sim.regime import (
    build_h1_bars, compute_atr, classify_regime, POINT as REGIME_POINT,
    SESSION_CFG, BROKER_OFFSET_H,
)

from sim_wfo_hedge_retry import (STREAM_CFGS, make_stream_cfg,
                                  slice_window, aggregate, run_baseline_window,
                                  ts_arr_from_ticks,
                                  SYMBOL, DEPOSIT, SPREAD, POINT, CONTRACT,
                                  PARENT_RISK_SWEEP, PARENT_RISK_PROD)


# ==== Grid (per spec 2026-05-14) ====
TP_MULTS = [0.5, 0.75, 1.0, 1.25, 1.5]        # 5 (per-stream)
EXPIRES_MIN = [240, 480, 720, 1440]            # 4 (global)
F1_CUTOFFS_SEC = [0, 1800, 3600, 7200]         # 4 (global): 0=off, 30/60/120 min cap
REGIME_GATES = ["off", "TIGHT_NORMAL", "TIGHT_only"]  # 3 (global)


@dataclass(frozen=True)
class ReverseHedgeCfg:
    exp_min: int            # expire_minutes
    f1_sec: int             # F1 filter (0 = disabled)
    regime_gate: str        # "off" | "TIGHT_NORMAL" | "TIGHT_only"
    tp_mult: float          # mirrored TP distance multiplier on sl_dist


@dataclass(frozen=True)
class GlobalCfg:
    """Used by rank_with_p0 — global params shared across streams."""
    exp_min: int
    f1_sec: int
    regime_gate: str


def tag_session_regimes(ticks: pd.DataFrame, m1: pd.DataFrame) -> dict:
    """Compute regime for each (date, session) in the tick window.

    Returns dict {(date_iso, "LDN" | "NY"): regime_label}.

    Tick timestamps are broker-time-labeled-as-UTC. We compute range_pts +
    ATR in that label space (the broker offset is constant so ratios are
    unaffected).
    """
    if ticks.empty:
        return {}
    df = ticks[["ts"]].copy()
    df["mid"] = (ticks["bid"] + ticks["ask"]) / 2.0
    h1 = build_h1_bars(df.assign(mid=df["mid"]).rename(columns={}))
    atr_series = compute_atr(h1)

    out = {}
    # Walk all unique broker dates in the tick window.
    start_date = ticks["ts"].iloc[0].date()
    end_date = ticks["ts"].iloc[-1].date()
    cur = start_date
    while cur <= end_date:
        for session in ("LDN", "NY"):
            cfg = SESSION_CFG[session]
            start_broker_h = cfg["start_h_real_utc"] + BROKER_OFFSET_H
            base = pd.Timestamp(cur).tz_localize("UTC")
            rng_start = base.replace(hour=start_broker_h, minute=0)
            rng_end = rng_start + pd.Timedelta(minutes=cfg["range_min"])
            slc = df[(df["ts"] >= rng_start) & (df["ts"] < rng_end)]
            if slc.empty:
                continue
            range_pts = (slc["mid"].max() - slc["mid"].min()) / REGIME_POINT
            atr_pts = float("nan")
            if not atr_series.empty:
                prior = atr_series.loc[atr_series.index < rng_start]
                if not prior.empty:
                    atr_pts = float(prior.iloc[-1]) / REGIME_POINT
            ratio = range_pts / atr_pts if (atr_pts and atr_pts > 0 and not np.isnan(atr_pts)) else float("nan")
            regime = classify_regime(range_pts, ratio)
            out[(cur.isoformat(), session)] = regime
        cur = (pd.Timestamp(cur) + pd.Timedelta(days=1)).date()
    return out


def event_session(ts_ns: int) -> tuple[str, str]:
    """Map an SL event timestamp (broker-time-as-UTC ns) to (date_iso, session)."""
    t = pd.Timestamp(ts_ns, tz="UTC")
    h = t.hour
    if 7 <= h < 13:
        sess = "LDN"
    elif 13 <= h < 22:
        sess = "NY"
    else:
        sess = "OTHER"
    return t.date().isoformat(), sess


def regime_allowed(regime: str, gate: str) -> bool:
    """Returns True if hedge is allowed under the regime_gate policy."""
    if gate == "off":
        return True
    if gate == "TIGHT_NORMAL":
        return regime in ("TIGHT", "NORMAL")
    if gate == "TIGHT_only":
        return regime == "TIGHT"
    return True


def simulate_reverse_hedges(sl_events, ticks_arr, stream_cfg: dict,
                              hcfg: ReverseHedgeCfg,
                              regime_by_session: dict) -> list:
    """Post-process reverse-hedge for one window. Returns list of (exit_ts_ns, pnl).

    For each parent SL event, places an OPPOSITE-direction LIMIT pending at
    the parent's original entry price, with mirrored SL/TP.

    Filters:
      - F1: skip if (sl_ts - entry_ts) > f1_sec
      - regime_gate: skip if session regime not allowed under policy
    """
    ts_arr = ticks_arr["ts_ns"]; bid = ticks_arr["bid"]; ask = ticks_arr["ask"]
    expire_ns = int(hcfg.exp_min * 60 * 1_000_000_000)
    walk_max_ns = int(3 * 24 * 3600 * 1_000_000_000)
    sl_dist_price = stream_cfg["fixed_sl_pts"] * POINT
    tp_dist_price = sl_dist_price * hcfg.tp_mult
    out = []

    for ev in sl_events:
        sl_ts = ev["ts_ns"]
        direction = ev["direction"]
        entry_price = ev["entry_price"]
        lots = ev["lots"]

        # F1 filter
        if hcfg.f1_sec > 0:
            elapsed_s = (sl_ts - ev["entry_ts_ns"]) / 1_000_000_000
            if elapsed_s > hcfg.f1_sec:
                continue

        # Regime gate
        if hcfg.regime_gate != "off":
            date_iso, sess = event_session(sl_ts)
            regime = regime_by_session.get((date_iso, sess), "UNKNOWN")
            if not regime_allowed(regime, hcfg.regime_gate):
                continue

        # Reverse hedge geometry (mirrored)
        if direction == 1:    # parent BUY → reverse SELL_LIMIT at entry
            hedge_dir = -1
            hedge_entry = entry_price
            hedge_sl = entry_price + sl_dist_price       # above (SELL SL)
            hedge_tp = entry_price - tp_dist_price       # below (SELL TP)
        else:                  # parent SELL → reverse BUY_LIMIT at entry
            hedge_dir = 1
            hedge_entry = entry_price
            hedge_sl = entry_price - sl_dist_price       # below (BUY SL)
            hedge_tp = entry_price + tp_dist_price       # above (BUY TP)

        # Pending lifecycle: from sl_ts to sl_ts + expire_ns
        i0 = np.searchsorted(ts_arr, sl_ts)
        i1 = np.searchsorted(ts_arr, sl_ts + expire_ns)
        if i1 <= i0:
            continue

        # LIMIT fire triggers:
        #   SELL_LIMIT: bid >= entry (price rose to entry from below)
        #   BUY_LIMIT:  ask <= entry (price dropped to entry from above)
        if hedge_dir == -1:  # SELL_LIMIT
            hits = np.where(bid[i0:i1] >= hedge_entry)[0]
        else:                 # BUY_LIMIT
            hits = np.where(ask[i0:i1] <= hedge_entry)[0]
        if len(hits) == 0:
            continue
        ent_idx = i0 + hits[0]
        ent_ts = ts_arr[ent_idx]
        i_end = np.searchsorted(ts_arr, ent_ts + walk_max_ns)
        post_bid = bid[ent_idx + 1: i_end]
        post_ask = ask[ent_idx + 1: i_end]
        if len(post_bid) == 0:
            continue

        # Position lifecycle (hedge_dir = -1 SELL, +1 BUY):
        #   SELL position: SL when ask >= hedge_sl, TP when bid <= hedge_tp
        #   BUY position:  SL when bid <= hedge_sl, TP when ask >= hedge_tp
        if hedge_dir == -1:
            sl_h = np.where(post_ask >= hedge_sl)[0]
            tp_h = np.where(post_bid <= hedge_tp)[0]
        else:
            sl_h = np.where(post_bid <= hedge_sl)[0]
            tp_h = np.where(post_ask >= hedge_tp)[0]
        sl_first = sl_h[0] if len(sl_h) else 10**18
        tp_first = tp_h[0] if len(tp_h) else 10**18
        if sl_first == 10**18 and tp_first == 10**18:
            continue
        if sl_first <= tp_first:
            ex_px = hedge_sl; ex_idx = ent_idx + 1 + sl_first
        else:
            ex_px = hedge_tp; ex_idx = ent_idx + 1 + tp_first
        # PnL is hedge_dir * (exit - entry) * contract * lots
        pnl = hedge_dir * (ex_px - hedge_entry) * CONTRACT * lots
        out.append((int(ts_arr[ex_idx]), float(pnl)))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--windows", default="may9", choices=["may9", "may16"],
                    help="WFO window set (may9 for retro validation, may16 for Sat reopt)")
    args = ap.parse_args()
    WINDOWS = WINDOWS_MAY9 if args.windows == "may9" else WINDOWS_MAY16
    win_tag = args.windows

    n_globals = len(EXPIRES_MIN) * len(F1_CUTOFFS_SEC) * len(REGIME_GATES)
    print("=" * 110)
    print(f"  REVERSE-HEDGE WFO (global exp_min/f1_sec/regime_gate, per-stream tp_mult)")
    print(f"  Windows: {win_tag.upper()} ({len(WINDOWS)} folds, IS+OOS each)")
    print(f"  Grid: tp_mult({len(TP_MULTS)}) per-stream  ×  "
          f"exp({len(EXPIRES_MIN)}) × f1({len(F1_CUTOFFS_SEC)}) × gate({len(REGIME_GATES)}) global")
    print(f"  Globals = {n_globals}.  Per-stream sims per window = {n_globals*len(TP_MULTS)*6}")
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
        print(f"\n  Data {start.date()}->{end.date()}: ticks={len(full_ticks):,} "
              f"M1={len(full_m1):,} M5={len(full_m5):,}")

        t_start = time.time()
        # per-window per-stream: baselines + hedge cells keyed by ReverseHedgeCfg
        is_results = {}; oos_results = {}
        is_baselines = {}; oos_baselines = {}
        is_regimes = {}; oos_regimes = {}
        all_streams = list(STREAM_CFGS.keys())

        for label, is_s, is_e, oos_s, oos_e in WINDOWS:
            for tag, (s, e), results, baselines, regimes in [
                ("IS",  (to_utc(is_s),  to_utc(is_e)),  is_results,  is_baselines,  is_regimes),
                ("OOS", (to_utc(oos_s), to_utc(oos_e)), oos_results, oos_baselines, oos_regimes),
            ]:
                ticks = slice_window(full_ticks, "ts", s, e)
                m1 = slice_window(full_m1, "ts", s, e)
                m5 = slice_window(full_m5, "ts", s, e)
                t_arr = ts_arr_from_ticks(ticks)
                # Per-window regime tags (LDN + NY for each date)
                regime_map = tag_session_regimes(ticks, m1)
                regimes[label] = regime_map
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
                            for gate in REGIME_GATES:
                                for tp_mult in TP_MULTS:
                                    hcfg = ReverseHedgeCfg(
                                        exp_min=exp_min, f1_sec=f1,
                                        regime_gate=gate, tp_mult=tp_mult,
                                    )
                                    h_deals = simulate_reverse_hedges(
                                        sl_ev, t_arr, STREAM_CFGS[stream],
                                        hcfg, regime_map,
                                    )
                                    results[label][stream][(exp_min, f1, gate, tp_mult)] = h_deals
                print(f"  {label} {tag} {s.date()}->{e.date()} done streams={len(all_streams)}  "
                      f"[{time.time()-t_start:.0f}s]")

        # For each global candidate, pick per-stream best tp_mult by IS NP sum.
        print("\n  Choosing per-stream tp_mult for each global (exp, f1, gate)...")
        global_combos = [(e, f, g) for e in EXPIRES_MIN
                         for f in F1_CUTOFFS_SEC
                         for g in REGIME_GATES]
        per_global_tp = {}
        for (exp_min, f1, gate) in global_combos:
            tp_choice = {}
            for stream in all_streams:
                best_tp = None; best_total_np = -1e18
                for tp_mult in TP_MULTS:
                    total_np = 0.0
                    for label, *_ in WINDOWS:
                        base_deals = is_baselines[label][stream]
                        h_deals = is_results[label][stream][(exp_min, f1, gate, tp_mult)]
                        np_, _, _ = aggregate(base_deals + h_deals)
                        total_np += np_
                    if total_np > best_total_np:
                        best_total_np = total_np
                        best_tp = tp_mult
                tp_choice[stream] = best_tp
            per_global_tp[(exp_min, f1, gate)] = tp_choice

        # Build per-window portfolio NP/DD using selected tp_mults, ready for rank_with_p0.
        grid_for_rank = [GlobalCfg(exp_min=e, f1_sec=f, regime_gate=g)
                         for (e, f, g) in global_combos]
        rows_is = {label: [] for label, *_ in WINDOWS}
        rows_oos = {label: [] for label, *_ in WINDOWS}
        for (exp_min, f1, gate) in global_combos:
            tp_choice = per_global_tp[(exp_min, f1, gate)]
            for label, *_ in WINDOWS:
                p_is = []; p_oos = []
                for stream in all_streams:
                    tp = tp_choice[stream]
                    p_is.extend(is_baselines[label][stream])
                    p_is.extend(is_results[label][stream][(exp_min, f1, gate, tp)])
                    p_oos.extend(oos_baselines[label][stream])
                    p_oos.extend(oos_results[label][stream][(exp_min, f1, gate, tp)])
                np_is, dd_is, pf_is = aggregate(p_is)
                np_oos, dd_oos, pf_oos = aggregate(p_oos)
                rows_is[label].append({"exp_min": exp_min, "f1_sec": f1, "regime_gate": gate,
                                          "net_profit": np_is, "drawdown_pct": dd_is, "profit_factor": pf_is})
                rows_oos[label].append({"exp_min": exp_min, "f1_sec": f1, "regime_gate": gate,
                                          "net_profit": np_oos, "drawdown_pct": dd_oos, "profit_factor": pf_oos})

        is_per = {label: pd.DataFrame(rows_is[label]) for label, *_ in WINDOWS}
        oos_per = {label: pd.DataFrame(rows_oos[label]) for label, *_ in WINDOWS}

        ranked = rank_with_p0(grid_for_rank, oos_per, WINDOWS, decay_threshold=-0.25,
                              grid_configs=grid_for_rank, is_per_window=is_per)
        print_phase_d_with_p0(ranked[:15], f"global reverse-hedge ({win_tag.upper()})",
                                decay_threshold=-0.25)
        winner = select_winner_with_p0(ranked) or ranked[0]
        flagged = check_winner_boundaries(winner["cfg"], grid_for_rank)
        print_boundary_check(flagged)

        w_exp = winner["cfg"].exp_min; w_f1 = winner["cfg"].f1_sec; w_gate = winner["cfg"].regime_gate
        w_tp = per_global_tp[(w_exp, w_f1, w_gate)]
        print(f"\n  WINNER: exp_min={w_exp}  f1_sec={w_f1}  regime_gate={w_gate}")
        print(f"  Per-stream tp_mult:")
        for s in all_streams:
            print(f"    {s}: tp_mult={w_tp[s]}")

        out_dir = ROOT / "output" / f"wfo_hedge_reverse_{win_tag}"
        out_dir.mkdir(parents=True, exist_ok=True)
        wj = {"expire_minutes": int(w_exp),
              "max_seconds_after_entry": int(w_f1),
              "regime_gate": w_gate,
              "per_stream_tp_mult": {s: float(w_tp[s]) for s in all_streams}}
        (out_dir / "winner.json").write_text(json.dumps(wj, indent=2))
        print(f"  Persisted: {out_dir / 'winner.json'}")

        # Portfolio compare at PROD risk (1.5% per stream = 9% total)
        print("\n" + "=" * 110)
        print(f"  PORTFOLIO COMPARE  ({win_tag.upper()})  $10k deposit, 1.5% per stream")
        print("=" * 110)
        ticks_full = slice_window(full_ticks, "ts", start, end)
        m1_full = slice_window(full_m1, "ts", start, end)
        m5_full = slice_window(full_m5, "ts", start, end)
        t_arr_full = ts_arr_from_ticks(ticks_full)
        regime_map_full = tag_session_regimes(ticks_full, m1_full)

        all_base = []; all_w = []; per_stream_summary = {}
        for stream in all_streams:
            base_deals, sl_ev = run_baseline_window(
                stream, ticks_full, m1_full, m5_full, meta, PARENT_RISK_PROD
            )
            tp = w_tp[stream]
            hcfg = ReverseHedgeCfg(exp_min=w_exp, f1_sec=w_f1, regime_gate=w_gate, tp_mult=tp)
            h_deals = simulate_reverse_hedges(sl_ev, t_arr_full,
                                                STREAM_CFGS[stream], hcfg, regime_map_full)
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
        print(f"  {'no-reverse':<14} ${np_b:>+8,.0f} {dd_b:>5.2f}% {ndd_b:>7.2f} {pf_b:>5.2f} {len(all_base):>7}")
        print(f"  {'+reverse':<14} ${np_w:>+8,.0f} {dd_w:>5.2f}% {ndd_w:>7.2f} {pf_w:>5.2f} {len(all_w):>7}")
        d_np = np_w - np_b; d_dd = dd_w - dd_b; d_ndd = ndd_w - ndd_b
        print(f"  {'delta':<14} ${d_np:>+8,.0f} {d_dd:>+5.1f}p {d_ndd:>+7.2f}")

        print(f"\n  Per-stream reverse-hedge contribution "
              f"(global exp={w_exp} f1={w_f1} gate={w_gate}):")
        print(f"  {'Stream':<6}  {'tp_mult':<7} {'parent_NP':>10} {'parent_n':>8} "
              f"{'rev_NP':>10} {'rev_n':>6} {'rev_W':>6}")
        for s in all_streams:
            ps = per_stream_summary[s]
            print(f"  {s:<6}  {ps['tp_mult']:<7} ${ps['parent_np']:>+8,.0f} {ps['parent_n']:>8} "
                  f"${ps['hedge_np']:>+8,.0f} {ps['hedge_n']:>6} {ps['hedge_wr']:>5.0f}%")

    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
