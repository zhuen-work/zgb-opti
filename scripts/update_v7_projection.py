"""Update forward_projection.json + console for v7 (parents + SMA cross-exit + STOP-ext hedge).

Mirrors update_v6_projection.py structure but:
  - Reads v7 setfile (dt818_pro_v7_{label}pct_may30_may23.set) directly
  - Propagates _ORB_FractalConfirm, _ORB_SMA_CrossExit, fast/slow periods into
    each parent stream's ORBConfig (so simulate_fast applies them)
  - Adds STOP-on-extension hedge layer on top of each parent stream's SL events
    (unchanged from v6: ext_pts=100, tp_mult=3.0, sl_mult=1.0)

Usage:
  python scripts/update_v7_projection.py             # update + push
  python scripts/update_v7_projection.py --no-push   # update only
  python scripts/update_v7_projection.py --dry-run   # print, don't write
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

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.cf_publish import _load_dotenv, publish_projection
from sim_wfo_hedge_retry import STREAM_CFGS, ts_arr_from_ticks
from sim_wfo_hedge_reverse import StopExtensionCfg, simulate_stop_extension_hedges
from compare_v6_v7_portfolio import (parse_setfile, build_cfg_for_stream,
                                      get_bool, get_float, get_int)

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
SPREAD = 30
SIM_START = datetime(2026, 2, 14, tzinfo=timezone.utc)
SIM_END   = datetime(2026, 5, 23, tzinfo=timezone.utc)
PROJECTION_PATH = ROOT / "output" / "forward_projection.json"
HAIRCUT_NP = 0.94

# v7 STOP-ext hedge (unchanged from v6)
STOPEXT_CFG = StopExtensionCfg(exp_min=240, f1_sec=1800, ext_pts=100, tp_mult=3.0, sl_mult=1.0)


def extract_sl_events_fmt(deals):
    open_pos = []
    out = []
    for d in deals:
        ts_ns = pd.Timestamp(d.ts).value
        if "entry" in str(d.kind).lower():
            open_pos.append({"ts_ns": ts_ns, "direction": int(d.direction),
                              "entry_price": float(d.price), "lots": float(d.lots)})
            continue
        m = -1
        for i, op in enumerate(open_pos):
            if op["direction"] == int(d.direction):
                m = i; break
        if m < 0: continue
        op = open_pos.pop(m)
        if d.pnl < 0:
            out.append({"ts_ns": ts_ns, "direction": op["direction"],
                        "entry_price": op["entry_price"],
                        "entry_ts_ns": op["ts_ns"], "lots": op["lots"]})
    return out


def slope_to_decay(avg_slope_pct: float) -> float:
    if avg_slope_pct >= -10: return 0.90
    if avg_slope_pct >= -30: return 0.80
    if avg_slope_pct >= -50: return 0.75
    return 0.65


def compute_per_stream_slope(weekly_nps: list[float]) -> float:
    n = len(weekly_nps)
    if n < 4:
        return 0.0
    half = n // 2
    first = float(np.mean(weekly_nps[:half]))
    second = float(np.mean(weekly_nps[half:]))
    if abs(first) < 1e-9:
        return 0.0
    return (second - first) / abs(first) * 100


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-push", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--per-stream-risk", type=float, default=1.5)
    args = ap.parse_args()
    _load_dotenv()

    PER_STREAM_RISK = args.per_stream_risk
    setfile_label = f"{int(PER_STREAM_RISK*6)}pct"
    setfile_name = f"dt818_pro_v7_{setfile_label}_may30_may23.set"
    setfile_path = ROOT / "configs" / "sets" / setfile_name

    print(f"=== v7 projection update | sim {SIM_START.date()} -> {SIM_END.date()} "
          f"| risk={PER_STREAM_RISK}%/stream ({int(PER_STREAM_RISK*6)}% total) | {SPREAD}pt ===")
    print(f"  Setfile: {setfile_path.name}")

    # === Load existing projection (for baseline_balance) ===
    existing = json.loads(PROJECTION_PATH.read_text())
    live_balance = existing.get("baseline_balance", 71539.25)
    print(f"  Live baseline: ${live_balance:,.2f}")

    # Parse v7 setfile globals (cross-exit flags etc.)
    sf = parse_setfile(setfile_path)
    fractal_confirm = get_bool(sf, "_ORB_FractalConfirm")
    fractal_width = get_int(sf, "_ORB_FractalWidth", 5)
    sma_cross_on = get_bool(sf, "_ORB_SMA_CrossExit")
    sma_fast = get_int(sf, "_ORB_SMA_FastPeriod", 8)
    sma_slow = get_int(sf, "_ORB_SMA_SlowPeriod", 21)
    print(f"  Globals: FractalConfirm={fractal_confirm}(w{fractal_width})  "
          f"SMA_CrossExit={sma_cross_on}({sma_fast}/{sma_slow})")

    # === Tick + bar load ===
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        ticks = load_ticks(SYMBOL, SIM_START, SIM_END, spread_pts=SPREAD)
        m1 = load_bars(SYMBOL, "M1", SIM_START, SIM_END)
        m5 = load_bars(SYMBOL, "M5", SIM_START, SIM_END)
    finally:
        kill_mt5_terminal()

    per_stream_deals = {}
    per_stream_trades = {}
    per_stream_np = {}
    per_stream_parent_np = {}
    per_stream_hedge_np = {}
    per_stream_hedge_n = {}
    per_stream_hedge_wr = {}
    per_stream_params = {}
    ticks_arr = ts_arr_from_ticks(ticks)

    for sn in range(1, 7):
        s = f"S{sn}"
        cfg = build_cfg_for_stream(sf, sn, PER_STREAM_RISK)
        per_stream_params[s] = dict(
            range_minutes=cfg.range_minutes, fixed_sl_pts=cfg.fixed_sl_pts,
            rr_ratio=cfg.rr_ratio, half_tp_ratio=cfg.half_tp_ratio,
            pending_expire_minutes=cfg.pending_expire_minutes,
        )
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        parent_deals = [(d.ts, d.pnl) for d in r.deals if str(d.kind).lower() != "entry"]
        parent_np = sum(p for _, p in parent_deals)

        # Hedge layer: STOP-ext on parent SL events. STREAM_CFGS entry used for
        # hedge dispatcher's parent-meta lookup (it only reads risk_pct etc.).
        sl_ev = extract_sl_events_fmt(r.deals)
        hedge_pnls_ns = simulate_stop_extension_hedges(sl_ev, ticks_arr,
                                                       STREAM_CFGS[s], STOPEXT_CFG)
        hedge_deals = [(pd.Timestamp(ts_ns), pnl) for ts_ns, pnl in hedge_pnls_ns]
        hedge_np = sum(p for _, p in hedge_deals)
        hedge_n = len(hedge_deals)
        hedge_wins = sum(1 for _, p in hedge_deals if p > 0)
        merged = parent_deals + hedge_deals
        per_stream_deals[s] = merged
        per_stream_trades[s] = len(merged)
        per_stream_np[s] = sum(p for _, p in merged)
        per_stream_parent_np[s] = parent_np
        per_stream_hedge_np[s] = hedge_np
        per_stream_hedge_n[s] = hedge_n
        per_stream_hedge_wr[s] = round(hedge_wins / hedge_n * 100, 1) if hedge_n else 0.0
        print(f"  {s}: parent ${parent_np:+,.0f}  hedge ${hedge_np:+,.0f}  "
              f"merged ${per_stream_np[s]:+,.0f}  (parent_trades={len(parent_deals)} "
              f"hedge_n={hedge_n} hedge_wr={per_stream_hedge_wr[s]:.1f}%)")

    # Portfolio aggregate
    all_deals = sorted([(ts, s, p) for s, ds in per_stream_deals.items() for ts, p in ds],
                       key=lambda x: x[0])
    portfolio_np = sum(p for _, _, p in all_deals)
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gains = losses = 0.0; wins = 0
    for _, _, p in all_deals:
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p >= 0: gains += p; wins += 1
        else: losses += -p
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    pf = gains / losses if losses > 0 else float("inf")
    wr = wins / len(all_deals) * 100 if all_deals else 0
    ndd = portfolio_np / dd_abs if dd_abs > 0 else 0
    sim_days = (SIM_END - SIM_START).days
    sim_weeks = sim_days / 7
    print(f"  Full-period portfolio: NP=${portfolio_np:+,.0f} DD={dd_pct:.2f}% PF={pf:.2f} "
          f"NP/DD$={ndd:.2f} trades={len(all_deals)} WR={wr:.1f}%")

    # === Weekly NP distribution ===
    weekly_np_by_stream = {}
    for s in ("S1","S2","S3","S4","S5","S6"):
        week_buckets = {}
        for ts, p in per_stream_deals[s]:
            wk = pd.Timestamp(ts).to_period("W").start_time
            week_buckets.setdefault(wk, 0.0)
            week_buckets[wk] += p
        weekly_np_by_stream[s] = list(week_buckets.values())

    port_week_buckets = {}
    for ts, _s, p in all_deals:
        wk = pd.Timestamp(ts).to_period("W").start_time
        port_week_buckets.setdefault(wk, 0.0)
        port_week_buckets[wk] += p
    port_weekly = sorted(port_week_buckets.items())
    weekly_nps = [v for _, v in port_weekly]
    n_weeks = len(weekly_nps)
    print(f"  Sample weeks: {n_weeks}")

    # === Per-stream slopes (first half vs second half) ===
    per_stream_slopes = {f"ORB_{s}": compute_per_stream_slope(weekly_np_by_stream[s])
                         for s in ("S1","S2","S3","S4","S5","S6")}
    avg_slope_pct = float(np.mean(list(per_stream_slopes.values())))
    decay = slope_to_decay(avg_slope_pct)
    combined_haircut = HAIRCUT_NP * decay
    print(f"  avg OOS slope: {avg_slope_pct:.1f}%  ->  decay={decay}  combined={combined_haircut:.3f}")

    # === Live projection (Method B compound rate × live balance × combined haircut) ===
    if portfolio_np > -DEPOSIT and sim_weeks > 0:
        compound_weekly_rate = (1 + portfolio_np / DEPOSIT) ** (1 / sim_weeks) - 1
    else:
        compound_weekly_rate = 0.0
    smean = compound_weekly_rate * live_balance * combined_haircut

    if weekly_nps and abs(np.mean(weekly_nps)) > 1e-9:
        sim_mean = float(np.mean(weekly_nps))
        sim_p10 = float(np.percentile(weekly_nps, 10))
        sim_p90 = float(np.percentile(weekly_nps, 90))
        sim_worst = float(min(weekly_nps))
        sim_best = float(max(weekly_nps))
        sim_median = float(np.median(weekly_nps))
        sim_std = float(np.std(weekly_nps, ddof=0))
        smedian = smean * (sim_median / sim_mean)
        sp10    = smean * (sim_p10    / sim_mean)
        sp90    = smean * (sim_p90    / sim_mean)
        sworst  = smean * (sim_worst  / sim_mean)
        sbest   = smean * (sim_best   / sim_mean)
        sstd    = abs(smean) * (sim_std / abs(sim_mean))
        green_prob = sum(1 for w in weekly_nps if w > 0) / len(weekly_nps)
    else:
        smedian = sstd = sp10 = sp90 = sworst = sbest = green_prob = 0

    weekly_live = {
        "mean_np": round(smean),
        "median_np": round(smedian),
        "p10_np": round(sp10),
        "p90_np": round(sp90),
        "worst_np": round(sworst),
        "best_np": round(sbest),
        "std_np": round(sstd),
        "green_week_prob": round(green_prob, 2),
        "expected_roi_pct": round(smean / live_balance * 100, 2),
        "n_sample_weeks": n_weeks,
        "view_c_planning_range_low": round(smean * 0.55),
        "view_c_planning_range_high": round(smean * 1.10),
        "view_c_tolerance_red_usd": round(smean * -0.74),
        "parent_contribution_mean_np": round(smean * sum(per_stream_parent_np.values()) / max(portfolio_np, 1)),
        "hedge_contribution_mean_np":  round(smean * sum(per_stream_hedge_np.values())  / max(portfolio_np, 1)),
        "hedge_share_pct": round(sum(per_stream_hedge_np.values()) / max(portfolio_np, 1) * 100, 1),
        "target_balance_floor": round(live_balance + sp10),
        "target_balance_mean": round(live_balance + smean),
        "target_balance_stretch": round(live_balance + sp90),
    }
    daily_live = {
        "mean_np": round(smean / 5),
        "p10_np": round(sp10 / 5),
        "p90_np": round(sp90 / 5),
        "expected_roi_pct": round(smean / 5 / live_balance * 100, 2),
        "parent_contribution_mean": round((smean / 5) * sum(per_stream_parent_np.values()) / max(portfolio_np, 1)),
        "hedge_contribution_mean":  round((smean / 5) * sum(per_stream_hedge_np.values())  / max(portfolio_np, 1)),
        "target_balance_floor": round(live_balance + sp10 / 5),
        "target_balance_mean": round(live_balance + smean / 5),
        "target_balance_stretch": round(live_balance + sp90 / 5),
    }
    monthly_compound_roi = ((1 + compound_weekly_rate * combined_haircut) ** 4 - 1) * 100
    target_floor_4w = live_balance * (1 + sp10 / live_balance) ** 4
    target_mean_4w = live_balance * (1 + smean / live_balance) ** 4
    target_stretch_4w = live_balance * (1 + sp90 / live_balance) ** 4
    monthly_live = {
        "horizon_weeks": 4,
        "compound_mean_roi_pct": round(monthly_compound_roi, 2),
        "target_balance_floor": round(target_floor_4w, 2),
        "target_balance_mean": round(target_mean_4w, 2),
        "target_balance_stretch": round(target_stretch_4w, 2),
    }

    bal_k_str = f"{int(round(live_balance / 1000))}k" if live_balance >= 100_000 else f"{int(round(live_balance))}usd"
    contrib_key = f"per_stream_sim_contribution_{sim_days}d_{bal_k_str}"
    per_stream_contrib = {s: {
        "parent_np": round(per_stream_parent_np[s], 0),
        "hedge_np": round(per_stream_hedge_np[s], 0),
        "combined": round(per_stream_np[s], 0),
        "hedge_n": per_stream_hedge_n[s],
        "hedge_wr": per_stream_hedge_wr[s],
        "params": per_stream_params[s],
    } for s in ("S1","S2","S3","S4","S5","S6")}

    # === Build updated projection ===
    proj = dict(existing)
    proj["setfile"] = setfile_name
    proj["method"] = "view_c_decay_adjusted_v7_smacross_plus_stopext"
    proj["deposit_sim"] = DEPOSIT
    proj["spread_pts"] = SPREAD
    proj["decay_factor"] = decay
    proj["live_haircut_np"] = HAIRCUT_NP
    proj["combined_haircut"] = combined_haircut
    proj["avg_oos_slope_pct"] = avg_slope_pct
    proj["per_stream_slopes_pct"] = per_stream_slopes
    proj["sanity_window"] = {
        "start": SIM_START.date().isoformat(),
        "end": SIM_END.date().isoformat(),
        "iso_weeks": n_weeks,
    }
    proj["raw_sim"] = {
        "portfolio_np": round(portfolio_np, 2),
        "dd_pct": round(dd_pct, 4),
        "dd_abs": round(dd_abs, 2),
        "ndd": round(ndd, 4),
        "pf": round(pf, 4),
        "trades": len(all_deals),
        "wr": round(wr, 4),
    }
    raw_key = f"raw_sim_{sim_days}d"
    parent_total = sum(per_stream_parent_np.values())
    hedge_total = sum(per_stream_hedge_np.values())
    proj[raw_key] = {
        "v7_combined_np": round(portfolio_np, 0),
        "v7_parent_np": round(parent_total, 0),
        "v7_hedge_np": round(hedge_total, 0),
        "v7_hedge_share_pct": round(hedge_total / max(portfolio_np, 1) * 100, 1),
        "v7_dd_pct": round(dd_pct, 2),
        "v7_ndd": round(ndd, 2),
        "v7_pf": round(pf, 2),
        "v7_trades": len(all_deals),
        "v7_hedge_n": sum(per_stream_hedge_n.values()),
        "compound_weekly_rate_v7": compound_weekly_rate,
    }
    for k in list(proj.keys()):
        if k.startswith("raw_sim_") and k != raw_key and k != "raw_sim":
            del proj[k]
    proj[contrib_key] = per_stream_contrib
    for k in list(proj.keys()):
        if k.startswith("per_stream_sim_contribution_") and k != contrib_key:
            del proj[k]
    proj["per_stream_hedge_cfg"] = {s: {
        "type": "stop-extension",
        "ext_pts": STOPEXT_CFG.ext_pts,
        "tp_mult": STOPEXT_CFG.tp_mult,
        "sl_mult": STOPEXT_CFG.sl_mult,
    } for s in ("S1","S2","S3","S4","S5","S6")}
    proj["global_hedge_cfg"] = {
        "type": "stop-extension",
        "expire_minutes": STOPEXT_CFG.exp_min,
        "max_seconds_after_entry": STOPEXT_CFG.f1_sec,
        "ext_pts": STOPEXT_CFG.ext_pts,
        "tp_mult": STOPEXT_CFG.tp_mult,
        "sl_mult": STOPEXT_CFG.sl_mult,
    }
    proj["parent_smacross_cfg"] = {
        "fractal_confirm": fractal_confirm,
        "fractal_width": fractal_width,
        "sma_cross_exit": sma_cross_on,
        "sma_fast_period": sma_fast,
        "sma_slow_period": sma_slow,
    }
    proj["ea"] = "DT818_pro_v7.mq5"
    proj["ea_version"] = (f"v7 = v6 parents + SMA({sma_fast},{sma_slow}) cross-exit "
                          f"on post-HTP runners (M5) + V2 fractal-confirm + STOP-ext hedge "
                          f"(WFO 2026-05-26 may23 windows, widened grid)")
    proj["weekly_live"] = weekly_live
    proj["daily_live"] = daily_live
    proj["monthly_live"] = monthly_live
    # Top-level mirrors for console flatten-whitelist
    proj["weekly_target_balance_floor"] = weekly_live["target_balance_floor"]
    proj["weekly_target_balance_mean"]  = weekly_live["target_balance_mean"]
    proj["weekly_target_balance_stretch"] = weekly_live["target_balance_stretch"]
    proj["daily_target_balance_floor"]  = daily_live["target_balance_floor"]
    proj["daily_target_balance_mean"]   = daily_live["target_balance_mean"]
    proj["daily_target_balance_stretch"] = daily_live["target_balance_stretch"]
    proj["monthly_target_balance_floor"] = monthly_live["target_balance_floor"]
    proj["monthly_target_balance_mean"]  = monthly_live["target_balance_mean"]
    proj["monthly_target_balance_stretch"] = monthly_live["target_balance_stretch"]
    proj["monthly_compound_mean_roi_pct"] = monthly_live["compound_mean_roi_pct"]
    proj["target_balance_floor"] = weekly_live["target_balance_floor"]
    proj["target_balance_mean"]  = weekly_live["target_balance_mean"]
    proj["target_balance_stretch"] = weekly_live["target_balance_stretch"]
    proj["tolerance"] = {
        "single_week_red_usd": round(smean - 2 * sstd, 2),
        "consecutive_red_weeks": 2,
        "daily_outlier_red_usd": round((smean - 2 * sstd) / 5, 2),
        "four_week_trailing_mean_floor": round(smean * 0.35, 2),
        "investigation_triggers": [
            "single week NP < single_week_red_usd",
            "2+ consecutive red weeks",
            "4-week trailing mean < four_week_trailing_mean_floor",
            "weekly ROI < 5% sustained for 3 weeks",
        ],
    }
    notes = proj.get("notes", [])
    notes.append(
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}: v7 setfile "
        f"(v6 parents widened-WFO + SMA({sma_fast},{sma_slow}) cross-exit + STOP-ext hedge). "
        f"Sim {sim_days}d at {PER_STREAM_RISK}%/stream {SPREAD}pt: "
        f"parent NP=${parent_total:+,.0f} + hedge NP=${hedge_total:+,.0f} = "
        f"combined ${portfolio_np:+,.0f} NP/DD$={ndd:.2f}. "
        f"Method: view_c_decay_adjusted_v7_smacross_plus_stopext."
    )
    proj["notes"] = notes[-10:]
    proj["wfo_log_source"] = str(ROOT / "output" / "wfo_orb_v7_smacross_may23" / "oos_rank.csv")
    proj["saved_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    if args.dry_run:
        print(f"\n--- DRY RUN: would write to {PROJECTION_PATH} ---")
        print(json.dumps(proj, indent=2)[:2500] + "\n...(truncated)")
        return 0

    PROJECTION_PATH.write_text(json.dumps(proj, indent=2), encoding="utf-8")
    print(f"\n  Wrote: {PROJECTION_PATH}")
    print(f"  setfile: {setfile_name}")
    prev_weekly = existing.get('weekly_live', {}).get('mean_np', '?')
    prev_monthly = existing.get('monthly_live', {}).get('compound_mean_roi_pct', '?')
    print(f"  weekly_mean_np: ${weekly_live['mean_np']:+,.0f}  (was ${prev_weekly})")
    print(f"  monthly_compound_mean_roi_pct: {monthly_live['compound_mean_roi_pct']:.2f}%  (was {prev_monthly}%)")
    print(f"  decay_factor: {decay} (was {existing.get('decay_factor','?')})")

    if not args.no_push:
        ok = publish_projection(proj)
        if ok:
            print(f"  Pushed to console: OK")
        else:
            print(f"  Push to console: FAILED (check CONSOLE_API_BASE + CONSOLE_INGEST_TOKEN)")
    else:
        print(f"  --no-push set; not publishing")
    return 0


if __name__ == "__main__":
    sys.exit(main())
