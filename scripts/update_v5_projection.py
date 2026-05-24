"""Update forward_projection.json + console for v5 (parents-only, no hedge).

Differs from update_v3_projection_from_wfo.py: that one parses hedge WFO logs.
v5 has NO hedge layer, so we compute directly from a full-period sim of the
current STREAM_CFGS (top-6 from latest WFO) and derive weekly stats.

Run order in pipeline:
  1. Latest WFO → STREAM_CFGS updated
  2. (Optional) hedge WFO → DISABLED in v5
  3. THIS script: full-period sim → weekly distribution → projection JSON
  4. cf_publish.publish_projection (unless --no-push)

Usage:
  python scripts/update_v5_projection.py             # update + push
  python scripts/update_v5_projection.py --no-push   # update only
  python scripts/update_v5_projection.py --dry-run   # print, don't write
"""
from __future__ import annotations

import argparse
import json
import sys
import statistics
from datetime import datetime, timezone, timedelta
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
from sim_wfo_hedge_retry import STREAM_CFGS, make_stream_cfg

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
SPREAD = 30                     # live-match per [[feedback_default_test_conditions]]
SIM_START = datetime(2026, 2, 14, tzinfo=timezone.utc)
SIM_END   = datetime(2026, 5, 23, tzinfo=timezone.utc)
PROJECTION_PATH = ROOT / "output" / "forward_projection.json"

HAIRCUT_NP = 0.94               # live-vs-sim haircut


def slope_to_decay(avg_slope_pct: float) -> float:
    if avg_slope_pct >= -10: return 0.90
    if avg_slope_pct >= -30: return 0.80
    if avg_slope_pct >= -50: return 0.75
    return 0.65


def compute_per_stream_slope(weekly_nps: list[float]) -> float:
    """Linear regression slope over weekly NPs, expressed as % of first-half mean."""
    n = len(weekly_nps)
    if n < 4:
        return 0.0
    half = n // 2
    first = np.mean(weekly_nps[:half])
    second = np.mean(weekly_nps[half:])
    if abs(first) < 1e-9:
        return 0.0
    return float((second - first) / abs(first) * 100)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-push", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--per-stream-risk", type=float, default=1.5,
                    help="1.5 = 9pct setfile (default, matches current live cadence)")
    args = ap.parse_args()
    _load_dotenv()

    PER_STREAM_RISK = args.per_stream_risk
    setfile_label = f"{int(PER_STREAM_RISK*6)}pct"
    setfile_name = f"dt818_pro_v5_{setfile_label}_may23_may16.set"

    print(f"=== v5 projection update | sim {SIM_START.date()} -> {SIM_END.date()} | risk={PER_STREAM_RISK}%/stream ({int(PER_STREAM_RISK*6)}% total) | {SPREAD}pt ===")

    # === Load existing projection (for baseline_balance) ===
    existing = json.loads(PROJECTION_PATH.read_text())
    live_balance = existing.get("baseline_balance", 71539.25)
    print(f"  Live baseline: ${live_balance:,.2f}")

    # === Run full-period sim ===
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

    per_stream_deals = {}    # stream -> list of (ts, pnl)
    per_stream_trades = {}
    per_stream_np = {}
    for stream in ("S1","S2","S3","S4","S5","S6"):
        cfg = make_stream_cfg(stream, PER_STREAM_RISK)
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        deals = [(d.ts, d.pnl) for d in r.deals if str(d.kind).lower() != "entry"]
        per_stream_deals[stream] = deals
        per_stream_trades[stream] = len(deals)
        per_stream_np[stream] = sum(p for _, p in deals)

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
    print(f"  Full-period portfolio: NP=${portfolio_np:+,.0f} DD={dd_pct:.2f}% PF={pf:.2f} NP/DD$={ndd:.2f} trades={len(all_deals)} WR={wr:.1f}%")

    # === Weekly NP distribution ===
    weekly_np_by_stream = {}
    for stream in ("S1","S2","S3","S4","S5","S6"):
        week_buckets = {}
        for ts, p in per_stream_deals[stream]:
            wk = pd.Timestamp(ts).to_period("W").start_time
            week_buckets.setdefault(wk, 0.0)
            week_buckets[wk] += p
        weekly_np_by_stream[stream] = list(week_buckets.values())

    # Portfolio weekly NPs
    port_week_buckets = {}
    for ts, _s, p in all_deals:
        wk = pd.Timestamp(ts).to_period("W").start_time
        port_week_buckets.setdefault(wk, 0.0)
        port_week_buckets[wk] += p
    port_weekly = sorted(port_week_buckets.items())
    weekly_nps = [v for _, v in port_weekly]
    n_weeks = len(weekly_nps)
    print(f"  Sample weeks: {n_weeks}")

    # === Per-stream OOS slopes (first-half vs second-half) ===
    per_stream_slopes = {}
    for stream in ("S1","S2","S3","S4","S5","S6"):
        per_stream_slopes[f"ORB_{stream}"] = compute_per_stream_slope(weekly_np_by_stream[stream])
    avg_slope_pct = float(np.mean(list(per_stream_slopes.values())))
    decay = slope_to_decay(avg_slope_pct)
    combined_haircut = HAIRCUT_NP * decay
    print(f"  avg OOS slope: {avg_slope_pct:.1f}%  ->  decay={decay}  combined={combined_haircut:.3f}")

    # === Live projections (Method B: compound rate × live balance × combined haircut) ===
    # Matches update_v3_projection_from_wfo.py methodology so v4/v5 numbers
    # are apples-to-apples. Sim NP is compounded — lots scale with running
    # balance. The compound weekly rate is the observed weekly growth rate
    # of the $10k sim base. Applied to live balance with haircut.
    if portfolio_np > -DEPOSIT and sim_weeks > 0:
        compound_weekly_rate = (1 + portfolio_np / DEPOSIT) ** (1 / sim_weeks) - 1
    else:
        compound_weekly_rate = 0.0
    smean = compound_weekly_rate * live_balance * combined_haircut

    # Distribution: anchor on smean, use ratios derived from the actual sim's
    # weekly NP distribution (relative to its own mean) so we preserve the
    # observed skew/spread while having a realistic mean.
    if weekly_nps and abs(np.mean(weekly_nps)) > 1e-9:
        sim_mean = float(np.mean(weekly_nps))
        sim_p10 = float(np.percentile(weekly_nps, 10))
        sim_p90 = float(np.percentile(weekly_nps, 90))
        sim_worst = float(min(weekly_nps))
        sim_best = float(max(weekly_nps))
        sim_median = float(np.median(weekly_nps))
        sim_std = float(np.std(weekly_nps, ddof=0))
        # Ratios of percentiles to mean (sign-preserving)
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
        "parent_contribution_mean_np": round(smean),      # all NP is parent (no hedge in v5)
        "hedge_contribution_mean_np": 0,
        "hedge_share_pct": 0.0,
        "target_balance_floor": round(live_balance + sp10),
        "target_balance_mean": round(live_balance + smean),
        "target_balance_stretch": round(live_balance + sp90),
    }
    daily_live = {
        "mean_np": round(smean / 5),
        "p10_np": round(sp10 / 5),
        "p90_np": round(sp90 / 5),
        "expected_roi_pct": round(smean / 5 / live_balance * 100, 2),
        "parent_contribution_mean": round(smean / 5),
        "hedge_contribution_mean": 0,
        "target_balance_floor": round(live_balance + sp10 / 5),
        "target_balance_mean": round(live_balance + smean / 5),
        "target_balance_stretch": round(live_balance + sp90 / 5),
    }
    # 4-week compound projection
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

    # === Per-stream sim contribution block ===
    bal_k_str = f"{int(round(live_balance / 1000))}k" if live_balance >= 100_000 else f"{int(round(live_balance))}usd"
    contrib_key = f"per_stream_sim_contribution_{sim_days}d_{bal_k_str}"
    per_stream_contrib = {}
    for s in ("S1","S2","S3","S4","S5","S6"):
        per_stream_contrib[s] = {
            "parent_np": round(per_stream_np[s], 0),
            "hedge_np": 0.0,
            "combined": round(per_stream_np[s], 0),
            "hedge_n": 0,
            "hedge_wr": 0.0,
        }

    # === Build updated projection ===
    proj = dict(existing)
    proj["setfile"] = setfile_name
    proj["method"] = "view_c_decay_adjusted_v5_parents_only"
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
    proj[raw_key] = {
        "v5_no_hedge_np": round(portfolio_np, 0),
        "v5_dd_pct": round(dd_pct, 2),
        "v5_ndd": round(ndd, 2),
        "v5_pf": round(pf, 2),
        "v5_trades": len(all_deals),
        "compound_weekly_rate_v5": compound_weekly_rate,
    }
    # Remove stale raw_sim_*d
    for k in list(proj.keys()):
        if k.startswith("raw_sim_") and k != raw_key and k != "raw_sim":
            del proj[k]
    proj[contrib_key] = per_stream_contrib
    # Remove stale contrib keys
    for k in list(proj.keys()):
        if k.startswith("per_stream_sim_contribution_") and k != contrib_key:
            del proj[k]
    proj.pop("per_stream_hedge_cfg", None)
    proj.pop("global_hedge_cfg", None)
    proj.pop("comparison_vs_v2_1", None)
    proj["ea"] = "DT818_pro_v5.mq5"
    proj["ea_version"] = "v5 V2 fractal-confirmed entry (no rotation; hedges disabled, MAY23 reopt 2026-05-24)"
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
    # Tolerance triggers (rescaled to v5 weekly distribution)
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
        f"2026-05-24: v5 setfile (no rotation, fractal_confirm=true, hedges DISABLED). "
        f"Parents from WFO expire-extend top-6 + rank#7 substitution for S6. "
        f"v5 hedge WFO failed decay-filter on recent tape; ship parents-only. "
        f"Projection sim {sim_days}d at {PER_STREAM_RISK}%/stream {SPREAD}pt: NP=${portfolio_np:+,.0f} NP/DD$={ndd:.2f}. "
        f"Method: view_c_decay_adjusted_v5_parents_only."
    )
    proj["notes"] = notes[-10:]   # cap notes at last 10
    proj["wfo_log_source"] = str(ROOT / "output" / "wfo_orb_v5_expire_extend" / "oos_rank.csv")
    # Refresh saved_at (use now + 1s to outrank any existing rows)
    proj["saved_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    if args.dry_run:
        print(f"\n--- DRY RUN: would write to {PROJECTION_PATH} ---")
        print(json.dumps(proj, indent=2)[:2000] + "\n...(truncated)")
        return 0

    PROJECTION_PATH.write_text(json.dumps(proj, indent=2), encoding="utf-8")
    print(f"\n  Wrote: {PROJECTION_PATH}")
    print(f"  setfile: {setfile_name}")
    print(f"  weekly_mean_np: ${weekly_live['mean_np']:+,.0f}  (was ${existing.get('weekly_live',{}).get('mean_np','?')})")
    print(f"  monthly_compound_mean_roi_pct: {monthly_live['compound_mean_roi_pct']:.2f}%  (was {existing.get('monthly_live',{}).get('compound_mean_roi_pct','?')}%)")
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
