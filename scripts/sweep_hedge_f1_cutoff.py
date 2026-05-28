"""F1 cutoff sweep: find optimal MaxSecondsAfterEntry threshold.

Tests 9 cutoff values from 15min to 240min (= effectively always-on, since
parent pending expires at 240min anyway). Per-stream + portfolio metrics
on FULL IS + 5 chunks.

Goal: find cutoff that maximises mean NP/DD$ across chunks while keeping
robustness (high min, low variance).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import importlib.util
_spec = importlib.util.spec_from_file_location("wfo_hedge", ROOT / "scripts" / "sim_wfo_hedge.py")
hg = importlib.util.module_from_spec(_spec); sys.modules["wfo_hedge"] = hg
_spec.loader.exec_module(hg)

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate

DEPOSIT = 10_000.0
SPREAD = 30  # per feedback_default_test_conditions.md (all live = 30pt 2026-05-16)
PER_STREAM_RISK = 1.0
HEDGE_RISK = 1.0

STREAM_CFGS = {
    "S1": dict(range_minutes=90, fixed_sl_pts=500, rr_ratio=4.0, half_tp_ratio=0.25),
    "S2": dict(range_minutes=90, fixed_sl_pts=400, rr_ratio=4.0, half_tp_ratio=0.0),
    "S3": dict(range_minutes=90, fixed_sl_pts=350, rr_ratio=4.0, half_tp_ratio=0.5),
}
HEDGE_CFGS = {
    "S1": hg.HedgeCfg(buf=350, h_sl=500, h_rr=4.0, exp=30),
    "S2": hg.HedgeCfg(buf=100, h_sl=500, h_rr=4.0, exp=120),
    "S3": hg.HedgeCfg(buf=100, h_sl=500, h_rr=4.0, exp=120),
}

# Cutoff values in seconds: 15m, 30m, 45m, 60m, 90m, 120m, 180m, 240m, 0(=always)
CUTOFFS = [900, 1800, 2700, 3600, 5400, 7200, 10800, 14400, 0]
CUTOFF_LABELS = {0: "ALWAYS"}
for c in CUTOFFS:
    if c > 0:
        CUTOFF_LABELS[c] = f"{c // 60}m"

WINDOWS = [
    ("FULL IS",        datetime(2026, 2, 14, tzinfo=timezone.utc), datetime(2026, 5, 1, tzinfo=timezone.utc)),
    ("IS-1",  datetime(2026, 2, 14, tzinfo=timezone.utc), datetime(2026, 2, 28, tzinfo=timezone.utc)),
    ("IS-2",  datetime(2026, 2, 28, tzinfo=timezone.utc), datetime(2026, 3, 14, tzinfo=timezone.utc)),
    ("IS-3",  datetime(2026, 3, 14, tzinfo=timezone.utc), datetime(2026, 3, 28, tzinfo=timezone.utc)),
    ("IS-4",  datetime(2026, 3, 28, tzinfo=timezone.utc), datetime(2026, 4, 11, tzinfo=timezone.utc)),
    ("IS-5",  datetime(2026, 4, 11, tzinfo=timezone.utc), datetime(2026, 5, 1, tzinfo=timezone.utc)),
]


def make_cfg(stream_params, risk_pct):
    return ORBConfig(
        risk_pct=risk_pct, range_minutes=stream_params["range_minutes"],
        buffer_pts=0, min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=stream_params["fixed_sl_pts"],
        rr_ratio=stream_params["rr_ratio"],
        half_tp_ratio=stream_params["half_tp_ratio"],
        pending_expire_minutes=240, daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13, comment="ORB",
    )


def aggregate(deals):
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0; gp = gl = 0.0
    for _, p in sorted(deals, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p > 0: gp += p
        elif p < 0: gl += p
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    pf = (gp / abs(gl)) if gl < 0 else float("inf")
    ndd = (np_ / dd_abs) if dd_abs > 0 else 0
    return np_, dd_pct, pf, ndd


def collect_stream_data(stream, params, ticks, m1, m5, meta):
    cfg = make_cfg(params, PER_STREAM_RISK)
    r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
    base = []; sl_events = []
    entries_by_session = {}
    for d in r.deals:
        ts = pd.Timestamp(d.ts)
        sid = f"{ts.date()}-{'LDN' if 7 <= ts.hour < 13 else 'NY'}"
        if d.kind == "entry":
            entries_by_session.setdefault(sid, ts)
            continue
        base.append((ts.value, d.pnl))
        if d.kind == "sl":
            entry_ts = entries_by_session.get(sid, ts)
            ttsl_sec = (ts - entry_ts).total_seconds()
            sl_events.append({
                "ts_ns": ts.value, "direction": int(d.direction),
                "sl_price": float(d.price), "lots": float(d.lots),
                "time_to_sl_sec": ttsl_sec,
            })
    return base, sl_events


def run_window(label, start, end, ticks_full, m1_full, m5_full, meta):
    ticks = ticks_full[(ticks_full.ts >= start) & (ticks_full.ts < end)].reset_index(drop=True)
    m1 = m1_full[(m1_full.ts >= start) & (m1_full.ts < end)].reset_index(drop=True)
    m5 = m5_full[(m5_full.ts >= start) & (m5_full.ts < end)].reset_index(drop=True)
    t_arr = hg.ts_arr_from_ticks(ticks)

    # Collect stream data once (parent + sl_events)
    streams = {}
    for s_label, params in STREAM_CFGS.items():
        base, sls = collect_stream_data(s_label, params, ticks, m1, m5, meta)
        streams[s_label] = {"base": base, "sls": sls}

    out = {}
    for cutoff in CUTOFFS:
        all_deals = []
        h_fires_total = 0
        h_pnl_total = 0.0
        h_wins_total = 0
        sl_total = 0
        for s_label, sd in streams.items():
            all_deals.extend(sd["base"])
            if cutoff == 0:
                filtered = sd["sls"]
            else:
                filtered = [ev for ev in sd["sls"] if ev["time_to_sl_sec"] <= cutoff]
            hg.HEDGE_RISK_PCT = HEDGE_RISK
            h_deals = hg.simulate_hedges(filtered, t_arr, HEDGE_CFGS[s_label])
            h_fires_total += len(h_deals)
            h_pnl_total += sum(p for _, p in h_deals)
            h_wins_total += sum(1 for _, p in h_deals if p > 0)
            sl_total += len(sd["sls"])
            for ts, p in h_deals:
                all_deals.append((ts, p))
        np_, dd, pf, ndd = aggregate(all_deals)
        out[cutoff] = {
            "np": np_, "dd": dd, "pf": pf, "ndd": ndd,
            "n_trades": len(all_deals),
            "h_fires": h_fires_total, "h_pnl": h_pnl_total,
            "h_wins": h_wins_total, "sl_total": sl_total,
        }
    return out


def main():
    print("=" * 130)
    print(f"  F1 CUTOFF SWEEP  |  9 thresholds × 6 windows")
    print(f"  Cutoffs (seconds): {CUTOFFS}  ({[CUTOFF_LABELS[c] for c in CUTOFFS]})")
    print("=" * 130)

    try:
        m = symbol_meta("XAUUSD")
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        full_start = WINDOWS[0][1]; full_end = WINDOWS[0][2]
        m1 = load_bars("XAUUSD", "M1", full_start, full_end)
        m5 = load_bars("XAUUSD", "M5", full_start, full_end)
        ticks = load_ticks("XAUUSD", full_start, full_end, spread_pts=SPREAD)
        print(f"  Loaded ticks={len(ticks):,} M1={len(m1):,} M5={len(m5):,}\n")

        results = []
        for label, s, e in WINDOWS:
            print(f"  Running {label}...")
            results.append((label, run_window(label, s, e, ticks, m1, m5, meta)))

        # Per-window NP/DD$ table
        print(f"\n  Portfolio NP/DD$ per window per cutoff:")
        hdr = f"  {'Window':<10}"
        for c in CUTOFFS:
            hdr += f" {CUTOFF_LABELS[c]:>8}"
        print(hdr)
        for label, res in results:
            row = f"  {label:<10}"
            for c in CUTOFFS:
                row += f" {res[c]['ndd']:>+7.2f}"
            print(row)

        # Per-cutoff aggregate (across the 5 chunks, exclude FULL IS to avoid double-counting)
        chunk_results = [(l, r) for l, r in results if "FULL" not in l]
        print(f"\n  Aggregate across 5 chunks (mean / median / min / max NP/DD$):")
        print(f"  {'Cutoff':<8} {'mean':>7} {'median':>7} {'min':>7} {'max':>7} "
              f"{'WinsVsAlw':>10} {'FULL_IS':>9} {'h_fires':>8} {'h_WR':>5} {'h_NP_FULL':>10}")
        always_per_chunk = [r[0] for _, r in chunk_results for c, r_ in [(0, r[0])] if c == 0]
        always_per_chunk = [chunk_results[i][1][0]["ndd"] for i in range(len(chunk_results))]
        for c in CUTOFFS:
            ndds = [r[c]["ndd"] for _, r in chunk_results]
            mean_ = np.mean(ndds); med = np.median(ndds)
            wins_vs_always = sum(1 for x, a in zip(ndds, always_per_chunk) if x > a)
            full_is = results[0][1][c]
            wr = (full_is["h_wins"] / full_is["h_fires"] * 100) if full_is["h_fires"] else 0
            print(f"  {CUTOFF_LABELS[c]:<8} {mean_:>+6.2f} {med:>+6.2f} "
                  f"{min(ndds):>+6.2f} {max(ndds):>+6.2f} "
                  f"{wins_vs_always:>9}/5 {full_is['ndd']:>+8.2f} "
                  f"{full_is['h_fires']:>5}/{full_is['sl_total']} {wr:>4.0f}% "
                  f"${full_is['h_pnl']:>+8,.0f}")

        # Best
        best_mean = max(CUTOFFS, key=lambda c: np.mean([r[c]["ndd"] for _, r in chunk_results]))
        best_min = max(CUTOFFS, key=lambda c: min(r[c]["ndd"] for _, r in chunk_results))
        best_full = max(CUTOFFS, key=lambda c: results[0][1][c]["ndd"])
        print(f"\n  BEST by mean across chunks: {CUTOFF_LABELS[best_mean]}")
        print(f"  BEST by floor (min): {CUTOFF_LABELS[best_min]}")
        print(f"  BEST on FULL IS (76d): {CUTOFF_LABELS[best_full]}")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
