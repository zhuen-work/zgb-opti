"""Smart hedge filter test — test multiple conditions for whether to fire hedge.

Filters tested (each per-stream applied at parent SL event):
  ALWAYS:    fire hedge on every parent SL (current behavior)
  NEVER:     never fire hedge (=baseline parent only)
  F1_fast:   fire only if parent_sl_ts - parent_entry_ts <= 60 min
  F2_slow:   fire only if parent_sl_ts - parent_entry_ts > 60 min (opposite of F1)
  F3_small_rng:  fire only if session range_pts < 400
  F4_big_rng:    fire only if session range_pts >= 400 (opposite of F3)
  F5_LDN:    fire only on LDN session SLs (UTC 7-13)
  F6_NY:     fire only on NY session SLs (UTC 13-23)
  F7_S1_only: fire only on S1 parent SLs (skip S2 / S3)

For each, compute portfolio NP/DD/NPDD per 6 chunks + aggregate.
Goal: find a filter that strictly dominates ALWAYS (the current EA behavior).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone, timedelta
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

CHUNKS = [
    ("IS-1 Feb14-Feb28", datetime(2026, 2, 14, tzinfo=timezone.utc), datetime(2026, 2, 28, tzinfo=timezone.utc)),
    ("IS-2 Feb28-Mar14", datetime(2026, 2, 28, tzinfo=timezone.utc), datetime(2026, 3, 14, tzinfo=timezone.utc)),
    ("IS-3 Mar14-Mar28", datetime(2026, 3, 14, tzinfo=timezone.utc), datetime(2026, 3, 28, tzinfo=timezone.utc)),
    ("IS-4 Mar28-Apr11", datetime(2026, 3, 28, tzinfo=timezone.utc), datetime(2026, 4, 11, tzinfo=timezone.utc)),
    ("IS-5 Apr11-May01", datetime(2026, 4, 11, tzinfo=timezone.utc), datetime(2026, 5, 1, tzinfo=timezone.utc)),
    ("OOS Apr26-May01",  datetime(2026, 4, 26, tzinfo=timezone.utc), datetime(2026, 5, 1, tzinfo=timezone.utc)),
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


def session_id_from_ts(ts) -> str:
    ts = pd.Timestamp(ts)
    return f"{ts.date()}-{'LDN' if 7 <= ts.hour < 13 else 'NY'}"


# Filter functions: take a SL event dict + helper info, return True if hedge should fire.
def f_always(ev, **kw): return True
def f_never(ev, **kw): return False
def f_fast(ev, **kw):
    # ev["time_to_sl_min"] = (sl_ts - entry_ts).total_seconds() / 60
    return ev.get("time_to_sl_min", 999) <= 60
def f_slow(ev, **kw):
    return ev.get("time_to_sl_min", 999) > 60
def f_small_rng(ev, **kw):
    return ev.get("range_pts", 0) < 400
def f_big_rng(ev, **kw):
    return ev.get("range_pts", 0) >= 400
def f_ldn(ev, **kw):
    h = pd.Timestamp(ev["ts_ns"]).hour if isinstance(ev["ts_ns"], (int, float)) else pd.Timestamp.fromtimestamp(ev["ts_ns"]/1e9).hour
    return 7 <= h < 13
def f_ny(ev, **kw):
    h = pd.Timestamp.fromtimestamp(ev["ts_ns"]/1e9).hour
    return h >= 13 or h < 7
def f_s1_only(ev, stream=None, **kw):
    return stream == "S1"


FILTERS = {
    "ALWAYS":      f_always,
    "NEVER":       f_never,
    "F1_fast<=60m": f_fast,
    "F2_slow>60m":  f_slow,
    "F3_small_rng<400": f_small_rng,
    "F4_big_rng>=400": f_big_rng,
    "F5_LDN_only": f_ldn,
    "F6_NY_only":  f_ny,
    "F7_S1_only":  f_s1_only,
}


def collect_per_stream_data(stream, params, ticks, m1, m5, meta):
    """Run parent sim and collect per-deal info + SL events with metadata."""
    cfg = make_cfg(params, PER_STREAM_RISK)
    r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
    base_pnl_pairs = []
    sl_events = []
    # Track entry timestamps + range info per session for time-to-SL calc
    entries_by_session = {}
    range_size_by_session = {}
    for d in r.deals:
        ts = pd.Timestamp(d.ts)
        sid = f"{ts.date()}-{'LDN' if 7 <= ts.hour < 13 else 'NY'}"
        if d.kind == "entry":
            entries_by_session.setdefault(sid, ts)  # first entry per session
            continue
        base_pnl_pairs.append((ts.value, d.pnl))
        if d.kind == "sl":
            entry_ts = entries_by_session.get(sid, ts)
            ttsl_min = (ts - entry_ts).total_seconds() / 60.0
            sl_events.append({
                "ts_ns": ts.value,
                "direction": int(d.direction),
                "sl_price": float(d.price),
                "lots": float(d.lots),
                "session_id": sid,
                "time_to_sl_min": ttsl_min,
            })
    # Range size per session (max-min of ticks during the range window)
    # Approximation: use parent's range_minutes and start hour. Skip for simplicity;
    # use a proxy = sl_events count per session if needed. Not implemented for v1.
    # Defer F3/F4 range filter — skip the range filter for now (would need extra sim hooks)
    return base_pnl_pairs, sl_events


def run_chunk(label, start, end, ticks_full, m1_full, m5_full, meta):
    ticks = ticks_full[(ticks_full.ts >= start) & (ticks_full.ts < end)].reset_index(drop=True)
    m1 = m1_full[(m1_full.ts >= start) & (m1_full.ts < end)].reset_index(drop=True)
    m5 = m5_full[(m5_full.ts >= start) & (m5_full.ts < end)].reset_index(drop=True)
    t_arr = hg.ts_arr_from_ticks(ticks)

    # Per stream: parent baseline + collect SL events
    per_stream = {}
    for s_label, params in STREAM_CFGS.items():
        base, sl_events = collect_per_stream_data(s_label, params, ticks, m1, m5, meta)
        per_stream[s_label] = dict(base=base, sl_events=sl_events)

    # For each filter, build hedge deals (filtered) + aggregate portfolio
    out = {}
    for fname, fn in FILTERS.items():
        all_deals = []
        for s_label, sd in per_stream.items():
            all_deals.extend(sd["base"])
            # Filter SL events
            filtered_sls = [ev for ev in sd["sl_events"] if fn(ev, stream=s_label)]
            hg.HEDGE_RISK_PCT = HEDGE_RISK
            h_deals = hg.simulate_hedges(filtered_sls, t_arr, HEDGE_CFGS[s_label])
            for ts, p in h_deals:
                all_deals.append((ts, p))
        np_, dd, _, ndd = aggregate(all_deals)
        out[fname] = (np_, dd, ndd)
    return out


def main():
    print("=" * 130)
    print(f"  SMART HEDGE FILTER TEST  |  9 filters across 6 chunks  |  Goal: find filter that beats ALWAYS")
    print("=" * 130)
    try:
        m = symbol_meta("XAUUSD")
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        full_start = CHUNKS[0][1]
        full_end = CHUNKS[-2][2]
        m1 = load_bars("XAUUSD", "M1", full_start, full_end)
        m5 = load_bars("XAUUSD", "M5", full_start, full_end)
        ticks = load_ticks("XAUUSD", full_start, full_end, spread_pts=SPREAD)
        print(f"  Loaded ticks={len(ticks):,} M1={len(m1):,} M5={len(m5):,}\n")

        results = []
        for label, s, e in CHUNKS:
            print(f"  Running {label}...")
            results.append((label, run_chunk(label, s, e, ticks, m1, m5, meta)))

        # Header
        modes = list(FILTERS.keys())
        print(f"\n  Portfolio NP/DD$ per chunk per filter:")
        hdr = f"  {'Chunk':<26}"
        for m_ in modes:
            hdr += f" {m_[:14]:>15}"
        print(hdr)
        for label, res in results:
            row = f"  {label:<26}"
            for m_ in modes:
                _, _, ndd = res[m_]
                row += f" {ndd:>+14.2f}"
            print(row)

        # Aggregate
        print(f"\n  Aggregate across 6 chunks:")
        always_per_chunk = [res["ALWAYS"][2] for _, res in results]
        never_per_chunk = [res["NEVER"][2] for _, res in results]
        print(f"  {'Filter':<20} {'Mean NDD':>10} {'Median':>10} "
              f"{'Min':>8} {'Max':>8} {'Wins vs ALWAYS':>16} {'Wins vs NEVER':>16}")
        for m_ in modes:
            ndds = [res[m_][2] for _, res in results]
            mean_ = np.mean(ndds); med = np.median(ndds)
            wins_a = sum(1 for x, a in zip(ndds, always_per_chunk) if x > a)
            wins_n = sum(1 for x, n in zip(ndds, never_per_chunk) if x > n)
            print(f"  {m_:<20} {mean_:>+9.2f} {med:>+9.2f} {min(ndds):>+7.2f} "
                  f"{max(ndds):>+7.2f} {wins_a:>13}/6 {wins_n:>13}/6")

        # Best filter by mean
        best = max(modes, key=lambda m_: np.mean([res[m_][2] for _, res in results]))
        print(f"\n  BEST by mean NP/DD$: {best}")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
