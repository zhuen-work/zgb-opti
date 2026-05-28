"""Conditional OCO: enable only in trending regimes, disable in choppy.

Tests three regime filters:
  F1. Prior day directional efficiency (DE) — abs(D-1 close-open) / D-1 range.
      DE > 0.5 → trending → OCO on. Else → off.
  F2. ATR ratio — D1 ATR(5) / D1 ATR(20). > 1.0 → trending.
  F3. 5-day cumulative absolute return / 5-day total range — trend persistence.
      > 0.4 → trending.

For each filter, runs across same 6 chunks and compares to:
  - Never OCO (current EA behavior)
  - Always OCO (full OCO)

Metric: portfolio NP/DD$ per chunk + aggregate.

Goal: find filter that strictly dominates both extremes (or at least matches
the better extreme without the worst-case downside).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate

DEPOSIT = 10_000.0
SPREAD = 30  # per feedback_default_test_conditions.md (all live = 30pt 2026-05-16)
PER_STREAM_RISK = 1.0

STREAMS = [
    ("S1", dict(range_minutes=90, fixed_sl_pts=500, rr_ratio=4.0, half_tp_ratio=0.25)),
    ("S2", dict(range_minutes=90, fixed_sl_pts=400, rr_ratio=4.0, half_tp_ratio=0.0)),
    ("S3", dict(range_minutes=90, fixed_sl_pts=350, rr_ratio=4.0, half_tp_ratio=0.5)),
]

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
        ny_enabled=True,  ny_start_hour=13, comment="ORB",
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


def session_id(ts) -> str:
    ts = pd.Timestamp(ts)
    return f"{ts.date()}-{'LDN' if 7 <= ts.hour < 13 else 'NY'}"


def session_date(ts) -> pd.Timestamp:
    """Normalize to naive UTC date at midnight (consistent for dict lookup)."""
    t = pd.Timestamp(ts)
    if t.tz is not None:
        t = t.tz_convert("UTC").tz_localize(None)
    return t.normalize()


def split_oco_conditional(stream_deals: list, oco_enabled_for_date: dict) -> list:
    """Apply OCO only on dates where filter says enable.

    oco_enabled_for_date: {date -> bool}.
    For dates where OCO is disabled, keep ALL deals (current behavior).
    """
    by_session = {}
    for d in stream_deals:
        sid = session_id(d["ts"])
        by_session.setdefault(sid, []).append(d)

    out = []
    for sid, ds in by_session.items():
        ds_sorted = sorted(ds, key=lambda x: pd.Timestamp(x["ts"]))
        date = session_date(ds_sorted[0]["ts"])
        oco_on = oco_enabled_for_date.get(date, False)
        if not oco_on:
            # No OCO: keep all non-entry deals
            for d in ds_sorted:
                if d["kind"] != "entry":
                    out.append((pd.Timestamp(d["ts"]).value, d["pnl"]))
        else:
            # OCO: keep only first-direction non-entry deals
            entries = [d for d in ds_sorted if d["kind"] == "entry"]
            if not entries:
                continue
            first_dir = entries[0]["direction"]
            for d in ds_sorted:
                if d["kind"] != "entry" and d["direction"] == first_dir:
                    out.append((pd.Timestamp(d["ts"]).value, d["pnl"]))
    return out


def compute_d1_bars(m1: pd.DataFrame) -> pd.DataFrame:
    """Resample M1 → D1 (UTC) for regime calc."""
    df = m1.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.set_index("ts")
    d1 = df.resample("1D").agg({"open": "first", "high": "max", "low": "min", "close": "last"}).dropna()
    return d1


def _normalize_dates(idx) -> list:
    """Return list of naive UTC date keys for a DatetimeIndex (for dict lookup)."""
    out = []
    for ts in idx:
        t = pd.Timestamp(ts)
        if t.tz is not None:
            t = t.tz_convert("UTC").tz_localize(None)
        out.append(t.normalize())
    return out


def filter_de(d1: pd.DataFrame, threshold: float = 0.5) -> dict:
    """F1: prior day directional efficiency. Returns {date -> oco_on?}."""
    d1 = d1.copy()
    rng = (d1["high"] - d1["low"]).replace(0, np.nan)
    de = (d1["close"] - d1["open"]).abs() / rng
    de = de.fillna(0.0)
    out = {}
    dates = _normalize_dates(d1.index)
    for i, d in enumerate(dates):
        if i == 0:
            out[d] = False
            continue
        out[d] = bool(de.iloc[i - 1] > threshold)
    return out


def filter_atr_ratio(d1: pd.DataFrame, fast: int = 5, slow: int = 20,
                      threshold: float = 1.0) -> dict:
    """F2: ATR(fast)/ATR(slow) on D1."""
    d1 = d1.copy()
    tr = pd.concat([
        (d1["high"] - d1["low"]),
        (d1["high"] - d1["close"].shift()).abs(),
        (d1["low"] - d1["close"].shift()).abs(),
    ], axis=1).max(axis=1)
    atr_fast = tr.rolling(fast).mean()
    atr_slow = tr.rolling(slow).mean()
    ratio = (atr_fast / atr_slow).fillna(1.0)
    out = {}
    for d, r in zip(_normalize_dates(d1.index), ratio):
        out[d] = bool(r > threshold)
    return out


def filter_trend_persistence(d1: pd.DataFrame, lookback: int = 5,
                               threshold: float = 0.4) -> dict:
    """F3: 5-day net move / 5-day total range."""
    out = {}
    rng = d1["high"] - d1["low"]
    closes = d1["close"]
    dates = _normalize_dates(d1.index)
    for i, d in enumerate(dates):
        if i < lookback:
            out[d] = False
            continue
        net_move = abs(closes.iloc[i - 1] - closes.iloc[i - lookback])
        total_range = rng.iloc[i - lookback: i].sum()
        score = net_move / total_range if total_range > 0 else 0
        out[d] = bool(score > threshold)
    return out


def run_chunk(label, start, end, ticks, m1_full, m5, meta, regime_filters: dict):
    """Returns dict per filter (incl. NEVER + ALWAYS) of portfolio NP/DD/NPDD."""
    ticks_c = ticks[(ticks.ts >= start) & (ticks.ts < end)].reset_index(drop=True)
    m1_c = m1_full[(m1_full.ts >= start) & (m1_full.ts < end)].reset_index(drop=True)
    m5_c = m5[(m5.ts >= start) & (m5.ts < end)].reset_index(drop=True)

    # Pre-collect all deals across streams
    all_deals_by_stream = {}
    for s_label, params in STREAMS:
        cfg = make_cfg(params, PER_STREAM_RISK)
        r = orb_simulate(ticks_c, m5_c, m1_c, cfg, meta, initial_balance=DEPOSIT)
        all_deals_by_stream[s_label] = [
            {"ts": d.ts, "kind": d.kind, "direction": int(d.direction),
             "lots": float(d.lots), "price": float(d.price), "pnl": float(d.pnl)}
            for d in r.deals
        ]

    out = {}

    # NEVER OCO baseline
    base_deals = []
    for s_label, deals in all_deals_by_stream.items():
        for d in deals:
            if d["kind"] != "entry":
                base_deals.append((pd.Timestamp(d["ts"]).value, d["pnl"]))
    np_, dd, _, ndd = aggregate(base_deals)
    out["NEVER"] = (np_, dd, ndd)

    # ALWAYS OCO — keys are naive-UTC dates for consistent lookup
    always_on = {session_date(d): True for d in pd.date_range(start, end, freq="D")}
    deals_always = []
    for s_label, deals in all_deals_by_stream.items():
        deals_always.extend(split_oco_conditional(deals, always_on))
    np_, dd, _, ndd = aggregate(deals_always)
    out["ALWAYS"] = (np_, dd, ndd)

    # Conditional filters
    for fname, filt_dict in regime_filters.items():
        deals_f = []
        for s_label, deals in all_deals_by_stream.items():
            deals_f.extend(split_oco_conditional(deals, filt_dict))
        np_, dd, _, ndd = aggregate(deals_f)
        out[fname] = (np_, dd, ndd)
    return out


def main():
    print("=" * 110)
    print(f"  CONDITIONAL OCO TEST  |  3 regime filters tested across 6 chunks")
    print(f"  Goal: find a filter that dominates both NEVER-OCO and ALWAYS-OCO portfolio NP/DD$")
    print("=" * 110)

    try:
        m = symbol_meta("XAUUSD")
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        full_start = CHUNKS[0][1]  # Feb 14 (no extra lookback — cache doesn't extend earlier)
        full_end = CHUNKS[-2][2]   # May 1 — IS-5 end
        m1 = load_bars("XAUUSD", "M1", full_start, full_end)
        m5 = load_bars("XAUUSD", "M5", full_start, full_end)
        ticks = load_ticks("XAUUSD", full_start, full_end, spread_pts=SPREAD)
        print(f"  Loaded ticks={len(ticks):,} M1={len(m1):,} M5={len(m5):,}\n")

        # Compute D1 bars + filters once globally (uses 30d lookback)
        d1 = compute_d1_bars(m1)
        filters = {
            "F1_DE>0.5":         filter_de(d1, threshold=0.5),
            "F2_ATR_5/20>1.0":   filter_atr_ratio(d1, 5, 20, threshold=1.0),
            "F3_persist>0.4":    filter_trend_persistence(d1, lookback=5, threshold=0.4),
        }

        # Run each chunk
        per_chunk = []
        for label, s, e in CHUNKS:
            print(f"  Running {label}...")
            res = run_chunk(label, s, e, ticks, m1, m5, meta, filters)
            per_chunk.append((label, res))

        # Summary table
        modes = ["NEVER", "ALWAYS"] + list(filters.keys())
        print(f"\n  Portfolio NP/DD$ per chunk per mode:")
        hdr = f"  {'Chunk':<28}"
        for m_ in modes:
            hdr += f" {m_:>14}"
        print(hdr)
        for label, res in per_chunk:
            row = f"  {label:<28}"
            for m_ in modes:
                np_, dd, ndd = res[m_]
                row += f" {ndd:>+13.2f}"
            print(row)

        # Aggregate (mean across chunks)
        print(f"\n  Aggregate NP/DD$ (mean across 6 chunks):")
        for m_ in modes:
            mean_ndd = np.mean([res[m_][2] for _, res in per_chunk])
            wins_vs_never = sum(1 for _, res in per_chunk if res[m_][2] > res["NEVER"][2])
            wins_vs_always = sum(1 for _, res in per_chunk if res[m_][2] > res["ALWAYS"][2])
            print(f"  {m_:<18} mean NP/DD$={mean_ndd:>+5.2f}  "
                  f"vs NEVER wins {wins_vs_never}/6  vs ALWAYS wins {wins_vs_always}/6")

        # Best mode by mean
        best_mode = max(modes, key=lambda m_: np.mean([res[m_][2] for _, res in per_chunk]))
        print(f"\n  BEST MODE by mean NP/DD$: {best_mode}")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
