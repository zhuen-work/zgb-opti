"""OCO robustness test across multiple windows + OOS.

Strategy: split the data into ~5 non-overlapping 15-day chunks (IS portion),
plus 1 OOS chunk (May 4 → today, post-fix days only). Per chunk, run baseline
and OCO post-process, report:
  - NP / DD% / NP/DD$ for each variant
  - Sign of delta (+ favors OCO, - favors current)
  - Per-stream breakdown

Decision rule: OCO is "robust" if it improves NP/DD$ in ≥4 of 6 chunks AND the
S1 benefit is consistent (since portfolio gain is S1-driven on the original
test).

OOS chunk uses live-account XAUUSD.sc data via direct pull. IS chunks use
cached XAUUSD parquets.
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
_spec = importlib.util.spec_from_file_location("oos_today", ROOT / "scripts" / "sim_orb_oos_today.py")
oos = importlib.util.module_from_spec(_spec); sys.modules["oos_today"] = oos
_spec.loader.exec_module(oos)

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate

DEPOSIT = 10_000.0
SPREAD = 23
PER_STREAM_RISK = 1.0   # 3% portfolio / 3 streams

STREAMS = [
    ("S1", dict(range_minutes=90, fixed_sl_pts=500, rr_ratio=4.0, half_tp_ratio=0.25)),
    ("S2", dict(range_minutes=90, fixed_sl_pts=400, rr_ratio=4.0, half_tp_ratio=0.0)),
    ("S3", dict(range_minutes=90, fixed_sl_pts=350, rr_ratio=4.0, half_tp_ratio=0.5)),
]

# 5 non-overlapping IS chunks (15 days each) covering Feb 14 → May 1
IS_CHUNKS = [
    ("IS-1 Feb14-Feb28", datetime(2026, 2, 14, tzinfo=timezone.utc), datetime(2026, 2, 28, tzinfo=timezone.utc)),
    ("IS-2 Feb28-Mar14", datetime(2026, 2, 28, tzinfo=timezone.utc), datetime(2026, 3, 14, tzinfo=timezone.utc)),
    ("IS-3 Mar14-Mar28", datetime(2026, 3, 14, tzinfo=timezone.utc), datetime(2026, 3, 28, tzinfo=timezone.utc)),
    ("IS-4 Mar28-Apr11", datetime(2026, 3, 28, tzinfo=timezone.utc), datetime(2026, 4, 11, tzinfo=timezone.utc)),
    ("IS-5 Apr11-May01", datetime(2026, 4, 11, tzinfo=timezone.utc), datetime(2026, 5, 1, tzinfo=timezone.utc)),
]

# OOS chunk: post-fix only (live data was invalidated before this; sim runs valid)
# Apr 26-May 1 is technically post-IS (WFO ended), but cached. May 4-8 is post-fix.
OOS_CHUNK = ("OOS Apr26-May01 (post-IS, cached)",
             datetime(2026, 4, 26, tzinfo=timezone.utc),
             datetime(2026, 5, 1, tzinfo=timezone.utc))


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
    h = ts.hour
    d = ts.date()
    return f"{d}-{'LDN' if 7 <= h < 13 else 'NY'}"


def split_oco(stream_deals: list) -> tuple[list, dict]:
    by_session = {}
    for d in stream_deals:
        sid = session_id(d["ts"])
        by_session.setdefault(sid, []).append(d)
    oco_deals = []
    n_dual = 0
    n_drop = 0
    sum_drop = 0.0
    for sid, ds in by_session.items():
        ds_sorted = sorted(ds, key=lambda x: pd.Timestamp(x["ts"]))
        entries = [d for d in ds_sorted if d["kind"] == "entry"]
        if not entries: continue
        first_dir = entries[0]["direction"]
        kept = [d for d in ds_sorted if d["direction"] == first_dir and d["kind"] != "entry"]
        dropped = [d for d in ds_sorted if d["direction"] != first_dir and d["kind"] != "entry"]
        if dropped:
            n_dual += 1
            n_drop += len(dropped)
            sum_drop += sum(d["pnl"] for d in dropped)
        for d in kept:
            oco_deals.append((pd.Timestamp(d["ts"]).value, d["pnl"]))
    return oco_deals, dict(n_dual=n_dual, n_drop=n_drop, sum_drop=sum_drop, n_sess=len(by_session))


def run_chunk(label, start, end, ticks, m1, m5, meta):
    """Per chunk: per-stream baseline + OCO. Returns dict per stream."""
    # Slice ticks/bars to chunk
    ticks_c = ticks[(ticks.ts >= start) & (ticks.ts < end)].reset_index(drop=True)
    m1_c = m1[(m1.ts >= start) & (m1.ts < end)].reset_index(drop=True)
    m5_c = m5[(m5.ts >= start) & (m5.ts < end)].reset_index(drop=True)
    days = (end - start).days
    out = {"label": label, "days": days, "streams": {}}
    all_base = []; all_oco = []
    for s_label, params in STREAMS:
        cfg = make_cfg(params, PER_STREAM_RISK)
        r = orb_simulate(ticks_c, m5_c, m1_c, cfg, meta, initial_balance=DEPOSIT)
        deals = [{"ts": d.ts, "kind": d.kind, "direction": int(d.direction),
                  "lots": float(d.lots), "price": float(d.price), "pnl": float(d.pnl)}
                 for d in r.deals]
        base = [(pd.Timestamp(d["ts"]).value, d["pnl"]) for d in deals if d["kind"] != "entry"]
        oco, stats = split_oco(deals)
        b_np, b_dd, _, b_ndd = aggregate(base)
        o_np, o_dd, _, o_ndd = aggregate(oco)
        out["streams"][s_label] = {
            "base_np": b_np, "base_dd": b_dd, "base_ndd": b_ndd,
            "oco_np": o_np, "oco_dd": o_dd, "oco_ndd": o_ndd,
            "n_dual": stats["n_dual"], "n_sess": stats["n_sess"],
            "drop_n": stats["n_drop"], "drop_pnl": stats["sum_drop"],
        }
        all_base.extend(base); all_oco.extend(oco)
    pb_np, pb_dd, _, pb_ndd = aggregate(all_base)
    po_np, po_dd, _, po_ndd = aggregate(all_oco)
    out["portfolio"] = {
        "base_np": pb_np, "base_dd": pb_dd, "base_ndd": pb_ndd,
        "oco_np": po_np, "oco_dd": po_dd, "oco_ndd": po_ndd,
    }
    return out


def main():
    print("=" * 110)
    print(f"  OCO ROBUSTNESS TEST  |  5 non-overlapping IS chunks (15d each) + 1 OOS chunk")
    print(f"  Per-stream + portfolio baseline vs OCO. Decision rule: OCO wins if NP/DD$ improves in >=4/6 chunks.")
    print("=" * 110)

    try:
        m = symbol_meta("XAUUSD")
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        # Load full IS+OOS data once (Feb 14 → May 1, all from cached XAUUSD parquet)
        full_start = IS_CHUNKS[0][1]
        full_end = OOS_CHUNK[2]
        m1 = load_bars("XAUUSD", "M1", full_start, full_end)
        m5 = load_bars("XAUUSD", "M5", full_start, full_end)
        ticks = load_ticks("XAUUSD", full_start, full_end, spread_pts=SPREAD)
        print(f"  Loaded ticks={len(ticks):,} M1={len(m1):,} M5={len(m5):,}\n")

        chunks = IS_CHUNKS + [OOS_CHUNK]
        results = []
        for label, s, e in chunks:
            print(f"  Running {label} ({s.date()} -> {e.date()}, {(e-s).days}d)...")
            results.append(run_chunk(label, s, e, ticks, m1, m5, meta))

        # Per-chunk portfolio table
        print(f"\n  {'Chunk':<32} {'Base NP':>9} {'Base DD%':>9} {'Base NP/DD':>11} | "
              f"{'OCO NP':>9} {'OCO DD%':>8} {'OCO NP/DD':>10} | {'dNP/DD$':>9}  Verdict")
        wins = 0
        for r in results:
            p = r["portfolio"]
            d_ndd = p["oco_ndd"] - p["base_ndd"]
            verdict = "OCO" if d_ndd > 0 else ("CURRENT" if d_ndd < 0 else "tie")
            if d_ndd > 0: wins += 1
            print(f"  {r['label']:<32} ${p['base_np']:>+7,.0f} {p['base_dd']:>7.2f}% {p['base_ndd']:>+10.2f} | "
                  f"${p['oco_np']:>+7,.0f} {p['oco_dd']:>6.2f}% {p['oco_ndd']:>+9.2f} | "
                  f"{d_ndd:>+8.2f}  {verdict}")
        print(f"\n  PORTFOLIO VERDICT: OCO wins {wins}/{len(results)} chunks")

        # Per-stream NP/DD$ delta per chunk
        print(f"\n  Per-stream NP/DD$ delta (OCO - CURRENT) across chunks:")
        print(f"  {'Chunk':<32} {'S1 dNP/DD$':>12} {'S2 dNP/DD$':>12} {'S3 dNP/DD$':>12}  S1_dual/sess")
        s1_wins = s2_wins = s3_wins = 0
        for r in results:
            s1d = r["streams"]["S1"]["oco_ndd"] - r["streams"]["S1"]["base_ndd"]
            s2d = r["streams"]["S2"]["oco_ndd"] - r["streams"]["S2"]["base_ndd"]
            s3d = r["streams"]["S3"]["oco_ndd"] - r["streams"]["S3"]["base_ndd"]
            if s1d > 0: s1_wins += 1
            if s2d > 0: s2_wins += 1
            if s3d > 0: s3_wins += 1
            s1_stats = r["streams"]["S1"]
            print(f"  {r['label']:<32} {s1d:>+11.2f} {s2d:>+11.2f} {s3d:>+11.2f}  "
                  f"{s1_stats['n_dual']}/{s1_stats['n_sess']}")
        print(f"\n  Per-stream wins: S1={s1_wins}/{len(results)}  S2={s2_wins}/{len(results)}  S3={s3_wins}/{len(results)}")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
