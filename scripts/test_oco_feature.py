"""Test OCO (one-cancels-other) feature for parent ORB pendings.

Current EA behavior: both BUY STOP + SELL STOP placed at range_high/low. When ONE
fires, the OTHER stays pending — both directions can fire in same session.

OCO behavior (proposed): when one pending fires, cancel the opposite-direction
pending. Each session can only fire in ONE direction.

This script post-processes existing sim deals and identifies sessions where BOTH
directions fired. Computes baseline (current behavior) vs OCO (drop the second
direction's trades) NP / DD / NP/DD$ side-by-side per stream.

Window: Feb 14 -> May 1 (76d, $10k, 23pt). Same as Phase E.
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
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
SPREAD = 23
START = datetime(2026, 2, 14, tzinfo=timezone.utc)
END = datetime(2026, 5, 1, tzinfo=timezone.utc)

STREAMS = [
    ("S1", dict(range_minutes=90, fixed_sl_pts=500, rr_ratio=4.0, half_tp_ratio=0.25)),
    ("S2", dict(range_minutes=90, fixed_sl_pts=400, rr_ratio=4.0, half_tp_ratio=0.0)),
    ("S3", dict(range_minutes=90, fixed_sl_pts=350, rr_ratio=4.0, half_tp_ratio=0.5)),
]


def make_cfg(stream_params, risk_pct):
    return ORBConfig(
        risk_pct=risk_pct,
        range_minutes=stream_params["range_minutes"],
        buffer_pts=0, min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=stream_params["fixed_sl_pts"],
        rr_ratio=stream_params["rr_ratio"],
        half_tp_ratio=stream_params["half_tp_ratio"],
        pending_expire_minutes=240,
        daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True,  ny_start_hour=13, comment="ORB",
    )


def aggregate(deals):
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gp = gl = 0.0
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


def session_id(ts: pd.Timestamp) -> str:
    """Bucket a deal into LDN or NY session for the day. UTC 7-13 = LDN, 13-21 = NY."""
    h = ts.hour
    d = ts.date()
    if 7 <= h < 13:
        return f"{d}-LDN"
    else:
        return f"{d}-NY"


def split_oco(stream_deals: list) -> tuple[list, dict]:
    """Apply OCO: per session, keep only the first-fired direction's deals.

    stream_deals: list of dicts {ts, kind, direction, lots, price, pnl}
    Returns (oco_deals_pnl_pairs, stats).
    """
    # Group by session
    by_session = {}
    for d in stream_deals:
        sid = session_id(pd.Timestamp(d["ts"]))
        by_session.setdefault(sid, []).append(d)

    oco_deals = []
    n_dual_sessions = 0
    n_dropped_trades = 0
    sum_dropped_pnl = 0.0
    for sid, ds in by_session.items():
        # Sort by time within session
        ds_sorted = sorted(ds, key=lambda x: pd.Timestamp(x["ts"]))
        # Find the first ENTRY (kind="entry") — its direction is the "kept" direction
        entries = [d for d in ds_sorted if d["kind"] == "entry"]
        if not entries:
            continue
        first_dir = entries[0]["direction"]
        # Keep only deals whose direction matches the first entry's direction
        kept = [d for d in ds_sorted if d["direction"] == first_dir]
        dropped = [d for d in ds_sorted if d["direction"] != first_dir]
        if dropped:
            n_dual_sessions += 1
            n_dropped_trades += sum(1 for d in dropped if d["kind"] != "entry")
            sum_dropped_pnl += sum(d["pnl"] for d in dropped)
        # Append non-entry deals (= closing PnL trades) for the kept direction
        for d in kept:
            if d["kind"] != "entry":
                oco_deals.append((pd.Timestamp(d["ts"]).value, d["pnl"]))
    return oco_deals, dict(n_dual_sessions=n_dual_sessions,
                            n_dropped_trades=n_dropped_trades,
                            sum_dropped_pnl=sum_dropped_pnl,
                            n_total_sessions=len(by_session))


def main():
    print("=" * 110)
    print(f"  OCO FEATURE TEST  |  {START.date()} -> {END.date()} (76d, $10k, {SPREAD}pt)")
    print(f"  Compares CURRENT behavior (both pendings stay) vs OCO (cancel opposite on first fire)")
    print("=" * 110)

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        m1 = load_bars(SYMBOL, "M1", START, END)
        m5 = load_bars(SYMBOL, "M5", START, END)
        ticks = load_ticks(SYMBOL, START, END, spread_pts=SPREAD)
        print(f"  Loaded ticks={len(ticks):,} M1={len(m1):,} M5={len(m5):,}\n")

        # Per-stream comparison at 1% per stream (production sizing in 3% setfile)
        per_stream_risk = 1.0
        all_base_deals = []
        all_oco_deals = []
        results = []
        for label, params in STREAMS:
            cfg = make_cfg(params, per_stream_risk)
            r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
            stream_deals = [
                {"ts": d.ts, "kind": d.kind, "direction": int(d.direction),
                 "lots": float(d.lots), "price": float(d.price), "pnl": float(d.pnl)}
                for d in r.deals
            ]
            base_pnl_pairs = [(pd.Timestamp(d["ts"]).value, d["pnl"]) for d in stream_deals if d["kind"] != "entry"]
            oco_pnl_pairs, stats = split_oco(stream_deals)
            b_np, b_dd, b_pf, b_ndd = aggregate(base_pnl_pairs)
            o_np, o_dd, o_pf, o_ndd = aggregate(oco_pnl_pairs)
            results.append({
                "stream": label, "params": params,
                "base_np": b_np, "base_dd": b_dd, "base_pf": b_pf, "base_ndd": b_ndd, "base_n": len(base_pnl_pairs),
                "oco_np": o_np, "oco_dd": o_dd, "oco_pf": o_pf, "oco_ndd": o_ndd, "oco_n": len(oco_pnl_pairs),
                **stats,
            })
            all_base_deals.extend(base_pnl_pairs)
            all_oco_deals.extend(oco_pnl_pairs)

        # Per-stream table
        print(f"  {'Stream':<6} {'n_sess':>7} {'dual':>5} {'drop_n':>6} {'drop_$':>9}  "
              f"{'CURRENT':<22}  {'OCO':<22}  delta NP / NP/DD$")
        print(f"  {'':>6} {'':>7} {'':>5} {'':>6} {'':>9}  "
              f"{'NP / DD% / NP/DD$':<22}  {'NP / DD% / NP/DD$':<22}")
        for r in results:
            cur = f"${r['base_np']:>+7,.0f}/{r['base_dd']:>4.1f}%/{r['base_ndd']:>5.2f}"
            oco = f"${r['oco_np']:>+7,.0f}/{r['oco_dd']:>4.1f}%/{r['oco_ndd']:>5.2f}"
            d_np = r["oco_np"] - r["base_np"]
            d_ndd = r["oco_ndd"] - r["base_ndd"]
            print(f"  {r['stream']:<6} {r['n_total_sessions']:>7} {r['n_dual_sessions']:>5} "
                  f"{r['n_dropped_trades']:>6} ${r['sum_dropped_pnl']:>+7,.0f}  "
                  f"{cur:<22}  {oco:<22}  ${d_np:>+7,.0f} / {d_ndd:>+5.2f}")

        # Combined portfolio (3-stream sum)
        b_np, b_dd, b_pf, b_ndd = aggregate(all_base_deals)
        o_np, o_dd, o_pf, o_ndd = aggregate(all_oco_deals)
        print(f"\n  PORTFOLIO (3-stream sum at 1% per stream = 3% total):")
        print(f"    CURRENT:  NP=${b_np:+,.0f}  DD={b_dd:.2f}%  NP/DD$={b_ndd:.2f}  PF={b_pf:.2f}  trades={len(all_base_deals)}")
        print(f"    OCO:      NP=${o_np:+,.0f}  DD={o_dd:.2f}%  NP/DD$={o_ndd:.2f}  PF={o_pf:.2f}  trades={len(all_oco_deals)}")
        print(f"    DELTA:    NP=${o_np-b_np:+,.0f}  DD={o_dd-b_dd:+.2f}p  NP/DD$={o_ndd-b_ndd:+.2f}")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
