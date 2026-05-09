"""Detailed comparison: hedge ALWAYS vs F1 fast-SL filter on full IS + 6 chunks.

Shows NP / DD% / NP/DD$ / PF / total trades / hedge fires for each mode, per
stream + portfolio. Helps decide whether F1 is worth deploying.
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
SPREAD = 23
PER_STREAM_RISK = 1.0
HEDGE_RISK = 1.0
F1_CUTOFF_SEC = 3600  # 60 min

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

WINDOWS = [
    ("FULL IS Feb14-May01", datetime(2026, 2, 14, tzinfo=timezone.utc), datetime(2026, 5, 1, tzinfo=timezone.utc)),
    ("IS-1 Feb14-Feb28", datetime(2026, 2, 14, tzinfo=timezone.utc), datetime(2026, 2, 28, tzinfo=timezone.utc)),
    ("IS-2 Feb28-Mar14", datetime(2026, 2, 28, tzinfo=timezone.utc), datetime(2026, 3, 14, tzinfo=timezone.utc)),
    ("IS-3 Mar14-Mar28", datetime(2026, 3, 14, tzinfo=timezone.utc), datetime(2026, 3, 28, tzinfo=timezone.utc)),
    ("IS-4 Mar28-Apr11", datetime(2026, 3, 28, tzinfo=timezone.utc), datetime(2026, 4, 11, tzinfo=timezone.utc)),
    ("IS-5 Apr11-May01", datetime(2026, 4, 11, tzinfo=timezone.utc), datetime(2026, 5, 1, tzinfo=timezone.utc)),
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


def collect_stream(stream, params, ticks, m1, m5, meta):
    """Run parent sim, return (base_pnl_pairs, sl_events with time_to_sl)."""
    cfg = make_cfg(params, PER_STREAM_RISK)
    r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
    base_pnl_pairs = []
    sl_events = []
    entries_by_session = {}
    for d in r.deals:
        ts = pd.Timestamp(d.ts)
        sid = f"{ts.date()}-{'LDN' if 7 <= ts.hour < 13 else 'NY'}"
        if d.kind == "entry":
            entries_by_session.setdefault(sid, ts)
            continue
        base_pnl_pairs.append((ts.value, d.pnl))
        if d.kind == "sl":
            entry_ts = entries_by_session.get(sid, ts)
            ttsl_sec = (ts - entry_ts).total_seconds()
            sl_events.append({
                "ts_ns": ts.value, "direction": int(d.direction),
                "sl_price": float(d.price), "lots": float(d.lots),
                "time_to_sl_sec": ttsl_sec,
            })
    return base_pnl_pairs, sl_events


def run_window(label, start, end, ticks_full, m1_full, m5_full, meta):
    ticks = ticks_full[(ticks_full.ts >= start) & (ticks_full.ts < end)].reset_index(drop=True)
    m1 = m1_full[(m1_full.ts >= start) & (m1_full.ts < end)].reset_index(drop=True)
    m5 = m5_full[(m5_full.ts >= start) & (m5_full.ts < end)].reset_index(drop=True)
    t_arr = hg.ts_arr_from_ticks(ticks)

    out = {"label": label, "modes": {}}
    for mode in ("ALWAYS", "F1"):
        all_deals = []
        per_stream_stats = {}
        for s_label, params in STREAM_CFGS.items():
            base, sl_events = collect_stream(s_label, params, ticks, m1, m5, meta)
            # Apply filter
            if mode == "F1":
                filtered_sls = [ev for ev in sl_events if ev["time_to_sl_sec"] <= F1_CUTOFF_SEC]
            else:
                filtered_sls = sl_events
            hg.HEDGE_RISK_PCT = HEDGE_RISK
            h_deals = hg.simulate_hedges(filtered_sls, t_arr, HEDGE_CFGS[s_label])
            h_pnl = sum(p for _, p in h_deals)
            h_n = len(h_deals)
            h_w = sum(1 for _, p in h_deals if p > 0)
            base_pnl = sum(p for _, p in base)
            per_stream_stats[s_label] = {
                "base_pnl": base_pnl, "base_n": len(base),
                "hedge_pnl": h_pnl, "hedge_n": h_n, "hedge_w": h_w,
                "sl_events_total": len(sl_events), "sl_events_passed": len(filtered_sls),
            }
            all_deals.extend(base)
            for ts, p in h_deals:
                all_deals.append((ts, p))
        np_, dd, pf, ndd = aggregate(all_deals)
        out["modes"][mode] = {
            "np": np_, "dd": dd, "pf": pf, "ndd": ndd,
            "n_trades": len(all_deals), "per_stream": per_stream_stats,
        }
    return out


def main():
    print("=" * 130)
    print(f"  HEDGE: ALWAYS vs F1 (fast-SL <={F1_CUTOFF_SEC}s = 60min)  |  Full IS + 5 chunks")
    print(f"  Sizing: parent 1% per stream, hedge 1% per stream (production 3% setfile)")
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
            results.append(run_window(label, s, e, ticks, m1, m5, meta))

        # Detailed comparison
        for r in results:
            print(f"\n{'=' * 130}")
            print(f"  {r['label']}")
            print(f"{'=' * 130}")
            a = r["modes"]["ALWAYS"]
            f = r["modes"]["F1"]
            print(f"  {'Mode':<10} {'NP':>10} {'DD%':>6} {'NP/DD$':>7} {'PF':>5} {'Trades':>7}  "
                  f"{'h_fires/SLevents (passrate)':<28}  hedge_NP")
            # Compute aggregate hedge stats
            for mode, m_data in [("ALWAYS", a), ("F1", f)]:
                tot_h_n = sum(s["hedge_n"] for s in m_data["per_stream"].values())
                tot_h_w = sum(s["hedge_w"] for s in m_data["per_stream"].values())
                tot_sl = sum(s["sl_events_total"] for s in m_data["per_stream"].values())
                tot_sl_pass = sum(s["sl_events_passed"] for s in m_data["per_stream"].values())
                tot_h_pnl = sum(s["hedge_pnl"] for s in m_data["per_stream"].values())
                wr = (tot_h_w / tot_h_n * 100) if tot_h_n else 0
                pass_rate = (tot_sl_pass / tot_sl * 100) if tot_sl else 0
                pass_str = f"{tot_h_n}/{tot_sl_pass}/{tot_sl} ({pass_rate:.0f}%)"
                print(f"  {mode:<10} ${m_data['np']:>+8,.0f} {m_data['dd']:>5.2f}% {m_data['ndd']:>+6.2f} "
                      f"{m_data['pf']:>5.2f} {m_data['n_trades']:>7}  "
                      f"{pass_str:<28}  ${tot_h_pnl:>+7,.0f} (WR {wr:.0f}%)")
            # Delta
            d_np = f["np"] - a["np"]; d_dd = f["dd"] - a["dd"]; d_ndd = f["ndd"] - a["ndd"]
            print(f"  {'DELTA F1-ALW':<10} ${d_np:>+8,.0f} {d_dd:>+5.2f}p {d_ndd:>+6.2f}")

            # Per-stream details (FULL IS only — for chunks too noisy)
            if "FULL" in r["label"]:
                print(f"\n  Per-stream breakdown (FULL IS):")
                print(f"  {'Stream':<6} {'Mode':<8} {'parent_NP':>10} {'parent_n':>8} "
                      f"{'hedge_NP':>9} {'hedge_n':>7} {'h_WR':>5} "
                      f"{'SLs_passed/total':<18}")
                for s_label in ("S1", "S2", "S3"):
                    for mode, m_data in [("ALWAYS", a), ("F1", f)]:
                        ps = m_data["per_stream"][s_label]
                        wr = (ps["hedge_w"] / ps["hedge_n"] * 100) if ps["hedge_n"] else 0
                        passed = f"{ps['sl_events_passed']}/{ps['sl_events_total']}"
                        print(f"  {s_label:<6} {mode:<8} ${ps['base_pnl']:>+8,.0f} {ps['base_n']:>7}   "
                              f"${ps['hedge_pnl']:>+7,.0f} {ps['hedge_n']:>6}  {wr:>4.0f}% {passed:<18}")

        # Final summary
        print(f"\n{'=' * 130}")
        print(f"  SUMMARY: F1 vs ALWAYS across all windows")
        print(f"{'=' * 130}")
        print(f"  {'Window':<28} {'ALWAYS NP/DD$':>14} {'F1 NP/DD$':>11} {'Δ NP/DD$':>10} "
              f"{'ALWAYS NP':>10} {'F1 NP':>10} {'Δ NP':>9}")
        f1_wins = 0
        for r in results:
            a = r["modes"]["ALWAYS"]; f = r["modes"]["F1"]
            d_ndd = f["ndd"] - a["ndd"]; d_np = f["np"] - a["np"]
            verdict = "F1" if d_ndd > 0 else "ALWAYS"
            if d_ndd > 0: f1_wins += 1
            print(f"  {r['label']:<28} {a['ndd']:>+13.2f} {f['ndd']:>+10.2f} {d_ndd:>+9.2f} "
                  f"${a['np']:>+8,.0f} ${f['np']:>+8,.0f} ${d_np:>+7,.0f}  {verdict}")
        print(f"\n  F1 wins {f1_wins}/{len(results)} windows on NP/DD$")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
