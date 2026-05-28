"""3-way comparison: may23 WFO at spread=30/60/120 pt on live forward window.

Loads top-6 from each of the three WFOs, runs each setfile on live XAUUSD.sc
ticks for 2026-05-23 -> now using v7 architecture + 30pt test spread (live-match).
Reports portfolio NP, DD, NDD, trades per WFO + per-stream breakdown.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast
from zgb_sim.tick_loader import kill_mt5_terminal
from sim_orb_oos_today import fetch_window, fetch_meta
from sim_wfo_hedge_reverse import StopExtensionCfg, simulate_stop_extension_hedges
from sim_orb_oos_today_hedge_v6 import extract_sl_events, ts_arr_from_ticks


DEPOSIT = 10_000.0
LIVE_SPREAD = 30
RISK_PCT = 1.5

START = datetime(2026, 5, 23, tzinfo=timezone.utc)
END   = datetime.now(timezone.utc)

V7 = dict(fractal_confirm=True, fractal_width=5,
          sma_cross_exit=True, sma_cross_fast=8, sma_cross_slow=21)
HCFG = StopExtensionCfg(exp_min=240, f1_sec=1800, ext_pts=100, tp_mult=3.0, sl_mult=1.0)

WFO_RANKS = {
    "spread30":  ROOT / "output/wfo_orb_v2_may23_spread30/oos_rank.csv",
    "spread60":  ROOT / "output/wfo_orb_v2_may23/oos_rank.csv",
    "spread120": ROOT / "output/wfo_orb_v2_may23_spread120/oos_rank.csv",
}


def cfg_from_row(row) -> ORBConfig:
    return ORBConfig(
        risk_pct=RISK_PCT,
        range_minutes=int(row["range_minutes"]), buffer_pts=0,
        min_range_pts=0, max_range_pts=999_999,
        fixed_sl_pts=int(row["fixed_sl_pts"]),
        rr_ratio=float(row["rr_ratio"]),
        half_tp_ratio=round(float(row["half_tp_ratio"]), 2),
        pending_expire_minutes=int(row["pending_expire_minutes"]),
        daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13,
        **V7, comment="ORB",
    )


def top_n(rank_csv: Path, n: int) -> list[ORBConfig]:
    df = pd.read_csv(rank_csv).head(n)
    return [cfg_from_row(r) for _, r in df.iterrows()]


def run_portfolio(cfgs, ticks, m1, m5, meta, ticks_arr):
    deals = []
    per_stream = []
    for sn, cfg in enumerate(cfgs, start=1):
        r = simulate_fast(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        parent_pnls, sl_events = extract_sl_events(r.deals)
        parent_np = sum(p for _, p in parent_pnls)
        for ts, p in parent_pnls:
            deals.append((ts, sn, p))
        sm = {"magic": sn, "risk_pct": RISK_PCT,
              "fixed_sl_pts": cfg.fixed_sl_pts, "rr_ratio": cfg.rr_ratio,
              "half_tp_ratio": cfg.half_tp_ratio,
              "range_minutes": cfg.range_minutes,
              "pending_expire_minutes": cfg.pending_expire_minutes}
        hpn = simulate_stop_extension_hedges(sl_events, ticks_arr, sm, HCFG)
        hedge_np = sum(p for _, p in hpn)
        for ts_ns, p in hpn:
            deals.append((int(ts_ns), sn, p))
        per_stream.append({
            "stream": f"S{sn}",
            "params": f"R{cfg.range_minutes:<3} SL{cfg.fixed_sl_pts:<4} RR{cfg.rr_ratio:<4} HTP{cfg.half_tp_ratio:<4} E{cfg.pending_expire_minutes:<4}",
            "parent_np": parent_np, "hedge_np": hedge_np,
            "combined": parent_np + hedge_np,
            "parent_trades": len(parent_pnls), "hedge_n": len(hpn),
        })
    deals.sort(key=lambda x: x[0])
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gains = losses = 0.0; wins = 0
    for _, _sn, p in deals:
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p >= 0: gains += p; wins += 1
        else: losses += -p
    np_ = bal - DEPOSIT
    pf = gains / losses if losses > 0 else float("inf")
    ndd = np_ / dd_abs if dd_abs > 0 else 0.0
    return {"np": np_, "dd": dd_abs, "ndd": ndd, "pf": pf,
            "trades": len(deals), "wins": wins, "per_stream": per_stream}


def main():
    print("=" * 110)
    print(f"  may23 WFO @ spread 30 vs 60 vs 120 pt — forward live-tick comparison")
    print(f"  Test window: {START.date()} -> {END.strftime('%Y-%m-%d %H:%M UTC')} "
          f"({(END-START).days}d)  Test spread: {LIVE_SPREAD}pt (live-match)")
    print(f"  v7 architecture, $10k base, 9% risk (1.5%/stream × 6)")
    print("=" * 110)

    top6 = {}
    for label, rank_csv in WFO_RANKS.items():
        if not rank_csv.exists():
            print(f"  [{label}] rank CSV missing: {rank_csv}")
            top6[label] = None
            continue
        top6[label] = top_n(rank_csv, 6)
        print(f"\n  {label} top-6:")
        for i, c in enumerate(top6[label], 1):
            print(f"    S{i}: R{c.range_minutes:<3} SL{c.fixed_sl_pts:<4} "
                  f"RR{c.rr_ratio:<4} HTP{c.half_tp_ratio:<4} E{c.pending_expire_minutes:<4}")

    if not all(top6.values()):
        print("  Missing one or more rank CSVs — aborting.")
        return 1

    sym, m = fetch_meta(None, account="live")
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])
    sym, ticks, m1, m5 = fetch_window(sym, START, END, LIVE_SPREAD, account="live")
    ticks_arr = ts_arr_from_ticks(ticks)
    print(f"\n  Loaded {len(ticks):,} ticks, {len(m5):,} M5 bars")

    results = {}
    for label in ("spread30", "spread60", "spread120"):
        print(f"\n  Running {label} portfolio sim...")
        r = run_portfolio(top6[label], ticks, m1, m5, meta, ticks_arr)
        results[label] = r
        print(f"    NP=${r['np']:+,.0f}  DD=${r['dd']:,.0f}  NDD={r['ndd']:.2f}  PF={r['pf']:.2f}  trades={r['trades']}")

    kill_mt5_terminal()

    # Reporting
    print()
    print("=" * 110)
    print(f"  3-WAY PORTFOLIO COMPARISON (live-tick forward, {LIVE_SPREAD}pt test spread)")
    print("=" * 110)
    print(f"  {'Metric':<18}  {'spread30':>14}  {'spread60':>14}  {'spread120':>14}  {'Winner':<20}")
    print(f"  {'-' * 92}")
    metrics = [
        ("Net Profit",  "np",  "${:>+12,.0f}", "max"),
        ("Drawdown $",  "dd",  "${:>+12,.0f}", "min"),
        ("NP/DD$",      "ndd", "{:>14.2f}",    "max"),
        ("Profit Factor","pf", "{:>14.2f}",    "max"),
        ("Trades",      "trades","{:>14d}",     "—"),
        ("Wins",        "wins", "{:>14d}",     "—"),
    ]
    for name, k, fmt, prefer in metrics:
        vals = {l: results[l][k] for l in ("spread30","spread60","spread120")}
        if prefer == "max":
            winner = max(vals, key=vals.get)
        elif prefer == "min":
            winner = min(vals, key=vals.get)
        else:
            winner = "—"
        row = f"  {name:<18}  " + "  ".join(fmt.format(vals[l]) for l in ("spread30","spread60","spread120"))
        row += f"  {winner:<20}"
        print(row)
    print()

    # Per-stream
    print("  --- Per-stream combined NP ---")
    print(f"  {'Stream':<7}  {'spread30 params':<32}  {'NP':>9}  | "
          f"{'spread60 params':<32}  {'NP':>9}  | "
          f"{'spread120 params':<32}  {'NP':>9}")
    for s30, s60, s120 in zip(results["spread30"]["per_stream"],
                                results["spread60"]["per_stream"],
                                results["spread120"]["per_stream"]):
        print(f"  {s30['stream']:<7}  {s30['params']:<32}  ${s30['combined']:>+8,.0f}  | "
              f"{s60['params']:<32}  ${s60['combined']:>+8,.0f}  | "
              f"{s120['params']:<32}  ${s120['combined']:>+8,.0f}")
    print("=" * 110)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
