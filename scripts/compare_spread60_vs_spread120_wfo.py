"""Compare may23 WFO winners selected at SPREAD=60pt vs SPREAD=120pt
(legacy file — for 3-way comparison incl. 30pt see scripts/compare_3way_spread_wfos.py)
on real live tick data over the forward window 2026-05-23 -> now.

Both setfiles use v7 architecture (fractal_confirm + SMA(8,21) cross-exit +
STOP-on-extension hedge), $10k base, 9% risk, deal-merged portfolio.

Forward test spread = 30pt (live-match per feedback_sim_vs_live_calibration).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone, date
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
        stream_meta = {
            "magic": sn, "risk_pct": RISK_PCT,
            "fixed_sl_pts": cfg.fixed_sl_pts, "rr_ratio": cfg.rr_ratio,
            "half_tp_ratio": cfg.half_tp_ratio,
            "range_minutes": cfg.range_minutes,
            "pending_expire_minutes": cfg.pending_expire_minutes,
        }
        hedge_pnls_ns = simulate_stop_extension_hedges(sl_events, ticks_arr, stream_meta, HCFG)
        hedge_np = sum(p for _, p in hedge_pnls_ns)
        for ts_ns, p in hedge_pnls_ns:
            deals.append((int(ts_ns), sn, p))
        per_stream.append({
            "stream": f"S{sn}",
            "params": f"R{cfg.range_minutes} SL{cfg.fixed_sl_pts} RR{cfg.rr_ratio} HTP{cfg.half_tp_ratio} E{cfg.pending_expire_minutes}",
            "parent_np": parent_np, "hedge_np": hedge_np,
            "combined": parent_np + hedge_np,
            "parent_trades": len(parent_pnls), "hedge_n": len(hedge_pnls_ns),
        })
    deals.sort(key=lambda x: x[0])
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0; wins = 0
    for _, _sn, p in deals:
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p >= 0: wins += 1
    np_ = bal - DEPOSIT
    ndd = np_ / dd_abs if dd_abs > 0 else 0.0
    return {"np": np_, "dd": dd_abs, "ndd": ndd, "trades": len(deals),
            "wins": wins, "per_stream": per_stream}


def main():
    rank60 = ROOT / "output/wfo_orb_v2_may23/oos_rank.csv"
    rank120 = ROOT / "output/wfo_orb_v2_may23_spread120/oos_rank.csv"

    top6_60 = top_n(rank60, 6)
    top6_120 = top_n(rank120, 6)

    print("=" * 110)
    print(f"  may23 WFO winners: spread=60pt vs spread=120pt — forward live-tick comparison")
    print(f"  Test window: {START.date()} -> {END.strftime('%Y-%m-%d %H:%M UTC')}  "
          f"({(END-START).days}d)")
    print(f"  Test spread: {LIVE_SPREAD}pt (live-match)  v7 architecture  $10k base  9% risk")
    print("=" * 110)
    print()
    print("  60pt WFO top-6 picks:")
    for i, c in enumerate(top6_60, 1):
        print(f"    S{i}: R{c.range_minutes:>3} SL{c.fixed_sl_pts:>4} RR{c.rr_ratio:<4} "
              f"HTP{c.half_tp_ratio:<4} E{c.pending_expire_minutes}")
    print()
    print("  120pt WFO top-6 picks:")
    for i, c in enumerate(top6_120, 1):
        print(f"    S{i}: R{c.range_minutes:>3} SL{c.fixed_sl_pts:>4} RR{c.rr_ratio:<4} "
              f"HTP{c.half_tp_ratio:<4} E{c.pending_expire_minutes}")

    sym, m = fetch_meta(None, account="live")
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])
    sym, ticks, m1, m5 = fetch_window(sym, START, END, LIVE_SPREAD, account="live")
    ticks_arr = ts_arr_from_ticks(ticks)
    print(f"\n  Loaded {len(ticks):,} ticks, {len(m5):,} M5 bars")

    print("\n  Running 60pt-winners portfolio...")
    r60 = run_portfolio(top6_60, ticks, m1, m5, meta, ticks_arr)
    print(f"    NP=${r60['np']:+,.0f}  DD=${r60['dd']:,.0f}  NDD={r60['ndd']:.2f}  "
          f"trades={r60['trades']}")

    print("  Running 120pt-winners portfolio...")
    r120 = run_portfolio(top6_120, ticks, m1, m5, meta, ticks_arr)
    print(f"    NP=${r120['np']:+,.0f}  DD=${r120['dd']:,.0f}  NDD={r120['ndd']:.2f}  "
          f"trades={r120['trades']}")
    kill_mt5_terminal()

    delta_np = r120["np"] - r60["np"]
    delta_pct = (delta_np / abs(r60["np"]) * 100) if r60["np"] else 0.0

    print()
    print("=" * 110)
    print(f"  PORTFOLIO COMPARISON (forward live-tick test, {LIVE_SPREAD}pt spread)")
    print("=" * 110)
    print(f"  {'Metric':<20}  {'60pt-WFO':>14}  {'120pt-WFO':>14}  {'Diff (120-60)':>16}")
    print(f"  {'-' * 70}")
    print(f"  {'Net Profit':<20}  ${r60['np']:>+12,.2f}  ${r120['np']:>+12,.2f}  "
          f"${delta_np:>+14,.2f}  ({delta_pct:+.1f}%)")
    print(f"  {'Drawdown $':<20}  ${r60['dd']:>+12,.2f}  ${r120['dd']:>+12,.2f}  "
          f"${r120['dd']-r60['dd']:>+14,.2f}")
    print(f"  {'NP/DD$':<20}  {r60['ndd']:>14.2f}  {r120['ndd']:>14.2f}  "
          f"{r120['ndd']-r60['ndd']:>+16.2f}")
    print(f"  {'Trades':<20}  {r60['trades']:>14d}  {r120['trades']:>14d}  "
          f"{r120['trades']-r60['trades']:>+16d}")
    print(f"  {'Wins':<20}  {r60['wins']:>14d}  {r120['wins']:>14d}  "
          f"{r120['wins']-r60['wins']:>+16d}")
    print()
    if delta_np > 0:
        print(f"  >>> 120pt WFO picks WIN on live forward test by ${delta_np:+,.0f}")
    elif delta_np < 0:
        print(f"  >>> 60pt WFO picks WIN on live forward test by ${-delta_np:+,.0f}")
    else:
        print(f"  >>> tied")

    print()
    print("  --- Per-stream breakdown ---")
    print(f"  {'Stream':<7} {'60pt params':<30} {'60 NP':>9} | "
          f"{'120pt params':<30} {'120 NP':>9}")
    for s60, s120 in zip(r60["per_stream"], r120["per_stream"]):
        print(f"  {s60['stream']:<7} {s60['params']:<30} ${s60['combined']:>+8,.0f} | "
              f"{s120['params']:<30} ${s120['combined']:>+8,.0f}")
    print("=" * 110)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
