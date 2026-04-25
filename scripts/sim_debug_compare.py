"""Side-by-side comparison of sim deals vs MT5 Model=4 deals.

Produces a first-divergence report focused on a single trading day, so we can
pinpoint the sim bug causing the trade-count gap.

Workflow:
  1. Run sim with debug logging on reference config (if output not already present)
  2. Parse MT5 .htm deals for the same config
  3. Produce per-day deal counts and a first-divergence trace

Usage: python scripts/sim_debug_compare.py [--rerun] [--day YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import load_ticks, load_bars, symbol_meta, kill_mt5_terminal
from zgb_sim.scalper_v1 import S1Config, SymbolMeta, simulate
from sim_parse_mt5_deals import parse_mt5_deals


MT5_REPORT = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400/scalp_s1_only_model4.htm")
SIM_OUT_BASE = ROOT / "output" / "sim_debug" / "s1_reference"


def run_sim():
    symbol = "XAUUSD"
    start = datetime(2026, 3, 7, tzinfo=timezone.utc)
    end   = datetime(2026, 4, 18, tzinfo=timezone.utc)

    print("Loading meta...")
    m = symbol_meta(symbol)
    meta = SymbolMeta(
        point=m["point"], digits=m["digits"],
        tick_size=m["tick_size"], tick_value=m["tick_value"],
        stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
        volume_max=m["volume_max"], volume_step=m["volume_step"],
    )
    print(f"  {m}")

    print(f"Loading ticks {start.date()} -> {end.date()}...")
    t0 = time.time()
    ticks = load_ticks(symbol, start, end)
    print(f"  {len(ticks):,} ticks ({time.time()-t0:.1f}s)")

    m1 = load_bars(symbol, "M1", start, end)
    m5 = load_bars(symbol, "M5", start, end)
    print(f"  M1={len(m1):,}  M5={len(m5):,}")

    cfg = S1Config(
        risk_pct=1.0, donchian_bars=20,
        take_profit_pts=200, stop_loss_pts=50, half_tp_ratio=0.6,
        pending_expire_bars=2,
        start_hour=14, end_hour=22,
        daily_target_pct=3.0, daily_loss_pct=6.0,
        block_fri_pm=True, hedge_mode=False,
    )

    SIM_OUT_BASE.parent.mkdir(parents=True, exist_ok=True)
    print("Running sim with debug logging...")
    t0 = time.time()
    r = simulate(ticks, m5, m1, cfg, meta, initial_balance=100.0,
                 debug_path=str(SIM_OUT_BASE))
    print(f"  Sim done in {time.time()-t0:.1f}s")
    print(f"  {r.summary()}")
    return r


def compare(target_day: str | None):
    # MT5 side
    print(f"\nParsing MT5 report: {MT5_REPORT}")
    mt5_deals, mt5_orders = parse_mt5_deals(MT5_REPORT)
    mt5_deals["ts"] = pd.to_datetime(mt5_deals["ts"])
    mt5_orders["ts_open"] = pd.to_datetime(mt5_orders["ts_open"])
    print(f"  MT5 deals: {len(mt5_deals):,}  orders: {len(mt5_orders):,}")

    # Sim side
    sim_deals = pd.read_csv(SIM_OUT_BASE.with_suffix(".deals.csv"))
    sim_orders = pd.read_csv(SIM_OUT_BASE.with_suffix(".orders.csv"))
    sim_placements = pd.read_csv(SIM_OUT_BASE.with_suffix(".placements.csv"))
    sim_deals["ts"] = pd.to_datetime(sim_deals["ts"])
    sim_orders["ts"] = pd.to_datetime(sim_orders["ts"])
    sim_placements["bar_ts"] = pd.to_datetime(sim_placements["bar_ts"])
    print(f"  Sim deals: {len(sim_deals):,}  order_events: {len(sim_orders):,}  placements: {len(sim_placements):,}")

    # Per-day summary
    print("\n=== Per-day summary ===")
    mt5_daily = mt5_deals[mt5_deals.direction == "out"].groupby(mt5_deals["ts"].dt.date).size()
    sim_exits = sim_deals[sim_deals.kind != "entry"]
    sim_daily = sim_exits.groupby(sim_exits["ts"].dt.date).size()
    all_days = sorted(set(mt5_daily.index) | set(sim_daily.index))
    print(f"  {'Day':<12} {'MT5':>5} {'Sim':>5} {'Diff':>5}")
    for d in all_days:
        m = int(mt5_daily.get(d, 0))
        s = int(sim_daily.get(d, 0))
        print(f"  {str(d):<12} {m:>5} {s:>5} {s-m:>+5}")

    # Focus on target day
    if target_day is None:
        # First common trading day from MT5
        target_day = str(sorted(mt5_daily.index)[0])
    tgt = pd.Timestamp(target_day).date()
    print(f"\n=== Deep-dive on {tgt} ===")

    mt5_day = mt5_deals[mt5_deals["ts"].dt.date == tgt].reset_index(drop=True)
    sim_day_deals = sim_deals[sim_deals["ts"].dt.date == tgt].reset_index(drop=True)
    sim_day_pl = sim_placements[sim_placements["bar_ts"].dt.date == tgt].reset_index(drop=True)
    mt5_day_orders = mt5_orders[mt5_orders["ts_open"].dt.date == tgt].reset_index(drop=True)

    print(f"\nMT5 placements (orders opened) on {tgt}:")
    mt5_placed = mt5_day_orders[mt5_day_orders.type.str.endswith("_stop") | mt5_day_orders.type.str.endswith("_limit")]
    print(f"  {len(mt5_placed)} pending orders placed")
    if not mt5_placed.empty:
        by_minute = mt5_placed.groupby(mt5_placed["ts_open"].dt.floor("min")).size().head(10)
        print("  First 10 placement-minutes:")
        print(by_minute.to_string())

    print(f"\nSim placements (M1 bars that placed orders) on {tgt}:")
    print(f"  {len(sim_day_pl)} placement events")
    if not sim_day_pl.empty:
        print(sim_day_pl.head(10)[["bar_ts", "donch_high", "donch_low", "ask", "bid", "break_buy", "break_sell"]].to_string())

    print(f"\nFirst 15 MT5 deals on {tgt}:")
    print(mt5_day.head(15)[["ts", "direction", "type", "lots", "price", "profit", "tag"]].to_string())

    print(f"\nFirst 15 sim deals on {tgt}:")
    print(sim_day_deals.head(15)[["ts", "kind", "direction", "lots", "price", "pnl"]].to_string())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rerun", action="store_true", help="Force rerun the sim even if outputs exist")
    ap.add_argument("--day", default=None, help="YYYY-MM-DD target day for deep-dive")
    args = ap.parse_args()

    try:
        deals_file = SIM_OUT_BASE.with_suffix(".deals.csv")
        if args.rerun or not deals_file.exists():
            run_sim()
        else:
            print(f"Reusing existing sim output at {SIM_OUT_BASE.with_suffix('.deals.csv')}")
        compare(args.day)
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
