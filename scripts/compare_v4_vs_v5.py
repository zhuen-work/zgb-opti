"""Quick v4 vs v5 portfolio comparison.

Runs the current 6-stream production config twice:
  - v4 equivalent: fractal_confirm = False (current EA behavior)
  - v5 with flag : fractal_confirm = True, fractal_width = 5

Reports per-stream + deal-merged portfolio + live haircut.

Window: 2026-05-02 -> 2026-05-23 (3 weeks, freshest available data).
$10k deposit, 30pt spread, 1.0% per stream (6 streams = 6% total).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from sim_wfo_hedge_retry import STREAM_CFGS, make_stream_cfg

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
START = datetime(2026, 5, 2, tzinfo=timezone.utc)
END = datetime(2026, 5, 23, tzinfo=timezone.utc)
SPREAD = 30
PER_STREAM_RISK = 1.0
HAIRCUT_NP = 0.94
HAIRCUT_PF = 0.25


def build_cfg(stream: str, fractal_on: bool) -> ORBConfig:
    cfg = make_stream_cfg(stream, PER_STREAM_RISK)
    cfg.fractal_confirm = fractal_on
    cfg.fractal_width = 5
    return cfg


def aggregate(deals):
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gains = losses = 0.0; wins = trades = 0
    for ts, _s, p in sorted(deals, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p >= 0: gains += p; wins += 1
        else: losses += -p
        trades += 1
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    ndd = np_ / dd_abs if dd_abs > 0 else 0
    pf = gains / losses if losses > 0 else float("inf")
    wr = wins / trades * 100 if trades > 0 else 0
    return dict(np=np_, dd_pct=dd_pct, dd_abs=dd_abs, ndd=ndd, pf=pf, trades=trades, wr=wr)


def run_one(label, fractal_on, ticks, m1, m5, meta):
    merged = []
    per_s = {}
    for s in ("S1", "S2", "S3", "S4", "S5", "S6"):
        cfg = build_cfg(s, fractal_on)
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        sd = [(d.ts, s, d.pnl) for d in r.deals if d.kind != "entry"]
        agg = aggregate(sd)
        per_s[s] = agg
        merged.extend(sd)
    port = aggregate(merged)
    np_hc = port["np"] * HAIRCUT_NP
    pf_hc = max(port["pf"] - HAIRCUT_PF, 0.0)
    ndd_hc = np_hc / port["dd_abs"] if port["dd_abs"] > 0 else 0
    print(f"\n  === {label} ===")
    print(f"  {'Stream':<5} {'NP':>10} {'DD$':>9} {'PF':>5} {'NP/DD$':>7} {'Trd':>4} {'WR%':>5}")
    for s in ("S1","S2","S3","S4","S5","S6"):
        a = per_s[s]
        print(f"  {s:<5} {'$'+f'{a['np']:+,.0f}':>10} {'$'+f'{a['dd_abs']:,.0f}':>9} "
              f"{a['pf']:>5.2f} {a['ndd']:>7.2f} {a['trades']:>4} {a['wr']:>5.1f}")
    print(f"  {'PORT':<5} {'$'+f'{port['np']:+,.0f}':>10} {'$'+f'{port['dd_abs']:,.0f}':>9} "
          f"{port['pf']:>5.2f} {port['ndd']:>7.2f} {port['trades']:>4} {port['wr']:>5.1f}")
    print(f"  PORT_HC: NP=${np_hc:+,.0f}  PF={pf_hc:.2f}  NP/DD$_hc={ndd_hc:.2f}")
    return dict(per_s=per_s, port=port, np_hc=np_hc, pf_hc=pf_hc, ndd_hc=ndd_hc)


def main():
    print(f"=== v4 vs v5 portfolio | {START.date()} -> {END.date()} | {SPREAD}pt | $10k | {PER_STREAM_RISK}%/stream ===")
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        ticks = load_ticks(SYMBOL, START, END, spread_pts=SPREAD)
        m1 = load_bars(SYMBOL, "M1", START, END)
        m5 = load_bars(SYMBOL, "M5", START, END)

        v4 = run_one("v4 (FractalConfirm=false)", False, ticks, m1, m5, meta)
        v5 = run_one("v5 (FractalConfirm=true, width=5)", True, ticks, m1, m5, meta)

        print(f"\n  === DELTA (v5 - v4) ===")
        d_np = v5["port"]["np"] - v4["port"]["np"]
        d_np_hc = v5["np_hc"] - v4["np_hc"]
        d_dd = v5["port"]["dd_abs"] - v4["port"]["dd_abs"]
        d_pf = v5["port"]["pf"] - v4["port"]["pf"]
        d_ndd = v5["ndd_hc"] - v4["ndd_hc"]
        d_trd = v5["port"]["trades"] - v4["port"]["trades"]
        mult_np = (v5["port"]["np"] / v4["port"]["np"]) if v4["port"]["np"] != 0 else 0
        mult_ndd = (v5["ndd_hc"] / v4["ndd_hc"]) if v4["ndd_hc"] != 0 else 0
        print(f"  NP:        ${d_np:+,.0f}  ({mult_np:.2f}x)")
        print(f"  NP_hc:     ${d_np_hc:+,.0f}")
        print(f"  DD$:       ${d_dd:+,.0f}")
        print(f"  PF:        {d_pf:+.2f}")
        print(f"  NP/DD$_hc: {d_ndd:+.2f}  ({mult_ndd:.2f}x)")
        print(f"  Trades:    {d_trd:+d}  (v5 declines low-quality breaks)")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
