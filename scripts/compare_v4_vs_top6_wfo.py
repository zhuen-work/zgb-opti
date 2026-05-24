"""Clean head-to-head: v4 current rotation vs top-6 from latest WFO.

Drops the "rotation" pattern entirely. v5 portfolio = top 6 ranked configs
from output/wfo_orb_v5_expire_extend/oos_rank.csv used as S1..S6 directly.

Window: May 2 -> May 23 (3 weeks). Spread: 30pt sanity. Risk: 1.0%/stream.
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

# Top-6 by rank from output/wfo_orb_v5_expire_extend/oos_rank.csv
# Format: (stream_label, sl, rr, htp, expire, prof_count_in_wfo, wfo_np_dd)
WFO_TOP6 = [
    ("S1", 550, 4.0, 0.2,  720, "4/4", 3875),
    ("S2", 400, 3.5, 0.4,  240, "4/4", 3498),
    ("S3", 550, 4.0, 0.2,  480, "4/4", 4039),
    ("S4", 550, 4.0, 0.2,  240, "4/4", 3587),
    ("S5", 550, 2.0, 0.4,  720, "4/4", 3632),
    ("S6", 400, 3.5, 0.4, 1440, "3/4", 3317),
]


def build_v4_cfg(stream: str) -> ORBConfig:
    cfg = make_stream_cfg(stream, PER_STREAM_RISK)
    cfg.fractal_confirm = False
    return cfg


def build_top6_cfg(stream: str) -> ORBConfig:
    for (label, sl, rr, htp, exp, _, _) in WFO_TOP6:
        if label == stream:
            return ORBConfig(
                risk_pct=PER_STREAM_RISK,
                range_minutes=90, buffer_pts=0,
                min_range_pts=0, max_range_pts=999_999,
                fixed_sl_pts=sl, rr_ratio=rr, half_tp_ratio=htp,
                pending_expire_minutes=exp,
                daily_target_pct=0.0, daily_loss_pct=0.0,
                ldn_enabled=True, ldn_start_hour=7,
                ny_enabled=True, ny_start_hour=13,
                fractal_confirm=True, fractal_width=5,
                comment=stream,
            )
    raise ValueError(f"no top6 entry for {stream}")


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
    ndd = np_ / dd_abs if dd_abs > 0 else 0
    pf = gains / losses if losses > 0 else float("inf")
    wr = wins / trades * 100 if trades > 0 else 0
    return dict(np=np_, dd_abs=dd_abs, ndd=ndd, pf=pf, trades=trades, wr=wr)


def run_one(label, build_fn, ticks, m1, m5, meta):
    merged = []
    per_s = {}
    for s in ("S1","S2","S3","S4","S5","S6"):
        cfg = build_fn(s)
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        sd = [(d.ts, s, d.pnl) for d in r.deals if d.kind != "entry"]
        per_s[s] = aggregate(sd)
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
    print(f"=== v4 current rotation vs top-6-from-LATEST-WFO (no rotation) ===")
    print(f"=== {START.date()} -> {END.date()} | {SPREAD}pt | $10k | {PER_STREAM_RISK}%/stream ===")
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        ticks = load_ticks(SYMBOL, START, END, spread_pts=SPREAD)
        m1 = load_bars(SYMBOL, "M1", START, END)
        m5 = load_bars(SYMBOL, "M5", START, END)

        print(f"\n  --- v4 streams (current rotation: S1-S3 from MAY16 WFO, S4-S6 from MAY23 WFO) ---")
        for s in ("S1","S2","S3","S4","S5","S6"):
            sc = STREAM_CFGS[s]
            print(f"  {s}: SL={sc['fixed_sl_pts']:>4} RR={sc['rr_ratio']:<3} HTP={sc['half_tp_ratio']:<3} Exp=240 (default)")
        print(f"\n  --- v5 streams (top-6 from output/wfo_orb_v5_expire_extend/oos_rank.csv) ---")
        for (s, sl, rr, htp, exp, prof, ndd) in WFO_TOP6:
            print(f"  {s}: SL={sl:>4} RR={rr:<3} HTP={htp:<3} Exp={exp:>4}  (WFO rank prof={prof} NP/DD$={ndd})")

        v4 = run_one("v4 current rotation (fractal OFF)",                 build_v4_cfg,    ticks, m1, m5, meta)
        v5 = run_one("v5 top-6-from-LATEST-WFO (fractal ON, no rotation)", build_top6_cfg, ticks, m1, m5, meta)

        print(f"\n  === DELTA (v5 vs v4) ===")
        dnp = v5["port"]["np"] - v4["port"]["np"]
        ddd = v5["port"]["dd_abs"] - v4["port"]["dd_abs"]
        dnpdd = v5["ndd_hc"] - v4["ndd_hc"]
        dtrd = v5["port"]["trades"] - v4["port"]["trades"]
        mult_np = (v5["port"]["np"]/v4["port"]["np"]) if v4["port"]["np"]!=0 else 0
        mult_npdd = (v5["ndd_hc"]/v4["ndd_hc"]) if v4["ndd_hc"]!=0 else 0
        print(f"  NP:        ${dnp:+,.0f}  ({mult_np:.2f}x)")
        print(f"  DD$:       ${ddd:+,.0f}")
        print(f"  NP/DD$_hc: {dnpdd:+.2f}  ({mult_npdd:.2f}x)")
        print(f"  Trades:    {dtrd:+d}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
