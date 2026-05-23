"""ORB x Fractals screening test (spec: docs/superpowers/specs/2026-05-23-orb-fractal-screen-design.md).

Runs 7 configs (baseline + V1/V2/V3 x widths 3/5) across 6 streams.
Reports per-stream + deal-merged portfolio NP/DD$ with live haircut.
Decision: variant advances to WFO iff portfolio haircut-NP/DD$ >= 1.10 x baseline.

Usage:
  python scripts/sim_orb_fractal_screen.py
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
START = datetime(2026, 2, 14, tzinfo=timezone.utc)
END = datetime(2026, 4, 25, tzinfo=timezone.utc)
SPREAD = 30
PER_STREAM_RISK = 1.0   # 6 streams x 1.0% = 6% total per feedback_default_test_conditions

HAIRCUT_NP = 0.94
HAIRCUT_PF = 0.25
ADVANCE_THRESHOLD = 1.10

OUT_DIR = ROOT / "output" / "fractal_screen_2026_05_23"
OUT_DIR.mkdir(parents=True, exist_ok=True)


CONFIGS = [
    ("baseline",       dict()),
    ("V1_trail_w3",    dict(fractal_trail=True,   fractal_width=3)),
    ("V1_trail_w5",    dict(fractal_trail=True,   fractal_width=5)),
    ("V2_confirm_w3",  dict(fractal_confirm=True, fractal_width=3)),
    ("V2_confirm_w5",  dict(fractal_confirm=True, fractal_width=5)),
    ("V3_range_w3",    dict(fractal_range=True,   fractal_width=3)),
    ("V3_range_w5",    dict(fractal_range=True,   fractal_width=5)),
]


def build_cfg(stream: str, flags: dict) -> ORBConfig:
    base = make_stream_cfg(stream, PER_STREAM_RISK)
    for k, v in flags.items():
        setattr(base, k, v)
    return base


def aggregate(deals_with_label):
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gains = 0.0; losses = 0.0; wins = 0; trades = 0
    for _, _s, p in sorted(deals_with_label, key=lambda x: x[0]):
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


def main() -> int:
    print(f"=== ORB x Fractals Screen | {START.date()} -> {END.date()} | {SPREAD}pt | $10k | {PER_STREAM_RISK}%/stream ===")
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        ticks = load_ticks(SYMBOL, START, END, spread_pts=SPREAD)
        m1 = load_bars(SYMBOL, "M1", START, END)
        m5 = load_bars(SYMBOL, "M5", START, END)

        per_stream_rows = []
        portfolio_rows = []

        for cfg_name, flags in CONFIGS:
            merged_deals = []
            for s in ("S1", "S2", "S3", "S4", "S5", "S6"):
                cfg = build_cfg(s, flags)
                r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
                stream_deals = [(d.ts, s, d.pnl) for d in r.deals if d.kind != "entry"]
                agg = aggregate(stream_deals)
                per_stream_rows.append({
                    "config": cfg_name, "stream": s,
                    "np": agg["np"], "dd": agg["dd_abs"], "pf": agg["pf"],
                    "ndd": agg["ndd"], "trades": agg["trades"], "wr": agg["wr"],
                })
                merged_deals.extend(stream_deals)

            port = aggregate(merged_deals)
            np_hc = port["np"] * HAIRCUT_NP
            pf_hc = max(port["pf"] - HAIRCUT_PF, 0.0)
            ndd_hc = np_hc / port["dd_abs"] if port["dd_abs"] > 0 else 0
            portfolio_rows.append({
                "config": cfg_name,
                "np": port["np"], "np_hc": np_hc,
                "dd_abs": port["dd_abs"], "pf": port["pf"], "pf_hc": pf_hc,
                "ndd": port["ndd"], "ndd_hc": ndd_hc,
                "trades": port["trades"], "wr": port["wr"],
            })
            print(f"  {cfg_name:<16} NP=${port['np']:>+8,.0f} (hc ${np_hc:>+8,.0f})  "
                  f"DD=${port['dd_abs']:>7,.0f}  NP/DD$_hc={ndd_hc:>5.2f}  PF={port['pf']:.2f}  trades={port['trades']}")

        per_df = pd.DataFrame(per_stream_rows)
        port_df = pd.DataFrame(portfolio_rows)
        per_df.to_csv(OUT_DIR / "per_stream.csv", index=False)

        # Decision
        baseline_ndd = float(port_df.loc[port_df["config"] == "baseline", "ndd_hc"].iloc[0])
        port_df["advances_to_wfo"] = port_df["ndd_hc"] >= ADVANCE_THRESHOLD * baseline_ndd
        port_df.to_csv(OUT_DIR / "portfolio.csv", index=False)

        # Summary markdown
        lines = [f"# ORB x Fractals Screen -- {START.date()} to {END.date()}", "",
                 f"**Window:** {START.date()} -> {END.date()} ({(END-START).days}d)  ",
                 f"**Spread:** {SPREAD}pt  |  **Deposit:** $10k  |  **Per-stream risk:** {PER_STREAM_RISK}%",
                 f"**Haircut:** NP x {HAIRCUT_NP}, PF - {HAIRCUT_PF}  |  **Advance threshold:** {ADVANCE_THRESHOLD}x baseline NP/DD$_hc",
                 "",
                 "## Portfolio results (deal-merged, haircut applied)", "",
                 "| Config | NP | NP_hc | DD$ | NP/DD$_hc | PF_hc | Trades | Advances? |",
                 "|---|---|---|---|---|---|---|---|"]
        for _, r in port_df.iterrows():
            lines.append(f"| {r['config']} | ${r['np']:+,.0f} | ${r['np_hc']:+,.0f} | "
                         f"${r['dd_abs']:,.0f} | {r['ndd_hc']:.2f} | {r['pf_hc']:.2f} | "
                         f"{int(r['trades'])} | {'YES' if r['advances_to_wfo'] else 'no'} |")
        (OUT_DIR / "summary.md").write_text("\n".join(lines), encoding="utf-8")
        print(f"\nWrote: {OUT_DIR/'per_stream.csv'}, {OUT_DIR/'portfolio.csv'}, {OUT_DIR/'summary.md'}")
    finally:
        kill_mt5_terminal()

    return 0


if __name__ == "__main__":
    sys.exit(main())
