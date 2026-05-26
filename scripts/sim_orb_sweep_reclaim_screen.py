# scripts/sim_orb_sweep_reclaim_screen.py
"""SR_v1 sweep-and-reclaim Stage-1 screen (spec: docs/superpowers/specs/2026-05-27-sweep-reclaim-v1-design.md).

Runs 3 configs across the 6-stream architecture:
  - baseline : current ORB portfolio (orb_simulate)
  - SR_stop  : 6 SR streams, V_stop entry,  ORB OFF
  - SR_limit : 6 SR streams, V_limit entry, ORB OFF

Emits per-stream + portfolio-haircut metrics and a PASS/PARTIAL/REJECT decision.

Usage:
  python scripts/sim_orb_sweep_reclaim_screen.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.sweep_reclaim import simulate as sr_simulate, SRConfig

from sim_wfo_hedge_retry import STREAM_CFGS, make_stream_cfg

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
START = datetime(2026, 2, 14, tzinfo=timezone.utc)
END = datetime(2026, 4, 25, tzinfo=timezone.utc)
SPREAD = 30
PER_STREAM_RISK = 1.0

HAIRCUT_NP = 0.94
HAIRCUT_PF = 0.25

MIN_HAIRCUT_NDD = 0.50
MAX_PAIR_CORR   = 0.30

OUT_DIR = ROOT / "output" / "sweep_reclaim_screen_2026_05_27"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def aggregate(deals):
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gains = 0.0; losses = 0.0; wins = 0; trades = 0
    for _, _s, p in sorted(deals, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p >= 0: gains += p; wins += 1
        else:      losses += -p
        trades += 1
    np_ = bal - DEPOSIT
    ndd = np_ / dd_abs if dd_abs > 0 else 0
    pf = gains / losses if losses > 0 else float("inf")
    wr = wins / trades * 100 if trades > 0 else 0
    return dict(np=np_, dd_abs=dd_abs, ndd=ndd, pf=pf, trades=trades, wr=wr)


def weekly_np(deals):
    if not deals:
        return {}
    df = pd.DataFrame(deals, columns=["ts", "stream", "pnl"])
    df["week"] = pd.to_datetime(df["ts"]).dt.to_period("W").dt.start_time
    return df.groupby("week")["pnl"].sum().to_dict()


def pair_correlation(sr_weekly_by_stream, orb_weekly_by_stream):
    rows = []
    for s in ("S1","S2","S3","S4","S5","S6"):
        sr  = sr_weekly_by_stream.get(s, {})
        orb = orb_weekly_by_stream.get(s, {})
        weeks = sorted(set(sr.keys()) | set(orb.keys()))
        a = np.array([sr.get(w, 0.0)  for w in weeks])
        b = np.array([orb.get(w, 0.0) for w in weeks])
        if len(weeks) < 2 or a.std() == 0 or b.std() == 0:
            corr = float("nan")
        else:
            corr = float(np.corrcoef(a, b)[0, 1])
        rows.append({"stream": s, "weeks": len(weeks), "corr": corr})
    df = pd.DataFrame(rows)
    mean_c = float(df["corr"].dropna().mean()) if df["corr"].notna().any() else float("nan")
    return df, mean_c


def run_baseline(ticks, m5, m1, meta):
    merged = []
    per = []
    weekly_by_stream = {}
    for s in ("S1","S2","S3","S4","S5","S6"):
        cfg = make_stream_cfg(s, PER_STREAM_RISK)
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        deals = [(d.ts, s, d.pnl) for d in r.deals if d.kind != "entry"]
        agg = aggregate(deals)
        per.append({"config":"baseline","stream":s,
                    "np":agg["np"],"dd":agg["dd_abs"],"pf":agg["pf"],
                    "ndd":agg["ndd"],"trades":agg["trades"],"wr":agg["wr"],
                    "skip_no_sweep":0,"expire_no_fill":0})
        merged.extend(deals)
        weekly_by_stream[s] = weekly_np(deals)
    return per, aggregate(merged), weekly_by_stream


def run_sr(m5, m1, meta, mode):
    merged = []
    per = []
    weekly_by_stream = {}
    for s in ("S1","S2","S3","S4","S5","S6"):
        parent_cfg = make_stream_cfg(s, PER_STREAM_RISK)
        sr_cfg = SRConfig(risk_pct=PER_STREAM_RISK, mode=mode, buffer_pts=0)
        r = sr_simulate(m5, m1, parent_cfg, sr_cfg, meta, initial_balance=DEPOSIT)
        deals = [(d.ts, s, d.pnl) for d in r.deals]
        agg = aggregate(deals)
        per.append({"config":f"SR_{mode}","stream":s,
                    "np":agg["np"],"dd":agg["dd_abs"],"pf":agg["pf"],
                    "ndd":agg["ndd"],"trades":agg["trades"],"wr":agg["wr"],
                    "skip_no_sweep":r.skip_no_sweep,
                    "expire_no_fill":r.expire_no_fill})
        merged.extend(deals)
        weekly_by_stream[s] = weekly_np(deals)
    return per, aggregate(merged), weekly_by_stream


def decision(port_hc_ndd, mean_corr):
    if port_hc_ndd <= 0 or port_hc_ndd < MIN_HAIRCUT_NDD:
        return "REJECT"
    if not np.isnan(mean_corr) and mean_corr > MAX_PAIR_CORR:
        return "PARTIAL"
    return "PASS"


def main() -> int:
    print(f"=== SR_v1 Screen | {START.date()} -> {END.date()} | {SPREAD}pt | $10k | {PER_STREAM_RISK}%/stream ===")
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        ticks = load_ticks(SYMBOL, START, END, spread_pts=SPREAD)
        m1 = load_bars(SYMBOL, "M1", START, END)
        m5 = load_bars(SYMBOL, "M5", START, END)

        baseline_per, baseline_port, baseline_weekly = run_baseline(ticks, m5, m1, meta)
        sr_stop_per,  sr_stop_port,  sr_stop_weekly  = run_sr(m5, m1, meta, "stop")
        sr_limit_per, sr_limit_port, sr_limit_weekly = run_sr(m5, m1, meta, "limit")

        per_df = pd.DataFrame(baseline_per + sr_stop_per + sr_limit_per)
        per_df.to_csv(OUT_DIR / "per_stream.csv", index=False)

        port_rows = []
        for name, port, weekly in [
            ("baseline", baseline_port, None),
            ("SR_stop",  sr_stop_port,  sr_stop_weekly),
            ("SR_limit", sr_limit_port, sr_limit_weekly),
        ]:
            np_hc  = port["np"] * HAIRCUT_NP
            pf_hc  = max(port["pf"] - HAIRCUT_PF, 0.0)
            ndd_hc = np_hc / port["dd_abs"] if port["dd_abs"] > 0 else 0
            mean_corr = float("nan")
            if weekly is not None:
                _, mean_corr = pair_correlation(weekly, baseline_weekly)
            dec = ("baseline" if name == "baseline"
                   else decision(ndd_hc, mean_corr))
            port_rows.append({
                "config": name, "np": port["np"], "np_hc": np_hc,
                "dd_abs": port["dd_abs"], "pf": port["pf"], "pf_hc": pf_hc,
                "ndd": port["ndd"], "ndd_hc": ndd_hc,
                "mean_pair_corr": mean_corr,
                "trades": port["trades"], "wr": port["wr"],
                "decision": dec,
            })
            print(f"  {name:<10} NP=${port['np']:>+8,.0f} (hc ${np_hc:>+8,.0f})  "
                  f"DD=${port['dd_abs']:>7,.0f}  NP/DD$_hc={ndd_hc:>5.2f}  "
                  f"corr={mean_corr:>+.2f}  trades={port['trades']}  -> {dec}")
        port_df = pd.DataFrame(port_rows)
        port_df.to_csv(OUT_DIR / "portfolio.csv", index=False)

        for name, weekly in [("SR_stop", sr_stop_weekly),
                              ("SR_limit", sr_limit_weekly)]:
            df, _ = pair_correlation(weekly, baseline_weekly)
            df.insert(0, "config", name)
            (df.to_csv(OUT_DIR / f"pair_corr_{name}.csv", index=False))

        lines = [
            f"# SR_v1 Sweep-and-Reclaim Screen -- {START.date()} to {END.date()}",
            "",
            f"**Window:** {START.date()} -> {END.date()} ({(END-START).days}d)  ",
            f"**Spread:** {SPREAD}pt  |  **Deposit:** $10k  |  **Per-stream risk:** {PER_STREAM_RISK}%",
            f"**Haircut:** NP x {HAIRCUT_NP}, PF - {HAIRCUT_PF}  ",
            f"**Stage-1 thresholds:** min haircut-NP/DD$ >= {MIN_HAIRCUT_NDD}, mean pair corr <= {MAX_PAIR_CORR}",
            "",
            "## Portfolio results (deal-merged, haircut applied)", "",
            "| Config | NP | NP_hc | DD$ | NP/DD$_hc | PF_hc | MeanCorr | Trades | Decision |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for _, r in port_df.iterrows():
            corr_str = f"{r['mean_pair_corr']:+.2f}" if not np.isnan(r['mean_pair_corr']) else "n/a"
            lines.append(f"| {r['config']} | ${r['np']:+,.0f} | ${r['np_hc']:+,.0f} | "
                         f"${r['dd_abs']:,.0f} | {r['ndd_hc']:.2f} | {r['pf_hc']:.2f} | "
                         f"{corr_str} | {int(r['trades'])} | {r['decision']} |")
        (OUT_DIR / "summary.md").write_text("\n".join(lines), encoding="utf-8")
        print(f"\nWrote: {OUT_DIR/'per_stream.csv'}, {OUT_DIR/'portfolio.csv'}, "
              f"{OUT_DIR/'summary.md'}, pair_corr_*.csv")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
