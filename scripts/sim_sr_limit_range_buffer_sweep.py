"""SR_limit focused 2-dim sweep: range_minutes x buffer_pts.

Followup to docs/superpowers/specs/2026-05-27-sweep-reclaim-v1-design.md Stage 1.
Probes whether SR_limit's marginal Stage-1 result (PF_hc=0.97) is a parameter
problem (cell exists with PF_hc >= 1.10) or a structural problem (no cell works).

Architecture: standalone SR_limit (no ORB pairing). One instance per cell,
LDN+NY both enabled, default session hours, 70d window.

Usage:
  python scripts/sim_sr_limit_range_buffer_sweep.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.sweep_reclaim import simulate as sr_simulate, SRConfig


SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
START = datetime(2026, 2, 14, tzinfo=timezone.utc)
END = datetime(2026, 4, 25, tzinfo=timezone.utc)
SPREAD = 30
RISK_PCT = 1.0

HAIRCUT_NP = 0.94
HAIRCUT_PF = 0.25

PENDING_EXPIRE_MIN = 240
RANGE_MINUTES = [30, 60, 90, 120]
BUFFER_PTS    = [0, 10, 20, 30, 50]

ADVANCE_PF_HC  = 1.10
PARTIAL_FLOOR  = 0.97

OUT_DIR = ROOT / "output" / "sr_limit_sweep_2026_05_27"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def aggregate(deals):
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gains = 0.0; losses = 0.0; wins = 0; trades = 0
    for d in sorted(deals, key=lambda x: x.ts):
        bal += d.pnl
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if d.pnl >= 0: gains += d.pnl; wins += 1
        else:          losses += -d.pnl
        trades += 1
    np_ = bal - DEPOSIT
    ndd = np_ / dd_abs if dd_abs > 0 else 0.0
    pf = gains / losses if losses > 0 else float("inf")
    wr = wins / trades * 100 if trades > 0 else 0.0
    return dict(np=np_, dd_abs=dd_abs, ndd=ndd, pf=pf,
                trades=trades, wr=wr)


def build_parent_cfg(range_min: int) -> ORBConfig:
    # Minimal ORBConfig used only for session enumeration in _build_sr_sessions.
    # SR ignores SL/RR/HTP fields — only range_minutes, ldn/ny enables/hours,
    # pending_expire_minutes are consumed.
    return ORBConfig(
        range_minutes=range_min,
        pending_expire_minutes=PENDING_EXPIRE_MIN,
        ldn_enabled=True,  ldn_start_hour=7,
        ny_enabled=True,   ny_start_hour=13,
    )


def decision(pf_hc: float) -> str:
    if pf_hc >= ADVANCE_PF_HC:
        return "ADVANCE_TO_WFO"
    if pf_hc >= PARTIAL_FLOOR:
        return "PARTIAL"
    return "REJECT"


def main() -> int:
    print(f"=== SR_limit range x buffer sweep | {START.date()} -> {END.date()} "
          f"| {SPREAD}pt | $10k | {RISK_PCT}%/setup | expire={PENDING_EXPIRE_MIN}min ===")
    print(f"Grid: range_min={RANGE_MINUTES} x buffer_pts={BUFFER_PTS} "
          f"= {len(RANGE_MINUTES)*len(BUFFER_PTS)} cells")
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        m1 = load_bars(SYMBOL, "M1", START, END)
        m5 = load_bars(SYMBOL, "M5", START, END)

        rows = []
        for rm in RANGE_MINUTES:
            parent = build_parent_cfg(rm)
            for bp in BUFFER_PTS:
                cfg = SRConfig(risk_pct=RISK_PCT, mode="limit", buffer_pts=bp)
                r = sr_simulate(m5, m1, parent, cfg, meta, initial_balance=DEPOSIT)
                agg = aggregate(r.deals)
                np_hc  = agg["np"] * HAIRCUT_NP
                pf_hc  = max(agg["pf"] - HAIRCUT_PF, 0.0) if np.isfinite(agg["pf"]) else float("inf")
                ndd_hc = np_hc / agg["dd_abs"] if agg["dd_abs"] > 0 else 0.0
                skip_rate = ((r.skip_no_sweep + r.skip_no_rr) / r.sessions_total * 100
                             if r.sessions_total > 0 else 0.0)
                expire_rate = (r.expire_no_fill / r.sessions_total * 100
                               if r.sessions_total > 0 else 0.0)
                row = {
                    "range_min": rm, "buffer_pts": bp,
                    "sessions": r.sessions_total,
                    "trades": agg["trades"], "wr": agg["wr"],
                    "np": agg["np"], "np_hc": np_hc,
                    "dd_abs": agg["dd_abs"],
                    "pf": agg["pf"], "pf_hc": pf_hc,
                    "ndd": agg["ndd"], "ndd_hc": ndd_hc,
                    "skip_no_sweep": r.skip_no_sweep,
                    "skip_no_rr":    r.skip_no_rr,
                    "expire_no_fill": r.expire_no_fill,
                    "skip_rate_pct": skip_rate,
                    "expire_rate_pct": expire_rate,
                    "decision": decision(pf_hc),
                }
                rows.append(row)
                print(f"  rm={rm:>3}m buf={bp:>2}pt  trades={agg['trades']:>3} WR={agg['wr']:>4.1f}%  "
                      f"NP=${agg['np']:>+8,.0f} (hc ${np_hc:>+8,.0f})  "
                      f"DD=${agg['dd_abs']:>7,.0f}  PF_hc={pf_hc:>4.2f}  "
                      f"NP/DD$_hc={ndd_hc:>5.2f}  -> {row['decision']}")

        grid_df = pd.DataFrame(rows)
        grid_df.to_csv(OUT_DIR / "grid.csv", index=False)

        # Heatmaps (4 x 5)
        pf_hc_pivot  = grid_df.pivot(index="range_min", columns="buffer_pts", values="pf_hc")
        ndd_hc_pivot = grid_df.pivot(index="range_min", columns="buffer_pts", values="ndd_hc")
        np_hc_pivot  = grid_df.pivot(index="range_min", columns="buffer_pts", values="np_hc")
        pf_hc_pivot.to_csv(OUT_DIR / "heatmap_pf_hc.csv")
        ndd_hc_pivot.to_csv(OUT_DIR / "heatmap_ndd_hc.csv")
        np_hc_pivot.to_csv(OUT_DIR / "heatmap_np_hc.csv")

        # Best cell + decision
        best = grid_df.sort_values("pf_hc", ascending=False).iloc[0]
        overall = decision(float(best["pf_hc"]))

        lines = [
            f"# SR_limit range_minutes x buffer_pts sweep -- {START.date()} to {END.date()}",
            "",
            f"**Window:** {START.date()} -> {END.date()} ({(END-START).days}d)  ",
            f"**Spread:** {SPREAD}pt  |  **Deposit:** $10k  |  **Risk:** {RISK_PCT}%/setup  "
            f"|  **Expire:** {PENDING_EXPIRE_MIN}min  |  **Mode:** limit  ",
            f"**Haircut:** NP x {HAIRCUT_NP}, PF - {HAIRCUT_PF}",
            "",
            "## Best cell",
            "",
            f"- `range_minutes = {int(best['range_min'])}`, `buffer_pts = {int(best['buffer_pts'])}`",
            f"- NP_hc = ${best['np_hc']:+,.0f}  |  DD$ = ${best['dd_abs']:,.0f}  "
            f"|  PF_hc = {best['pf_hc']:.2f}  |  NP/DD$_hc = {best['ndd_hc']:.2f}",
            f"- Trades = {int(best['trades'])}  |  WR = {best['wr']:.1f}%  "
            f"|  Skip-rate = {best['skip_rate_pct']:.1f}%",
            "",
            f"**Decision:** {overall}",
            "",
            "## PF_hc heatmap (rows = range_min, cols = buffer_pts)",
            "",
            pf_hc_pivot.round(2).to_markdown(),
            "",
            "## NP/DD$_hc heatmap",
            "",
            ndd_hc_pivot.round(2).to_markdown(),
            "",
            "## NP_hc heatmap ($)",
            "",
            np_hc_pivot.round(0).astype(int).to_markdown(),
            "",
            "## Full grid",
            "",
            grid_df.round(2).to_markdown(index=False),
        ]
        (OUT_DIR / "summary.md").write_text("\n".join(lines), encoding="utf-8")
        print(f"\nWrote: {OUT_DIR/'grid.csv'}, {OUT_DIR/'summary.md'}, "
              f"heatmap_*.csv")
        print(f"\nBest: range_min={int(best['range_min'])} buf={int(best['buffer_pts'])}pt  "
              f"PF_hc={best['pf_hc']:.2f}  NP/DD$_hc={best['ndd_hc']:.2f}  -> {overall}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
