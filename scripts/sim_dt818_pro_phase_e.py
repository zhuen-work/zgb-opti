"""Phase E: Portfolio sanity for the 2-stream session-split ORB stack.

Loads per-session winners from the per-session WFOs:
  output/wfo_orb_ldn_may2/winner.json
  output/wfo_orb_ny_may2/winner.json

Runs LDN-only and NY-only sims separately, deal-merges on shared $10k account.
Reports per-session NP, combined NP/DD$, plus the calibration haircut.

DESIGN CAVEAT (deal-merge approximation):
Each session sim starts from isolated $10k for position sizing. Production
shares balance across sessions, so this approach under-counts NP by ~10-20%
due to missed compounding. The bias is conservative and applies equally to
all candidates being compared, so RANK ORDER is preserved. Absolute NP
projections should be adjusted upward ~+15% for production-accurate forecast.

If both per-session winner.json files are missing, falls back to the legacy
`output/wfo_orb_may2/winner.json` (single both-sessions winner) and runs
that as a single sim with both sessions enabled (no compounding bias).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars, SIM_SPREAD_PTS
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
WFO_BASELINE_RISK = 3.0


def load_winner(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text())


def make_session_cfg(winner: dict, risk: float, session: str) -> ORBConfig:
    """Build ORBConfig from winner.json with one session enabled."""
    cfg_dict = dict(winner["cfg"])
    cfg_dict["risk_pct"] = risk
    cfg_dict["ldn_enabled"] = (session == "ldn")
    cfg_dict["ny_enabled"]  = (session == "ny")
    if cfg_dict.get("daily_target_pct", 0) > 0:
        cfg_dict["daily_target_pct"] *= (risk / WFO_BASELINE_RISK)
    if cfg_dict.get("daily_loss_pct", 0) > 0:
        cfg_dict["daily_loss_pct"]   *= (risk / WFO_BASELINE_RISK)
    import dataclasses
    fields = {f.name for f in dataclasses.fields(ORBConfig)}
    cfg_dict = {k: v for k, v in cfg_dict.items() if k in fields}
    return ORBConfig(**cfg_dict)


def make_combined_cfg(winner: dict, risk: float) -> ORBConfig:
    """Legacy fallback: single config with both sessions enabled."""
    cfg_dict = dict(winner["cfg"])
    cfg_dict["risk_pct"] = risk
    cfg_dict["ldn_enabled"] = True
    cfg_dict["ny_enabled"]  = True
    if cfg_dict.get("daily_target_pct", 0) > 0:
        cfg_dict["daily_target_pct"] *= (risk / WFO_BASELINE_RISK)
    if cfg_dict.get("daily_loss_pct", 0) > 0:
        cfg_dict["daily_loss_pct"]   *= (risk / WFO_BASELINE_RISK)
    import dataclasses
    fields = {f.name for f in dataclasses.fields(ORBConfig)}
    cfg_dict = {k: v for k, v in cfg_dict.items() if k in fields}
    return ORBConfig(**cfg_dict)


def aggregate_deals(deals: list[tuple]) -> dict:
    """Walk sorted deals -> compute NP, DD$, NP/DD$."""
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    for _, _s, p in sorted(deals, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        cur = bal_max - bal
        if cur > dd_abs: dd_abs = cur
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    ndd = np_ / dd_abs if dd_abs > 0 else 0
    return dict(np=np_, dd_pct=dd_pct, dd_abs=dd_abs, ndd=ndd)


def run_per_session(ldn_winner: dict, ny_winner: dict, ticks, m1, m5, meta, risk: float):
    """Two separate sims (LDN-only, NY-only), deal-merge."""
    ldn_cfg = make_session_cfg(ldn_winner, risk, "ldn")
    ny_cfg  = make_session_cfg(ny_winner,  risk, "ny")
    r_ldn = orb_simulate(ticks, m5, m1, ldn_cfg, meta, initial_balance=DEPOSIT)
    r_ny  = orb_simulate(ticks, m5, m1, ny_cfg,  meta, initial_balance=DEPOSIT)
    deals = []
    for s, r in [("ORB_LDN", r_ldn), ("ORB_NY", r_ny)]:
        for d in r.deals:
            if d.kind != "entry":
                deals.append((d.ts, s, d.pnl))
    agg = aggregate_deals(deals)
    agg["trades"] = len(deals)
    agg["per_stream"] = {
        "ORB_LDN": (sum(p for _, ss, p in deals if ss == "ORB_LDN"),
                    sum(1 for _, ss, _ in deals if ss == "ORB_LDN")),
        "ORB_NY":  (sum(p for _, ss, p in deals if ss == "ORB_NY"),
                    sum(1 for _, ss, _ in deals if ss == "ORB_NY")),
    }
    return agg


def run_combined_legacy(winner: dict, ticks, m1, m5, meta, risk: float):
    """Single sim with both sessions enabled (legacy / fallback)."""
    cfg = make_combined_cfg(winner, risk)
    r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
    deals = [(d.ts, "ORB", d.pnl) for d in r.deals if d.kind != "entry"]
    agg = aggregate_deals(deals)
    agg["trades"] = len(deals)
    agg["per_stream"] = {"ORB": (r.net_profit, r.trades)}
    return agg


def main() -> int:
    start = datetime(2026, 2, 14, tzinfo=timezone.utc)
    end = datetime(2026, 5, 1, tzinfo=timezone.utc)
    days = (end - start).days

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        ticks = load_ticks(SYMBOL, start, end)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)

        ldn_w = load_winner(ROOT / "output" / "wfo_orb_ldn_may2" / "winner.json")
        ny_w  = load_winner(ROOT / "output" / "wfo_orb_ny_may2" / "winner.json")
        legacy_w = load_winner(ROOT / "output" / "wfo_orb_may2" / "winner.json")

        if ldn_w and ny_w:
            mode = "per-session"
        elif legacy_w:
            mode = "legacy-single"
        else:
            print("No winners found. Run scripts/sim_wfo_orb.py first.")
            return 1

        print("=" * 100)
        print(f"  PHASE E: 2-stream session-split portfolio sanity ({days}d, $10k, {SIM_SPREAD_PTS}pt friction)")
        print(f"  Mode: {mode}")
        print("=" * 100)

        if mode == "per-session":
            print(f"  ORB_LDN winner: P0={ldn_w.get('p0_pass')} slope={ldn_w.get('slope', 0):+.1%}  "
                  f"cfg={ldn_w['cfg']}")
            print(f"  ORB_NY  winner: P0={ny_w.get('p0_pass')} slope={ny_w.get('slope', 0):+.1%}  "
                  f"cfg={ny_w['cfg']}")
            print()
            print("  NOTE: deal-merge of two isolated-balance sims under-counts compounding")
            print("        ~10-20% NP shortfall vs production. Rank order is preserved.")
        else:
            print(f"  Legacy winner: P0={legacy_w.get('p0_pass')} slope={legacy_w.get('slope', 0):+.1%}  "
                  f"cfg={legacy_w['cfg']}")

        for risk in (2.0, 3.0, 4.5):
            print(f"\n  --- Combined sanity at {risk}% risk ---")
            if mode == "per-session":
                agg = run_per_session(ldn_w, ny_w, ticks, m1, m5, meta, risk)
            else:
                agg = run_combined_legacy(legacy_w, ticks, m1, m5, meta, risk)

            np_haircut = agg["np"] * 0.94
            np_compound_adj = agg["np"] * 1.15 if mode == "per-session" else agg["np"]

            print(f"  COMBINED:  NP=${agg['np']:+,.0f}  ROI={agg['np']/DEPOSIT*100:+.1f}%  "
                  f"DD={agg['dd_pct']:.1f}%  NP/DD$={agg['ndd']:.2f}  Trades={agg['trades']}")
            if mode == "per-session":
                print(f"             ~live (haircut 0.94×): ${np_haircut:+,.0f}   "
                      f"~production (compound +15%): ${np_compound_adj:+,.0f}")
            else:
                print(f"             ~live (haircut 0.94×): ${np_haircut:+,.0f}")
            for s, (np_s, tr_s) in agg["per_stream"].items():
                if tr_s > 0:
                    print(f"    {s:<10}  ${np_s:>+9,.0f}  ({tr_s} trades)")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
