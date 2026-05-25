"""V2 sweep driver: retrace-gated MA7 trail across 4 windows x 6 streams x 5 modes.

Run:  $env:PYTHONPATH = "src"; python scripts/sim_orb_ma7_trail_retrace_sweep.py

Outputs:
  docs/reports/2026-05-25-ma7-trail-retrace-sweep.md  -- portfolio + detail + verdict

Per spec docs/superpowers/specs/2026-05-25-ma7-trail-retrace-gate-design.md.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys
import traceback

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast
from zgb_sim.scalper_v1 import SymbolMeta
from sim_orb_oos_today import fetch_window
from sim_orb_oos_today_hedge_v6 import parse_v6_setfile


SETFILE = ROOT / "configs" / "sets" / "dt818_pro_v6_9pct_may23_may16.set"
REPORT  = ROOT / "docs" / "reports" / "2026-05-25-ma7-trail-retrace-sweep.md"
DEPOSIT = 10_000.0
SPREAD  = 30
RISK_PER_STREAM = 1.5

WINDOWS = [
    ("W1", datetime(2026, 4, 25, tzinfo=timezone.utc), datetime(2026, 5, 2,  tzinfo=timezone.utc)),
    ("W2", datetime(2026, 5, 2,  tzinfo=timezone.utc), datetime(2026, 5, 9,  tzinfo=timezone.utc)),
    ("W3", datetime(2026, 5, 9,  tzinfo=timezone.utc), datetime(2026, 5, 16, tzinfo=timezone.utc)),
    ("W4", datetime(2026, 5, 16, tzinfo=timezone.utc), datetime(2026, 5, 23, tzinfo=timezone.utc)),
]

MODES = [
    ("off",  False, 0.0),
    ("r0.0", True,  0.0),
    ("r0.2", True,  0.20),
    ("r0.3", True,  0.30),
    ("r0.4", True,  0.40),
]


def _meta() -> SymbolMeta:
    return SymbolMeta(point=0.01, tick_size=0.01, tick_value=1.0, stops_level_pts=0,
                      volume_min=0.01, volume_max=100.0, volume_step=0.01, digits=2)


def _build_cfg(stream: dict, ma_trail: bool, retrace_pct: float) -> ORBConfig:
    return ORBConfig(
        risk_pct=RISK_PER_STREAM,
        range_minutes=stream["range_minutes"],
        fixed_sl_pts=stream["fixed_sl_pts"],
        rr_ratio=stream["rr_ratio"],
        half_tp_ratio=stream["half_tp_ratio"],
        pending_expire_minutes=stream["pending_expire_minutes"],
        fractal_confirm=True, fractal_width=5,
        ma_trail=ma_trail,
        ma_trail_retrace_pct=retrace_pct,
    )


def _haircut(net: float, dd: float) -> tuple[float, float]:
    """6% NP haircut + NP/DD$_hc ratio per feedback_sim_vs_live_calibration."""
    net_hc = net * 0.94
    nd = (net_hc / dd) if dd > 0 else 0.0
    return net_hc, nd


def main() -> int:
    cfg_data = parse_v6_setfile(SETFILE)
    streams = cfg_data["streams"]
    meta = _meta()

    rows: list[dict] = []
    skipped: list[str] = []

    for wname, wstart, wend in WINDOWS:
        days = (wend - wstart).days
        print(f"\n=== {wname} ({wstart.date()} to {wend.date()}, {days}d) ===")
        try:
            sym, ticks, m1, m5 = fetch_window(None, wstart, wend, spread_pts=SPREAD, account="sim")
            print(f"  fetched {len(ticks):,} ticks, {len(m5)} M5 bars, {len(m1)} M1 bars  [{sym}]")
        except Exception as exc:
            print(f"  SKIP -- fetch failed: {exc}")
            skipped.append(f"{wname}: fetch error -- {exc}")
            continue

        for sidx, s in enumerate(streams, start=1):
            for mode_name, ma, rp in MODES:
                try:
                    r = simulate_fast(ticks, m5, m1, _build_cfg(s, ma, rp), meta, DEPOSIT)
                    net_hc, nd_hc = _haircut(r.net_profit, r.max_drawdown)
                    rows.append({
                        "window": wname, "days": days, "stream": f"S{sidx}",
                        "mode": mode_name,
                        "net": round(r.net_profit, 2),
                        "dd": round(r.max_drawdown, 2),
                        "pf": round(r.profit_factor, 3),
                        "trades": r.trades,
                        "net_hc": round(net_hc, 2),
                        "nd_hc": round(nd_hc, 4),
                    })
                    print(f"  {wname} S{sidx} {mode_name:4s}: net={r.net_profit:+.2f}  dd={r.max_drawdown:.2f}"
                          f"  NP/DD$_hc={nd_hc:.3f}  trades={r.trades}")
                except Exception as exc:
                    print(f"  WARN {wname} S{sidx} {mode_name}: sim error -- {exc}")
                    traceback.print_exc()
                    rows.append({
                        "window": wname, "days": days, "stream": f"S{sidx}",
                        "mode": mode_name,
                        "net": float("nan"), "dd": float("nan"),
                        "pf": float("nan"), "trades": 0,
                        "net_hc": float("nan"), "nd_hc": float("nan"),
                    })

    if not rows:
        print("ERROR: no results -- all windows skipped or all sims failed.")
        return 1

    df = pd.DataFrame(rows)

    portfolio = df.groupby(["window", "days", "mode"], as_index=False).agg(
        net=("net", "sum"), dd=("dd", "sum"),
        trades=("trades", "sum"), net_hc=("net_hc", "sum"),
    )
    portfolio["nd_hc"] = portfolio.apply(
        lambda r: (r["net_hc"] / r["dd"]) if r["dd"] > 0 else 0.0, axis=1)

    # Compute deltas vs off baseline per window.
    off_nd = portfolio[portfolio["mode"] == "off"].set_index("window")["nd_hc"]
    portfolio["delta_nd_hc"] = portfolio.apply(
        lambda r: r["nd_hc"] - off_nd.get(r["window"], 0.0), axis=1)
    portfolio["delta_pct"] = portfolio.apply(
        lambda r: ((r["nd_hc"] - off_nd.get(r["window"], 0.0)) /
                   off_nd.get(r["window"], np.nan) * 100)
                  if off_nd.get(r["window"], 0.0) != 0 else np.nan,
        axis=1)

    # Decision: per non-off mode, count wins (D% >= +10), regressions (D% <= -15), mean D%.
    summary = []
    for mode_name, _, _ in MODES:
        if mode_name == "off":
            continue
        sub = portfolio[portfolio["mode"] == mode_name]
        wins = int((sub["delta_pct"] >= 10).sum())
        regressions = int((sub["delta_pct"] <= -15).sum())
        mean_delta = float(sub["delta_pct"].mean())
        summary.append({"mode": mode_name, "wins_ge10": wins,
                        "regressions_le-15": regressions, "mean_delta_pct": mean_delta})
    summary_df = pd.DataFrame(summary)

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    with REPORT.open("w", encoding="utf-8") as f:
        f.write("# MA7 Trail Retrace-Gate Sweep (2026-05-25)\n\n")
        f.write(f"Setfile: `{SETFILE.name}`  .  spread {SPREAD}pt  .  deposit ${DEPOSIT:,.0f}  .  risk {RISK_PER_STREAM}%/stream\n\n")

        if skipped:
            f.write("## Skipped windows\n\n")
            for note in skipped:
                f.write(f"- {note}\n")
            f.write("\n")

        f.write("## Portfolio per (window, mode)\n\n")
        f.write(portfolio.round(4).to_markdown(index=False))
        f.write("\n\n## Per-mode decision summary (vs `off` baseline)\n\n")
        f.write(summary_df.round(4).to_markdown(index=False))

        # Verdict
        candidates = [r for r in summary if r["regressions_le-15"] == 0 and r["wins_ge10"] >= 3]
        f.write("\n\n## Verdict\n\n")
        if candidates:
            winner = max(candidates, key=lambda r: r["mean_delta_pct"])
            f.write(f"- **ADVANCE** with `retrace_pct={winner['mode']}` "
                    f"(mean delta% = {winner['mean_delta_pct']:+.1f}, wins {winner['wins_ge10']}/4, "
                    f"regressions {winner['regressions_le-15']}/4).\n")
        else:
            f.write("- **REJECT V2** -- no `retrace_pct` value clears the threshold "
                    "(needs >=3 wins at +10% AND 0 regressions <= -15%).\n")
            for r in summary:
                f.write(f"  - {r['mode']}: mean delta% = {r['mean_delta_pct']:+.1f}, "
                        f"wins {r['wins_ge10']}/4, regressions {r['regressions_le-15']}/4\n")

        f.write("\n\n## Per-stream detail\n\n")
        f.write(df.to_markdown(index=False))

    print(f"\nwrote {REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
