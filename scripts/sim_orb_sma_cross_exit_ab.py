"""V3 A/B driver: SMA(3)x(5) cross-exit on v6 setfile across 4 weekly windows.

Run:  $env:PYTHONPATH = "src"; python scripts/sim_orb_sma_cross_exit_ab.py

Outputs:
  docs/reports/2026-05-25-sma-cross-exit-ab.md  — comparison table + verdict

Per spec docs/superpowers/specs/2026-05-25-sma35-cross-exit-design.md.
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
REPORT  = ROOT / "docs" / "reports" / "2026-05-25-sma-cross-exit-ab.md"
DEPOSIT = 10_000.0
SPREAD  = 30
RISK_PER_STREAM = 1.5

WINDOWS = [
    ("W1", datetime(2026, 4, 25, tzinfo=timezone.utc), datetime(2026, 5, 2,  tzinfo=timezone.utc)),
    ("W2", datetime(2026, 5, 2,  tzinfo=timezone.utc), datetime(2026, 5, 9,  tzinfo=timezone.utc)),
    ("W3", datetime(2026, 5, 9,  tzinfo=timezone.utc), datetime(2026, 5, 16, tzinfo=timezone.utc)),
    ("W4", datetime(2026, 5, 16, tzinfo=timezone.utc), datetime(2026, 5, 23, tzinfo=timezone.utc)),
]


def _meta() -> SymbolMeta:
    return SymbolMeta(
        point=0.01, tick_size=0.01, tick_value=1.0, stops_level_pts=0,
        volume_min=0.01, volume_max=100.0, volume_step=0.01, digits=2,
    )


def _build_cfg(stream: dict, cross_on: bool) -> ORBConfig:
    return ORBConfig(
        risk_pct=RISK_PER_STREAM,
        range_minutes=stream["range_minutes"],
        fixed_sl_pts=stream["fixed_sl_pts"],
        rr_ratio=stream["rr_ratio"],
        half_tp_ratio=stream["half_tp_ratio"],
        pending_expire_minutes=stream["pending_expire_minutes"],
        fractal_confirm=True, fractal_width=5,
        sma_cross_exit=cross_on,
    )


def _haircut(net: float, dd: float) -> tuple[float, float]:
    """6% NP haircut per feedback_sim_vs_live_calibration."""
    net_hc = net * 0.94
    nd = (net_hc / dd) if dd > 0 else 0.0
    return net_hc, nd


def main() -> int:
    cfg_data = parse_v6_setfile(SETFILE)
    streams = cfg_data["streams"]
    meta = _meta()

    rows: list[dict] = []
    skipped_windows: list[str] = []

    for wname, wstart, wend in WINDOWS:
        days = (wend - wstart).days
        print(f"\n=== {wname} ({wstart.date()} to {wend.date()}, {days}d) ===")
        try:
            sym, ticks, m1, m5 = fetch_window(None, wstart, wend, spread_pts=SPREAD, account="sim")
            print(f"  fetched {len(ticks):,} ticks, {len(m5)} M5 bars, {len(m1)} M1 bars  [{sym}]")
        except Exception as exc:
            print(f"  SKIP — fetch failed: {exc}")
            skipped_windows.append(f"{wname}: fetch error — {exc}")
            continue

        for sidx, s in enumerate(streams, start=1):
            for mode_name, on in [("off", False), ("on", True)]:
                try:
                    r = simulate_fast(ticks, m5, m1, _build_cfg(s, on), meta, DEPOSIT)
                    net_hc, nd_hc = _haircut(r.net_profit, r.max_drawdown)
                    rows.append({
                        "window": wname,
                        "days": days,
                        "stream": f"S{sidx}",
                        "mode": mode_name,
                        "net": round(r.net_profit, 2),
                        "dd": round(r.max_drawdown, 2),
                        "pf": round(r.profit_factor, 3),
                        "trades": r.trades,
                        "net_hc": round(net_hc, 2),
                        "nd_hc": round(nd_hc, 4),
                    })
                    print(f"  {wname} S{sidx} {mode_name:3s}: net={r.net_profit:+.2f}  dd={r.max_drawdown:.2f}"
                          f"  NP/DD$_hc={nd_hc:.4f}  trades={r.trades}")
                except Exception as exc:
                    print(f"  WARN {wname} S{sidx} {mode_name}: sim error — {exc}")
                    traceback.print_exc()
                    rows.append({
                        "window": wname,
                        "days": days,
                        "stream": f"S{sidx}",
                        "mode": mode_name,
                        "net": float("nan"),
                        "dd": float("nan"),
                        "pf": float("nan"),
                        "trades": 0,
                        "net_hc": float("nan"),
                        "nd_hc": float("nan"),
                    })

    if not rows:
        print("ERROR: no results — all windows skipped or all sims failed.")
        return 1

    df = pd.DataFrame(rows)

    # Per-window portfolio totals (sum across 6 streams).
    portfolio = df.groupby(["window", "days", "mode"], as_index=False).agg(
        net=("net", "sum"),
        dd=("dd", "sum"),
        trades=("trades", "sum"),
        net_hc=("net_hc", "sum"),
    )
    portfolio["nd_hc"] = portfolio.apply(
        lambda row: (row["net_hc"] / row["dd"]) if row["dd"] > 0 else 0.0,
        axis=1,
    )

    pivot = portfolio.pivot(
        index=["window", "days"],
        columns="mode",
        values=["net", "dd", "trades", "net_hc", "nd_hc"],
    )
    pivot[("delta_nd_hc", "")] = pivot[("nd_hc", "on")] - pivot[("nd_hc", "off")]
    pivot[("delta_pct", "")] = (
        pivot[("delta_nd_hc", "")] / pivot[("nd_hc", "off")].replace(0, np.nan)
    ) * 100

    wins        = int((pivot[("delta_pct", "")] >= 10).sum())
    regressions = int((pivot[("delta_pct", "")] <= -15).sum())

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    with REPORT.open("w", encoding="utf-8") as f:
        f.write("# SMA(3) x SMA(5) Cross-Exit A/B Report (2026-05-25)\n\n")
        f.write(
            f"Setfile: `{SETFILE.name}`  ·  spread {SPREAD}pt"
            f"  ·  deposit ${DEPOSIT:,.0f}  ·  risk {RISK_PER_STREAM}%/stream\n\n"
        )

        if skipped_windows:
            f.write("## Skipped windows\n\n")
            for note in skipped_windows:
                f.write(f"- {note}\n")
            f.write("\n")

        f.write("## Portfolio per window (sum of 6 streams)\n\n")
        f.write(pivot.round(4).to_markdown())
        f.write("\n\n")

        # Pretty per-window comparison summary
        f.write("## Per-window NP/DD$_hc comparison\n\n")
        f.write("| Window | Days | off_nd_hc | on_nd_hc | delta_nd_hc | delta_pct |\n")
        f.write("|--------|------|-----------|----------|-------------|----------|\n")
        for idx, row in pivot.iterrows():
            wname_str, days_val = idx
            off_nd = float(row[("nd_hc", "off")])
            on_nd  = float(row[("nd_hc", "on")])
            d_nd   = float(row[("delta_nd_hc", "")])
            d_pct  = float(row[("delta_pct", "")])
            f.write(
                f"| {wname_str} | {days_val} | {off_nd:.4f} | {on_nd:.4f}"
                f" | {d_nd:+.4f} | {d_pct:+.2f}% |\n"
            )
        f.write("\n")

        f.write("## Per-stream detail (all rows)\n\n")
        f.write(df.to_markdown(index=False))
        f.write("\n\n")

        f.write("## Decision\n\n")
        f.write(f"- Windows where `on` improves NP/DD$_hc by >=+10%: **{wins} / 4**\n")
        f.write(f"- Windows where `on` regresses NP/DD$_hc by <=-15%: **{regressions} / 4**\n\n")
        if wins >= 3 and regressions == 0:
            verdict = "**Verdict: ADVANCE** — >=3/4 windows improved >=+10% with zero regressions. Promote sma_cross_exit=on as a WFO dim."
        elif regressions > 0:
            verdict = "**Verdict: REJECT** — regime-dependent regression observed (<=-15% in >=1 window)."
        else:
            verdict = "**Verdict: ITERATE** — mixed results; revisit SMA periods or filter conditions."
        f.write(f"- {verdict}\n")

    print(f"\nwrote {REPORT}")
    print(f"Verdict: wins={wins}/4  regressions={regressions}/4")
    print(f"  => {verdict}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
