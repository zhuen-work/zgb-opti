"""V5 sweep driver: SMA cross-exit period sweep (fast x slow) across 4 weekly windows.

Run:  $env:PYTHONPATH = "src"; python scripts/sim_orb_sma_cross_period_sweep.py

Outputs:
  docs/reports/2026-05-25-sma-cross-period-sweep.md
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
REPORT  = ROOT / "docs" / "reports" / "2026-05-25-sma-cross-period-sweep.md"
DEPOSIT = 10_000.0
SPREAD  = 30
RISK_PER_STREAM = 1.5

WINDOWS = [
    ("W1", datetime(2026, 4, 25, tzinfo=timezone.utc), datetime(2026, 5, 2,  tzinfo=timezone.utc)),
    ("W2", datetime(2026, 5, 2,  tzinfo=timezone.utc), datetime(2026, 5, 9,  tzinfo=timezone.utc)),
    ("W3", datetime(2026, 5, 9,  tzinfo=timezone.utc), datetime(2026, 5, 16, tzinfo=timezone.utc)),
    ("W4", datetime(2026, 5, 16, tzinfo=timezone.utc), datetime(2026, 5, 23, tzinfo=timezone.utc)),
]

# Modes: (label, sma_cross_exit, fast, slow)
MODES = [
    ("off",   False, 0,  0),
    ("s3_5",  True,  3,  5),
    ("s3_8",  True,  3,  8),
    ("s5_10", True,  5,  10),
    ("s5_20", True,  5,  20),
    ("s8_21", True,  8,  21),
]


def _meta() -> SymbolMeta:
    return SymbolMeta(
        point=0.01, tick_size=0.01, tick_value=1.0, stops_level_pts=0,
        volume_min=0.01, volume_max=100.0, volume_step=0.01, digits=2,
    )


def _build_cfg(stream: dict, cross_on: bool, fast: int, slow: int) -> ORBConfig:
    kwargs = dict(
        risk_pct=RISK_PER_STREAM,
        range_minutes=stream["range_minutes"],
        fixed_sl_pts=stream["fixed_sl_pts"],
        rr_ratio=stream["rr_ratio"],
        half_tp_ratio=stream["half_tp_ratio"],
        pending_expire_minutes=stream["pending_expire_minutes"],
        fractal_confirm=True,
        fractal_width=5,
        sma_cross_exit=cross_on,
    )
    if cross_on:
        kwargs["sma_cross_fast"] = fast
        kwargs["sma_cross_slow"] = slow
    return ORBConfig(**kwargs)


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
            for mode_name, on, fast, slow in MODES:
                try:
                    r = simulate_fast(
                        ticks, m5, m1,
                        _build_cfg(s, on, fast, slow),
                        meta, DEPOSIT,
                    )
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
                    print(
                        f"  {wname} S{sidx} {mode_name:6s}: net={r.net_profit:+.2f}"
                        f"  dd={r.max_drawdown:.2f}  NP/DD$_hc={nd_hc:.4f}  trades={r.trades}"
                    )
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

    # Compute delta vs off baseline per window
    off_nd = portfolio[portfolio["mode"] == "off"].set_index("window")["nd_hc"]
    off_net = portfolio[portfolio["mode"] == "off"].set_index("window")["net_hc"]

    portfolio["delta_nd_hc"] = portfolio.apply(
        lambda row: row["nd_hc"] - off_nd.get(row["window"], 0.0), axis=1
    )
    portfolio["delta_pct"] = portfolio.apply(
        lambda row: (
            (row["nd_hc"] - off_nd.get(row["window"], 0.0))
            / off_nd.get(row["window"], np.nan)
            * 100
        ) if off_nd.get(row["window"], 0.0) != 0 else np.nan,
        axis=1,
    )

    # Per-mode summary over all 4 windows
    summary = []
    for mode_name, _, _, _ in MODES:
        if mode_name == "off":
            continue
        sub = portfolio[portfolio["mode"] == mode_name]
        wins = int((sub["delta_pct"] >= 10).sum())
        regressions = int((sub["delta_pct"] <= -15).sum())
        mean_delta = float(sub["delta_pct"].mean())
        net_sum = float(sub["net_hc"].sum())
        nd_mean = float(sub["nd_hc"].mean())
        summary.append({
            "mode": mode_name,
            "wins_ge10": wins,
            "regressions_le-15": regressions,
            "mean_delta_pct": round(mean_delta, 2),
            "mean_nd_hc": round(nd_mean, 4),
            "total_net_hc": round(net_sum, 2),
        })
    summary_df = pd.DataFrame(summary)

    # 4-week total NP for off
    off_total_net = float(portfolio[portfolio["mode"] == "off"]["net_hc"].sum())
    summary_df["delta_total_net_hc"] = (summary_df["total_net_hc"] - off_total_net).round(2)

    # Back-fill delta_total_net_hc into the list of dicts (used in verdict text)
    delta_map = dict(zip(summary_df["mode"], summary_df["delta_total_net_hc"]))
    for r in summary:
        r["delta_total_net_hc"] = float(delta_map.get(r["mode"], float("nan")))

    # Verdict: candidates need >= 3 wins AND 0 regressions
    candidates = [
        r for r in summary
        if r["regressions_le-15"] == 0 and r["wins_ge10"] >= 3
    ]

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    with REPORT.open("w", encoding="utf-8") as f:
        f.write("# V5: SMA Cross-Exit Period Sweep (2026-05-25)\n\n")
        f.write(
            f"Setfile: `{SETFILE.name}`  ·  spread {SPREAD}pt"
            f"  ·  deposit ${DEPOSIT:,.0f}  ·  risk {RISK_PER_STREAM}%/stream\n\n"
        )
        f.write(f"Off-baseline 4-week total NP_hc: **${off_total_net:,.2f}**\n\n")

        if skipped_windows:
            f.write("## Skipped windows\n\n")
            for note in skipped_windows:
                f.write(f"- {note}\n")
            f.write("\n")

        f.write("## Portfolio per (window, mode)\n\n")
        f.write(portfolio.round(4).to_markdown(index=False))
        f.write("\n\n")

        # Pretty per-window breakdown for each mode
        f.write("## Per-window NP/DD$_hc vs off\n\n")
        f.write("| window | days | mode | off_nd_hc | mode_nd_hc | delta_nd_hc | delta_pct |\n")
        f.write("|--------|------|------|-----------|------------|-------------|----------|\n")
        for _, row in portfolio[portfolio["mode"] != "off"].iterrows():
            wnd = row["window"]
            f.write(
                f"| {wnd} | {row['days']} | {row['mode']}"
                f" | {off_nd.get(wnd, float('nan')):.4f}"
                f" | {row['nd_hc']:.4f}"
                f" | {row['delta_nd_hc']:+.4f}"
                f" | {row['delta_pct']:+.2f}% |\n"
            )
        f.write("\n")

        f.write("## Per-mode summary (vs off baseline)\n\n")
        f.write(summary_df.to_markdown(index=False))
        f.write("\n\n")

        f.write("## Verdict\n\n")
        if candidates:
            winner = max(candidates, key=lambda r: r["mean_delta_pct"])
            f.write(
                f"- **ADVANCE** with `sma_cross_fast/slow = {winner['mode']}` "
                f"(mean delta%={winner['mean_delta_pct']:+.1f}, wins {winner['wins_ge10']}/4, "
                f"regressions {winner['regressions_le-15']}/4, "
                f"4wk NP delta=${winner['delta_total_net_hc']:+,.0f}).\n"
            )
        else:
            f.write(
                "- **REJECT V5** -- no SMA-period pair clears"
                " (>=3 wins at +10% AND 0 regressions <= -15%).\n"
            )
            for r in summary:
                f.write(
                    f"  - {r['mode']}: mean delta%={r['mean_delta_pct']:+.1f},"
                    f" wins {r['wins_ge10']}/4, regressions {r['regressions_le-15']}/4,"
                    f" 4wk NP delta=${r['delta_total_net_hc']:+,.0f}\n"
                )
        f.write("\n\n")

        f.write("## Per-stream detail\n\n")
        f.write(df.to_markdown(index=False))
        f.write("\n")

    print(f"\nwrote {REPORT}")

    # Console summary
    print("\n=== Per-mode summary ===")
    print(summary_df.to_string(index=False))
    print(f"\nOff-baseline 4-week total NP_hc: ${off_total_net:,.2f}")
    if candidates:
        winner = max(candidates, key=lambda r: r["mean_delta_pct"])
        print(f"\nVerdict: ADVANCE with {winner['mode']}"
              f" (mean delta%={winner['mean_delta_pct']:+.1f},"
              f" wins {winner['wins_ge10']}/4, regressions {winner['regressions_le-15']}/4)")
    else:
        print("\nVerdict: REJECT V5 -- no pair clears the bar")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
