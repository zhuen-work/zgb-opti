"""3×3 forward comparison: 3 WFO eras × 3 WFO spreads on live ticks.

For each (era, spread) pair, build a v7 setfile from that WFO's top-6 and run
it on the forward window AFTER that era. Reports a 3×3 grid of portfolio NP,
plus per-era and per-spread aggregates.

Forward windows (clean — neither WFO peeked at the test data):
  W_FWD1: 2026-05-09 -> 2026-05-16   (forward of may9 WFO)
  W_FWD2: 2026-05-16 -> 2026-05-23   (forward of may16 WFO)
  W_FWD3: 2026-05-23 -> now          (forward of may23 WFO)

For each window, compare 3 spreads of the LATEST WFO.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast
from zgb_sim.tick_loader import kill_mt5_terminal
from sim_orb_oos_today import fetch_window, fetch_meta
from sim_wfo_hedge_reverse import StopExtensionCfg, simulate_stop_extension_hedges
from sim_orb_oos_today_hedge_v6 import extract_sl_events, ts_arr_from_ticks

DEPOSIT = 10_000.0
LIVE_SPREAD = 30   # forward-test spread (live-match)
RISK_PCT = 1.5

V7 = dict(fractal_confirm=True, fractal_width=5,
          sma_cross_exit=True, sma_cross_fast=8, sma_cross_slow=21)
HCFG = StopExtensionCfg(exp_min=240, f1_sec=1800, ext_pts=100, tp_mult=3.0, sl_mult=1.0)

# (era_label, test_start, test_end, wfo_dir_template)
ERAS = [
    ("W_FWD1: may9 → 5-9..5-16",  datetime(2026,5,9,tzinfo=timezone.utc),  datetime(2026,5,16,tzinfo=timezone.utc), "wfo_orb_v2_may9_spread{}"),
    ("W_FWD2: may16 → 5-16..5-23", datetime(2026,5,16,tzinfo=timezone.utc), datetime(2026,5,23,tzinfo=timezone.utc), "wfo_orb_v2_may16_spread{}"),
    ("W_FWD3: may23 → 5-23..now",  datetime(2026,5,23,tzinfo=timezone.utc), None, "wfo_orb_v2_may23_spread{}"),  # may23 60pt has special path
]
SPREADS = ["30", "60", "120"]


def cfg_from_row(row) -> ORBConfig:
    return ORBConfig(
        risk_pct=RISK_PCT,
        range_minutes=int(row["range_minutes"]), buffer_pts=0,
        min_range_pts=0, max_range_pts=999_999,
        fixed_sl_pts=int(row["fixed_sl_pts"]),
        rr_ratio=float(row["rr_ratio"]),
        half_tp_ratio=round(float(row["half_tp_ratio"]), 2),
        pending_expire_minutes=int(row["pending_expire_minutes"]),
        daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13,
        **V7, comment="ORB",
    )


def top_n(rank_csv: Path, n: int) -> list[ORBConfig]:
    df = pd.read_csv(rank_csv).head(n)
    return [cfg_from_row(r) for _, r in df.iterrows()]


def run_portfolio(cfgs, ticks, m1, m5, meta, ticks_arr):
    deals = []
    for sn, cfg in enumerate(cfgs, start=1):
        r = simulate_fast(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        parent_pnls, sl_events = extract_sl_events(r.deals)
        for ts, p in parent_pnls:
            deals.append((ts, sn, p))
        sm = {"magic": sn, "risk_pct": RISK_PCT,
              "fixed_sl_pts": cfg.fixed_sl_pts, "rr_ratio": cfg.rr_ratio,
              "half_tp_ratio": cfg.half_tp_ratio,
              "range_minutes": cfg.range_minutes,
              "pending_expire_minutes": cfg.pending_expire_minutes}
        hpn = simulate_stop_extension_hedges(sl_events, ticks_arr, sm, HCFG)
        for ts_ns, p in hpn:
            deals.append((int(ts_ns), sn, p))
    deals.sort(key=lambda x: x[0])
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gains = losses = 0.0; wins = 0
    for _, _sn, p in deals:
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p >= 0: gains += p; wins += 1
        else: losses += -p
    np_ = bal - DEPOSIT
    pf = gains / losses if losses > 0 else float("inf")
    ndd = np_ / dd_abs if dd_abs > 0 else 0.0
    dd_pct = (dd_abs / max(bal_max, DEPOSIT) * 100.0) if bal_max > 0 else 0.0
    return {"np": np_, "dd": dd_abs, "dd_pct": dd_pct, "ndd": ndd, "pf": pf,
            "trades": len(deals), "wins": wins}


def wfo_dir_for(template: str, spread: str) -> Path:
    # Existing may23 60pt is at wfo_orb_v2_may23 (no _spread60 suffix)
    if template == "wfo_orb_v2_may23_spread{}" and spread == "60":
        return ROOT / "output" / "wfo_orb_v2_may23"
    return ROOT / "output" / template.format(spread)


def main():
    sym, m = fetch_meta(None, account="live")
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])

    print("=" * 110)
    print(f"  3×3 forward live comparison: WFO eras × spreads")
    print(f"  Test spread: {LIVE_SPREAD}pt (live-match)  v7 architecture  $10k base  9% risk")
    print("=" * 110)

    results = {}  # (era_idx, spread) -> result
    eras_resolved = []
    for ei, (era_label, ts, te, template) in enumerate(ERAS):
        if te is None:
            te = datetime.now(timezone.utc)
        eras_resolved.append((ei, era_label, ts, te))
        print(f"\n[{era_label}] {ts.date()} → {te.strftime('%Y-%m-%d %H:%M UTC')}")
        sym, ticks, m1, m5 = fetch_window(sym, ts, te, LIVE_SPREAD, account="live")
        ticks_arr = ts_arr_from_ticks(ticks)
        for sp in SPREADS:
            rank_csv = wfo_dir_for(template, sp) / "oos_rank.csv"
            if not rank_csv.exists():
                print(f"  [{sp}pt] MISSING: {rank_csv}")
                continue
            cfgs = top_n(rank_csv, 6)
            r = run_portfolio(cfgs, ticks, m1, m5, meta, ticks_arr)
            results[(ei, sp)] = r
            print(f"  spread={sp}pt   NP=${r['np']:>+8,.0f}  "
                  f"DD=${r['dd']:>7,.0f} ({r['dd_pct']:>5.2f}%)  "
                  f"NDD={r['ndd']:>5.2f}  PF={r['pf']:>5.2f}  trades={r['trades']:>3}")
    kill_mt5_terminal()

    # 3×3 grid output
    print()
    print("=" * 110)
    print(f"  3×3 GRID — Net Profit by (era, WFO spread)")
    print("=" * 110)
    print(f"  {'Era':<30}  {'spread30':>14}  {'spread60':>14}  {'spread120':>14}  {'Best':<14}")
    print(f"  {'-' * 90}")
    per_era_best = {}
    for ei, era_label, _, _ in eras_resolved:
        nps = {sp: results.get((ei, sp), {}).get("np", float("nan")) for sp in SPREADS}
        valid = {k: v for k, v in nps.items() if not (v != v)}  # filter nan
        best = max(valid, key=valid.get) if valid else "—"
        per_era_best[ei] = best
        line = f"  {era_label:<30}  " + "  ".join(
            f"${nps[sp]:>+12,.0f}" if not (nps[sp] != nps[sp]) else f"{'—':>14}"
            for sp in SPREADS)
        line += f"  spread{best}"
        print(line)
    print()

    # Aggregate by spread
    print(f"  AGGREGATE across {len(eras_resolved)} forward windows:")
    print(f"  {'Metric':<20}  {'spread30':>14}  {'spread60':>14}  {'spread120':>14}  {'Winner':<10}")
    print(f"  {'-' * 80}")
    metrics = [("Total NP", "np"), ("Total DD $", "dd"), ("Mean DD %", "dd_pct"),
               ("Mean NDD", "ndd"), ("Mean PF", "pf"), ("Total trades", "trades")]
    for name, k in metrics:
        agg = {}
        for sp in SPREADS:
            vals = [results[(ei, sp)][k] for ei, _, _, _ in eras_resolved if (ei, sp) in results]
            if not vals:
                agg[sp] = float("nan")
                continue
            if name.startswith("Mean"):
                agg[sp] = sum(vals) / len(vals)
            else:
                agg[sp] = sum(vals)
        if name in ("Total DD $", "Mean DD %"):
            winner = min(agg, key=agg.get)
        else:
            winner = max(agg, key=agg.get)
        if name == "Total trades":
            row = f"  {name:<20}  " + "  ".join(f"{int(agg[sp]):>14d}" for sp in SPREADS)
        elif name == "Mean DD %":
            row = f"  {name:<20}  " + "  ".join(f"{agg[sp]:>13.2f}%" for sp in SPREADS)
        elif name.startswith("Mean"):
            row = f"  {name:<20}  " + "  ".join(f"{agg[sp]:>14.2f}" for sp in SPREADS)
        else:
            row = f"  {name:<20}  " + "  ".join(f"${agg[sp]:>+12,.0f}" for sp in SPREADS)
        row += f"  spread{winner}"
        print(row)

    # Window-by-window winners
    print()
    win_counts = {sp: 0 for sp in SPREADS}
    for ei in per_era_best:
        win_counts[per_era_best[ei]] = win_counts.get(per_era_best[ei], 0) + 1
    print(f"  Window-wins:  spread30={win_counts['30']}  spread60={win_counts['60']}  "
          f"spread120={win_counts['120']}")
    print("=" * 110)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
