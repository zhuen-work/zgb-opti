"""6-stream ORB portfolio sim with live haircut.

Runs all 6 streams (S1..S6) from the current setfile, deal-merges on a shared
$10k account, reports portfolio NP/DD$/PF + per-stream contribution + the
live-calibrated haircut projection (NP * 0.94, PF - 0.25).

Per [[feedback_portfolio_sim_after_rotation]] — always run this after a
rotation. Default to the latest v2.1 may16_may9 setfile and the sanity
window (Mar 14 -> May 2, 49d).

Usage:
  python scripts/sim_portfolio_6stream.py
  python scripts/sim_portfolio_6stream.py --setfile configs/sets/dt818_pro_v2.1_9pct_may16_may9.set
  python scripts/sim_portfolio_6stream.py --risk 1.5 --spread 60
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
HAIRCUT_NP = 0.94
HAIRCUT_PF = 0.25


def parse_setfile(path: Path) -> dict:
    out = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith(";") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        val = val.split("||")[0].split(";")[0].strip()
        try:
            v = float(val) if "." in val or val.lstrip("-").isdigit() else val
            if isinstance(v, float) and v.is_integer() and "." not in val:
                v = int(v)
        except ValueError:
            v = val
        out[key.strip()] = v
    return out


def extract_streams(cfg: dict) -> list[dict]:
    """Return [{label, magic, range_min, fixed_sl, rr, htp, expire}] for enabled streams."""
    out = []
    for s in ("S1", "S2", "S3", "S4", "S5", "S6"):
        en = str(cfg.get(f"_ORB_{s}_Enabled", "false")).lower() == "true"
        if not en:
            continue
        out.append({
            "label": f"ORB_{s}",
            "magic": int(cfg[f"_ORB_{s}_Magic"]),
            "range_min": int(cfg[f"_ORB_{s}_RangeMinutes"]),
            "fixed_sl": int(cfg[f"_ORB_{s}_FixedSL_Pts"]),
            "rr": float(cfg[f"_ORB_{s}_RR_Ratio"]),
            "htp": float(cfg[f"_ORB_{s}_HalfTP_Ratio"]),
            "expire": int(cfg[f"_ORB_{s}_PendingExpireMinutes"]),
        })
    return out


def run_stream(s: dict, risk: float, ticks, m1, m5, meta):
    cfg = ORBConfig(
        risk_pct=risk,
        range_minutes=s["range_min"], buffer_pts=0,
        min_range_pts=0, max_range_pts=999_999,
        fixed_sl_pts=s["fixed_sl"], rr_ratio=s["rr"],
        half_tp_ratio=s["htp"], pending_expire_minutes=s["expire"],
        daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13,
        comment=s["label"],
    )
    return orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)


def aggregate(deals: list) -> dict:
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gains = 0.0; losses = 0.0; wins = 0; trades = 0
    for _, _s, p in sorted(deals, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        cur = bal_max - bal
        if cur > dd_abs: dd_abs = cur
        if p >= 0:
            gains += p; wins += 1
        else:
            losses += -p
        trades += 1
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    ndd = np_ / dd_abs if dd_abs > 0 else 0
    pf = gains / losses if losses > 0 else float("inf")
    wr = wins / trades * 100 if trades > 0 else 0
    return dict(np=np_, dd_pct=dd_pct, dd_abs=dd_abs, ndd=ndd, pf=pf, trades=trades, wr=wr)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setfile", default="configs/sets/dt818_pro_v2.1_9pct_may16_may9.set")
    ap.add_argument("--start", default="2026-03-14")
    ap.add_argument("--end", default="2026-05-02")
    ap.add_argument("--spread", type=int, default=60)
    ap.add_argument("--risk", type=float, default=None,
                    help="Override per-stream risk. Default = setfile's _RiskPct.")
    args = ap.parse_args()

    setpath = ROOT / args.setfile
    cfg = parse_setfile(setpath)
    streams = extract_streams(cfg)
    risk = args.risk if args.risk is not None else float(cfg["_RiskPct"])
    total_risk = risk * len(streams)

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    days = (end - start).days

    print("=" * 100)
    print(f"  6-STREAM PORTFOLIO SIM  --  {setpath.name}")
    print(f"  Window: {args.start} -> {args.end} ({days}d)  Deposit: ${DEPOSIT:,.0f}")
    print(f"  Per-stream risk: {risk:.2f}%  ({len(streams)} streams active)  Total: {total_risk:.1f}%")
    print(f"  Spread: {args.spread}pt    Range filter: DISABLED (Min=0, Max=999999)")
    print("=" * 100)

    print("\n  STREAMS:")
    print(f"  {'Stream':<8}  {'Magic':<5}  {'Range':<5}  {'SL':<4}  {'RR':<4}  {'HTP':<4}  {'Expire':<6}")
    for s in streams:
        print(f"  {s['label']:<8}  {s['magic']:<5}  {s['range_min']:<5}  {s['fixed_sl']:<4}  "
              f"{s['rr']:<4}  {s['htp']:<4}  {s['expire']:<6}")

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        ticks = load_ticks(SYMBOL, start, end, spread_pts=args.spread)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)

        print(f"\n  Running per-stream sims (deal-merge, isolated $10k baseline)...")
        deals = []
        per_stream_rows = []
        for s in streams:
            r = run_stream(s, risk, ticks, m1, m5, meta)
            stream_pnl = 0.0; stream_trades = 0
            for d in r.deals:
                if d.kind == "entry":
                    continue
                deals.append((d.ts, s["label"], d.pnl))
                stream_pnl += d.pnl
                stream_trades += 1
            per_stream_rows.append((s["label"], stream_pnl, stream_trades, r.profit_factor,
                                     r.max_drawdown_pct))

        agg = aggregate(deals)

        # Weekly stats for forward projection (ISO week year-week buckets)
        from collections import defaultdict
        weekly = defaultdict(float)
        weekly_trades = defaultdict(int)
        for ts, _s, p in deals:
            wk = ts.isocalendar()
            key = (wk.year, wk.week)
            weekly[key] += p
            weekly_trades[key] += 1
        week_pnls = sorted(weekly.values())
        n_weeks = len(week_pnls)
        weeks_green = sum(1 for p in week_pnls if p > 0)
        weeks_red = n_weeks - weeks_green
        if n_weeks > 0:
            wk_mean = sum(week_pnls) / n_weeks
            wk_med = week_pnls[n_weeks // 2]
            wk_p10 = week_pnls[max(0, int(n_weeks * 0.10))]
            wk_p90 = week_pnls[min(n_weeks - 1, int(n_weeks * 0.90))]
            wk_best = week_pnls[-1]
            wk_worst = week_pnls[0]
            wk_std = (sum((p - wk_mean) ** 2 for p in week_pnls) / n_weeks) ** 0.5
        else:
            wk_mean = wk_med = wk_p10 = wk_p90 = wk_best = wk_worst = wk_std = 0

        print(f"\n  PER-STREAM (isolated $10k -- not deal-merged):")
        print(f"  {'Stream':<8}  {'NP':>10}  {'ROI':>7}  {'PF':>5}  {'DD%':>6}  {'Trades':>6}")
        for label, np_, tr, pf, dd in per_stream_rows:
            roi = np_ / DEPOSIT * 100
            pf_s = f"{pf:.2f}" if pf != float("inf") else "inf"
            print(f"  {label:<8}  {np_:>+10,.0f}  {roi:>+6.1f}%  {pf_s:>5}  {dd:>5.1f}%  {tr:>6}")

        print(f"\n  PORTFOLIO (deal-merged on shared ${DEPOSIT:,.0f}):")
        roi = agg["np"] / DEPOSIT * 100
        pf_s = f"{agg['pf']:.2f}" if agg['pf'] != float("inf") else "inf"
        print(f"    NP:        ${agg['np']:+,.0f}    ROI: {roi:+.1f}%")
        print(f"    DD%:       {agg['dd_pct']:.1f}%  (${agg['dd_abs']:,.0f})")
        print(f"    NP/DD$:    {agg['ndd']:.2f}")
        print(f"    PF:        {pf_s}")
        print(f"    Win rate:  {agg['wr']:.1f}%")
        print(f"    Trades:    {agg['trades']}")

        hc_np = agg["np"] * HAIRCUT_NP
        hc_pf = agg["pf"] - HAIRCUT_PF if agg["pf"] != float("inf") else float("inf")
        hc_pf_s = f"{hc_pf:.2f}" if hc_pf != float("inf") else "inf"
        hc_roi = hc_np / DEPOSIT * 100
        hc_dd = agg["dd_abs"] * 1.05
        # Peak balance from sim: dd_pct = dd_abs / peak_bal * 100, so peak_bal = dd_abs / (dd_pct/100).
        peak_bal = agg["dd_abs"] / (agg["dd_pct"] / 100.0) if agg["dd_pct"] > 0 else DEPOSIT
        hc_peak_bal = peak_bal * HAIRCUT_NP  # scale peak proportionally with NP haircut
        hc_dd_pct = (hc_dd / hc_peak_bal * 100) if hc_peak_bal > 0 else 0
        hc_ndd = hc_np / hc_dd if hc_dd > 0 else 0

        print(f"\n  LIVE HAIRCUT (NP x {HAIRCUT_NP}, PF - {HAIRCUT_PF}, DD$ +5%):")
        print(f"    Expected live NP:    ${hc_np:+,.0f}   ROI: {hc_roi:+.1f}%")
        print(f"    Expected live DD:    ~{hc_dd_pct:.1f}%  (${hc_dd:,.0f})")
        print(f"    Expected live NP/DD: {hc_ndd:.2f}")
        print(f"    Expected live PF:    {hc_pf_s}")

        print(f"\n  CAVEAT: deal-merge of isolated-balance sims under-counts compounding ~10-20%.")
        print(f"          Production with shared balance + compounding likely +15% NP higher.")
        print(f"          Production-adjusted NP estimate: ${agg['np']*1.15:+,.0f}")

        print(f"\n  WEEKLY DISTRIBUTION (n={n_weeks} ISO weeks in window):")
        print(f"  {'Metric':<22} {'Raw sim':>12} {'Live haircut':>14}")
        print(f"  {'Mean week':<22} {wk_mean:>+12,.0f} {wk_mean*HAIRCUT_NP:>+14,.0f}")
        print(f"  {'Median week':<22} {wk_med:>+12,.0f} {wk_med*HAIRCUT_NP:>+14,.0f}")
        print(f"  {'p10 (bad week)':<22} {wk_p10:>+12,.0f} {wk_p10*HAIRCUT_NP:>+14,.0f}")
        print(f"  {'p90 (good week)':<22} {wk_p90:>+12,.0f} {wk_p90*HAIRCUT_NP:>+14,.0f}")
        print(f"  {'Worst week':<22} {wk_worst:>+12,.0f} {wk_worst*HAIRCUT_NP:>+14,.0f}")
        print(f"  {'Best week':<22} {wk_best:>+12,.0f} {wk_best*HAIRCUT_NP:>+14,.0f}")
        print(f"  {'Std dev':<22} {wk_std:>12,.0f} {wk_std*HAIRCUT_NP:>14,.0f}")
        print(f"  {'Green weeks':<22} {weeks_green:>5}/{n_weeks}  ({weeks_green/max(n_weeks,1)*100:.0f}%)")

        print(f"\n  NEXT-WEEK PROJECTION (live):")
        print(f"    Best estimate:    ${wk_mean*HAIRCUT_NP:+,.0f}   ({wk_mean*HAIRCUT_NP/DEPOSIT*100:+.1f}% on $10k)")
        print(f"    Likely range:     ${wk_p10*HAIRCUT_NP:+,.0f}  to  ${wk_p90*HAIRCUT_NP:+,.0f}  (p10-p90)")
        print(f"    Bad-week worst:   ${wk_worst*HAIRCUT_NP:+,.0f}   (1-in-{n_weeks} observed)")
        print(f"    Green-week prob:  ~{weeks_green/max(n_weeks,1)*100:.0f}%")
        print()
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
