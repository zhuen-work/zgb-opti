"""A/B test: ORB entry at wick extremes vs body extremes.

Hypothesis: body-based entries (max(open,close) / min(open,close)) trigger
sooner than wick-based (max(high) / min(low)) because body_high <= wick_high
always. This means:
  - MORE trades (lower threshold to fire)
  - Wider SL distances relative to entry (range looks narrower)
  - Possibly NOISIER (some entries that would've been "wick poked but body held"
    in wick mode now register as breakouts)

Test conditions per feedback_default_test_conditions.md (6% baseline 2026-05-19):
  - Risk: 6% TOTAL split across 6 streams = 1.0% per stream
  - Spread: 30pt
  - Deposit: $10,000
  - Window: 2026-02-14 to 2026-04-25 (70 days)
  - All 6 v3 streams (S1-S6) from current setfile

Reports per-stream + portfolio NP, DD%, NP/DD$, PF, trade count, win rate
for both wick and body modes.
"""
from __future__ import annotations
import sys
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd
from zgb_sim.tick_loader import load_ticks, load_bars, symbol_meta, kill_mt5_terminal
from zgb_sim.mt5_accounts import init_account
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate

LIVE_SETFILE = ROOT / "configs" / "sets" / "dt818_pro_v3_9pct_may16_may9.set"
DEPOSIT = 10_000.0
SPREAD = 30  # per feedback_default_test_conditions.md
RISK_PCT_PER_STREAM = 1.0     # 6% total / 6 streams (baseline 2026-05-19)
START = date(2026, 2, 14)
END   = date(2026, 4, 25)
DAYS  = (END - START).days


def parse_setfile(path: Path):
    text = path.read_text()
    rows = []
    for i in range(1, 7):
        def _get(key):
            m = re.search(rf"_ORB_S{i}_{key}=([^|]+)\|\|", text)
            return m.group(1).strip()
        rows.append({
            "stream": f"S{i}",
            "range_minutes": int(_get("RangeMinutes")),
            "fixed_sl_pts": int(_get("FixedSL_Pts")),
            "rr_ratio": float(_get("RR_Ratio")),
            "half_tp_ratio": float(_get("HalfTP_Ratio")),
        })
    return rows


def row_to_cfg(row, mode: str) -> ORBConfig:
    return ORBConfig(
        risk_pct=RISK_PCT_PER_STREAM,
        range_minutes=row["range_minutes"],
        buffer_pts=0,
        min_range_pts=0, max_range_pts=999_999,
        fixed_sl_pts=row["fixed_sl_pts"],
        rr_ratio=row["rr_ratio"],
        half_tp_ratio=row["half_tp_ratio"],
        pending_expire_minutes=240,
        daily_target_pct=999.0, daily_loss_pct=999.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True,  ny_start_hour=13,
        entry_mode=mode,
        comment=f"{row['stream']}_{mode}",
    )


def aggregate_deals(deals_by_stream: dict[str, list]):
    """Deal-merge across streams for portfolio NP + DD."""
    all_deals = []
    for stream, deals in deals_by_stream.items():
        for d in deals:
            all_deals.append((d.ts, d.pnl, stream))
    all_deals.sort(key=lambda x: x[0])
    bal = DEPOSIT
    bal_max = DEPOSIT
    dd_abs = 0.0
    for _, pnl, _ in all_deals:
        bal += pnl
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    ndd = np_ / dd_abs if dd_abs > 0 else 0
    wins = sum(1 for _, p, _ in all_deals if p > 0)
    losses = sum(1 for _, p, _ in all_deals if p < 0)
    grossW = sum(p for _, p, _ in all_deals if p > 0)
    grossL = sum(p for _, p, _ in all_deals if p < 0)
    pf = grossW / abs(grossL) if grossL != 0 else float("inf")
    return {
        "np": np_, "dd_abs": dd_abs, "dd_pct": dd_pct, "ndd": ndd,
        "trades": wins + losses, "wins": wins, "losses": losses,
        "win_rate": wins / (wins + losses) if (wins + losses) > 0 else 0,
        "pf": pf,
    }


def run_mode(mode: str, streams: list, ticks, m1, m5, meta):
    print(f"\n{'='*100}")
    print(f"ENTRY MODE: {mode.upper()}")
    print(f"{'='*100}")
    print(f"{'Stream':<5} {'Range':>5} {'SL':>5} {'RR':>4} {'HTP':>4}  "
          f"{'Trades':>6} {'W/L':>9} {'WR%':>5} {'PF':>5} {'NP$':>10} {'DD%':>5} {'NP/DD$':>7}")
    deals_by_stream = {}
    for row in streams:
        cfg = row_to_cfg(row, mode)
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        stream_deals = [d for d in r.deals if d.kind != "entry"]
        deals_by_stream[row["stream"]] = stream_deals
        s_stats = aggregate_deals({row["stream"]: stream_deals})
        print(f"{row['stream']:<5} {row['range_minutes']:>5} {row['fixed_sl_pts']:>5} "
              f"{row['rr_ratio']:>4.1f} {row['half_tp_ratio']:>4.1f}  "
              f"{s_stats['trades']:>6} {s_stats['wins']}/{s_stats['losses']:<7} "
              f"{s_stats['win_rate']*100:>4.1f}% "
              f"{s_stats['pf']:>5.2f} {s_stats['np']:>+10,.0f} {s_stats['dd_pct']:>5.1f}% "
              f"{s_stats['ndd']:>7.2f}")

    p = aggregate_deals(deals_by_stream)
    print(f"{'-'*100}")
    print(f"{'TOTAL':<5} {'':>5} {'':>5} {'':>4} {'':>4}  "
          f"{p['trades']:>6} {p['wins']}/{p['losses']:<7} "
          f"{p['win_rate']*100:>4.1f}% "
          f"{p['pf']:>5.2f} {p['np']:>+10,.0f} {p['dd_pct']:>5.1f}% "
          f"{p['ndd']:>7.2f}")
    return p, deals_by_stream


def main():
    streams = parse_setfile(LIVE_SETFILE)
    print(f"A/B test: entry at wick extremes vs body extremes")
    print(f"  Window:  {START} -> {END}  ({DAYS} days)")
    print(f"  Spread:  {SPREAD}pt")
    print(f"  Risk:    {RISK_PCT_PER_STREAM}%/stream ({RISK_PCT_PER_STREAM*6}% total)")
    print(f"  Deposit: ${DEPOSIT:,.0f}")
    print(f"  Setfile: {LIVE_SETFILE.name}")
    print(f"  Streams: {len(streams)}")

    init_account("sim")  # required for MT5 fallback when cache miss
    print(f"\nLoading ticks for XAUUSD {START} -> {END} (+1 day pad)...")
    pad_end = END + timedelta(days=1)
    start_dt = datetime.combine(START, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = datetime.combine(pad_end, datetime.min.time(), tzinfo=timezone.utc)
    SYMBOL = "XAUUSD"
    m_dict = symbol_meta(SYMBOL)
    ticks = load_ticks(SYMBOL, start_dt, end_dt, spread_pts=SPREAD)
    m1 = load_bars(SYMBOL, "M1", start_dt, end_dt)
    m5 = load_bars(SYMBOL, "M5", start_dt, end_dt)
    print(f"  Ticks: {len(ticks):,}  M1: {len(m1):,}  M5: {len(m5):,}")
    meta = SymbolMeta(
        point=m_dict["point"], digits=m_dict["digits"],
        tick_size=m_dict["tick_size"], tick_value=m_dict["tick_value"],
        stops_level_pts=m_dict["stops_level"],
        volume_min=m_dict["volume_min"], volume_max=m_dict["volume_max"],
        volume_step=m_dict["volume_step"],
    )

    try:
        wick_p, _ = run_mode("wick", streams, ticks, m1, m5, meta)
        body_p, _ = run_mode("body", streams, ticks, m1, m5, meta)

        print(f"\n{'='*100}")
        print("COMPARISON (body - wick)")
        print(f"{'='*100}")
        rows = [
            ("Trades",    wick_p['trades'],         body_p['trades'],         body_p['trades'] - wick_p['trades'],         "{:+d}"),
            ("Win %",     wick_p['win_rate']*100,   body_p['win_rate']*100,   (body_p['win_rate']-wick_p['win_rate'])*100, "{:+.1f}pp"),
            ("PF",        wick_p['pf'],             body_p['pf'],             body_p['pf'] - wick_p['pf'],                 "{:+.2f}"),
            ("NP $",      wick_p['np'],             body_p['np'],             body_p['np'] - wick_p['np'],                 "{:+,.0f}"),
            ("DD %",      wick_p['dd_pct'],         body_p['dd_pct'],         body_p['dd_pct'] - wick_p['dd_pct'],         "{:+.2f}pp"),
            ("NP/DD $",   wick_p['ndd'],            body_p['ndd'],            body_p['ndd'] - wick_p['ndd'],                "{:+.2f}"),
        ]
        print(f"  {'Metric':<10} {'WICK':>15} {'BODY':>15} {'delta (body-wick)':>18}")
        for label, w, b, d, fmt in rows:
            if isinstance(w, float):
                print(f"  {label:<10} {w:>15.2f} {b:>15.2f} {fmt.format(d):>18}")
            else:
                print(f"  {label:<10} {w:>15} {b:>15} {fmt.format(d):>18}")

        better = "BODY" if body_p['ndd'] > wick_p['ndd'] else "WICK"
        print(f"\n  Verdict: {better} mode has higher NP/DD$ on this 70d window.")
        if abs(body_p['ndd'] - wick_p['ndd']) < 0.5:
            print(f"  (delta NP/DD$ is small — likely noise; would want multi-window confirmation)")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
