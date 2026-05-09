"""OOS through today — 6-stream v2 rank portfolio.

Window: today (UTC) -> now.
Streams (matches DT818_pro_v2 production):
  S1-3 = PREVIOUS-week WFO top 3 (output/wfo_orb_may2)  magics 1111/2222/3333
  S4-6 = CURRENT-week  WFO top 3 (output/wfo_orb_may9)  magics 4444/5555/6666

Spread: 23pt (live calibration). Deposit: $10k.
Risks: 3% / 6% / 9% total (matches dt818_pro_v2_{3,6,9}pct setfiles, per_stream = total/6).
"""
from __future__ import annotations

import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.wfo_helpers import (WINDOWS_MAY2, WINDOWS_MAY9, rank_with_p0)

SYMBOL = "XAUUSD"
LIVE_SYMBOL = None
DEPOSIT = 10_000.0
PREV_WFO_DIR = ROOT / "output" / "wfo_orb_may2"   # S1-3 source (previous week)
CURR_WFO_DIR = ROOT / "output" / "wfo_orb_may9"   # S4-6 source (current week)
SPREAD_LIVE = 23
_now = datetime.now(timezone.utc)
OOS_START = datetime(_now.year, _now.month, _now.day, tzinfo=timezone.utc)
XAU_POINT = 0.01


def _wait_connected(mt5, timeout_s: int = 30):
    import time
    for _ in range(timeout_s):
        ti = mt5.terminal_info()
        if ti and ti.connected:
            return True
        time.sleep(1)
    return False


def _resolve_xau_symbol(mt5) -> str:
    """Pick the gold symbol available on the current MT5 account."""
    for cand in ("XAUUSD", "XAUUSD.sc", "XAUUSD.crp"):
        if mt5.symbol_select(cand, True) and mt5.symbol_info(cand) is not None:
            return cand
    raise RuntimeError("No XAUUSD variant found on this account")


def fetch_window(symbol: str | None, start: datetime, end: datetime, spread_pts: int,
                  account: str = "sim"):
    """Pull ticks + M1 + M5 directly from MT5 for [start, end). Returns (symbol_used, ticks, m1, m5).

    account: "sim" (XAUUSD on 18912087) or "live" (XAUUSD.sc on 23836999).
    Use account="live" to match the symbol the EA actually trades on — fixes the
    sim-vs-live direction divergence on range-break days.
    """
    import MetaTrader5 as mt5
    import numpy as np
    from zgb_sim.mt5_accounts import init_account
    spec = init_account(account)
    try:
        symbol = symbol or spec.symbol
        # ticks
        arr = mt5.copy_ticks_range(symbol, start, end, mt5.COPY_TICKS_ALL)
        if arr is None or len(arr) == 0:
            raise RuntimeError(f"No ticks for {symbol}: {mt5.last_error()}")
        ticks = pd.DataFrame(arr)
        ticks["ts"] = pd.to_datetime(ticks["time_msc"], unit="ms", utc=True)
        ticks = ticks[["ts", "bid", "ask"]].copy()
        ticks["bid"] = ticks["bid"].astype(np.float64)
        ticks["ask"] = ticks["ask"].astype(np.float64)
        # spread override
        if spread_pts > 0:
            mid = (ticks["bid"] + ticks["ask"]) / 2.0
            half = spread_pts * XAU_POINT / 2.0
            ticks["bid"] = mid - half
            ticks["ask"] = mid + half
        # bars: prime via copy_rates_from_pos (MT5 needs paging for short ranges),
        # then pull the requested window with copy_rates_range
        pad = timedelta(days=5)
        out = {}
        for tf_name, tf_const in [("M1", mt5.TIMEFRAME_M1), ("M5", mt5.TIMEFRAME_M5)]:
            _ = mt5.copy_rates_from_pos(symbol, tf_const, 0, 5000)
            barr = mt5.copy_rates_range(symbol, tf_const, start - pad, end)
            if barr is None or len(barr) == 0:
                raise RuntimeError(f"No {tf_name} bars for {symbol}: {mt5.last_error()}")
            df = pd.DataFrame(barr)
            df["ts"] = pd.to_datetime(df["time"], unit="s", utc=True)
            out[tf_name] = df[["ts", "open", "high", "low", "close"]].reset_index(drop=True)
        return symbol, ticks.reset_index(drop=True), out["M1"], out["M5"]
    finally:
        mt5.shutdown()
        kill_mt5_terminal()


def fetch_meta(symbol: str | None, account: str = "sim"):
    import MetaTrader5 as mt5
    from zgb_sim.mt5_accounts import init_account
    spec = init_account(account)
    try:
        symbol = symbol or spec.symbol
        info = mt5.symbol_info(symbol)
        if info is None:
            raise RuntimeError(f"No symbol info for {symbol}: {mt5.last_error()}")
        # XAUUSD on Vantage trades 100 oz/lot. Some accounts misreport
        # trade_contract_size=1.0; tick_value=1.0 ($1/0.01 tick @ 100 oz) is correct.
        return symbol, {
            "point": info.point, "digits": info.digits,
            "tick_size": info.trade_tick_size,
            "tick_value": 1.0,
            "stops_level": info.trade_stops_level,
            "volume_min": info.volume_min, "volume_max": info.volume_max,
            "volume_step": info.volume_step,
        }
    finally:
        mt5.shutdown()
        kill_mt5_terminal()


def row_to_cfg(row, comment: str, risk_pct: float) -> ORBConfig:
    return ORBConfig(
        risk_pct=risk_pct,
        range_minutes=int(row["range_minutes"]),
        buffer_pts=0,
        min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=int(row["fixed_sl_pts"]),
        rr_ratio=float(row["rr_ratio"]),
        half_tp_ratio=round(float(row["half_tp_ratio"]), 2),
        pending_expire_minutes=240,
        daily_target_pct=float(row["daily_target_pct"]),
        daily_loss_pct=float(row["daily_loss_pct"]),
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True,  ny_start_hour=13,
        comment=comment,
    )


def aggregate(deals):
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    for _, _s, p in sorted(deals, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    return dict(np=np_, dd_pct=dd_pct, dd_abs=dd_abs,
                ndd=(np_ / dd_abs if dd_abs > 0 else 0))


def per_day_breakdown(deals):
    by_day = defaultdict(lambda: {"n": 0, "pnl": 0.0, "per_s": defaultdict(lambda: [0, 0.0])})
    for ts, s, pnl in deals:
        d = pd.Timestamp(ts).date()
        by_day[d]["n"] += 1
        by_day[d]["pnl"] += pnl
        by_day[d]["per_s"][s][0] += 1
        by_day[d]["per_s"][s][1] += pnl
    return by_day


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", choices=["sim", "live"], default="sim",
                    help="Which MT5 account to pull tick data from. 'sim' = 18912087 / XAUUSD "
                         "(default, matches existing parquet cache). 'live' = 23836999 / XAUUSD.sc "
                         "(matches the symbol the EA actually trades on — fixes direction divergence).")
    args = ap.parse_args()
    end = datetime.now(timezone.utc)
    days = (end - OOS_START).days

    def load_top3(wfo_dir: Path, windows):
        # Parquets either named is_{label}.parquet (may2) or p1_is_{label}.parquet (may9).
        def _read(label):
            for prefix in ("", "p1_"):
                p_is = wfo_dir / f"{prefix}is_{label}.parquet"
                p_oos = wfo_dir / f"{prefix}oos_{label}.parquet"
                if p_is.exists() and p_oos.exists():
                    return pd.read_parquet(p_is), pd.read_parquet(p_oos)
            raise FileNotFoundError(f"No IS/OOS parquet for {label} in {wfo_dir}")
        is_per, oos_per = {}, {}
        for label, _, _, _, _ in windows:
            is_per[label], oos_per[label] = _read(label)
        cands = [row_to_cfg(r, "ORB", 3.0) for _, r in oos_per["W1"].iterrows()]
        grid = [row_to_cfg(r, "ORB", 3.0) for _, r in is_per["W1"].iterrows()]
        ranked = rank_with_p0(cands, oos_per, windows, decay_threshold=-0.25,
                              grid_configs=grid, is_per_window=is_per)
        return [ranked[i]["cfg"] for i in range(3)]

    prev_top3 = load_top3(PREV_WFO_DIR, WINDOWS_MAY2)   # S1-3
    curr_top3 = load_top3(CURR_WFO_DIR, WINDOWS_MAY9)   # S4-6

    def cfg_to_row(c):
        return {"range_minutes": c.range_minutes, "fixed_sl_pts": c.fixed_sl_pts,
                "rr_ratio": c.rr_ratio, "half_tp_ratio": c.half_tp_ratio,
                "daily_target_pct": c.daily_target_pct, "daily_loss_pct": c.daily_loss_pct}
    rows = [cfg_to_row(c) for c in (prev_top3 + curr_top3)]
    labels = ["S1", "S2", "S3", "S4", "S5", "S6"]
    sources = ["MAY2 R1 (prev)", "MAY2 R2 (prev)", "MAY2 R3 (prev)",
               "MAY9 R1 (curr)", "MAY9 R2 (curr)", "MAY9 R3 (curr)"]

    print("=" * 100)
    print(f"  v2 6-stream OOS  |  {OOS_START.date()} -> {end.strftime('%Y-%m-%d %H:%M UTC')}  ({days}d)")
    print(f"  Spread {SPREAD_LIVE}pt (live), $10k, total risk split N=6 (per_stream = total/6)")
    print(f"  S1-3 = PREVIOUS week (wfo_orb_may2) | S4-6 = CURRENT week (wfo_orb_may9)")
    for lbl, src, r in zip(labels, sources, rows):
        print(f"    {lbl} ({src}): Range={r['range_minutes']} SL={r['fixed_sl_pts']} "
              f"RR={r['rr_ratio']} HTP={r['half_tp_ratio']}")
    print("=" * 100)

    try:
        sym_used, m = fetch_meta(LIVE_SYMBOL, account=args.account)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        sym_used, ticks, m1, m5 = fetch_window(sym_used, OOS_START, end, SPREAD_LIVE,
                                                account=args.account)
        print(f"  Account: {args.account}  Symbol: {sym_used}")
        print(f"  Ticks: {len(ticks):,} rows  range={ticks.ts.min()} -> {ticks.ts.max()}")
        print(f"  Bars: M1={len(m1):,}  M5={len(m5):,}")

        print(f"\n  {'TotalRisk':<10} {'PerStream':<10} {'NP':>10} {'NP-haircut':>12} "
              f"{'ROI':>7} {'DD%':>6} {'NP/DD$':>7} {'Trades':>7}  Per-stream NP")
        risk_results = {}
        for total_risk in (3.0, 6.0, 9.0):
            per_stream = total_risk / 6
            cfgs = [(labels[i], row_to_cfg(rows[i], labels[i], per_stream)) for i in range(6)]
            deals = []; per_s = {}
            for label, cfg in cfgs:
                r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
                np_s = 0.0; tr_s = 0
                for d in r.deals:
                    if d.kind != "entry":
                        deals.append((d.ts, label, d.pnl))
                        np_s += d.pnl; tr_s += 1
                per_s[label] = (np_s, tr_s)
            agg = aggregate(deals)
            np_haircut = agg["np"] * 0.94
            roi = agg["np"] / DEPOSIT * 100
            ps_str = " ".join(f"{s}:${np_s:+,.0f}({tr_s})" for s, (np_s, tr_s) in per_s.items())
            print(f"  {total_risk:>5.1f}%    {per_stream:>5.2f}%    "
                  f"${agg['np']:>+8,.0f} ${np_haircut:>+10,.0f} {roi:>+6.1f}% "
                  f"{agg['dd_pct']:>5.1f}% {agg['ndd']:>7.2f} {len(deals):>7}  {ps_str}")
            risk_results[total_risk] = (deals, per_s)

        # Per-day breakdown at the live-deployed 9% risk
        deals_live, _ = risk_results[9.0]
        by_day = per_day_breakdown(deals_live)
        print(f"\n  Per-day breakdown @ 9% total risk ({SPREAD_LIVE}pt, matches live deployment):")
        print(f"  {'Date':<12} {'DOW':<4} {'Trades':>6} {'PnL$':>10}  Per-stream")
        for d in sorted(by_day):
            row = by_day[d]
            ps = " ".join(f"{s}:{n}t/${pnl:+,.0f}" for s, (n, pnl) in sorted(row["per_s"].items()))
            print(f"  {str(d):<12} {d.strftime('%a'):<4} {row['n']:>6} ${row['pnl']:>+8,.0f}  {ps}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
