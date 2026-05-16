"""ATR-based dynamic SL test.

HYPOTHESIS: replacing fixed SL_pts with k × ATR(20, H1) adapts to volatility:
  - Quiet days: narrower SL → less $-per-trade risk, but might over-stop
  - Wide days: wider SL → breathing room, fewer premature stops
  - Position size auto-adjusts (fixed-% risk per trade): tighter SL → more lots, wider SL → fewer

METHOD:
  1. Run baseline parent sim (existing fixed-SL deals as entries)
  2. For each entry: compute ATR(period, H1) at entry time → new_sl = k × ATR
  3. Re-walk ticks forward from entry to determine new SL/TP outcome
  4. Recompute lots from fixed-% risk using new_sl: lots = risk_$/((new_sl_pts*point)*contract)
  5. New PnL = (exit_price - entry_price) × direction × new_lots × contract
  6. Aggregate per-stream + portfolio. Compare to baseline.

CAVEATS:
  - HTP not simulated (would shrink the P&L delta marginally; not material at this scale)
  - Pending expiry not affected (entry stays same; only exit geometry changes)
  - This is a counterfactual on EXISTING entries — doesn't test whether different ATR-SL
    would have changed which entries fired (it wouldn't, since entries are pure ORB).

Usage:
  python scripts/sim_atr_sl.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.wfo_helpers import WINDOWS_MAY9 as WINDOWS, to_utc

from sim_wfo_hedge_retry import (STREAM_CFGS, make_stream_cfg,
                                  SYMBOL, DEPOSIT, SPREAD, POINT, CONTRACT,
                                  PARENT_RISK_PROD)


# Sweep grid
ATR_PERIOD = 20             # H1 bars
ATR_TF = "H1"
K_MULTS = [0.1, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50, 0.75, 1.0, 1.5, 2.0]   # SL = k × ATR
RR = 4.0                    # keep RR at current production value


def build_h1_atr(bars_h1: pd.DataFrame, period: int) -> pd.Series:
    """Wilder-style ATR(period) on H1 bars. Returns Series of ATR values in PRICE units
    indexed by bar-close ts (broker-as-UTC)."""
    if bars_h1.empty:
        return pd.Series(dtype=float)
    df = bars_h1.set_index("ts")
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat([(h - l), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=max(5, period // 4)).mean()


def lookup_atr_at(atr_series: pd.Series, ts_ns: int) -> float:
    """Most-recent ATR value strictly before ts_ns. Returns nan if none."""
    if atr_series.empty:
        return float("nan")
    ts = pd.Timestamp(ts_ns, tz="UTC")
    prior = atr_series.loc[atr_series.index < ts]
    if prior.empty:
        return float("nan")
    return float(prior.iloc[-1])


def run_baseline_with_entries(stream: str, ticks, m1, m5, meta, risk_pct: float):
    """Run parent sim and return list of dicts with entry context."""
    cfg = make_stream_cfg(stream, risk_pct)
    r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
    open_positions = []
    closed = []
    for d in r.deals:
        ts_ns = pd.Timestamp(d.ts).value
        if d.kind == "entry":
            open_positions.append({
                "entry_ts_ns": ts_ns,
                "direction": int(d.direction),
                "lots": float(d.lots),
                "entry_price": float(d.price),
            })
            continue
        match_idx = -1
        for i, op in enumerate(open_positions):
            if op["direction"] == int(d.direction):
                match_idx = i; break
        if match_idx < 0:
            continue
        op = open_positions.pop(match_idx)
        closed.append({
            "ts_ns": ts_ns,
            "entry_ts_ns": op["entry_ts_ns"],
            "direction": int(d.direction),
            "entry_price": op["entry_price"],
            "exit_price": float(d.price),
            "baseline_lots": op["lots"],
            "baseline_pnl": float(d.pnl),
            "kind": d.kind,
        })
    return closed


def resim_with_atr_sl(deals: list, ticks_arr: dict, atr_series: pd.Series,
                       k: float, rr: float, sl_pts_min: int = 50, sl_pts_max: int = 5000):
    """Re-simulate each deal with new SL = k * ATR. Returns list of (ts_ns, new_pnl, hit)."""
    ts_arr = ticks_arr["ts_ns"]
    bid = ticks_arr["bid"]
    ask = ticks_arr["ask"]
    out = []
    walk_max_ns = int(8 * 3600 * 1_000_000_000)   # 8h max walk per trade
    risk_dollar = DEPOSIT * (PARENT_RISK_PROD / 100.0)   # constant fixed-% per trade
    for d in deals:
        # ATR at entry time
        atr_price = lookup_atr_at(atr_series, d["entry_ts_ns"])
        if pd.isna(atr_price) or atr_price <= 0:
            continue
        new_sl_pts = int(k * atr_price / POINT)
        # Sanity clamp
        if new_sl_pts < sl_pts_min: new_sl_pts = sl_pts_min
        if new_sl_pts > sl_pts_max: new_sl_pts = sl_pts_max
        new_sl_dist = new_sl_pts * POINT
        new_tp_dist = rr * new_sl_dist

        # Lots from fixed-% risk
        new_lots = risk_dollar / (new_sl_dist * CONTRACT)
        # Round to 0.01 step, floor to 0.01 minimum
        new_lots = max(0.01, round(new_lots / 0.01) * 0.01)

        entry_p = d["entry_price"]
        direction = d["direction"]
        if direction == 1:
            sl_price = entry_p - new_sl_dist
            tp_price = entry_p + new_tp_dist
        else:
            sl_price = entry_p + new_sl_dist
            tp_price = entry_p - new_tp_dist

        # Walk ticks from entry forward
        i0 = np.searchsorted(ts_arr, d["entry_ts_ns"])
        i1 = np.searchsorted(ts_arr, d["entry_ts_ns"] + walk_max_ns)
        post_bid = bid[i0:i1]
        post_ask = ask[i0:i1]
        if len(post_bid) == 0:
            continue
        # Determine which hits first
        if direction == 1:  # BUY position
            sl_hits = np.where(post_bid <= sl_price)[0]
            tp_hits = np.where(post_bid >= tp_price)[0]
        else:                # SELL position
            sl_hits = np.where(post_ask >= sl_price)[0]
            tp_hits = np.where(post_ask <= tp_price)[0]
        sl_first = sl_hits[0] if len(sl_hits) else 10**18
        tp_first = tp_hits[0] if len(tp_hits) else 10**18
        if sl_first == 10**18 and tp_first == 10**18:
            # No hit within walk window — mark as expire at last mid
            last_mid = (post_bid[-1] + post_ask[-1]) / 2
            new_pnl = direction * (last_mid - entry_p) * CONTRACT * new_lots
            hit = "EXPIRE"
        elif sl_first <= tp_first:
            new_pnl = direction * (sl_price - entry_p) * CONTRACT * new_lots
            hit = "SL"
        else:
            new_pnl = direction * (tp_price - entry_p) * CONTRACT * new_lots
            hit = "TP"
        out.append({"ts_ns": d["ts_ns"], "pnl": float(new_pnl), "hit": hit,
                    "new_sl_pts": new_sl_pts, "new_lots": new_lots})
    return out


def aggregate_pnl(deals_list: list) -> tuple[float, float, float, int]:
    """Compute (NP, DD%, NP/DD$, n) from list of deal dicts with 'ts_ns'+'pnl'."""
    pairs = sorted([(d["ts_ns"], d["pnl"]) for d in deals_list])
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gp = gl = 0.0
    for _, p in pairs:
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p > 0: gp += p
        elif p < 0: gl += p
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    ndd = np_ / dd_abs if dd_abs > 0 else 0
    return np_, dd_pct, ndd, len(pairs)


def ts_arr_from_ticks(ticks: pd.DataFrame) -> dict:
    ts_ns = ticks["ts"].dt.tz_convert("UTC").dt.tz_localize(None).astype("datetime64[ns]").astype("int64").to_numpy()
    return {"ts_ns": ts_ns,
            "bid": ticks["bid"].to_numpy(dtype=np.float64),
            "ask": ticks["ask"].to_numpy(dtype=np.float64)}


def main() -> int:
    print("=" * 110)
    print(f"  ATR-DYNAMIC-SL TEST  (counterfactual on baseline parent ORB entries)")
    print(f"  Sweep: SL_pts = k * ATR({ATR_PERIOD}, {ATR_TF}), k in {K_MULTS}")
    print(f"  Streams: {list(STREAM_CFGS.keys())}, MAY9 windows (77 days)")
    print(f"  RR fixed at {RR}, HTP NOT simulated (full SL/TP only)")
    print("=" * 110)

    try:
        from zgb_sim.mt5_accounts import init_account
        init_account("sim")
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        start = to_utc(WINDOWS[0][1])
        end = to_utc(WINDOWS[-1][4])
        print(f"\n  Loading data {start.date()} -> {end.date()}...")
        ticks = load_ticks(SYMBOL, start, end, spread_pts=SPREAD)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)
        h1 = load_bars(SYMBOL, "H1", start, end)
        print(f"  ticks={len(ticks):,}  M1={len(m1):,}  M5={len(m5):,}  H1={len(h1):,}")

        atr_series = build_h1_atr(h1, ATR_PERIOD)
        ticks_arr = ts_arr_from_ticks(ticks)
        # Show ATR distribution
        atr_pts = atr_series.dropna() / POINT
        print(f"\n  ATR({ATR_PERIOD}, H1) distribution (pts): "
              f"p25={atr_pts.quantile(0.25):.0f}, "
              f"med={atr_pts.median():.0f}, "
              f"p75={atr_pts.quantile(0.75):.0f}, "
              f"max={atr_pts.max():.0f}")

        # Baseline parent sims
        print(f"\n  Running baseline parent sims at {PARENT_RISK_PROD}% per stream...")
        per_stream_deals = {}
        per_stream_baseline = {}
        for stream in STREAM_CFGS:
            deals = run_baseline_with_entries(stream, ticks, m1, m5, meta, PARENT_RISK_PROD)
            per_stream_deals[stream] = deals
            base_list = [{"ts_ns": d["ts_ns"], "pnl": d["baseline_pnl"]} for d in deals]
            np_, dd, ndd, n = aggregate_pnl(base_list)
            per_stream_baseline[stream] = (np_, dd, ndd, n)
            print(f"    {stream}: {n} deals, baseline NP=${np_:+,.0f}, DD={dd:.2f}%, NP/DD$={ndd:.2f}")

        # Per-stream ATR sweep
        print(f"\n" + "=" * 110)
        print(f"  PER-STREAM SWEEP")
        print("=" * 110)
        atr_results = {}  # atr_results[stream][k] = (np, dd, ndd, n)
        for stream in STREAM_CFGS:
            atr_results[stream] = {}
            base_np, base_dd, base_ndd, base_n = per_stream_baseline[stream]
            print(f"\n  {stream}  (baseline: SL={STREAM_CFGS[stream]['fixed_sl_pts']}pt  "
                  f"NP=${base_np:+,.0f}  DD={base_dd:.2f}%  NP/DD$={base_ndd:.2f}  n={base_n})")
            print(f"    {'k':>5} {'NP':>10} {'DD%':>6} {'NP/DD$':>8} {'TP':>4} {'SL':>4} "
                  f"{'EXP':>4} {'med SL pt':>10} {'vs base NP/DD$':>16}")
            for k in K_MULTS:
                resimmed = resim_with_atr_sl(per_stream_deals[stream], ticks_arr, atr_series, k, RR)
                np_, dd, ndd, n = aggregate_pnl(resimmed)
                tps = sum(1 for r in resimmed if r["hit"] == "TP")
                sls = sum(1 for r in resimmed if r["hit"] == "SL")
                exps = sum(1 for r in resimmed if r["hit"] == "EXPIRE")
                med_sl = int(np.median([r["new_sl_pts"] for r in resimmed])) if resimmed else 0
                d_ndd = ndd - base_ndd
                marker = " ***" if d_ndd > 0 else ""
                atr_results[stream][k] = (np_, dd, ndd, n, tps, sls, exps, med_sl)
                print(f"    {k:>5.2f} ${np_:>+8,.0f} {dd:>5.2f}% {ndd:>+7.2f} "
                      f"{tps:>4} {sls:>4} {exps:>4} {med_sl:>10} {d_ndd:>+15.2f}{marker}")

        # Portfolio aggregate
        print(f"\n" + "=" * 110)
        print(f"  PORTFOLIO COMPARISON (all 6 streams combined)")
        print("=" * 110)
        all_baseline = []
        for stream in STREAM_CFGS:
            for d in per_stream_deals[stream]:
                all_baseline.append({"ts_ns": d["ts_ns"], "pnl": d["baseline_pnl"]})
        b_np, b_dd, b_ndd, b_n = aggregate_pnl(all_baseline)
        print(f"\n  {'Variant':<14} {'NP':>10} {'DD%':>6} {'NP/DD$':>7} {'Trades':>7}  "
              f"{'vs base NP':>11}  {'vs base NP/DD$':>15}")
        print(f"  {'baseline':<14} ${b_np:>+8,.0f} {b_dd:>5.2f}% {b_ndd:>+6.2f} {b_n:>7}        --              --")
        for k in K_MULTS:
            combined = []
            for stream in STREAM_CFGS:
                resimmed = resim_with_atr_sl(per_stream_deals[stream], ticks_arr, atr_series, k, RR)
                combined.extend(resimmed)
            np_, dd, ndd, n = aggregate_pnl(combined)
            d_np = np_ - b_np
            d_ndd = ndd - b_ndd
            label = f"k={k:.2f} ATR"
            marker = " ***" if d_ndd > 0 else ""
            print(f"  {label:<14} ${np_:>+8,.0f} {dd:>5.2f}% {ndd:>+6.2f} {n:>7} ${d_np:>+9,.0f}  {d_ndd:>+14.2f}{marker}")

    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
