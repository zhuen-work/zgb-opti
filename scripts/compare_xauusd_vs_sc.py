"""Compare cached XAUUSD ticks vs Vantage XAUUSD.sc for the same period.

Pulls XAUUSD.sc directly from the running MT5 terminal (Vantage account 23836999),
compares against the existing XAUUSD parquet cache for Apr 26 -> May 1, 2026.
Reports: tick count delta, spread distribution delta, price deviation stats.

If material divergence detected, we should re-pull all backtest data as XAUUSD.sc
for proper live-fidelity calibration.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "output" / "sim_cache"

POINT = 0.01
SC_SYMBOL = "XAUUSD.sc"
PLAIN_SYMBOL = "XAUUSD"


def load_plain_xauusd(start: datetime, end: datetime) -> pd.DataFrame:
    """Load existing cached XAUUSD ticks for the period."""
    files = sorted(CACHE_DIR.glob("ticks_XAUUSD_2026*.parquet"))
    if not files:
        return pd.DataFrame()
    dfs = [pd.read_parquet(p, columns=["ts", "bid", "ask"]) for p in files]
    df = pd.concat(dfs, ignore_index=True)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df[(df["ts"] >= start) & (df["ts"] < end)].copy()


def pull_sc_ticks(start: datetime, end: datetime) -> pd.DataFrame:
    """Pull XAUUSD.sc ticks from the running MT5 terminal."""
    import MetaTrader5 as mt5
    sys.path.insert(0, str(ROOT / "src"))
    from zgb_sim.tick_loader import kill_mt5_terminal
    if not mt5.initialize():
        raise RuntimeError(f"MT5 init failed: {mt5.last_error()}")
    try:
        info = mt5.symbol_info(SC_SYMBOL)
        if info is None:
            print(f"  [error] {SC_SYMBOL} not found in this MT5 terminal")
            return pd.DataFrame()
        if not info.visible:
            mt5.symbol_select(SC_SYMBOL, True)
        print(f"  {SC_SYMBOL}: digits={info.digits} point={info.point} "
              f"spread={info.spread} trade_mode={info.trade_mode}")
        ticks = mt5.copy_ticks_range(SC_SYMBOL, start, end, mt5.COPY_TICKS_ALL)
        if ticks is None or len(ticks) == 0:
            print(f"  [warn] no ticks returned for {SC_SYMBOL}")
            return pd.DataFrame()
        df = pd.DataFrame(ticks)
        df["ts"] = pd.to_datetime(df["time_msc"], unit="ms", utc=True)
        return df[["ts", "bid", "ask"]].copy()
    finally:
        mt5.shutdown()
        kill_mt5_terminal()


def stats_block(df: pd.DataFrame, label: str) -> dict:
    if df.empty:
        print(f"  {label}: EMPTY")
        return {}
    spread = ((df["ask"] - df["bid"]) / POINT).round().astype("int32")
    s = {
        "label": label,
        "n_ticks": len(df),
        "first_ts": df["ts"].min(),
        "last_ts": df["ts"].max(),
        "spread_mean": spread.mean(),
        "spread_p50": int(np.percentile(spread, 50)),
        "spread_p90": int(np.percentile(spread, 90)),
        "spread_p99": int(np.percentile(spread, 99)),
        "spread_max": int(spread.max()),
        "bid_mean": df["bid"].mean(),
        "bid_min": df["bid"].min(),
        "bid_max": df["bid"].max(),
    }
    print(f"\n  {label}")
    print(f"    Ticks:        {s['n_ticks']:>10,}")
    print(f"    First / last: {s['first_ts']}  ->  {s['last_ts']}")
    print(f"    Spread mean:  {s['spread_mean']:.2f} pt")
    print(f"    Spread p50:   {s['spread_p50']} pt")
    print(f"    Spread p90:   {s['spread_p90']} pt")
    print(f"    Spread p99:   {s['spread_p99']} pt")
    print(f"    Spread max:   {s['spread_max']} pt")
    print(f"    Bid range:    {s['bid_min']:.2f}  ->  {s['bid_max']:.2f}  (mean {s['bid_mean']:.2f})")
    return s


def per_minute_diff(plain: pd.DataFrame, sc: pd.DataFrame) -> None:
    """Resample to 1-min bid-mean and compare."""
    if plain.empty or sc.empty:
        return
    plain_m = plain.set_index("ts")["bid"].resample("1min").mean().dropna()
    sc_m = sc.set_index("ts")["bid"].resample("1min").mean().dropna()
    common = plain_m.index.intersection(sc_m.index)
    if len(common) == 0:
        print("\n  [no overlap on 1-min bins]")
        return
    diff = (sc_m.loc[common] - plain_m.loc[common])
    print(f"\n  Per-minute bid difference (SC - plain), {len(common):,} common minutes:")
    print(f"    Mean:    {diff.mean():+.4f}")
    print(f"    Stdev:   {diff.std():.4f}")
    print(f"    Min:     {diff.min():+.4f}")
    print(f"    Max:     {diff.max():+.4f}")
    print(f"    p1:      {np.percentile(diff, 1):+.4f}")
    print(f"    p99:     {np.percentile(diff, 99):+.4f}")


def main() -> int:
    # Apr 28 (Tue) -> May 2 (Sat). Apr 26-27 was weekend so no XAUUSD.sc ticks.
    start = datetime(2026, 4, 28, tzinfo=timezone.utc)
    end = datetime(2026, 5, 2, tzinfo=timezone.utc)

    print("=" * 80)
    print(f"  Comparing XAUUSD vs XAUUSD.sc — {start.date()} -> {end.date()}")
    print("=" * 80)

    plain = load_plain_xauusd(start, end)
    sc = pull_sc_ticks(start, end)

    s_plain = stats_block(plain, f"PLAIN: {PLAIN_SYMBOL} (cached)")
    s_sc = stats_block(sc, f"SC:    {SC_SYMBOL} (pulled fresh from Vantage)")

    if not plain.empty and not sc.empty:
        per_minute_diff(plain, sc)

        print("\n" + "=" * 80)
        print("  VERDICT")
        print("=" * 80)
        spread_delta = s_sc["spread_p50"] - s_plain["spread_p50"]
        bid_delta = s_sc["bid_mean"] - s_plain["bid_mean"]
        tick_ratio = s_sc["n_ticks"] / max(s_plain["n_ticks"], 1)
        print(f"  Spread p50 delta:   {spread_delta:+d} pt  (sc - plain)")
        print(f"  Bid mean delta:     {bid_delta:+.4f}      (price-level alignment)")
        print(f"  Tick density ratio: {tick_ratio:.3f}      (sc/plain)")
        if abs(spread_delta) >= 3 or abs(bid_delta) >= 1.0 or tick_ratio < 0.7 or tick_ratio > 1.4:
            print(f"\n  >> MATERIAL DIVERGENCE — recommend re-pulling backtest cache as XAUUSD.sc")
        else:
            print(f"\n  >> Within tolerance — XAUUSD cache is acceptable proxy for XAUUSD.sc")

    return 0


if __name__ == "__main__":
    sys.exit(main())
