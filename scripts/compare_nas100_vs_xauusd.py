"""Pull NAS100.r tick data on the sim account and compare against XAUUSD
to scope what DT818_pro_v2 (ORB) needs to be modified for an index symbol.

Outputs:
  - symbol meta (point/digits/tick_value/contract_size)
  - spread distribution (median / p90 / p99)
  - per-session range stats (LDN 07-08 UTC, NY 13-14 UTC) over 90/60/30 min
  - daily ATR-style range (H1 / D1)
  - tick density per minute (sanity check on liquidity)
  - which UTC hours the symbol actually trades

Run:  python scripts/compare_nas100_vs_xauusd.py
"""
from __future__ import annotations
import json
import sys
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# Make src importable when running as a script
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import MetaTrader5 as mt5  # noqa: E402

from zgb_sim.mt5_accounts import init_account  # noqa: E402
from zgb_sim.tick_loader import (  # noqa: E402
    CACHE_DIR, kill_mt5_terminal, load_ticks, load_bars,
)


NAS = "NAS100.r"
GOLD = "XAUUSD"

# Window: last ~30 trading days
END = datetime(2026, 5, 9, tzinfo=timezone.utc)
START = datetime(2026, 4, 1, tzinfo=timezone.utc)


# -------------------------------------------------------------------- #
# Pull/cache NAS100.r ticks + bars (XAUUSD already cached via WFO runs)
# -------------------------------------------------------------------- #

def _pull_ticks_month(symbol: str, year: int, month: int) -> pd.DataFrame:
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    end = (datetime(year + (month == 12), 1 if month == 12 else month + 1, 1,
                    tzinfo=timezone.utc))
    arr = mt5.copy_ticks_range(symbol, start, end, mt5.COPY_TICKS_ALL)
    if arr is None or len(arr) == 0:
        raise RuntimeError(f"No ticks for {symbol} {year}-{month:02d}: "
                           f"{mt5.last_error()}")
    df = pd.DataFrame(arr)
    df["ts"] = pd.to_datetime(df["time_msc"], unit="ms", utc=True)
    return df[["ts", "bid", "ask"]].astype(
        {"bid": "float64", "ask": "float64"}).reset_index(drop=True)


def _ensure_nas_cached() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    months = [(2026, 4), (2026, 5)]
    missing = [(y, m) for y, m in months
               if not (CACHE_DIR / f"ticks_{NAS}_{y}{m:02d}.parquet").exists()]
    meta_path = CACHE_DIR / f"meta_{NAS}.json"
    if not missing and meta_path.exists():
        return

    spec = init_account("sim")
    print(f"[mt5] connected as {spec.login} ({spec.purpose})")
    try:
        # Symbol metadata
        if not meta_path.exists():
            mt5.symbol_select(NAS, True)
            si = mt5.symbol_info(NAS)
            if si is None:
                raise RuntimeError(f"{NAS} not found on sim account.")
            meta = {
                "point": si.point, "digits": si.digits,
                "tick_size": si.trade_tick_size,
                "tick_value": si.trade_tick_value,
                "stops_level": si.trade_stops_level,
                "volume_min": si.volume_min,
                "volume_max": si.volume_max,
                "volume_step": si.volume_step,
                "contract_size": si.trade_contract_size,
                "spread_current_pts": si.spread,
                "trade_mode": si.trade_mode,
            }
            meta_path.write_text(json.dumps(meta, indent=2))
            print(f"  saved meta: {meta}")

        for y, m in missing:
            print(f"  pulling {NAS} ticks {y}-{m:02d} ...")
            df = _pull_ticks_month(NAS, y, m)
            df.to_parquet(CACHE_DIR / f"ticks_{NAS}_{y}{m:02d}.parquet",
                          index=False)
            print(f"    {len(df):,} ticks  "
                  f"{df['ts'].min()} -> {df['ts'].max()}")
    finally:
        mt5.shutdown()
        kill_mt5_terminal()


def _load_nas_ticks(start: datetime, end: datetime) -> pd.DataFrame:
    parts = []
    for y, m in [(2026, 4), (2026, 5)]:
        p = CACHE_DIR / f"ticks_{NAS}_{y}{m:02d}.parquet"
        if p.exists():
            parts.append(pd.read_parquet(p))
    df = pd.concat(parts, ignore_index=True)
    return df[(df["ts"] >= start) & (df["ts"] < end)].reset_index(drop=True)


# -------------------------------------------------------------------- #
# Stats
# -------------------------------------------------------------------- #

def spread_stats(ticks: pd.DataFrame, point: float) -> dict:
    sp = ((ticks["ask"] - ticks["bid"]) / point).to_numpy()
    sp = sp[np.isfinite(sp) & (sp >= 0)]
    return {
        "median_pts": float(np.median(sp)),
        "p75_pts": float(np.percentile(sp, 75)),
        "p90_pts": float(np.percentile(sp, 90)),
        "p99_pts": float(np.percentile(sp, 99)),
        "max_pts": float(np.max(sp)),
        "n_ticks": int(sp.size),
    }


def hourly_activity(ticks: pd.DataFrame) -> pd.Series:
    s = ticks.set_index("ts")["bid"].resample("1h").count()
    by_hour = s.groupby(s.index.hour).mean()
    return by_hour


def session_range_stats(
    ticks: pd.DataFrame, hour_utc: int, length_min: int, point: float
) -> dict:
    """For each weekday, range = max(mid)-min(mid) over [hour, hour+length)."""
    mid = (ticks["bid"] + ticks["ask"]) / 2.0
    df = pd.DataFrame({"ts": ticks["ts"], "mid": mid})
    df["date"] = df["ts"].dt.date
    df["hr"] = df["ts"].dt.hour
    df["min"] = df["ts"].dt.minute
    in_window = (df["ts"].dt.hour * 60 + df["ts"].dt.minute >= hour_utc * 60) & \
                (df["ts"].dt.hour * 60 + df["ts"].dt.minute < hour_utc * 60 + length_min)
    sub = df[in_window].copy()
    sub = sub[sub["ts"].dt.weekday < 5]
    g = sub.groupby("date")["mid"].agg(["max", "min", "count"])
    g = g[g["count"] > 5]
    rng_pts = (g["max"] - g["min"]) / point
    return {
        "n_days": int(len(g)),
        "median_range_pts": float(np.median(rng_pts)) if len(g) else float("nan"),
        "p25_range_pts": float(np.percentile(rng_pts, 25)) if len(g) else float("nan"),
        "p75_range_pts": float(np.percentile(rng_pts, 75)) if len(g) else float("nan"),
    }


def daily_range_pts(ticks: pd.DataFrame, point: float) -> dict:
    mid = (ticks["bid"] + ticks["ask"]) / 2.0
    df = pd.DataFrame({"ts": ticks["ts"], "mid": mid})
    df["date"] = df["ts"].dt.date
    df = df[df["ts"].dt.weekday < 5]
    g = df.groupby("date")["mid"].agg(["max", "min", "count"])
    g = g[g["count"] > 50]
    rng = (g["max"] - g["min"]) / point
    return {
        "n_days": int(len(g)),
        "median_pts": float(np.median(rng)),
        "p25_pts": float(np.percentile(rng, 25)),
        "p75_pts": float(np.percentile(rng, 75)),
    }


def trading_hours_coverage(ticks: pd.DataFrame) -> dict:
    """Fraction of weekdays that have >=1 tick in each UTC hour."""
    df = pd.DataFrame({"ts": ticks["ts"]})
    df = df[df["ts"].dt.weekday < 5]
    df["date"] = df["ts"].dt.date
    df["hr"] = df["ts"].dt.hour
    n_days = df["date"].nunique()
    cov = df.groupby("hr")["date"].nunique() / max(n_days, 1)
    return cov.to_dict()


# -------------------------------------------------------------------- #
# Main
# -------------------------------------------------------------------- #

def main() -> None:
    print(f"\nWindow: {START.date()} -> {END.date()}\n")

    _ensure_nas_cached()

    nas = _load_nas_ticks(START, END)
    print(f"[NAS100.r] loaded {len(nas):,} ticks")

    # XAUUSD: load with REAL spreads (spread_pts=0) so comparison is apples-to-apples
    gold = load_ticks(GOLD, START, END, spread_pts=0)
    print(f"[XAUUSD]   loaded {len(gold):,} ticks")

    # ---------- meta ----------
    nas_meta = json.loads((CACHE_DIR / f"meta_{NAS}.json").read_text())
    gold_meta_path = CACHE_DIR / f"meta_{GOLD}.json"
    gold_meta = json.loads(gold_meta_path.read_text()) if gold_meta_path.exists() else {
        "point": 0.01, "digits": 2, "tick_size": 0.01, "tick_value": 1.0,
        "contract_size": 100.0, "volume_min": 0.01, "volume_step": 0.01,
    }

    print("\n========== SYMBOL META ==========")
    print(f"{'field':<22}{'NAS100.r':>18}{'XAUUSD':>18}")
    for k in ["point", "digits", "tick_size", "tick_value",
              "contract_size", "volume_min", "volume_step", "stops_level"]:
        nv = nas_meta.get(k, "-")
        gv = gold_meta.get(k, "-")
        print(f"{k:<22}{str(nv):>18}{str(gv):>18}")

    # ---------- spread ----------
    print("\n========== SPREAD (real) — pts ==========")
    ns = spread_stats(nas, nas_meta["point"])
    gs = spread_stats(gold, gold_meta["point"])
    print(f"{'metric':<14}{'NAS100.r':>14}{'XAUUSD':>14}")
    for k in ["median_pts", "p75_pts", "p90_pts", "p99_pts", "max_pts"]:
        print(f"{k:<14}{ns[k]:>14.1f}{gs[k]:>14.1f}")

    # Spread $ cost per side per min lot (1 tick = tick_value $)
    ns_cost = ns["median_pts"] * (nas_meta["tick_value"] / 1.0) * nas_meta["volume_min"]
    gs_cost = gs["median_pts"] * (gold_meta["tick_value"] / 1.0) * gold_meta["volume_min"]
    print(f"\nSpread $ cost per side @ volume_min:")
    print(f"  NAS100.r: ${ns_cost:.3f}   XAUUSD: ${gs_cost:.3f}")

    # ---------- daily range ----------
    print("\n========== DAILY RANGE — pts ==========")
    ndr = daily_range_pts(nas, nas_meta["point"])
    gdr = daily_range_pts(gold, gold_meta["point"])
    print(f"{'metric':<14}{'NAS100.r':>14}{'XAUUSD':>14}")
    print(f"{'n_days':<14}{ndr['n_days']:>14}{gdr['n_days']:>14}")
    for k in ["p25_pts", "median_pts", "p75_pts"]:
        print(f"{k:<14}{ndr[k]:>14.0f}{gdr[k]:>14.0f}")

    # ---------- session range (ORB-relevant) ----------
    print("\n========== SESSION OPENING RANGE (median pts) ==========")
    print(f"{'session':<26}{'NAS100.r':>14}{'XAUUSD':>14}")
    for label, hr, length in [
        ("LDN 07:00 UTC, 30min", 7, 30),
        ("LDN 07:00 UTC, 60min", 7, 60),
        ("LDN 07:00 UTC, 90min", 7, 90),
        ("NY  13:00 UTC, 30min", 13, 30),
        ("NY  13:00 UTC, 60min", 13, 60),
        ("NY  13:00 UTC, 90min", 13, 90),
        ("US-cash 14:30 UTC, 30min", 14, 30),  # actually 14:30 close enough
    ]:
        ns = session_range_stats(nas, hr, length, nas_meta["point"])
        gs = session_range_stats(gold, hr, length, gold_meta["point"])
        print(f"{label:<26}{ns['median_range_pts']:>14.0f}"
              f"{gs['median_range_pts']:>14.0f}")

    # ---------- trading hours coverage ----------
    print("\n========== HOURLY TICK COVERAGE (% of weekdays w/ ticks) ==========")
    ncov = trading_hours_coverage(nas)
    gcov = trading_hours_coverage(gold)
    print(f"{'hr_utc':<8}{'NAS100.r':>12}{'XAUUSD':>12}")
    for h in range(24):
        n = ncov.get(h, 0.0) * 100
        g = gcov.get(h, 0.0) * 100
        print(f"{h:<8}{n:>11.0f}%{g:>11.0f}%")

    # ---------- tick density ----------
    print("\n========== TICK DENSITY (avg ticks/hour by UTC hr) ==========")
    nh = hourly_activity(nas)
    gh = hourly_activity(gold)
    print(f"{'hr_utc':<8}{'NAS100.r':>12}{'XAUUSD':>12}")
    for h in range(24):
        nv = nh.get(h, 0)
        gv = gh.get(h, 0)
        print(f"{h:<8}{nv:>12.0f}{gv:>12.0f}")


if __name__ == "__main__":
    main()
