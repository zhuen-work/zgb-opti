"""OOS through today — 6-stream v3 rank portfolio.

Window: today (UTC) -> now.
Streams (matches DT818_pro_v3 production, params parsed from live setfile):
  configs/sets/dt818_pro_v3_9pct_may16_may9.set

Spread: 23pt (live calibration). Deposit: $10k.
Risks: 3% / 6% / 9% total (per_stream = total/6).

Parent ORB only (hedge contribution excluded — use sim_orb_oos_today_hedge.py for full v3).
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
SYMBOL = "XAUUSD"
LIVE_SYMBOL = None
DEPOSIT = 10_000.0
LIVE_SETFILE = ROOT / "configs" / "sets" / "dt818_pro_v7_9pct_may30_may23.set"
# Per feedback_default_test_conditions.md (2026-05-16: "all live = 30pt moving
# forward"). Was 23pt previously; 30pt is the calibration-tight conservative
# pick vs Vantage gold spreads (median ~25pt, max ~32pt).
SPREAD_LIVE = 30
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

    BROKER-TZ FIX: bake the auto-detected broker offset into the bounds passed
    to MT5. MT5 reads datetime args as broker wall-clock; without the shift,
    ~3h of recent data is silently truncated. See
    feedback_no_unverified_account_claims.md (2026-05-15 incident).
    """
    import MetaTrader5 as mt5
    import numpy as np
    from zgb_sim.mt5_accounts import init_account, get_broker_offset
    spec = init_account(account)
    try:
        symbol = symbol or spec.symbol
        # Auto-detect offset; shift end forward so MT5's broker-time read covers
        # all current data. Shift start by same offset only if it's a recent
        # real-UTC moment; for fixed historical bounds (e.g. midnight) it's
        # already broker-aligned and the shift would skip the first 3h.
        # Heuristic: if start is within the last 2 days, treat it as a recent
        # real-UTC moment and shift it. Otherwise leave it.
        broker_off = get_broker_offset(symbol)
        now_utc = datetime.now(timezone.utc)
        days_ago = (now_utc - start).days
        if days_ago < 2:
            mt5_start = start + broker_off
        else:
            mt5_start = start
        mt5_end = end + broker_off
        # ticks
        arr = mt5.copy_ticks_range(symbol, mt5_start, mt5_end, mt5.COPY_TICKS_ALL)
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
        # then pull the requested window with copy_rates_range. Use the same
        # broker-shifted bounds as ticks above for consistency.
        pad = timedelta(days=5)
        out = {}
        for tf_name, tf_const in [("M1", mt5.TIMEFRAME_M1), ("M5", mt5.TIMEFRAME_M5)]:
            _ = mt5.copy_rates_from_pos(symbol, tf_const, 0, 5000)
            barr = mt5.copy_rates_range(symbol, tf_const, mt5_start - pad, mt5_end)
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
    # NOTE: ldn_start_hour=7 / ny_start_hour=13 are BROKER hours.
    # With Vantage at UTC+3 these are REAL UTC 04:00 and 10:00 (Asian + Indian),
    # NOT the LDN/NY sessions live EA actually trades (real UTC 07/13).
    # Sim PnL here will NOT match live PnL even on the same day. See
    # reference_vantage_broker_time.md.
    # v7 deployment: range filter OFF, per-stream expire, V2 fractal-confirm +
    # SMA(8,21) cross-exit globals (parsed from setfile) so apparent friction
    # is apples-to-apples with the live v7 EA.
    return ORBConfig(
        risk_pct=risk_pct,
        range_minutes=int(row["range_minutes"]),
        buffer_pts=0,
        min_range_pts=0, max_range_pts=999_999,
        fixed_sl_pts=int(row["fixed_sl_pts"]),
        rr_ratio=float(row["rr_ratio"]),
        half_tp_ratio=round(float(row["half_tp_ratio"]), 2),
        pending_expire_minutes=int(row.get("pending_expire_minutes", 240)),
        daily_target_pct=float(row["daily_target_pct"]),
        daily_loss_pct=float(row["daily_loss_pct"]),
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True,  ny_start_hour=13,
        fractal_confirm=bool(row.get("fractal_confirm", True)),
        fractal_width=int(row.get("fractal_width", 5)),
        sma_cross_exit=bool(row.get("sma_cross_exit", True)),
        sma_cross_fast=int(row.get("sma_cross_fast", 8)),
        sma_cross_slow=int(row.get("sma_cross_slow", 21)),
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
    # `end` is the requested window upper bound in real UTC. fetch_window()
    # now handles the broker-tz shift internally; just pass real UTC.
    end = datetime.now(timezone.utc)
    days = (end - OOS_START).days

    def parse_setfile(path: Path):
        """Parse _ORB_S{i}_{Param}=val||... lines + global fractal/SMA exit
        settings from the EA setfile, return 6 row dicts (globals attached to each)."""
        import re
        text = path.read_text()

        def _glob(key, default):
            m = re.search(rf"_ORB_{key}=([^|\n;]+)", text)
            if not m:
                return default
            v = m.group(1).strip()
            return v

        fractal_confirm = str(_glob("FractalConfirm", "true")).lower() == "true"
        fractal_width = int(float(_glob("FractalWidth", 5)))
        sma_cross_exit = str(_glob("SMA_CrossExit", "true")).lower() == "true"
        sma_fast = int(float(_glob("SMA_FastPeriod", 8)))
        sma_slow = int(float(_glob("SMA_SlowPeriod", 21)))

        rows = []
        for i in range(1, 7):
            def _get(key, default=None):
                m = re.search(rf"_ORB_S{i}_{key}=([^|]+)\|\|", text)
                if not m:
                    if default is not None:
                        return default
                    raise RuntimeError(f"S{i} {key} not found in {path}")
                return m.group(1).strip()
            rows.append({
                "range_minutes": int(_get("RangeMinutes")),
                "fixed_sl_pts": int(_get("FixedSL_Pts")),
                "rr_ratio": float(_get("RR_Ratio")),
                "half_tp_ratio": float(_get("HalfTP_Ratio")),
                "pending_expire_minutes": int(_get("PendingExpireMinutes", 240)),
                "fractal_confirm": fractal_confirm,
                "fractal_width": fractal_width,
                "sma_cross_exit": sma_cross_exit,
                "sma_cross_fast": sma_fast,
                "sma_cross_slow": sma_slow,
                "daily_target_pct": 999.0,
                "daily_loss_pct": 999.0,
            })
        return rows

    rows = parse_setfile(LIVE_SETFILE)
    labels = ["S1", "S2", "S3", "S4", "S5", "S6"]
    # Read per-stream comment tags for source attribution (v7 setfile encodes them).
    import re as _re
    _txt = LIVE_SETFILE.read_text()
    sources = []
    for i in range(1, 7):
        m = _re.search(rf"_ORB_S{i}_Comment=([^|\n]+)", _txt)
        sources.append(m.group(1).strip() if m else f"S{i}")

    print("=" * 100)
    print(f"  v7 6-stream OOS  |  {OOS_START.date()} -> {end.strftime('%Y-%m-%d %H:%M UTC')}  ({days}d)")
    print(f"  Spread {SPREAD_LIVE}pt (live), $10k, total risk split N=6 (per_stream = total/6)")
    print(f"  Setfile: {LIVE_SETFILE.name}")
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

        # Push today's friction to dt818-console.
        # Today sim NP = sum of trades in by_day matching today's date (live broker
        # date). Today live NP = pulled from D1 via /api/today.
        try:
            import json as _json
            import urllib.request as _u
            import os as _os
            today_d = datetime.now(timezone.utc).date()
            sim_today_raw = sum(row["pnl"] for d, row in by_day.items() if d == today_d)
            # Pull today's live NP + balance anchor from the dashboard API.
            # cf_publish lazy-loads .env; call its loader first so direct
            # os.environ reads below see the dotenv-loaded vars.
            from zgb_sim.cf_publish import _load_dotenv as _cfp_load_dotenv
            _cfp_load_dotenv()
            api = _os.environ.get("CONSOLE_API_BASE", "")
            tok = _os.environ.get("CONSOLE_READ_TOKEN", "") or _os.environ.get("CONSOLE_INGEST_TOKEN", "")
            live_today = 0.0
            balance_anchor = 0.0
            api_ok = False
            if api and tok:
                req = _u.Request(api.rstrip("/") + "/api/today",
                                  headers={"Authorization": f"Bearer {tok}",
                                            "User-Agent": "sim-oos-friction/1.0"})
                with _u.urlopen(req, timeout=10) as resp:
                    body = _json.loads(resp.read().decode())
                today_deals = body.get("today_deals") or []
                # Friction is apples-to-apples PARENT vs PARENT (this script's sim
                # only runs parent ORB; hedge contribution is excluded). For full
                # v3 portfolio friction (parent+hedge) use sim_orb_oos_today_hedge.py.
                prod_magics = {1111, 2222, 3333, 4444, 5555, 6666}
                live_today = sum(float(d.get("profit", 0))
                                  for d in today_deals if int(d.get("magic", 0)) in prod_magics)
                # Balance anchor for scaling sim: prefer projection.baseline_balance
                # (= week-start balance when projection was saved), fall back to
                # current account.balance + |live_today| (rough estimate of period-start).
                proj = body.get("projection") or {}
                balance_anchor = float(proj.get("baseline_balance") or 0.0)
                if balance_anchor <= 0:
                    acct = body.get("account") or {}
                    cur_bal = float(acct.get("balance") or 0.0)
                    balance_anchor = cur_bal - live_today  # rough Mon-open
                api_ok = True
            # Scale sim to live's balance anchor. sim ran on DEPOSIT=$10k; live runs
            # on ~$149k+. Without scaling, dollar comparison is meaningless (sim NP
            # is ~15x smaller, producing nonsense friction% like +94%).
            sim_scaled = sim_today_raw * (balance_anchor / DEPOSIT) if balance_anchor > 0 else sim_today_raw
            from zgb_sim.cf_publish import publish_friction
            # Refuse to publish if API returned no parent deals yet (e.g., live_check
            # hasn't run yet today). A 0 live_np with a non-zero sim would produce a
            # nonsensical friction% and overwrite yesterday's valid record.
            if api_ok and live_today == 0.0 and sim_today_raw != 0.0:
                print(f"\n  [cf_publish] friction skipped: live=$0 (API has no parent deals "
                      f"yet today). Run live_check.py first, then re-run this. "
                      f"sim_raw=${sim_today_raw:+,.0f}")
            elif sim_today_raw == 0.0:
                print(f"\n  [cf_publish] friction skipped: sim_raw=$0 (no sim baseline). "
                      f"Publishing would yield bogus % via denom floor. "
                      f"live=${live_today:+,.0f}")
            else:
                ok = publish_friction(
                    date=today_d.isoformat(),
                    sim_np=float(sim_scaled),  # ALREADY SCALED to live balance
                    live_np=float(live_today),
                    spread_pts=int(SPREAD_LIVE), total_risk=9.0,
                    notes=(f"sim_orb_oos_today vs live; sim_raw=${sim_today_raw:+,.0f} "
                           f"scaled by balance_anchor=${balance_anchor:,.0f}/${DEPOSIT:,.0f}"),
                )
                print(f"\n  [cf_publish] friction push: {'OK' if ok else 'FAIL'} "
                      f"sim_raw=${sim_today_raw:+,.0f} -> sim_scaled=${sim_scaled:+,.0f} "
                      f"(anchor ${balance_anchor:,.0f}/${DEPOSIT:,.0f}), "
                      f"live=${live_today:+,.0f}")
        except Exception as e:
            print(f"\n  [cf_publish] friction skipped: {type(e).__name__}: {e}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
