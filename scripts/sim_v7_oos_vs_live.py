"""v7 OOS sim vs live-actual side-by-side for the same window.

Runs the v7 setfile (with SMA cross-exit + V2 fractal-confirm) on the same
window as the most recent live_check (since last_check marker), pulls actual
live P&L per stream from MT5, and prints side-by-side delta.

Per [[feedback_oos_vs_compare_triggers]] -- this is "show me the difference
OOS vs live", which means BOTH legs + apparent friction delta.

Run: python scripts/sim_v7_oos_vs_live.py
     python scripts/sim_v7_oos_vs_live.py --start 2026-05-25T14:00:00+00:00
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast
from zgb_sim.tick_loader import kill_mt5_terminal
from sim_orb_oos_today import fetch_window, fetch_meta
from sim_wfo_hedge_reverse import (StopExtensionCfg, simulate_stop_extension_hedges,
                                    tag_session_regimes)
from sim_orb_oos_today_hedge_v6 import (parse_v6_setfile, extract_sl_events,
                                         ts_arr_from_ticks)
from compare_v6_v7_portfolio import (parse_setfile, build_cfg_for_stream,
                                      get_bool, get_float, get_int)

V7_SETFILE = ROOT / "configs/sets/dt818_pro_v7_9pct_may30_may23.set"
DEPOSIT = 10_000.0
SPREAD_LIVE = 30

PARENT_MAGICS = {1: 1111, 2: 2222, 3: 3333, 4: 4444, 5: 5555, 6: 6666}
# Hedge magics are read DYNAMICALLY from the deployed setfile (see main) so the
# live-deal lookup always matches whatever is actually deployed. This matters
# because the v7 setfile generator shipped a bug (gen_setfile_v6.py: hedge magic
# = 8000+parent-1000, correct only for S1) so the live setfile uses 8111/9222/
# 10333/11444/12555/13666 instead of 8111-8666. See
# project_setfile_hedge_magic_bug_2026_05_28. The default below is the CORRECT
# scheme, used only if the setfile omits the field.
HEDGE_MAGICS_DEFAULT = {1: 8111, 2: 8222, 3: 8333, 4: 8444, 5: 8555, 6: 8666}


def get_last_check_marker() -> datetime:
    """Read live_check's last_check marker file. Falls back to 24h ago."""
    marker = Path(os.environ.get("APPDATA", "")) / "live_check_state.json"
    candidates = [
        marker,
        ROOT / "output" / "live_check_state.json",
        Path.home() / ".cache" / "live_check_state.json",
    ]
    for p in candidates:
        if p.exists():
            try:
                data = json.loads(p.read_text())
                ts = data.get("last_check_ts") or data.get("last_check")
                if ts:
                    return datetime.fromisoformat(ts)
            except Exception:
                pass
    # Fallback: 48h ago to capture the recent loss window
    return datetime.now(timezone.utc) - timedelta(hours=48)


def fetch_live_deals(start: datetime, end: datetime) -> dict:
    """Return {magic: (count, net_pnl)} for closed deals in window from LIVE account."""
    import MetaTrader5 as mt5
    from zgb_sim.mt5_accounts import init_account
    init_account("live")
    try:
        deals = mt5.history_deals_get(start, end)
        if deals is None:
            return {}
        per_magic = {}
        # Walk in time order — pair entry/out; only count OUT deals for P&L
        for d in deals:
            if d.symbol != "XAUUSD.sc":
                continue
            if d.entry != mt5.DEAL_ENTRY_OUT:
                continue
            per_magic.setdefault(d.magic, {"n": 0, "pnl": 0.0})
            per_magic[d.magic]["n"] += 1
            per_magic[d.magic]["pnl"] += d.profit + d.commission + d.swap
        return per_magic
    finally:
        mt5.shutdown()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None, help="ISO datetime; default = last_check marker or 48h ago")
    ap.add_argument("--end", default=None, help="ISO datetime; default = now")
    args = ap.parse_args()

    end = datetime.fromisoformat(args.end) if args.end else datetime.now(timezone.utc)
    if args.start:
        start = datetime.fromisoformat(args.start)
    else:
        start = get_last_check_marker()
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    print("=" * 110)
    print(f"  v7 OOS sim vs LIVE side-by-side")
    print(f"  Window: {start.isoformat()}  ->  {end.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Setfile: {V7_SETFILE.name}  spread={SPREAD_LIVE}pt  base=$10k")

    # ----- Parse v7 setfile -----
    d = parse_setfile(V7_SETFILE)
    risk = get_float(d, "_RiskPct", 1.5)
    fractal_confirm = get_bool(d, "_ORB_FractalConfirm")
    sma_cross_on = get_bool(d, "_ORB_SMA_CrossExit")
    sma_fast = get_int(d, "_ORB_SMA_FastPeriod", 8)
    sma_slow = get_int(d, "_ORB_SMA_SlowPeriod", 21)
    print(f"  Globals: fractal_confirm={fractal_confirm}  sma_cross_exit={sma_cross_on} ({sma_fast},{sma_slow})")

    # Hedge cfg (uniform v6 STOP-ext, unchanged in v7)
    hedge_cfg = StopExtensionCfg(
        exp_min=get_int(d, "_HEDGE_S1_ExpireMinutes", 240),
        f1_sec=get_int(d, "_HEDGE_S1_MaxSecondsAfterEntry", 1800),
        ext_pts=get_int(d, "_HEDGE_S1_ExtPts", 100),
        tp_mult=get_float(d, "_HEDGE_S1_TPMult", 3.0),
        sl_mult=get_float(d, "_HEDGE_S1_SLMult", 1.0),
    )
    print(f"  Hedge: STOP-ext  ExtPts={hedge_cfg.ext_pts}  TPMult={hedge_cfg.tp_mult}  "
          f"SLMult={hedge_cfg.sl_mult}  F1={hedge_cfg.f1_sec}s")
    # Read hedge magics from the deployed setfile so live-deal lookup matches reality.
    HEDGE_MAGICS = {sn: get_int(d, f"_HEDGE_S{sn}_Magic", HEDGE_MAGICS_DEFAULT[sn])
                    for sn in range(1, 7)}
    if HEDGE_MAGICS != HEDGE_MAGICS_DEFAULT:
        print(f"  [!] setfile hedge magics differ from standard 8111-8666: {HEDGE_MAGICS} "
              f"(magic bug — see project_setfile_hedge_magic_bug_2026_05_28)")
    print("=" * 110)

    # ----- Tick + bar load -----
    sym_used, m = fetch_meta(None, account="live")
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])
    sym_used, ticks, m1, m5 = fetch_window(sym_used, start, end, SPREAD_LIVE, account="live")
    ticks_arr = ts_arr_from_ticks(ticks)
    print(f"  Account: live  Symbol: {sym_used}  Ticks: {len(ticks):,}  M5: {len(m5):,}")

    # ----- v7 per-stream sim (parent + hedge) -----
    rows = []
    for sn in range(1, 7):
        cfg = build_cfg_for_stream(d, sn, risk)
        r = simulate_fast(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        parent_pnls, sl_events = extract_sl_events(r.deals)
        parent_np = sum(p for _, p in parent_pnls)
        parent_trades = len(parent_pnls)
        # Hedge layer
        # STREAM_CFGS row required by hedge dispatcher — use parent's own params
        stream_meta = {
            "magic": HEDGE_MAGICS[sn],
            "risk_pct": risk,
            "fixed_sl_pts": cfg.fixed_sl_pts,
            "rr_ratio": cfg.rr_ratio,
            "half_tp_ratio": cfg.half_tp_ratio,
            "range_minutes": cfg.range_minutes,
            "pending_expire_minutes": cfg.pending_expire_minutes,
        }
        hedge_pnls_ns = simulate_stop_extension_hedges(sl_events, ticks_arr, stream_meta, hedge_cfg)
        hedge_np = sum(p for _, p in hedge_pnls_ns)
        hedge_n = len(hedge_pnls_ns)
        rows.append({
            "stream": f"S{sn}",
            "parent_magic": PARENT_MAGICS[sn],
            "hedge_magic": HEDGE_MAGICS[sn],
            "params": f"R{cfg.range_minutes} SL{cfg.fixed_sl_pts} RR{cfg.rr_ratio} HTP{cfg.half_tp_ratio} E{cfg.pending_expire_minutes}",
            "sim_parent_np": parent_np,
            "sim_parent_trades": parent_trades,
            "sim_hedge_np": hedge_np,
            "sim_hedge_n": hedge_n,
            "sim_total": parent_np + hedge_np,
        })

    kill_mt5_terminal()

    # ----- Live per-magic from MT5 -----
    live = fetch_live_deals(start, end)

    print()
    print("=" * 110)
    print(f"{'Stream':<7} {'Params':<35} {'Sim Parent':>12} {'Live Parent':>12} {'ΔP':>10} | {'Sim Hedge':>11} {'Live Hedge':>11} {'ΔH':>10}")
    print("-" * 110)
    total_sim_p = total_live_p = total_sim_h = total_live_h = 0.0
    for r in rows:
        lp = live.get(r["parent_magic"], {"pnl": 0.0, "n": 0})["pnl"]
        lh = live.get(r["hedge_magic"], {"pnl": 0.0, "n": 0})["pnl"]
        dp = lp - r["sim_parent_np"]
        dh = lh - r["sim_hedge_np"]
        print(f"{r['stream']:<7} {r['params']:<35} "
              f"${r['sim_parent_np']:>+10,.0f} ${lp:>+10,.0f} ${dp:>+8,.0f} | "
              f"${r['sim_hedge_np']:>+9,.0f} ${lh:>+9,.0f} ${dh:>+8,.0f}")
        total_sim_p += r["sim_parent_np"]
        total_live_p += lp
        total_sim_h += r["sim_hedge_np"]
        total_live_h += lh
    print("-" * 110)
    dp_tot = total_live_p - total_sim_p
    dh_tot = total_live_h - total_sim_h
    print(f"{'TOTAL':<7} {'':<35} "
          f"${total_sim_p:>+10,.0f} ${total_live_p:>+10,.0f} ${dp_tot:>+8,.0f} | "
          f"${total_sim_h:>+9,.0f} ${total_live_h:>+9,.0f} ${dh_tot:>+8,.0f}")
    print()
    sim_total = total_sim_p + total_sim_h
    live_total = total_live_p + total_live_h
    delta_total = live_total - sim_total
    friction_pct = (delta_total / max(abs(sim_total), 100)) * 100
    print(f"  Combined sim NP : ${sim_total:+,.2f}")
    print(f"  Combined live NP: ${live_total:+,.2f}")
    print(f"  Delta (live-sim): ${delta_total:+,.2f}")
    print(f"  Apparent friction = (live-sim)/|sim| = {friction_pct:+.1f}%   "
          f"(- = live worse than sim, + = live better)")
    print("=" * 110)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
