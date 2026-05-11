"""Per-stream hedge WFO — uses WINDOWS_MAY2 + rank_with_p0.

For each of S1/S2/S3:
  - Parent params FIXED at the existing rank-N winner (from output/wfo_orb_may2)
  - Sweep hedge grid: buffer × hedge_sl × hedge_rr × expire (= 5*4*3*2 = 120 cells)
  - For each window (W1-W4), evaluate baseline parent + hedge on IS and OOS slices
  - Store combined NP/DD$ per (cell, window) — feed to rank_with_p0 with plateau
  - Save rank-1 hedge config per stream

Then run a portfolio comparison on Feb 14 -> May 1 (76d):
  - Without hedge: 3-stream portfolio at production sizing
  - With hedge:    3-stream portfolio + per-stream hedges

Sizing:
  - Per-stream WFO: parent at 3% (matching IS sweep methodology), hedge at 1%.
    Ranking is invariant to scaling; this just keeps numbers comparable to sweep.
  - Portfolio compare: each stream parent at 1%, each hedge at 1% (mirrors
    production 3% setfile per-stream allocation).
"""
from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.wfo_helpers import (WINDOWS_MAY9 as WINDOWS, rank_with_p0,
                                  print_phase_d_with_p0, select_winner_with_p0,
                                  check_winner_boundaries, print_boundary_check, to_utc)

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
SPREAD = 23
POINT = 0.01
CONTRACT = 100

STREAM_CFGS = {
    "S1": dict(range_minutes=90, fixed_sl_pts=650, rr_ratio=4.0, half_tp_ratio=0.25),
    "S2": dict(range_minutes=90, fixed_sl_pts=350, rr_ratio=4.0, half_tp_ratio=0.5),
    "S3": dict(range_minutes=90, fixed_sl_pts=400, rr_ratio=4.0, half_tp_ratio=0.5),
}

# Hedge sweep grid for the WFO
BUFFERS    = [0, 50, 100, 200, 350]
HEDGE_SLS  = [300, 500, 700, 900]
HEDGE_RRS  = [2.0, 3.0, 4.0]
EXPIRES    = [30, 120]
HEDGE_RISK_PCT = 1.0   # mirror production per-stream allocation

PARENT_RISK_SWEEP = 3.0  # used for ranking (invariant to scaling)
PARENT_RISK_PROD  = 1.0  # used for portfolio comparison


@dataclass(frozen=True)
class HedgeCfg:
    buf: int
    h_sl: int
    h_rr: float
    exp: int


def make_stream_cfg(stream: str, risk_pct: float) -> ORBConfig:
    sc = STREAM_CFGS[stream]
    # NOTE: ldn=7/ny=13 are BROKER-time hours; with Vantage at UTC+3 these
    # select REAL UTC 04:00 / 10:00, NOT actual LDN/NY. See
    # reference_vantage_broker_time.md. Live EA uses TimeGMT() = real UTC,
    # so live trades a different session than this sim optimizes.
    return ORBConfig(
        risk_pct=risk_pct,
        range_minutes=sc["range_minutes"],
        buffer_pts=0,
        min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=sc["fixed_sl_pts"],
        rr_ratio=sc["rr_ratio"],
        half_tp_ratio=sc["half_tp_ratio"],
        pending_expire_minutes=240,
        daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True,  ny_start_hour=13,
        comment=stream,
    )


def slice_window(df: pd.DataFrame, ts_col: str, start: datetime, end: datetime) -> pd.DataFrame:
    return df[(df[ts_col] >= start) & (df[ts_col] < end)].reset_index(drop=True)


def aggregate(deals_pnl_pairs):
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gp = gl = 0.0
    for _, p in sorted(deals_pnl_pairs, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p > 0: gp += p
        elif p < 0: gl += p
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    pf = (gp / abs(gl)) if gl < 0 else float("inf")
    return np_, dd_pct, pf


def run_baseline_window(stream: str, ticks, m1, m5, meta, risk_pct: float):
    """Run parent sim on a window. Returns (deals_pnl_pairs, sl_events)."""
    cfg = make_stream_cfg(stream, risk_pct)
    r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
    deals = []
    sl_events = []
    for d in r.deals:
        if d.kind == "entry":
            continue
        ts_ns = pd.Timestamp(d.ts).value
        deals.append((ts_ns, d.pnl))
        if d.kind == "sl":
            sl_events.append({"ts_ns": ts_ns, "direction": int(d.direction),
                              "sl_price": float(d.price), "lots": float(d.lots)})
    return deals, sl_events


def simulate_hedges(sl_events, ticks_arr, hcfg: HedgeCfg, order_type: str = "limit") -> list:
    """Post-process hedge for one window. Returns list of (exit_ts_ns, pnl).

    order_type:
      "limit" — opposite-direction LIMIT at SL ± buffer (REVERSAL bet).
                For parent BUY: SELL LIMIT ABOVE SL, fires when bid >= entry.
      "stop"  — opposite-direction STOP at SL ∓ buffer (CONTINUATION bet).
                For parent BUY: SELL STOP BELOW SL, fires when bid <= entry.
    """
    ts_arr = ticks_arr["ts_ns"]; bid = ticks_arr["bid"]; ask = ticks_arr["ask"]
    expire_ns = int(hcfg.exp * 60 * 1_000_000_000)
    walk_max_ns = int(2 * 24 * 3600 * 1_000_000_000)
    hedge_lots = (HEDGE_RISK_PCT / 100.0) * DEPOSIT / hcfg.h_sl
    out = []
    for ev in sl_events:
        sl_price = ev["sl_price"]; direction = ev["direction"]; sl_ts = ev["ts_ns"]
        # Hedge entry placement: LIMIT bets reversal (place at SL + buffer in price-bounce direction);
        # STOP bets continuation (place at SL + buffer in price-continuation direction).
        if direction == 1:           # parent BUY stopped
            hd = -1
            if order_type == "limit":
                entry = sl_price + hcfg.buf * POINT   # ABOVE SL — wait for bounce up
            else:                                       # stop
                entry = sl_price - hcfg.buf * POINT   # BELOW SL — bet continued drop
            h_sl_px = entry + hcfg.h_sl * POINT
            h_tp_px = entry - hcfg.h_sl * hcfg.h_rr * POINT
        else:                         # parent SELL stopped
            hd = 1
            if order_type == "limit":
                entry = sl_price - hcfg.buf * POINT   # BELOW SL — wait for bounce down
            else:                                       # stop
                entry = sl_price + hcfg.buf * POINT   # ABOVE SL — bet continued rise
            h_sl_px = entry - hcfg.h_sl * POINT
            h_tp_px = entry + hcfg.h_sl * hcfg.h_rr * POINT
        i0 = np.searchsorted(ts_arr, sl_ts)
        i1 = np.searchsorted(ts_arr, sl_ts + expire_ns)
        # Trigger: LIMIT fires when price comes TO the limit (mean-reverting touch);
        #          STOP fires when price moves THROUGH the stop (momentum touch).
        if hd == -1:                  # SELL hedge
            if order_type == "limit": # SELL LIMIT — fires when bid rises to entry
                hits = np.where(bid[i0:i1] >= entry)[0]
            else:                     # SELL STOP — fires when bid drops through entry
                hits = np.where(bid[i0:i1] <= entry)[0]
        else:                         # BUY hedge
            if order_type == "limit": # BUY LIMIT — fires when ask drops to entry
                hits = np.where(ask[i0:i1] <= entry)[0]
            else:                     # BUY STOP — fires when ask rises through entry
                hits = np.where(ask[i0:i1] >= entry)[0]
        if len(hits) == 0:
            continue
        ent_idx = i0 + hits[0]
        ent_ts = ts_arr[ent_idx]
        i_end = np.searchsorted(ts_arr, ent_ts + walk_max_ns)
        sl_bid = bid[ent_idx + 1: i_end]; sl_ask = ask[ent_idx + 1: i_end]
        if len(sl_bid) == 0:
            continue
        if hd == -1:
            sl_h = np.where(sl_ask >= h_sl_px)[0]
            tp_h = np.where(sl_bid <= h_tp_px)[0]
        else:
            sl_h = np.where(sl_bid <= h_sl_px)[0]
            tp_h = np.where(sl_ask >= h_tp_px)[0]
        sl_first = sl_h[0] if len(sl_h) else 10**18
        tp_first = tp_h[0] if len(tp_h) else 10**18
        if sl_first == 10**18 and tp_first == 10**18:
            continue
        if sl_first <= tp_first:
            ex_px = h_sl_px; ex_idx = ent_idx + 1 + sl_first
        else:
            ex_px = h_tp_px; ex_idx = ent_idx + 1 + tp_first
        pnl = hd * (ex_px - entry) * CONTRACT * hedge_lots
        out.append((int(ts_arr[ex_idx]), float(pnl)))
    return out


def ts_arr_from_ticks(ticks: pd.DataFrame) -> dict:
    ts_ns = ticks["ts"].dt.tz_convert("UTC").dt.tz_localize(None).astype("datetime64[ns]").astype("int64").to_numpy()
    return {"ts_ns": ts_ns,
            "bid": ticks["bid"].to_numpy(dtype=np.float64),
            "ask": ticks["ask"].to_numpy(dtype=np.float64)}


def wfo_one_stream(stream: str, full_ticks, full_m1, full_m5, meta) -> dict:
    """Run hedge WFO for one stream. Returns dict with rank_with_p0 results."""
    print("\n" + "=" * 110)
    sc = STREAM_CFGS[stream]
    print(f"  HEDGE WFO — STREAM {stream}  (parent fixed: Range={sc['range_minutes']} "
          f"SL={sc['fixed_sl_pts']} RR={sc['rr_ratio']} HTP={sc['half_tp_ratio']})")
    print(f"  Grid: buf={BUFFERS} h_sl={HEDGE_SLS} h_rr={HEDGE_RRS} exp={EXPIRES}  "
          f"= {len(BUFFERS)*len(HEDGE_SLS)*len(HEDGE_RRS)*len(EXPIRES)} cells × 4 windows × IS+OOS")
    print("=" * 110)
    grid = [HedgeCfg(b, hs, hr, e) for b in BUFFERS for hs in HEDGE_SLS
            for hr in HEDGE_RRS for e in EXPIRES]
    n_cells = len(grid)

    # Pre-slice per window for IS and OOS
    is_per: dict[str, pd.DataFrame] = {}
    oos_per: dict[str, pd.DataFrame] = {}
    t_start = time.time()
    for label, is_s, is_e, oos_s, oos_e in WINDOWS:
        for tag, (s, e), bucket in [("IS", (to_utc(is_s), to_utc(is_e)), is_per),
                                     ("OOS", (to_utc(oos_s), to_utc(oos_e)), oos_per)]:
            ticks = slice_window(full_ticks, "ts", s, e)
            m1 = slice_window(full_m1, "ts", s, e)
            m5 = slice_window(full_m5, "ts", s, e)
            base_deals, sl_ev = run_baseline_window(stream, ticks, m1, m5, meta, PARENT_RISK_SWEEP)
            base_np, base_dd, base_pf = aggregate(base_deals)
            t_arr = ts_arr_from_ticks(ticks)
            rows = []
            for hc in grid:
                h_deals = simulate_hedges(sl_ev, t_arr, hc)
                merged = base_deals + h_deals
                np_, dd, pf = aggregate(merged)
                rows.append({"buf": hc.buf, "h_sl": hc.h_sl, "h_rr": hc.h_rr, "exp": hc.exp,
                             "net_profit": np_, "drawdown_pct": dd, "profit_factor": pf,
                             "n_sl": len(sl_ev), "h_n": len(h_deals)})
            df = pd.DataFrame(rows)
            bucket[label] = df
            print(f"  {stream} {label} {tag} {s.date()}->{e.date()}  base NP=${base_np:+,.0f} DD={base_dd:.1f}% "
                  f"SL_evts={len(sl_ev):>3}  hedge cells={n_cells}  [{time.time()-t_start:.1f}s]")

    # Rank with P0 + plateau
    ranked = rank_with_p0(grid, oos_per, WINDOWS, decay_threshold=-0.25,
                          grid_configs=grid, is_per_window=is_per)
    print_phase_d_with_p0(ranked[:10], f"hedge {stream}", decay_threshold=-0.25)
    winner = select_winner_with_p0(ranked) or ranked[0]
    flagged = check_winner_boundaries(winner["cfg"], grid)
    print_boundary_check(flagged)
    print(f"\n  WINNER for {stream}: buf={winner['cfg'].buf} h_sl={winner['cfg'].h_sl} "
          f"h_rr={winner['cfg'].h_rr} exp={winner['cfg'].exp}")
    # Persist per-stream winner JSON for extract_top_n.py --hedge-per-stream-dir
    import json as _json
    out_dir = ROOT / "output" / "wfo_hedge_per_stream_may9"
    out_dir.mkdir(parents=True, exist_ok=True)
    wj = {"buffer_pts": winner["cfg"].buf, "fixed_sl_pts": winner["cfg"].h_sl,
          "rr_ratio": winner["cfg"].h_rr, "expire_minutes": winner["cfg"].exp}
    (out_dir / f"{stream}.json").write_text(_json.dumps(wj, indent=2))
    print(f"  Persisted winner: {out_dir / f'{stream}.json'}")
    return {"ranked": ranked, "winner": winner, "is_per": is_per, "oos_per": oos_per}


def portfolio_compare(winners: dict, full_ticks, full_m1, full_m5, meta):
    """Compare 3-stream portfolio with vs without hedge on Feb 14 -> May 1."""
    start = to_utc(WINDOWS[0][1])  # Feb 14
    end = to_utc(WINDOWS[-1][4])    # May 1
    print("\n" + "=" * 110)
    print(f"  PORTFOLIO COMPARISON  {start.date()} -> {end.date()} ({(end-start).days}d, $10k)")
    print(f"  Each stream parent at {PARENT_RISK_PROD}% (production sizing), each hedge at {HEDGE_RISK_PCT}%")
    print("=" * 110)
    ticks = slice_window(full_ticks, "ts", start, end)
    m1 = slice_window(full_m1, "ts", start, end)
    m5 = slice_window(full_m5, "ts", start, end)
    t_arr = ts_arr_from_ticks(ticks)

    # WITHOUT hedge: just baseline parents at production sizing
    all_base = []; per_s_base = {}
    for s in STREAM_CFGS:
        deals, _ = run_baseline_window(s, ticks, m1, m5, meta, PARENT_RISK_PROD)
        all_base.extend(deals)
        per_s_base[s] = sum(p for _, p in deals), len(deals)
    np_b, dd_b, pf_b = aggregate(all_base)
    ndd_b = (np_b / (dd_b/100 * (DEPOSIT + np_b))) if dd_b > 0 else 0

    # WITH hedge: baseline + per-stream winning hedge
    all_w = []; per_s_w = {}; per_h = {}
    for s in STREAM_CFGS:
        deals, sl_ev = run_baseline_window(s, ticks, m1, m5, meta, PARENT_RISK_PROD)
        h_deals = simulate_hedges(sl_ev, t_arr, winners[s]["winner"]["cfg"])
        all_w.extend(deals + h_deals)
        per_s_w[s] = sum(p for _, p in deals), len(deals)
        per_h[s] = (sum(p for _, p in h_deals), len(h_deals),
                    sum(1 for _, p in h_deals if p > 0))
    np_w, dd_w, pf_w = aggregate(all_w)
    ndd_w = (np_w / (dd_w/100 * (DEPOSIT + np_w))) if dd_w > 0 else 0

    print(f"\n  {'Variant':<14} {'NP':>10} {'DD%':>6} {'NP/DD$':>7} {'PF':>5} {'Trades':>7}")
    print(f"  {'no-hedge':<14} ${np_b:>+8,.0f} {dd_b:>5.2f}% {ndd_b:>7.2f} {pf_b:>5.2f} {len(all_base):>7}")
    print(f"  {'+hedge':<14} ${np_w:>+8,.0f} {dd_w:>5.2f}% {ndd_w:>7.2f} {pf_w:>5.2f} {len(all_w):>7}")
    d_np = np_w - np_b; d_dd = dd_w - dd_b; d_ndd = ndd_w - ndd_b
    print(f"  {'delta':<14} ${d_np:>+8,.0f} {d_dd:>+5.1f}p {d_ndd:>+7.2f}")

    print(f"\n  Per-stream contribution (with hedge):")
    print(f"  {'Stream':<6}  parent_NP   parent_n   hedge_NP   hedge_n  hedge_W")
    for s in STREAM_CFGS:
        pp, pn = per_s_w[s]; hp, hn, hw = per_h[s]
        wr = (hw / hn * 100) if hn else 0
        print(f"  {s:<6}  ${pp:>+7,.0f}    {pn:>4}     ${hp:>+7,.0f}    {hn:>4}    {wr:>4.0f}%")


def main() -> int:
    print("=" * 110)
    print(f"  PER-STREAM HEDGE WFO  |  WINDOWS_MAY2 (Feb 14 -> May 1, 4 windows)")
    print(f"  Spread {SPREAD}pt, $10k, parent (sweep) at {PARENT_RISK_SWEEP}%, hedge at {HEDGE_RISK_PCT}%")
    print(f"  Total cells per stream: {len(BUFFERS)*len(HEDGE_SLS)*len(HEDGE_RRS)*len(EXPIRES)}")
    print("=" * 110)
    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        # Load full window once (cache covers it)
        start = to_utc(WINDOWS[0][1])
        end = to_utc(WINDOWS[-1][4])
        full_m1 = load_bars(SYMBOL, "M1", start, end)
        full_m5 = load_bars(SYMBOL, "M5", start, end)
        full_ticks = load_ticks(SYMBOL, start, end, spread_pts=SPREAD)
        print(f"\n  Full data {start.date()}->{end.date()}: ticks={len(full_ticks):,} "
              f"M1={len(full_m1):,} M5={len(full_m5):,}")

        winners = {}
        for stream in ("S1", "S2", "S3"):
            winners[stream] = wfo_one_stream(stream, full_ticks, full_m1, full_m5, meta)

        portfolio_compare(winners, full_ticks, full_m1, full_m5, meta)
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
