"""Per-stream RETRY-hedge WFO — sim for DT818_pro_v2.1_h.mq5.

Hedge logic: when parent SL hits, place a NEW pending of the SAME type at
the parent's ORIGINAL entry price, with the SAME SL, TP, and lots.

Compared to sim_wfo_hedge.py (which sims opposite-direction limit/stop hedge),
retry-hedge:
  - Direction = SAME as parent (BUY_STOP retry for BUY parent, etc.)
  - Entry = parent's original entry (= sl_price ± SL_dist)
  - SL = parent's original SL (= where parent SL'd)
  - TP = parent's original TP (entry ± SL_dist * RR)
  - Lots = parent's lots

Tunables (small grid since most params are inherited from parent):
  - expire_minutes:    how long the retry pending stays armed after parent SL
  - f1_max_seconds:    skip retry if parent SL came > this many s after entry
                       (0 = no filter)
  - buffer_pts:        small offset on retry entry (default 0 = exact same as parent)

Output: per-stream winner JSON + portfolio compare (parent only vs parent+retry).

Run via:  python scripts/sim_wfo_hedge_retry.py
"""
from __future__ import annotations

import sys
import time
import json
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
DEPOSIT = 10_000.0   # standard sim baseline; one-off $149k forecast at 23:17 archived in run_round6.log
SPREAD = 30   # 2026-05-16: bumped 23->30 per user "all live = 30pt moving forward"
POINT = 0.01
CONTRACT = 100

# v2.1 parent params — copy from production setfile dt818_pro_v2.1_9pct_may16_may9.set.
# Rotation 2026-05-16: S1-3 = MAY9 winners (prev week), S4-6 = MAY16 winners (curr week).
# LDN=4 in setfile (real UTC); tick parquets are broker-time-labeled so use 7 here.
STREAM_CFGS = {
    "S1": dict(range_minutes=90, fixed_sl_pts=700, rr_ratio=2.5, half_tp_ratio=0.4),   # MAY9 R1
    "S2": dict(range_minutes=90, fixed_sl_pts=550, rr_ratio=3.5, half_tp_ratio=0.2),   # MAY9 R2
    "S3": dict(range_minutes=90, fixed_sl_pts=400, rr_ratio=4.0, half_tp_ratio=0.6),   # MAY9 R3
    "S4": dict(range_minutes=90, fixed_sl_pts=550, rr_ratio=4.0, half_tp_ratio=0.4),   # MAY16 R2 (R1 dedup-skipped)
    "S5": dict(range_minutes=90, fixed_sl_pts=550, rr_ratio=3.5, half_tp_ratio=0.4),   # MAY16 R3
    "S6": dict(range_minutes=90, fixed_sl_pts=550, rr_ratio=3.0, half_tp_ratio=0.4),   # MAY16 R4 (R5 dedup-skipped)
}

# Retry hedge grid (v2: A + B per user request 2026-05-12)
# A: drop buffer dim (exact-same-price per spec), extend expire grid
# B: add tp_mult dim (multiplier on parent's RR for retry's TP target —
#    shorter TP = higher hit rate but lower payoff; 1.0 = exact parent params)
EXPIRES_MIN = [60, 120, 240, 480, 720, 1440, 2880, 4320]   # 8 (extended further)
F1_CUTOFFS_SEC = [0, 1800, 3600, 7200, 14400]              # 5 (extended max to 4h)
BUFFERS_PTS = [0]                                           # 1 (exact-same-price spec)
TP_MULTS = [0.1, 0.25, 0.5, 0.75, 1.0, 1.5]                # 6 (finer + extended)
# Total cells per stream: 8 * 5 * 1 * 6 = 240

PARENT_RISK_SWEEP = 3.0   # used for ranking (invariant)
PARENT_RISK_PROD  = 1.5   # used for portfolio compare (matches v2.1_h 9pct setfile)


@dataclass(frozen=True)
class RetryHedgeCfg:
    exp_min: int         # expire_minutes for retry pending
    f1_sec: int          # F1 filter cutoff (0 = disabled)
    buf_pts: int         # entry offset from parent's original entry (usually 0)
    tp_mult: float       # multiplier on parent's RR for retry's TP (1.0 = exact parent TP)


def make_stream_cfg(stream: str, risk_pct: float) -> ORBConfig:
    sc = STREAM_CFGS[stream]
    return ORBConfig(
        risk_pct=risk_pct,
        range_minutes=sc["range_minutes"],
        buffer_pts=0, min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=sc["fixed_sl_pts"],
        rr_ratio=sc["rr_ratio"],
        half_tp_ratio=sc["half_tp_ratio"],
        pending_expire_minutes=240,
        daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True, ny_start_hour=13,
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
    """Run parent sim. Returns (deals_pnl_pairs, sl_events_with_context).

    sl_events_with_context fields per entry:
      ts_ns:        SL exit timestamp (ns since epoch)
      entry_ts_ns:  entry timestamp (for F1 filter)
      direction:    +1 for parent BUY, -1 for parent SELL
      sl_price:     where parent SL'd (= parent's original SL level)
      lots:         parent position lots
    """
    cfg = make_stream_cfg(stream, risk_pct)
    r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
    deals = []
    # Pair entry deals with subsequent SL deals (same direction, next exit)
    open_positions = []   # stack of {entry_ts_ns, direction, lots, entry_price}
    sl_events = []
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
        deals.append((ts_ns, d.pnl))
        # Find matching open position (same direction, FIFO)
        match_idx = -1
        for i, op in enumerate(open_positions):
            if op["direction"] == int(d.direction):
                match_idx = i
                break
        if match_idx >= 0:
            op = open_positions.pop(match_idx)
            if d.kind == "sl":
                sl_events.append({
                    "ts_ns": ts_ns,
                    "entry_ts_ns": op["entry_ts_ns"],
                    "direction": op["direction"],
                    "sl_price": float(d.price),
                    "entry_price": op["entry_price"],
                    "lots": op["lots"],
                })
    return deals, sl_events


def ts_arr_from_ticks(ticks: pd.DataFrame) -> dict:
    ts_ns = ticks["ts"].dt.tz_convert("UTC").dt.tz_localize(None).astype("datetime64[ns]").astype("int64").to_numpy()
    return {"ts_ns": ts_ns,
            "bid": ticks["bid"].to_numpy(dtype=np.float64),
            "ask": ticks["ask"].to_numpy(dtype=np.float64)}


def simulate_retry_hedges(sl_events, ticks_arr, stream_cfg: dict, hcfg: RetryHedgeCfg) -> list:
    """Post-process retry-hedge for one window. Returns list of (exit_ts_ns, pnl).

    For each parent SL event, places a NEW pending of the SAME type at the
    parent's original entry price with SAME SL, TP, lots.
    """
    ts_arr = ticks_arr["ts_ns"]; bid = ticks_arr["bid"]; ask = ticks_arr["ask"]
    expire_ns = int(hcfg.exp_min * 60 * 1_000_000_000)
    walk_max_ns = int(3 * 24 * 3600 * 1_000_000_000)
    sl_dist_price = stream_cfg["fixed_sl_pts"] * POINT
    tp_dist_price = sl_dist_price * stream_cfg["rr_ratio"] * hcfg.tp_mult
    out = []
    for ev in sl_events:
        sl_ts = ev["ts_ns"]
        direction = ev["direction"]
        sl_price = ev["sl_price"]
        entry_price = ev["entry_price"]   # parent's original entry — same as retry entry
        lots = ev["lots"]

        # F1 filter: skip retry if parent SL came too late
        if hcfg.f1_sec > 0:
            elapsed_s = (sl_ts - ev["entry_ts_ns"]) / 1_000_000_000
            if elapsed_s > hcfg.f1_sec:
                continue

        # Retry pending: same direction, same entry (with optional buffer offset)
        if direction == 1:    # parent BUY → retry BUY_STOP at entry
            retry_entry = entry_price + hcfg.buf_pts * POINT
            retry_sl = retry_entry - sl_dist_price
            retry_tp = retry_entry + tp_dist_price
        else:                  # parent SELL → retry SELL_STOP at entry
            retry_entry = entry_price - hcfg.buf_pts * POINT
            retry_sl = retry_entry + sl_dist_price
            retry_tp = retry_entry - tp_dist_price

        # Pending lifecycle: from sl_ts to sl_ts + expire_ns, wait for entry trigger
        i0 = np.searchsorted(ts_arr, sl_ts)
        i1 = np.searchsorted(ts_arr, sl_ts + expire_ns)
        if i1 <= i0:
            continue

        # Trigger:
        #   BUY_STOP fires when ask rises to retry_entry
        #   SELL_STOP fires when bid drops to retry_entry
        if direction == 1:  # BUY retry
            hits = np.where(ask[i0:i1] >= retry_entry)[0]
        else:                # SELL retry
            hits = np.where(bid[i0:i1] <= retry_entry)[0]
        if len(hits) == 0:
            continue
        ent_idx = i0 + hits[0]
        ent_ts = ts_arr[ent_idx]
        i_end = np.searchsorted(ts_arr, ent_ts + walk_max_ns)
        post_bid = bid[ent_idx + 1: i_end]
        post_ask = ask[ent_idx + 1: i_end]
        if len(post_bid) == 0:
            continue

        # Position lifecycle:
        #   BUY position: SL when bid <= retry_sl; TP when bid >= retry_tp
        #   SELL position: SL when ask >= retry_sl; TP when ask <= retry_tp
        if direction == 1:
            sl_h = np.where(post_bid <= retry_sl)[0]
            tp_h = np.where(post_bid >= retry_tp)[0]
        else:
            sl_h = np.where(post_ask >= retry_sl)[0]
            tp_h = np.where(post_ask <= retry_tp)[0]
        sl_first = sl_h[0] if len(sl_h) else 10**18
        tp_first = tp_h[0] if len(tp_h) else 10**18
        if sl_first == 10**18 and tp_first == 10**18:
            continue
        if sl_first <= tp_first:
            ex_px = retry_sl; ex_idx = ent_idx + 1 + sl_first
        else:
            ex_px = retry_tp; ex_idx = ent_idx + 1 + tp_first
        pnl = direction * (ex_px - retry_entry) * CONTRACT * lots
        out.append((int(ts_arr[ex_idx]), float(pnl)))
    return out


def wfo_one_stream(stream: str, full_ticks, full_m1, full_m5, meta) -> dict:
    print(f"\n{'=' * 110}")
    print(f"  RETRY-HEDGE WFO  stream={stream}  parent: SL={STREAM_CFGS[stream]['fixed_sl_pts']} "
          f"RR={STREAM_CFGS[stream]['rr_ratio']} HTP={STREAM_CFGS[stream]['half_tp_ratio']}")
    print('=' * 110)

    grid = [RetryHedgeCfg(e, f, b, t) for e in EXPIRES_MIN for f in F1_CUTOFFS_SEC
            for b in BUFFERS_PTS for t in TP_MULTS]
    n_cells = len(grid)

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
                h_deals = simulate_retry_hedges(sl_ev, t_arr, STREAM_CFGS[stream], hc)
                merged = base_deals + h_deals
                np_, dd, pf = aggregate(merged)
                rows.append({"exp": hc.exp_min, "f1": hc.f1_sec, "buf": hc.buf_pts,
                             "tp_mult": hc.tp_mult,
                             "net_profit": np_, "drawdown_pct": dd, "profit_factor": pf,
                             "n_sl": len(sl_ev), "h_n": len(h_deals)})
            df = pd.DataFrame(rows)
            bucket[label] = df
            print(f"  {stream} {label} {tag} {s.date()}->{e.date()}  base NP=${base_np:+,.0f} DD={base_dd:.1f}% "
                  f"SL_evts={len(sl_ev):>3}  retry cells={n_cells}  [{time.time()-t_start:.1f}s]")

    ranked = rank_with_p0(grid, oos_per, WINDOWS, decay_threshold=-0.25,
                          grid_configs=grid, is_per_window=is_per)
    print_phase_d_with_p0(ranked[:10], f"retry-hedge {stream}", decay_threshold=-0.25)
    winner = select_winner_with_p0(ranked) or ranked[0]
    flagged = check_winner_boundaries(winner["cfg"], grid)
    print_boundary_check(flagged)
    print(f"\n  WINNER for {stream}: exp={winner['cfg'].exp_min}min "
          f"f1={winner['cfg'].f1_sec}s buf={winner['cfg'].buf_pts}pt "
          f"tp_mult={winner['cfg'].tp_mult}")

    out_dir = ROOT / "output" / "wfo_hedge_retry_may9"
    out_dir.mkdir(parents=True, exist_ok=True)
    wj = {"expire_minutes": winner["cfg"].exp_min,
          "max_seconds_after_entry": winner["cfg"].f1_sec,
          "buffer_pts": winner["cfg"].buf_pts,
          "tp_mult": winner["cfg"].tp_mult}
    (out_dir / f"{stream}.json").write_text(json.dumps(wj, indent=2))
    print(f"  Persisted winner: {out_dir / f'{stream}.json'}")
    return {"ranked": ranked, "winner": winner, "is_per": is_per, "oos_per": oos_per}


def portfolio_compare(winners: dict, full_ticks, full_m1, full_m5, meta):
    start = to_utc(WINDOWS[0][1])
    end = to_utc(WINDOWS[-1][4])
    print("\n" + "=" * 110)
    print(f"  PORTFOLIO COMPARISON  {start.date()} -> {end.date()} ({(end-start).days}d, $10k)")
    print(f"  Each stream parent at {PARENT_RISK_PROD}% (matches v2.1_h 9pct setfile per-stream)")
    print("=" * 110)
    ticks = slice_window(full_ticks, "ts", start, end)
    m1 = slice_window(full_m1, "ts", start, end)
    m5 = slice_window(full_m5, "ts", start, end)
    t_arr = ts_arr_from_ticks(ticks)

    all_base = []; per_s_base = {}
    for s in STREAM_CFGS:
        deals, _ = run_baseline_window(s, ticks, m1, m5, meta, PARENT_RISK_PROD)
        all_base.extend(deals)
        per_s_base[s] = sum(p for _, p in deals), len(deals)
    np_b, dd_b, pf_b = aggregate(all_base)
    ndd_b = (np_b / (dd_b/100 * (DEPOSIT + np_b))) if dd_b > 0 else 0

    all_w = []; per_s_w = {}; per_h = {}
    for s in STREAM_CFGS:
        deals, sl_ev = run_baseline_window(s, ticks, m1, m5, meta, PARENT_RISK_PROD)
        h_deals = simulate_retry_hedges(sl_ev, t_arr, STREAM_CFGS[s], winners[s]["winner"]["cfg"])
        all_w.extend(deals + h_deals)
        per_s_w[s] = sum(p for _, p in deals), len(deals)
        per_h[s] = (sum(p for _, p in h_deals), len(h_deals),
                    sum(1 for _, p in h_deals if p > 0))
    np_w, dd_w, pf_w = aggregate(all_w)
    ndd_w = (np_w / (dd_w/100 * (DEPOSIT + np_w))) if dd_w > 0 else 0

    print(f"\n  {'Variant':<14} {'NP':>10} {'DD%':>6} {'NP/DD$':>7} {'PF':>5} {'Trades':>7}")
    print(f"  {'no-retry':<14} ${np_b:>+8,.0f} {dd_b:>5.2f}% {ndd_b:>7.2f} {pf_b:>5.2f} {len(all_base):>7}")
    print(f"  {'+retry':<14} ${np_w:>+8,.0f} {dd_w:>5.2f}% {ndd_w:>7.2f} {pf_w:>5.2f} {len(all_w):>7}")
    d_np = np_w - np_b; d_dd = dd_w - dd_b; d_ndd = ndd_w - ndd_b
    print(f"  {'delta':<14} ${d_np:>+8,.0f} {d_dd:>+5.1f}p {d_ndd:>+7.2f}")

    print(f"\n  Per-stream retry contribution:")
    print(f"  {'Stream':<6}  parent_NP   parent_n   retry_NP   retry_n  retry_W")
    for s in STREAM_CFGS:
        pp, pn = per_s_w[s]; hp, hn, hw = per_h[s]
        wr = (hw / hn * 100) if hn else 0
        print(f"  {s:<6}  ${pp:>+7,.0f}    {pn:>4}     ${hp:>+7,.0f}    {hn:>4}    {wr:>4.0f}%")


def main() -> int:
    print("=" * 110)
    print(f"  PER-STREAM RETRY-HEDGE WFO  |  v2.1_h sim")
    print(f"  Windows: WINDOWS_MAY9 (Feb 21 -> May 9, 4 windows)")
    print(f"  Spread {SPREAD}pt, $10k, parent (sweep) at {PARENT_RISK_SWEEP}%")
    n_cells_per = len(EXPIRES_MIN)*len(F1_CUTOFFS_SEC)*len(BUFFERS_PTS)*len(TP_MULTS)
    print(f"  Grid (A+B): exp*f1*buf*tp_mult = {len(EXPIRES_MIN)}*{len(F1_CUTOFFS_SEC)}*"
          f"{len(BUFFERS_PTS)}*{len(TP_MULTS)} = {n_cells_per} cells per stream")
    print("=" * 110)
    try:
        from zgb_sim.mt5_accounts import init_account
        init_account("sim")  # connect to sim account before MT5 fetches
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        start = to_utc(WINDOWS[0][1])
        end = to_utc(WINDOWS[-1][4])
        full_m1 = load_bars(SYMBOL, "M1", start, end)
        full_m5 = load_bars(SYMBOL, "M5", start, end)
        full_ticks = load_ticks(SYMBOL, start, end, spread_pts=SPREAD)
        print(f"\n  Full data {start.date()}->{end.date()}: ticks={len(full_ticks):,} "
              f"M1={len(full_m1):,} M5={len(full_m5):,}")

        winners = {}
        for stream in ("S1", "S2", "S3", "S4", "S5", "S6"):
            winners[stream] = wfo_one_stream(stream, full_ticks, full_m1, full_m5, meta)

        portfolio_compare(winners, full_ticks, full_m1, full_m5, meta)
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
