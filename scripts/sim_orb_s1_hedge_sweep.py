"""S1-only hedge sweep — capture false-breakout reversals after SL.

Concept: when S1's BUY breakout hits SL, often it's a false breakout that
reverses through the original range. Place a SELL LIMIT at SL_price+buffer
that arms only after the SL fires. Mirror for SELLs.

Approach: post-process baseline S1-only sim deals — for each SL-hit, look
forward in tick data to see if hedge would have triggered + how it'd resolve.
No simulator change. If any cell shows clear edge, promote to WFO.

Sweep v2: buffer × hedge_sl × hedge_rr × expire_min (5*3*3*2 = 90 cells)
- Wider buffer range (0/50/100/200/350) — earlier v1 capped at 50.
- Hedge sized by HEDGE_RISK_PCT (3% of deposit), NOT by parent lot.
  v1 inherited parent lots → h_sl=800 cells were running at 4.8% real risk
  vs h_sl=250 cells at 1.5% — apples-to-oranges. Now all hedges = 3% risk.
Window: Feb 14 -> May 1 (76d), $10k, 23pt spread, S1 at 3% risk.
"""
from __future__ import annotations

import sys
import time
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

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
START = datetime(2026, 2, 14, tzinfo=timezone.utc)
END = datetime(2026, 5, 1, tzinfo=timezone.utc)
SPREAD = 30  # per feedback_default_test_conditions.md (all live = 30pt 2026-05-16)
POINT = 0.01
CONTRACT = 100  # $/lot/pt for XAUUSD

# Production configs from May 2 WFO ranks 1/2/3
STREAM_CFGS = {
    "S1": dict(range_minutes=90, fixed_sl_pts=500, rr_ratio=4.0, half_tp_ratio=0.25,
               daily_target_pct=0.0, daily_loss_pct=0.0),
    "S2": dict(range_minutes=90, fixed_sl_pts=400, rr_ratio=4.0, half_tp_ratio=0.0,
               daily_target_pct=0.0, daily_loss_pct=0.0),
    "S3": dict(range_minutes=90, fixed_sl_pts=350, rr_ratio=4.0, half_tp_ratio=0.5,
               daily_target_pct=0.0, daily_loss_pct=0.0),
}

# Sweep grid (v2: risk-normalized hedge sizing + wider buffers)
BUFFERS = [0, 50, 100, 200, 350]    # pts past SL where hedge limit sits
HEDGE_SLS = [250, 500, 800]         # hedge stop loss in pts
HEDGE_RRS = [1.0, 2.0, 3.0]         # hedge take-profit RR
EXPIRES = [30, 120]                 # hedge order valid window (minutes after parent SL)
HEDGE_RISK_PCT = 1.0                # size hedge at this %/deposit (mirrors per-stream allocation in 3-stream production setfile)


def make_stream_cfg(stream: str, risk_pct: float) -> ORBConfig:
    sc = STREAM_CFGS[stream]
    return ORBConfig(
        risk_pct=risk_pct,
        range_minutes=sc["range_minutes"],
        buffer_pts=0,
        min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=sc["fixed_sl_pts"],
        rr_ratio=sc["rr_ratio"],
        half_tp_ratio=sc["half_tp_ratio"],
        pending_expire_minutes=240,
        daily_target_pct=sc["daily_target_pct"],
        daily_loss_pct=sc["daily_loss_pct"],
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True,  ny_start_hour=13,
        comment=stream,
    )


def aggregate_balance_curve(deals):
    """Return (np, dd_pct, dd_abs, pf, n_trades). Walks deals in time order."""
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gp = gl = 0.0
    for _, p in sorted(deals, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p > 0: gp += p
        elif p < 0: gl += p
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    pf = (gp / abs(gl)) if gl < 0 else float("inf")
    return np_, dd_pct, pf, len(deals)


def run_baseline(stream: str, ticks, m1, m5, meta):
    """Run stream-only sim, return (all_deals_pnl_pairs, sl_events)."""
    cfg = make_stream_cfg(stream, 3.0)
    r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
    base_deals = []   # list of (ts_ns, pnl) for non-entry deals
    sl_events = []    # list of dicts: {ts, direction, sl_price, lots}
    for d in r.deals:
        if d.kind == "entry":
            continue
        ts_ns = pd.Timestamp(d.ts).value
        base_deals.append((ts_ns, d.pnl))
        if d.kind == "sl":
            sl_events.append({
                "ts_ns": ts_ns,
                "direction": int(d.direction),  # +1 BUY, -1 SELL
                "sl_price": float(d.price),
                "lots": float(d.lots),
            })
    return base_deals, sl_events


def simulate_hedges(sl_events, ticks_arr, buffer_pts, hedge_sl_pts, hedge_rr,
                    expire_min):
    """For each SL event, simulate the hedge trade. Return list of (ts_ns, pnl)."""
    # ticks_arr columns: ts_ns, bid, ask
    ts_arr = ticks_arr["ts_ns"]
    bid_arr = ticks_arr["bid"]
    ask_arr = ticks_arr["ask"]
    n = len(ts_arr)

    expire_ns = int(expire_min * 60 * 1_000_000_000)
    walk_max_ns = int(2 * 24 * 3600 * 1_000_000_000)  # 2-day walk cap for hedge resolution

    # Risk-normalize hedge: size by configured risk%, NOT parent lot.
    # hedge_lots × hedge_sl_pts × $1 = HEDGE_RISK_PCT% × DEPOSIT
    hedge_lots_const = (HEDGE_RISK_PCT / 100.0) * DEPOSIT / hedge_sl_pts

    hedge_deals = []
    for ev in sl_events:
        sl_price = ev["sl_price"]
        direction = ev["direction"]
        lots = hedge_lots_const  # risk-normalized, parent lot ignored
        sl_ts = ev["ts_ns"]

        # Hedge is opposite direction
        if direction == 1:  # original BUY -> hedge SELL
            entry_trigger = sl_price + buffer_pts * POINT
            hedge_dir = -1
            hedge_sl = entry_trigger + hedge_sl_pts * POINT
            hedge_tp = entry_trigger - hedge_sl_pts * hedge_rr * POINT
        else:               # original SELL -> hedge BUY
            entry_trigger = sl_price - buffer_pts * POINT
            hedge_dir = 1
            hedge_sl = entry_trigger - hedge_sl_pts * POINT
            hedge_tp = entry_trigger + hedge_sl_pts * hedge_rr * POINT

        # Find tick range to scan: [sl_ts, sl_ts + expire_ns]
        i0 = np.searchsorted(ts_arr, sl_ts)
        i1_expire = np.searchsorted(ts_arr, sl_ts + expire_ns)

        # Find entry trigger
        if hedge_dir == -1:  # SELL LIMIT fires when bid >= entry_trigger
            window_bid = bid_arr[i0:i1_expire]
            hits = np.where(window_bid >= entry_trigger)[0]
        else:                # BUY LIMIT fires when ask <= entry_trigger
            window_ask = ask_arr[i0:i1_expire]
            hits = np.where(window_ask <= entry_trigger)[0]
        if len(hits) == 0:
            continue  # hedge never armed within expire window
        entry_idx = i0 + hits[0]
        entry_ts = ts_arr[entry_idx]

        # Walk forward from entry to find SL or TP
        i_walk_end = np.searchsorted(ts_arr, entry_ts + walk_max_ns)
        sl_bid = bid_arr[entry_idx + 1 : i_walk_end]
        sl_ask = ask_arr[entry_idx + 1 : i_walk_end]
        if len(sl_bid) == 0:
            continue
        if hedge_dir == -1:  # SELL: SL when ask >= hedge_sl, TP when bid <= hedge_tp
            sl_hits = np.where(sl_ask >= hedge_sl)[0]
            tp_hits = np.where(sl_bid <= hedge_tp)[0]
        else:                # BUY: SL when bid <= hedge_sl, TP when ask >= hedge_tp
            sl_hits = np.where(sl_bid <= hedge_sl)[0]
            tp_hits = np.where(sl_ask >= hedge_tp)[0]
        sl_first = sl_hits[0] if len(sl_hits) else 10**18
        tp_first = tp_hits[0] if len(tp_hits) else 10**18
        if sl_first == 10**18 and tp_first == 10**18:
            # neither hit within walk_max — flat at end of walk; conservatively treat as $0
            continue
        if sl_first <= tp_first:
            exit_price = hedge_sl
            exit_idx = entry_idx + 1 + sl_first
        else:
            exit_price = hedge_tp
            exit_idx = entry_idx + 1 + tp_first
        exit_ts = ts_arr[exit_idx]

        # PnL: hedge_dir × (exit - entry_trigger) × CONTRACT × lots
        pnl = hedge_dir * (exit_price - entry_trigger) * CONTRACT * lots
        hedge_deals.append((int(exit_ts), float(pnl)))
    return hedge_deals


def sweep_stream(stream: str, ticks_arr, ticks, m1, m5, meta):
    """Run baseline + 90-cell hedge sweep for one stream. Returns (baseline_dict, sorted_cells)."""
    print("\n" + "=" * 110)
    sc = STREAM_CFGS[stream]
    print(f"  STREAM {stream}  |  Range={sc['range_minutes']} SL={sc['fixed_sl_pts']} "
          f"RR={sc['rr_ratio']} HTP={sc['half_tp_ratio']}")
    print("=" * 110)
    t0 = time.time()
    base_deals, sl_events = run_baseline(stream, ticks, m1, m5, meta)
    base_np, base_dd, base_pf, base_n = aggregate_balance_curve(base_deals)
    base_dd_abs = base_dd / 100 * (DEPOSIT + base_np)
    base_ndd = (base_np / base_dd_abs) if base_dd_abs > 0 else 0
    n_sl = len(sl_events)
    print(f"  Baseline {stream}: NP=${base_np:+,.0f} DD={base_dd:.2f}% PF={base_pf:.2f} "
          f"NP/DD$={base_ndd:.2f} trades={base_n} (SL events={n_sl})  [{time.time()-t0:.1f}s]")
    if n_sl == 0:
        print(f"  No SL events — skipping hedge sweep.")
        return dict(np=base_np, dd=base_dd, ndd=base_ndd, pf=base_pf, n=base_n), []

    print(f"\n  {'buf':>4} {'h_sl':>5} {'h_rr':>5} {'exp':>4}  "
          f"{'NP':>10} {'dDD':>6} {'PF':>5}  {'h_NP':>9} {'h_n':>4} {'h_WR':>5}  "
          f"{'NP/DD$':>7} {'dNP/DD$':>9}")
    cells = []
    t_sweep = time.time()
    for buf in BUFFERS:
        for h_sl in HEDGE_SLS:
            for h_rr in HEDGE_RRS:
                for exp in EXPIRES:
                    h_deals = simulate_hedges(sl_events, ticks_arr, buf, h_sl, h_rr, exp)
                    merged = base_deals + h_deals
                    np_, dd, pf, n = aggregate_balance_curve(merged)
                    h_np = sum(p for _, p in h_deals)
                    h_n = len(h_deals)
                    h_wins = sum(1 for _, p in h_deals if p > 0)
                    h_wr = (h_wins / h_n * 100) if h_n else 0
                    ndd = (np_ / (dd / 100 * (DEPOSIT + np_))) if dd > 0 else 0
                    d_ndd = ndd - base_ndd
                    d_dd = dd - base_dd
                    cells.append({"buf": buf, "h_sl": h_sl, "h_rr": h_rr, "exp": exp,
                                  "np": np_, "dd": dd, "pf": pf, "n": n,
                                  "h_np": h_np, "h_n": h_n, "h_wr": h_wr,
                                  "ndd": ndd, "d_ndd": d_ndd})
                    print(f"  {buf:>4} {h_sl:>5} {h_rr:>5.1f} {exp:>4}  "
                          f"${np_:>+8,.0f} {d_dd:>+5.1f}p {pf:>5.2f}  "
                          f"${h_np:>+7,.0f} {h_n:>4} {h_wr:>4.0f}%  "
                          f"{ndd:>7.2f} {d_ndd:>+8.2f}")
    cells.sort(key=lambda c: c["ndd"], reverse=True)
    print(f"\n  Sweep done in {time.time()-t_sweep:.1f}s")
    print(f"\n  Top 5 by NP/DD$ (vs baseline {base_ndd:.2f}):")
    print(f"  {'rank':>4}  {'buf':>4} {'h_sl':>5} {'h_rr':>5} {'exp':>4}  "
          f"{'NP':>10} {'DD%':>6} {'NP/DD$':>7} {'dNP/DD$':>9}  hedge: {'NP':>9} {'n':>3} {'WR':>4}")
    for i, c in enumerate(cells[:5], 1):
        print(f"  #{i:<3}  {c['buf']:>4} {c['h_sl']:>5} {c['h_rr']:>5.1f} {c['exp']:>4}  "
              f"${c['np']:>+8,.0f} {c['dd']:>5.2f}% {c['ndd']:>7.2f} {c['d_ndd']:>+8.2f}  "
              f"${c['h_np']:>+7,.0f} {c['h_n']:>3} {c['h_wr']:>3.0f}%")
    return dict(np=base_np, dd=base_dd, ndd=base_ndd, pf=base_pf, n=base_n), cells


def main() -> int:
    days = (END - START).days
    print("=" * 110)
    print(f"  HEDGE SWEEP — S1+S2+S3  |  {START.date()} -> {END.date()} ({days}d, $10k, {SPREAD}pt)")
    print(f"  Sweep grid: buffers={BUFFERS} hedge_sl={HEDGE_SLS} hedge_rr={HEDGE_RRS} expire={EXPIRES}min")
    print(f"  Hedge risk: {HEDGE_RISK_PCT}% / deposit (mirrors per-stream allocation in 3-stream production)")
    print(f"  Cells per stream: {len(BUFFERS)*len(HEDGE_SLS)*len(HEDGE_RRS)*len(EXPIRES)}  |  Total: x3 streams")
    print("=" * 110)

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        m1 = load_bars(SYMBOL, "M1", START, END)
        m5 = load_bars(SYMBOL, "M5", START, END)
        ticks = load_ticks(SYMBOL, START, END, spread_pts=SPREAD)
        print(f"\n  Ticks: {len(ticks):,}  M1: {len(m1):,}  M5: {len(m5):,}")

        ts_ns = ticks["ts"].dt.tz_convert("UTC").dt.tz_localize(None).astype("datetime64[ns]").astype("int64").to_numpy()
        ticks_arr = {
            "ts_ns": ts_ns,
            "bid": ticks["bid"].to_numpy(dtype=np.float64),
            "ask": ticks["ask"].to_numpy(dtype=np.float64),
        }

        results = {}
        for stream in ("S1", "S2", "S3"):
            base, cells = sweep_stream(stream, ticks_arr, ticks, m1, m5, meta)
            results[stream] = (base, cells)

        # Cross-stream summary
        print("\n" + "=" * 110)
        print("  CROSS-STREAM SUMMARY  (best hedge cell vs baseline per stream)")
        print("=" * 110)
        print(f"  {'Stream':<6} {'Base NP':>9} {'Base NP/DD$':>12}  "
              f"{'Best NP':>9} {'Best NP/DD$':>12} {'dNP/DD$':>9}  "
              f"{'buf':>4} {'h_sl':>5} {'h_rr':>5} {'exp':>4}  {'h_n':>4} {'h_WR':>4}  {'h_NP':>8}")
        for stream, (base, cells) in results.items():
            if not cells:
                print(f"  {stream:<6}  (no hedge events)")
                continue
            top = cells[0]
            print(f"  {stream:<6} ${base['np']:>+7,.0f} {base['ndd']:>12.2f}  "
                  f"${top['np']:>+7,.0f} {top['ndd']:>12.2f} {top['d_ndd']:>+8.2f}  "
                  f"{top['buf']:>4} {top['h_sl']:>5} {top['h_rr']:>5.1f} {top['exp']:>4}  "
                  f"{top['h_n']:>4} {top['h_wr']:>3.0f}%  ${top['h_np']:>+6,.0f}")

        # Robustness check: are top cells in a consistent region?
        print(f"\n  Per-stream top-3 hedge configs (looking for shared (buf,h_sl,h_rr,exp) regions):")
        for stream, (base, cells) in results.items():
            if not cells: continue
            tops = [(c['buf'], c['h_sl'], c['h_rr'], c['exp']) for c in cells[:3]]
            print(f"  {stream}: {tops}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
