"""Compare v3 / v4 / v5 / v6 / v7 EAs on the last 2 weeks of live ticks.

For each version: run the parent ORB sim with version-correct flags, then run
the version-correct hedge layer on top. Output side-by-side comparison.

Versions:
  v3: parents (no fractal, no SMA cross-exit) + single-TP reverse-LIMIT hedge
  v4: + smart-TP two-stage hedge (alpha + profit_mult)
  v5: + V2 fractal-confirm (entries gated by M5 fractal)
  v6: same parents as v5, but hedge replaced with STOP-on-extension
  v7: + SMA(8,21) cross-exit on post-HTP runners

Window: last 2 weeks (2026-05-18 -> 2026-05-27, real now).
Account: live (XAUUSD.sc).
Base: $10k deposit, 9% total risk (1.5%/stream × 6).

Run: python scripts/compare_v4_v5_v6_v7_2weeks.py
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

from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast
from zgb_sim.tick_loader import kill_mt5_terminal
from sim_orb_oos_today import fetch_window, fetch_meta
from sim_wfo_hedge_reverse import (ReverseHedgeCfg, simulate_reverse_hedges,
                                    StopExtensionCfg, simulate_stop_extension_hedges,
                                    tag_session_regimes, POINT, CONTRACT)
from sim_orb_oos_today_hedge_v6 import extract_sl_events, ts_arr_from_ticks
from compare_v6_v7_portfolio import (parse_setfile, build_cfg_for_stream,
                                      get_bool, get_float, get_int)

DEPOSIT = 10_000.0
SPREAD = 30

START = datetime(2026, 5, 18, tzinfo=timezone.utc)
END   = datetime.now(timezone.utc)

def simulate_single_tp_reverse_hedge(sl_events, ticks_arr, stream_meta,
                                       tp_mult, sl_mult, exp_min, f1_sec):
    """v3 single-TP reverse-LIMIT hedge (no smart-TP staging).
    Places LIMIT at parent entry; TP at entry ± tp_mult × parent_sl_dist;
    SL at entry ∓ sl_mult × parent_sl_dist. Risk-equalized lots.
    """
    ts_arr = ticks_arr["ts_ns"]; bid = ticks_arr["bid"]; ask = ticks_arr["ask"]
    expire_ns = int(exp_min * 60 * 1_000_000_000)
    walk_max_ns = int(3 * 24 * 3600 * 1_000_000_000)
    parent_sl_dist = stream_meta["fixed_sl_pts"] * POINT
    sl_dist_price = parent_sl_dist * sl_mult
    tp_dist_price = parent_sl_dist * tp_mult
    out = []
    for ev in sl_events:
        sl_ts = ev["ts_ns"]
        direction = ev["direction"]
        entry_price = ev["entry_price"]
        hedge_lots = ev["lots"] / sl_mult  # risk-equalized

        # F1 filter
        if f1_sec > 0:
            elapsed_s = (sl_ts - ev["entry_ts_ns"]) / 1_000_000_000
            if elapsed_s > f1_sec:
                continue

        # Geometry
        if direction == 1:    # parent BUY SL'd → SELL_LIMIT
            hedge_dir = -1
            hedge_entry = entry_price
            hedge_sl = hedge_entry + sl_dist_price
            hedge_tp = hedge_entry - tp_dist_price
        else:                  # parent SELL SL'd → BUY_LIMIT
            hedge_dir = 1
            hedge_entry = entry_price
            hedge_sl = hedge_entry - sl_dist_price
            hedge_tp = hedge_entry + tp_dist_price

        i0 = np.searchsorted(ts_arr, sl_ts)
        i1 = np.searchsorted(ts_arr, sl_ts + expire_ns)
        if i1 <= i0:
            continue
        if hedge_dir == -1:
            hits = np.where(bid[i0:i1] >= hedge_entry)[0]
        else:
            hits = np.where(ask[i0:i1] <= hedge_entry)[0]
        if len(hits) == 0:
            continue
        ent_idx = i0 + hits[0]
        ent_ts = ts_arr[ent_idx]
        i_end = np.searchsorted(ts_arr, ent_ts + walk_max_ns)
        post_bid = bid[ent_idx + 1: i_end]
        post_ask = ask[ent_idx + 1: i_end]
        if len(post_bid) == 0:
            continue
        if hedge_dir == -1:
            sl_h = np.where(post_ask >= hedge_sl)[0]
            tp_h = np.where(post_bid <= hedge_tp)[0]
        else:
            sl_h = np.where(post_bid <= hedge_sl)[0]
            tp_h = np.where(post_ask >= hedge_tp)[0]
        sl_first = sl_h[0] if len(sl_h) else 10**18
        tp_first = tp_h[0] if len(tp_h) else 10**18
        if sl_first == 10**18 and tp_first == 10**18:
            continue
        if tp_first < sl_first:
            ex_px = hedge_tp; ex_idx = ent_idx + 1 + tp_first
        else:
            ex_px = hedge_sl; ex_idx = ent_idx + 1 + sl_first
        pnl = hedge_dir * (ex_px - hedge_entry) * CONTRACT * hedge_lots
        out.append((int(ts_arr[ex_idx]), float(pnl)))
    return out


# Version scenarios: (label, setfile, hedge_arch, parent_overrides)
SCENARIOS = [
    ("v3", "configs/sets/dt818_pro_v3_9pct_may16_may9.set", "v3_singleTP",
        dict(fractal_confirm=False, fractal_width=5, sma_cross_exit=False)),
    ("v4", "configs/sets/dt818_pro_v4_9pct_may23_may16.set", "smartTP",
        dict(fractal_confirm=False, fractal_width=5, sma_cross_exit=False)),
    ("v5", "configs/sets/dt818_pro_v5_9pct_may23_may16.set", "smartTP",
        dict(fractal_confirm=True, fractal_width=5, sma_cross_exit=False)),
    ("v6", "configs/sets/dt818_pro_v6_9pct_may23_may16.set", "STOPext",
        dict(fractal_confirm=True, fractal_width=5, sma_cross_exit=False)),
    ("v7", "configs/sets/dt818_pro_v7_9pct_may30_may23.set", "STOPext",
        dict(fractal_confirm=True, fractal_width=5,
             sma_cross_exit=True, sma_cross_fast=8, sma_cross_slow=21)),
]


def run_scenario(label, setfile_rel, hedge_arch, parent_overrides,
                  ticks, m1, m5, meta, ticks_arr, regime):
    setpath = ROOT / setfile_rel
    d = parse_setfile(setpath)
    risk = get_float(d, "_RiskPct", 1.5)
    streams = []
    for sn in range(1, 7):
        if not get_bool(d, f"_ORB_S{sn}_Enabled"):
            continue
        cfg = build_cfg_for_stream(d, sn, risk)
        # Apply version-correct overrides on the global flags
        for k, v in parent_overrides.items():
            setattr(cfg, k, v)
        streams.append((sn, cfg))

    # Per-version hedge cfg (v3 reads per-stream inside the loop instead)
    hcfg = None
    if hedge_arch == "smartTP":
        hcfg = ReverseHedgeCfg(
            exp_min=get_int(d, "_HEDGE_S1_ExpireMinutes", 240),
            f1_sec=get_int(d, "_HEDGE_S1_MaxSecondsAfterEntry", 1800),
            regime_gate="off",
            sl_mult=get_float(d, "_HEDGE_S1_SLMult", 1.0),
            partial_fraction=get_float(d, "_HEDGE_S1_PartialFraction", 0.5),
            profit_mult=get_float(d, "_HEDGE_S1_ProfitMult", 3.0),
            fractal_confirm=False, fractal_width=5,
            tier_count=1, tier_spacing=0.0,
        )
    elif hedge_arch == "STOPext":
        hcfg = StopExtensionCfg(
            exp_min=get_int(d, "_HEDGE_S1_ExpireMinutes", 240),
            f1_sec=get_int(d, "_HEDGE_S1_MaxSecondsAfterEntry", 1800),
            ext_pts=get_int(d, "_HEDGE_S1_ExtPts", 100),
            tp_mult=get_float(d, "_HEDGE_S1_TPMult", 3.0),
            sl_mult=get_float(d, "_HEDGE_S1_SLMult", 1.0),
        )

    per_stream = {}
    deal_stream = []  # (ts_ns, pnl, kind) merged across streams for cap overlay
    for sn, cfg in streams:
        r = simulate_fast(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        parent_pnls, sl_events = extract_sl_events(r.deals)
        parent_np = sum(p for _, p in parent_pnls)
        for tsn, p in parent_pnls:
            deal_stream.append((int(tsn), float(p), "P"))
        # Stream meta for hedge sim
        stream_meta = {
            "magic": sn,
            "risk_pct": risk,
            "fixed_sl_pts": cfg.fixed_sl_pts,
            "rr_ratio": cfg.rr_ratio,
            "half_tp_ratio": cfg.half_tp_ratio,
            "range_minutes": cfg.range_minutes,
            "pending_expire_minutes": cfg.pending_expire_minutes,
        }
        if hedge_arch == "smartTP":
            hedge_pnls_ns = simulate_reverse_hedges(sl_events, ticks_arr, stream_meta,
                                                     hcfg, regime, fractal_cache=None)
        elif hedge_arch == "v3_singleTP":
            # Per-stream TPMult / SLMult read from v3 setfile
            tp_mult_s = get_float(d, f"_HEDGE_S{sn}_TPMult", 3.0)
            sl_mult_s = get_float(d, f"_HEDGE_S{sn}_SLMult", 1.0)
            hedge_pnls_ns = simulate_single_tp_reverse_hedge(
                sl_events, ticks_arr, stream_meta,
                tp_mult=tp_mult_s, sl_mult=sl_mult_s,
                exp_min=get_int(d, f"_HEDGE_S{sn}_ExpireMinutes", 240),
                f1_sec=get_int(d, f"_HEDGE_S{sn}_MaxSecondsAfterEntry", 1800))
        else:
            hedge_pnls_ns = simulate_stop_extension_hedges(sl_events, ticks_arr,
                                                            stream_meta, hcfg)
        hedge_np = sum(p for _, p in hedge_pnls_ns)
        for tsn, p in hedge_pnls_ns:
            deal_stream.append((int(tsn), float(p), "H"))
        per_stream[f"S{sn}"] = {
            "parent_np": parent_np,
            "parent_trades": len(parent_pnls),
            "hedge_np": hedge_np,
            "hedge_n": len(hedge_pnls_ns),
            "combined": parent_np + hedge_np,
        }
    parent_total = sum(s["parent_np"] for s in per_stream.values())
    hedge_total = sum(s["hedge_np"] for s in per_stream.values())
    return {
        "label": label,
        "setfile": setpath.name,
        "hedge_arch": hedge_arch,
        "parent_total": parent_total,
        "hedge_total": hedge_total,
        "combined_total": parent_total + hedge_total,
        "parent_trades": sum(s["parent_trades"] for s in per_stream.values()),
        "hedge_n": sum(s["hedge_n"] for s in per_stream.values()),
        "per_stream": per_stream,
        "deal_stream": sorted(deal_stream, key=lambda x: x[0]),
    }


def apply_daily_loss_cap(deal_stream, loss_pct, deposit=DEPOSIT):
    """v8 realized-only daily-loss cap overlay (drop-subsequent approximation).
    Returns capped (parent_total, hedge_total, combined, days_capped)."""
    from datetime import datetime as _dt, timezone as _tz
    bal = deposit
    cur_day = None
    day_start_bal = bal
    day_cum = 0.0
    locked = False
    days_capped = 0
    p_tot = h_tot = 0.0
    for ts_ns, pnl, kind in deal_stream:
        day = _dt.fromtimestamp(ts_ns / 1e9, tz=_tz.utc).date()
        if day != cur_day:
            cur_day = day; day_start_bal = bal; day_cum = 0.0; locked = False
        if locked:
            continue
        bal += pnl; day_cum += pnl
        if kind == "P": p_tot += pnl
        else: h_tot += pnl
        if loss_pct > 0 and day_cum <= -day_start_bal * loss_pct / 100.0:
            locked = True; days_capped += 1
    return p_tot, h_tot, p_tot + h_tot, days_capped


def main():
    # Live data load
    sym, m = fetch_meta(None, account="live")
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])
    sym, ticks, m1, m5 = fetch_window(sym, START, END, SPREAD, account="live")
    print(f"Loaded: {len(ticks):,} ticks, {len(m5):,} M5 bars on {sym}")
    ticks_arr = ts_arr_from_ticks(ticks)
    regime = tag_session_regimes(ticks, m1)
    kill_mt5_terminal()

    results = []
    for label, setfile, arch, overrides in SCENARIOS:
        print(f"\nrunning {label} ({arch})...")
        r = run_scenario(label, setfile, arch, overrides, ticks, m1, m5, meta, ticks_arr, regime)
        results.append(r)
        print(f"  parent total: ${r['parent_total']:+,.0f}  hedge total: ${r['hedge_total']:+,.0f}  "
              f"combined: ${r['combined_total']:+,.0f}")

    # v8 = v7 parents/hedges + GLOBAL realized-only daily-loss cap (4.5% at 9pct).
    v7 = next((x for x in results if x["label"] == "v7"), None)
    if v7 is not None:
        cap_pct = 4.5
        p, h, c, nc = apply_daily_loss_cap(v7["deal_stream"], cap_pct)
        results.append({
            "label": "v8", "setfile": "dt818_pro_v8_9pct_may30_may23.set",
            "hedge_arch": "STOPext+cap", "parent_total": p, "hedge_total": h,
            "combined_total": c, "parent_trades": v7["parent_trades"],
            "hedge_n": v7["hedge_n"], "per_stream": {}, "deal_stream": [],
        })
        print(f"\nv8 = v7 + {cap_pct}% daily-loss cap: combined ${c:+,.0f}  ({nc} days capped)")

    # ---------- VERSION CHANGES TABLE ----------
    print()
    print("=" * 110)
    print("  VERSION-BY-VERSION CHANGES (what changed at each step)")
    print("=" * 110)
    print(f"  {'Version':<5} {'Parent fractal':<18} {'SMA cross-exit':<18} {'Hedge architecture':<28} {'Setfile':<35}")
    print("  " + "-" * 106)
    rows = [
        ("v3", "OFF", "OFF", "single-TP reverse-LIMIT (per-stream TPMult)", "dt818_pro_v3_9pct_may16_may9"),
        ("v4", "OFF", "OFF", "smart-TP reverse-LIMIT (α=0.5, pm=3.0)", "dt818_pro_v4_9pct_may23_may16"),
        ("v5", "ON (V2 w5)", "OFF", "smart-TP reverse-LIMIT (same)", "dt818_pro_v5_9pct_may23_may16"),
        ("v6", "ON (V2 w5)", "OFF", "STOP-on-extension (ExtPts=100, TPMult=3.0, SLMult=1.0)", "dt818_pro_v6_9pct_may23_may16"),
        ("v7", "ON (V2 w5)", "ON (8,21)", "STOP-on-extension (unchanged)", "dt818_pro_v7_9pct_may30_may23"),
    ]
    for vr, frac, sma, hedge, setf in rows:
        print(f"  {vr:<5} {frac:<18} {sma:<18} {hedge:<28} {setf:<35}")
    print()
    print("  Key change deltas:")
    print("    v3 -> v4: hedge TP geometry upgrade (single-TP -> smart-TP two-stage partial close)")
    print("    v4 -> v5: parent entry gate added (V2 fractal-confirm) — fewer false-breakout entries")
    print("    v5 -> v6: hedge ARCHITECTURE swap (reverse-LIMIT -> STOP-on-extension)")
    print("    v6 -> v7: SMA(8,21) cross-exit added to runners (post-HTP exit on momentum reversal)")
    print("=" * 110)

    # ---------- COMPARISON RESULTS TABLE ----------
    print()
    print("=" * 110)
    print(f"  COMPARISON RESULTS — last 2 weeks ({START.date()} -> {END.strftime('%Y-%m-%d %H:%M UTC')})")
    print(f"  $10k base, 9% total risk, {SPREAD}pt spread, LIVE tick data")
    print("=" * 110)
    print(f"  {'Version':<6} {'Hedge':<13} {'Parent NP':>11} {'Hedge NP':>11} {'Combined':>11} "
          f"{'P trades':>9} {'H fires':>8}  {'Δ vs v3':>9}  {'Δ vs prev':>10}")
    print("  " + "-" * 110)
    base_combined = results[0]["combined_total"]
    prev_combined = None
    for r in results:
        delta_base = r["combined_total"] - base_combined
        delta_prev = r["combined_total"] - prev_combined if prev_combined is not None else 0.0
        prev_combined = r["combined_total"]
        print(f"  {r['label']:<6} {r['hedge_arch']:<13} "
              f"${r['parent_total']:>+9,.0f} ${r['hedge_total']:>+9,.0f} ${r['combined_total']:>+9,.0f} "
              f"{r['parent_trades']:>9} {r['hedge_n']:>8}  "
              f"${delta_base:>+7,.0f}  ${delta_prev:>+8,.0f}")
    print()

    # ---------- PER-STREAM DETAIL ----------
    print("  --- Per-stream COMBINED NP (parent + hedge) ---")
    header = "  " + " ".join(f"{r['label']:>11}" for r in results)
    print(f"  Stream{header}")
    for sn in range(1, 7):
        s = f"S{sn}"
        line = f"  {s:<6}"
        for r in results:
            ps = r["per_stream"].get(s)
            if ps:
                line += f"  ${ps['combined']:>+8,.0f}"
            else:
                line += f"  {'—':>9}"
        print(line)
    print()
    print("  --- Per-stream PARENT NP only ---")
    print(f"  Stream{header}")
    for sn in range(1, 7):
        s = f"S{sn}"
        line = f"  {s:<6}"
        for r in results:
            ps = r["per_stream"].get(s)
            if ps:
                line += f"  ${ps['parent_np']:>+8,.0f}"
            else:
                line += f"  {'—':>9}"
        print(line)
    print()
    print("  --- Per-stream HEDGE NP only ---")
    print(f"  Stream{header}")
    for sn in range(1, 7):
        s = f"S{sn}"
        line = f"  {s:<6}"
        for r in results:
            ps = r["per_stream"].get(s)
            if ps:
                line += f"  ${ps['hedge_np']:>+8,.0f}"
            else:
                line += f"  {'—':>9}"
        print(line)
    print()

    # Live actual reference (two-week)
    print("  --- LIVE ACTUAL (for reference, NOT scaled) ---")
    print("  Last week ran v5 (smart-TP); this week ran v7 (SMA cross + STOP-ext)")
    print("  Live parent total (2 weeks): $-79,039")
    print("  Live hedge  total (2 weeks): $-25,976  (v5 hedges last week + v7 hedges this week)")
    print("  Live combined total (2 wks): $-105,016")
    print("  NOTE: live ran at much larger lot sizes than $10k sim → direct comparison requires scaling")
    print("=" * 110)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
