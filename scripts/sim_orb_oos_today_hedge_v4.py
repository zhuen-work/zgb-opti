"""Today's OOS — 6-stream v4 parent + smart-TP reverse-hedge.

Apples-to-apples sim for the LIVE DT818_pro_v4 EA so /live-check can show
sim-vs-live for BOTH parent and hedge contributions.

Reuses:
  - parent ORB sim from scripts/sim_orb_oos_today.py (tick fetch + simulate_fast)
  - smart-TP reverse-hedge core from scripts/sim_wfo_hedge_reverse_pm_fine.py
  - v4 setfile parser for per-stream parent + hedge params

Window: today (00:00 UTC -> now).
Output: per-stream parent NP, hedge NP, total, with 3/6/9% risk levels.

Run:  python scripts/sim_orb_oos_today_hedge_v4.py
       python scripts/sim_orb_oos_today_hedge_v4.py --account live  # XAUUSD.sc match
"""
from __future__ import annotations

import importlib.util
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.tick_loader import kill_mt5_terminal

# Reuse tick/meta fetch from the parent-only OOS script
_spec_oos = importlib.util.spec_from_file_location(
    "oos_today", ROOT / "scripts" / "sim_orb_oos_today.py")
_oos = importlib.util.module_from_spec(_spec_oos); sys.modules["oos_today"] = _oos
_spec_oos.loader.exec_module(_oos)

# Reuse smart-TP hedge sim core
_spec_rh = importlib.util.spec_from_file_location(
    "wfo_rev", ROOT / "scripts" / "sim_wfo_hedge_reverse_pm_fine.py")
_rh = importlib.util.module_from_spec(_spec_rh); sys.modules["wfo_rev"] = _rh
_spec_rh.loader.exec_module(_rh)


DEPOSIT = 10_000.0
SPREAD_LIVE = 30  # matches sim_orb_oos_today.py / live calibration
LIVE_SETFILE = ROOT / "configs" / "sets" / "dt818_pro_v4_9pct_may16_may9.set"
POINT = 0.01
CONTRACT = 100  # XAUUSD = 100 oz/lot

_now = datetime.now(timezone.utc)
OOS_START = datetime(_now.year, _now.month, _now.day, tzinfo=timezone.utc)


def parse_v4_setfile(path: Path) -> dict:
    """Returns {streams: [...rows...], hedge: {Sn: {...}}, global_hedge: {...}}.

    Each parent row: range_minutes, fixed_sl_pts, rr_ratio, half_tp_ratio.
    Each hedge cfg: partial_fraction (alpha), profit_mult (pm), sl_mult,
                    exp_min, f1_sec.
    """
    text = path.read_text()
    def _val(key: str) -> str:
        m = re.search(rf"{re.escape(key)}=([^|\n]+)", text)
        if not m:
            raise RuntimeError(f"{key} not found in {path.name}")
        return m.group(1).strip()

    streams = []
    hedges = {}
    for i in range(1, 7):
        s = f"S{i}"
        streams.append({
            "range_minutes": int(_val(f"_ORB_S{i}_RangeMinutes")),
            "fixed_sl_pts": int(_val(f"_ORB_S{i}_FixedSL_Pts")),
            "rr_ratio":     float(_val(f"_ORB_S{i}_RR_Ratio")),
            "half_tp_ratio": float(_val(f"_ORB_S{i}_HalfTP_Ratio")),
            "daily_target_pct": 999.0, "daily_loss_pct": 999.0,
        })
        hedges[s] = {
            "alpha":   float(_val(f"_HEDGE_S{i}_PartialFraction")),
            "pm":      float(_val(f"_HEDGE_S{i}_ProfitMult")),
            "sl_mult": float(_val(f"_HEDGE_S{i}_SLMult")),
            "exp_min": int(_val(f"_HEDGE_S{i}_ExpireMinutes")),
            "f1_sec":  int(_val(f"_HEDGE_S{i}_MaxSecondsAfterEntry")),
        }
    return {"streams": streams, "hedges": hedges}


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


def extract_sl_events(deals_iter):
    """Walk simulate_fast deals; pair entry+sl pairs (FIFO by direction).

    Returns (pnl_pairs, sl_events). Mirrors sim_wfo_hedge_retry.run_baseline_window
    but accepts a pre-run deal iterable so caller controls config.
    """
    pnl_pairs = []
    open_positions = []
    sl_events = []
    for d in deals_iter:
        ts_ns = pd.Timestamp(d.ts).value
        if d.kind == "entry":
            open_positions.append({
                "entry_ts_ns": ts_ns,
                "direction": int(d.direction),
                "lots": float(d.lots),
                "entry_price": float(d.price),
            })
            continue
        pnl_pairs.append((ts_ns, d.pnl))
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
    return pnl_pairs, sl_events


def ts_arr_from_ticks(ticks: pd.DataFrame) -> dict:
    ts_ns = (ticks["ts"].dt.tz_convert("UTC").dt.tz_localize(None)
             .astype("datetime64[ns]").astype("int64").to_numpy())
    return {"ts_ns": ts_ns,
            "bid": ticks["bid"].to_numpy(dtype=np.float64),
            "ask": ticks["ask"].to_numpy(dtype=np.float64)}


def aggregate_pnl(pnl_pairs) -> tuple[float, float, int]:
    """Returns (np, dd_abs, n_trades)."""
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    for _, p in sorted(pnl_pairs, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
    return bal - DEPOSIT, dd_abs, len(pnl_pairs)


def fetch_live_today_split():
    """Pull today's live parent + hedge NP from the dashboard API.

    Returns (live_parent, live_hedge, proj_anchor, current_balance, day_open_balance, ok).
    Hedge magics 8xxx, parent magics 1111/2222/3333/4444/5555/6666.
    proj_anchor = projection baseline_balance (what the dashboard scales by).
    day_open_balance = current_balance - today's net P&L (sim's "real" anchor).
    """
    import json as _json
    import os as _os
    import urllib.request as _u
    from zgb_sim.cf_publish import _load_dotenv as _cfp_load_dotenv
    _cfp_load_dotenv()
    api = _os.environ.get("CONSOLE_API_BASE", "")
    tok = (_os.environ.get("CONSOLE_READ_TOKEN", "")
           or _os.environ.get("CONSOLE_INGEST_TOKEN", ""))
    if not (api and tok):
        return 0.0, 0.0, 0.0, 0.0, 0.0, False
    req = _u.Request(api.rstrip("/") + "/api/today",
                     headers={"Authorization": f"Bearer {tok}",
                              "User-Agent": "sim-oos-hedge-v4/1.0"})
    with _u.urlopen(req, timeout=10) as resp:
        body = _json.loads(resp.read().decode())
    today_deals = body.get("today_deals") or []
    parent_mags = {1111, 2222, 3333, 4444, 5555, 6666}
    hedge_mags = {8111, 8222, 8333, 8444, 8555, 8666}
    live_parent = sum(float(d.get("profit", 0))
                       for d in today_deals if int(d.get("magic", 0)) in parent_mags)
    live_hedge = sum(float(d.get("profit", 0))
                      for d in today_deals if int(d.get("magic", 0)) in hedge_mags)
    proj = body.get("projection") or {}
    proj_anchor = float(proj.get("baseline_balance") or 0.0)
    acct = body.get("account") or {}
    current_balance = float(acct.get("balance") or 0.0)
    day_open_balance = current_balance - (live_parent + live_hedge)
    return live_parent, live_hedge, proj_anchor, current_balance, day_open_balance, True


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", choices=["sim", "live"], default="sim",
                    help="MT5 account for tick data. 'sim' = XAUUSD on 18912087; "
                         "'live' = XAUUSD.sc on 21478621 (matches EA's symbol).")
    args = ap.parse_args()

    sf = parse_v4_setfile(LIVE_SETFILE)
    streams = sf["streams"]
    hedges = sf["hedges"]
    labels = ["S1", "S2", "S3", "S4", "S5", "S6"]
    sources = ["MAY9 R1", "MAY9 R2", "MAY9 R3",
               "MAY16 R2", "MAY16 R3", "MAY16 R4"]

    end = datetime.now(timezone.utc)
    days = (end - OOS_START).days

    print("=" * 110)
    print(f"  v4 6-stream OOS (parent + smart-TP reverse-hedge)  |  "
          f"{OOS_START.date()} -> {end.strftime('%Y-%m-%d %H:%M UTC')}  ({days}d)")
    print(f"  Spread {SPREAD_LIVE}pt, $10k, total risk split N=6 (per_stream = total/6)")
    print(f"  Setfile: {LIVE_SETFILE.name}")
    for lbl, src, r, h in zip(labels, sources, streams, [hedges[s] for s in labels]):
        print(f"    {lbl} ({src}): Range={r['range_minutes']} SL={r['fixed_sl_pts']} "
              f"RR={r['rr_ratio']} HTP={r['half_tp_ratio']} | "
              f"hedge sl_mult={h['sl_mult']} alpha={h['alpha']} pm={h['pm']}")
    print("=" * 110)

    try:
        sym_used, m = _oos.fetch_meta(None, account=args.account)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        sym_used, ticks, m1, m5 = _oos.fetch_window(
            sym_used, OOS_START, end, SPREAD_LIVE, account=args.account)
        print(f"  Account: {args.account}  Symbol: {sym_used}")
        print(f"  Ticks: {len(ticks):,} rows  range={ticks.ts.min()} -> {ticks.ts.max()}")
        print(f"  Bars: M1={len(m1):,}  M5={len(m5):,}")

        ticks_arr = ts_arr_from_ticks(ticks)

        print(f"\n  {'TotalRisk':<10} {'PerStream':<10} "
              f"{'Parent NP':>10} {'Hedge NP':>10} {'Total NP':>10} "
              f"{'P trades':>8} {'H trades':>8} {'P+H ROI':>8}  "
              f"Per-stream (parent / hedge)")
        risk_results = {}
        for total_risk in (3.0, 6.0, 9.0):
            per_stream = total_risk / 6
            parent_pairs_all = []
            hedge_pairs_all = []
            per_s = {}
            for label, row in zip(labels, streams):
                cfg = row_to_cfg(row, label, per_stream)
                r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
                parent_pairs, sl_events = extract_sl_events(r.deals)

                hcfg_dict = hedges[label]
                hcfg = _rh.ReverseHedgeCfg(
                    exp_min=hcfg_dict["exp_min"],
                    f1_sec=hcfg_dict["f1_sec"],
                    regime_gate="off",
                    sl_mult=hcfg_dict["sl_mult"],
                    partial_fraction=hcfg_dict["alpha"],
                    profit_mult=hcfg_dict["pm"],
                )
                stream_cfg = {"fixed_sl_pts": row["fixed_sl_pts"]}
                hedge_pairs = _rh.simulate_reverse_hedges(
                    sl_events, ticks_arr, stream_cfg, hcfg,
                    regime_by_session={})

                p_np, _, p_n = aggregate_pnl(parent_pairs)
                h_np, _, h_n = aggregate_pnl(hedge_pairs)
                per_s[label] = (p_np, p_n, h_np, h_n)
                parent_pairs_all.extend(parent_pairs)
                hedge_pairs_all.extend(hedge_pairs)

            P_np, P_dd, P_n = aggregate_pnl(parent_pairs_all)
            H_np, H_dd, H_n = aggregate_pnl(hedge_pairs_all)
            T_np = P_np + H_np
            roi = T_np / DEPOSIT * 100
            ps_str = " ".join(
                f"{s}:${p_np:+,.0f}({p_n})/${h_np:+,.0f}({h_n})"
                for s, (p_np, p_n, h_np, h_n) in per_s.items())
            print(f"  {total_risk:>5.1f}%    {per_stream:>5.2f}%    "
                  f"${P_np:>+8,.0f} ${H_np:>+8,.0f} ${T_np:>+8,.0f} "
                  f"{P_n:>8} {H_n:>8} {roi:>+7.1f}%  {ps_str}")
            risk_results[total_risk] = (parent_pairs_all, hedge_pairs_all, per_s)

        # ===== Live comparison @ 9% risk =====
        P_pairs, H_pairs, per_s = risk_results[9.0]
        P_np, _, _ = aggregate_pnl(P_pairs)
        H_np, _, _ = aggregate_pnl(H_pairs)
        T_np = P_np + H_np

        (live_parent, live_hedge, proj_anchor, current_balance,
         day_open_balance, ok) = fetch_live_today_split()
        print()
        print("=" * 110)
        print(f"  SIM vs LIVE today @ 9% risk  (live deployed setfile)")
        print("=" * 110)
        if not ok:
            print(f"  [no API access; showing sim only]")
            print(f"  Sim @ 9% raw:  parent=${P_np:+,.0f}  hedge=${H_np:+,.0f}  "
                  f"total=${T_np:+,.0f}")
            return 0
        live_total = live_parent + live_hedge

        def diff_pct(live, sim):
            if sim == 0:
                return float("nan")
            return (live - sim) / abs(sim) * 100

        def print_anchor_block(label: str, anchor: float):
            if anchor <= 0:
                print(f"\n  [{label}: anchor unavailable]")
                return
            scale = anchor / DEPOSIT
            P_sc = P_np * scale
            H_sc = H_np * scale
            T_sc = T_np * scale
            print(f"\n  [{label}]  anchor=${anchor:,.0f}  (sim raw $10k -> x{scale:.2f})")
            print(f"  {'Component':<10} {'Sim raw':>12} {'Sim scaled':>14} {'Live':>14} "
                  f"{'d$ (live-sim)':>16} {'d%':>8}")
            print(f"  {'Parent':<10} ${P_np:>+10,.0f} ${P_sc:>+12,.0f} "
                  f"${live_parent:>+12,.0f} ${live_parent - P_sc:>+14,.0f} "
                  f"{diff_pct(live_parent, P_sc):>+7.1f}%")
            print(f"  {'Hedge':<10} ${H_np:>+10,.0f} ${H_sc:>+12,.0f} "
                  f"${live_hedge:>+12,.0f} ${live_hedge - H_sc:>+14,.0f} "
                  f"{diff_pct(live_hedge, H_sc):>+7.1f}%")
            print(f"  {'TOTAL':<10} ${T_np:>+10,.0f} ${T_sc:>+12,.0f} "
                  f"${live_total:>+12,.0f} ${live_total - T_sc:>+14,.0f} "
                  f"{diff_pct(live_total, T_sc):>+7.1f}%")

        # Primary anchor: day-open balance (apples-to-apples — sim lots vs live
        # lots both sized on the same notional). This is the meaningful number.
        print_anchor_block("balance-anchored (day open)", day_open_balance)
        # Secondary anchor: projection baseline (what dt818-console publishes).
        # Useful for dashboard cross-check; distorts when current bal differs.
        print_anchor_block("projection-baseline anchored (dashboard view)", proj_anchor)

        sim_hedge_pct = (H_np / abs(P_np) * 100) if P_np != 0 else 0.0
        live_hedge_pct = (live_hedge / abs(live_parent) * 100) if live_parent != 0 else 0.0
        print(f"\n  Hedge contribution to |parent| loss:")
        print(f"    sim:  {sim_hedge_pct:+.1f}%  (smart-TP design target on this whipsaw shape)")
        print(f"    live: {live_hedge_pct:+.1f}%  (actual EA execution)")
        print(f"    -> live hedge {'beat' if live_hedge_pct > sim_hedge_pct else 'underperformed'} sim "
              f"by {abs(live_hedge_pct - sim_hedge_pct):.1f}pp.")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
