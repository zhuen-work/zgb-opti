"""Today's OOS — 6-stream v6 parent (V2 fractal-confirm) + STOP-on-extension hedge.

Apples-to-apples sim for the LIVE DT818_pro_v6 EA so /live-check can show
sim-vs-live for BOTH parent and hedge contributions.

Reuses:
  - parent ORB sim from scripts/sim_orb_oos_today.py (tick fetch + simulate_fast)
  - STOP-extension hedge core from scripts/sim_wfo_hedge_reverse.py
  - v6 setfile parser (ExtPts, TPMult, SLMult per stream)

Window: today (00:00 UTC -> now).
Output: per-stream parent NP, hedge NP, total, with 3/6/9% risk levels.

Run:  python scripts/sim_orb_oos_today_hedge_v6.py
       python scripts/sim_orb_oos_today_hedge_v6.py --account live  # XAUUSD.sc match
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

# Import StopExtensionCfg + simulate_stop_extension_hedges directly
sys.path.insert(0, str(ROOT / "scripts"))
from sim_wfo_hedge_reverse import (StopExtensionCfg, simulate_stop_extension_hedges,
                                    tag_session_regimes)


DEPOSIT = 10_000.0
SPREAD_LIVE = 30
LIVE_SETFILE = ROOT / "configs" / "sets" / "dt818_pro_v6_9pct_may23_may16.set"
POINT = 0.01
CONTRACT = 100  # XAUUSD = 100 oz/lot

_now = datetime.now(timezone.utc)
OOS_START = datetime(_now.year, _now.month, _now.day, tzinfo=timezone.utc)


def parse_v6_setfile(path: Path) -> dict:
    """Returns {streams: [...rows...], hedges: {Sn: {...}}, globals: {...}}.

    Each parent row: range_minutes, fixed_sl_pts, rr_ratio, half_tp_ratio,
                     pending_expire_minutes.
    Each hedge cfg: ext_pts, tp_mult, sl_mult, exp_min, f1_sec.
    Globals: fractal_confirm, fractal_width.
    """
    text = path.read_text()
    def _val(key: str) -> str:
        m = re.search(rf"{re.escape(key)}=([^|\n;]+)", text)
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
            "pending_expire_minutes": int(_val(f"_ORB_S{i}_PendingExpireMinutes")),
        })
        hedges[s] = {
            "ext_pts": int(_val(f"_HEDGE_S{i}_ExtPts")),
            "tp_mult": float(_val(f"_HEDGE_S{i}_TPMult")),
            "sl_mult": float(_val(f"_HEDGE_S{i}_SLMult")),
            "exp_min": int(_val(f"_HEDGE_S{i}_ExpireMinutes")),
            "f1_sec":  int(_val(f"_HEDGE_S{i}_MaxSecondsAfterEntry")),
        }
    globals_ = {
        "fractal_confirm": _val("_ORB_FractalConfirm").lower() == "true",
        "fractal_width": int(_val("_ORB_FractalWidth")),
    }
    return {"streams": streams, "hedges": hedges, "globals": globals_}


def row_to_cfg(row, comment: str, risk_pct: float, fractal_confirm: bool, fractal_width: int) -> ORBConfig:
    return ORBConfig(
        risk_pct=risk_pct,
        range_minutes=int(row["range_minutes"]),
        buffer_pts=0,
        min_range_pts=0, max_range_pts=999_999,
        fixed_sl_pts=int(row["fixed_sl_pts"]),
        rr_ratio=float(row["rr_ratio"]),
        half_tp_ratio=round(float(row["half_tp_ratio"]), 2),
        pending_expire_minutes=int(row["pending_expire_minutes"]),
        daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True,  ny_start_hour=13,
        fractal_confirm=fractal_confirm,
        fractal_width=fractal_width,
        comment=comment,
    )


def extract_sl_events(deals_iter):
    """Walk deals; match entry+exit pairs FIFO by direction; capture SL exits.

    Returns (pnl_pairs, sl_events, f1_stats) where f1_stats has counts so the
    caller can report how many SLs would be F1-filtered.
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
                match_idx = i; break
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


def f1_filter_stats(sl_events, f1_sec: int):
    """Return (n_total, n_within_f1, n_rejected, elapsed_min_list)."""
    elapsed = [(s["ts_ns"] - s["entry_ts_ns"]) / 1e9 / 60.0 for s in sl_events]
    within = sum(1 for e in elapsed if e * 60 <= f1_sec)
    return len(sl_events), within, len(sl_events) - within, elapsed


def ts_arr_from_ticks(ticks: pd.DataFrame) -> dict:
    ts_ns = (ticks["ts"].dt.tz_convert("UTC").dt.tz_localize(None)
             .astype("datetime64[ns]").astype("int64").to_numpy())
    return {"ts_ns": ts_ns,
            "bid": ticks["bid"].to_numpy(dtype=np.float64),
            "ask": ticks["ask"].to_numpy(dtype=np.float64)}


def aggregate_pnl(pnl_pairs):
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    for _, p in sorted(pnl_pairs, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
    return bal - DEPOSIT, dd_abs, len(pnl_pairs)


def fetch_live_today_split():
    import json as _json, os as _os
    import urllib.request as _u
    from zgb_sim.cf_publish import _load_dotenv as _cfp_load_dotenv
    _cfp_load_dotenv()
    api = _os.environ.get("CONSOLE_API_BASE", "")
    tok = (_os.environ.get("CONSOLE_READ_TOKEN", "")
           or _os.environ.get("CONSOLE_INGEST_TOKEN", ""))
    if not (api and tok):
        return 0.0, 0.0, 0.0, 0.0, 0.0, False
    req = _u.Request(api.rstrip("/") + f"/api/today?_={int(_now.timestamp())}",
                     headers={"Authorization": f"Bearer {tok}",
                              "User-Agent": "sim-oos-hedge-v6/1.0"})
    with _u.urlopen(req, timeout=10) as r:
        body = _json.loads(r.read())
    deals = body.get("today_deals") or []
    PARENT = {1111, 2222, 3333, 4444, 5555, 6666}
    HEDGE = {8111, 8222, 8333, 8444, 8555, 8666}
    live_parent = sum(d.get("profit", 0) for d in deals if d.get("magic", 0) in PARENT)
    live_hedge  = sum(d.get("profit", 0) for d in deals if d.get("magic", 0) in HEDGE)
    proj = body.get("projection") or {}
    proj_anchor = float(proj.get("baseline_balance") or 0.0)
    acct = body.get("account") or {}
    current_balance = float(acct.get("balance") or 0.0)
    day_open_balance = current_balance - (live_parent + live_hedge)
    return live_parent, live_hedge, proj_anchor, current_balance, day_open_balance, True


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", choices=["sim", "live"], default="live",
                    help="MT5 account for tick data. 'live' = XAUUSD.sc (matches EA's symbol).")
    args = ap.parse_args()

    sf = parse_v6_setfile(LIVE_SETFILE)
    streams = sf["streams"]; hedges = sf["hedges"]; globals_ = sf["globals"]
    labels = ["S1","S2","S3","S4","S5","S6"]

    end = datetime.now(timezone.utc)

    print("=" * 110)
    print(f"  v6 6-stream OOS (V2 fractal-confirm parent + STOP-on-extension hedge)")
    print(f"  Window: {OOS_START.isoformat()} -> {end.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Spread {SPREAD_LIVE}pt, $10k base, fractal_confirm={globals_['fractal_confirm']} width={globals_['fractal_width']}")
    print(f"  Setfile: {LIVE_SETFILE.name}")
    for lbl, r in zip(labels, streams):
        h = hedges[lbl]
        print(f"    {lbl}: parent Range={r['range_minutes']} SL={r['fixed_sl_pts']} "
              f"RR={r['rr_ratio']} HTP={r['half_tp_ratio']} Exp={r['pending_expire_minutes']} "
              f"| hedge ExtPts={h['ext_pts']} TPMult={h['tp_mult']} SLMult={h['sl_mult']}")
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
        print(f"  Ticks: {len(ticks):,}  M1: {len(m1):,}  M5: {len(m5):,}")

        ticks_arr = ts_arr_from_ticks(ticks)
        regime = tag_session_regimes(ticks, m1)

        print(f"\n  {'TotalRisk':<10} {'PerStream':<10} "
              f"{'Parent NP':>10} {'Hedge NP':>10} {'Total NP':>10} "
              f"{'P trades':>8} {'H trades':>8} {'P+H ROI':>8}")
        risk_results = {}
        sl_diag = None  # cache 9% diag for display below
        for total_risk in (3.0, 6.0, 9.0):
            per_stream = total_risk / 6
            parent_pairs_all = []
            hedge_pairs_all = []
            sl_diag_run = []  # per-stream (n_sl, n_within_f1, n_rejected)
            for label, row in zip(labels, streams):
                cfg = row_to_cfg(row, label, per_stream,
                                  globals_["fractal_confirm"], globals_["fractal_width"])
                r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
                parent_pairs, sl_events = extract_sl_events(r.deals)

                hcfg_dict = hedges[label]
                hcfg = StopExtensionCfg(
                    exp_min=hcfg_dict["exp_min"], f1_sec=hcfg_dict["f1_sec"],
                    ext_pts=hcfg_dict["ext_pts"], tp_mult=hcfg_dict["tp_mult"],
                    sl_mult=hcfg_dict["sl_mult"],
                )
                n_sl, n_in, n_rej, elapsed = f1_filter_stats(sl_events, hcfg_dict["f1_sec"])
                sl_diag_run.append((label, n_sl, n_in, n_rej, elapsed))
                stream_cfg = {"fixed_sl_pts": row["fixed_sl_pts"]}
                hedge_pairs = simulate_stop_extension_hedges(
                    sl_events, ticks_arr, stream_cfg, hcfg)

                parent_pairs_all.extend(parent_pairs)
                hedge_pairs_all.extend(hedge_pairs)

            P_np, _, P_n = aggregate_pnl(parent_pairs_all)
            H_np, _, H_n = aggregate_pnl(hedge_pairs_all)
            T_np = P_np + H_np
            roi = T_np / DEPOSIT * 100
            print(f"  {total_risk:>5.1f}%    {per_stream:>5.2f}%    "
                  f"${P_np:>+8,.0f} ${H_np:>+8,.0f} ${T_np:>+8,.0f} "
                  f"{P_n:>8} {H_n:>8} {roi:>+7.1f}%")
            risk_results[total_risk] = (parent_pairs_all, hedge_pairs_all)
            if total_risk == 9.0:
                sl_diag = sl_diag_run

        # Per-stream SL/F1 diagnostic (9% run)
        if sl_diag:
            print(f"\n  Per-stream SL events & F1 filter (F1=1800s = 30min):")
            print(f"  {'Stream':<6} {'#SL':>4} {'within F1':>10} {'rejected':>9} {'elapsed min (each SL)':<30}")
            for label, n_sl, n_in, n_rej, elapsed in sl_diag:
                el_str = ", ".join(f"{e:.1f}" for e in elapsed) if elapsed else "-"
                print(f"  {label:<6} {n_sl:>4} {n_in:>10} {n_rej:>9}  {el_str}")

        # Live vs sim comparison @ 9% risk
        P_pairs, H_pairs = risk_results[9.0]
        P_np, _, _ = aggregate_pnl(P_pairs)
        H_np, _, _ = aggregate_pnl(H_pairs)
        T_np = P_np + H_np

        (live_parent, live_hedge, proj_anchor, current_balance,
         day_open_balance, ok) = fetch_live_today_split()
        print()
        print("=" * 110)
        print(f"  SIM vs LIVE today @ 9% risk")
        print("=" * 110)
        if not ok:
            print(f"  [no console API; showing sim only]")
            print(f"  Sim @ 9% raw:  parent=${P_np:+,.0f}  hedge=${H_np:+,.0f}  total=${T_np:+,.0f}")
            return 0
        live_total = live_parent + live_hedge

        # Sim is at $10k base; live is at $71.5k. Scale sim → live by balance ratio.
        scale = day_open_balance / DEPOSIT if day_open_balance > 0 else 1.0
        P_scaled = P_np * scale
        H_scaled = H_np * scale
        T_scaled = T_np * scale

        def fpct(live, sim):
            if abs(sim) < 0.01: return float("nan")
            return (sim - live) / sim * 100

        print(f"  Live balance: ${current_balance:,.2f}  day_open: ${day_open_balance:,.2f}  proj_anchor: ${proj_anchor:,.2f}")
        print(f"  Sim base $10k -> Live scale ×{scale:.3f}")
        print()
        print(f"  {'Component':<10} {'Sim @ 9% raw':>14} {'Sim scaled to live':>20} {'Live':>14} {'Friction%':>10}")
        print(f"  {'Parent':<10} ${P_np:>+13,.0f}  ${P_scaled:>+18,.0f}    ${live_parent:>+12,.0f}  {fpct(live_parent, P_scaled):>+9.1f}%")
        print(f"  {'Hedge':<10} ${H_np:>+13,.0f}  ${H_scaled:>+18,.0f}    ${live_hedge:>+12,.0f}  {fpct(live_hedge, H_scaled):>+9.1f}%")
        print(f"  {'TOTAL':<10} ${T_np:>+13,.0f}  ${T_scaled:>+18,.0f}    ${live_total:>+12,.0f}  {fpct(live_total, T_scaled):>+9.1f}%")
        print()
        # Per-layer verdict — split parent vs hedge so hedge-timing divergence
        # doesn't mask a parent friction that's actually OK.
        parent_friction = fpct(live_parent, P_scaled)
        def _layer_verdict(name, fr):
            if fr != fr:  # NaN
                return f"  {name}: n/a (sim=0)"
            if abs(fr) <= 10:
                return f"  {name}: OK (|friction| <= 10%) friction={fr:+.1f}%"
            if abs(fr) <= 20:
                return f"  {name}: WARN moderate divergence ({fr:+.1f}%)"
            return f"  {name}: FAIL large divergence ({fr:+.1f}%) -- investigate"
        print(_layer_verdict("Parent", parent_friction))
        # Hedge layer — if sim n=0 but live > 0, this is a timing-divergence
        # signal (sim parent SLs fell outside F1 window vs live's). Not the
        # same as parent friction.
        if abs(H_np) < 0.01:
            if abs(live_hedge) < 0.01:
                print(f"  Hedge:  OK (both sim & live = 0)")
            else:
                print(f"  Hedge:  TIMING-DIVERGENCE -- sim parent SLs all fell "
                      f"outside F1=30min, but live hedge fired (${live_hedge:+,.0f}). "
                      f"See per-stream elapsed table above.")
        else:
            print(_layer_verdict("Hedge", fpct(live_hedge, H_scaled)))
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
