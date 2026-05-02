"""Saturday-close A/B at 3% risk, 5-stream combined.

Trigger: every Saturday at UTC Fri 20:30 (= KL Sat 04:30).
At trigger, if aggregate unrealized PnL across all magics > 0 -> close all
positions and drop all pendings. Else -> do nothing.

Post-hoc filter on merged deals:
  For each Friday 20:30 UTC, find positions OPEN at trigger.
  Compute mark-to-market via M1 close.
  If aggregate > 0: replace each position's exit with (trig_ts, mtm_close, mtm_pnl).
  Else: leave them alone (positions ride to their natural SL/TP).
NO new-entry block (per latest spec).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from copy import deepcopy

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta, Deal
from zgb_sim.fbo_s1 import FBOS1Config
from zgb_sim.fbo_s1_fast import simulate_fast as fbo_simulate
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.lsfvg import LSFVGConfig
from zgb_sim.lsfvg_fast import simulate_fast as lsfvg_simulate
from zgb_sim.ema_pullback import EMAPullbackConfig
from zgb_sim.ema_pullback_fast import simulate_fast as ep_simulate

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
RISK = 3.0


def _pair_deals_fifo(deals):
    pairs = []
    open_q = []
    for d in deals:
        if d.kind == "entry":
            open_q.append(d)
        else:
            if open_q:
                e = open_q.pop(0)
                pairs.append((e, d))
    return pairs


def _close_at(m1_df, ts):
    arr = m1_df["__ts_naive"].to_numpy()
    target = pd.Timestamp(ts)
    if target.tzinfo is not None:
        target = target.tz_convert("UTC").tz_localize(None)
    idx = arr.searchsorted(target.to_numpy(), side="right") - 1
    if idx < 0 or idx >= len(m1_df):
        return None
    return float(m1_df.iloc[idx]["close"])


def _mtm_pnl(entry_deal, close_price, meta):
    diff = (close_price - entry_deal.price) * entry_deal.direction
    return diff * entry_deal.lots * meta.tick_value / meta.tick_size


def apply_sat_close(stream_pairs, m1, meta):
    paired = deepcopy(stream_pairs)
    if "__ts_naive" not in m1.columns:
        ts_col = m1["ts"]
        if pd.api.types.is_datetime64_any_dtype(ts_col) and ts_col.dt.tz is not None:
            m1 = m1.assign(__ts_naive=ts_col.dt.tz_convert("UTC").dt.tz_localize(None))
        else:
            m1 = m1.assign(__ts_naive=ts_col)

    all_ts = []
    for ps in paired.values():
        for e, x in ps:
            all_ts.extend([e.ts, x.ts])
    if not all_ts:
        return paired, []

    def _as_naive(ts):
        t = pd.Timestamp(ts)
        if t.tzinfo is not None:
            t = t.tz_convert("UTC").tz_localize(None)
        return t

    span_start = _as_naive(min(all_ts)).normalize()
    span_end = _as_naive(max(all_ts)).normalize() + pd.Timedelta(days=2)

    triggers = []
    cur = span_start
    while cur <= span_end:
        if cur.weekday() == 4:  # Friday in UTC = Saturday in KL
            triggers.append(cur.replace(hour=20, minute=30))
        cur += pd.Timedelta(days=1)

    events = []
    for trig in triggers:
        close_at = _close_at(m1, trig)
        if close_at is None:
            continue
        open_positions = []
        for stream, ps in paired.items():
            for i, (e, x) in enumerate(ps):
                if e.ts < trig and x.ts > trig:
                    open_positions.append((stream, i, e, x))
        agg = sum(_mtm_pnl(e, close_at, meta) for _, _, e, _ in open_positions)
        n_open = len(open_positions)
        action = "no-positions"
        if n_open > 0:
            if agg > 0:
                for stream, i, e, x in open_positions:
                    pnl = _mtm_pnl(e, close_at, meta)
                    new_x = Deal(ts=trig, kind="other", direction=e.direction,
                                 lots=e.lots, price=close_at, pnl=pnl)
                    paired[stream][i] = (e, new_x)
                action = "closed-all"
            else:
                action = "kept-all"
        events.append(dict(trig=trig, n_open=n_open, mtm_pnl=agg, action=action))
    return paired, events


def metrics_from_pairs(paired, deposit=DEPOSIT):
    flat = []
    for s, ps in paired.items():
        for e, x in ps:
            flat.append((x.ts, s, x.pnl))
    flat.sort(key=lambda t: t[0])
    bal = deposit
    bal_max = deposit
    dd = 0.0
    for _, _s, p in flat:
        bal += p
        if bal > bal_max: bal_max = bal
        cur = bal_max - bal
        if cur > dd: dd = cur
    np_ = bal - deposit
    dd_pct = (dd / bal_max * 100.0) if bal_max > 0 else 0.0
    ndd = (np_ / dd) if dd > 0 else 0.0
    by_stream = {}
    for _, s, p in flat:
        by_stream.setdefault(s, [0.0, 0])
        by_stream[s][0] += p
        by_stream[s][1] += 1
    return dict(np=np_, dd=dd, dd_pct=dd_pct, ndd=ndd,
                trades=len(flat), by_stream=by_stream)


def main() -> int:
    start = datetime(2026, 2, 14, tzinfo=timezone.utc)
    end = datetime(2026, 4, 25, tzinfo=timezone.utc)
    days = (end - start).days

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        ticks = load_ticks(SYMBOL, start, end)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)
        m15 = load_bars(SYMBOL, "M15", start, end)
        m30 = load_bars(SYMBOL, "M30", start, end)

        print("=" * 100)
        print(f"  SatClose A/B (5-stream, {days}d, $10k, 3% risk, 70pt)")
        print(f"  Trigger: KL Sat 04:30 = UTC Fri 20:30. If agg unrealized > 0 -> close-all + drop pendings.")
        print("=" * 100)

        # Configs (current 3pct setfile)
        fbo_s1_cfg = FBOS1Config(
            risk_pct=RISK, fractal_bars=8, take_profit_pts=25_000,
            stop_loss_pts=10_000, half_tp_ratio=0.3, sma_period=10,
            pending_expire_bars=2, signal_tf_minutes=30, comment="FBO_A",
        )
        fbo_s2_cfg = FBOS1Config(
            risk_pct=RISK, fractal_bars=8, take_profit_pts=4_000,
            stop_loss_pts=4_000, half_tp_ratio=0.6, sma_period=50,
            pending_expire_bars=4, signal_tf_minutes=15, comment="FBO_B",
        )
        orb_cfg = ORBConfig(
            risk_pct=RISK, range_minutes=90, buffer_pts=0,
            min_range_pts=200, max_range_pts=5000,
            fixed_sl_pts=350, rr_ratio=2.0, half_tp_ratio=0.0,
            pending_expire_minutes=240,
            daily_target_pct=9.0, daily_loss_pct=6.0,
            ldn_enabled=True, ldn_start_hour=7,
            ny_enabled=True, ny_start_hour=13,
            comment="ORB",
        )
        lsfvg_cfg = LSFVGConfig(
            risk_pct=RISK, signal_tf_minutes=15, lookback_bars=10,
            min_fvg_pts=20, max_fvg_pts=5000, sweep_buffer_pts=30,
            rr_ratio=2.0, half_tp_ratio=0.5, pending_expire_bars=4,
            daily_target_pct=0.0, daily_loss_pct=0.0, comment="LSFVG",
        )
        ep_cfg = EMAPullbackConfig(
            risk_pct=RISK, signal_tf_minutes=15, ema_period=50,
            lookback_bars=3, pullback_band_pts=150,
            entry_buffer_pts=0, sl_buffer_pts=30,
            rr_ratio=2.0, half_tp_ratio=0.0, pending_expire_bars=3,
            daily_target_pct=0.0, daily_loss_pct=6.0, comment="EMAPullback",
        )

        print("\n  Running 5 streams...")
        r_s1 = fbo_simulate(ticks, m30, m1, fbo_s1_cfg, meta, initial_balance=DEPOSIT)
        r_s2 = fbo_simulate(ticks, m15, m1, fbo_s2_cfg, meta, initial_balance=DEPOSIT)
        r_orb = orb_simulate(ticks, m5, m1, orb_cfg, meta, initial_balance=DEPOSIT)
        r_lsf = lsfvg_simulate(ticks, m15, m1, lsfvg_cfg, meta, initial_balance=DEPOSIT)
        r_ep = ep_simulate(ticks, m15, m1, ep_cfg, meta, initial_balance=DEPOSIT)

        stream_pairs = {
            "FBO_S1": _pair_deals_fifo(r_s1.deals),
            "FBO_S2": _pair_deals_fifo(r_s2.deals),
            "ORB":    _pair_deals_fifo(r_orb.deals),
            "LSFVG":  _pair_deals_fifo(r_lsf.deals),
            "EMAPullback": _pair_deals_fifo(r_ep.deals),
        }

        # ---------- Baseline (no SatClose) ----------
        base = metrics_from_pairs(stream_pairs)

        # ---------- With SatClose ----------
        feat_pairs, events = apply_sat_close(stream_pairs, m1, meta)
        feat = metrics_from_pairs(feat_pairs)

        # ---------- Display ----------
        print(f"\n  {'Variant':<22} {'Days':>5} {'NP':>10} {'ROI':>8} "
              f"{'DD%':>6} {'NP/DD':>7} {'Trades':>7} {'Tr/day':>7}")
        print("-" * 90)
        for label, mm in (("BASELINE (no SatCl)", base),
                          ("WITH SatClose", feat)):
            roi = mm["np"] / DEPOSIT * 100
            print(f"  {label:<22} {days:>5} {mm['np']:>+10,.0f} {roi:>+7.1f}% "
                  f"{mm['dd_pct']:>5.1f}% {mm['ndd']:>7.2f} "
                  f"{mm['trades']:>7} {mm['trades']/days:>7.2f}")

        d_np = feat["np"] - base["np"]
        d_dd = feat["dd_pct"] - base["dd_pct"]
        d_ndd = feat["ndd"] - base["ndd"]
        print(f"\n  Delta (with SatClose - baseline): NP {d_np:+,.0f}  "
              f"DD {d_dd:+.1f}pp  NP/DD {d_ndd:+.2f}")

        # Per-stream
        print("\n  Per-stream NP (with SatClose):")
        for s in ("FBO_S1", "FBO_S2", "ORB", "LSFVG", "EMAPullback"):
            if s in feat["by_stream"]:
                np_s, tr_s = feat["by_stream"][s]
                base_np = base["by_stream"].get(s, [0, 0])[0]
                d = np_s - base_np
                print(f"    {s:<12}  ${np_s:>+9,.0f}  ({tr_s} trades)  | delta vs base: ${d:+,.0f}")

        # Per-Friday event log
        print(f"\n  SatClose events ({len([e for e in events if e['action']=='closed-all'])} fired, "
              f"{len([e for e in events if e['action']=='kept-all'])} no-action, "
              f"{len([e for e in events if e['action']=='no-positions'])} no-positions):")
        print(f"  {'Trigger (UTC)':<22} {'Open':>5} {'MTM PnL':>10} {'Action':<14}")
        for ev in events:
            print(f"  {str(ev['trig']):<22} {ev['n_open']:>5} ${ev['mtm_pnl']:>+9,.0f} {ev['action']:<14}")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
