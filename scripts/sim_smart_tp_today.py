"""One-day smart-TP reverse-hedge OOS comparison.

Runs today's parent ORB sim + smart-TP reverse hedge (alpha=0.5, pm=1.2 defaults
from configs/sets/dt818_pro_v3_9pct_may16_may9.set) and compares vs:
  - Live actual (parent + OLD single-tp hedge result from the EA running today)

Why: 2026-05-20 catastrophic whipsaw day -$83k (parent -$43k + OLD hedge -$40k).
User asks: if smart-TP hedge had been deployed today, would the hedge contribution
have been better or worse?

Smart-TP places TWO LIMITs per parent SL:
  - Stage 1 (alpha=0.5 of lots): TP at sl_dist*sl_mult/alpha  (combined BE)
  - Stage 2 ((1-alpha)=0.5 of lots): TP at sl_dist*sl_mult*pm/(1-alpha)  (+20% combined)
Both share entry (= parent's orig_entry) and SL (= entry ± sl_dist*sl_mult).
"""
from __future__ import annotations

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

from sim_orb_oos_today import fetch_meta, fetch_window, SPREAD_LIVE, DEPOSIT
from sim_wfo_hedge_reverse import simulate_reverse_hedges, ReverseHedgeCfg
from sim_wfo_hedge_retry import ts_arr_from_ticks, run_baseline_window, STREAM_CFGS

SETFILE = ROOT / "configs" / "sets" / "dt818_pro_v3_9pct_may16_may9.set"
LIVE_PRE_BALANCE = 169_990.71   # 2026-05-20 pre-day from live_check reconciliation
LIVE_PARENT_NP = -43_582.14
LIVE_HEDGE_NP_OLD = -39_860.40
LIVE_HEDGE_TRADES = 36

# Live EA's risk-equalization anchor: per-stream parent risk is 1.5% of deposit.
# Sim deposit is $10k → per-stream parent ~$150 risk; live balance is $169,991.
# Scale factor = balance / DEPOSIT.
PER_STREAM_RISK_PCT = 1.5


def parse_setfile(path: Path) -> dict:
    text = path.read_text()
    streams = {}
    for i in range(1, 7):
        s = f"S{i}"
        def g(key, kind=float):
            m = re.search(rf"_ORB_{s}_{key}=([^|]+)\|\|", text)
            if not m:
                raise RuntimeError(f"{s}/{key} not in setfile")
            return kind(m.group(1).strip())
        def gh(key, kind=float):
            m = re.search(rf"_HEDGE_{s}_{key}=([^|]+)\|\|", text)
            if not m:
                raise RuntimeError(f"{s}/HEDGE/{key} not in setfile")
            return kind(m.group(1).strip())
        streams[s] = {
            "range_minutes": g("RangeMinutes", int),
            "fixed_sl_pts": g("FixedSL_Pts", int),
            "rr_ratio": g("RR_Ratio", float),
            "half_tp_ratio": g("HalfTP_Ratio", float),
            "sl_mult": gh("SLMult", float),
            "partial_fraction": gh("PartialFraction", float),
            "profit_mult": gh("ProfitMult", float),
        }
    return streams


def build_cfg(sc: dict, label: str, risk_pct: float) -> ORBConfig:
    return ORBConfig(
        risk_pct=risk_pct,
        range_minutes=sc["range_minutes"],
        buffer_pts=0, min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=sc["fixed_sl_pts"],
        rr_ratio=sc["rr_ratio"],
        half_tp_ratio=sc["half_tp_ratio"],
        pending_expire_minutes=240,
        daily_target_pct=999.0, daily_loss_pct=999.0,
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True,  ny_start_hour=13,
        comment=label,
    )


def main() -> int:
    streams = parse_setfile(SETFILE)
    end = datetime.now(timezone.utc)
    start = datetime(end.year, end.month, end.day, tzinfo=timezone.utc)

    print("=" * 100)
    print(f"  SMART-TP TODAY: parent + smart-TP hedge | {start.date()} -> {end.strftime('%H:%M UTC')}")
    print(f"  Setfile: {SETFILE.name}")
    print(f"  Spread {SPREAD_LIVE}pt, deposit ${DEPOSIT:,.0f}, per-stream risk {PER_STREAM_RISK_PCT}%")
    print(f"  Hedge cfg per stream (sl_mult / alpha=PartialFraction / pm=ProfitMult):")
    for s, sc in streams.items():
        print(f"    {s}: SL={sc['fixed_sl_pts']} RR={sc['rr_ratio']} HTP={sc['half_tp_ratio']}  "
              f"hedge: sl_mult={sc['sl_mult']:.2f} alpha={sc['partial_fraction']:.2f} pm={sc['profit_mult']:.2f}")
    print("=" * 100)

    sym_used, m = fetch_meta(None, account="sim")
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                       tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                       volume_min=m["volume_min"], volume_max=m["volume_max"],
                       volume_step=m["volume_step"])
    sym_used, ticks, m1, m5 = fetch_window(sym_used, start, end, SPREAD_LIVE, account="sim")
    print(f"  Account: sim  Symbol: {sym_used}  Ticks: {len(ticks):,}")
    print(f"  Window: {ticks.ts.min()} -> {ticks.ts.max()}")

    ticks_arr = ts_arr_from_ticks(ticks)
    regime_by_session = {}   # gate=off → unused

    parent_np = 0.0
    parent_trades = 0
    smart_hedge_np = 0.0
    smart_hedge_trades = 0
    per_stream_summary = []

    for s, sc in streams.items():
        # run_baseline_window uses STREAM_CFGS internally (matches our setfile params
        # by construction — both derived from same WFO winners) and returns the
        # canonical (deals, sl_events) pair already used by sim_wfo_hedge_reverse.
        p_deals, sl_events = run_baseline_window(s, ticks, m1, m5, meta, PER_STREAM_RISK_PCT)
        p_np = sum(pnl for _, pnl in p_deals)
        p_tr = len(p_deals)

        hcfg = ReverseHedgeCfg(
            exp_min=240, f1_sec=1800, regime_gate="off",
            sl_mult=sc["sl_mult"],
            partial_fraction=sc["partial_fraction"],
            profit_mult=sc["profit_mult"],
        )
        # stream_cfg dict only needs fixed_sl_pts for simulate_reverse_hedges
        stream_cfg = {"fixed_sl_pts": sc["fixed_sl_pts"]}
        h_deals = simulate_reverse_hedges(sl_events, ticks_arr, stream_cfg, hcfg, regime_by_session)
        h_np = sum(pnl for _, pnl in h_deals)
        h_tr = len(h_deals)   # 2 deals per SL event (stage1 + stage2)

        parent_np += p_np
        parent_trades += p_tr
        smart_hedge_np += h_np
        smart_hedge_trades += h_tr
        per_stream_summary.append({
            "s": s, "parent_np": p_np, "parent_tr": p_tr,
            "sl_events": len(sl_events),
            "hedge_np": h_np, "hedge_tr": h_tr,
        })

    print()
    print(f"  {'Stream':<8} {'ParentNP':>10} {'P-tr':>5} {'SL-ev':>6} "
          f"{'SmartTP-HedgeNP':>16} {'H-tr':>5}")
    for row in per_stream_summary:
        print(f"  {row['s']:<8} ${row['parent_np']:>+8,.0f} {row['parent_tr']:>5} "
              f"{row['sl_events']:>6} ${row['hedge_np']:>+14,.0f} {row['hedge_tr']:>5}")
    print(f"  {'TOTAL':<8} ${parent_np:>+8,.0f} {parent_trades:>5} "
          f"{sum(r['sl_events'] for r in per_stream_summary):>6} "
          f"${smart_hedge_np:>+14,.0f} {smart_hedge_trades:>5}")

    # Scale to live balance ($169,990 pre-day vs $10k sim deposit).
    scale = LIVE_PRE_BALANCE / DEPOSIT
    parent_np_scaled = parent_np * scale
    smart_hedge_np_scaled = smart_hedge_np * scale
    combined_smart = parent_np_scaled + smart_hedge_np_scaled

    print()
    print("=" * 100)
    print(f"  SCALED TO LIVE BALANCE  (${LIVE_PRE_BALANCE:,.0f} pre-day, scale ×{scale:.1f})")
    print("=" * 100)
    print(f"  {'Scenario':<35} {'Parent NP':>14} {'Hedge NP':>14} {'Combined':>14} {'Trades':>9}")
    print(f"  {'LIVE ACTUAL (old single-tp)':<35} ${LIVE_PARENT_NP:>+12,.0f} ${LIVE_HEDGE_NP_OLD:>+12,.0f} "
          f"${LIVE_PARENT_NP + LIVE_HEDGE_NP_OLD:>+12,.0f} {LIVE_HEDGE_TRADES:>9}")
    print(f"  {'SIM PARENT-ONLY (no hedge)':<35} ${parent_np_scaled:>+12,.0f} ${0:>+12,.0f} "
          f"${parent_np_scaled:>+12,.0f} {parent_trades:>9}")
    print(f"  {'SIM SMART-TP (alpha=0.5, pm=1.2)':<35} ${parent_np_scaled:>+12,.0f} ${smart_hedge_np_scaled:>+12,.0f} "
          f"${combined_smart:>+12,.0f} {parent_trades + smart_hedge_trades:>9}")

    print()
    print(f"  delta smart-TP vs live old-hedge: ${combined_smart - (LIVE_PARENT_NP + LIVE_HEDGE_NP_OLD):>+12,.0f} "
          f"(hedge-only: ${smart_hedge_np_scaled - LIVE_HEDGE_NP_OLD:>+,.0f})")
    print(f"  delta smart-TP hedge vs no-hedge: ${smart_hedge_np_scaled:>+12,.0f}")
    print("=" * 100)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
