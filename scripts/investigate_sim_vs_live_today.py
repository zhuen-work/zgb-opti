"""Deep investigation: every sim deal vs every live deal today, per stream.

Dumps timestamps + side + kind + price for both, then highlights mismatches.
"""
from __future__ import annotations
import sys
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from sim_orb_oos_today import (PREV_WFO_DIR, CURR_WFO_DIR, SPREAD_LIVE, DEPOSIT,
                                fetch_meta, fetch_window, row_to_cfg)
from zgb_sim.wfo_helpers import WINDOWS_MAY2, WINDOWS_MAY9, rank_with_p0
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.mt5_accounts import init_account
import pandas as pd

end = datetime.now(timezone.utc)
start = datetime(end.year, end.month, end.day, tzinfo=timezone.utc)

MAGIC_TO_LABEL = {1111: "S1", 2222: "S2", 3333: "S3", 4444: "S4", 5555: "S5", 6666: "S6"}
LABEL_TO_MAGIC = {v: k for k, v in MAGIC_TO_LABEL.items()}


def load_top3(wfo_dir, windows):
    def _read(label):
        for prefix in ("", "p1_"):
            p_is = wfo_dir / f"{prefix}is_{label}.parquet"
            p_oos = wfo_dir / f"{prefix}oos_{label}.parquet"
            if p_is.exists() and p_oos.exists():
                return pd.read_parquet(p_is), pd.read_parquet(p_oos)
        raise FileNotFoundError(label)
    is_per, oos_per = {}, {}
    for label, _, _, _, _ in windows:
        is_per[label], oos_per[label] = _read(label)
    cands = [row_to_cfg(r, "ORB", 3.0) for _, r in oos_per["W1"].iterrows()]
    grid = [row_to_cfg(r, "ORB", 3.0) for _, r in is_per["W1"].iterrows()]
    ranked = rank_with_p0(cands, oos_per, windows, decay_threshold=-0.25,
                          grid_configs=grid, is_per_window=is_per)
    return [ranked[i]["cfg"] for i in range(3)]


def pull_live_deals():
    """Pull every closing deal today, grouped by magic."""
    import MetaTrader5 as mt5
    from zgb_sim.tick_loader import kill_mt5_terminal
    spec = init_account("live")
    try:
        deals = mt5.history_deals_get(start, end) or ()
        by_mag = defaultdict(list)
        for d in deals:
            if d.magic not in MAGIC_TO_LABEL:
                continue
            if d.entry != mt5.DEAL_ENTRY_OUT:
                continue
            by_mag[d.magic].append({
                "ts": pd.Timestamp(d.time, unit="s", tz="UTC"),
                "side": "BUY" if d.type == mt5.DEAL_TYPE_SELL else "SELL",
                # NOTE: closing-deal type is OPPOSITE of position direction (sell to close a buy)
                # so flip back for clarity
                "pos_side": "BUY" if d.type == mt5.DEAL_TYPE_SELL else "SELL",
                "exit_price": d.price,
                "pnl": d.profit + d.swap + d.commission,
                "comment": d.comment,
                "lots": d.volume,
            })
        return by_mag
    finally:
        mt5.shutdown()
        kill_mt5_terminal()


def main():
    prev = load_top3(PREV_WFO_DIR, WINDOWS_MAY2)
    curr = load_top3(CURR_WFO_DIR, WINDOWS_MAY9)
    labels = ["S1", "S2", "S3", "S4", "S5", "S6"]
    cfgs_base = prev + curr

    print(f"\n{'='*110}")
    print(f"  Sim vs Live deal-by-deal investigation  |  {start.date()}  ({start.time()} -> {end.strftime('%H:%M UTC')})")
    print(f"{'='*110}")

    sym_used, m = fetch_meta(None, account="live")
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])
    sym_used, ticks, m1, m5 = fetch_window(sym_used, start, end, SPREAD_LIVE, account="live")
    print(f"  Symbol: {sym_used}  Ticks: {len(ticks):,}")
    print(f"  Tick window: {ticks.ts.min()} -> {ticks.ts.max()}\n")

    live_by_mag = pull_live_deals()

    for lbl, base_cfg in zip(labels, cfgs_base):
        print(f"\n{'='*110}")
        print(f"  STREAM {lbl} (magic {LABEL_TO_MAGIC[lbl]})  Cfg: SL={base_cfg.fixed_sl_pts} "
              f"RR={base_cfg.rr_ratio} HTP={base_cfg.half_tp_ratio}")
        print(f"{'='*110}")

        # SIM
        cfg = row_to_cfg({"range_minutes": base_cfg.range_minutes,
                          "fixed_sl_pts": base_cfg.fixed_sl_pts,
                          "rr_ratio": base_cfg.rr_ratio,
                          "half_tp_ratio": base_cfg.half_tp_ratio,
                          "daily_target_pct": 0.0, "daily_loss_pct": 0.0},
                         lbl, 9.0/6)  # 9% / 6 streams = 1.5%
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        sim_deals = []
        entry_dir = None
        for d in r.deals:
            if d.kind == "entry":
                entry_dir = "BUY" if d.direction == 1 else "SELL"
                continue
            sim_deals.append({
                "ts": pd.Timestamp(d.ts),
                "kind": d.kind, "pos_side": "BUY" if d.direction == 1 else "SELL",
                "exit_price": d.price, "pnl": d.pnl, "lots": d.lots,
            })

        print(f"  --- SIM ({len(sim_deals)} deals) ---")
        if sim_deals:
            print(f"  {'Time (UTC)':<20} {'Side':<5} {'Kind':<6} {'ExitPx':>9} {'Lots':>6} {'PnL$':>9}")
            for d in sorted(sim_deals, key=lambda x: x["ts"]):
                print(f"  {str(d['ts']):<20} {d['pos_side']:<5} {d['kind']:<6} "
                      f"{d['exit_price']:>9.2f} {d['lots']:>6.3f} ${d['pnl']:>+7,.2f}")
        else:
            print("  (none)")

        # LIVE
        magic = LABEL_TO_MAGIC[lbl]
        live_deals = live_by_mag.get(magic, [])
        print(f"\n  --- LIVE ({len(live_deals)} deals) ---")
        if live_deals:
            print(f"  {'Time (UTC)':<20} {'Side':<5} {'ExitPx':>9} {'Lots':>6} {'PnL$':>9}  Comment")
            for d in sorted(live_deals, key=lambda x: x["ts"]):
                print(f"  {str(d['ts']):<20} {d['pos_side']:<5} "
                      f"{d['exit_price']:>9.2f} {d['lots']:>6.3f} ${d['pnl']:>+7,.2f}  {d['comment']}")
        else:
            print("  (none)")

        # diagnostic
        sim_buy = sum(1 for d in sim_deals if d["pos_side"] == "BUY")
        sim_sell = sum(1 for d in sim_deals if d["pos_side"] == "SELL")
        live_buy = sum(1 for d in live_deals if d["pos_side"] == "BUY")
        live_sell = sum(1 for d in live_deals if d["pos_side"] == "SELL")
        print(f"\n  -> sim: {sim_buy} BUY-pos exits, {sim_sell} SELL-pos exits")
        print(f"  -> live: {live_buy} BUY-pos exits, {live_sell} SELL-pos exits")
        if sim_sell > 0 and live_sell == 0:
            print(f"  ** DIVERGENCE: sim filled SELL pending(s), live did NOT.")
        if sim_buy != live_buy:
            print(f"  ** BUY count differs: sim={sim_buy} vs live={live_buy}")


if __name__ == "__main__":
    main()
