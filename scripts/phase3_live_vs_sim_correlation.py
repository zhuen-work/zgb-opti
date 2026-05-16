"""Phase 3: Cross-check which session live actually trades.

Pulls all post-fix live deals (since 2026-05-11, the fresh v2 deployment).
Runs two parallel sims using the same v2 cfgs:
  - Current/wrong hours: ldn=7, ny=13 (broker labels = real UTC 04/10)
  - Corrected hours:     ldn=10, ny=16 (broker labels = real UTC 07/13)

Per-day PnL series for all 3, then Pearson correlation. Higher correlation
between live and one variant = that's the session live trades.
"""
from __future__ import annotations
import sys
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from sim_orb_oos_today import (PREV_WFO_DIR, CURR_WFO_DIR, SPREAD_LIVE, DEPOSIT,
                                fetch_meta, fetch_window, row_to_cfg)
from zgb_sim.wfo_helpers import WINDOWS_MAY2, WINDOWS_MAY9, rank_with_p0, to_utc
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.orb import ORBConfig
from zgb_sim.mt5_accounts import init_account
from zgb_sim.tick_loader import kill_mt5_terminal
import MetaTrader5 as mt5
import pandas as pd

# v2 magics
V2_MAGICS = {1111, 2222, 3333, 4444, 5555, 6666}

# Live data window: from when v2 deployed (Mon 2026-05-11) up to "now"
START = datetime(2026, 5, 11, 0, 0, tzinfo=timezone.utc)
END = datetime.now(timezone.utc)


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


def make_cfg(base_cfg, ldn_hour: int, ny_hour: int, risk: float, comment: str) -> ORBConfig:
    return ORBConfig(
        risk_pct=risk,
        range_minutes=base_cfg.range_minutes,
        buffer_pts=0,
        min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=base_cfg.fixed_sl_pts,
        rr_ratio=base_cfg.rr_ratio,
        half_tp_ratio=base_cfg.half_tp_ratio,
        pending_expire_minutes=240,
        daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=ldn_hour,
        ny_enabled=True, ny_start_hour=ny_hour,
        comment=comment,
    )


def pull_live_daily_pnl():
    from zgb_sim.mt5_accounts import get_broker_offset
    spec = init_account("live")
    try:
        # BROKER-TZ FIX: shift bounds + filter strict.
        broker_off = get_broker_offset(spec.symbol)
        mt5_start = START + broker_off
        mt5_end = END + broker_off
        s_epoch = int(mt5_start.timestamp())
        e_epoch = int(mt5_end.timestamp())
        raw = mt5.history_deals_get(mt5_start, mt5_end) or ()
        deals = [d for d in raw if s_epoch <= d.time <= e_epoch]
        by_day = defaultdict(float)
        for d in deals:
            if d.magic not in V2_MAGICS:
                continue
            if d.entry != mt5.DEAL_ENTRY_OUT:
                continue
            day = pd.Timestamp(d.time, unit="s", tz="UTC").date()
            by_day[day] += d.profit + d.swap + d.commission
        return dict(by_day)
    finally:
        mt5.shutdown()
        kill_mt5_terminal()


def sim_daily_pnl(cfgs_base, ldn_hour, ny_hour, ticks, m1, m5, meta, risk_per_stream):
    by_day = defaultdict(float)
    for i, base_cfg in enumerate(cfgs_base, 1):
        cfg = make_cfg(base_cfg, ldn_hour, ny_hour, risk_per_stream, f"S{i}")
        r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        for d in r.deals:
            if d.kind == "entry":
                continue
            day = pd.Timestamp(d.ts).date()
            by_day[day] += d.pnl
    return dict(by_day)


def main() -> int:
    print("=" * 100)
    print(f"  Phase 3: live vs sim daily-PnL correlation")
    print(f"  Window: {START.date()} -> {END.date()}")
    print(f"  Sim variants: current (ldn=7/ny=13 broker = real UTC 04/10)")
    print(f"                corrected (ldn=10/ny=16 broker = real UTC 07/13)")
    print("=" * 100)

    # Live cfgs (same MAY2 prev + MAY9 curr as live setfile uses)
    prev = load_top3(PREV_WFO_DIR, WINDOWS_MAY2)
    curr = load_top3(CURR_WFO_DIR, WINDOWS_MAY9)
    cfgs_base = prev + curr  # S1-3 prev + S4-6 curr
    risk_per_stream = 9.0 / 6  # match live 9pct setfile

    # Pull live daily PnL
    print("\n[live] pulling deals...")
    live_pnl = pull_live_daily_pnl()
    print(f"  live deals span: {min(live_pnl) if live_pnl else 'no data'} -> {max(live_pnl) if live_pnl else '-'}")

    # Pull tick stream once
    print("\n[sim] fetching ticks/bars...")
    sym_used, m = fetch_meta(None, account="live")
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])
    sym_used, ticks, m1, m5 = fetch_window(sym_used, START, END, SPREAD_LIVE, account="live")
    print(f"  ticks: {len(ticks):,}  m5: {len(m5):,}")

    # Sim variants
    print("\n[sim-current] ldn=7/ny=13 broker (= real UTC 04/10)...")
    sim_cur = sim_daily_pnl(cfgs_base, 7, 13, ticks, m1, m5, meta, risk_per_stream)
    print("\n[sim-corrected] ldn=10/ny=16 broker (= real UTC 07/13)...")
    sim_cor = sim_daily_pnl(cfgs_base, 10, 16, ticks, m1, m5, meta, risk_per_stream)

    # Combine
    all_days = sorted(set(live_pnl) | set(sim_cur) | set(sim_cor))
    print(f"\n{'='*100}")
    print(f"  Daily PnL ($ at $10k baseline; live is at $61k account, divide by 6.15 to compare)")
    print(f"{'='*100}")
    print(f"  {'Date':<12} {'Live$':>10} {'LiveNorm$10k':>13} {'Sim_curr$':>10} {'Sim_corr$':>10}")
    live_norm_series = []
    sim_cur_series = []
    sim_cor_series = []
    for d in all_days:
        l = live_pnl.get(d, 0)
        l_norm = l / 6.15  # account scale to $10k
        sc = sim_cur.get(d, 0)
        scc = sim_cor.get(d, 0)
        live_norm_series.append(l_norm)
        sim_cur_series.append(sc)
        sim_cor_series.append(scc)
        print(f"  {str(d):<12} ${l:>+8,.0f} ${l_norm:>+11,.0f} ${sc:>+8,.0f} ${scc:>+8,.0f}")

    # Correlation
    if len(all_days) >= 3:
        live_s = pd.Series(live_norm_series)
        cur_s = pd.Series(sim_cur_series)
        cor_s = pd.Series(sim_cor_series)
        corr_cur = live_s.corr(cur_s)
        corr_cor = live_s.corr(cor_s)
        print(f"\n{'='*100}")
        print(f"  Correlation (Pearson) of daily PnL")
        print(f"{'='*100}")
        print(f"  live vs sim_current  (broker hours 7/13):  r = {corr_cur:+.3f}")
        print(f"  live vs sim_corrected (broker hours 10/16): r = {corr_cor:+.3f}")
        if corr_cor > corr_cur:
            print(f"\n  -> Live tracks CORRECTED sim better. Live is trading real UTC LDN/NY.")
            print(f"  -> Current WFO is wrong session; needs re-run.")
        else:
            print(f"\n  -> Live tracks CURRENT sim better. Live is trading broker hours 7/13 (= real UTC 04/10).")
            print(f"  -> WFO is right; but live EA is firing at the wrong session for what was optimized.")
    else:
        print("\n  (not enough days for correlation; need 3+)")

    # Cumulative comparison
    print(f"\n{'='*100}")
    print(f"  Cumulative PnL totals ({len(all_days)} days)")
    print(f"{'='*100}")
    print(f"  Live (real $):              ${sum(live_pnl.values()):>+10,.0f}")
    print(f"  Live (norm to $10k):        ${sum(live_norm_series):>+10,.0f}")
    print(f"  Sim current (broker 7/13):  ${sum(sim_cur_series):>+10,.0f}")
    print(f"  Sim corrected (broker 10/16): ${sum(sim_cor_series):>+10,.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
