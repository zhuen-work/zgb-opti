"""V1: after fill, trail SL to most recent opposite-side fractal that's more
favorable than current SL. Never moves SL adversely.
"""
import numpy as np
import pandas as pd

from zgb_sim.orb import ORBConfig, simulate
from zgb_sim.scalper_v1 import SymbolMeta

META = SymbolMeta(point=0.01, digits=2, tick_size=0.01, tick_value=0.01,
                  stops_level_pts=0, volume_min=0.1, volume_max=100.0, volume_step=0.1)


def test_v1_off_matches_baseline():
    n = 60
    highs = [2000 + 0.1*(i%5) for i in range(n)]
    lows  = [1999 + 0.1*(i%5) for i in range(n)]
    ts5 = pd.date_range("2026-01-05 06:00", periods=n, freq="5min", tz="UTC")
    m5 = pd.DataFrame({"ts": ts5, "open": highs, "high": highs, "low": lows, "close": highs})
    tt = pd.date_range("2026-01-05 06:00", periods=n*5, freq="1min", tz="UTC")
    bid = np.full(len(tt), 2000.0); ask = np.full(len(tt), 2000.01)
    ticks = pd.DataFrame({"ts": tt, "bid": bid, "ask": ask})
    m1 = pd.DataFrame({"ts": tt, "open": bid, "high": ask, "low": bid, "close": bid})

    cfg_base = ORBConfig(risk_pct=1.0, range_minutes=30, ldn_start_hour=6,
                         ny_enabled=False, min_range_pts=0, max_range_pts=999_999)
    cfg_off = ORBConfig(risk_pct=1.0, range_minutes=30, ldn_start_hour=6,
                        ny_enabled=False, min_range_pts=0, max_range_pts=999_999,
                        fractal_trail=False)
    r_base = simulate(ticks, m5, m1, cfg_base, META, initial_balance=10_000.0)
    r_off = simulate(ticks, m5, m1, cfg_off, META, initial_balance=10_000.0)
    assert r_base.final_balance == r_off.final_balance


def test_v1_on_ratchets_sl_to_confirmed_down_fractal_for_buy():
    """BUY fills at 2000.2.  A down-fractal at bar 8 (low=2000.4, above entry)
    confirms at bar 10 (06:50). V1 must move SL from original 1999.2 to 2000.4.
    When bid dips to 2000.3 at bar 12, the trailed SL fires -> small profit.
    Baseline (no V1) keeps SL at 1999.2 so the dip doesn't stop it out;
    price then recovers to bid=2002.2 and hits TP -> larger profit.
    """
    # Bar layout (M5, 5-min bars starting 06:00 UTC):
    #   0-5  : range bars  high=2000.2  low=1999.2  => range=100pt
    #          Session fires at 06:30: buy_entry=2000.2, SL=1999.2, TP=2002.2 (RR=2)
    #   6    : break bar   high=2000.6  low=2000.50
    #   7    :             high=2000.6  low=2000.60
    #   8    :             high=2000.6  low=2000.40  <- down-fractal centre (strictly lowest 6..10)
    #   9    :             high=2000.6  low=2000.60
    #   10   :             high=2000.6  low=2000.60  <- fractal confirms here (bar 8 + half=2)
    #   11   :             high=2000.6  low=2000.60
    #   12   :             high=2000.3  low=2000.30  <- bid dips; V1 SL=2000.4 fires
    #   13+  :             high=2002.2  low=2002.20  <- baseline TP=2002.2 hit (bid=2002.2)
    highs = [2000.2]*6 + [2000.6, 2000.6, 2000.6, 2000.6, 2000.6, 2000.6, 2000.3, 2002.2] + [2002.2]*20
    lows  = [1999.2]*6 + [2000.5, 2000.6, 2000.4, 2000.6, 2000.6, 2000.6, 2000.3, 2002.2] + [2002.2]*20
    n = len(highs)
    ts5 = pd.date_range("2026-01-05 06:00", periods=n, freq="5min", tz="UTC")
    m5 = pd.DataFrame({"ts": ts5, "open": highs, "high": highs, "low": lows, "close": highs})

    # Ticks: 5 per bar; bid=low, ask=high for that bar.
    tt = pd.date_range("2026-01-05 06:00", periods=n*5, freq="1min", tz="UTC")
    bid = np.empty(len(tt)); ask = np.empty(len(tt))
    for i in range(len(tt)):
        bi = min(i // 5, n - 1)
        bid[i] = lows[bi]
        ask[i] = highs[bi]

    # Tick 30 (06:30, session-fire tick):
    # Force ask=2000.1 < buy_entry=2000.2 so the pending IS placed.
    bid[30] = 2000.0; ask[30] = 2000.1
    # Ticks 31-34: ask=2000.2 => BuyStop fills at 2000.2
    bid[31:35] = 2000.1; ask[31:35] = 2000.2

    # Bar 12 (ticks 60-64): bid dips to 2000.3 -> V1 SL=2000.4 triggers (profit lock).
    bid[60:65] = 2000.3; ask[60:65] = 2000.3

    # Bar 13+ (ticks 65+): bid=2002.2 => baseline TP=2002.2 triggers (bid >= tp).
    bid[65:] = 2002.2; ask[65:] = 2002.2

    ticks = pd.DataFrame({"ts": tt, "bid": bid, "ask": ask})
    m1 = pd.DataFrame({"ts": tt, "open": bid, "high": ask, "low": bid, "close": bid})

    cfg_base = ORBConfig(risk_pct=1.0, range_minutes=30, ldn_start_hour=6,
                         ny_enabled=False, min_range_pts=0, max_range_pts=999_999,
                         rr_ratio=2.0, pending_expire_minutes=60)
    cfg_v1 = ORBConfig(risk_pct=1.0, range_minutes=30, ldn_start_hour=6,
                       ny_enabled=False, min_range_pts=0, max_range_pts=999_999,
                       rr_ratio=2.0, pending_expire_minutes=60,
                       fractal_trail=True, fractal_width=5)
    r_base = simulate(ticks, m5, m1, cfg_base, META, initial_balance=10_000.0)
    r_v1   = simulate(ticks, m5, m1, cfg_v1,   META, initial_balance=10_000.0)

    pnl_base = sum(d.pnl for d in r_base.deals if d.kind != "entry")
    pnl_v1   = sum(d.pnl for d in r_v1.deals   if d.kind != "entry")

    # Baseline: position survives the dip and hits TP at 2002.2 -> full profit.
    # V1: trailed SL=2000.4 fires on bid=2000.3 -> smaller but positive profit.
    assert pnl_base > pnl_v1, (
        f"Expected baseline pnl ({pnl_base:.4f}) > V1 pnl ({pnl_v1:.4f})"
    )
    assert pnl_v1 > 0, (
        f"Expected V1 to lock a positive profit but got {pnl_v1:.4f}"
    )
