"""V2: pending stops arm only after a same-direction fractal confirms past the
break level. Baseline arms immediately at range close.
"""
import numpy as np
import pandas as pd

from zgb_sim.orb import ORBConfig, simulate
from zgb_sim.scalper_v1 import SymbolMeta

META = SymbolMeta(point=0.01, digits=2, tick_size=0.01, tick_value=0.01,
                  stops_level_pts=0, volume_min=0.1, volume_max=100.0, volume_step=0.1)


def test_v2_off_matches_baseline():
    n = 60
    highs = [2000 + 0.1*(i%5) for i in range(n)]
    lows  = [1999 + 0.1*(i%5) for i in range(n)]
    ts5 = pd.date_range("2026-01-05 06:00", periods=n, freq="5min", tz="UTC")
    m5 = pd.DataFrame({"ts": ts5, "open": highs, "high": highs, "low": lows, "close": highs})
    tt = pd.date_range("2026-01-05 06:00", periods=n*5+60, freq="1min", tz="UTC")
    bid = np.full(len(tt), 2000.0); ask = np.full(len(tt), 2000.01)
    ticks = pd.DataFrame({"ts": tt, "bid": bid, "ask": ask})
    m1 = pd.DataFrame({"ts": tt, "open": bid, "high": ask, "low": bid, "close": bid})

    cfg_off = ORBConfig(risk_pct=1.0, range_minutes=30, ldn_start_hour=6,
                        ny_enabled=False, min_range_pts=0, max_range_pts=999_999,
                        fractal_confirm=False)
    cfg_base = ORBConfig(risk_pct=1.0, range_minutes=30, ldn_start_hour=6,
                         ny_enabled=False, min_range_pts=0, max_range_pts=999_999)
    r_off = simulate(ticks, m5, m1, cfg_off, META, initial_balance=10_000.0)
    r_base = simulate(ticks, m5, m1, cfg_base, META, initial_balance=10_000.0)
    # Use the actual SimResult field name (final_balance, not balance)
    assert r_off.final_balance == r_base.final_balance
    assert len(r_off.deals) == len(r_base.deals)


def test_v2_on_blocks_fill_until_fractal_confirms():
    """Baseline fills a BuyStop at the post-range spike. V2 must suppress the
    fill until a same-direction (up) fractal confirms above the break price.
    """
    n_bars = 60
    # Range bars 0..5: flat highs=2001, lows=1999 → BuyStop at 2001, SellStop at 1999.
    # Post-range bars 6..n: trend rises monotonically (NO local maxima → no up-fractals)
    base_highs = [2001.0]*6 + [2002.0 + 0.01*i for i in range(n_bars - 6)]
    base_lows  = [1999.0]*6 + [2001.5 + 0.01*i for i in range(n_bars - 6)]
    ts5 = pd.date_range("2026-01-05 06:00", periods=n_bars, freq="5min", tz="UTC")
    m5 = pd.DataFrame({"ts": ts5, "open": base_highs, "high": base_highs, "low": base_lows, "close": base_highs})

    n_ticks = n_bars*5 + 30
    tt = pd.date_range("2026-01-05 06:00", periods=n_ticks, freq="1min", tz="UTC")
    bid = np.full(n_ticks, 1999.5); ask = np.full(n_ticks, 1999.51)
    # At minute 35 (post 30-min range), price spikes to 2002 (would fill BuyStop@2001)
    bid[35:] = 2002.0; ask[35:] = 2002.01
    ticks = pd.DataFrame({"ts": tt, "bid": bid, "ask": ask})
    m1 = pd.DataFrame({"ts": tt, "open": bid, "high": ask, "low": bid, "close": bid})

    cfg_base = ORBConfig(risk_pct=1.0, range_minutes=30, ldn_start_hour=6,
                         ny_enabled=False, min_range_pts=0, max_range_pts=999_999)
    cfg_v2 = ORBConfig(risk_pct=1.0, range_minutes=30, ldn_start_hour=6,
                       ny_enabled=False, min_range_pts=0, max_range_pts=999_999,
                       fractal_confirm=True, fractal_width=5,
                       pending_expire_minutes=30)
    r_base = simulate(ticks, m5, m1, cfg_base, META, initial_balance=10_000.0)
    r_v2 = simulate(ticks, m5, m1, cfg_v2, META, initial_balance=10_000.0)
    entries_base = [d for d in r_base.deals if d.kind == "entry"]
    entries_v2 = [d for d in r_v2.deals if d.kind == "entry"]
    assert len(entries_base) >= 1, "Baseline should fill the BuyStop on the spike"
    assert len(entries_v2) == 0, "V2 should suppress: monotonic post-range = no up-fractal"
