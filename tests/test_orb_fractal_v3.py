"""V3: range H/L derived from confirmed fractals inside the range window."""
import numpy as np
import pandas as pd

from zgb_sim.orb import ORBConfig, simulate
from zgb_sim.scalper_v1 import SymbolMeta


META = SymbolMeta(
    point=0.01, digits=2, tick_size=0.01, tick_value=0.01,
    stops_level_pts=0, volume_min=0.1, volume_max=100.0, volume_step=0.1,
)


def _make_inputs(highs, lows, base_price=2000.0):
    """Build M5 bars (06:00 UTC start, 5min freq) and matching flat-spread ticks."""
    n = len(highs)
    ts5 = pd.date_range("2026-01-05 06:00", periods=n, freq="5min", tz="UTC")
    m5 = pd.DataFrame({"ts": ts5, "open": [base_price]*n, "high": highs, "low": lows, "close": [base_price]*n})
    tt = pd.date_range("2026-01-05 06:00", periods=n*5 + 600, freq="1min", tz="UTC")
    bid = np.full(len(tt), base_price - 0.005)
    ask = np.full(len(tt), base_price + 0.005)
    ticks = pd.DataFrame({"ts": tt, "bid": bid, "ask": ask})
    m1 = pd.DataFrame({"ts": tt, "open": bid, "high": ask, "low": bid, "close": bid})
    return ticks, m1, m5


def test_v3_off_matches_baseline_when_flag_false():
    highs = [2000 + i*0.1 for i in range(20)]
    lows  = [2000 - i*0.1 for i in range(20)]
    ticks, m1, m5 = _make_inputs(highs, lows)
    cfg_base = ORBConfig(risk_pct=1.0, range_minutes=30, ldn_start_hour=6,
                          ny_enabled=False, min_range_pts=0, max_range_pts=999_999)
    cfg_v3off = ORBConfig(risk_pct=1.0, range_minutes=30, ldn_start_hour=6,
                          ny_enabled=False, min_range_pts=0, max_range_pts=999_999,
                          fractal_range=False)
    r1 = simulate(ticks, m5, m1, cfg_base, META, initial_balance=10_000.0)
    r2 = simulate(ticks, m5, m1, cfg_v3off, META, initial_balance=10_000.0)
    assert r1.final_balance == r2.final_balance


def test_v3_on_skips_session_with_no_fractal():
    # Monotonically rising bars — no up-fractal in the range window.
    # With width=5, only first ~4 bars of a 30-min (=6-bar) range can host a fractal,
    # and monotonic rise has no local maxima -> 0 fractals -> session skipped.
    highs = [2000 + i*0.1 for i in range(30)]
    lows  = [1999 + i*0.1 for i in range(30)]
    ticks, m1, m5 = _make_inputs(highs, lows)
    cfg = ORBConfig(risk_pct=1.0, range_minutes=30, ldn_start_hour=6,
                    ny_enabled=False, min_range_pts=0, max_range_pts=999_999,
                    fractal_range=True, fractal_width=5)
    r = simulate(ticks, m5, m1, cfg, META, initial_balance=10_000.0)
    entries = [d for d in r.deals if d.kind == "entry"]
    assert len(entries) == 0


def test_v3_on_runs_without_error_when_fractal_present():
    # An up-fractal at bar 2 and down-fractal at bar 3 inside a 6-bar range.
    # With width=5, both confirm at bars 4 (=2+2) and 5 (=3+2) — inside range.
    # Goal: V3 path executes without crashing and produces a SimResult.
    highs = [2000.0, 2000.5, 2002.0, 2000.3, 2000.2, 2000.1] + [2000.5]*24
    lows  = [1999.0, 1999.5, 1999.7, 1997.0, 1999.6, 1999.8] + [1999.5]*24
    ticks, m1, m5 = _make_inputs(highs, lows)
    cfg = ORBConfig(risk_pct=1.0, range_minutes=30, ldn_start_hour=6,
                    ny_enabled=False, min_range_pts=0, max_range_pts=999_999,
                    fractal_range=True, fractal_width=5)
    r = simulate(ticks, m5, m1, cfg, META, initial_balance=10_000.0)
    # Flat ticks → no fills, but sim ran cleanly through V3 path
    assert r.final_balance == 10_000.0
