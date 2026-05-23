"""orb_fast.simulate_fast must produce the same final_balance as orb.simulate
for each of the 4 fractal config combinations on a small dataset.
"""
import numpy as np
import pandas as pd
import pytest

from zgb_sim.orb import ORBConfig, simulate as simulate_slow
from zgb_sim.orb_fast import simulate_fast
from zgb_sim.scalper_v1 import SymbolMeta

META = SymbolMeta(point=0.01, digits=2, tick_size=0.01, tick_value=0.01,
                  stops_level_pts=0, volume_min=0.1, volume_max=100.0, volume_step=0.1)


def _synth():
    """A 2-hour, slightly volatile dataset producing some fills."""
    n = 120
    rng = np.random.default_rng(42)
    base = 2000.0
    closes = base + np.cumsum(rng.normal(0, 0.5, n))
    highs = closes + np.abs(rng.normal(0.3, 0.2, n))
    lows  = closes - np.abs(rng.normal(0.3, 0.2, n))
    ts5 = pd.date_range("2026-01-05 06:00", periods=n, freq="5min", tz="UTC")
    m5 = pd.DataFrame({"ts": ts5, "open": closes, "high": highs, "low": lows, "close": closes})
    tt = pd.date_range("2026-01-05 06:00", periods=n*5, freq="1min", tz="UTC")
    mid = np.interp(np.arange(n*5), np.arange(n)*5, closes)
    bid = mid - 0.005; ask = mid + 0.005
    ticks = pd.DataFrame({"ts": tt, "bid": bid, "ask": ask})
    m1 = pd.DataFrame({"ts": tt, "open": bid, "high": ask, "low": bid, "close": bid})
    return ticks, m1, m5


@pytest.mark.parametrize("flags", [
    dict(),
    dict(fractal_trail=True),
    dict(fractal_confirm=True),
    dict(fractal_range=True),
])
def test_slow_fast_equivalence(flags):
    ticks, m1, m5 = _synth()
    cfg = ORBConfig(risk_pct=1.0, range_minutes=30, ldn_start_hour=6,
                    ny_enabled=False, min_range_pts=0, max_range_pts=999_999,
                    rr_ratio=2.0, pending_expire_minutes=60, **flags)
    r_slow = simulate_slow(ticks, m5, m1, cfg, META, initial_balance=10_000.0)
    r_fast = simulate_fast(ticks, m5, m1, cfg, META, initial_balance=10_000.0)

    # Sanity: baseline must produce at least 1 trade, else equivalence is vacuous
    if not flags:  # only for the baseline case
        assert len(r_slow.deals) > 0, "Baseline produced no trades — dataset is too quiet to validate equivalence"

    assert r_slow.final_balance == pytest.approx(r_fast.final_balance, abs=0.01), \
        f"slow={r_slow.final_balance:.4f} fast={r_fast.final_balance:.4f} flags={flags}"
