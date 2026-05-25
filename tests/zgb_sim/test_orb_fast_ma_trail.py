"""Safety net: ma_trail=False must produce byte-identical results to baseline.

The golden values were captured on 2026-05-19 to 2026-05-20 UTC with v6 setfile
S1 parameters (range_minutes=90, fixed_sl_pts=550, rr_ratio=4.0,
half_tp_ratio=0.2, pending_expire_minutes=720), fractal_confirm=True,
fractal_width=5, sim spread 30pt, $10k deposit on sim account (XAUUSD).

Date window chosen: 2026-05-19 00:00 UTC -> 2026-05-20 00:00 UTC (Monday).
Tick data was confirmed available on that date (689,875 ticks, 1,129 M5 bars).

API note: simulate_fast signature is (ticks, m5_bars, m1_bars, cfg, meta,
initial_balance). The plan template had args in a different order; this file
uses the actual signature. fetch_window() from sim_orb_oos_today handles MT5
init, shutdown, and kill_mt5_terminal() internally via its finally block.

To recapture golden values:
    cd /path/to/zgb-opti
    PYTHONPATH=src python -c "
    import sys; sys.path.insert(0, 'scripts')
    from sim_orb_oos_today import fetch_window
    from sim_orb_oos_today_hedge_v6 import parse_v6_setfile
    from zgb_sim.orb import ORBConfig
    from zgb_sim.orb_fast import simulate_fast
    from zgb_sim.scalper_v1 import SymbolMeta
    from datetime import datetime, timezone
    from pathlib import Path
    cfg_data = parse_v6_setfile(Path('configs/sets/dt818_pro_v6_9pct_may23_may16.set'))
    s1 = cfg_data['streams'][0]
    start = datetime(2026, 5, 19, tzinfo=timezone.utc)
    end   = datetime(2026, 5, 20, tzinfo=timezone.utc)
    sym, ticks, m1, m5 = fetch_window(None, start, end, spread_pts=30, account='sim')
    meta = SymbolMeta(point=0.01, tick_size=0.01, tick_value=1.0, stops_level_pts=0,
                      volume_min=0.01, volume_max=100.0, volume_step=0.01, digits=2)
    cfg = ORBConfig(risk_pct=1.5, range_minutes=s1['range_minutes'],
                    fixed_sl_pts=s1['fixed_sl_pts'], rr_ratio=s1['rr_ratio'],
                    half_tp_ratio=s1['half_tp_ratio'],
                    pending_expire_minutes=s1['pending_expire_minutes'],
                    fractal_confirm=True, fractal_width=5, ma_trail=False)
    r = simulate_fast(ticks, m5, m1, cfg, meta, 10_000.0)
    print(f'GOLDEN: net={r.net_profit:.4f} dd={r.max_drawdown:.4f} trades={r.trades}')
    "
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))


@pytest.mark.integration
def test_ma_trail_off_matches_baseline_golden():
    from sim_orb_oos_today import fetch_window
    from sim_orb_oos_today_hedge_v6 import parse_v6_setfile
    from zgb_sim.orb import ORBConfig
    from zgb_sim.orb_fast import simulate_fast
    from zgb_sim.scalper_v1 import SymbolMeta

    cfg_data = parse_v6_setfile(ROOT / "configs/sets/dt818_pro_v6_9pct_may23_may16.set")
    s1 = cfg_data["streams"][0]

    start = datetime(2026, 5, 19, tzinfo=timezone.utc)
    end = datetime(2026, 5, 20, tzinfo=timezone.utc)
    # fetch_window handles MT5 init, shutdown, and kill_mt5_terminal() internally.
    sym, ticks, m1, m5 = fetch_window(None, start, end, spread_pts=30, account="sim")

    meta = SymbolMeta(point=0.01, tick_size=0.01, tick_value=1.0, stops_level_pts=0,
                      volume_min=0.01, volume_max=100.0, volume_step=0.01, digits=2)
    cfg = ORBConfig(risk_pct=1.5, range_minutes=s1["range_minutes"],
                    fixed_sl_pts=s1["fixed_sl_pts"], rr_ratio=s1["rr_ratio"],
                    half_tp_ratio=s1["half_tp_ratio"],
                    pending_expire_minutes=s1["pending_expire_minutes"],
                    fractal_confirm=True, fractal_width=5, ma_trail=False)
    # Note: simulate_fast signature is (ticks, m5_bars, m1_bars, cfg, meta, initial_balance)
    r = simulate_fast(ticks, m5, m1, cfg, meta, 10_000.0)

    # Golden values captured 2026-05-25 on window 2026-05-19 -> 2026-05-20 UTC.
    GOLDEN_NET    = 200.2000
    GOLDEN_DD     = 231.0000
    GOLDEN_TRADES = 6

    assert r.trades == GOLDEN_TRADES, f"trades drifted: {r.trades} vs {GOLDEN_TRADES}"
    assert r.net_profit == pytest.approx(GOLDEN_NET, abs=0.01), \
        f"net drifted: {r.net_profit} vs {GOLDEN_NET}"
    assert r.max_drawdown == pytest.approx(GOLDEN_DD, abs=0.01), \
        f"dd drifted: {r.max_drawdown} vs {GOLDEN_DD}"
