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


def test_v1_ignores_pre_entry_fractals():
    """Regression: pre-entry fractals must NOT influence trail SL.

    Without the fix, sl_trail_idx is initialized to 0, so the first trail tick
    after fill sweeps the entire fractal history back to bar 0. A pre-entry
    up-fractal (SELL scenario) with high below the current SL would immediately
    ratchet the SL down to that level, turning a later normal stop-out into a
    phantom profit.

    Setup:
    - Bars 0-4 (06:10-06:30 UTC): form up-fractal at bar 2 (high=1985),
      confirmed at bar 4 (width=5 needs 2 bars each side).
    - Bars 5-9 (06:35-06:55): flat transition.
    - Bars 10-15 (07:00-07:25): range window [07:00, 07:30).
      range_high=2002, range_low=2000 -> sell_entry=2000, SL=2002, TP=1996 (RR=2).
    - Bar 16 (07:30): session fires. bar low=2000.5 > sell_entry -> SellStop@2000 placed.
    - Bar 17 (07:35): bid=1999 <= 2000 -> SellStop fills at 2000.
      BUG: trail from idx=0 finds up-fractal at 06:30 (high=1985 < SL=2002),
           ratchets sell SL from 2002 down to 1985. Price recovers above 1985 ->
           position "stops out" as SL with pnl > 0 (bogus profit).
      FIX: trail indices start past entry ts (07:35). No post-entry fractals formed
           yet -> SL stays at 2002. Price falls to TP=1996 -> legitimate profit.
    """
    # 35 M5 bars starting 06:10 UTC (Monday 2026-01-05)
    # bar 0=06:10, ..., bar 4=06:30 (fractal confirmed)
    # bar 10=07:00 (range start), bar 15=07:25 (last range bar)
    # bar 16=07:30 (session fires, bid=2000.5 > sell_entry=2000)
    # bar 17=07:35 (fill, bid=1999 <= 2000)
    # bars 18-20: price drops toward TP=1996
    highs = ([1980.0, 1982.0, 1985.0, 1982.0, 1980.0]   # 0-4: up-fractal at bar 2
           + [1980.0]*5                                    # 5-9: flat
           + [2002.0]*6                                    # 10-15: range
           + [2001.0]                                      # 16: session-fire bar (bid=2000.5)
           + [1999.0]                                      # 17: fill bar
           + [1997.0, 1996.5, 1996.0]                     # 18-20: toward TP
           + [1996.0]*14)
    lows  = ([1978.0, 1980.0, 1983.0, 1980.0, 1978.0]
           + [1978.0]*5
           + [2000.0]*6
           + [2000.5]                                      # 16: low=2000.5 > 2000 -> placed
           + [1999.0]                                      # 17: bid=1999 fills
           + [1997.0, 1996.5, 1996.0]
           + [1996.0]*14)
    n = len(highs)
    assert n == 35

    ts5 = pd.date_range("2026-01-05 06:10", periods=n, freq="5min", tz="UTC")
    m5 = pd.DataFrame({"ts": ts5, "open": lows, "high": highs, "low": lows, "close": lows})

    # 5 ticks per bar (1-min ticks)
    tt = pd.date_range("2026-01-05 06:10", periods=n*5, freq="1min", tz="UTC")
    bid = np.empty(len(tt))
    ask = np.empty(len(tt))
    for i in range(len(tt)):
        bi = min(i // 5, n - 1)
        bid[i] = lows[bi]
        ask[i] = highs[bi]
    # bar 17 ticks (85-89): force bid below sell_entry to trigger fill
    bid[85:90] = 1999.0
    ask[85:90] = 1999.0

    ticks = pd.DataFrame({"ts": tt, "bid": bid, "ask": ask})
    m1 = pd.DataFrame({"ts": tt, "open": bid, "high": ask, "low": bid, "close": bid})

    cfg_v1 = ORBConfig(
        risk_pct=1.0, range_minutes=30, ldn_start_hour=7, ldn_enabled=True,
        ny_enabled=False, min_range_pts=0, max_range_pts=999_999,
        rr_ratio=2.0, pending_expire_minutes=120,
        fractal_trail=True, fractal_width=5,
    )

    r = simulate(ticks, m5, m1, cfg_v1, META, initial_balance=10_000.0)

    # With the bug: pre-entry up-fractal (high=1985, confirmed at 06:30) is found on
    # first post-fill trail scan. Trail ratchets sell SL from 2002 -> 1985. When price
    # later touches 1985, position "stops out" at a profit (sell entry 2000 -> stop 1985
    # = +1500pt gain). The trade would close as kind='sl' with pnl > 0.
    #
    # With the fix: trail indices start at the first fractal after entry (07:35).
    # No post-entry fractals exist in this scenario -> SL stays at 2002.
    # Price falls to TP=1996 and closes correctly as kind='tp' with positive pnl.

    closing_deals = [d for d in r.deals if d.kind in ("sl", "tp", "other")]
    assert len(closing_deals) == 1, (
        f"Expected 1 closing deal, got {len(closing_deals)}: {closing_deals}"
    )
    close = closing_deals[0]

    # Must close as TP at 1996, NOT as SL at 1985 (which would indicate the bug)
    assert close.kind == "tp", (
        f"Expected TP close at 1996, got kind={close.kind} price={close.price:.2f} "
        f"pnl={close.pnl:.4f}. If kind='sl' with pnl > 0, the pre-entry fractal "
        f"contamination bug is still active."
    )
    assert abs(close.price - 1996.0) < 0.01, (
        f"Expected TP at 1996.0, got {close.price:.2f}"
    )

    # Unit-level check: Position.entry_ts_ns field must exist
    from zgb_sim.scalper_v1 import Position
    p = Position(direction=1, entry_price=2000.0, sl=1995.0, tp=2010.0, lots=0.1)
    assert hasattr(p, "entry_ts_ns"), "Position.entry_ts_ns field missing"
    assert p.entry_ts_ns == 0, "Default entry_ts_ns should be 0"
