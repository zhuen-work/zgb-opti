"""Compare MT5 server time vs system UTC. Verify if broker is actually UTC.
Also pull a recent tick and order to confirm timestamp interpretation.
"""
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.mt5_accounts import init_account
from zgb_sim.tick_loader import kill_mt5_terminal
import MetaTrader5 as mt5

spec = init_account("live")
try:
    sys_utc = datetime.now(timezone.utc)
    print(f"System UTC now:        {sys_utc}")

    # Pull a small tick stream from "now" to confirm what timestamps MT5 emits
    # symbol_info has tick time — symbol_select first to ensure fresh data
    mt5.symbol_select(spec.symbol, True)
    import time
    time.sleep(2)
    # Force fresh ticks
    _ = mt5.copy_ticks_from(spec.symbol, sys_utc - timedelta(minutes=5), 100, mt5.COPY_TICKS_ALL)
    si = mt5.symbol_info_tick(spec.symbol)
    if si:
        tick_t = datetime.fromtimestamp(si.time, tz=timezone.utc)
        delta = (sys_utc - tick_t).total_seconds()
        print(f"\nLatest tick on {spec.symbol}: {tick_t}  (system_utc - tick = {delta:.1f}s)")
        print(f"  If broker = UTC: this tick is ~{delta:.0f}s old → fine")
        print(f"  If broker offset = +N hours: this tick appears N hours in 'future' relative to UTC")

    # Pull most recent deal and compare its time to wall-clock
    recent_deals = mt5.history_deals_get(sys_utc - timedelta(hours=24), sys_utc) or ()
    if recent_deals:
        latest = max(recent_deals, key=lambda d: d.time)
        deal_t = datetime.fromtimestamp(latest.time, tz=timezone.utc)
        print(f"\nMost recent deal time: {deal_t} (treating as UTC)")
        print(f"  magic={latest.magic} symbol={latest.symbol} price={latest.price}")

    # Also pull all pending orders currently open
    pending = mt5.orders_get() or ()
    print(f"\n{len(pending)} pending orders currently open")
    for o in pending:
        t_setup = datetime.fromtimestamp(o.time_setup, tz=timezone.utc)
        t_exp = datetime.fromtimestamp(o.time_expiration, tz=timezone.utc) if o.time_expiration else None
        age_s = (sys_utc - t_setup).total_seconds()
        print(f"  magic={o.magic} type={o.type} entry={o.price_open:.2f} "
              f"setup={t_setup} (age {age_s:.0f}s)  expire={t_exp}")
finally:
    mt5.shutdown()
    kill_mt5_terminal()
