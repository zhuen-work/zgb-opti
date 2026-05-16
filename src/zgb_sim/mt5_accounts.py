"""MT5 account registry — maps logical purpose to (login, server, symbol).

Use init_account('live') / init_account('sim') instead of bare mt5.initialize()
so scripts pull from the right account. Credentials must already be saved in the
MT5 terminal; this only specifies which saved account to connect with.
"""
from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(frozen=True)
class AccountSpec:
    login: int
    server: str
    symbol: str
    purpose: str
    description: str


ACCOUNTS: dict[str, AccountSpec] = {
    "live": AccountSpec(
        login=21478621,
        server="VantageInternational-Live 3",
        symbol="XAUUSD.sc",
        purpose="live",
        description="Production trading account (updated 2026-05-12); what live_check + EA run against. Was 23836999 on Live 11 before. Symbol is XAUUSD.sc (account only has .sc variant; sim account on same server has plain XAUUSD).",
    ),
    "sim": AccountSpec(
        login=18912087,
        server="VantageInternational-Live 3",
        symbol="XAUUSD",
        purpose="sim",
        description="Payment — sim/research account; matches all parquet caches + WFO outputs",
    ),
}


def init_account(purpose: str, wait_connected_s: int = 30):
    """Initialize MT5 against the named account using saved credentials.

    Returns the AccountSpec on success. Caller is responsible for mt5.shutdown()
    + kill_mt5_terminal() in a finally block.
    """
    if purpose not in ACCOUNTS:
        raise ValueError(f"Unknown account purpose {purpose!r}. Known: {list(ACCOUNTS)}")
    spec = ACCOUNTS[purpose]

    import MetaTrader5 as mt5
    from .tick_loader import kill_mt5_terminal
    kill_mt5_terminal(); time.sleep(2)
    if not mt5.initialize(login=spec.login, server=spec.server):
        err = mt5.last_error()
        kill_mt5_terminal()
        raise RuntimeError(f"MT5 init failed for {purpose} acct {spec.login}: {err}")

    # Wait for connection + verify the account that came up matches what we asked for.
    for _ in range(wait_connected_s):
        ti = mt5.terminal_info()
        if ti and ti.connected:
            break
        time.sleep(1)
    ai = mt5.account_info()
    if ai is None:
        raise RuntimeError(f"MT5 connected but no account info for {purpose}")
    if ai.login != spec.login:
        raise RuntimeError(
            f"Account mismatch: asked for {spec.login} ({purpose}), got {ai.login}. "
            "Saved credentials may be missing — open the account once in the terminal first."
        )
    mt5.symbol_select(spec.symbol, True)
    return spec


def get_broker_offset(symbol: str | None = None,
                        poll_max_seconds: float = 8.0,
                        fresh_tick_window_s: int = 60):
    """Auto-detect current broker timezone offset vs real UTC.

    Returns a `timedelta` representing (broker_wall_clock - real_UTC). Vantage =
    UTC+3 in summer (around late Mar -> late Oct), UTC+2 in winter. Cannot be
    hardcoded across DST boundaries.

    Detection strategy (robust to stale ticks during quiet markets):
      Tick lag silently understates the offset (a 60-min-old tick observed
      against real-now gives "offset = true_offset - 1h"). To handle this, we
      poll multiple liquid symbols repeatedly for up to `poll_max_seconds`,
      take the MAX observed (tick.time - real_now), and ACCEPT the value once
      it's within `fresh_tick_window_s` of being integer-aligned (proxy for
      "this tick is fresh enough that the true offset is the rounded value").

      Fallback: if we never see a fresh tick, return the highest observation
      ROUNDED TO NEAREST and emit a warning. Sanity-check result is in
      [-1, +12] hours; raise on out-of-range.

    Caller's responsibility: MT5 must already be initialized (e.g. via
    init_account()). Pass `symbol` if you know the live symbol; otherwise tries
    common ones first.
    """
    import time
    import math
    import MetaTrader5 as mt5
    from datetime import datetime, timezone, timedelta

    candidates = [symbol] if symbol else []
    candidates += ["XAUUSD.sc", "XAUUSD", "EURUSD", "GBPUSD", "USDJPY"]
    candidates = [s for s in candidates if s]
    # Pre-select symbols once
    for sym in candidates:
        try:
            mt5.symbol_select(sym, True)
        except Exception:
            pass

    poll_start = time.time()
    best_offset_s = None
    while time.time() - poll_start < poll_max_seconds:
        real_now_epoch = int(datetime.now(timezone.utc).timestamp())
        for sym in candidates:
            try:
                tick = mt5.symbol_info_tick(sym)
                if tick is None or tick.time <= 0:
                    continue
                offset_s = tick.time - real_now_epoch
                if best_offset_s is None or offset_s > best_offset_s:
                    best_offset_s = offset_s
                # Accept early if this tick gives a near-integer offset
                # (= within fresh_tick_window of integer hours).
                fractional = offset_s - round(offset_s / 3600) * 3600
                if abs(fractional) <= fresh_tick_window_s:
                    return timedelta(hours=round(offset_s / 3600))
            except Exception:
                continue
        time.sleep(0.5)

    if best_offset_s is None:
        raise RuntimeError(
            "Could not detect broker offset — no candidate symbol returned a "
            "tick during polling. Pass symbol explicitly to get_broker_offset(symbol)."
        )
    # No fresh tick observed in poll window. Use the BEST (most-positive) value
    # rounded to nearest hour. May be wrong by 1h if all ticks were stale.
    hours = round(best_offset_s / 3600)
    fractional = best_offset_s - hours * 3600
    print(f"  [warn] get_broker_offset: no fresh tick after {poll_max_seconds}s. "
          f"Best observed = {best_offset_s/3600:+.3f}h, using {hours}h. "
          f"If this seems wrong, broker market may be quiet — retry during active hours.")
    if hours < -1 or hours > 12:
        raise RuntimeError(
            f"Detected broker offset {hours}h is out of plausible range [-1, +12]."
        )
    return timedelta(hours=hours)
