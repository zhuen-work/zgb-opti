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
