"""Pre-warm M1 + M5 bars cache for XAUUSD over WINDOWS_MAY23 range so the
single-threaded WFO doesn't have to call MT5 from worker subprocesses.
"""
from __future__ import annotations
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import load_bars, kill_mt5_terminal
from zgb_sim.mt5_accounts import init_account
init_account("sim")  # sim acct exposes XAUUSD (live uses XAUUSD.sc)

START = datetime(2026, 2, 10, tzinfo=timezone.utc)  # cover MAY9 W1 IS start (Feb 21) - 2-day pad
END   = datetime(2026, 5, 23, tzinfo=timezone.utc)

for tf in ("M1", "M5"):
    print(f"Loading {tf} bars XAUUSD {START.date()} -> {END.date()}...")
    df = load_bars("XAUUSD", tf, START, END)
    print(f"  {tf}: {len(df):,} rows  range {df['ts'].min()} -> {df['ts'].max()}")

kill_mt5_terminal()
print("done")
