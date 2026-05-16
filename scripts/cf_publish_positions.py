"""Push open-positions snapshot to dt818-console every minute via Task Scheduler.

Lightweight cousin of live_check.py:
  - No deal reconciliation
  - No journal write
  - No projection compare
  - Just: open positions + account snapshot -> Worker /ingest/positions

Designed to be safe to run every 60s during trading hours.

Windows Task Scheduler setup:
  Action:    pythonw.exe
  Argument:  C:\\Users\\Zhu-En\\zgb-opti\\scripts\\cf_publish_positions.py
  Trigger:   daily, repeat every 1 min for 12h, start at 06:00 (UTC+3 broker = NY morning)
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main() -> int:
    import MetaTrader5 as mt5
    from zgb_sim.mt5_accounts import init_account
    from zgb_sim.tick_loader import kill_mt5_terminal
    from zgb_sim.cf_publish import publish_positions

    STREAM_NAMES = {1111: "ORB_S1", 2222: "ORB_S2", 3333: "ORB_S3",
                    4444: "ORB_S4", 5555: "ORB_S5", 6666: "ORB_S6"}
    CONTRACT_SIZE = 100  # XAUUSD.sc on Vantage

    try:
        spec = init_account("live")
        if not mt5.initialize():
            return 1
        ai = mt5.account_info()
        if ai is None:
            return 1
        mt5.symbol_select(spec.symbol, True)
        positions = mt5.positions_get(symbol=spec.symbol) or ()

        now_iso = datetime.now(timezone.utc).isoformat()
        account_snap = {
            "ts": now_iso, "balance": float(ai.balance), "equity": float(ai.equity),
            "margin": float(ai.margin), "margin_free": float(ai.margin_free),
            "open_positions": len(positions),
            "unrealized": sum(float(p.profit) for p in positions),
        }
        position_payloads = [{
            "ticket": int(p.ticket), "magic": int(p.magic),
            "stream": STREAM_NAMES.get(int(p.magic), f"m{p.magic}"),
            "symbol": spec.symbol, "side": "buy" if p.type == 0 else "sell",
            "volume": float(p.volume), "price_open": float(p.price_open),
            "sl": float(p.sl) if p.sl > 0 else None,
            "tp": float(p.tp) if p.tp > 0 else None,
            "unrealized": float(p.profit),
            "sl_usd": (1 if p.type == 0 else -1) * (float(p.sl) - float(p.price_open)) * CONTRACT_SIZE * float(p.volume) if p.sl > 0 else None,
            "tp_usd": (1 if p.type == 0 else -1) * (float(p.tp) - float(p.price_open)) * CONTRACT_SIZE * float(p.volume) if p.tp > 0 else None,
            "comment": (p.comment or None),
            "ts_open": datetime.fromtimestamp(p.time, tz=timezone.utc).isoformat(),
        } for p in positions]
        ok = publish_positions(position_payloads, account=account_snap)
        return 0 if ok else 1
    finally:
        try:
            mt5.shutdown()
        except Exception:
            pass
        kill_mt5_terminal()


if __name__ == "__main__":
    sys.exit(main())
