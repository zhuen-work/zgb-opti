"""Parse MT5 .htm backtest report into a normalized deals DataFrame.

Usage:
    python scripts/sim_parse_mt5_deals.py <path-to-report.htm>
    -> writes <report>.deals.csv and <report>.orders.csv next to it

Or import parse_mt5_deals(path) -> (deals_df, orders_df)
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd


# MT5 reports are UTF-16-LE.
def _read(path: Path) -> str:
    return path.read_text(encoding="utf-16-le", errors="ignore")


def _extract_rows(html: str):
    for row in re.findall(r"<tr[^>]*>(.+?)</tr>", html, re.DOTALL):
        cells = [
            re.sub(r"<[^>]+>", "", c).strip()
            for c in re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        ]
        yield cells


def _num(s: str) -> float:
    """Parse MT5-formatted number: strips non-breaking spaces and regular spaces."""
    s = s.replace(" ", "").replace(" ", "").strip()
    if not s:
        return 0.0
    return float(s)


def parse_mt5_deals(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (deals_df, orders_df).

    deals_df columns: ts (datetime64), direction (in/out), lots, price, order_id,
                      profit, balance, comment, tag (tp/sl/other for 'out' rows)

    orders_df columns: ts_open (datetime64), order_id, type (buy_stop/sell_stop/...),
                       lots_open, lots_filled, price, sl, tp, ts_close (datetime64),
                       state (filled/canceled), comment
    """
    html = _read(path)

    deals = []
    orders = []

    for cells in _extract_rows(html):
        n = len(cells)
        if n == 13:
            # Deal row: time, deal_id, symbol, type, direction, volume, price, order_id,
            #           commission, swap, profit, balance, comment
            if "XAUUSD" not in (cells[2] if len(cells) > 2 else ""):
                continue
            try:
                ts = pd.to_datetime(cells[0])
            except Exception:
                continue
            direction = cells[4].lower()   # 'in' or 'out'
            lots = _num(cells[5])
            price = _num(cells[6])
            order_id = cells[7]
            profit = _num(cells[10])
            balance = _num(cells[11])
            comment = cells[12]
            tag = "entry"
            if direction == "out":
                c = comment.lower()
                if c.startswith("tp "):
                    tag = "tp"
                elif c.startswith("sl "):
                    tag = "sl"
                else:
                    tag = "other"
            deals.append({
                "ts": ts,
                "direction": direction,
                "type": cells[3].lower(),          # buy / sell
                "lots": lots,
                "price": price,
                "order_id": order_id,
                "profit": profit,
                "balance": balance,
                "comment": comment,
                "tag": tag,
            })

        elif n == 11:
            # Order row: time_open, order_id, symbol, type, volume (x/y), price, sl, tp,
            #           time_done, state, comment
            if "XAUUSD" not in (cells[2] if len(cells) > 2 else ""):
                continue
            try:
                ts_open = pd.to_datetime(cells[0])
                ts_close = pd.to_datetime(cells[8]) if cells[8] else pd.NaT
            except Exception:
                continue
            vol = cells[4]   # "0.02 / 0.02"
            vol_open = 0.0
            vol_filled = 0.0
            if "/" in vol:
                a, b = vol.split("/", 1)
                try:
                    vol_open = _num(a)
                    vol_filled = _num(b)
                except ValueError:
                    pass
            orders.append({
                "ts_open": ts_open,
                "order_id": cells[1],
                "type": cells[3].lower().replace(" ", "_"),   # "buy stop" -> "buy_stop"
                "lots_open": vol_open,
                "lots_filled": vol_filled,
                "price": _num(cells[5]),
                "sl": _num(cells[6]),
                "tp": _num(cells[7]),
                "ts_close": ts_close,
                "state": cells[9].lower(),    # filled / canceled
                "comment": cells[10],
            })

    deals_df = pd.DataFrame(deals).sort_values("ts").reset_index(drop=True) if deals else pd.DataFrame()
    orders_df = pd.DataFrame(orders).sort_values("ts_open").reset_index(drop=True) if orders else pd.DataFrame()
    return deals_df, orders_df


def main():
    if len(sys.argv) != 2:
        print("Usage: sim_parse_mt5_deals.py <report.htm>")
        sys.exit(1)
    p = Path(sys.argv[1])
    deals, orders = parse_mt5_deals(p)
    deals_path = p.with_suffix(".deals.csv")
    orders_path = p.with_suffix(".orders.csv")
    deals.to_csv(deals_path, index=False)
    orders.to_csv(orders_path, index=False)
    print(f"Deals : {len(deals):,} rows -> {deals_path}")
    print(f"Orders: {len(orders):,} rows -> {orders_path}")
    if not deals.empty:
        tag_counts = deals[deals["direction"] == "out"]["tag"].value_counts()
        print("\nExit-deal tag counts:")
        print(tag_counts.to_string())
        print(f"\nDate range: {deals['ts'].min()} -> {deals['ts'].max()}")


if __name__ == "__main__":
    main()
