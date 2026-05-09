"""Parse a generated setfile and run its parent + hedge configs through the sim.

Confirms the setfile values produce the expected portfolio NP/DD$ — catches any
extract_top_n.py bugs (wrong magics, swapped values, format errors).

Usage:
  python scripts/verify_setfile.py configs/sets/dt818_pro_3pct_with_hedge_per_stream.set
"""
from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import importlib.util
_spec = importlib.util.spec_from_file_location("wfo_hedge", ROOT / "scripts" / "sim_wfo_hedge.py")
hg = importlib.util.module_from_spec(_spec)
sys.modules["wfo_hedge"] = hg
_spec.loader.exec_module(hg)

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
SPREAD = 23
START = datetime(2026, 2, 14, tzinfo=timezone.utc)
END = datetime(2026, 5, 1, tzinfo=timezone.utc)


def parse_setfile(path: Path) -> dict:
    """Extract _RiskPct + per-stream parent + hedge values from a setfile."""
    text = path.read_text()
    out = {"global": {}, "ORB": {}, "HEDGE": {}}
    # Lines look like:  KEY=value||...||...||N   (skip ||... pipe parts)
    line_re = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=([^|\r\n]+)")
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith(";"):
            continue
        m = line_re.match(s)
        if not m:
            continue
        key, val = m.group(1), m.group(2).strip()
        # Try numeric cast
        try:
            v = float(val) if "." in val else int(val)
        except ValueError:
            v = val  # keep string (Comment, Enabled etc.)
        # Bucket by prefix
        m2 = re.match(r"_ORB_(S\d+)_(\w+)", key)
        if m2:
            out["ORB"].setdefault(m2.group(1), {})[m2.group(2)] = v; continue
        m2 = re.match(r"_HEDGE_(S\d+)_(\w+)", key)
        if m2:
            out["HEDGE"].setdefault(m2.group(1), {})[m2.group(2)] = v; continue
        out["global"][key] = v
    return out


def build_parent_cfg(stream_block: dict, risk_pct: float) -> ORBConfig:
    return ORBConfig(
        risk_pct=risk_pct,
        range_minutes=int(stream_block["RangeMinutes"]),
        buffer_pts=0,
        min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=int(stream_block["FixedSL_Pts"]),
        rr_ratio=float(stream_block["RR_Ratio"]),
        half_tp_ratio=round(float(stream_block["HalfTP_Ratio"]), 2),
        pending_expire_minutes=int(stream_block["PendingExpireMinutes"]),
        daily_target_pct=float(stream_block["DailyTargetPct"]),
        daily_loss_pct=float(stream_block["DailyLossPct"]),
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True,  ny_start_hour=13,
        comment=str(stream_block["Comment"]),
    )


def aggregate(deals):
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0; gp = gl = 0.0
    for _, p in sorted(deals, key=lambda x: x[0]):
        bal += p
        if bal > bal_max: bal_max = bal
        if (bal_max - bal) > dd_abs: dd_abs = bal_max - bal
        if p > 0: gp += p
        elif p < 0: gl += p
    np_ = bal - DEPOSIT
    dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
    pf = (gp / abs(gl)) if gl < 0 else float("inf")
    ndd = (np_ / dd_abs) if dd_abs > 0 else 0
    return np_, dd_pct, pf, ndd


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/verify_setfile.py <setfile.set>")
        return 1
    setfile = Path(sys.argv[1])
    if not setfile.is_absolute():
        setfile = ROOT / setfile

    parsed = parse_setfile(setfile)
    risk_pct = float(parsed["global"]["_RiskPct"])
    print("=" * 100)
    print(f"  VERIFY {setfile.name}")
    print(f"  Per-stream _RiskPct={risk_pct}%  (= total risk {risk_pct*3}% / 3 streams)")
    print("=" * 100)
    print(f"\n  Parsed parents:")
    for s in ("S1", "S2", "S3"):
        b = parsed["ORB"][s]
        print(f"    {s} (magic {b['Magic']}): Range={b['RangeMinutes']} SL={b['FixedSL_Pts']} "
              f"RR={b['RR_Ratio']} HTP={b['HalfTP_Ratio']}  Comment={b['Comment']}")
    print(f"\n  Parsed hedges:")
    for s in ("S1", "S2", "S3"):
        if s not in parsed["HEDGE"]:
            print(f"    {s}: NO HEDGE BLOCK"); continue
        b = parsed["HEDGE"][s]
        print(f"    {s} (magic {b['Magic']}, parent {b['ParentMagic']}): "
              f"buf={b['BufferPts']} SL={b['FixedSL_Pts']} RR={b['RR_Ratio']} "
              f"exp={b['ExpireMinutes']}min risk={b['RiskPct']}%  Comment={b['Comment']}")

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        m1 = load_bars(SYMBOL, "M1", START, END)
        m5 = load_bars(SYMBOL, "M5", START, END)
        ticks = load_ticks(SYMBOL, START, END, spread_pts=SPREAD)
        t_arr = hg.ts_arr_from_ticks(ticks)
        print(f"\n  Loaded ticks={len(ticks):,} M1={len(m1):,} M5={len(m5):,}  spread={SPREAD}pt")

        # Run portfolio with setfile-derived configs
        all_deals = []; per_s = {}; per_h = {}
        for s in ("S1", "S2", "S3"):
            pcfg = build_parent_cfg(parsed["ORB"][s], risk_pct)
            r = orb_simulate(ticks, m5, m1, pcfg, meta, initial_balance=DEPOSIT)
            sl_events = []
            for d in r.deals:
                if d.kind == "entry":
                    continue
                all_deals.append((pd.Timestamp(d.ts).value, d.pnl))
                if d.kind == "sl":
                    sl_events.append({"ts_ns": pd.Timestamp(d.ts).value,
                                      "direction": int(d.direction),
                                      "sl_price": float(d.price),
                                      "lots": float(d.lots)})
            per_s[s] = (r.net_profit, r.trades)
            # Hedge: use setfile values directly
            if s in parsed["HEDGE"]:
                hb = parsed["HEDGE"][s]
                hg.HEDGE_RISK_PCT = float(hb["RiskPct"])
                hcfg = hg.HedgeCfg(buf=int(hb["BufferPts"]),
                                    h_sl=int(hb["FixedSL_Pts"]),
                                    h_rr=float(hb["RR_Ratio"]),
                                    exp=int(hb["ExpireMinutes"]))
                h_deals = hg.simulate_hedges(sl_events, t_arr, hcfg)
                h_pnl = sum(p for _, p in h_deals)
                h_n = len(h_deals)
                h_w = sum(1 for _, p in h_deals if p > 0)
                per_h[s] = (h_pnl, h_n, h_w)
                for ts, p in h_deals:
                    all_deals.append((ts, p))
            else:
                per_h[s] = (0.0, 0, 0)

        np_, dd, pf, ndd = aggregate(all_deals)
        total_risk = risk_pct * 3
        roi = np_ / DEPOSIT * 100
        print(f"\n  PORTFOLIO RESULT (parent+hedge from setfile, {SPREAD}pt, $10k, 76d):")
        print(f"  NP=${np_:+,.0f}  ROI={roi:+.1f}%  DD={dd:.2f}%  NP/DD$={ndd:.2f}  PF={pf:.2f}  Trades={len(all_deals)}")
        print(f"\n  Per-stream:")
        for s in ("S1", "S2", "S3"):
            np_p, n_p = per_s[s]
            np_h, n_h, w_h = per_h[s]
            wr = (w_h / n_h * 100) if n_h else 0
            print(f"    {s}: parent NP=${np_p:>+8,.0f} ({n_p:>3} trades)  "
                  f"hedge NP=${np_h:>+7,.0f} ({n_h:>3} trades, {wr:>4.0f}% WR)")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
