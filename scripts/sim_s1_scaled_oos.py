"""S1 OOS at 1.5x risk (scaled together with daily target/loss). S2 disabled."""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone, date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import load_ticks, load_bars, symbol_meta, kill_mt5_terminal
from zgb_sim.scalper_v1 import S1Config, SymbolMeta
from zgb_sim.scalper_v1_fast import simulate_fast


import argparse


def make_cfg(scale: float) -> S1Config:
    """Phase B winner scaled. Base: Donch=35 TP=300 SL=40 HTP=0.7 Tgt=15 Loss=6 PEB=2."""
    return S1Config(
        risk_pct=1.0 * scale,
        donchian_bars=35,
        take_profit_pts=300,
        stop_loss_pts=40,
        half_tp_ratio=0.7,
        pending_expire_bars=2,
        start_hour=14,
        end_hour=22,
        daily_target_pct=15.0 * scale,
        daily_loss_pct=6.0 * scale,
        block_fri_pm=True,
        hedge_mode=False,
    )

WINDOWS = [
    ("W1", date(2026, 3, 14), date(2026, 3, 28)),
    ("W2", date(2026, 3, 28), date(2026, 4, 11)),
    ("W3", date(2026, 4, 11), date(2026, 4, 25)),
]
DEPOSIT = 100.0


def _utc(d):
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", type=float, default=1.5)
    args = ap.parse_args()
    scale = args.scale
    cfg = make_cfg(scale)

    try:
        m = symbol_meta("XAUUSD")
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )

        print("=" * 72)
        print(f"  S1 OOS @ {scale}x scale (Phase B winner)")
        print(f"  Donch=35 TP=300 SL=40 HTP=0.7 PEB=2  Risk={cfg.risk_pct}%  "
              f"Tgt={cfg.daily_target_pct}%  Loss={cfg.daily_loss_pct}%")
        print(f"  Hours 14-22 UTC, deposit ${DEPOSIT}")
        print("=" * 72)

        results = []
        for label, start, end in WINDOWS:
            print(f"\n--- {label} OOS ({start} -> {end}) ---")
            ticks = load_ticks("XAUUSD", _utc(start), _utc(end))
            m1 = load_bars("XAUUSD", "M1", _utc(start), _utc(end))
            m5 = load_bars("XAUUSD", "M5", _utc(start), _utc(end))
            t0 = time.time()
            r = simulate_fast(ticks, m5, m1, cfg, meta, DEPOSIT)
            print(f"  {label} done in {time.time()-t0:.1f}s")
            print(f"  {r.summary()}")
            results.append((label, r))

        # Full span
        print("\n" + "=" * 72)
        print("  FULL OOS SPAN: Mar 14 -> Apr 25")
        print("=" * 72)
        ticks = load_ticks("XAUUSD", _utc(date(2026, 3, 14)), _utc(date(2026, 4, 25)))
        m1 = load_bars("XAUUSD", "M1", _utc(date(2026, 3, 14)), _utc(date(2026, 4, 25)))
        m5 = load_bars("XAUUSD", "M5", _utc(date(2026, 3, 14)), _utc(date(2026, 4, 25)))
        t0 = time.time()
        r_full = simulate_fast(ticks, m5, m1, cfg, meta, DEPOSIT)
        print(f"  Done in {time.time()-t0:.1f}s")
        print(f"  FULL: {r_full.summary()}")

        # Summary table vs 1.0x baseline (from prior WFO)
        print("\n" + "=" * 72)
        print(f"  PER-WINDOW SUMMARY ({scale}x)")
        print("=" * 72)
        print(f"  {'Window':<8}{'NP':>10}{'ROI%':>9}{'PF':>7}{'DD%':>7}{'Trades':>8}")
        for label, r in results:
            roi = r.net_profit / DEPOSIT * 100.0
            print(f"  {label:<8}{r.net_profit:>+10,.2f}{roi:>+8.1f}%"
                  f"{r.profit_factor:>7.2f}{r.max_drawdown_pct:>6.1f}%{r.trades:>8}")
        total_np = sum(r.net_profit for _, r in results)
        avg_roi = total_np / (DEPOSIT * len(results)) * 100.0
        print(f"  {'Total':<8}{total_np:>+10,.2f}{avg_roi:>+8.1f}% (avg/win)")

        print("\n  Reference (1.0x S1, prior WFO winner):")
        print(f"    W1=+$123.56  W2=+$36.51  W3=+$13.92  Total=+$173.99  Full-span=+$251.64  DD=12.6%")
    finally:
        kill_mt5_terminal()


if __name__ == "__main__":
    main()
