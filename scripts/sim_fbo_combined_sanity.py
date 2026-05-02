"""Combined S1+S2 sanity ET — both streams running simultaneously on shared balance.

Reads S1 winner params (from today's known WFO result) and S2 winner params
(parsed from the setfile written by sim_wfo_fbo_s2_spread60.py).

Single continuous run on Mar 14 -> Apr 25 at $10k deposit, spread=60.

Output: combined NP/DD/PF + per-stream contribution breakdown.
"""
from __future__ import annotations

import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.fbo_s1 import FBOS1Config
from zgb_sim.fbo_combined import simulate_combined


SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0

# S1 winner from sim_wfo_fbo_s1_spread60 run on 2026-04-26
S1_WINNER = FBOS1Config(
    risk_pct=3.0,
    fractal_bars=8,
    take_profit_pts=25_000,
    stop_loss_pts=10_000,
    half_tp_ratio=0.3,
    sma_period=10,
    pending_expire_bars=2,
    signal_tf_minutes=30,
    comment="FBO_A",
)

S2_SETFILE = ROOT / "configs" / "sets" / "fbo_s2_sim_spread60_apr25.set"


def parse_s2_setfile(path: Path) -> FBOS1Config:
    """Parse the S2 winner setfile written by sim_wfo_fbo_s2_spread60."""
    text = path.read_text(encoding="utf-8")
    def _val(key: str) -> str:
        m = re.search(rf"^{re.escape(key)}=([^|]+)", text, re.MULTILINE)
        if not m:
            raise ValueError(f"Key {key} not found in setfile {path}")
        return m.group(1).strip()
    return FBOS1Config(
        risk_pct=float(_val("_RiskPct")),
        fractal_bars=int(_val("_Bars2")),
        take_profit_pts=int(_val("_take_profit2")),
        stop_loss_pts=int(_val("_stop_loss2")),
        half_tp_ratio=float(_val("_HalfTP2")),
        sma_period=int(_val("_EMA_Period2")),
        pending_expire_bars=int(_val("_PendingExpireBars")),
        signal_tf_minutes=240,    # H4
        comment="FBO_B",
    )


def main() -> int:
    print("=" * 72)
    print("  FBO COMBINED SANITY ET (S1 + S2, shared $10k, spread=60)")
    print("=" * 72)

    if not S2_SETFILE.exists():
        print(f"  ERROR: S2 winner setfile not found at {S2_SETFILE}")
        print(f"  Run sim_wfo_fbo_s2_spread60.py first.")
        return 1

    s2_cfg = parse_s2_setfile(S2_SETFILE)

    print(f"\n  S1 (M30): Bars={S1_WINNER.fractal_bars} TP={S1_WINNER.take_profit_pts} "
          f"SL={S1_WINNER.stop_loss_pts} HTP={S1_WINNER.half_tp_ratio} "
          f"SMA={S1_WINNER.sma_period} PEB={S1_WINNER.pending_expire_bars}")
    print(f"  S2 (H4):  Bars={s2_cfg.fractal_bars} TP={s2_cfg.take_profit_pts} "
          f"SL={s2_cfg.stop_loss_pts} HTP={s2_cfg.half_tp_ratio} "
          f"SMA={s2_cfg.sma_period} PEB={s2_cfg.pending_expire_bars}")

    full_start = datetime(2026, 3, 14, tzinfo=timezone.utc)
    full_end = datetime(2026, 4, 25, tzinfo=timezone.utc)

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(
            point=m["point"], digits=m["digits"],
            tick_size=m["tick_size"], tick_value=m["tick_value"],
            stops_level_pts=m["stops_level"], volume_min=m["volume_min"],
            volume_max=m["volume_max"], volume_step=m["volume_step"],
        )
        print(f"\n  Loading data...")
        ticks = load_ticks(SYMBOL, full_start, full_end)
        m1 = load_bars(SYMBOL, "M1", full_start, full_end)
        m30 = load_bars(SYMBOL, "M30", full_start, full_end)
        h4 = load_bars(SYMBOL, "H4", full_start, full_end)
        print(f"  Loaded ticks={len(ticks):,}  M1={len(m1):,}  M30={len(m30):,}  H4={len(h4):,}")

        print("\n  Running combined sim (S1 + S2)...")
        result = simulate_combined(
            ticks, m30, h4, m1,
            S1_WINNER, s2_cfg, meta,
            initial_balance=DEPOSIT,
        )

        print("\n" + "=" * 72)
        print("  COMBINED RESULT")
        print("=" * 72)
        s = result.summary
        print(f"  Total NP:    ${result.total_np:+,.2f}  "
              f"({result.total_np / DEPOSIT * 100:+.1f}% ROI on ${DEPOSIT:,.0f})")
        print(f"  Combined DD: {result.combined_dd_pct:.2f}%")
        print(f"  Combined PF: {s.profit_factor:.2f}")
        print(f"  Combined trades: {s.trades}  (TP={s.tp_count} SL={s.sl_count} Other={s.other_count})")
        print()
        print(f"  S1 contribution: ${result.s1_np:+,.2f}  "
              f"(trades={result.s1_result.trades}: "
              f"TP={result.s1_result.tp_count} SL={result.s1_result.sl_count})")
        print(f"  S2 contribution: ${result.s2_np:+,.2f}  "
              f"(trades={result.s2_result.trades}: "
              f"TP={result.s2_result.tp_count} SL={result.s2_result.sl_count})")
        print()

        # Compare to per-stream sanity (what each does in isolation)
        print("  (For comparison, per-stream isolation results saved earlier:)")
        print(f"    S1 isolated sanity: NP=+$1,400  (from FBO S1 WFO sanity ET)")
        print(f"    S2 isolated sanity: see {S2_SETFILE.parent}/sim_wfo_fbo_s2_*/winner_sanity_ET")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
