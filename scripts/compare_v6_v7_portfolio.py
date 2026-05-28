"""Compare v6 vs v7 6-stream portfolio sim on a common window.

Reads BOTH setfiles, propagates the global feature flags (_ORB_FractalConfirm,
_ORB_FractalWidth, _ORB_SMA_CrossExit, _ORB_SMA_FastPeriod, _ORB_SMA_SlowPeriod)
into each stream's ORBConfig, deal-merges across the 6 streams, applies the
live haircut, and prints a side-by-side comparison.

Run: python scripts/compare_v6_v7_portfolio.py
"""
from __future__ import annotations

import sys
import re
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, load_ticks, load_bars, kill_mt5_terminal
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
HAIRCUT_NP = 0.94
HAIRCUT_PF = 0.25
SPREAD = 30

# Common comparison window: most-recent 4 weeks (V5 sweep windows union)
START = datetime(2026, 4, 25, tzinfo=timezone.utc)
END   = datetime(2026, 5, 23, tzinfo=timezone.utc)


def parse_setfile(path: Path) -> dict:
    out = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith(";") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        val = val.split("||")[0].split(";")[0].strip()
        out[key.strip()] = val
    return out


def get_bool(d: dict, k: str, default=False) -> bool:
    v = d.get(k, str(default)).lower()
    return v in ("true", "1", "yes")


def get_int(d: dict, k: str, default=0) -> int:
    try:
        return int(d.get(k, default))
    except ValueError:
        return default


def get_float(d: dict, k: str, default=0.0) -> float:
    try:
        return float(d.get(k, default))
    except ValueError:
        return default


def build_cfg_for_stream(d: dict, sn: int, risk_pct: float) -> ORBConfig:
    return ORBConfig(
        risk_pct=risk_pct,
        range_minutes=get_int(d, f"_ORB_S{sn}_RangeMinutes"),
        buffer_pts=0,
        min_range_pts=0,
        max_range_pts=999_999,
        fixed_sl_pts=get_int(d, f"_ORB_S{sn}_FixedSL_Pts"),
        rr_ratio=get_float(d, f"_ORB_S{sn}_RR_Ratio"),
        half_tp_ratio=get_float(d, f"_ORB_S{sn}_HalfTP_Ratio"),
        pending_expire_minutes=get_int(d, f"_ORB_S{sn}_PendingExpireMinutes"),
        daily_target_pct=0.0, daily_loss_pct=0.0,
        ldn_enabled=True, ldn_start_hour=get_int(d, "_ORB_LDN_StartHour", 7),
        ny_enabled=True,  ny_start_hour=get_int(d, "_ORB_NY_StartHour", 13),
        fractal_confirm=get_bool(d, "_ORB_FractalConfirm"),
        fractal_width=get_int(d, "_ORB_FractalWidth", 5),
        sma_cross_exit=get_bool(d, "_ORB_SMA_CrossExit"),
        sma_cross_fast=get_int(d, "_ORB_SMA_FastPeriod", 3),
        sma_cross_slow=get_int(d, "_ORB_SMA_SlowPeriod", 5),
        comment=f"ORB_S{sn}",
    )


def run_portfolio(setfile_path: Path, ticks, m1, m5, meta) -> dict:
    d = parse_setfile(setfile_path)
    risk_pct = get_float(d, "_RiskPct", 1.5)
    streams = []
    for sn in range(1, 7):
        if not get_bool(d, f"_ORB_S{sn}_Enabled"):
            continue
        streams.append((sn, build_cfg_for_stream(d, sn, risk_pct)))

    # Deal-merge across streams onto shared balance
    deals = []
    per_stream = []
    for sn, cfg in streams:
        r = simulate_fast(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
        s_pnl = 0.0; s_trades = 0
        for de in r.deals:
            if de.kind == "entry":
                continue
            deals.append((de.ts, sn, de.pnl))
            s_pnl += de.pnl
            s_trades += 1
        per_stream.append({
            "stream": f"S{sn}",
            "range_min": cfg.range_minutes, "sl": cfg.fixed_sl_pts,
            "rr": cfg.rr_ratio, "htp": cfg.half_tp_ratio,
            "expire": cfg.pending_expire_minutes,
            "net": s_pnl, "trades": s_trades,
            "indiv_dd": r.max_drawdown, "indiv_pf": r.profit_factor,
        })

    # Aggregate on shared balance
    bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
    gains = 0.0; losses = 0.0; wins = 0; trades = 0
    for ts, _sn, p in sorted(deals, key=lambda x: x[0]):
        bal += p
        if bal > bal_max:
            bal_max = bal
        cur_dd = bal_max - bal
        if cur_dd > dd_abs:
            dd_abs = cur_dd
        if p >= 0:
            gains += p; wins += 1
        else:
            losses += -p
        trades += 1
    np_ = bal - DEPOSIT
    pf = gains / losses if losses > 0 else float("inf")
    ndd = np_ / dd_abs if dd_abs > 0 else 0.0

    np_hc = np_ * HAIRCUT_NP
    pf_hc = pf - HAIRCUT_PF if pf != float("inf") else pf
    ndd_hc = np_hc / dd_abs if dd_abs > 0 else 0.0

    return {
        "setfile": setfile_path.name,
        "risk_pct_total": risk_pct * len(streams),
        "fractal_confirm": get_bool(d, "_ORB_FractalConfirm"),
        "sma_cross_exit": get_bool(d, "_ORB_SMA_CrossExit"),
        "sma_periods": f"{get_int(d, '_ORB_SMA_FastPeriod', 0)}/{get_int(d, '_ORB_SMA_SlowPeriod', 0)}",
        "streams_active": len(streams),
        "np": np_, "dd_abs": dd_abs, "pf": pf, "ndd": ndd, "trades": trades, "wins": wins,
        "np_hc": np_hc, "pf_hc": pf_hc, "ndd_hc": ndd_hc,
        "per_stream": per_stream,
    }


def main() -> int:
    from zgb_sim.mt5_accounts import init_account
    init_account("sim")
    m = symbol_meta(SYMBOL)
    meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                      tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                      volume_min=m["volume_min"], volume_max=m["volume_max"],
                      volume_step=m["volume_step"])
    print(f"loading ticks/M5/M1 for {SYMBOL} {START.date()} -> {END.date()} spread={SPREAD}pt...")
    ticks = load_ticks(SYMBOL, START, END, spread_pts=SPREAD)
    m1 = load_bars(SYMBOL, "M1", START, END)
    m5 = load_bars(SYMBOL, "M5", START, END)
    print(f"  {len(ticks):,} ticks, {len(m5):,} M5 bars, {len(m1):,} M1 bars")

    v6_path = ROOT / "configs/sets/dt818_pro_v6_9pct_may23_may16.set"
    v7_path = ROOT / "configs/sets/dt818_pro_v7_9pct_may30_may23.set"

    print("\n=== v6 ===")
    v6 = run_portfolio(v6_path, ticks, m1, m5, meta)
    print(f"  np=${v6['np']:,.2f}  dd=${v6['dd_abs']:,.2f}  pf={v6['pf']:.3f}  "
          f"ndd={v6['ndd']:.2f}  trades={v6['trades']}")

    print("\n=== v7 ===")
    v7 = run_portfolio(v7_path, ticks, m1, m5, meta)
    print(f"  np=${v7['np']:,.2f}  dd=${v7['dd_abs']:,.2f}  pf={v7['pf']:.3f}  "
          f"ndd={v7['ndd']:.2f}  trades={v7['trades']}")

    kill_mt5_terminal()

    # Side-by-side comparison table
    delta_np = v7['np'] - v6['np']
    delta_pct = (delta_np / abs(v6['np']) * 100) if v6['np'] else 0.0
    delta_ndd = v7['ndd_hc'] - v6['ndd_hc']

    print("\n" + "=" * 80)
    print("PORTFOLIO COMPARISON (4-week window {} -> {})".format(START.date(), END.date()))
    print("=" * 80)
    print(f"{'Metric':<24}  {'v6':>14}  {'v7':>14}  {'Diff':>14}")
    print("-" * 80)
    print(f"{'Total risk':<24}  {v6['risk_pct_total']:>13.1f}%  {v7['risk_pct_total']:>13.1f}%  {'':>14}")
    print(f"{'Streams active':<24}  {v6['streams_active']:>14d}  {v7['streams_active']:>14d}  {'':>14}")
    print(f"{'FractalConfirm':<24}  {str(v6['fractal_confirm']):>14}  {str(v7['fractal_confirm']):>14}  {'':>14}")
    print(f"{'SMA cross-exit':<24}  {str(v6['sma_cross_exit']):>14}  {str(v7['sma_cross_exit']):>14}  {'':>14}")
    print(f"{'SMA periods':<24}  {v6['sma_periods']:>14}  {v7['sma_periods']:>14}  {'':>14}")
    print("-" * 80)
    print(f"{'Trades':<24}  {v6['trades']:>14d}  {v7['trades']:>14d}  {v7['trades']-v6['trades']:>+14d}")
    print(f"{'Wins':<24}  {v6['wins']:>14d}  {v7['wins']:>14d}  {v7['wins']-v6['wins']:>+14d}")
    print(f"{'Net Profit':<24}  ${v6['np']:>13,.2f}  ${v7['np']:>13,.2f}  ${delta_np:>+13,.2f}  ({delta_pct:+.1f}%)")
    print(f"{'Drawdown ($)':<24}  ${v6['dd_abs']:>13,.2f}  ${v7['dd_abs']:>13,.2f}  ${v7['dd_abs']-v6['dd_abs']:>+13,.2f}")
    print(f"{'Profit Factor':<24}  {v6['pf']:>14.3f}  {v7['pf']:>14.3f}  {v7['pf']-v6['pf']:>+14.3f}")
    print(f"{'NP/DD$':<24}  {v6['ndd']:>14.2f}  {v7['ndd']:>14.2f}  {v7['ndd']-v6['ndd']:>+14.2f}")
    print("-" * 80)
    print(f"{'NP_hc (× 0.94)':<24}  ${v6['np_hc']:>13,.2f}  ${v7['np_hc']:>13,.2f}  ${v7['np_hc']-v6['np_hc']:>+13,.2f}")
    print(f"{'PF_hc (− 0.25)':<24}  {v6['pf_hc']:>14.3f}  {v7['pf_hc']:>14.3f}  {v7['pf_hc']-v6['pf_hc']:>+14.3f}")
    print(f"{'NP/DD$_hc':<24}  {v6['ndd_hc']:>14.2f}  {v7['ndd_hc']:>14.2f}  {delta_ndd:>+14.2f}")
    print("=" * 80)

    # Per-stream comparison
    print("\nPer-stream params + isolated NP (no deal-merge):")
    print(f"  {'Stream':<6}  {'v6 params':<30}  {'v6 NP':>10}  {'v7 params':<30}  {'v7 NP':>10}")
    for i, (s6, s7) in enumerate(zip(v6['per_stream'], v7['per_stream'])):
        v6_p = f"R={s6['range_min']} SL={s6['sl']} RR={s6['rr']} HTP={s6['htp']} E={s6['expire']}"
        v7_p = f"R={s7['range_min']} SL={s7['sl']} RR={s7['rr']} HTP={s7['htp']} E={s7['expire']}"
        print(f"  {s6['stream']:<6}  {v6_p:<30}  ${s6['net']:>9,.0f}  {v7_p:<30}  ${s7['net']:>9,.0f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
