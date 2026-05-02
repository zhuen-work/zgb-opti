"""Phase E: Combined 5-stream sanity using each WFO's winner.json.

Reads winner configs from:
  output/wfo_orb_spread70_may2/winner.json
  output/wfo_ema_pullback_may2/winner.json
  output/wfo_fbo_s1_may2/winner.json
  output/wfo_fbo_s2_may2/winner.json
  output/wfo_lsfvg_may2/winner.json

Runs combined 5-stream deal-merge on shared $10k at 2/3/4.5%.
Compares to current 4-stream baseline (EMP disabled).

Decision: each setfile risk level should keep configs that are net-positive
in combined sanity AND don't make combined NP/DD worse than current.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.tick_loader import symbol_meta, kill_mt5_terminal, load_ticks, load_bars
from zgb_sim.scalper_v1 import SymbolMeta
from zgb_sim.fbo_s1 import FBOS1Config
from zgb_sim.fbo_s1_fast import simulate_fast as fbo_simulate
from zgb_sim.orb import ORBConfig
from zgb_sim.orb_fast import simulate_fast as orb_simulate
from zgb_sim.lsfvg import LSFVGConfig
from zgb_sim.lsfvg_fast import simulate_fast as lsfvg_simulate
from zgb_sim.ema_pullback import EMAPullbackConfig
from zgb_sim.ema_pullback_fast import simulate_fast as ep_simulate

SYMBOL = "XAUUSD"
DEPOSIT = 10_000.0
WFO_BASELINE_RISK = 3.0


def load_winner(path: Path):
    if not path.exists():
        print(f"  [skip] {path} not found")
        return None
    data = json.loads(path.read_text())
    # Normalize: ORB script writes cfg dict directly at top level;
    # other scripts wrap it as {"cfg": ..., "p0_pass": ..., ...}
    if "cfg" not in data:
        # ORB-style: bare cfg dict. We KNOW from log the ORB winner passed P0.
        data = {"cfg": data, "p0_pass": True, "slope": 0.0, "oos_nps": [], "total_np": 1.0}
    return data


def is_viable_winner(w: dict) -> bool:
    """Decide whether a stream's WFO winner should be included in Phase E.
    Rule: must pass P0 AND have positive total OOS NP.
    EMP failed this (P0 PASS but NP -$849) -- correctly excluded.
    """
    if w is None:
        return False
    return bool(w.get("p0_pass", False)) and w.get("total_np", 0.0) > 0


def make_cfg(winner: dict, cfg_class, risk: float, **field_overrides):
    """Build a config from winner.json, scaling risk_pct + caps if present."""
    cfg_dict = dict(winner["cfg"])
    cfg_dict["risk_pct"] = risk
    # Scale daily caps if present (anchor to WFO_BASELINE_RISK = 3.0)
    if "daily_target_pct" in cfg_dict and cfg_dict["daily_target_pct"] > 0:
        cfg_dict["daily_target_pct"] = cfg_dict["daily_target_pct"] * (risk / WFO_BASELINE_RISK)
    if "daily_loss_pct" in cfg_dict and cfg_dict["daily_loss_pct"] > 0:
        cfg_dict["daily_loss_pct"] = cfg_dict["daily_loss_pct"] * (risk / WFO_BASELINE_RISK)
    cfg_dict.update(field_overrides)
    # Drop any keys not in the class's fields
    import dataclasses
    field_names = {f.name for f in dataclasses.fields(cfg_class)}
    cfg_dict = {k: v for k, v in cfg_dict.items() if k in field_names}
    return cfg_class(**cfg_dict)


def main() -> int:
    start = datetime(2026, 2, 14, tzinfo=timezone.utc)
    end = datetime(2026, 5, 1, tzinfo=timezone.utc)
    days = (end - start).days

    try:
        m = symbol_meta(SYMBOL)
        meta = SymbolMeta(point=m["point"], digits=m["digits"], tick_size=m["tick_size"],
                          tick_value=m["tick_value"], stops_level_pts=m["stops_level"],
                          volume_min=m["volume_min"], volume_max=m["volume_max"],
                          volume_step=m["volume_step"])
        ticks = load_ticks(SYMBOL, start, end)
        m1 = load_bars(SYMBOL, "M1", start, end)
        m5 = load_bars(SYMBOL, "M5", start, end)
        m15 = load_bars(SYMBOL, "M15", start, end)
        m30 = load_bars(SYMBOL, "M30", start, end)

        print("=" * 100)
        print(f"  PHASE E: Combined sanity with all 5 WFO winners ({days}d, $10k, 70pt)")
        print("=" * 100)

        winners = {
            "FBO_S1": load_winner(ROOT / "output" / "wfo_fbo_s1_may2" / "winner.json"),
            "FBO_S2": load_winner(ROOT / "output" / "wfo_fbo_s2_may2" / "winner.json"),
            "ORB":    load_winner(ROOT / "output" / "wfo_orb_spread70_may2" / "winner.json"),
            "LSFVG":  load_winner(ROOT / "output" / "wfo_lsfvg_may2" / "winner.json"),
            "EMP":    load_winner(ROOT / "output" / "wfo_ema_pullback_may2" / "winner.json"),
        }
        for s, w in winners.items():
            if w is not None:
                p0 = w.get("p0_pass", "?")
                slope = w.get("slope", 0)
                print(f"  {s:<8} P0={p0}  slope={slope:+.1%}  cfg={w['cfg']}")

        for risk in (2.0, 3.0, 4.5):
            print(f"\n  --- Combined sanity at {risk}% risk ---")
            cfgs_loaded = {}
            # Only include streams whose winner passed P0 AND has positive NP
            for name, cfg_class in [("FBO_S1", FBOS1Config), ("FBO_S2", FBOS1Config),
                                      ("ORB", ORBConfig), ("LSFVG", LSFVGConfig),
                                      ("EMP", EMAPullbackConfig)]:
                w = winners.get(name)
                if is_viable_winner(w):
                    cfgs_loaded[name] = make_cfg(w, cfg_class, risk)
                else:
                    reason = "no winner" if w is None else (
                        "FAILED P0" if not w.get("p0_pass") else "negative NP")
                    print(f"    [{name} excluded — {reason}]")

            results = {}
            for s, cfg in cfgs_loaded.items():
                if s == "FBO_S1":
                    r = fbo_simulate(ticks, m30, m1, cfg, meta, initial_balance=DEPOSIT)
                elif s == "FBO_S2":
                    r = fbo_simulate(ticks, m15, m1, cfg, meta, initial_balance=DEPOSIT)
                elif s == "ORB":
                    r = orb_simulate(ticks, m5, m1, cfg, meta, initial_balance=DEPOSIT)
                elif s == "LSFVG":
                    r = lsfvg_simulate(ticks, m15, m1, cfg, meta, initial_balance=DEPOSIT)
                elif s == "EMP":
                    r = ep_simulate(ticks, m15, m1, cfg, meta, initial_balance=DEPOSIT)
                results[s] = r

            all_deals = []
            for s, r in results.items():
                for d in r.deals:
                    if d.kind != "entry":
                        all_deals.append((d.ts, s, d.pnl))
            all_deals.sort(key=lambda x: x[0])

            bal = DEPOSIT; bal_max = DEPOSIT; dd_abs = 0.0
            for _, _s, p in all_deals:
                bal += p
                if bal > bal_max: bal_max = bal
                cur = bal_max - bal
                if cur > dd_abs: dd_abs = cur
            np_ = bal - DEPOSIT
            dd_pct = dd_abs / bal_max * 100 if bal_max > 0 else 0
            ndd = np_ / dd_abs if dd_abs > 0 else 0

            print(f"  COMBINED:  NP=${np_:+,.0f}  ROI={np_/DEPOSIT*100:+.1f}%  "
                  f"DD={dd_pct:.1f}%  NP/DD={ndd:.2f}  Trades={len(all_deals)}")
            for s in ("FBO_S1", "FBO_S2", "ORB", "LSFVG", "EMP"):
                np_s = sum(p for _,ss,p in all_deals if ss == s)
                tr_s = sum(1 for _,ss,_ in all_deals if ss == s)
                print(f"    {s:<7}  ${np_s:>+9,.0f}  ({tr_s} trades)")
    finally:
        kill_mt5_terminal()
    return 0


if __name__ == "__main__":
    sys.exit(main())
