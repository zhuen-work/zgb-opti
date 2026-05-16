"""Asia-vs-NY correlation analysis: does Asia session outcome predict NY result?

Joins:
  - output/asia_session_log.csv   (per-day Asia BUY/SELL outcomes + dominant pattern)
  - output/daily_trade_log.csv    (per-trade NY parent results, with regime/range_pts)

Aggregates NY outcomes (NP, win/loss, by side) per Asia dominant pattern.
Output is a contingency table — rows = Asia dominant pattern, columns = NY outcome.

Usage:
  python scripts/asia_ny_correlation.py

Run AFTER both loggers have data. Need >= 10-20 NY-trading days for any
meaningful pattern; >= 30 for a real correlation claim.
"""
from __future__ import annotations
import sys
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
import pandas as pd

ASIA_CSV  = ROOT / "output" / "asia_session_log.csv"
NY_CSV    = ROOT / "output" / "daily_trade_log.csv"


def main() -> int:
    if not ASIA_CSV.exists():
        print(f"  Missing: {ASIA_CSV}. Run scripts/asia_session_log.py first.")
        return 1
    if not NY_CSV.exists():
        print(f"  Missing: {NY_CSV}. Need scripts/daily_trade_log.py runs to accumulate.")
        return 1

    asia = pd.read_csv(ASIA_CSV)
    ny = pd.read_csv(NY_CSV)
    ny = ny[ny["session"] == "NY"].copy()

    print(f"=== Asia-vs-NY correlation ===\n")
    print(f"  Asia sessions logged: {len(asia)}  ({asia['date'].min()} -> {asia['date'].max()})")
    print(f"  NY trades logged:     {len(ny)}    ({ny['date'].min() if len(ny) else 'n/a'} -> "
          f"{ny['date'].max() if len(ny) else 'n/a'})")

    if len(ny) == 0:
        print("\n  No NY trades yet — keep accumulating with daily_trade_log.py.")
        return 0

    # Aggregate NY by date: total NP, total trades, BUY net, SELL net
    ny_agg = ny.groupby("date").agg(
        ny_total_pnl=("pnl", "sum"),
        ny_n_trades=("pnl", "count"),
        ny_n_wins=("pnl", lambda s: (s > 0).sum()),
    ).reset_index()
    # Side splits
    buy_pnl = ny[ny["side"] == "BUY"].groupby("date")["pnl"].sum().rename("ny_buy_pnl")
    sell_pnl = ny[ny["side"] == "SELL"].groupby("date")["pnl"].sum().rename("ny_sell_pnl")
    ny_agg = ny_agg.merge(buy_pnl, on="date", how="left").merge(sell_pnl, on="date", how="left").fillna(0)

    # Join
    joined = asia.merge(ny_agg, on="date", how="inner")
    print(f"  Days with BOTH Asia + NY data: {len(joined)}")

    if len(joined) == 0:
        print("\n  No date overlap yet between Asia log and NY trade log.")
        print("  -> Keep both loggers running daily; revisit after 10+ overlap days.")
        return 0

    # Per-day join
    print(f"\n  --- Per-day join ---")
    print(f"  {'Date':<12} {'Asia dom':<14} {'A_BUY':<8} {'A_SELL':<8} "
          f"{'NY $':>9} {'NY W/L':>6} {'NY BUY $':>9} {'NY SELL $':>9}")
    print(f"  {'-'*12} {'-'*14} {'-'*8} {'-'*8} {'-'*9} {'-'*6} {'-'*9} {'-'*9}")
    for _, r in joined.iterrows():
        wl = f"{int(r['ny_n_wins'])}/{int(r['ny_n_trades'])}"
        print(f"  {r['date']:<12} {r['dominant']:<14} {r['buy_outcome']:<8} {r['sell_outcome']:<8} "
              f"${r['ny_total_pnl']:>+7,.0f} {wl:>6} ${r['ny_buy_pnl']:>+7,.0f} ${r['ny_sell_pnl']:>+7,.0f}")

    # Aggregate by Asia dominant pattern
    print(f"\n  --- NY outcomes by Asia dominant pattern ---")
    print(f"  {'Asia dominant':<14} {'days':>4} {'NY $ sum':>10} {'NY $/day':>10} "
          f"{'NY W%':>6} {'BUY $':>9} {'SELL $':>9}")
    print(f"  {'-'*14} {'-'*4} {'-'*10} {'-'*10} {'-'*6} {'-'*9} {'-'*9}")
    for dom, sub in joined.groupby("dominant"):
        n = len(sub)
        total = sub["ny_total_pnl"].sum()
        per_day = total / n
        wins = sub["ny_n_wins"].sum()
        trades = sub["ny_n_trades"].sum()
        wr = wins / trades * 100 if trades else 0
        buy_sum = sub["ny_buy_pnl"].sum()
        sell_sum = sub["ny_sell_pnl"].sum()
        print(f"  {dom:<14} {n:>4} ${total:>+8,.0f} ${per_day:>+8,.0f} {wr:>5.0f}% "
              f"${buy_sum:>+7,.0f} ${sell_sum:>+7,.0f}")

    # Filter signal candidate test: Asia BUY-side outcome -> NY BUY-side outcome
    print(f"\n  --- Asia BUY outcome -> NY BUY-side $ ---")
    print(f"  (Hypothesis: Asia BUY TP/SL predicts NY BUY direction)")
    print(f"  {'Asia BUY':<10} {'days':>4} {'NY BUY $ sum':>13} {'NY BUY $/day':>13}")
    for out, sub in joined.groupby("buy_outcome"):
        n = len(sub)
        total = sub["ny_buy_pnl"].sum()
        per_day = total / n
        print(f"  {out:<10} {n:>4} ${total:>+11,.0f} ${per_day:>+11,.0f}")

    print(f"\n  --- Asia SELL outcome -> NY SELL-side $ ---")
    print(f"  {'Asia SELL':<10} {'days':>4} {'NY SELL $ sum':>14} {'NY SELL $/day':>14}")
    for out, sub in joined.groupby("sell_outcome"):
        n = len(sub)
        total = sub["ny_sell_pnl"].sum()
        per_day = total / n
        print(f"  {out:<10} {n:>4} ${total:>+12,.0f} ${per_day:>+12,.0f}")

    # ============================================================
    # FILTER SIMULATION — counterfactual NY $ under candidate rules
    # ============================================================
    print(f"\n" + "=" * 80)
    print("  FILTER SIMULATION (counterfactual NY $ under each rule)")
    print("=" * 80)

    baseline = joined["ny_total_pnl"].sum()
    base_buy = joined["ny_buy_pnl"].sum()
    base_sell = joined["ny_sell_pnl"].sum()

    # Each rule = function(asia_row) -> ('TAKE_BOTH'|'BUY_ONLY'|'SELL_ONLY'|'SKIP')
    def rule_no_filter(r):       return "TAKE_BOTH"
    def rule_skip_chop(r):       # skip BOTH_SL + INSIDE_RANGE + MIXED
        return "SKIP" if r["dominant"] in ("BOTH_SL", "INSIDE_RANGE", "MIXED") else "TAKE_BOTH"
    def rule_follow_asia_winner(r):  # if Asia BUY won = take only NY BUYs; etc.
        if r["buy_outcome"] == "TP" and r["sell_outcome"] != "TP":
            return "BUY_ONLY"
        if r["sell_outcome"] == "TP" and r["buy_outcome"] != "TP":
            return "SELL_ONLY"
        if r["dominant"] in ("BOTH_SL", "INSIDE_RANGE"):
            return "SKIP"
        return "TAKE_BOTH"
    def rule_fade_asia_loser(r):  # if Asia BUY lost = take NY SELLs (reversal)
        if r["buy_outcome"] == "SL" and r["sell_outcome"] != "SL":
            return "SELL_ONLY"
        if r["sell_outcome"] == "SL" and r["buy_outcome"] != "SL":
            return "BUY_ONLY"
        return "TAKE_BOTH"
    def rule_only_directional(r):  # only trade days with single-side BUY_ONLY/SELL_ONLY signal
        if r["dominant"] == "BUY_ONLY":  return "BUY_ONLY"
        if r["dominant"] == "SELL_ONLY": return "SELL_ONLY"
        return "SKIP"
    def rule_skip_inside(r):     # skip only INSIDE_RANGE days
        return "SKIP" if r["dominant"] == "INSIDE_RANGE" else "TAKE_BOTH"

    rules = [
        ("baseline (no filter)", rule_no_filter),
        ("skip chop days",       rule_skip_chop),
        ("follow Asia winner",   rule_follow_asia_winner),
        ("fade Asia loser",      rule_fade_asia_loser),
        ("only directional",     rule_only_directional),
        ("skip INSIDE_RANGE",    rule_skip_inside),
    ]

    print(f"\n  {'Rule':<24} {'days_taken':>10} {'NY $ (cf)':>11} {'vs base':>9} "
          f"{'BUY $':>9} {'SELL $':>9}")
    print(f"  {'-'*24} {'-'*10} {'-'*11} {'-'*9} {'-'*9} {'-'*9}")
    for name, fn in rules:
        cf_total = 0.0; cf_buy = 0.0; cf_sell = 0.0; days_taken = 0
        for _, r in joined.iterrows():
            decision = fn(r)
            if decision == "SKIP":
                continue
            days_taken += 1
            if decision == "TAKE_BOTH":
                cf_total += r["ny_total_pnl"]
                cf_buy += r["ny_buy_pnl"]
                cf_sell += r["ny_sell_pnl"]
            elif decision == "BUY_ONLY":
                cf_total += r["ny_buy_pnl"]
                cf_buy += r["ny_buy_pnl"]
            elif decision == "SELL_ONLY":
                cf_total += r["ny_sell_pnl"]
                cf_sell += r["ny_sell_pnl"]
        delta = cf_total - baseline
        print(f"  {name:<24} {days_taken:>10} ${cf_total:>+9,.0f} ${delta:>+7,.0f} "
              f"${cf_buy:>+7,.0f} ${cf_sell:>+7,.0f}")

    # Sample-size honesty
    print(f"\n  Sample size: {len(joined)} days. ", end="")
    if len(joined) < 10:
        print("Way too small — patterns are noise. Need >= 30 days for any claim.")
    elif len(joined) < 30:
        print("Suggestive but not statistically meaningful. Need >= 30 days.")
    else:
        print("Approaching usable; >= 60 days for a real claim.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
