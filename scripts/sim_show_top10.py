"""Display top-10 by Recovery Factor for a window's IS parquet."""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

if len(sys.argv) != 2:
    print("usage: sim_show_top10.py <parquet_path>")
    sys.exit(1)

p = Path(sys.argv[1])
if not p.exists():
    print(f"  {p.name} not yet present")
    sys.exit(0)

df = pd.read_parquet(p)
prof = df[(df.net_profit > 0) & (df.trades >= 10) & df.error.isna()]
top = prof.sort_values("recovery_factor", ascending=False).head(10)
print(f"  {p.stem}: {len(prof)}/{len(df)} profitable. Top-10 by RF:")
print(f"    {'NP':>10} {'ROI%':>7} {'PF':>5} {'DD%':>6} {'Tr':>4} {'RF':>6}  Donch  TP    SL   HTP  Tgt  Loss")
for _, r in top.iterrows():
    print(f"    {r.net_profit:>+10,.2f} {r.return_pct:>+6.1f}% {r.profit_factor:>5.2f} "
          f"{r.drawdown_pct:>5.1f}% {int(r.trades):>4} {r.recovery_factor:>6.1f}  "
          f"{int(r.donchian_bars):>5}  {int(r.take_profit_pts):>4}  {int(r.stop_loss_pts):>4}  "
          f"{r.half_tp_ratio:>4.1f}  {int(r.daily_target_pct):>3}  {int(r.daily_loss_pct):>4}")
