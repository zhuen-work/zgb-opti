"""Measure XAUUSD.sc spread distribution at real UTC 04:00 (Session A LDN start).

Run this script BEFORE 04:00 UTC. It will:
- Poll symbol_info_tick every 5 seconds from start time -> end time
- Log timestamp, bid, ask, spread in points
- Print stats: min, median, p95, max
- Save raw to output/asian_spread_probe_<date>.csv

Tells us if 50pt MaxSpreadPts filter is appropriate for Session A.

Usage:
  python scripts/probe_asian_hour_spread.py            # uses today's UTC 04:00
  python scripts/probe_asian_hour_spread.py 04:00 90   # custom start + duration min
"""
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.mt5_accounts import init_account
from zgb_sim.tick_loader import kill_mt5_terminal
import MetaTrader5 as mt5
import pandas as pd

# Args: optional start hour HH:MM and duration in minutes
start_hour, start_min = 4, 0
duration_min = 90  # full Session A LDN range window
if len(sys.argv) > 1:
    parts = sys.argv[1].split(":")
    start_hour = int(parts[0]); start_min = int(parts[1]) if len(parts) > 1 else 0
if len(sys.argv) > 2:
    duration_min = int(sys.argv[2])

now = datetime.now(timezone.utc)
start_t = now.replace(hour=start_hour, minute=start_min, second=0, microsecond=0)
if start_t < now:
    start_t = start_t + timedelta(days=1)
end_t = start_t + timedelta(minutes=duration_min)

print(f"=== Asian-hour spread probe ===")
print(f"  Start: {start_t} UTC")
print(f"  End:   {end_t} UTC")
print(f"  Duration: {duration_min} min @ 5s polling")
print(f"  Note: MT5 reports tick.time in broker-time (UTC+3). Spread is real-time though.")

# Wait until start
wait_s = (start_t - datetime.now(timezone.utc)).total_seconds()
if wait_s > 0:
    print(f"  Waiting {wait_s:.0f}s until start...")
    time.sleep(wait_s)

spec = init_account("live")
samples = []
try:
    sym = spec.symbol
    mt5.symbol_select(sym, True)
    print(f"\n  Polling {sym}...")
    poll_count = 0
    while datetime.now(timezone.utc) < end_t:
        si = mt5.symbol_info_tick(sym)
        if si and si.bid > 0 and si.ask > 0:
            spread_pts = round((si.ask - si.bid) / 0.01)
            samples.append({
                "ts_real_utc": datetime.now(timezone.utc).isoformat(),
                "broker_tick_time": datetime.fromtimestamp(si.time, tz=timezone.utc).isoformat(),
                "bid": si.bid, "ask": si.ask, "spread_pts": spread_pts,
            })
        poll_count += 1
        if poll_count % 12 == 0:  # log every 1 min
            print(f"  [{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}] "
                  f"samples={len(samples)} latest_spread={samples[-1]['spread_pts'] if samples else '-'}pt")
        time.sleep(5)
finally:
    mt5.shutdown()
    kill_mt5_terminal()

if not samples:
    print("\n  NO SAMPLES collected — MT5 connection issue?")
    sys.exit(1)

df = pd.DataFrame(samples)
out_csv = ROOT / "output" / f"asian_spread_probe_{start_t.date()}.csv"
out_csv.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(out_csv, index=False)

print(f"\n=== Results ({len(df)} samples) ===")
print(f"  spread min:    {df['spread_pts'].min():.0f}pt")
print(f"  spread median: {df['spread_pts'].median():.0f}pt")
print(f"  spread p95:    {df['spread_pts'].quantile(0.95):.0f}pt")
print(f"  spread p99:    {df['spread_pts'].quantile(0.99):.0f}pt")
print(f"  spread max:    {df['spread_pts'].max():.0f}pt")
print(f"\n  % of time spread > 50pt: {(df['spread_pts'] > 50).mean()*100:.1f}%")
print(f"  % of time spread > 60pt: {(df['spread_pts'] > 60).mean()*100:.1f}%")
print(f"  % of time spread > 100pt: {(df['spread_pts'] > 100).mean()*100:.1f}%")
print(f"\n  Saved: {out_csv}")
print(f"\nRecommendation:")
p95 = df['spread_pts'].quantile(0.95)
if p95 <= 50:
    print(f"  p95={p95:.0f}pt within current MaxSpreadPts=50 → no change needed")
elif p95 <= 60:
    print(f"  p95={p95:.0f}pt exceeds 50pt → consider bumping MaxSpreadPts to 70-80")
else:
    print(f"  p95={p95:.0f}pt is HIGH → Session A may have execution risk; consider:")
    print(f"  (a) bump MaxSpreadPts to {int(p95 + 10)} (lets through 5% extreme spikes)")
    print(f"  (b) compare with sim WFO friction (55pt) — if live regularly > 55, sim is too optimistic")
