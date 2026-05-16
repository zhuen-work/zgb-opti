"""Daily trade logger — appends every closed parent deal to a CSV with
session + range_pts features, so we can analyze win/loss outcomes
against ORB range size.

Counterpart to scripts/sl_post_analysis.py (which logs SL events only,
with hedge sims). This one logs ALL outcomes (TP + SL + expire) so we
can answer questions like:
  - Win rate by range_pts bucket
  - Per-stream PnL distribution by session/range
  - Whether a hypothetical MAX-range filter @ N would have saved or
    sacrificed net PnL

Output: output/daily_trade_log.csv  (one row per closed parent deal)
Dedupe key: (broker_ts_epoch, magic, exit_price) — handles HTP halves
that MT5 sometimes splits into two deal rows in hedging mode.
"""
from __future__ import annotations
import sys
import csv
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.mt5_accounts import init_account, get_broker_offset
from zgb_sim.tick_loader import kill_mt5_terminal
from zgb_sim.regime import (
    session_window_broker, compute_range_pts, build_h1_bars, compute_atr,
    lookup_atr_pts_at, classify_regime, POINT,
)
import MetaTrader5 as mt5
import pandas as pd

# Stream cfg — names only here (no hedge sim, no sl_pts/tp_mult needed).
STREAM_NAMES = {1111: "S1", 2222: "S2", 3333: "S3",
                 4444: "S4", 5555: "S5", 6666: "S6"}


def session_for_deal(deal_time_broker: pd.Timestamp, broker_off_h: int = 3):
    """Pick session for a deal time. Returns (sess, rng_start, rng_end).
    LDN broker range starts at (4 + broker_off_h):00 (= 07:00 in summer).
    NY broker range starts at (10 + broker_off_h):00 (= 13:00 in summer)."""
    ldn_start = 4 + broker_off_h
    ny_start = 10 + broker_off_h
    h = deal_time_broker.hour
    if ldn_start <= h < ny_start:
        sess = "LDN"
    elif ny_start <= h < (ny_start + 9):  # NY ~9h window for fills/exits
        sess = "NY"
    else:
        return "OTHER", None, None
    rng_start, rng_end = session_window_broker(deal_time_broker.date(), sess,
                                                  broker_offset_h=broker_off_h)
    return sess, rng_start, rng_end


def parse_outcome(comment: str) -> str:
    """[tp xxxx] -> 'TP', [sl xxxx] -> 'SL', else 'OTHER'."""
    if not comment:
        return "OTHER"
    c = comment.lower().lstrip("[")
    if c.startswith("tp"):
        return "TP"
    if c.startswith("sl"):
        return "SL"
    if c.startswith("expire") or c.startswith("exp"):
        return "EXPIRE"
    return "OTHER"


def find_parent_entry(deals, magic, exit_time):
    """Most-recent DEAL_ENTRY_IN for this magic before exit_time."""
    best = None
    for d in deals:
        if d.magic != magic:
            continue
        if d.entry != mt5.DEAL_ENTRY_IN:
            continue
        d_ts = pd.Timestamp(d.time, unit="s", tz="UTC")
        if d_ts > exit_time:
            continue
        if best is None or d_ts > best[0]:
            best = (d_ts, d.price)
    return best if best is not None else (None, None)


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", type=str, default=None,
                    help="Backfill a specific YYYY-MM-DD (default: today)")
    ap.add_argument("--days-back", type=int, default=0,
                    help="Backfill the last N days (e.g. --days-back 7). "
                         "Overrides --date.")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    if args.days_back > 0:
        # Backfill range
        target_dates = [now.date() - timedelta(days=i) for i in range(args.days_back, -1, -1)]
    elif args.date:
        target_dates = [datetime.strptime(args.date, "%Y-%m-%d").date()]
    else:
        target_dates = [now.date()]

    spec = init_account("live")
    sym = spec.symbol
    try:
        broker_off_td = get_broker_offset(spec.symbol)
        broker_off_h = int(broker_off_td.total_seconds() // 3600)
        for tgt in target_dates:
            run_one_day(tgt, sym, now, broker_off_h)
    finally:
        mt5.shutdown()
        kill_mt5_terminal()
    return 0


def run_one_day(target_date, sym, now, broker_off_h=3):
    """Process one trading day. Skips silently if no parent deals.
    Caller is responsible for MT5 init + teardown (so backfills don't
    re-init for every day)."""
    start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    # NOTE: do NOT clamp `end` to `now` — we want the full broker-day boundary.
    # MT5 history_deals_get is fine with end > current broker time (just returns
    # what exists). Clamping to real-UTC `now` was the original bug: MT5 reads
    # the datetime as broker wall-clock, so real-UTC `now` = broker (now-3h),
    # cutting off ~3h of recent trades. Per feedback_no_unverified_account_claims.md
    # (2026-05-15 incident).
    print(f"=== Daily trade log  {target_date}  (broker-time labels) ===\n")

    if True:
        # Include 12h pre-day lookback so we can match entries that
        # crossed midnight (very rare for ORB but cheap insurance).
        lookback_start = start - timedelta(hours=12)
        # MT5 reads datetime args as broker wall-clock. start/end have wall-clock
        # values matching broker midnights (since they're real-UTC midnights but
        # MT5 ignores the tz), so the broker-day query is correct as-is.
        deals = mt5.history_deals_get(lookback_start, end) or ()
        # Filter: keep deals where d.time (broker-as-epoch) is in [start, end)
        # using broker-wall-clock-as-epoch comparison. Note that for tz-aware UTC
        # datetimes, .timestamp() and "broker-wall-clock-as-epoch" coincide because
        # both treat the wall-clock value as UTC.
        ls_epoch = int(lookback_start.timestamp())
        end_epoch = int(end.timestamp())
        deals = [d for d in deals if ls_epoch <= d.time < end_epoch]

        # Collect exits for parent magics today. Dedupe by ticket only —
        # separate positions can close at identical (time, magic, price)
        # during cluster stops, so we must keep them distinct.
        exits = []
        seen_tickets = set()
        for d in deals:
            if d.magic not in STREAM_NAMES:
                continue
            if d.entry != mt5.DEAL_ENTRY_OUT:
                continue
            ts = pd.Timestamp(d.time, unit="s", tz="UTC")
            if ts < start or ts >= end:
                continue
            if d.ticket in seen_tickets:
                continue
            seen_tickets.add(d.ticket)
            exits.append({
                "ts_broker": ts,
                "magic": d.magic,
                "stream": STREAM_NAMES[d.magic],
                "side_parent": "BUY" if d.type == mt5.DEAL_TYPE_SELL else "SELL",
                "exit_price": d.price,
                "pnl": d.profit,
                "volume": d.volume,
                "outcome": parse_outcome(d.comment),
                "comment": d.comment,
            })
        print(f"  {len(exits)} closed parent deals today\n")

        if not exits:
            print("  (no deals)")
            return 0

        # Pull tick stream covering session range windows + 4h tail.
        last_exit = max(ev["ts_broker"] for ev in exits)
        tick_end = last_exit + timedelta(hours=4)
        arr = mt5.copy_ticks_range(sym, start, tick_end, mt5.COPY_TICKS_ALL)
        ticks = pd.DataFrame(arr)
        ticks["ts"] = pd.to_datetime(ticks["time_msc"], unit="ms", utc=True)
        ticks["mid"] = (ticks["bid"] + ticks["ask"]) / 2.0

        # H1 bars + ATR over the tick stream — used for regime classification.
        h1_bars = build_h1_bars(ticks)
        atr_series = compute_atr(h1_bars)

        # Enrich each exit with entry + session + regime features.
        for ev in exits:
            ent_ts, ent_px = find_parent_entry(deals, ev["magic"], ev["ts_broker"])
            ev["entry_ts"] = ent_ts
            ev["entry_price"] = ent_px
            ev["time_to_exit_min"] = (
                (ev["ts_broker"] - ent_ts).total_seconds() / 60.0 if ent_ts is not None else float("nan")
            )
            # Session is decided by the ENTRY time (NY entry held into next day
            # still belongs to NY). Fall back to exit time if entry missing.
            sess_ts = ent_ts if ent_ts is not None else ev["ts_broker"]
            sess, rng_start, rng_end = session_for_deal(sess_ts, broker_off_h)
            ev["session"] = sess
            ev["range_pts"] = compute_range_pts(ticks, rng_start, rng_end) if rng_start is not None else float("nan")
            ev["atr_h1_20_pts"] = lookup_atr_pts_at(atr_series, rng_start) if rng_start is not None else float("nan")
            ev["range_atr_ratio"] = (
                ev["range_pts"] / ev["atr_h1_20_pts"]
                if (not pd.isna(ev["range_pts"]) and not pd.isna(ev["atr_h1_20_pts"]) and ev["atr_h1_20_pts"] > 0)
                else float("nan")
            )
            ev["regime"] = classify_regime(ev["range_pts"], ev["range_atr_ratio"])

        # --- Console summary ---
        print(f"  {'Time':<19} {'Str':<4} {'Side':<4} {'Out':<4} {'Sess':<5} {'Range':>5} "
              f"{'Reg':<7} {'TtX':>5}  {'Entry':>9}  {'Exit':>9}  {'PnL$':>10}")
        print(f"  {'-'*19} {'-'*4} {'-'*4} {'-'*4} {'-'*5} {'-'*5} {'-'*7} {'-'*5}  "
              f"{'-'*9}  {'-'*9}  {'-'*10}")
        tot_pnl = 0.0
        for ev in sorted(exits, key=lambda e: e["ts_broker"]):
            rng_s = f"{ev['range_pts']:>5.0f}" if not pd.isna(ev["range_pts"]) else "  n/a"
            ttx_s = f"{ev['time_to_exit_min']:>5.0f}" if not pd.isna(ev["time_to_exit_min"]) else "  n/a"
            ep_s = f"{ev['entry_price']:>9.2f}" if ev["entry_price"] is not None else "      n/a"
            print(f"  {ev['ts_broker'].strftime('%Y-%m-%d %H:%M'):<19} "
                  f"{ev['stream']:<4} {ev['side_parent']:<4} {ev['outcome']:<4} "
                  f"{ev['session']:<5} {rng_s} {ev['regime']:<7} {ttx_s}  {ep_s}  {ev['exit_price']:>9.2f}  "
                  f"{ev['pnl']:>+10.2f}")
            tot_pnl += ev["pnl"]
        print(f"\n  Total PnL today: ${tot_pnl:+,.2f}")

        # Per-session summary
        sess_breakdown = {}
        for ev in exits:
            k = (ev["session"], ev["outcome"])
            sess_breakdown[k] = sess_breakdown.get(k, [0, 0.0])
            sess_breakdown[k][0] += 1
            sess_breakdown[k][1] += ev["pnl"]
        print()
        print(f"  --- Per-session × outcome ---")
        for (sess, out), (n, pnl) in sorted(sess_breakdown.items()):
            print(f"    {sess:<5} {out:<4}: {n:3d} trades  ${pnl:+,.2f}")

        # Range-bucket summary (for the regime-gate thesis)
        print()
        print(f"  --- PnL by range_pts bucket ---")
        buckets = [(0, 1000), (1000, 2500), (2500, 4000), (4000, 99999)]
        for lo, hi in buckets:
            in_b = [ev for ev in exits if not pd.isna(ev["range_pts"]) and lo <= ev["range_pts"] < hi]
            if not in_b:
                continue
            wins = sum(1 for ev in in_b if ev["pnl"] > 0)
            net = sum(ev["pnl"] for ev in in_b)
            print(f"    range [{lo:>5}-{hi:>5}): {len(in_b):3d} trades  "
                  f"{wins}W/{len(in_b)-wins}L  net ${net:+,.2f}")

        # --- Idempotent CSV upsert keyed on ts_broker (unique per deal) ---
        out_dir = ROOT / "output"
        out_dir.mkdir(exist_ok=True)
        csv_path = out_dir / "daily_trade_log.csv"
        cols = ["date", "ts_broker", "stream", "magic", "side", "outcome",
                "entry_price", "exit_price", "pnl", "volume",
                "time_to_exit_min", "session", "range_pts",
                "atr_h1_20_pts", "range_atr_ratio", "regime", "comment"]

        def fmt_row(ev):
            return {
                "date": ev["ts_broker"].date().isoformat(),
                "ts_broker": ev["ts_broker"].isoformat(),
                "stream": ev["stream"],
                "magic": ev["magic"],
                "side": ev["side_parent"],
                "outcome": ev["outcome"],
                "entry_price": ev["entry_price"] if ev["entry_price"] is not None else "",
                "exit_price": ev["exit_price"],
                "pnl": round(ev["pnl"], 2),
                "volume": ev["volume"],
                "time_to_exit_min": round(ev["time_to_exit_min"], 2) if not pd.isna(ev["time_to_exit_min"]) else "",
                "session": ev["session"],
                "range_pts": round(ev["range_pts"], 1) if not pd.isna(ev["range_pts"]) else "",
                "atr_h1_20_pts": round(ev["atr_h1_20_pts"], 1) if not pd.isna(ev["atr_h1_20_pts"]) else "",
                "range_atr_ratio": round(ev["range_atr_ratio"], 2) if not pd.isna(ev["range_atr_ratio"]) else "",
                "regime": ev["regime"],
                "comment": ev["comment"],
            }

        # Read existing rows; drop rows for THIS target_date so we replace them cleanly.
        existing = []
        if csv_path.exists():
            try:
                existing_df = pd.read_csv(csv_path, dtype={"comment": str}, keep_default_na=False)
                existing_df = existing_df[existing_df["date"] != target_date.isoformat()]
                existing = existing_df.to_dict(orient="records")
            except Exception as e:
                print(f"  [warn] could not read existing CSV ({e}); rewriting from scratch")
                existing = []

        new_rows = [fmt_row(ev) for ev in sorted(exits, key=lambda e: e["ts_broker"])]
        all_rows = sorted(existing + new_rows, key=lambda r: r["ts_broker"])

        with csv_path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in all_rows:
                w.writerow({c: r.get(c, "") for c in cols})
        print(f"\n  Wrote {len(new_rows)} new rows for {target_date}, total {len(all_rows)} in {csv_path}")


if __name__ == "__main__":
    sys.exit(main())
