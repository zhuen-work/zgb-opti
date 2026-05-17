"""Auto-update forward_projection.json hedge fields from a hedge WFO run log.

Closes the gap in [[project_v3_weekly_reopt_workflow]] Step 5b: after running
sim_wfo_hedge_reverse.py each Saturday, this script parses the portfolio
compare + per-stream contribution blocks from the run log and updates the
hedge-derived fields in output/forward_projection.json, then optionally pushes
to dt818-console.

The script:
  1. Locates the latest output/wfo_hedge_reverse_*/run.log
  2. Parses:
     - Rank 1 line  ->  avg_oos_slope_pct
     - PORTFOLIO COMPARE block  ->  raw_sim_77d (v2.1 vs v3 portfolio numbers)
     - Per-stream contribution table  ->  per_stream_sim_contribution_<balance>
  3. Recomputes weekly_live + daily_live using compound-rate method (Method B)
     at the current baseline_balance from the existing projection
  4. Updates output/forward_projection.json in-place
  5. Pushes to dt818-console via cf_publish.publish_projection (unless --no-push)

Usage:
  python scripts/update_v3_projection_from_wfo.py                       # latest run.log + push
  python scripts/update_v3_projection_from_wfo.py --log <path>          # specific log
  python scripts/update_v3_projection_from_wfo.py --no-push             # update file only
  python scripts/update_v3_projection_from_wfo.py --dry-run             # print, don't write
"""
from __future__ import annotations

import argparse
import json
import json as _json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

# Load .env so CONSOLE_API_BASE / CONSOLE_READ_TOKEN are available for the
# DB-saved_at fetch (cf_publish loads .env lazily on first publish call, but
# we need the env vars BEFORE that for the DB-aware saved_at logic).
from zgb_sim.cf_publish import _load_dotenv  # noqa: E402
_load_dotenv()

PROJECTION_PATH = ROOT / "output" / "forward_projection.json"
HAIRCUT_NP = 0.94


def slope_to_decay(avg_slope_pct: float) -> float:
    if avg_slope_pct >= -10:
        return 0.90
    if avg_slope_pct >= -30:
        return 0.80
    if avg_slope_pct >= -50:
        return 0.75
    return 0.65


def find_latest_log() -> Path | None:
    """Return the latest output/wfo_hedge_reverse_*/run.log by mtime."""
    candidates = list((ROOT / "output").glob("wfo_hedge_reverse_*/run.log"))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def parse_money(s: str) -> float:
    """Parse strings like '$+1,722,097' or '-$2,240' -> float."""
    s = s.strip().replace("$", "").replace(",", "").replace("p", "")
    return float(s)


def parse_log(log_path: Path) -> dict:
    """Parse the WFO log. Returns dict with all extracted fields, or empty if parse fails."""
    text = log_path.read_text(encoding="utf-8", errors="replace")
    out = {"log_path": str(log_path), "windows_tag": log_path.parent.name.replace("wfo_hedge_reverse_", "")}

    # ---- Rank 1 line (avg slope) ----
    # "  1     FAIL 4/4 $+138,974  19.8%     +7005  -46.9%  $+49061 -> ..."
    rank_re = re.compile(r"^\s*1\s+(?:PASS|FAIL)\s+\d+/\d+\s+\$\S+\s+\S+%\s+[+-]?\d+\s+([+-]?\d+\.\d+)%", re.M)
    m = rank_re.search(text)
    if m:
        out["avg_oos_slope_pct"] = float(m.group(1))

    # ---- PORTFOLIO COMPARE block ----
    # Look for "Variant ... NP ... DD% ... NP/DD$ ... PF ... Trades" header then 3 rows.
    # Money tokens may have a space after $ (e.g. "$ +73,832") or not ("$+73,832").
    money_tok = r"(\$\s*[+-]?[\d,]+)"
    pc_block = re.search(
        r"PORTFOLIO COMPARE.*?Variant\s+NP\s+DD%\s+NP/DD\$\s+PF\s+Trades\s*\n"
        rf"\s+no-reverse\s+{money_tok}\s+([\d.]+)%\s+(\S+)\s+(\S+)\s+(\d+)\s*\n"
        rf"\s+\+reverse\s+{money_tok}\s+([\d.]+)%\s+(\S+)\s+(\S+)\s+(\d+)\s*\n"
        rf"\s+delta\s+{money_tok}\s+([+-][\d.]+)p\s+(\S+)",
        text, re.S)
    if pc_block:
        out["v2_1_no_hedge_np"] = parse_money(pc_block.group(1))
        out["v2_1_dd_pct"] = float(pc_block.group(2))
        out["v2_1_ndd"] = float(pc_block.group(3))
        out["v2_1_pf"] = float(pc_block.group(4))
        out["v2_1_trades"] = int(pc_block.group(5))
        out["v3_with_hedge_np"] = parse_money(pc_block.group(6))
        out["v3_dd_pct"] = float(pc_block.group(7))
        out["v3_ndd"] = float(pc_block.group(8))
        out["v3_pf"] = float(pc_block.group(9))
        out["v3_trades"] = int(pc_block.group(10))
        out["v3_hedge_only_np"] = parse_money(pc_block.group(11))
        out["dd_delta_pp"] = float(pc_block.group(12))
        out["ndd_delta"] = float(pc_block.group(13))
        out["v3_parent_trades"] = out["v2_1_trades"]
        out["v3_hedge_trades"] = out["v3_trades"] - out["v2_1_trades"]

    # ---- Per-stream contribution table ----
    # Old (pre-2026-05-17): "  S1      5.0   1.0   $+152,971      268 $ +95,627     87    28%"
    # New (R6+, with buf col): "  S1      5.0   1.0   0     $ +10,006      268 $  +6,377     87    28%"
    # Money tokens sometimes have space after $ ($ +6,377), sometimes not ($+164,738).
    money = r"(\$\s*[+-]?[\d,]+)"
    stream_re = re.compile(
        rf"^\s+(S\d)\s+([\d.]+)\s+([\d.]+)\s+(?:(\d+)\s+)?{money}\s+(\d+)\s+{money}\s+(\d+)\s+(\d+)%\s*$", re.M)
    streams = {}
    for m in stream_re.finditer(text):
        s = m.group(1)
        streams[s] = {
            "tp_mult": float(m.group(2)),
            "sl_mult": float(m.group(3)),
            "buffer_pts": int(m.group(4)) if m.group(4) is not None else 0,
            "parent_np": parse_money(m.group(5)),
            "parent_n": int(m.group(6)),
            "hedge_np": parse_money(m.group(7)),
            "hedge_n": int(m.group(8)),
            "hedge_wr": float(m.group(9)) / 100.0,
        }
    if streams:
        out["per_stream"] = streams

    # ---- Sim deposit from PORTFOLIO COMPARE header (e.g. "$10k deposit, 1.5% per stream") ----
    # Parsing this prevents stale `deposit_sim` in projection from skewing the
    # compound-weekly-rate normalization (cost us 4.83x understatement on 2026-05-17 R7).
    dep = re.search(r"PORTFOLIO COMPARE.*?\$([\d.]+)k\s*deposit", text, re.S)
    if dep:
        out["sim_deposit"] = float(dep.group(1)) * 1000.0

    # ---- Sanity window from log header (e.g., "Data 2026-02-28->2026-05-16") ----
    win = re.search(r"Data\s+(\d{4}-\d{2}-\d{2})->(\d{4}-\d{2}-\d{2})", text)
    if win:
        out["sim_start"] = win.group(1)
        out["sim_end"] = win.group(2)
        from datetime import date
        s_d = date.fromisoformat(out["sim_start"])
        e_d = date.fromisoformat(out["sim_end"])
        out["sim_days"] = (e_d - s_d).days
        out["sim_weeks"] = max(1, out["sim_days"] // 7)

    # ---- Winner (tp_mult, sl_mult, global) ----
    # "WINNER: exp_min=240  f1_sec=1800  regime_gate=off"
    w = re.search(r"WINNER:\s+exp_min=(\d+)\s+f1_sec=(\d+)\s+regime_gate=(\w+)", text)
    if w:
        out["winner_exp_min"] = int(w.group(1))
        out["winner_f1_sec"] = int(w.group(2))
        out["winner_regime_gate"] = w.group(3)

    return out


def build_projection_update(parsed: dict, existing: dict) -> dict:
    """Merge parsed WFO numbers into the existing projection structure."""
    proj = dict(existing)  # shallow copy

    # Sim baseline + balance. Prefer the deposit parsed from the WFO log
    # over the existing projection's deposit_sim — the latter can go stale
    # if a one-off sim ran at a non-standard deposit (e.g. R6 ran at $149k).
    sim_baseline = float(parsed.get("sim_deposit") or existing.get("deposit_sim", 10_000.0))
    live_balance = float(existing.get("baseline_balance", sim_baseline))
    proj["deposit_sim"] = sim_baseline  # write back so it stays in sync with the source log
    weeks = int(parsed.get("sim_weeks", existing.get("sanity_window", {}).get("iso_weeks", 11)))

    # Slope -> decay
    slope_pct = parsed.get("avg_oos_slope_pct", existing.get("avg_oos_slope_pct", -40.0))
    decay = slope_to_decay(slope_pct)
    combined = HAIRCUT_NP * decay

    proj["avg_oos_slope_pct"] = slope_pct
    proj["decay_factor"] = decay
    proj["combined_haircut"] = combined
    proj["live_haircut_np"] = HAIRCUT_NP

    if "sim_start" in parsed:
        proj["sanity_window"] = {
            "start": parsed["sim_start"], "end": parsed["sim_end"], "iso_weeks": weeks,
        }

    # raw_sim_<weeks>d block (e.g. raw_sim_77d). Convention: keep one block per sim.
    raw_key = f"raw_sim_{parsed.get('sim_days', 77)}d"
    raw_block = {
        "v2_1_no_hedge_np": parsed.get("v2_1_no_hedge_np"),
        "v3_with_hedge_np": parsed.get("v3_with_hedge_np"),
        "v3_hedge_only_np": parsed.get("v3_hedge_only_np"),
        "v3_dd_pct": parsed.get("v3_dd_pct"),
        "v3_ndd": parsed.get("v3_ndd"),
        "v3_pf": parsed.get("v3_pf"),
        "v3_trades": parsed.get("v3_trades"),
        "v3_hedge_trades": parsed.get("v3_hedge_trades"),
        "v3_parent_trades": parsed.get("v3_parent_trades"),
        "compound_weekly_rate_v3": (1 + parsed["v3_with_hedge_np"] / sim_baseline) ** (1 / weeks) - 1
            if parsed.get("v3_with_hedge_np") is not None else None,
        "compound_weekly_rate_v2_1": (1 + parsed["v2_1_no_hedge_np"] / sim_baseline) ** (1 / weeks) - 1
            if parsed.get("v2_1_no_hedge_np") is not None else None,
    }
    proj[raw_key] = {k: v for k, v in raw_block.items() if v is not None}

    # Remove any stale raw_sim_*d keys with different day counts
    for k in list(proj.keys()):
        if re.match(r"^raw_sim_\d+d$", k) and k != raw_key:
            del proj[k]

    # Per-stream sim contribution (use balance suffix for clarity)
    per_stream = parsed.get("per_stream", {})
    if per_stream:
        bal_k_str = f"{int(round(live_balance / 1000))}k" if live_balance >= 100_000 else f"{int(round(live_balance))}usd"
        contrib_key = f"per_stream_sim_contribution_{parsed.get('sim_days', 77)}d_{bal_k_str}"
        proj[contrib_key] = {
            s: {
                "parent_np": d["parent_np"],
                "hedge_np": d["hedge_np"],
                "combined": d["parent_np"] + d["hedge_np"],
                "hedge_n": d["hedge_n"],
                "hedge_wr": d["hedge_wr"],
            }
            for s, d in per_stream.items()
        }
        # Clean up old contribution keys with different sim_days
        for k in list(proj.keys()):
            if k.startswith("per_stream_sim_contribution_") and k != contrib_key:
                del proj[k]

        # per_stream_hedge_cfg from the same parsed data (source = WFO winner)
        proj["per_stream_hedge_cfg"] = {
            s: {"tp_mult": d["tp_mult"], "sl_mult": d["sl_mult"],
                "r_ratio": round(d["tp_mult"] / d["sl_mult"], 2)}
            for s, d in per_stream.items()
        }

    # Weekly + daily projections (Method B: compound rate × live balance × haircut)
    v3_np = parsed.get("v3_with_hedge_np")
    v2_np = parsed.get("v2_1_no_hedge_np")
    if v3_np is not None and v2_np is not None and sim_baseline > 0:
        cr_v3 = (1 + v3_np / sim_baseline) ** (1 / weeks) - 1
        cr_v2 = (1 + v2_np / sim_baseline) ** (1 / weeks) - 1

        weekly_mean_v3 = cr_v3 * live_balance * combined
        weekly_mean_v2 = cr_v2 * live_balance * combined
        # Rough distribution: p10/p90 ratio observed historically = 0.34 / 1.55
        proj["weekly_live"] = {
            "mean_np": round(weekly_mean_v3),
            "median_np": round(weekly_mean_v3 * 1.09),
            "p10_np": round(weekly_mean_v3 * 0.34),
            "p90_np": round(weekly_mean_v3 * 1.55),
            "worst_np": round(weekly_mean_v3 * -0.52),
            "best_np": round(weekly_mean_v3 * 2.74),
            "std_np": round(weekly_mean_v3 * 0.49),
            "green_week_prob": 0.91,
            "expected_roi_pct": round(cr_v3 * combined * 100, 2),
            "n_sample_weeks": weeks,
            "view_c_planning_range_low": round(weekly_mean_v3 * 0.55),
            "view_c_planning_range_high": round(weekly_mean_v3 * 1.10),
            "view_c_tolerance_red_usd": round(weekly_mean_v3 * -0.74),
            # Parent / hedge split (proportional to NP shares)
            "parent_contribution_mean_np": round(weekly_mean_v3 * v2_np / v3_np),
            "hedge_contribution_mean_np":  round(weekly_mean_v3 * (v3_np - v2_np) / v3_np),
            "hedge_share_pct": round((v3_np - v2_np) / v3_np * 100, 1),
        }
        proj["daily_live"] = {
            "mean_np": round(weekly_mean_v3 / 5),
            "p10_np": round(weekly_mean_v3 * 0.34 / 5),
            "p90_np": round(weekly_mean_v3 * 1.55 / 5),
            "expected_roi_pct": round(cr_v3 * combined * 100 / 5, 2),
            "parent_contribution_mean": round(weekly_mean_v3 / 5 * v2_np / v3_np),
            "hedge_contribution_mean":  round(weekly_mean_v3 / 5 * (v3_np - v2_np) / v3_np),
        }

        proj["comparison_vs_v2_1"] = {
            "v2_1_weekly_mean": round(weekly_mean_v2),
            "v3_weekly_mean": round(weekly_mean_v3),
            "delta_weekly": round(weekly_mean_v3 - weekly_mean_v2),
            "delta_pct": round((weekly_mean_v3 - weekly_mean_v2) / max(weekly_mean_v2, 1) * 100, 1),
            "v2_1_decay_factor": slope_to_decay(slope_pct - 10),  # rough: v2.1 slope is ~10pp worse
            "v3_decay_factor": decay,
            "note": (f"v3 uses decay {decay:.2f} based on observed avg OOS slope "
                     f"{slope_pct:.1f}%. v2.1 decay is approximated 1 bucket worse."),
        }

    # Guarantee this row sorts newest under the console's ORDER BY saved_at DESC.
    # Take max(now_utc, local_saved_at, DB_latest_saved_at) + 1s.
    # Required because some legacy rows have manually future-dated saved_at values.
    from datetime import timedelta
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    candidates = [now_iso]
    if existing.get("saved_at"):
        candidates.append(existing["saved_at"])
    # Fetch DB's latest saved_at via the public API
    try:
        import urllib.request as _u
        api = os.environ.get("CONSOLE_API_BASE")
        tok = os.environ.get("CONSOLE_READ_TOKEN")
        if api and tok:
            req = _u.Request(api.rstrip("/") + "/api/today",
                              headers={"Authorization": f"Bearer {tok}",
                                       "User-Agent": "update-v3-projection/1.0"})
            with _u.urlopen(req, timeout=10) as resp:
                body = _json.loads(resp.read().decode())
            db_saved = body.get("projection", {}).get("saved_at")
            if db_saved:
                candidates.append(db_saved)
    except Exception:
        pass
    latest = max(candidates)
    try:
        latest_dt = datetime.fromisoformat(latest.replace("Z", "+00:00"))
        proj["saved_at"] = (latest_dt + timedelta(seconds=1)).replace(microsecond=0).isoformat()
    except Exception:
        proj["saved_at"] = now_iso
    proj["method"] = "view_c_decay_adjusted_auto_from_wfo_log"
    proj["wfo_log_source"] = parsed.get("log_path")

    if "winner_regime_gate" in parsed:
        # carry the global hedge knobs (used by console / readers as audit metadata)
        proj["global_hedge_cfg"] = {
            "expire_minutes": parsed["winner_exp_min"],
            "max_seconds_after_entry": parsed["winner_f1_sec"],
            "regime_gate": parsed["winner_regime_gate"],
        }

    return proj


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", type=str, default=None,
                    help="Path to specific WFO run.log. Default: latest output/wfo_hedge_reverse_*/run.log")
    ap.add_argument("--no-push", action="store_true",
                    help="Update forward_projection.json but don't push to dt818-console")
    ap.add_argument("--dry-run", action="store_true",
                    help="Parse + print the proposed update, don't write")
    args = ap.parse_args()

    log_path = Path(args.log) if args.log else find_latest_log()
    if log_path is None or not log_path.exists():
        print(f"[error] No WFO log found. Run sim_wfo_hedge_reverse.py first or pass --log <path>.")
        return 1

    print(f"  Parsing WFO log: {log_path}")
    parsed = parse_log(log_path)
    if not parsed.get("per_stream") or "v3_with_hedge_np" not in parsed:
        print(f"[error] Log doesn't contain expected portfolio compare / per-stream blocks.")
        print(f"        Parsed keys: {list(parsed.keys())}")
        return 1

    print(f"    avg OOS slope: {parsed.get('avg_oos_slope_pct', '?')}%")
    print(f"    v2.1 NP: ${parsed['v2_1_no_hedge_np']:+,.0f}    "
          f"v3 NP: ${parsed['v3_with_hedge_np']:+,.0f}    "
          f"hedge delta: ${parsed['v3_hedge_only_np']:+,.0f}")
    print(f"    Per-stream picks: " + ", ".join(
        f"{s}(tp={d['tp_mult']}, sl={d['sl_mult']})" for s, d in parsed["per_stream"].items()))

    if not PROJECTION_PATH.exists():
        print(f"[warn] {PROJECTION_PATH} doesn't exist; will create a new one.")
        existing = {"deposit_sim": 10_000.0, "baseline_balance": 10_000.0}
    else:
        existing = json.loads(PROJECTION_PATH.read_text(encoding="utf-8"))

    proj = build_projection_update(parsed, existing)

    if args.dry_run:
        print("\n  [DRY RUN] Proposed update (key fields):")
        print(f"    weekly_live.mean_np: {proj['weekly_live']['mean_np']:+,}")
        print(f"    weekly_live.hedge_contribution_mean_np: {proj['weekly_live']['hedge_contribution_mean_np']:+,}")
        print(f"    weekly_live.parent_contribution_mean_np: {proj['weekly_live']['parent_contribution_mean_np']:+,}")
        print(f"    weekly_live.hedge_share_pct: {proj['weekly_live']['hedge_share_pct']}%")
        print(f"    weekly_live.expected_roi_pct: {proj['weekly_live']['expected_roi_pct']}%")
        print(f"    comparison_vs_v2_1: delta_weekly ${proj['comparison_vs_v2_1']['delta_weekly']:+,}  "
              f"({proj['comparison_vs_v2_1']['delta_pct']:+.1f}%)")
        return 0

    PROJECTION_PATH.write_text(json.dumps(proj, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n  Updated {PROJECTION_PATH}")
    print(f"    weekly_live.mean_np: ${proj['weekly_live']['mean_np']:+,}  "
          f"(parent ${proj['weekly_live']['parent_contribution_mean_np']:+,} + "
          f"hedge ${proj['weekly_live']['hedge_contribution_mean_np']:+,})")

    if not args.no_push:
        try:
            from zgb_sim.cf_publish import publish_projection
            ok = publish_projection(proj)
            print(f"  {'Pushed to dt818-console.' if ok else 'Push FAILED -- check .env'}")
        except Exception as e:
            print(f"  [cf_publish] skipped: {type(e).__name__}: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
