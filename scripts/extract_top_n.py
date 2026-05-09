"""Reusable WFO post-processor — extract top-N ranked candidates as setfile blocks.

After any joint-session ORB WFO completes (caches IS/OOS parquets in --wfo-dir),
this script re-runs Phase D ranking using the production ranker (P0 + plateau +
prof_count primary tiebreak) and emits top-N candidates in three formats:

  1. Top-N table with cfg + ranking metadata
  2. Setfile-ready block per stream (paste into your setfile)
  3. Full setfile (--out path.set), with risk_pct = total_risk / N per stream

Optionally appends parallel hedge sub-stream blocks (_HEDGE_S{N}_*) when
--hedge-cfg-json is provided. Hedge magics use the parent magic + 4000 offset
(1111->5111, 2222->5222, 3333->5333). Hedge risk = same as parent per-stream.

Usage examples:
  # Just print the top-3 from the May 2 WFO
  python scripts/extract_top_n.py --wfo-dir output/wfo_orb_may2 --n 3

  # Generate a 3% risk setfile from top 3 (no hedge)
  python scripts/extract_top_n.py --wfo-dir output/wfo_orb_may2 --n 3 \\
      --total-risk 3.0 --out configs/sets/dt818_pro_3pct_may2_reopt_may16.set

  # With hedge (after sim_wfo_hedge_global.py produces winner.json)
  python scripts/extract_top_n.py --wfo-dir output/wfo_orb_may2 --n 3 \\
      --hedge-cfg-json output/wfo_hedge_global_may2/winner.json \\
      --total-risk 3.0 --out configs/sets/dt818_pro_3pct_with_hedge.set

  # Custom magics
  python scripts/extract_top_n.py --wfo-dir output/wfo_orb_may2 --n 3 \\
      --magics 1111 2222 3333 --total-risk 3.0
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zgb_sim.orb import ORBConfig
from zgb_sim.wfo_helpers import (WINDOWS_MAY9 as WINDOWS, rank_with_p0,
                                  print_rank_sanity_check)

DEFAULT_MAGICS = [1111, 2222, 3333, 4444, 5555]
DEFAULT_RISK = 3.0


def row_to_cfg(row, comment: str = "ORB") -> ORBConfig:
    return ORBConfig(
        risk_pct=3.0,  # placeholder; overridden when emitting setfile
        range_minutes=int(row["range_minutes"]),
        buffer_pts=0,
        min_range_pts=200, max_range_pts=5000,
        fixed_sl_pts=int(row["fixed_sl_pts"]),
        rr_ratio=float(row["rr_ratio"]),
        half_tp_ratio=round(float(row["half_tp_ratio"]), 2),
        pending_expire_minutes=240,
        daily_target_pct=float(row.get("daily_target_pct", 0.0)),
        daily_loss_pct=float(row.get("daily_loss_pct", 0.0)),
        ldn_enabled=True, ldn_start_hour=7,
        ny_enabled=True,  ny_start_hour=13,
        comment=comment,
    )


def extract_top_n(wfo_dir: Path, n: int):
    """Re-rank cached parquets and return top-N candidate dicts."""
    is_per = {}
    oos_per = {}
    for label, _, _, _, _ in WINDOWS:
        is_path = wfo_dir / f"is_{label}.parquet"
        oos_path = wfo_dir / f"oos_{label}.parquet"
        # Try p1_ prefix (used by per-phase WFO mode)
        if not is_path.exists():
            is_path = wfo_dir / f"p1_is_{label}.parquet"
        if not oos_path.exists():
            oos_path = wfo_dir / f"p1_oos_{label}.parquet"
        if not is_path.exists() or not oos_path.exists():
            raise FileNotFoundError(f"Missing IS/OOS parquets for window {label} in {wfo_dir}")
        is_per[label]  = pd.read_parquet(is_path)
        oos_per[label] = pd.read_parquet(oos_path)

    candidates = [row_to_cfg(r) for _, r in oos_per["W1"].iterrows()]
    full_grid  = [row_to_cfg(r) for _, r in is_per["W1"].iterrows()]

    ranked = rank_with_p0(candidates, oos_per, WINDOWS, decay_threshold=-0.25,
                          grid_configs=full_grid, is_per_window=is_per)
    return ranked[:n]


def setfile_block(stream_idx: int, magic: int, cfg: ORBConfig) -> list[str]:
    """Generate the per-stream setfile block (S1/S2/S3...)."""
    label = f"S{stream_idx}"
    return [
        f"; ===== ORB_{label} — rank {stream_idx} (magic {magic}) =====",
        f"_ORB_{label}_Enabled=true",
        f"_ORB_{label}_Magic={magic}||{magic}||1||{magic}||{magic}||N",
        f"_ORB_{label}_Comment=ORB_{label}",
        f"_ORB_{label}_RangeMinutes={cfg.range_minutes}||{cfg.range_minutes}||1||{cfg.range_minutes}||{cfg.range_minutes}||N",
        f"_ORB_{label}_FixedSL_Pts={cfg.fixed_sl_pts}||{cfg.fixed_sl_pts}||1||{cfg.fixed_sl_pts}||{cfg.fixed_sl_pts}||N",
        f"_ORB_{label}_RR_Ratio={cfg.rr_ratio}||{cfg.rr_ratio}||1||{cfg.rr_ratio}||{cfg.rr_ratio}||N",
        f"_ORB_{label}_HalfTP_Ratio={cfg.half_tp_ratio}||{cfg.half_tp_ratio}||1||{cfg.half_tp_ratio}||{cfg.half_tp_ratio}||N",
        f"_ORB_{label}_PendingExpireMinutes={cfg.pending_expire_minutes}||{cfg.pending_expire_minutes}||1||{cfg.pending_expire_minutes}||{cfg.pending_expire_minutes}||N",
        f"_ORB_{label}_DailyTargetPct={cfg.daily_target_pct}||{cfg.daily_target_pct}||1||{cfg.daily_target_pct}||{cfg.daily_target_pct}||N",
        f"_ORB_{label}_DailyLossPct={cfg.daily_loss_pct}||{cfg.daily_loss_pct}||1||{cfg.daily_loss_pct}||{cfg.daily_loss_pct}||N",
        "",
    ]


def hedge_magic_for(stream_idx: int) -> int:
    """Hedge magic = 5111 / 5222 / 5333 for streams 1 / 2 / 3.

    Derived from stream index (not parent magic) so the pattern always holds
    regardless of what parent magics are configured. The leading '5' indicates
    hedge sub-stream; the trailing repeated digit matches the stream index.
    """
    return 5000 + 111 * stream_idx


def hedge_setfile_block(stream_idx: int, parent_magic: int, hedge_cfg: dict,
                          per_stream_risk: float) -> list[str]:
    """Generate _HEDGE_S{N}_* block parallel to the parent _ORB_S{N}_* block.

    hedge_cfg keys: buffer_pts, fixed_sl_pts, rr_ratio, expire_minutes.
    Hedge magic = hedge_magic_for(stream_idx) (5111/5222/5333). Comment = ORB_S{N}h.
    Risk = per_stream_risk (mirrors parent allocation per user spec).
    """
    label = f"S{stream_idx}"
    hmagic = hedge_magic_for(stream_idx)
    buf = hedge_cfg["buffer_pts"]
    sl  = hedge_cfg["fixed_sl_pts"]
    rr  = hedge_cfg["rr_ratio"]
    exp = hedge_cfg["expire_minutes"]
    # F1 fast-SL filter: 3600s = 60min cutoff (validated 2026-05-08).
    # 0 disables the filter (= old always-on hedge behavior).
    f1_secs = hedge_cfg.get("max_seconds_after_entry", 3600)
    return [
        f"; ===== HEDGE_{label} — global hedge for ORB_{label} (magic {hmagic}) =====",
        f"_HEDGE_{label}_Enabled=true",
        f"_HEDGE_{label}_Magic={hmagic}||{hmagic}||1||{hmagic}||{hmagic}||N",
        f"_HEDGE_{label}_Comment=ORB_{label}h",
        f"_HEDGE_{label}_ParentMagic={parent_magic}||{parent_magic}||1||{parent_magic}||{parent_magic}||N",
        f"_HEDGE_{label}_BufferPts={buf}||{buf}||1||{buf}||{buf}||N",
        f"_HEDGE_{label}_FixedSL_Pts={sl}||{sl}||1||{sl}||{sl}||N",
        f"_HEDGE_{label}_RR_Ratio={rr}||{rr}||1||{rr}||{rr}||N",
        f"_HEDGE_{label}_ExpireMinutes={exp}||{exp}||1||{exp}||{exp}||N",
        f"_HEDGE_{label}_RiskPct={per_stream_risk}||{per_stream_risk}||1||{per_stream_risk}||{per_stream_risk}||N",
        f"_HEDGE_{label}_DailyLossPct=0.0||0.0||1||0.0||0.0||N",
        f"_HEDGE_{label}_MaxSecondsAfterEntry={f1_secs}||{f1_secs}||1||{f1_secs}||{f1_secs}||N",
        "",
    ]


def full_setfile(top_n_winners: list, magics: list[int],
                  total_risk: float, wfo_dir: Path,
                  hedge_cfg: dict | None = None,
                  per_stream_hedge: dict | None = None) -> str:
    """Build the complete setfile.

    per_stream_hedge: optional dict {S1: cfg, S2: cfg, S3: cfg}. Takes precedence
    over global hedge_cfg if both supplied.
    """
    """Generate a complete N-stream setfile."""
    n = len(top_n_winners)
    per_stream_risk = round(total_risk / n, 4)
    today = date.today().isoformat()
    lines = [
        f"; DT818_pro - {n}-stream ORB rank portfolio, {total_risk}% total per setup",
        f"; Generated {today} from {wfo_dir.name}",
        ";",
        f"; Architecture: top {n} ranks from joint-session WFO running in parallel.",
        f"; _RiskPct = {per_stream_risk} per stream so total when all {n} fire together = {total_risk}%.",
        "; Each stream has both LDN and NY enabled. Independent magics + caps.",
        ";",
        "; Top-N ranks selected:",
    ]
    for i, w in enumerate(top_n_winners, 1):
        c = w["cfg"]
        slope = w.get("slope", 0)
        plat = w.get("plateau_score", 0) or 0
        lines.append(f";   Rank {i}: Range={c.range_minutes}/SL={c.fixed_sl_pts}/RR={c.rr_ratio}/"
                     f"HTP={c.half_tp_ratio}  P0={'PASS' if w.get('p0_pass') else 'FAIL'} "
                     f"slope={slope:+.1%} plat=${plat:,.0f}")
    lines += [
        ";",
        "; Spread guard: _MaxSpreadPts=50 (Vantage max observed 32pt; 50pt = 56% safety margin)",
        "",
        "; ===== Global account =====",
        "_CapitalProtectionAmount=0.0||0.0||1||0.0||0.0||N",
        f"_RiskPct={per_stream_risk}||{per_stream_risk}||1||{per_stream_risk}||{per_stream_risk}||N",
        "_LotMode=1||1||1||1||1||N",
        "TierBase=2000||2000||1||2000||2000||N",
        "LotStep=0.01||0.01||1||0.01||0.01||N",
        "_MaxSpreadPts=50||50||1||50||50||N",
        "",
        "; ===== ORB shared params =====",
        "_BrokerGMTOffsetHours=0||0||1||0||0||N  ; DEPRECATED post 2026-05-07 (EA uses TimeGMT)",
        "_ORB_BufferPts=0||0||1||0||0||N",
        "_ORB_MinRangePts=200||200||1||200||200||N",
        "_ORB_MaxRangePts=5000||5000||1||5000||5000||N",
        "_ORB_LDN_Enabled=true",
        "_ORB_LDN_StartHour=7||7||1||7||7||N",
        "_ORB_NY_Enabled=true",
        "_ORB_NY_StartHour=13||13||1||13||13||N",
        "",
    ]
    for i, (w, mag) in enumerate(zip(top_n_winners, magics), 1):
        lines.extend(setfile_block(i, mag, w["cfg"]))
        if per_stream_hedge is not None:
            stream_key = f"S{i}"
            if stream_key in per_stream_hedge:
                lines.extend(hedge_setfile_block(i, mag, per_stream_hedge[stream_key],
                                                  per_stream_risk))
        elif hedge_cfg is not None:
            lines.extend(hedge_setfile_block(i, mag, hedge_cfg, per_stream_risk))
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wfo-dir", required=True, help="Path to WFO output dir with IS/OOS parquets")
    ap.add_argument("--n", type=int, default=3, help="Number of streams to extract (default 3)")
    ap.add_argument("--magics", type=int, nargs="*", default=DEFAULT_MAGICS,
                    help="Magic numbers per stream (default: 1111 2222 3333 ...)")
    ap.add_argument("--total-risk", type=float, default=None,
                    help="Total per-setup risk %% (split across N streams). Required if --out.")
    ap.add_argument("--out", type=Path, default=None,
                    help="If set, write full setfile to this path. Otherwise just print blocks.")
    ap.add_argument("--hedge-cfg-json", type=Path, default=None,
                    help="Single global hedge cfg (from sim_wfo_hedge_global.py). "
                         "Adds 3 identical _HEDGE_S{N}_* blocks.")
    ap.add_argument("--hedge-per-stream-dir", type=Path, default=None,
                    help="Dir with per-stream {S1,S2,S3}.json (from sim_wfo_hedge.py). "
                         "Adds per-stream _HEDGE_S{N}_* blocks. Overrides --hedge-cfg-json.")
    args = ap.parse_args()

    wfo_dir = Path(args.wfo_dir)
    if not wfo_dir.is_absolute():
        wfo_dir = ROOT / wfo_dir

    top = extract_top_n(wfo_dir, args.n)

    print("=" * 100)
    print(f"  Top {args.n} from {wfo_dir.name}")
    print("=" * 100)
    print(f"  {'Rank':<5} {'P0':>4} {'Prof':>5} {'NP':>9} {'NP/DD':>8} {'Slope':>8} {'Plat$':>9}  Cfg")
    for i, w in enumerate(top, 1):
        c = w["cfg"]
        cfg_str = f"R={c.range_minutes} SL={c.fixed_sl_pts} RR={c.rr_ratio} HTP={c.half_tp_ratio}"
        plat = w.get("plateau_score") or 0
        print(f"  {i:<5} {'PASS' if w['p0_pass'] else 'FAIL':>4} "
              f"{w['prof_count']}/{len(w['oos_nps'])} ${w['total_np']:>+8,.0f} "
              f"{w['np_dd_ratio']:>+8.0f} {w['slope']:>+7.1%} ${plat:>+8,.0f}  {cfg_str}")

    # Sanity check: surface stability-vs-magnitude tradeoff in top-N (added 2026-05-06).
    # Reads top from extract_top_n() output; threshold default 30% NP gap.
    print_rank_sanity_check(top, top_n=args.n)

    hedge_cfg = None
    per_stream_hedge = None
    if args.hedge_per_stream_dir is not None:
        per_stream_hedge = {}
        for s in (f"S{i}" for i in range(1, args.n + 1)):
            p = args.hedge_per_stream_dir / f"{s}.json"
            per_stream_hedge[s] = json.loads(p.read_text())
            h = per_stream_hedge[s]
            print(f"  Hedge {s}: buf={h['buffer_pts']} h_sl={h['fixed_sl_pts']} "
                  f"h_rr={h['rr_ratio']} exp={h['expire_minutes']}min")
    elif args.hedge_cfg_json is not None:
        hedge_cfg = json.loads(args.hedge_cfg_json.read_text())
        print(f"\n  Hedge cfg loaded from {args.hedge_cfg_json}: "
              f"buf={hedge_cfg['buffer_pts']} h_sl={hedge_cfg['fixed_sl_pts']} "
              f"h_rr={hedge_cfg['rr_ratio']} exp={hedge_cfg['expire_minutes']}min")

    if args.out is not None:
        if args.total_risk is None:
            print("\nERROR: --total-risk required when --out is given.")
            return 1
        if len(args.magics) < args.n:
            print(f"\nERROR: --magics needs at least {args.n} values.")
            return 1
        text = full_setfile(top, args.magics[:args.n], args.total_risk, wfo_dir,
                              hedge_cfg, per_stream_hedge)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text, encoding="utf-8")
        print(f"\n  Wrote setfile: {args.out}")
        per_stream = args.total_risk / args.n
        print(f"  Per-stream risk: {per_stream:.4f}% (parent + matching hedge each at this risk)")
        if hedge_cfg or per_stream_hedge:
            print(f"  Hedge magics: {[hedge_magic_for(i) for i in range(1, args.n + 1)]} "
                  f"(5000 + 111×stream_idx)")
            print(f"  Combined max-stop exposure if all {args.n} parents + {args.n} hedges stop: "
                  f"{2 * args.total_risk}% of equity")
            print(f"  Mode: {'per-stream' if per_stream_hedge else 'global'}")
    else:
        print("\n" + "=" * 100)
        print(f"  Setfile blocks (paste into setfile or pass --out + --total-risk to write a full file):")
        print("=" * 100)
        for i, (w, mag) in enumerate(zip(top, args.magics[:args.n]), 1):
            print()
            print("\n".join(setfile_block(i, mag, w["cfg"])))
            if hedge_cfg is not None:
                per_stream = (args.total_risk / args.n) if args.total_risk else 1.0
                print("\n".join(hedge_setfile_block(i, mag, hedge_cfg, per_stream)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
