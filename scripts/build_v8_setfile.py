"""Generate v8 setfiles from the deployed v7 setfiles.

v8 = v7 + GLOBAL (portfolio-level) daily target/loss caps. This builder:
  1. Copies each v7 setfile (9pct + 6pct).
  2. Updates the header to v8 + documents the global caps.
  3. Injects _GlobalDailyTargetPct / _GlobalDailyLossPct in the global section
     (default 0.0 = disabled; set/sweep these to activate the portfolio cap).
  4. Corrects hedge magics to the standard 8000+N*111 scheme (v7 setfiles shipped
     the gen_setfile_v6 bug — see project_setfile_hedge_magic_bug_2026_05_28).
  5. Re-tags parent + hedge _Comment fields v7 -> v8.

Writes:
  configs/sets/dt818_pro_v8_9pct_may30_may23.set
  configs/sets/dt818_pro_v8_6pct_may30_may23.set
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# (source v7 setfile, output v8 setfile, total_pct, global_loss_pct)
# Circuit-breaker loss cap: 9pct=4.5%, 6pct=4.5/1.5=3.0% (linear risk scaling).
# Target stays 0 (a daily profit target degrades the trend edge). Chosen
# 2026-05-28 as a FIXED default (not swept per-WFO).
JOBS = [
    ("dt818_pro_v7_9pct_may30_may23.set", "dt818_pro_v8_9pct_may30_may23.set", 9, 4.5),
    ("dt818_pro_v7_6pct_may30_may23.set", "dt818_pro_v8_6pct_may30_may23.set", 6, 3.0),
]

GLOBAL_TARGET_PCT = 0.0


def build(src_name: str, out_name: str, total_pct: int, global_loss_pct: float) -> int:
    src = ROOT / "configs/sets" / src_name
    if not src.exists():
        print(f"missing source: {src}")
        return 1
    text = src.read_text(encoding="utf-8")

    # 1. Header: v7 -> v8 on the title line + retarget the EA reference.
    text = text.replace(
        "; DT818_pro_v7 - 6-stream ORB rank portfolio + 6 STOP-ext hedges + SMA(8,21) cross-exit",
        "; DT818_pro_v8 - 6-stream ORB rank portfolio + 6 STOP-ext hedges + SMA(8,21) cross-exit\n"
        "; + GLOBAL (portfolio-level) daily target/loss caps",
        1,
    )
    text = text.replace("Use with ea/DT818_pro_v7.mq5.",
                        "Use with ea/DT818_pro_v8.mq5.", 1)

    # 2. Inject global cap params right after the _MaxSpreadPts line.
    t = GLOBAL_TARGET_PCT
    l = global_loss_pct
    global_block = (
        f"\n; v8: GLOBAL portfolio-level daily caps. Aggregate REALIZED P&L across all\n"
        f"; 6 parents + 6 hedges since broker day-start. When the loss cap is hit, the\n"
        f"; ENTIRE portfolio locks for the day (positions closed, pendings cancelled).\n"
        f"; Circuit-breaker default {l}% ({total_pct}pct risk; 6pct=9pct/1.5). Target\n"
        f"; LOCKED OFF (a profit target truncates the trend edge). Realized-only.\n"
        f"_GlobalDailyTargetPct={t}||{t}||1||{t}||{t}||N\n"
        f"_GlobalDailyLossPct={l}||{l}||1||{l}||{l}||N\n"
    )
    text = re.sub(r"(_MaxSpreadPts=[^\n]+\n)", r"\1" + global_block, text, count=1)

    # 3. Correct hedge magics to 8000+N*111 (fix the inherited gen_setfile_v6 bug).
    for n in range(1, 7):
        hmagic = 8000 + n * 111
        text = re.sub(rf"_HEDGE_S{n}_Magic=[^\n]+",
                      f"_HEDGE_S{n}_Magic={hmagic}||{hmagic}||1||{hmagic}||{hmagic}||N",
                      text, count=1)

    # 4. Re-tag comments v7 -> v8 (parent + hedge).
    for n in range(1, 7):
        text = re.sub(rf"_ORB_S{n}_Comment=ORB_S{n}_v7([^\n]*)",
                      rf"_ORB_S{n}_Comment=ORB_S{n}_v8\1", text, count=1)
        text = re.sub(rf"_HEDGE_S{n}_Comment=ORB_S{n}r_v7([^\n]*)",
                      rf"_HEDGE_S{n}_Comment=ORB_S{n}r_v8\1", text, count=1)

    out = ROOT / "configs/sets" / out_name
    out.write_text(text, encoding="utf-8")
    print(f"wrote {out}")
    return 0


def main() -> int:
    rc = 0
    for src, out, pct, loss in JOBS:
        rc |= build(src, out, pct, loss)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
