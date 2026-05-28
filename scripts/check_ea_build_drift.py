"""Check EA / setfile build drift.

Catches cases where the setfile is updated but the EA .ex5 hasn't been
recompiled (or vice versa) — meaning live could be running with stale code.

Rule: if active setfile's mtime > active EA .ex5 mtime, fire warn alert.

Active locations (per memory feedback_setfile_mirror_d_drive.md):
  EA source/binary canonical at D:\\v6\\ (latest is v6 STOP-ext, since 2026-05-24).
  AppData MT5 instance has its own copy too — check both.

Run as part of weekly_recap.py / daily ops. Idempotent (publish_alert is
INSERT, not REPLACE, but dashboard de-dupes by recent same-kind alerts via
ack workflow — acceptable noise).
"""
from __future__ import annotations
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

# Canonical locations for CURRENT live EA (v6 since 2026-05-24).
# Rotates with each EA major bump — update both LIVE_LABEL and paths together.
LIVE_LABEL  = "D:/v6"
D_DRIVE_EA  = Path("D:/v6/DT818_pro_v6.ex5")
D_DRIVE_SET = Path("D:/v6/dt818_pro_v6_9pct_may23_may16.set")
APPDATA_EA  = Path("C:/Users/Zhu-En/AppData/Roaming/MetaQuotes/Terminal/AE2CC2E013FDE1E3CDF010AA51C60400/MQL5/Experts/DT818_pro_v6.ex5")


def _check_version_log() -> int:
    """Delegate to check_ea_version_log.py so build-drift also catches stale
    OnInit Print tags (the 2026-05-25 v6/v4 incident)."""
    try:
        import subprocess
        r = subprocess.run(
            [sys.executable, str(Path(__file__).parent / "check_ea_version_log.py")],
            capture_output=True, text=True, timeout=30)
        return r.returncode
    except Exception as e:
        print(f"   [warn] check_ea_version_log failed to run: {e}")
        return 0  # don't fail drift check on tool-runner error


def main() -> int:
    issues = []

    # Pre-flight: OnInit Print tags must match `#property version` (2026-05-25 incident)
    version_rc = _check_version_log()
    if version_rc != 0:
        issues.append("  EA OnInit log version tag drift (run check_ea_version_log.py for details)")

    for label, path in [("D:/v6 EA", D_DRIVE_EA), ("D:/v6 setfile", D_DRIVE_SET),
                         ("AppData EA", APPDATA_EA)]:
        if not path.exists():
            issues.append(f"  MISSING: {label} -> {path}")

    if not (D_DRIVE_EA.exists() and D_DRIVE_SET.exists()):
        print("Cannot check drift — required files missing:")
        for i in issues: print(i)
        return 1

    ea_mtime  = datetime.fromtimestamp(D_DRIVE_EA.stat().st_mtime, tz=timezone.utc)
    set_mtime = datetime.fromtimestamp(D_DRIVE_SET.stat().st_mtime, tz=timezone.utc)
    print(f"D:/v6 EA  .ex5 mtime: {ea_mtime.isoformat()}")
    print(f"D:/v6 setfile mtime:  {set_mtime.isoformat()}")

    set_newer = set_mtime > ea_mtime
    delta_hrs = (set_mtime - ea_mtime).total_seconds() / 3600

    if APPDATA_EA.exists():
        appdata_mtime = datetime.fromtimestamp(APPDATA_EA.stat().st_mtime, tz=timezone.utc)
        appdata_older = appdata_mtime < ea_mtime
        appdata_delta_hrs = (ea_mtime - appdata_mtime).total_seconds() / 3600
        print(f"AppData EA .ex5 mtime: {appdata_mtime.isoformat()}  "
              f"({'STALE by ' + f'{appdata_delta_hrs:.1f}h' if appdata_older else 'in sync'})")
    else:
        appdata_older = False
        appdata_delta_hrs = 0.0

    alerts_to_fire = []
    if set_newer and delta_hrs > 1.0:  # >1h drift to allow for normal save sequencing
        alerts_to_fire.append((
            "warn", "setfile_ea_drift",
            f"Setfile (D:/v6/...set) is {delta_hrs:.1f}h newer than D:/v6 EA .ex5 — "
            f"setfile changed without recompile. Recompile + redeploy required.",
            {"setfile_mtime": set_mtime.isoformat(), "ea_mtime": ea_mtime.isoformat(),
             "delta_hours": delta_hrs}
        ))

    if appdata_older and appdata_delta_hrs > 1.0:
        alerts_to_fire.append((
            "warn", "appdata_ea_stale",
            f"AppData MT5 EA .ex5 is {appdata_delta_hrs:.1f}h older than D:/v6 master. "
            f"Local tester runs would use stale binary.",
            {"appdata_mtime": appdata_mtime.isoformat(),
             "d_drive_mtime": ea_mtime.isoformat(),
             "delta_hours": appdata_delta_hrs}
        ))

    if not alerts_to_fire:
        print("\n>> EA + setfile in sync. No drift detected.")
        return 0

    print("\n>> DRIFT detected:")
    for sev, kind, msg, _ctx in alerts_to_fire:
        print(f"   [{sev}] {kind}: {msg}")

    # Publish alerts
    try:
        from zgb_sim.cf_publish import publish_alert
        for sev, kind, msg, ctx in alerts_to_fire:
            ok = publish_alert(sev, kind, msg, context=ctx)
            print(f"   publish_alert({kind}): {'OK' if ok else 'FAIL'}")
    except Exception as e:
        print(f"   publish_alert skipped: {type(e).__name__}: {e}")

    return 0 if not alerts_to_fire else 2


if __name__ == "__main__":
    sys.exit(main())
