"""Push live_check / weekly_recap / projection events to the dt818-console Cloudflare Worker.

Reads CONSOLE_API_BASE + CONSOLE_INGEST_TOKEN from env (.env file or os.environ).
Posts JSON payloads to /ingest/* endpoints with Bearer auth.

Fails open: any HTTP / connection error logs a warning and returns False; the
calling script keeps running. The local tracker is the source of truth; the
Worker is a mirror for the UI.

Example:
    from zgb_sim.cf_publish import publish_deals
    publish_deals(deal_payloads, account=acct_snapshot)
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[2]
DOTENV = ROOT / ".env"

# Lazy-load .env once per process
_env_loaded = False


def _load_dotenv() -> None:
    global _env_loaded
    if _env_loaded or not DOTENV.exists():
        _env_loaded = True
        return
    for raw in DOTENV.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    _env_loaded = True


def _api_base() -> str | None:
    _load_dotenv()
    return os.environ.get("CONSOLE_API_BASE")


def _token() -> str | None:
    _load_dotenv()
    return os.environ.get("CONSOLE_INGEST_TOKEN")


def _post(path: str, payload: Mapping[str, Any], timeout_s: float = 8.0) -> bool:
    base = _api_base()
    token = _token()
    if not base or not token:
        # Silent fail: dashboard not yet configured. Don't spam logs.
        return False
    url = base.rstrip("/") + path
    body = json.dumps(payload, default=_json_default).encode("utf-8")
    # Cloudflare's default WAF blocks requests with no User-Agent (returns 403
    # before reaching the Worker). Setting one bypasses the bot heuristic.
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "dt818-console-publisher/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            ok = 200 <= resp.status < 300
            if not ok:
                print(f"[cf_publish] {path} -> HTTP {resp.status}", file=sys.stderr)
            return ok
    except urllib.error.HTTPError as e:
        print(f"[cf_publish] {path} -> HTTPError {e.code} {e.reason}", file=sys.stderr)
        return False
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"[cf_publish] {path} -> {type(e).__name__}: {e}", file=sys.stderr)
        return False


def _json_default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.astimezone(timezone.utc).isoformat()
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    raise TypeError(f"Not serializable: {type(obj)}")


def publish_deals(deals: Sequence[Mapping[str, Any]],
                   account: Mapping[str, Any] | None = None) -> bool:
    """Push a list of closed-deal payloads + optional account snapshot."""
    return _post("/ingest/deals", {"deals": list(deals), "account": account})


def publish_positions(positions: Sequence[Mapping[str, Any]],
                       account: Mapping[str, Any] | None = None,
                       snapshot_id: str | None = None) -> bool:
    """Push the current open-positions snapshot. snapshot_id defaults to UTC ISO ts."""
    ts = datetime.now(timezone.utc).isoformat()
    return _post("/ingest/positions", {
        "snapshot_id": snapshot_id or ts,
        "ts": ts,
        "positions": list(positions),
        "account": account,
    })


def publish_projection(projection: Mapping[str, Any]) -> bool:
    """Push a full forward_projection.json snapshot."""
    return _post("/ingest/projection", projection)


def publish_friction(date: str, sim_np: float, live_np: float,
                      spread_pts: int | None = None, total_risk: float | None = None,
                      notes: str | None = None) -> bool:
    """Post today's APPARENT friction row (sim_scaled vs live).

    Vocabulary (standardized 2026-05-18):
      "Friction"  = umbrella for sim-vs-live deviation
      "Apparent"  = the metric this function publishes (mixed broker + structural)
      "Slippage"  = pure broker SL-fill execution (see publish_slippage)

    IMPORTANT: caller must pass sim_np ALREADY SCALED to live's balance anchor:
        sim_scaled = sim_np_raw * (live_balance_anchor / sim_deposit)
    where live_balance_anchor is the live balance at the start of the comparison
    period (typically Monday-open). Without this scaling, the friction% is
    meaningless because sim_np was computed on a $10k deposit while live_np is
    on a $149k+ balance — direct comparison would give nonsensical values like
    +94% when the real apples-to-apples apparent friction is +17%.

    apparent_pct = (sim_scaled - live) / max(|sim_scaled|, 100) * 100
    (positive = sim optimistic vs live. The $100 denom floor prevents blow-ups
    when both legs are near zero.)

    DB column name remains `friction_pct` for backward compat — value is
    apparent friction. See project_live_vs_sim_calibration_log.md.

    Refuses to publish when the sim leg is too thin to anchor a ratio, OR when
    the resulting friction is so large the two legs are incomparable (window /
    sign mismatch). The old guard only caught sim_np == 0 EXACTLY, so a sim leg
    of e.g. $0.50 sailed through, hit the $100 denominator floor, and published
    `live_np / $100` — surfacing as artifacts like -12690.8% (= -$12,690 live
    parent / $100) or +4061%. See project_friction_zero_sim_guard_2026_05_25.
    """
    # A meaningful sim baseline (scaled to live balance) is at least a few
    # hundred $ on a real trading day. Below this the $100 denom floor dominates
    # and the percent becomes live_np/$100 — pure artifact, not friction.
    MIN_SIM_BASELINE = 250.0
    if abs(sim_np) < MIN_SIM_BASELINE:
        print(f"[cf_publish] friction skipped: |sim_np|=${abs(sim_np):,.0f} "
              f"< ${MIN_SIM_BASELINE:,.0f} baseline (sim leg too thin to anchor a ratio). "
              f"live_np=${live_np:+,.0f} would produce a denom-floor artifact.")
        return False
    denom = abs(sim_np)  # >= MIN_SIM_BASELINE, so no floor needed
    friction_pct = (sim_np - live_np) / denom * 100.0
    # Beyond this band the legs are incomparable (sign flip or window mismatch),
    # not "high friction". Skip rather than publish a meaningless 4-5 digit %.
    FRICTION_CAP = 300.0
    if abs(friction_pct) > FRICTION_CAP:
        print(f"[cf_publish] friction skipped: |friction|={friction_pct:+,.0f}% "
              f"> {FRICTION_CAP:.0f}% cap (sim=${sim_np:+,.0f} vs live=${live_np:+,.0f} "
              f"are incomparable — likely window/sign mismatch, not real friction).")
        return False
    return _post("/ingest/friction", {
        "date": date, "sim_np": sim_np, "live_np": live_np,
        "friction_pct": friction_pct, "spread_pts": spread_pts,
        "total_risk": total_risk, "notes": notes,
    })


def publish_slippage(date: str, slippage_pct: float, sl_trades: int,
                      expected_loss_usd: float, actual_loss_usd: float,
                      notes: str | None = None) -> bool:
    """Post today's slippage-only friction (pure broker execution quality on SLs).

    Lands in the same daily_friction row as publish_friction (worker uses UPSERT
    with COALESCE on each field). If the apparent friction row doesn't exist yet,
    the slippage record creates it with placeholder sim/live=0 values.

    slippage_pct = (Σactual_$loss / Σexpected_$loss - 1) × 100
    where expected_$loss per SL = lots × SL_pts × $1/pt (clean fill assumption).

    Triggers calibration retune when 5-day rolling mean > 10% (see
    project_live_vs_sim_calibration_log.md).
    """
    return _post("/ingest/friction", {
        "date": date,
        "slippage_pct": slippage_pct,
        "sl_trades": sl_trades,
        "expected_loss_usd": expected_loss_usd,
        "actual_loss_usd": actual_loss_usd,
        "notes": notes,
    })


def publish_alert(severity: str, kind: str, message: str,
                   context: Mapping[str, Any] | None = None,
                   ts: str | None = None) -> bool:
    """Post a trigger alert. severity: info|warn|critical."""
    return _post("/ingest/alert", {
        "ts": ts or datetime.now(timezone.utc).isoformat(),
        "severity": severity, "kind": kind, "message": message,
        "context": dict(context) if context else None,
    })


def publish_weekly_recap(week_ending: str, days: int, net_pnl: float, balance_end: float,
                          trades: int, wins: int, losses: int,
                          by_stream: Mapping[str, Any]) -> bool:
    return _post("/ingest/weekly", {
        "week_ending": week_ending, "days": days, "net_pnl": net_pnl,
        "balance_end": balance_end, "trades": trades, "wins": wins, "losses": losses,
        "by_stream": dict(by_stream),
    })
