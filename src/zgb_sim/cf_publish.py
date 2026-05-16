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
    """Post today's sim-vs-live friction row.
    friction_pct = (sim - live) / max(abs(live), 1) * 100 (positive = sim optimistic)."""
    denom = max(abs(live_np), 1.0)
    friction_pct = (sim_np - live_np) / denom * 100.0
    return _post("/ingest/friction", {
        "date": date, "sim_np": sim_np, "live_np": live_np,
        "friction_pct": friction_pct, "spread_pts": spread_pts,
        "total_risk": total_risk, "notes": notes,
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
