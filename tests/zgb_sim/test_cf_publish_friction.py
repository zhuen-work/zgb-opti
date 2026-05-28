import zgb_sim.cf_publish as cfp


def test_publish_friction_skips_when_sim_is_zero(monkeypatch):
    """sim_np=0 is a missing-data sentinel; without a sim baseline, the formula
    collapses to -live% via the $100 denom floor (this produced +4061% on the
    live dashboard on 2026-05-25). Publishing must refuse so the dashboard never
    surfaces that artifact again."""
    posts = []
    monkeypatch.setattr(cfp, "_post", lambda path, payload: posts.append((path, payload)) or True)
    ok = cfp.publish_friction(date="2026-05-25", sim_np=0.0, live_np=-4061.58)
    assert ok is False
    assert posts == []


def test_publish_friction_still_skips_when_both_zero(monkeypatch):
    posts = []
    monkeypatch.setattr(cfp, "_post", lambda path, payload: posts.append((path, payload)) or True)
    ok = cfp.publish_friction(date="2026-05-25", sim_np=0.0, live_np=0.0)
    assert ok is False
    assert posts == []


def test_publish_friction_publishes_with_real_sim(monkeypatch):
    posts = []
    monkeypatch.setattr(cfp, "_post", lambda path, payload: posts.append((path, payload)) or True)
    ok = cfp.publish_friction(date="2026-05-25", sim_np=2500.0, live_np=-1500.0,
                                spread_pts=30, total_risk=9.0)
    assert ok is True
    assert len(posts) == 1
    path, payload = posts[0]
    assert path == "/ingest/friction"
    # (2500 - (-1500)) / 2500 * 100 = 160.0
    assert payload["friction_pct"] == 160.0
    assert payload["sim_np"] == 2500.0
    assert payload["live_np"] == -1500.0


def test_publish_friction_skips_thin_sim_below_baseline(monkeypatch):
    """Strengthened 2026-05-28: a small-but-nonzero sim leg (below the $250
    baseline) used to sail through the `== 0` guard, hit the $100 denom floor,
    and publish `live_np / $100` — surfacing as -12690.8% (= -$12,690 live
    parent / $100) on the dashboard. Now skipped: the sim leg is too thin to
    anchor a ratio."""
    posts = []
    monkeypatch.setattr(cfp, "_post", lambda path, payload: posts.append((path, payload)) or True)
    ok = cfp.publish_friction(date="2026-05-28", sim_np=0.5, live_np=12690.76)
    assert ok is False
    assert posts == []


def test_publish_friction_skips_incomparable_legs_over_cap(monkeypatch):
    """A sim leg above the baseline but tiny relative to live still yields an
    absurd percent. The 300% cap catches sign-flip / window-mismatch days
    (e.g. the -712% trend day) rather than publishing meaningless friction."""
    posts = []
    monkeypatch.setattr(cfp, "_post", lambda path, payload: posts.append((path, payload)) or True)
    # sim=+1825 vs live=-11168 → (1825+11168)/1825 = +712% > 300% cap
    ok = cfp.publish_friction(date="2026-05-28", sim_np=1825.0, live_np=-11168.0)
    assert ok is False
    assert posts == []


def test_publish_friction_publishes_within_band(monkeypatch):
    """Normal apparent friction within the ±300% band publishes with no floor
    (denom = |sim_np| directly since it's >= $250 baseline)."""
    posts = []
    monkeypatch.setattr(cfp, "_post", lambda path, payload: posts.append((path, payload)) or True)
    ok = cfp.publish_friction(date="2026-05-28", sim_np=16900.0, live_np=12691.0)
    assert ok is True
    # (16900 - 12691) / 16900 * 100 = 24.9%
    assert round(posts[0][1]["friction_pct"], 1) == 24.9
