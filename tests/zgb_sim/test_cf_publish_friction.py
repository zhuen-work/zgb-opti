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


def test_publish_friction_uses_denom_floor_for_small_sim(monkeypatch):
    """Sanity-check the denominator floor still applies for small but non-zero
    sim — the floor exists to prevent blow-ups, not to mask missing data."""
    posts = []
    monkeypatch.setattr(cfp, "_post", lambda path, payload: posts.append((path, payload)) or True)
    ok = cfp.publish_friction(date="2026-05-25", sim_np=50.0, live_np=-50.0)
    assert ok is True
    # denom = max(|50|, 100) = 100; (50 - (-50))/100 * 100 = 100.0
    assert posts[0][1]["friction_pct"] == 100.0
