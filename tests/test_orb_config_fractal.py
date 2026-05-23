"""Smoke test: ORBConfig accepts new fractal fields and defaults them off."""
from zgb_sim.orb import ORBConfig


def test_defaults_off():
    c = ORBConfig()
    assert c.fractal_trail is False
    assert c.fractal_confirm is False
    assert c.fractal_range is False
    assert c.fractal_width == 5


def test_all_flags_settable():
    c = ORBConfig(fractal_trail=True, fractal_confirm=True, fractal_range=True, fractal_width=3)
    assert c.fractal_trail and c.fractal_confirm and c.fractal_range
    assert c.fractal_width == 3
