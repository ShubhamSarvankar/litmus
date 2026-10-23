import os
import signal

import pytest

from server import crash_points
from server.config import settings


@pytest.fixture
def kills(monkeypatch):
    """Record SIGKILLs crash_point() would send instead of sending them."""
    sent = []
    monkeypatch.setattr(crash_points.os, "kill", lambda pid, sig: sent.append((pid, sig)))
    return sent


@pytest.mark.parametrize("name", sorted(crash_points.CRASH_POINTS))
def test_inert_when_unset(monkeypatch, kills, name):
    monkeypatch.setattr(settings, "test_crash_point", "")
    crash_points.crash_point(name)
    assert kills == []


def test_only_the_named_point_fires(monkeypatch, kills):
    monkeypatch.setattr(settings, "test_crash_point", crash_points.COMPLETE_AFTER_ASSEMBLE)
    crash_points.crash_point(crash_points.PUT_AFTER_BYTES)
    crash_points.crash_point(crash_points.COMPLETE_AFTER_OBJECT_META)
    assert kills == []
    crash_points.crash_point(crash_points.COMPLETE_AFTER_ASSEMBLE)
    assert kills == [(os.getpid(), signal.SIGKILL)]


def test_unknown_point_name_in_code_is_an_error(kills):
    with pytest.raises(ValueError):
        crash_points.crash_point("no-such-point")


def test_unknown_configured_point_is_rejected(monkeypatch):
    monkeypatch.setattr(settings, "test_crash_point", "put-after-bytez")
    with pytest.raises(ValueError, match="not a known crash point"):
        crash_points.validate_configured_crash_point()


def test_unset_configured_point_is_accepted(monkeypatch):
    monkeypatch.setattr(settings, "test_crash_point", "")
    crash_points.validate_configured_crash_point()
