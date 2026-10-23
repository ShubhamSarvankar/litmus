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
    crash_points.crash_point(crash_points.COMPLETE_AFTER_COMMIT)
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


@pytest.mark.parametrize("name", sorted(crash_points.CRASH_POINTS))
def test_armed_point_is_none_when_unset(monkeypatch, name):
    """An unarmed server passes None where a mid-write point would go, so it makes
    exactly the calls it makes without crash points."""
    monkeypatch.setattr(settings, "test_crash_point", "")
    assert crash_points.armed_point(name) is None


def test_armed_point_names_only_the_armed_point(monkeypatch):
    monkeypatch.setattr(settings, "test_crash_point", crash_points.PUT_MID_WRITE)
    assert crash_points.armed_point(crash_points.PUT_MID_WRITE) == crash_points.PUT_MID_WRITE
    assert crash_points.armed_point(crash_points.PART_MID_WRITE) is None


def test_armed_point_rejects_an_unknown_name():
    with pytest.raises(ValueError):
        crash_points.armed_point("no-such-point")
