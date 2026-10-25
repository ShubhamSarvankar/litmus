"""If-Range (RFC 9110 13.1.5) without HTTP: the strong-validator rule needs a clock."""

from datetime import datetime, timedelta

import pytest
from starlette.datastructures import Headers

from server.api.conditions import if_range_holds

ETAG = '"0123456789abcdef0123456789abcdef"'
MODIFIED = datetime(2024, 5, 17, 10, 30, 0, 600_000)
DATE = "Fri, 17 May 2024 10:30:00 GMT"


def holds(value: str | None, now: datetime) -> bool:
    raw = [] if value is None else [(b"if-range", value.encode())]
    return if_range_holds(Headers(raw=raw), ETAG, MODIFIED, now)


def test_no_if_range_holds():
    assert holds(None, MODIFIED) is True


@pytest.mark.parametrize(
    ("age", "expected"),
    [
        (timedelta(0), False),
        (timedelta(seconds=0.999), False),
        (timedelta(seconds=1), True),
        (timedelta(hours=1), True),
    ],
)
def test_a_date_holds_only_once_last_modified_is_a_second_old(age, expected):
    assert holds(DATE, MODIFIED + age) is expected


def test_an_entity_tag_needs_no_clock():
    assert holds(ETAG, MODIFIED) is True
    assert holds(f"W/{ETAG}", MODIFIED) is False
