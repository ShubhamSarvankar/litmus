"""The read-condition evaluator (RFC 9110 13.1, 13.2.2) without HTTP."""

from datetime import datetime

import pytest
from starlette.datastructures import Headers

from server.api.conditions import evaluate_read_conditions, parse_http_date
from server.errors.s3errors import PreconditionFailed

ETAG = '"0123456789abcdef0123456789abcdef"'
OPAQUE = ETAG.strip('"')
# The row stores microseconds; the conditions see the second the header shows.
MODIFIED = datetime(2024, 5, 17, 10, 30, 0, 999_999)
SAME = "Fri, 17 May 2024 10:30:00 GMT"
BEFORE = "Fri, 17 May 2024 10:29:59 GMT"
AFTER = "Fri, 17 May 2024 10:30:01 GMT"


def headers(*pairs: tuple[str, str]) -> Headers:
    return Headers(raw=[(k.lower().encode(), v.encode()) for k, v in pairs])


def verdict(*pairs: tuple[str, str]) -> bool | str:
    try:
        return evaluate_read_conditions(headers(*pairs), ETAG, MODIFIED)
    except PreconditionFailed:
        return "412"


def test_no_condition_is_a_plain_response():
    assert verdict() is False


# --- If-Match: strong comparison, "*", lists, unquoted (RFC 9110 13.1.1, 8.8.3.2) ---


@pytest.mark.parametrize(
    "value",
    [
        ETAG,
        OPAQUE,
        "*",
        f'"other", {ETAG}',
        f'"other",{ETAG}',
        f'"a" , "b" ,\t{ETAG}',
        f"x, {OPAQUE}",
    ],
)
def test_if_match_that_matches_passes(value):
    assert verdict(("If-Match", value)) is False


@pytest.mark.parametrize(
    "value",
    ['"other"', f"W/{ETAG}", f'"other", W/{ETAG}', f"{ETAG}x", '"', '"",', "**", f"*, {ETAG[:-1]}"],
)
def test_if_match_that_does_not_match_is_412(value):
    assert verdict(("If-Match", value)) == "412"


def test_if_match_star_inside_a_list_is_not_a_wildcard():
    assert verdict(("If-Match", '*, "other"')) == "412"


def test_if_match_blank_is_no_condition():
    assert verdict(("If-Match", "  ")) is False


def test_if_match_repeated_header_lines_are_one_list():
    assert verdict(("If-Match", '"other"'), ("If-Match", ETAG)) is False


def test_an_entity_tag_may_hold_a_comma():
    comma = '"a,b"'
    assert evaluate_read_conditions(headers(("If-Match", comma)), comma, MODIFIED) is False
    with pytest.raises(PreconditionFailed):
        evaluate_read_conditions(headers(("If-Match", '"a"')), comma, MODIFIED)


# --- If-None-Match: weak comparison ---


@pytest.mark.parametrize(
    "value", [ETAG, OPAQUE, "*", f"W/{ETAG}", f'"other", W/{ETAG}', f"W/{OPAQUE}"]
)
def test_if_none_match_that_matches_is_304(value):
    assert verdict(("If-None-Match", value)) is True


@pytest.mark.parametrize("value", ['"other"', 'W/"other"', f"{ETAG}x", '"other", "another"', "x"])
def test_if_none_match_that_does_not_match_passes(value):
    assert verdict(("If-None-Match", value)) is False


def test_if_none_match_blank_is_no_condition():
    assert verdict(("If-None-Match", ""), ("If-Modified-Since", AFTER)) is True


# --- dates ---


@pytest.mark.parametrize(
    "value",
    [
        "Sun, 06 Nov 1994 08:49:37 GMT",
        "Sunday, 06-Nov-94 08:49:37 GMT",
        "Sun Nov  6 08:49:37 1994",
        "Sun Nov 06 08:49:37 1994",
    ],
)
def test_the_three_http_date_formats_parse(value):
    assert parse_http_date(headers(("D", value)), "d") == datetime(1994, 11, 6, 8, 49, 37)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "yesterday",
        "1994-11-06T08:49:37Z",
        "Sun, 06 Nov 1994 08:49:37 UTC",
        "Sun, 06 Nov 1994 08:49:37 +0100",
        "Sun, 06 Nov 1994 08:49:37 GMT, Mon, 07 Nov 1994 08:49:37 GMT",
        "Sun, 06 Nov 1994 25:49:37 GMT",
        "Sun, 06 Nov 1994 08:49:37 GMT trailing",
        "Sun, 06 Nov 1994 08:49:37 GMTé",
        "784111777",
    ],
)
def test_an_invalid_date_is_none(value):
    assert parse_http_date(headers(("D", value)), "d") is None


def test_a_repeated_date_header_is_none():
    h = headers(("D", SAME), ("D", SAME))
    assert parse_http_date(h, "d") is None


def test_a_two_digit_year_far_in_the_future_is_read_as_the_past():
    # 2099 would be more than 50 years ahead; RFC 9110 5.6.7 reads it as 1999.
    parsed = parse_http_date(headers(("D", "Friday, 01-Jan-99 00:00:00 GMT")), "d")
    assert parsed == datetime(1999, 1, 1)


# --- If-Modified-Since ---


@pytest.mark.parametrize(("value", "expected"), [(BEFORE, False), (SAME, True), (AFTER, True)])
def test_if_modified_since(value, expected):
    assert verdict(("If-Modified-Since", value)) is expected


@pytest.mark.parametrize("value", ["garbage", "", f"{BEFORE}, {AFTER}"])
def test_an_invalid_if_modified_since_is_ignored(value):
    assert verdict(("If-Modified-Since", value)) is False


# --- If-Unmodified-Since ---


@pytest.mark.parametrize(("value", "expected"), [(BEFORE, "412"), (SAME, False), (AFTER, False)])
def test_if_unmodified_since(value, expected):
    assert verdict(("If-Unmodified-Since", value)) == expected


@pytest.mark.parametrize("value", ["garbage", "", f"{BEFORE}, {AFTER}"])
def test_an_invalid_if_unmodified_since_is_ignored(value):
    assert verdict(("If-Unmodified-Since", value)) is False


# --- precedence (RFC 9110 13.2.2) ---


def test_if_match_true_and_if_unmodified_since_false_is_200():
    """The combination the S3 GetObject reference documents."""
    assert verdict(("If-Match", ETAG), ("If-Unmodified-Since", BEFORE)) is False


def test_if_match_false_is_412_whatever_follows():
    assert verdict(("If-Match", '"other"'), ("If-None-Match", '"other"')) == "412"
    assert verdict(("If-Match", '"other"'), ("If-Modified-Since", AFTER)) == "412"
    assert verdict(("If-Match", '"other"'), ("If-Unmodified-Since", AFTER)) == "412"


def test_if_unmodified_since_false_beats_if_none_match_false():
    assert verdict(("If-Unmodified-Since", BEFORE), ("If-None-Match", ETAG)) == "412"


def test_if_none_match_false_and_if_modified_since_true_is_304():
    """The other combination the S3 GetObject reference documents."""
    assert verdict(("If-None-Match", ETAG), ("If-Modified-Since", BEFORE)) is True


def test_if_none_match_true_makes_if_modified_since_irrelevant():
    assert verdict(("If-None-Match", '"other"'), ("If-Modified-Since", AFTER)) is False


def test_if_match_passing_leaves_the_later_conditions_to_run():
    assert verdict(("If-Match", "*"), ("If-None-Match", ETAG)) is True
    assert verdict(("If-Match", "*"), ("If-Modified-Since", SAME)) is True
