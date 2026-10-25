"""If-Range on GET (RFC 9110 13.1.5, 13.2.2 step 5): a true condition applies the Range, a
false one ignores it and sends the whole object with 200. The condition is for a GET
that carries a Range; without a Range it is ignored, and HEAD ignores it."""

import hashlib
import sqlite3
import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "range-cond"
KEY = "old.bin"  # Last-Modified an hour ago: a strong validator
FRESH = "fresh.bin"  # written just now: Last-Modified is not yet a strong validator
BODY = bytes(range(100))
ETAG = '"' + hashlib.md5(BODY).hexdigest() + '"'
RANGE = {"Range": "bytes=10-19"}
SLICE = BODY[10:20]
ISO = "%Y-%m-%dT%H:%M:%S.%f"


def _http_date(dt: datetime) -> str:
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    c = TestClient(create_app(storage=storage, metadata=metadata), raise_server_exceptions=False)
    assert c.put(f"/{BUCKET}").status_code == 200
    for key in (KEY, FRESH):
        assert c.put(f"/{BUCKET}/{key}", content=BODY).status_code == 200
    hour_ago = (datetime.now() - timedelta(hours=1)).replace(microsecond=250_000)
    with sqlite3.connect(tmp_path / "meta.db") as db:
        db.execute(
            "UPDATE objects SET last_modified = ? WHERE key = ?", (hour_ago.strftime(ISO), KEY)
        )
    return c


def _last_modified(client, key: str = KEY) -> datetime:
    header = client.head(f"/{BUCKET}/{key}").headers["last-modified"]
    return datetime.strptime(header, "%a, %d %b %Y %H:%M:%S GMT")


def _get(client, headers: dict, key: str = KEY, query: str = ""):
    return client.get(f"/{BUCKET}/{key}{query}", headers=headers)


def _assert_partial(resp):
    assert resp.status_code == 206
    assert resp.content == SLICE
    assert resp.headers["content-range"] == "bytes 10-19/100"
    assert resp.headers["etag"] == ETAG


def _assert_whole(resp):
    assert resp.status_code == 200
    assert resp.content == BODY
    assert "content-range" not in resp.headers
    assert resp.headers["content-length"] == "100"
    assert resp.headers["etag"] == ETAG


def test_the_fixture_dates_are_what_the_tests_assume(client):
    now = datetime.now(UTC).replace(tzinfo=None)
    assert now - _last_modified(client) > timedelta(minutes=59)


# --- entity tags: strong comparison ---


@pytest.mark.parametrize("value", [ETAG, ETAG.strip('"')])
def test_a_matching_entity_tag_applies_the_range(client, value):
    _assert_partial(_get(client, {**RANGE, "If-Range": value}))


def test_a_different_entity_tag_sends_the_whole_object(client):
    _assert_whole(_get(client, {**RANGE, "If-Range": '"other"'}))


def test_a_weak_entity_tag_never_matches(client):
    _assert_whole(_get(client, {**RANGE, "If-Range": f"W/{ETAG}"}))


@pytest.mark.parametrize("value", [f'"other", {ETAG}', "*"])
def test_a_list_or_a_wildcard_is_not_an_entity_tag(client, value):
    _assert_whole(_get(client, {**RANGE, "If-Range": value}))


# --- dates: exact match with a strong Last-Modified ---


def test_an_equal_date_applies_the_range(client):
    modified = _last_modified(client)
    _assert_partial(_get(client, {**RANGE, "If-Range": _http_date(modified)}))


def test_an_equal_date_in_the_other_formats_applies_the_range(client):
    modified = _last_modified(client)
    for value in (
        modified.strftime("%A, %d-%b-%y %H:%M:%S GMT"),
        modified.strftime("%a %b %d %H:%M:%S %Y"),
    ):
        _assert_partial(_get(client, {**RANGE, "If-Range": value}))


@pytest.mark.parametrize("delta", [timedelta(seconds=-1), timedelta(seconds=1), timedelta(days=1)])
def test_an_unequal_date_sends_the_whole_object(client, delta):
    value = _http_date(_last_modified(client) + delta)
    _assert_whole(_get(client, {**RANGE, "If-Range": value}))


def test_an_equal_date_does_not_hold_while_last_modified_is_less_than_a_second_old(client):
    """RFC 9110 13.1.5: false when the date is not a strong validator (8.8.2.2)."""
    modified = _last_modified(client, FRESH)
    resp = _get(client, {**RANGE, "If-Range": _http_date(modified)}, key=FRESH)
    assert resp.status_code == 200
    assert resp.content == BODY


# --- a value that is neither a valid entity tag nor a valid HTTP-date ---


@pytest.mark.parametrize(
    "value",
    ["", "garbage", "2024-05-17T10:30:00Z", "Fri, 17 May 2024 10:30:00 UTC", '"unterminated'],
)
def test_an_invalid_value_sends_the_whole_object(client, value):
    _assert_whole(_get(client, {**RANGE, "If-Range": value}))


def test_a_repeated_if_range_sends_the_whole_object(client):
    resp = client.get(
        f"/{BUCKET}/{KEY}",
        headers=[("Range", "bytes=10-19"), ("If-Range", ETAG), ("If-Range", ETAG)],
    )
    _assert_whole(resp)


# --- no Range: If-Range is ignored; HEAD ignores it ---


@pytest.mark.parametrize("value", [ETAG, '"other"', "garbage", ""])
def test_if_range_without_a_range_is_ignored(client, value):
    _assert_whole(_get(client, {"If-Range": value}))


def test_head_ignores_if_range(client):
    plain = client.head(f"/{BUCKET}/{KEY}", headers=RANGE)
    for value in (ETAG, '"other"', "garbage"):
        resp = client.head(f"/{BUCKET}/{KEY}", headers={**RANGE, "If-Range": value})
        assert resp.status_code == plain.status_code == 200
        assert resp.headers["content-length"] == "100"
        assert "content-range" not in resp.headers


def test_head_still_refuses_an_unsatisfiable_range_whatever_if_range_says(client):
    for value in (ETAG, '"other"'):
        headers = {"Range": "bytes=500-", "If-Range": value}
        assert client.head(f"/{BUCKET}/{KEY}", headers=headers).status_code == 416


# --- an unsatisfiable or unusable Range ---


def test_an_unsatisfiable_range_with_a_true_if_range_is_416(client):
    resp = _get(client, {"Range": "bytes=500-", "If-Range": ETAG})
    assert resp.status_code == 416
    assert ET.fromstring(resp.text).findtext("Code") == "InvalidRange"
    assert resp.headers["content-range"] == "bytes */100"


def test_an_unsatisfiable_range_with_a_false_if_range_sends_the_whole_object(client):
    _assert_whole(_get(client, {"Range": "bytes=500-", "If-Range": '"other"'}))


def test_a_range_that_is_ignored_anyway_stays_a_200_when_if_range_holds(client):
    for range_value in ("bytes=1-2,5-6", "items=1-2", "bytes=9-1"):
        _assert_whole(_get(client, {"Range": range_value, "If-Range": ETAG}))


# --- with the other conditions (RFC 9110 13.2.2: If-Range is step 5) ---


def test_if_range_true_with_conditions_that_pass_applies_the_range(client):
    modified = _last_modified(client)
    headers = {
        **RANGE,
        "If-Range": ETAG,
        "If-Match": "*",
        "If-None-Match": '"other"',
        "If-Unmodified-Since": _http_date(modified + timedelta(days=1)),
    }
    _assert_partial(_get(client, headers))


def test_if_range_false_with_conditions_that_pass_sends_the_whole_object(client):
    headers = {**RANGE, "If-Range": '"other"', "If-Match": ETAG, "If-None-Match": '"other"'}
    _assert_whole(_get(client, headers))


@pytest.mark.parametrize("if_range", [ETAG, '"other"'])
def test_a_304_comes_before_if_range(client, if_range):
    resp = _get(client, {**RANGE, "If-Range": if_range, "If-None-Match": ETAG})
    assert resp.status_code == 304
    assert resp.content == b""


@pytest.mark.parametrize("if_range", [ETAG, '"other"'])
def test_if_modified_since_in_the_future_gives_a_304_before_if_range(client, if_range):
    future = _http_date(_last_modified(client) + timedelta(days=1))
    resp = _get(client, {**RANGE, "If-Range": if_range, "If-Modified-Since": future})
    assert resp.status_code == 304


@pytest.mark.parametrize("if_range", [ETAG, '"other"'])
def test_a_412_comes_before_if_range(client, if_range):
    resp = _get(client, {**RANGE, "If-Range": if_range, "If-Match": '"other"'})
    assert resp.status_code == 412
    resp = _get(
        client,
        {
            **RANGE,
            "If-Range": if_range,
            "If-Unmodified-Since": _http_date(_last_modified(client) - timedelta(days=1)),
        },
    )
    assert resp.status_code == 412


def test_a_missing_key_is_404_whatever_if_range_says(client):
    resp = _get(client, {**RANGE, "If-Range": ETAG}, key="no-such-key")
    assert resp.status_code == 404


# --- response-* overrides apply on the 206 and on the 200 ---


def test_response_overrides_apply_to_the_206(client):
    resp = _get(client, {**RANGE, "If-Range": ETAG}, query="?response-content-type=text/plain")
    _assert_partial(resp)
    assert resp.headers["content-type"].startswith("text/plain")


def test_response_overrides_apply_to_the_200_a_false_if_range_gives(client):
    resp = _get(client, {**RANGE, "If-Range": '"other"'}, query="?response-content-type=text/plain")
    _assert_whole(resp)
    assert resp.headers["content-type"].startswith("text/plain")
