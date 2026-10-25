"""Conditional GET and HEAD: If-Match, If-None-Match, If-Modified-Since and
If-Unmodified-Since, evaluated in RFC 9110 13.2.2 order against the one row the
response is built from. A missing bucket or key is answered before any condition."""

import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "cond-bucket"
KEY = "obj.bin"
BODY = bytes(range(100))
ETAG = '"' + hashlib.md5(BODY).hexdigest() + '"'


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    c = TestClient(create_app(storage=storage, metadata=metadata), raise_server_exceptions=False)
    assert c.put(f"/{BUCKET}").status_code == 200
    put = c.put(
        f"/{BUCKET}/{KEY}",
        content=BODY,
        headers={"Cache-Control": "max-age=60", "Expires": "Wed, 01 Jan 2100 00:00:00 GMT"},
    )
    assert put.status_code == 200
    return c


def _http_date(dt: datetime) -> str:
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")


@pytest.fixture
def modified(client) -> datetime:
    """The object's Last-Modified, as the header shows it (naive UTC, whole seconds)."""
    header = client.head(f"/{BUCKET}/{KEY}").headers["last-modified"]
    return datetime.strptime(header, "%a, %d %b %Y %H:%M:%S GMT")


def _request(client, method: str, headers: dict, key: str = KEY):
    return getattr(client, method)(f"/{BUCKET}/{key}", headers=headers)


def _code(resp) -> str:
    return ET.fromstring(resp.text).findtext("Code")


METHODS = pytest.mark.parametrize("method", ["get", "head"])


# --- If-Match ---


@METHODS
@pytest.mark.parametrize(
    "value", [ETAG, ETAG.strip('"'), "*", f'"other", {ETAG}', f'"other",{ETAG}']
)
def test_if_match_that_matches_serves_the_object(client, method, value):
    resp = _request(client, method, {"If-Match": value})
    assert resp.status_code == 200
    assert resp.headers["etag"] == ETAG
    if method == "get":
        assert resp.content == BODY


@METHODS
@pytest.mark.parametrize("value", ['"other"', f"W/{ETAG}", f"{ETAG}x"])
def test_if_match_that_does_not_match_is_412(client, method, value):
    resp = _request(client, method, {"If-Match": value})
    assert resp.status_code == 412
    if method == "get":
        assert _code(resp) == "PreconditionFailed"
        assert "etag" not in resp.headers


# --- If-None-Match ---


@METHODS
@pytest.mark.parametrize("value", [ETAG, ETAG.strip('"'), "*", f"W/{ETAG}", f'"other", {ETAG}'])
def test_if_none_match_that_matches_is_304(client, method, value):
    resp = _request(client, method, {"If-None-Match": value})
    assert resp.status_code == 304
    assert resp.content == b""


@METHODS
@pytest.mark.parametrize("value", ['"other"', 'W/"other"', f"{ETAG}x"])
def test_if_none_match_that_does_not_match_serves_the_object(client, method, value):
    resp = _request(client, method, {"If-None-Match": value})
    assert resp.status_code == 200
    if method == "get":
        assert resp.content == BODY


@METHODS
def test_a_304_carries_the_etag_and_the_stored_cache_headers_and_nothing_else(client, method):
    resp = _request(client, method, {"If-None-Match": ETAG})
    assert resp.status_code == 304
    assert resp.headers["etag"] == ETAG
    assert resp.headers["cache-control"] == "max-age=60"
    assert resp.headers["expires"] == "Wed, 01 Jan 2100 00:00:00 GMT"
    for name in ("last-modified", "content-type", "content-length", "accept-ranges"):
        assert name not in resp.headers, name
    assert "x-amz-request-id" in resp.headers


@METHODS
def test_a_304_leaves_out_cache_headers_that_are_not_stored(client, method):
    assert client.put(f"/{BUCKET}/plain", content=b"x").status_code == 200
    etag = client.head(f"/{BUCKET}/plain").headers["etag"]
    resp = _request(client, method, {"If-None-Match": etag}, key="plain")
    assert resp.status_code == 304
    assert set(resp.headers) - {"x-amz-request-id", "x-amz-id-2"} == {"etag"}


@METHODS
def test_a_304_ignores_response_header_overrides(client, method):
    resp = client.request(
        method.upper(),
        f"/{BUCKET}/{KEY}?response-content-type=text/plain",
        headers={"If-None-Match": ETAG},
    )
    assert resp.status_code == 304
    assert "content-type" not in resp.headers


# --- If-Modified-Since ---


@METHODS
def test_if_modified_since(client, method, modified):
    before = _request(
        client, method, {"If-Modified-Since": _http_date(modified - timedelta(seconds=1))}
    )
    assert before.status_code == 200
    for when in (modified, modified + timedelta(seconds=1), modified + timedelta(days=3650)):
        resp = _request(client, method, {"If-Modified-Since": _http_date(when)})
        assert resp.status_code == 304, when
        assert resp.headers["etag"] == ETAG


@METHODS
def test_if_modified_since_in_the_other_http_date_formats(client, method, modified):
    rfc850 = (modified + timedelta(seconds=1)).strftime("%A, %d-%b-%y %H:%M:%S GMT")
    asctime = (modified + timedelta(seconds=1)).strftime("%a %b %d %H:%M:%S %Y")
    for value in (rfc850, asctime):
        assert _request(client, method, {"If-Modified-Since": value}).status_code == 304, value


@METHODS
@pytest.mark.parametrize(
    "value", ["", "yesterday", "2100-01-01T00:00:00Z", "Fri, 01 Jan 2100 00:00:00 UTC", "0"]
)
def test_an_invalid_if_modified_since_is_ignored(client, method, value):
    assert _request(client, method, {"If-Modified-Since": value}).status_code == 200


@METHODS
def test_a_list_of_dates_in_if_modified_since_is_ignored(client, method):
    value = "Fri, 01 Jan 2100 00:00:00 GMT, Sat, 02 Jan 2100 00:00:00 GMT"
    assert _request(client, method, {"If-Modified-Since": value}).status_code == 200


# --- If-Unmodified-Since ---


@METHODS
def test_if_unmodified_since(client, method, modified):
    resp = _request(
        client, method, {"If-Unmodified-Since": _http_date(modified - timedelta(seconds=1))}
    )
    assert resp.status_code == 412
    if method == "get":
        assert _code(resp) == "PreconditionFailed"
    for when in (modified, modified + timedelta(days=3650)):
        assert (
            _request(client, method, {"If-Unmodified-Since": _http_date(when)}).status_code == 200
        )


@METHODS
@pytest.mark.parametrize("value", ["", "yesterday", "Sat, 29 Oct 1994 19:43:31 UTC", "0"])
def test_an_invalid_if_unmodified_since_is_ignored(client, method, value):
    assert _request(client, method, {"If-Unmodified-Since": value}).status_code == 200


# --- precedence (RFC 9110 13.2.2; the two combinations the S3 reference documents) ---


@METHODS
def test_if_match_true_with_if_unmodified_since_false_is_200(client, method, modified):
    past = _http_date(modified - timedelta(days=1))
    resp = _request(client, method, {"If-Match": ETAG, "If-Unmodified-Since": past})
    assert resp.status_code == 200


@METHODS
def test_if_none_match_false_with_if_modified_since_true_is_304(client, method, modified):
    past = _http_date(modified - timedelta(days=1))
    resp = _request(client, method, {"If-None-Match": ETAG, "If-Modified-Since": past})
    assert resp.status_code == 304


@METHODS
def test_if_none_match_true_with_if_modified_since_false_is_200(client, method, modified):
    future = _http_date(modified + timedelta(days=1))
    resp = _request(client, method, {"If-None-Match": '"other"', "If-Modified-Since": future})
    assert resp.status_code == 200


@METHODS
def test_a_failed_if_match_is_412_before_any_304(client, method, modified):
    headers = {
        "If-Match": '"other"',
        "If-None-Match": ETAG,
        "If-Modified-Since": _http_date(modified + timedelta(days=1)),
    }
    assert _request(client, method, headers).status_code == 412


@METHODS
def test_a_failed_if_unmodified_since_is_412_before_a_304(client, method, modified):
    headers = {
        "If-Unmodified-Since": _http_date(modified - timedelta(days=1)),
        "If-None-Match": ETAG,
    }
    assert _request(client, method, headers).status_code == 412


@METHODS
def test_all_four_conditions_that_hold_serve_the_object(client, method, modified):
    headers = {
        "If-Match": "*",
        "If-Unmodified-Since": _http_date(modified + timedelta(days=1)),
        "If-None-Match": '"other"',
        "If-Modified-Since": _http_date(modified - timedelta(days=1)),
    }
    assert _request(client, method, headers).status_code == 200


# --- a missing bucket or key beats every condition ---


@METHODS
def test_a_missing_key_is_404_whatever_the_conditions(client, method):
    for headers in ({"If-Match": '"x"'}, {"If-None-Match": "*"}, {"If-Match": "*"}):
        resp = _request(client, method, headers, key="no-such-key")
        assert resp.status_code == 404, headers
        if method == "get":
            assert _code(resp) == "NoSuchKey"


@METHODS
def test_a_missing_bucket_is_404_whatever_the_conditions(client, method):
    resp = getattr(client, method)("/no-bucket/key", headers={"If-None-Match": "*"})
    assert resp.status_code == 404
    if method == "get":
        assert _code(resp) == "NoSuchBucket"


# --- a Range request (RFC 9110 13.2.2 comes before range handling) ---


def test_a_304_beats_a_satisfiable_range(client):
    resp = client.get(f"/{BUCKET}/{KEY}", headers={"Range": "bytes=0-9", "If-None-Match": ETAG})
    assert resp.status_code == 304
    assert "content-range" not in resp.headers


def test_a_304_beats_an_unsatisfiable_range(client):
    resp = client.get(f"/{BUCKET}/{KEY}", headers={"Range": "bytes=500-", "If-None-Match": ETAG})
    assert resp.status_code == 304


def test_a_412_beats_an_unsatisfiable_range(client):
    resp = client.get(f"/{BUCKET}/{KEY}", headers={"Range": "bytes=500-", "If-Match": '"other"'})
    assert resp.status_code == 412


def test_head_412_beats_an_unsatisfiable_range(client):
    resp = client.head(f"/{BUCKET}/{KEY}", headers={"Range": "bytes=500-", "If-Match": '"other"'})
    assert resp.status_code == 412


def test_conditions_that_hold_leave_a_range_to_be_served(client):
    resp = client.get(
        f"/{BUCKET}/{KEY}",
        headers={"Range": "bytes=10-19", "If-Match": ETAG, "If-None-Match": '"other"'},
    )
    assert resp.status_code == 206
    assert resp.content == BODY[10:20]
    assert resp.headers["content-range"] == "bytes 10-19/100"


def test_conditions_that_hold_leave_an_unsatisfiable_range_a_416(client):
    resp = client.get(f"/{BUCKET}/{KEY}", headers={"Range": "bytes=500-", "If-Match": ETAG})
    assert resp.status_code == 416
    assert _code(resp) == "InvalidRange"


# --- the object is untouched by any of it ---


def test_refused_and_not_modified_requests_leave_the_object_alone(client):
    client.get(f"/{BUCKET}/{KEY}", headers={"If-Match": '"other"'})
    client.get(f"/{BUCKET}/{KEY}", headers={"If-None-Match": ETAG})
    resp = client.get(f"/{BUCKET}/{KEY}")
    assert resp.status_code == 200
    assert resp.content == BODY
