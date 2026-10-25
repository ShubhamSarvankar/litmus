"""An unsatisfiable Range on GET is a 416 InvalidRange with the standard S3 error
body; on an empty object every Range form is unsatisfiable; HEAD answers 416 too."""

import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BODY = bytes(range(100))


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    app = create_app(storage=storage, metadata=metadata)
    c = TestClient(app, raise_server_exceptions=False)
    c.put("/range-bucket")
    c.put("/range-bucket/data.bin", content=BODY)
    c.put("/range-bucket/empty.bin", content=b"")
    return c


def _assert_invalid_range(resp, size):
    assert resp.status_code == 416
    assert resp.headers["content-type"] == "application/xml"
    assert resp.headers["content-range"] == f"bytes */{size}"
    root = ET.fromstring(resp.content)
    assert root.tag == "Error"
    assert root.findtext("Code") == "InvalidRange"
    assert root.findtext("Message")
    assert root.findtext("RequestId") == resp.headers["x-amz-request-id"]
    assert root.findtext("HostId") == resp.headers["x-amz-id-2"]


# --- non-empty object ---


@pytest.mark.parametrize(
    "spec",
    ["bytes=100-100", "bytes=100-", "bytes=200-300", "bytes=100-999", "bytes=-0"],
)
def test_unsatisfiable_range_is_invalid_range(client, spec):
    resp = client.get("/range-bucket/data.bin", headers={"Range": spec})
    _assert_invalid_range(resp, len(BODY))


def test_unsatisfiable_range_sends_no_object_headers(client):
    resp = client.get("/range-bucket/data.bin", headers={"Range": "bytes=200-300"})
    assert "etag" not in resp.headers
    assert "last-modified" not in resp.headers


def test_unsatisfiable_range_does_not_break_the_next_request(client):
    client.get("/range-bucket/data.bin", headers={"Range": "bytes=200-300"})
    resp = client.get("/range-bucket/data.bin")
    assert resp.status_code == 200
    assert resp.content == BODY


def test_unsatisfiable_range_error_ids_are_fresh(client):
    a = client.get("/range-bucket/data.bin", headers={"Range": "bytes=200-300"})
    b = client.get("/range-bucket/data.bin", headers={"Range": "bytes=200-300"})
    assert a.headers["x-amz-request-id"] != b.headers["x-amz-request-id"]


# --- empty object: an int-range names a byte that does not exist, and a zero suffix
# is unsatisfiable; a non-zero suffix is satisfiable but has nothing to send, so it is
# ignored (RFC 9110 14.1.2, 14.2) ---


@pytest.mark.parametrize(
    "spec",
    ["bytes=0-0", "bytes=0-", "bytes=0-10", "bytes=40-50", "bytes=5-", "bytes=-0"],
)
def test_empty_object_unsatisfiable_range_is_invalid_range(client, spec):
    resp = client.get("/range-bucket/empty.bin", headers={"Range": spec})
    _assert_invalid_range(resp, 0)


@pytest.mark.parametrize("spec", ["bytes=-1", "bytes=-10"])
def test_empty_object_suffix_range_is_ignored(client, spec):
    resp = client.get("/range-bucket/empty.bin", headers={"Range": spec})
    assert resp.status_code == 200
    assert resp.content == b""
    assert resp.headers["content-length"] == "0"
    assert "content-range" not in resp.headers


def test_empty_object_without_range_is_200(client):
    resp = client.get("/range-bucket/empty.bin")
    assert resp.status_code == 200
    assert resp.content == b""


# --- satisfiable ranges are unchanged ---


@pytest.mark.parametrize(
    "spec, start, end",
    [("bytes=0-9", 0, 9), ("bytes=99-99", 99, 99), ("bytes=50-", 50, 99), ("bytes=-10", 90, 99)],
)
def test_satisfiable_range_unchanged(client, spec, start, end):
    resp = client.get("/range-bucket/data.bin", headers={"Range": spec})
    assert resp.status_code == 206
    assert resp.content == BODY[start : end + 1]
    assert resp.headers["content-range"] == f"bytes {start}-{end}/{len(BODY)}"
    assert resp.headers["content-length"] == str(end - start + 1)


@pytest.mark.parametrize(
    "spec, start, end",
    [
        ("bytes=0-100", 0, 99),
        ("bytes=0-999999", 0, 99),
        ("bytes=99-100", 99, 99),
        ("bytes=50-99999999999999999999999", 50, 99),
        ("BYTES=0-9", 0, 9),
        ("Bytes=-10", 90, 99),
    ],
)
def test_end_past_the_object_is_clamped(client, spec, start, end):
    resp = client.get("/range-bucket/data.bin", headers={"Range": spec})
    assert resp.status_code == 206
    assert resp.content == BODY[start : end + 1]
    assert resp.headers["content-range"] == f"bytes {start}-{end}/{len(BODY)}"
    assert resp.headers["content-length"] == str(end - start + 1)


def test_suffix_longer_than_object_returns_whole_object(client):
    resp = client.get("/range-bucket/data.bin", headers={"Range": "bytes=-500"})
    assert resp.status_code == 206
    assert resp.content == BODY
    assert resp.headers["content-range"] == "bytes 0-99/100"


# --- a Range that is ignored serves the whole object with 200 and no Content-Range ---


@pytest.mark.parametrize(
    "spec",
    [
        "bytes=50-10",  # last-pos below first-pos: invalid
        "bytes=abc",
        "bytes=",
        "bytes=-",
        "bytes=a-b",
        "bytes=1-2-3",
        "bytes=+1-5",
        "bytes=1_0-20",
        "bytes= 0-9",
        "bytes=" + "9" * 5000 + "-",  # a numeral no client sends
        "0-9",
        "bytes",
        "items=0-9",
        "lines=0-9",
        "bytes=0-9,20-29",  # a set of ranges: S3 does not support them
        "bytes=0-0,-1",
        "bytes=500-600,0-5",
    ],
)
def test_ignored_range_serves_whole_object(client, spec):
    resp = client.get("/range-bucket/data.bin", headers={"Range": spec})
    assert resp.status_code == 200
    assert resp.content == BODY
    assert resp.headers["content-length"] == str(len(BODY))
    assert "content-range" not in resp.headers


def test_ignored_range_on_empty_object_is_200(client):
    resp = client.get("/range-bucket/empty.bin", headers={"Range": "bytes=0-9,20-29"})
    assert resp.status_code == 200
    assert resp.content == b""


# --- HEAD ---


@pytest.mark.parametrize("key, size", [("data.bin", len(BODY)), ("empty.bin", 0)])
def test_head_unsatisfiable_range_is_416_without_body(client, key, size):
    resp = client.head(f"/range-bucket/{key}", headers={"Range": "bytes=200-300"})
    assert resp.status_code == 416
    assert resp.content == b""
    assert resp.headers["content-range"] == f"bytes */{size}"
    assert resp.headers["x-amz-request-id"]


@pytest.mark.parametrize(
    "spec", ["bytes=0-999", "bytes=-500", "bytes=50-10", "bytes=0-9,20-29", "items=0-9"]
)
def test_head_matches_get_for_a_range_it_does_not_narrow(client, spec):
    # GET serves or ignores these; HEAD answers 200 for all of them.
    resp = client.head("/range-bucket/data.bin", headers={"Range": spec})
    assert resp.status_code == 200
    assert resp.headers["content-length"] == "100"
    assert "content-range" not in resp.headers


def test_head_empty_object_matches_get(client):
    unsatisfiable = client.head("/range-bucket/empty.bin", headers={"Range": "bytes=0-"})
    assert unsatisfiable.status_code == 416
    ignored = client.head("/range-bucket/empty.bin", headers={"Range": "bytes=-5"})
    assert ignored.status_code == 200


def test_head_satisfiable_range_is_unchanged(client):
    # S3 narrows only Content-Length for a satisfiable range on HEAD; Litmus answers
    # as if there were no Range (docs/compatibility.md, undocumented choices).
    resp = client.head("/range-bucket/data.bin", headers={"Range": "bytes=0-9"})
    assert resp.status_code == 200
    assert resp.headers["content-length"] == "100"
