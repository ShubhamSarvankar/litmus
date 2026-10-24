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
    ["bytes=100-100", "bytes=100-", "bytes=200-300", "bytes=50-10", "bytes=-0"],
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


# --- empty object: no byte position exists, so every Range form is unsatisfiable ---


@pytest.mark.parametrize(
    "spec",
    ["bytes=0-0", "bytes=0-", "bytes=0-10", "bytes=-1", "bytes=-10", "bytes=40-50", "bytes=5-"],
)
def test_empty_object_every_range_is_invalid_range(client, spec):
    resp = client.get("/range-bucket/empty.bin", headers={"Range": spec})
    _assert_invalid_range(resp, 0)


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


def test_suffix_longer_than_object_returns_whole_object(client):
    resp = client.get("/range-bucket/data.bin", headers={"Range": "bytes=-500"})
    assert resp.status_code == 206
    assert resp.content == BODY
    assert resp.headers["content-range"] == "bytes 0-99/100"


# --- HEAD ---


@pytest.mark.parametrize("key, size", [("data.bin", len(BODY)), ("empty.bin", 0)])
def test_head_unsatisfiable_range_is_416_without_body(client, key, size):
    resp = client.head(f"/range-bucket/{key}", headers={"Range": "bytes=200-300"})
    assert resp.status_code == 416
    assert resp.content == b""
    assert resp.headers["content-range"] == f"bytes */{size}"
    assert resp.headers["x-amz-request-id"]


def test_head_satisfiable_range_is_unchanged(client):
    # S3 narrows only Content-Length for a satisfiable range on HEAD; Litmus answers
    # as if there were no Range (docs/compatibility.md, undocumented choices).
    resp = client.head("/range-bucket/data.bin", headers={"Range": "bytes=0-9"})
    assert resp.status_code == 200
    assert resp.headers["content-length"] == "100"
