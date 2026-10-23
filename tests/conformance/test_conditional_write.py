"""Conditional writes are not evaluated yet, so they are refused rather than ignored.

Ignoring a write precondition would overwrite an object the client asked to keep.
Header names are from the S3 API reference: PutObject (If-Match, If-None-Match,
x-amz-write-offset-bytes) and CompleteMultipartUpload (If-Match, If-None-Match).
"""

import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.api.request_guard import (
    CONDITIONAL_COMPLETE_HEADERS,
    CONDITIONAL_PUT_OBJECT_HEADERS,
)
from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.filesystem import FilesystemBackend

BUCKET = "test-bucket"
KEY = "obj.txt"
BODY = b"precious content"
ETAG = '"' + hashlib.md5(BODY).hexdigest() + '"'

# Values that would satisfy each condition against the existing object (or a missing
# key, for If-None-Match), so a refusal cannot be mistaken for a precondition failure.
SATISFIED_ON_EXISTING = {
    "if-match": ETAG,
    "if-none-match": '"0000"',
    "x-amz-write-offset-bytes": str(len(BODY)),
}
SATISFIED_ON_MISSING = {"if-match": "*", "if-none-match": "*", "x-amz-write-offset-bytes": "0"}


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    storage = FilesystemBackend(data_dir=tmp_path / "objects", parts_dir=tmp_path / "parts")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    c = TestClient(create_app(storage=storage, metadata=metadata), raise_server_exceptions=False)
    assert c.put(f"/{BUCKET}").status_code == 200
    assert c.put(f"/{BUCKET}/{KEY}", content=BODY).status_code == 200
    return c


def _assert_intact(client: TestClient) -> None:
    resp = client.get(f"/{BUCKET}/{KEY}")
    assert resp.status_code == 200
    assert resp.content == BODY
    assert resp.headers["etag"] == ETAG


def _assert_not_implemented(resp) -> None:
    assert resp.status_code == 501
    assert ET.fromstring(resp.text).findtext("Code") == "NotImplemented"


def test_header_lists_match_the_reference():
    assert set(CONDITIONAL_PUT_OBJECT_HEADERS) == {
        "if-match",
        "if-none-match",
        "x-amz-write-offset-bytes",
    }
    assert set(CONDITIONAL_COMPLETE_HEADERS) == {"if-match", "if-none-match"}


# --- PutObject ---


@pytest.mark.parametrize("header", sorted(SATISFIED_ON_EXISTING))
def test_conditional_put_over_existing_object_returns_501_and_keeps_it(client, header):
    resp = client.put(
        f"/{BUCKET}/{KEY}", content=b"overwrite", headers={header: SATISFIED_ON_EXISTING[header]}
    )
    _assert_not_implemented(resp)
    _assert_intact(client)


@pytest.mark.parametrize("header", sorted(SATISFIED_ON_MISSING))
def test_conditional_put_of_new_key_returns_501_and_creates_nothing(client, header):
    resp = client.put(
        f"/{BUCKET}/new.txt", content=b"new", headers={header: SATISFIED_ON_MISSING[header]}
    )
    _assert_not_implemented(resp)
    assert client.get(f"/{BUCKET}/new.txt").status_code == 404


def test_unconditional_put_still_overwrites(client):
    assert client.put(f"/{BUCKET}/{KEY}", content=b"new body").status_code == 200
    assert client.get(f"/{BUCKET}/{KEY}").content == b"new body"


# --- CompleteMultipartUpload ---


def _prepare_upload(client: TestClient, key: str) -> tuple[str, bytes]:
    uid = ET.fromstring(client.post(f"/{BUCKET}/{key}?uploads").text).findtext("UploadId")
    r = client.put(f"/{BUCKET}/{key}?partNumber=1&uploadId={uid}", content=b"part one")
    assert r.status_code == 200
    body = (
        f"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>"
        f"<ETag>{r.headers['etag']}</ETag></Part></CompleteMultipartUpload>"
    ).encode()
    return uid, body


@pytest.mark.parametrize("header", sorted(CONDITIONAL_COMPLETE_HEADERS))
def test_conditional_complete_over_existing_object_returns_501_and_keeps_it(client, header):
    uid, body = _prepare_upload(client, KEY)
    value = SATISFIED_ON_EXISTING[header]
    resp = client.post(f"/{BUCKET}/{KEY}?uploadId={uid}", content=body, headers={header: value})
    _assert_not_implemented(resp)
    _assert_intact(client)
    # The upload is still open: completing it without the condition works.
    assert client.post(f"/{BUCKET}/{KEY}?uploadId={uid}", content=body).status_code == 200
    assert client.get(f"/{BUCKET}/{KEY}").content == b"part one"


@pytest.mark.parametrize("header", sorted(CONDITIONAL_COMPLETE_HEADERS))
def test_conditional_complete_of_new_key_returns_501_and_creates_nothing(client, header):
    uid, body = _prepare_upload(client, "new.txt")
    value = SATISFIED_ON_MISSING[header]
    resp = client.post(f"/{BUCKET}/new.txt?uploadId={uid}", content=body, headers={header: value})
    _assert_not_implemented(resp)
    assert client.get(f"/{BUCKET}/new.txt").status_code == 404


# --- unaffected operations ---


def test_upload_part_is_not_refused_for_these_headers(client):
    # If-Match / If-None-Match are not UploadPart parameters; the part is stored and
    # nothing is overwritten until Complete, which does check them.
    uid = ET.fromstring(client.post(f"/{BUCKET}/{KEY}?uploads").text).findtext("UploadId")
    resp = client.put(
        f"/{BUCKET}/{KEY}?partNumber=1&uploadId={uid}", content=b"p", headers={"if-match": "*"}
    )
    assert resp.status_code == 200
    _assert_intact(client)


def test_conditional_get_still_served(client):
    assert client.get(f"/{BUCKET}/{KEY}", headers={"if-match": ETAG}).status_code == 200
