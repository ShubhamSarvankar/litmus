"""Conditional deletes that are not evaluated are refused rather than ignored.

DeleteObject evaluates If-Match (tests/conformance/test_conditional_delete_object.py);
every other conditional header on it, and all of them on AbortMultipartUpload, are 501.
DeleteObjects evaluates the ETag of an entry (tests/conformance/test_delete_objects_etag.py);
its LastModifiedTime and Size are a per-key NotImplemented error.

Ignoring a precondition would delete an object (or abort an upload) the client asked
to keep. Header and element names are from the S3 API reference: DeleteObject
(If-Match, x-amz-if-match-last-modified-time, x-amz-if-match-size),
AbortMultipartUpload (x-amz-if-match-initiated-time) and the DeleteObjects
ObjectIdentifier (ETag, LastModifiedTime, Size; the last two for directory buckets
only). If-None-Match, If-Modified-Since and
If-Unmodified-Since are refused on DELETE as well.
"""

import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.api.request_guard import CONDITIONAL_DELETE_HEADERS
from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "test-bucket"
KEY = "obj.txt"
BODY = b"precious content"
ETAG = '"' + hashlib.md5(BODY).hexdigest() + '"'
NS = "http://s3.amazonaws.com/doc/2006-03-01/"

# A value that would satisfy each condition, so a refusal cannot be mistaken for a
# precondition failure.
SATISFIED = {
    "if-match": ETAG,
    "x-amz-if-match-last-modified-time": "Wed, 01 Jan 2020 00:00:00 GMT",
    "x-amz-if-match-size": str(len(BODY)),
    "x-amz-if-match-initiated-time": "Wed, 01 Jan 2020 00:00:00 GMT",
    "if-none-match": '"0000"',
    "if-modified-since": "Wed, 01 Jan 2020 00:00:00 GMT",
    "if-unmodified-since": "Fri, 01 Jan 2100 00:00:00 GMT",
}


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    c = TestClient(create_app(storage=storage, metadata=metadata), raise_server_exceptions=False)
    assert c.put(f"/{BUCKET}").status_code == 200
    assert c.put(f"/{BUCKET}/{KEY}", content=BODY).status_code == 200
    return c


def _assert_intact(client: TestClient) -> None:
    resp = client.get(f"/{BUCKET}/{KEY}")
    assert resp.status_code == 200
    assert resp.content == BODY


def _assert_not_implemented(resp) -> None:
    assert resp.status_code == 501
    assert ET.fromstring(resp.text).findtext("Code") == "NotImplemented"


def test_header_list_covers_the_reference_and_rfc_7232_preconditions():
    assert set(CONDITIONAL_DELETE_HEADERS) == set(SATISFIED)


@pytest.mark.parametrize("header", sorted(set(SATISFIED) - {"if-match"}))
def test_delete_object_with_condition_returns_501_and_keeps_object(client, header):
    _assert_not_implemented(client.delete(f"/{BUCKET}/{KEY}", headers={header: SATISFIED[header]}))
    _assert_intact(client)


def test_delete_with_null_version_and_condition_returns_501(client):
    resp = client.delete(
        f"/{BUCKET}/{KEY}?versionId=null", headers={"x-amz-if-match-size": str(len(BODY))}
    )
    _assert_not_implemented(resp)
    _assert_intact(client)


def test_delete_object_without_condition_still_deletes(client):
    assert client.delete(f"/{BUCKET}/{KEY}").status_code == 204
    assert client.get(f"/{BUCKET}/{KEY}").status_code == 404


@pytest.mark.parametrize("header", sorted(SATISFIED))
def test_abort_upload_with_condition_returns_501_and_keeps_upload(client, header):
    uid = ET.fromstring(client.post(f"/{BUCKET}/mp?uploads").text).findtext("UploadId")
    assert client.put(f"/{BUCKET}/mp?partNumber=1&uploadId={uid}", content=b"p").status_code == 200
    resp = client.delete(f"/{BUCKET}/mp?uploadId={uid}", headers={header: SATISFIED[header]})
    _assert_not_implemented(resp)
    # Still open: an unconditional abort succeeds.
    assert client.delete(f"/{BUCKET}/mp?uploadId={uid}").status_code == 204


# --- DeleteObjects: per-key conditions that are not evaluated ---


def _delete_objects(client: TestClient, inner: str, namespaced: bool = True):
    xmlns = f' xmlns="{NS}"' if namespaced else ""
    resp = client.post(f"/{BUCKET}?delete", content=f"<Delete{xmlns}>{inner}</Delete>".encode())
    assert resp.status_code == 200
    return ET.fromstring(resp.text)


@pytest.mark.parametrize("namespaced", [True, False])
@pytest.mark.parametrize(
    "condition",
    [
        "<LastModifiedTime>2020-01-01T00:00:00.000Z</LastModifiedTime>",
        f"<Size>{len(BODY)}</Size>",
    ],
)
def test_delete_objects_conditional_entry_gets_error_and_keeps_object(
    client, condition, namespaced
):
    root = _delete_objects(client, f"<Object><Key>{KEY}</Key>{condition}</Object>", namespaced)
    assert root.findall("Deleted") == []
    errors = root.findall("Error")
    assert [(e.findtext("Key"), e.findtext("Code")) for e in errors] == [(KEY, "NotImplemented")]
    _assert_intact(client)


def test_delete_objects_mixed_batch_deletes_only_unconditional_entries(client):
    assert client.put(f"/{BUCKET}/plain.txt", content=b"p").status_code == 200
    root = _delete_objects(
        client,
        f"<Object><Key>{KEY}</Key><Size>{len(BODY)}</Size></Object>"
        "<Object><Key>plain.txt</Key></Object>",
    )
    assert [d.findtext("Key") for d in root.findall("Deleted")] == ["plain.txt"]
    assert [e.findtext("Key") for e in root.findall("Error")] == [KEY]
    assert client.get(f"/{BUCKET}/plain.txt").status_code == 404
    _assert_intact(client)
