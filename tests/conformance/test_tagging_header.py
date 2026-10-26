"""PutObject and CreateMultipartUpload refuse a non-empty x-amz-tagging with 501
NotImplemented, before any body byte is read (server/api/request_guard.py). Litmus
stores no tags, so honoring the header would silently drop them. An empty or blank
value asks for no tags and is accepted. CopyObject, UploadPart and UploadPartCopy are
not touched by this check."""

import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "test-bucket"
KEY = "obj.txt"
OLD = b"old content"
NEW = b"new content that must never be stored"
TAGS = {"x-amz-tagging": "a=b&c=d"}


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    assert client.put(f"/{BUCKET}").status_code == 200
    return client, storage, metadata


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def _state(storage, metadata) -> tuple:
    """Everything a refused request must leave alone."""
    registry = sorted((r.blob_id, r.state) for r in metadata.list_blob_records())
    uploads = sorted(u.upload_id for u in metadata.list_incomplete_uploads())
    return registry, uploads, storage.temp_entries()


def _assert_clean(storage, metadata) -> None:
    assert run_consistency_sweep(metadata, storage).issues == []
    assert storage.temp_entries() == []
    assert {r.state for r in metadata.list_blob_records()} <= {"live"}


def _assert_not_implemented(resp) -> ET.Element:
    assert resp.status_code == 501
    assert resp.headers["content-type"].startswith("application/xml")
    root = ET.fromstring(resp.text)
    assert root.findtext("Code") == "NotImplemented"
    assert root.findtext("Message")
    assert root.findtext("RequestId")
    assert resp.headers["x-amz-request-id"] == root.findtext("RequestId")
    return root


# --- PutObject ---


@pytest.mark.parametrize("value", ["a=b", "a=b&c=d", "a", " a=b ", "=", "a=b,c"])
def test_put_object_with_tags_is_501_and_stores_nothing(env, value):
    client, storage, metadata = env
    before = _state(storage, metadata)
    resp = client.put(f"/{BUCKET}/{KEY}", content=NEW, headers={"x-amz-tagging": value})
    root = _assert_not_implemented(resp)
    assert root.findtext("Header") == "x-amz-tagging"
    assert client.get(f"/{BUCKET}/{KEY}").status_code == 404
    assert metadata.get_object_meta(BUCKET, KEY) is None
    assert _state(storage, metadata) == before
    _assert_clean(storage, metadata)


def test_put_object_with_tags_is_refused_whatever_the_directive_header(env):
    client, storage, metadata = env
    headers = {**TAGS, "x-amz-tagging-directive": "COPY"}
    _assert_not_implemented(client.put(f"/{BUCKET}/{KEY}", content=NEW, headers=headers))
    assert metadata.get_object_meta(BUCKET, KEY) is None
    _assert_clean(storage, metadata)


def test_put_object_with_tags_is_refused_with_an_sdk_operation_hint(env):
    client, storage, metadata = env
    resp = client.put(f"/{BUCKET}/{KEY}?x-id=PutObject", content=NEW, headers=TAGS)
    _assert_not_implemented(resp)
    assert metadata.get_object_meta(BUCKET, KEY) is None
    _assert_clean(storage, metadata)


def test_put_object_is_refused_before_the_body_is_read(env):
    """The body is a generator that fails if the server asks for a byte."""
    client, storage, metadata = env

    def body():
        raise AssertionError("the request body was read")
        yield b""

    resp = client.put(f"/{BUCKET}/{KEY}", content=body(), headers=TAGS)
    _assert_not_implemented(resp)
    _assert_clean(storage, metadata)


@pytest.mark.parametrize("value", ["", " ", " \t "])
def test_put_object_with_an_empty_or_blank_tag_header_is_stored(env, value):
    client, storage, metadata = env
    resp = client.put(f"/{BUCKET}/{KEY}", content=OLD, headers={"x-amz-tagging": value})
    assert resp.status_code == 200
    assert resp.headers["etag"] == _md5(OLD)
    got = client.get(f"/{BUCKET}/{KEY}")
    assert got.status_code == 200 and got.content == OLD
    _assert_clean(storage, metadata)


def test_put_object_without_the_tag_header_is_stored(env):
    client, _, _ = env
    assert client.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200


def test_put_object_with_tags_over_an_existing_key_leaves_the_old_object(env):
    client, storage, metadata = env
    assert client.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200
    before = _state(storage, metadata)
    meta_before = metadata.get_object_meta(BUCKET, KEY)
    _assert_not_implemented(client.put(f"/{BUCKET}/{KEY}", content=NEW, headers=TAGS))
    got = client.get(f"/{BUCKET}/{KEY}")
    assert got.status_code == 200
    assert got.content == OLD
    assert got.headers["etag"] == _md5(OLD)
    assert metadata.get_object_meta(BUCKET, KEY) == meta_before
    assert _state(storage, metadata) == before
    _assert_clean(storage, metadata)


# --- CreateMultipartUpload ---


@pytest.mark.parametrize("value", ["a=b", "a=b&c=d"])
def test_create_multipart_upload_with_tags_is_501_and_creates_no_upload(env, value):
    client, storage, metadata = env
    before = _state(storage, metadata)
    resp = client.post(f"/{BUCKET}/{KEY}?uploads", headers={"x-amz-tagging": value})
    root = _assert_not_implemented(resp)
    assert root.findtext("Header") == "x-amz-tagging"
    assert metadata.list_incomplete_uploads() == []
    assert _state(storage, metadata) == before
    assert client.get(f"/{BUCKET}/{KEY}").status_code == 404
    _assert_clean(storage, metadata)


@pytest.mark.parametrize("value", ["", "  "])
def test_create_multipart_upload_with_an_empty_or_blank_tag_header_creates_the_upload(env, value):
    client, _, metadata = env
    resp = client.post(f"/{BUCKET}/{KEY}?uploads", headers={"x-amz-tagging": value})
    assert resp.status_code == 200
    upload_id = ET.fromstring(resp.text).findtext("UploadId")
    assert metadata.upload_exists(upload_id)


# --- operations the check must not catch ---


def test_copy_object_with_tags_and_no_directive_still_succeeds(env):
    """A PUT with x-amz-copy-source is CopyObject, which decides x-amz-tagging with its
    directive: the tags come from the source, so the header is not read."""
    client, storage, metadata = env
    assert client.put(f"/{BUCKET}/src", content=OLD).status_code == 200
    resp = client.put(f"/{BUCKET}/dst", headers={"x-amz-copy-source": f"/{BUCKET}/src", **TAGS})
    assert resp.status_code == 200
    assert client.get(f"/{BUCKET}/dst").content == OLD
    _assert_clean(storage, metadata)


def test_upload_part_with_tags_is_not_caught_by_the_check(env):
    client, _, _ = env
    upload_id = ET.fromstring(client.post(f"/{BUCKET}/{KEY}?uploads").text).findtext("UploadId")
    resp = client.put(
        f"/{BUCKET}/{KEY}?partNumber=1&uploadId={upload_id}", content=OLD, headers=TAGS
    )
    assert resp.status_code == 200


def test_upload_part_copy_with_tags_is_not_caught_by_the_check(env):
    client, _, _ = env
    assert client.put(f"/{BUCKET}/src", content=OLD).status_code == 200
    upload_id = ET.fromstring(client.post(f"/{BUCKET}/{KEY}?uploads").text).findtext("UploadId")
    resp = client.put(
        f"/{BUCKET}/{KEY}?partNumber=1&uploadId={upload_id}",
        headers={"x-amz-copy-source": f"/{BUCKET}/src", **TAGS},
    )
    assert resp.status_code == 200
