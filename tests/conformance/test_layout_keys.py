"""Keys that the old path-derived layout could not store, and DeleteBucket with an
open upload (docs/design/storage-layout.md R1, §6.9, §16)."""

import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "layout"


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


def _listed(client) -> list[str]:
    root = ET.fromstring(client.get(f"/{BUCKET}").text)
    return [el.text for el in root.iter() if el.tag.endswith("Key")]


def _assert_object(client, key: str, body: bytes) -> None:
    resp = client.get(f"/{BUCKET}/{key}")
    assert resp.status_code == 200
    assert resp.content == body
    assert resp.headers["etag"] == _md5(body)


# --- R1: k vs k/, file vs directory ---


def test_k_and_k_slash_are_independent_objects(env):
    client, _, metadata = env
    assert client.put(f"/{BUCKET}/k", content=b"plain").status_code == 200
    assert client.put(f"/{BUCKET}/k/", content=b"slash").status_code == 200
    _assert_object(client, "k", b"plain")
    _assert_object(client, "k/", b"slash")
    assert (
        metadata.get_object_meta(BUCKET, "k").blob_id
        != metadata.get_object_meta(BUCKET, "k/").blob_id
    )
    assert _listed(client) == ["k", "k/"]


def test_deleting_k_slash_keeps_k(env):
    client, _, _ = env
    assert client.put(f"/{BUCKET}/k", content=b"plain").status_code == 200
    assert client.put(f"/{BUCKET}/k/", content=b"slash").status_code == 200
    assert client.delete(f"/{BUCKET}/k/").status_code == 204
    assert client.get(f"/{BUCKET}/k/").status_code == 404
    _assert_object(client, "k", b"plain")


@pytest.mark.parametrize(
    "first,second",
    [("a/b", "a/b/c"), ("a/b/c", "a/b")],
    ids=["file-then-deeper", "deeper-then-file"],
)
def test_file_and_directory_like_keys_coexist(env, first, second):
    client, _, _ = env
    assert client.put(f"/{BUCKET}/{first}", content=first.encode()).status_code == 200
    assert client.put(f"/{BUCKET}/{second}", content=second.encode()).status_code == 200
    _assert_object(client, first, first.encode())
    _assert_object(client, second, second.encode())
    assert sorted(_listed(client)) == sorted([first, second])


# --- 1024-byte keys (S3's maximum key length, in UTF-8 bytes) ---


KEYS_1024 = {
    "one-ascii-segment": "x" * 1024,
    "one-multibyte-segment": "é" * 512,
    "many-segments": ("seg/" * 256)[:1023] + "z",
}


@pytest.mark.parametrize("key", list(KEYS_1024.values()), ids=list(KEYS_1024))
def test_1024_byte_keys_round_trip(env, key):
    client, storage, _ = env
    assert len(key.encode()) == 1024
    body = key.encode()[:64] + b"-body"
    assert client.put(f"/{BUCKET}/{key}", content=body).status_code == 200
    _assert_object(client, key, body)
    assert client.head(f"/{BUCKET}/{key}").status_code == 200
    assert _listed(client) == [key]
    # The key never reaches the filesystem: one blob, named by its ID.
    assert [blob_id is not None for _, blob_id in storage.scan()] == [True]
    assert client.delete(f"/{BUCKET}/{key}").status_code == 204
    assert client.get(f"/{BUCKET}/{key}").status_code == 404


# --- DeleteBucket with an open upload (§6.9, decision 7) ---


def _open_upload(client, key: str = "mp") -> str:
    upload_id = ET.fromstring(client.post(f"/{BUCKET}/{key}?uploads").text).findtext("UploadId")
    for n in (1, 2):
        url = f"/{BUCKET}/{key}?partNumber={n}&uploadId={upload_id}"
        assert client.put(url, content=bytes([n]) * 1000).status_code == 200
    return upload_id


def test_delete_bucket_with_open_upload_aborts_the_upload(env):
    client, storage, metadata = env
    upload_id = _open_upload(client)
    assert client.delete(f"/{BUCKET}").status_code == 204

    assert client.head(f"/{BUCKET}").status_code == 404
    part = client.put(f"/{BUCKET}/mp?partNumber=3&uploadId={upload_id}", content=b"x")
    assert part.status_code == 404
    assert ET.fromstring(part.text).findtext("Code") == "NoSuchUpload"
    assert metadata.get_upload(upload_id) is None
    assert metadata.get_parts(upload_id) == []
    assert metadata.list_blob_records() == []
    assert list(storage.scan()) == []


def test_delete_bucket_with_an_object_keeps_the_object_and_the_upload(env):
    client, storage, metadata = env
    upload_id = _open_upload(client)
    assert client.put(f"/{BUCKET}/obj", content=b"keep me").status_code == 200
    before = (metadata.list_blob_records(), metadata.get_parts(upload_id))

    resp = client.delete(f"/{BUCKET}")
    assert resp.status_code == 409
    assert ET.fromstring(resp.text).findtext("Code") == "BucketNotEmpty"
    assert metadata.upload_exists(upload_id)
    assert (metadata.list_blob_records(), metadata.get_parts(upload_id)) == before
    _assert_object(client, "obj", b"keep me")
