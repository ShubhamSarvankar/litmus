"""A retried CompleteMultipartUpload on a completed upload (docs/design/storage-layout.md
decision 35).

The upload is completed, so a second Complete is 404 NoSuchUpload, the answer "Error
responses" and "CompleteMultipartUpload" document for an upload that was "aborted or
completed". The API reference describes no replay of the first answer. The retry changes
nothing: the object keeps its bytes and ETag, no upload, part or blob row is added, and no
file appears under the blob directory.
"""

import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "complete-retry"
KEY = "obj.bin"
PARTS = [b"part one. " * 50, b"part two! " * 60, b"part three " * 20]
BODY = b"".join(PARTS)
ETAG = '"' + hashlib.md5(b"".join(hashlib.md5(p).digest() for p in PARTS)).hexdigest() + '-3"'


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    assert client.put(f"/{BUCKET}").status_code == 200
    return client, storage, metadata


def _code(resp) -> str:
    return ET.fromstring(resp.text).findtext("Code")


def _body(numbered_etags: list[tuple[int, str]]) -> bytes:
    items = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>" for n, e in numbered_etags
    )
    return f"<CompleteMultipartUpload>{items}</CompleteMultipartUpload>".encode()


def _completed_upload(client) -> tuple[str, list[str]]:
    """An upload of PARTS completed once, answered 200; its id and the parts' ETags."""
    uid = ET.fromstring(client.post(f"/{BUCKET}/{KEY}?uploads").text).findtext("UploadId")
    etags = []
    for number, data in enumerate(PARTS, start=1):
        r = client.put(f"/{BUCKET}/{KEY}?partNumber={number}&uploadId={uid}", content=data)
        assert r.status_code == 200
        etags.append(r.headers["etag"])
    done = client.post(f"/{BUCKET}/{KEY}?uploadId={uid}", content=_body(list(enumerate(etags, 1))))
    assert done.status_code == 200
    assert ET.fromstring(done.text).findtext("ETag") == ETAG
    return uid, etags


def _complete(client, uid: str, numbered_etags: list[tuple[int, str]]):
    return client.post(f"/{BUCKET}/{KEY}?uploadId={uid}", content=_body(numbered_etags))


def _rows(storage, metadata, uid: str) -> tuple:
    """Every row and file the retry must leave alone."""
    return (
        metadata.list_objects(BUCKET),
        metadata.list_blob_records(),
        metadata.get_upload(uid, bucket=BUCKET, key=KEY),
        metadata.get_parts(uid),
        metadata.list_incomplete_uploads(),
        sorted(storage.scan()),
        storage.temp_entries(),
    )


def _assert_object_unchanged(client) -> None:
    got = client.get(f"/{BUCKET}/{KEY}")
    assert got.status_code == 200
    assert got.headers["etag"] == ETAG
    assert got.content == BODY


def _blob_files(tmp_path: Path) -> list[str]:
    return sorted(
        str(p.relative_to(tmp_path)) for p in (tmp_path / "blobs").rglob("*") if p.is_file()
    )


def _retry_changes_nothing(env, tmp_path: Path, retry_parts) -> None:
    client, storage, metadata = env
    uid, etags = _completed_upload(client)
    numbered = retry_parts(etags)
    rows_before = _rows(storage, metadata, uid)
    files_before = _blob_files(tmp_path)
    counts_before = (
        len(metadata.list_blob_records()),
        len(metadata.get_parts(uid)),
        len(metadata.list_objects(BUCKET)),
    )

    resp = _complete(client, uid, numbered)

    assert resp.status_code == 404
    assert _code(resp) == "NoSuchUpload"
    _assert_object_unchanged(client)
    assert _rows(storage, metadata, uid) == rows_before
    assert _blob_files(tmp_path) == files_before
    assert (
        len(metadata.list_blob_records()),
        len(metadata.get_parts(uid)),
        len(metadata.list_objects(BUCKET)),
    ) == counts_before


def test_retried_complete_with_the_same_body_is_no_such_upload(env, tmp_path):
    _retry_changes_nothing(env, tmp_path, lambda etags: list(enumerate(etags, start=1)))


def test_retried_complete_with_a_different_part_list_is_no_such_upload(env, tmp_path):
    _retry_changes_nothing(env, tmp_path, lambda etags: [(1, etags[0]), (2, etags[1])])
