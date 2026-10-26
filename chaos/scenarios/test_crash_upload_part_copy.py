"""
Tier B chaos: an UploadPartCopy killed at each of its crash points
(docs/design/storage-layout.md §6.6, §11, §15).

A part copy is an UploadPart whose bytes come from a range of another object's blob, so
it leaves what a killed UploadPart leaves (§6.6): a pending registry row and a partial or
complete unreferenced blob before the commit, the new part and (when it replaced one) a
garbage blob after it. An earlier part of the same number stays recorded and usable until
the commit. The source is never touched, whatever the point.
"""

import hashlib
import os
import signal
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest

from chaos.fixtures.subprocess import ServerProcess, _find_free_port
from server.consistency import (
    InterruptedCleanup,
    InterruptedWrite,
    LeftoverTemp,
    run_consistency_sweep,
)
from server.crash_points import (
    PART_COPY_AFTER_BYTES,
    PART_COPY_AFTER_COMMIT,
    PART_COPY_AFTER_REGISTER,
    PART_COPY_MID_WRITE,
)
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SIGKILL not available on Windows")

BUCKET = "part-copy-crash"
SRC = "src.bin"
KEY = "dest.bin"
SOURCE = os.urandom(200 * 1024)  # more than three 64 KiB chunks
FIRST, LAST = 1000, 150_999  # a range that does not start at 0 and spans three chunks
RANGED = SOURCE[FIRST : LAST + 1]
OLD = b"previously uploaded part " * 1000
COPY = {
    "x-amz-copy-source": f"{BUCKET}/{SRC}",
    "x-amz-copy-source-range": f"bytes={FIRST}-{LAST}",
}


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def _complete_body(etag: str) -> bytes:
    return (
        f"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part>"
        "</CompleteMultipartUpload>"
    ).encode()


def _start(tmp_path: Path, crash_point: str | None = None) -> tuple[ServerProcess, httpx.Client]:
    port = _find_free_port()
    server = ServerProcess(
        data_dir=tmp_path, db_path=tmp_path / "meta.db", port=port, crash_point=crash_point
    )
    server.start()
    server.wait_ready()
    return server, httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10.0)


@contextmanager
def _restarted(tmp_path: Path):
    server, client = _start(tmp_path)
    try:
        yield client
    finally:
        client.close()
        server.stop()


def _setup(tmp_path: Path, existing_part: bytes | None) -> str:
    """The bucket, the source, an upload and, when given, part 1, by an unarmed server."""
    server, client = _start(tmp_path)
    try:
        assert client.put(f"/{BUCKET}").status_code == 200
        assert client.put(f"/{BUCKET}/{SRC}", content=SOURCE).status_code == 200
        upload = ET.fromstring(client.post(f"/{BUCKET}/{KEY}?uploads").text).findtext("UploadId")
        if existing_part is not None:
            url = f"/{BUCKET}/{KEY}?partNumber=1&uploadId={upload}"
            assert client.put(url, content=existing_part).status_code == 200
        return upload
    finally:
        client.close()
        server.stop()


def _copy_into_crash(tmp_path: Path, point: str, upload: str) -> None:
    server, client = _start(tmp_path, crash_point=point)
    try:
        with pytest.raises(httpx.TransportError):
            client.put(f"/{BUCKET}/{KEY}?partNumber=1&uploadId={upload}", headers=COPY)
        assert server.wait_for_exit() == -signal.SIGKILL
    finally:
        client.close()


def _store(tmp_path: Path) -> SQLiteMetadataStore:
    return SQLiteMetadataStore(db_path=tmp_path / "meta.db")


def _blobs(tmp_path: Path) -> BlobStore:
    return BlobStore(tmp_path / "blobs")


def _sweep(tmp_path: Path):
    return run_consistency_sweep(_store(tmp_path), _blobs(tmp_path))


def _registry(tmp_path: Path, state: str | None = None):
    return [r for r in _store(tmp_path).list_blob_records() if state in (None, r.state)]


def _blob_files(tmp_path: Path) -> set[str]:
    return {blob_id for _, blob_id in _blobs(tmp_path).scan()}


def _bytes(tmp_path: Path, blob_id: str) -> bytes:
    return _blobs(tmp_path).blob_path(blob_id).read_bytes()


def _assert_source_intact(tmp_path: Path, row) -> None:
    assert _store(tmp_path).get_object_meta(BUCKET, SRC) == row
    assert _bytes(tmp_path, row.blob_id) == SOURCE


def _complete(client, upload: str, etag: str):
    return client.post(f"/{BUCKET}/{KEY}?uploadId={upload}", content=_complete_body(etag))


def _retry_then_complete(tmp_path: Path, upload: str) -> None:
    """After a restart the same copy succeeds and the upload completes with its bytes."""
    with _restarted(tmp_path) as client:
        resp = client.put(f"/{BUCKET}/{KEY}?partNumber=1&uploadId={upload}", headers=COPY)
        assert resp.status_code == 200
        etag = ET.fromstring(resp.text).findtext("ETag")
        assert etag == _md5(RANGED)
        assert _complete(client, upload, etag).status_code == 200
        got = client.get(f"/{BUCKET}/{KEY}")
        assert got.content == RANGED
        assert got.headers["etag"] == f'"{hashlib.md5(hashlib.md5(RANGED).digest()).hexdigest()}-1"'
        assert client.get(f"/{BUCKET}/{SRC}").content == SOURCE
    assert _sweep(tmp_path).issues == []
    assert len(_blob_files(tmp_path)) == 2  # the source and the completed object


# --- before the commit: a new part number ---


def test_part_copy_killed_after_register(tmp_path: Path):
    upload = _setup(tmp_path, existing_part=None)
    src = _store(tmp_path).get_object_meta(BUCKET, SRC)
    _copy_into_crash(tmp_path, PART_COPY_AFTER_REGISTER, upload)

    # (a) A registered ID with no bytes.
    assert _store(tmp_path).get_parts(upload) == []
    (pending,) = _registry(tmp_path, "pending")
    assert (pending.kind, pending.bucket, pending.key) == ("part", BUCKET, KEY)
    assert _blob_files(tmp_path) == {src.blob_id}
    assert _blobs(tmp_path).temp_entries() == []
    _assert_source_intact(tmp_path, src)

    # (b)
    assert _sweep(tmp_path).issues == [
        InterruptedWrite(pending.blob_id, "part", BUCKET, KEY, upload, 1, bytes_durable=False)
    ]
    assert [r.blob_id for r in _registry(tmp_path)] == [src.blob_id]

    # (c) and (d)
    with _restarted(tmp_path) as client:
        refused = _complete(client, upload, _md5(RANGED))
        assert ET.fromstring(refused.text).findtext("Code") == "InvalidPart"
    _retry_then_complete(tmp_path, upload)


def test_part_copy_killed_mid_write(tmp_path: Path):
    upload = _setup(tmp_path, existing_part=None)
    src = _store(tmp_path).get_object_meta(BUCKET, SRC)
    _copy_into_crash(tmp_path, PART_COPY_MID_WRITE, upload)

    # (a) The first chunk of the range in a temp file; no blob, no part.
    (pending,) = _registry(tmp_path, "pending")
    temp = _blobs(tmp_path).temp_path(pending.blob_id)
    assert _blobs(tmp_path).temp_entries() == [temp]
    assert temp.read_bytes() == RANGED[:65536]
    assert _blob_files(tmp_path) == {src.blob_id}
    assert _store(tmp_path).get_parts(upload) == []
    _assert_source_intact(tmp_path, src)

    # (b)
    assert _sweep(tmp_path).issues == [
        LeftoverTemp(temp),
        InterruptedWrite(pending.blob_id, "part", BUCKET, KEY, upload, 1, bytes_durable=False),
    ]
    assert _blobs(tmp_path).temp_entries() == []

    # (c) and (d)
    _retry_then_complete(tmp_path, upload)


def test_part_copy_killed_after_bytes_to_a_new_part(tmp_path: Path):
    upload = _setup(tmp_path, existing_part=None)
    src = _store(tmp_path).get_object_meta(BUCKET, SRC)
    _copy_into_crash(tmp_path, PART_COPY_AFTER_BYTES, upload)

    # (a) The range is durable and complete at its own blob path, and nothing names it.
    (pending,) = _registry(tmp_path, "pending")
    assert _bytes(tmp_path, pending.blob_id) == RANGED
    assert _store(tmp_path).get_parts(upload) == []
    _assert_source_intact(tmp_path, src)

    # (b)
    assert _sweep(tmp_path).issues == [
        InterruptedWrite(pending.blob_id, "part", BUCKET, KEY, upload, 1, bytes_durable=True)
    ]
    assert _blob_files(tmp_path) == {src.blob_id}

    # (c) and (d)
    with _restarted(tmp_path) as client:
        refused = _complete(client, upload, _md5(RANGED))
        assert ET.fromstring(refused.text).findtext("Code") == "InvalidPart"
    _retry_then_complete(tmp_path, upload)


# --- before the commit: replacing a part ---


def test_part_copy_over_a_part_killed_after_bytes_keeps_the_old_part(tmp_path: Path):
    upload = _setup(tmp_path, existing_part=OLD)
    src = _store(tmp_path).get_object_meta(BUCKET, SRC)
    (old,) = _store(tmp_path).get_parts(upload)
    _copy_into_crash(tmp_path, PART_COPY_AFTER_BYTES, upload)

    # (a) Part 1 is still the old one, intact; the copy is a pending, complete blob.
    assert _store(tmp_path).get_parts(upload) == [old]
    assert _bytes(tmp_path, old.blob_id) == OLD
    (pending,) = _registry(tmp_path, "pending")
    assert _bytes(tmp_path, pending.blob_id) == RANGED
    _assert_source_intact(tmp_path, src)

    # (b)
    assert _sweep(tmp_path).issues == [
        InterruptedWrite(pending.blob_id, "part", BUCKET, KEY, upload, 1, bytes_durable=True)
    ]
    assert _blob_files(tmp_path) == {src.blob_id, old.blob_id}

    # (c) Complete with the old ETag gives the old bytes; with the copy's, InvalidPart.
    with _restarted(tmp_path) as client:
        refused = _complete(client, upload, _md5(RANGED))
        assert ET.fromstring(refused.text).findtext("Code") == "InvalidPart"
        assert _complete(client, upload, old.etag).status_code == 200
        assert client.get(f"/{BUCKET}/{KEY}").content == OLD


# --- after the commit ---


def test_part_copy_killed_after_commit_to_a_new_part(tmp_path: Path):
    upload = _setup(tmp_path, existing_part=None)
    src = _store(tmp_path).get_object_meta(BUCKET, SRC)
    _copy_into_crash(tmp_path, PART_COPY_AFTER_COMMIT, upload)

    # (a) The part row points at the copy's live blob.
    (part,) = _store(tmp_path).get_parts(upload)
    assert (part.etag, part.size) == (_md5(RANGED), len(RANGED))
    assert _bytes(tmp_path, part.blob_id) == RANGED
    assert [r.state for r in _registry(tmp_path)].count("live") == 2
    _assert_source_intact(tmp_path, src)

    # (b) Nothing replaced, so nothing to clean.
    assert _sweep(tmp_path).issues == []

    # (c) The part is recorded: Complete with its ETag works.
    with _restarted(tmp_path) as client:
        assert _complete(client, upload, part.etag).status_code == 200
        assert client.get(f"/{BUCKET}/{KEY}").content == RANGED
        assert client.get(f"/{BUCKET}/{SRC}").content == SOURCE
    assert _sweep(tmp_path).issues == []


def test_part_copy_over_a_part_killed_after_commit(tmp_path: Path):
    upload = _setup(tmp_path, existing_part=OLD)
    src = _store(tmp_path).get_object_meta(BUCKET, SRC)
    (old,) = _store(tmp_path).get_parts(upload)
    _copy_into_crash(tmp_path, PART_COPY_AFTER_COMMIT, upload)

    # (a) Part 1 is the copy; the old blob is garbage with its file present.
    (part,) = _store(tmp_path).get_parts(upload)
    assert (part.etag, part.size) == (_md5(RANGED), len(RANGED))
    assert [r.blob_id for r in _registry(tmp_path, "garbage")] == [old.blob_id]
    assert _bytes(tmp_path, old.blob_id) == OLD
    _assert_source_intact(tmp_path, src)

    # (b)
    assert _sweep(tmp_path).issues == [
        InterruptedCleanup(old.blob_id, "part", BUCKET, KEY, upload, 1)
    ]
    assert _blob_files(tmp_path) == {src.blob_id, part.blob_id}

    # (c) The copy is the recorded part; the old ETag is no longer valid.
    with _restarted(tmp_path) as client:
        refused = _complete(client, upload, old.etag)
        assert ET.fromstring(refused.text).findtext("Code") == "InvalidPart"
        assert _complete(client, upload, part.etag).status_code == 200
        assert client.get(f"/{BUCKET}/{KEY}").content == RANGED
