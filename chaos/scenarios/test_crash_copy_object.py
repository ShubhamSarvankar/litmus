"""
Tier B chaos: a CopyObject killed at each of its crash points
(docs/design/storage-layout.md §6.10, §15).

A copy is a PutObject whose bytes come from another object's blob, so it leaves what a
killed PutObject leaves (§6.2): a pending registry row and a partial or complete
unreferenced blob before the commit, the new object and a garbage blob after it. The
source is never touched, whatever the point.
"""

import hashlib
import os
import signal
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
    COPY_AFTER_BYTES,
    COPY_AFTER_COMMIT,
    COPY_AFTER_GC_UNLINK,
    COPY_AFTER_REGISTER,
    COPY_MID_WRITE,
)
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SIGKILL not available on Windows")

BUCKET = "copy-crash"
SRC = "src.bin"
DST = "dst.bin"
SOURCE = os.urandom(200 * 1024)  # more than three 64 KiB chunks
OLD = b"old destination. " * 3000
COPY = {"x-amz-copy-source": f"{BUCKET}/{SRC}"}


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


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


def _setup(tmp_path: Path, destination: bytes | None) -> None:
    """The source, and the destination when given, written by an unarmed server."""
    server, client = _start(tmp_path)
    try:
        assert client.put(f"/{BUCKET}").status_code == 200
        assert client.put(f"/{BUCKET}/{SRC}", content=SOURCE).status_code == 200
        if destination is not None:
            assert client.put(f"/{BUCKET}/{DST}", content=destination).status_code == 200
    finally:
        client.close()
        server.stop()


def _copy_into_crash(tmp_path: Path, point: str, dst: str = DST, **headers) -> None:
    server, client = _start(tmp_path, crash_point=point)
    try:
        with pytest.raises(httpx.TransportError):
            client.put(f"/{BUCKET}/{dst}", headers={**COPY, **headers})
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


def _assert_source_intact(tmp_path: Path, row) -> None:
    assert _store(tmp_path).get_object_meta(BUCKET, SRC) == row
    assert _blobs(tmp_path).blob_path(row.blob_id).read_bytes() == SOURCE


def _assert_object(client, key: str, body: bytes) -> None:
    got = client.get(f"/{BUCKET}/{key}")
    assert got.status_code == 200 and got.content == body and got.headers["etag"] == _md5(body)


def _retry_works(tmp_path: Path) -> None:
    """After a restart the same copy succeeds and leaves one blob per object."""
    with _restarted(tmp_path) as client:
        resp = client.put(f"/{BUCKET}/{DST}", headers=COPY)
        assert resp.status_code == 200
        _assert_object(client, DST, SOURCE)
        _assert_object(client, SRC, SOURCE)
    assert _sweep(tmp_path).issues == []
    assert len(_blob_files(tmp_path)) == 2


# --- before the commit: a new key ---


def test_copy_killed_after_register(tmp_path: Path):
    _setup(tmp_path, destination=None)
    src = _store(tmp_path).get_object_meta(BUCKET, SRC)
    _copy_into_crash(tmp_path, COPY_AFTER_REGISTER)

    # (a) A registered ID with no bytes.
    assert _store(tmp_path).get_object_meta(BUCKET, DST) is None
    (pending,) = _registry(tmp_path, "pending")
    assert (pending.kind, pending.bucket, pending.key) == ("object", BUCKET, DST)
    assert _blob_files(tmp_path) == {src.blob_id}
    assert _blobs(tmp_path).temp_entries() == []
    _assert_source_intact(tmp_path, src)

    # (b)
    assert _sweep(tmp_path).issues == [
        InterruptedWrite(pending.blob_id, "object", BUCKET, DST, None, None, bytes_durable=False)
    ]
    assert [r.blob_id for r in _registry(tmp_path)] == [src.blob_id]

    # (c) and (d)
    with _restarted(tmp_path) as client:
        assert client.get(f"/{BUCKET}/{DST}").status_code == 404
    _retry_works(tmp_path)


def test_copy_killed_mid_write(tmp_path: Path):
    _setup(tmp_path, destination=None)
    src = _store(tmp_path).get_object_meta(BUCKET, SRC)
    _copy_into_crash(tmp_path, COPY_MID_WRITE)

    # (a) The first chunk of the source in a temp file; no blob, no row.
    (pending,) = _registry(tmp_path, "pending")
    temp = _blobs(tmp_path).temp_path(pending.blob_id)
    assert _blobs(tmp_path).temp_entries() == [temp]
    assert temp.read_bytes() == SOURCE[:65536]
    assert _blob_files(tmp_path) == {src.blob_id}
    assert _store(tmp_path).get_object_meta(BUCKET, DST) is None
    _assert_source_intact(tmp_path, src)

    # (b)
    assert _sweep(tmp_path).issues == [
        LeftoverTemp(temp),
        InterruptedWrite(pending.blob_id, "object", BUCKET, DST, None, None, bytes_durable=False),
    ]
    assert _blobs(tmp_path).temp_entries() == []

    # (c) and (d)
    with _restarted(tmp_path) as client:
        assert client.get(f"/{BUCKET}/{DST}").status_code == 404
    _retry_works(tmp_path)


def test_copy_killed_after_bytes_to_a_new_key(tmp_path: Path):
    _setup(tmp_path, destination=None)
    src = _store(tmp_path).get_object_meta(BUCKET, SRC)
    _copy_into_crash(tmp_path, COPY_AFTER_BYTES)

    # (a) The copy is durable and complete at its own blob path, and nothing names it.
    (pending,) = _registry(tmp_path, "pending")
    assert _blobs(tmp_path).blob_path(pending.blob_id).read_bytes() == SOURCE
    assert _store(tmp_path).get_object_meta(BUCKET, DST) is None
    _assert_source_intact(tmp_path, src)

    # (b)
    assert _sweep(tmp_path).issues == [
        InterruptedWrite(pending.blob_id, "object", BUCKET, DST, None, None, bytes_durable=True)
    ]
    assert _blob_files(tmp_path) == {src.blob_id}

    # (c) and (d)
    with _restarted(tmp_path) as client:
        assert client.get(f"/{BUCKET}/{DST}").status_code == 404
    _retry_works(tmp_path)


# --- before the commit: an existing destination (R2) ---


def test_copy_over_an_existing_key_killed_after_bytes_keeps_the_old_object(tmp_path: Path):
    _setup(tmp_path, destination=OLD)
    src = _store(tmp_path).get_object_meta(BUCKET, SRC)
    old = _store(tmp_path).get_object_meta(BUCKET, DST)
    _copy_into_crash(tmp_path, COPY_AFTER_BYTES)

    # (a) The row and the old blob are untouched; the copy's blob is pending and complete.
    assert _store(tmp_path).get_object_meta(BUCKET, DST) == old
    assert _blobs(tmp_path).blob_path(old.blob_id).read_bytes() == OLD
    (pending,) = _registry(tmp_path, "pending")
    assert _blobs(tmp_path).blob_path(pending.blob_id).read_bytes() == SOURCE
    _assert_source_intact(tmp_path, src)

    # (b)
    assert _sweep(tmp_path).issues == [
        InterruptedWrite(pending.blob_id, "object", BUCKET, DST, None, None, bytes_durable=True)
    ]
    assert _blob_files(tmp_path) == {src.blob_id, old.blob_id}

    # (c) Exactly the old object; (d) the retry replaces it.
    with _restarted(tmp_path) as client:
        _assert_object(client, DST, OLD)
        assert client.put(f"/{BUCKET}/{DST}", headers=COPY).status_code == 200
        _assert_object(client, DST, SOURCE)
        _assert_object(client, SRC, SOURCE)
    assert _sweep(tmp_path).issues == []
    assert len(_blob_files(tmp_path)) == 2


def test_copy_over_an_existing_key_killed_mid_write_keeps_the_old_object(tmp_path: Path):
    _setup(tmp_path, destination=OLD)
    old = _store(tmp_path).get_object_meta(BUCKET, DST)
    _copy_into_crash(tmp_path, COPY_MID_WRITE)

    (pending,) = _registry(tmp_path, "pending")
    temp = _blobs(tmp_path).temp_path(pending.blob_id)
    assert temp.read_bytes() == SOURCE[:65536]
    assert _store(tmp_path).get_object_meta(BUCKET, DST) == old

    assert _sweep(tmp_path).issues == [
        LeftoverTemp(temp),
        InterruptedWrite(pending.blob_id, "object", BUCKET, DST, None, None, bytes_durable=False),
    ]
    with _restarted(tmp_path) as client:
        _assert_object(client, DST, OLD)


# --- after the commit ---


def test_copy_to_a_new_key_killed_after_commit_is_complete(tmp_path: Path):
    _setup(tmp_path, destination=None)
    src = _store(tmp_path).get_object_meta(BUCKET, SRC)
    _copy_into_crash(tmp_path, COPY_AFTER_COMMIT)

    # (a) The row names the copy's live blob; there was nothing to replace.
    row = _store(tmp_path).get_object_meta(BUCKET, DST)
    assert (row.etag, row.size) == (_md5(SOURCE), len(SOURCE)) and row.blob_id != src.blob_id
    assert [r.state for r in _registry(tmp_path)] == ["live", "live"]
    _assert_source_intact(tmp_path, src)

    # (b) Nothing to clean up.
    assert _sweep(tmp_path).issues == []

    # (c) The client's retry is a harmless overwrite.
    with _restarted(tmp_path) as client:
        _assert_object(client, DST, SOURCE)
        assert client.put(f"/{BUCKET}/{DST}", headers=COPY).status_code == 200
        _assert_object(client, DST, SOURCE)
    assert _sweep(tmp_path).issues == []
    assert len(_blob_files(tmp_path)) == 2


def test_copy_over_an_existing_key_killed_after_commit(tmp_path: Path):
    _setup(tmp_path, destination=OLD)
    src = _store(tmp_path).get_object_meta(BUCKET, SRC)
    old = _store(tmp_path).get_object_meta(BUCKET, DST)
    _copy_into_crash(tmp_path, COPY_AFTER_COMMIT)

    # (a) The row names the new live blob; the old blob is garbage, its file present.
    new = _store(tmp_path).get_object_meta(BUCKET, DST)
    assert (new.etag, new.size) == (_md5(SOURCE), len(SOURCE)) and new.blob_id != old.blob_id
    assert [r.blob_id for r in _registry(tmp_path, "garbage")] == [old.blob_id]
    assert _blobs(tmp_path).blob_path(old.blob_id).read_bytes() == OLD
    assert _blobs(tmp_path).blob_path(new.blob_id).read_bytes() == SOURCE
    _assert_source_intact(tmp_path, src)

    # (b)
    assert _sweep(tmp_path).issues == [
        InterruptedCleanup(old.blob_id, "object", BUCKET, DST, None, None)
    ]
    assert _blob_files(tmp_path) == {src.blob_id, new.blob_id}

    with _restarted(tmp_path) as client:
        _assert_object(client, DST, SOURCE)
        assert client.put(f"/{BUCKET}/{DST}", headers=COPY).status_code == 200
        _assert_object(client, DST, SOURCE)
    assert _sweep(tmp_path).issues == []


def test_copy_over_an_existing_key_killed_after_gc_unlink(tmp_path: Path):
    _setup(tmp_path, destination=OLD)
    src = _store(tmp_path).get_object_meta(BUCKET, SRC)
    old = _store(tmp_path).get_object_meta(BUCKET, DST)
    _copy_into_crash(tmp_path, COPY_AFTER_GC_UNLINK)

    # (a) The old blob's file is unlinked; its garbage row is still there.
    new = _store(tmp_path).get_object_meta(BUCKET, DST)
    assert [r.blob_id for r in _registry(tmp_path, "garbage")] == [old.blob_id]
    assert _blob_files(tmp_path) == {src.blob_id, new.blob_id}

    # (b) A garbage row without a file is purged silently.
    assert _sweep(tmp_path).clean
    assert sorted(r.blob_id for r in _registry(tmp_path)) == sorted([src.blob_id, new.blob_id])

    with _restarted(tmp_path) as client:
        _assert_object(client, DST, SOURCE)
        _assert_object(client, SRC, SOURCE)


def test_copy_onto_itself_with_replace_killed_after_commit(tmp_path: Path):
    _setup(tmp_path, destination=None)
    src = _store(tmp_path).get_object_meta(BUCKET, SRC)
    _copy_into_crash(
        tmp_path,
        COPY_AFTER_COMMIT,
        dst=SRC,
        **{"x-amz-metadata-directive": "REPLACE", "x-amz-meta-new": "yes"},
    )

    # (a) The same bytes under a new blob; the source's old blob is garbage.
    row = _store(tmp_path).get_object_meta(BUCKET, SRC)
    assert row.blob_id != src.blob_id and row.etag == _md5(SOURCE)
    assert row.user_metadata == {"new": "yes"}
    assert [r.blob_id for r in _registry(tmp_path, "garbage")] == [src.blob_id]

    # (b)
    assert _sweep(tmp_path).issues == [
        InterruptedCleanup(src.blob_id, "object", BUCKET, SRC, None, None)
    ]
    with _restarted(tmp_path) as client:
        got = client.get(f"/{BUCKET}/{SRC}")
        assert got.content == SOURCE and got.headers["x-amz-meta-new"] == "yes"
    assert _sweep(tmp_path).issues == []
    assert len(_blob_files(tmp_path)) == 1
