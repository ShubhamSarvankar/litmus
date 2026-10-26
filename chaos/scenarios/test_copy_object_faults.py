"""
Tier A chaos: faults injected into CopyObject (docs/design/storage-layout.md §6.10, §11).

A copy that fails before its commit creates no destination row, leaves an existing
destination as it was, and leaves nothing behind: no temp file, no pending registry row,
no blob file the sweep would have to remove. A source that reads back differently from
its row (a different size, other bytes) is never copied under that row's ETag: 500.
"""

import errno
import hashlib
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from chaos.faults.faults import RaiseOSError
from chaos.fixtures.inprocess import make_faulty_server
from chaos.framework.injector import FaultInjector
from chaos.framework.triggers import CallCountTrigger
from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore, BlobWriter

BUCKET = "copy-faults"
SRC = "src"
DST = "dst"
SIZE = 200_000  # more than three 64 KiB chunks
DATA = os.urandom(SIZE)
OLD = b"old destination " * 50
SOURCE = {"x-amz-copy-source": f"{BUCKET}/{SRC}"}


def _etag(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    assert client.put(f"/{BUCKET}").status_code == 200
    assert client.put(f"/{BUCKET}/{SRC}", content=DATA).status_code == 200
    return client, storage, metadata


def _registry(metadata) -> list[tuple[str, str]]:
    return sorted((r.blob_id, r.state) for r in metadata.list_blob_records())


def _assert_nothing_left(storage, metadata, registry_before) -> None:
    """No temp file, no new registry row (pending or otherwise), no new blob file."""
    assert storage.temp_entries() == []
    assert _registry(metadata) == registry_before
    live = {blob_id for blob_id, _ in registry_before}
    on_disk = {entry.name for entry, blob_id in storage.scan() if blob_id}
    assert on_disk == live


def _blob_path(storage, metadata, key: str) -> Path:
    return storage.blob_path(metadata.get_object_meta(BUCKET, key).blob_id)


def _faulty(tmp_path: Path, method: str, errno_code: int):
    injector = FaultInjector(
        method=method, trigger=CallCountTrigger(fire_on=1), fault=RaiseOSError(errno_code)
    )
    client, faulty_storage, metadata, storage = make_faulty_server(tmp_path, injector)
    # The fault counts the first call: the setup below makes none to that method.
    assert client.put(f"/{BUCKET}").status_code == 200
    assert client.put(f"/{BUCKET}/{SRC}", content=DATA).status_code == 200
    return client, storage, metadata


# --- OSError on the source ---


@pytest.mark.parametrize("existing", [False, True], ids=["new-key", "existing-key"])
def test_oserror_opening_the_source_is_500_and_changes_nothing(tmp_path: Path, existing):
    # The proxy fires on the first open_blob call, which is the copy's: PutObject opens none.
    client, storage, metadata = _faulty(tmp_path, "open_blob", errno.EIO)
    if existing:
        assert client.put(f"/{BUCKET}/{DST}", content=OLD).status_code == 200
    before = _registry(metadata)

    resp = client.put(f"/{BUCKET}/{DST}", headers=SOURCE)

    assert resp.status_code == 500
    _assert_nothing_left(storage, metadata, before)
    got = client.get(f"/{BUCKET}/{DST}")
    assert (got.status_code, got.content) == ((200, OLD) if existing else (404, got.content))
    assert run_consistency_sweep(metadata, storage).issues == []


class _FailingReader:
    """An open source blob whose second read fails."""

    def __init__(self, real, errno_code: int):
        self._real, self._errno, self._reads = real, errno_code, 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._real.close()

    def fileno(self):
        return self._real.fileno()

    def close(self):
        self._real.close()

    def read(self, n=-1):
        self._reads += 1
        if self._reads == 2:
            raise OSError(self._errno, os.strerror(self._errno))
        return self._real.read(n)


@pytest.mark.parametrize("existing", [False, True], ids=["new-key", "existing-key"])
def test_oserror_reading_the_source_mid_copy_is_500_and_leaves_nothing(env, monkeypatch, existing):
    client, storage, metadata = env
    if existing:
        assert client.put(f"/{BUCKET}/{DST}", content=OLD).status_code == 200
    before = _registry(metadata)
    real = storage.open_blob
    monkeypatch.setattr(
        storage, "open_blob", lambda blob_id: _FailingReader(real(blob_id), errno.EIO)
    )

    resp = client.put(f"/{BUCKET}/{DST}", headers=SOURCE)

    assert resp.status_code == 500
    monkeypatch.undo()
    _assert_nothing_left(storage, metadata, before)
    assert client.get(f"/{BUCKET}/{SRC}").content == DATA
    assert client.get(f"/{BUCKET}/{DST}").content == OLD if existing else True
    assert run_consistency_sweep(metadata, storage).issues == []


@pytest.mark.parametrize("where", ["first", "middle", "last"])
def test_corrupt_source_bytes_are_never_copied(env, where):
    client, storage, metadata = env
    path = _blob_path(storage, metadata, SRC)
    offset = {"first": 0, "middle": SIZE // 2, "last": SIZE - 1}[where]
    raw = bytearray(path.read_bytes())
    raw[offset] ^= 0xFF
    path.write_bytes(bytes(raw))  # same size, so only the MD5 check can see it
    assert client.put(f"/{BUCKET}/{DST}", content=OLD).status_code == 200
    before = _registry(metadata)

    resp = client.put(f"/{BUCKET}/{DST}", headers=SOURCE)

    assert resp.status_code == 500
    _assert_nothing_left(storage, metadata, before)
    assert client.get(f"/{BUCKET}/{DST}").content == OLD
    # The copy did not make the corruption worse or hide it: GET of the source still
    # answers its row's ETag.
    assert metadata.get_object_meta(BUCKET, SRC).etag == _etag(DATA)


@pytest.mark.parametrize("change", ["truncated", "padded"])
def test_source_of_the_wrong_size_is_never_copied(env, change):
    client, storage, metadata = env
    path = _blob_path(storage, metadata, SRC)
    with open(path, "r+b") as f:
        if change == "truncated":
            f.truncate(SIZE - 7)
        else:
            f.seek(0, os.SEEK_END)
            f.write(b"extra")
    before = _registry(metadata)

    resp = client.put(f"/{BUCKET}/{DST}", headers=SOURCE)

    assert resp.status_code == 500
    # Refused before anything was registered.
    _assert_nothing_left(storage, metadata, before)
    assert client.get(f"/{BUCKET}/{DST}").status_code == 404


# --- OSError on the destination ---


@pytest.mark.parametrize("existing", [False, True], ids=["new-key", "existing-key"])
def test_oserror_opening_the_destination_writer_is_500_and_leaves_nothing(tmp_path, existing):
    client, storage, metadata = _faulty(tmp_path, "open_writer", errno.EIO)
    if existing:
        assert client.put(f"/{BUCKET}/{DST}", content=OLD).status_code == 200
    before = _registry(metadata)

    resp = client.put(f"/{BUCKET}/{DST}", headers=SOURCE)

    assert resp.status_code == 500
    _assert_nothing_left(storage, metadata, before)
    got = client.get(f"/{BUCKET}/{DST}")
    assert (got.status_code, got.content) == ((200, OLD) if existing else (404, got.content))
    assert run_consistency_sweep(metadata, storage).issues == []


@pytest.mark.parametrize("existing", [False, True], ids=["new-key", "existing-key"])
@pytest.mark.parametrize("code", [errno.ENOSPC, errno.EIO])
def test_oserror_writing_the_destination_mid_copy_is_500_and_leaves_nothing(
    env, monkeypatch, existing, code
):
    client, storage, metadata = env
    if existing:
        assert client.put(f"/{BUCKET}/{DST}", content=OLD).status_code == 200
    before = _registry(metadata)
    written = []
    real_write = BlobWriter.write

    def write(self, data):
        if written:
            raise OSError(code, os.strerror(code))
        written.append(len(data))
        real_write(self, data)

    monkeypatch.setattr(BlobWriter, "write", write)

    resp = client.put(f"/{BUCKET}/{DST}", headers=SOURCE)

    assert resp.status_code == 500 and written == [65536]
    monkeypatch.undo()
    # The temp file existed and held the first chunk; it is gone, and so is the row.
    _assert_nothing_left(storage, metadata, before)
    got = client.get(f"/{BUCKET}/{DST}")
    assert (got.status_code, got.content) == ((200, OLD) if existing else (404, got.content))
    assert run_consistency_sweep(metadata, storage).issues == []
    assert client.put(f"/{BUCKET}/{DST}", headers=SOURCE).status_code == 200
    assert client.get(f"/{BUCKET}/{DST}").content == DATA


def test_oserror_on_the_destination_fsync_is_500_and_leaves_nothing(env, monkeypatch):
    client, storage, metadata = env
    before = _registry(metadata)

    def fsync(fd):
        raise OSError(errno.EIO, "fsync")

    monkeypatch.setattr("server.storage.blobs.os.fsync", fsync)
    resp = client.put(f"/{BUCKET}/{DST}", headers=SOURCE)
    monkeypatch.undo()

    assert resp.status_code == 500
    _assert_nothing_left(storage, metadata, before)
    assert client.get(f"/{BUCKET}/{DST}").status_code == 404
    assert run_consistency_sweep(metadata, storage).issues == []
