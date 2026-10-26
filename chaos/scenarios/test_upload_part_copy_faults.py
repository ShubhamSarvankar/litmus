"""
Tier A chaos: faults injected into UploadPartCopy (docs/design/storage-layout.md §6.6, §11).

A part copy that fails before its commit records no part, leaves an existing part of the
same number as it was (still recorded, its bytes intact), and leaves nothing behind: no
temp file, no pending registry row, no blob file the sweep would have to remove. A source
that reads back differently from its row (a different size) is never copied under that
row, and corrupt bytes of a source copied whole are caught by the MD5 of the single part
source; a range has no digest to check, so only its length is.
"""

import errno
import hashlib
import os
import xml.etree.ElementTree as ET
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

BUCKET = "part-copy-faults"
SRC = "src"
DST = "dst"
SIZE = 200_000  # more than three 64 KiB chunks
DATA = os.urandom(SIZE)
OLD = b"old part " * 50
SOURCE = {"x-amz-copy-source": f"{BUCKET}/{SRC}"}


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def _setup(client) -> str:
    assert client.put(f"/{BUCKET}").status_code == 200
    assert client.put(f"/{BUCKET}/{SRC}", content=DATA).status_code == 200
    return ET.fromstring(client.post(f"/{BUCKET}/{DST}?uploads").text).findtext("UploadId")


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    return client, storage, metadata, _setup(client)


def _faulty(tmp_path: Path, method: str, errno_code: int):
    injector = FaultInjector(
        method=method, trigger=CallCountTrigger(fire_on=1), fault=RaiseOSError(errno_code)
    )
    client, _, metadata, storage = make_faulty_server(tmp_path, injector)
    # The fault counts the first call: the setup makes none to that method.
    return client, storage, metadata, _setup(client)


def _url(upload: str, n: int = 1) -> str:
    return f"/{BUCKET}/{DST}?partNumber={n}&uploadId={upload}"


def _existing(client, upload) -> None:
    resp = client.put(_url(upload), content=OLD)
    assert resp.status_code == 200


def _registry(metadata) -> list[tuple[str, str]]:
    return sorted((r.blob_id, r.state) for r in metadata.list_blob_records())


def _assert_nothing_left(storage, metadata, registry_before) -> None:
    """No temp file, no new registry row (pending or otherwise), no new blob file."""
    assert storage.temp_entries() == []
    assert _registry(metadata) == registry_before
    live = {blob_id for blob_id, _ in registry_before}
    on_disk = {entry.name for entry, blob_id in storage.scan() if blob_id}
    assert on_disk == live


def _part_bytes(storage, metadata, upload, n=1) -> bytes | None:
    for part in metadata.get_parts(upload):
        if part.part_number == n:
            with storage.open_blob(part.blob_id) as f:
                return f.read()
    return None


def _assert_part(storage, metadata, upload, existing: bool) -> None:
    assert _part_bytes(storage, metadata, upload) == (OLD if existing else None)
    assert run_consistency_sweep(metadata, storage).issues == []


def _blob_path(storage, metadata) -> Path:
    return storage.blob_path(metadata.get_object_meta(BUCKET, SRC).blob_id)


def _retry_works(client, storage, metadata, upload) -> None:
    resp = client.put(_url(upload), headers=SOURCE)
    assert resp.status_code == 200
    assert _part_bytes(storage, metadata, upload) == DATA


# --- OSError on the source ---


@pytest.mark.parametrize("existing", [False, True], ids=["new-part", "existing-part"])
def test_oserror_opening_the_source_is_500_and_changes_nothing(tmp_path: Path, existing):
    # The proxy fires on the first open_blob call, which is the copy's: UploadPart opens none.
    client, storage, metadata, upload = _faulty(tmp_path, "open_blob", errno.EIO)
    if existing:
        _existing(client, upload)
    before = _registry(metadata)

    resp = client.put(_url(upload), headers=SOURCE)

    assert resp.status_code == 500
    _assert_nothing_left(storage, metadata, before)
    _assert_part(storage, metadata, upload, existing)
    _retry_works(client, storage, metadata, upload)


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

    def seek(self, offset, whence=os.SEEK_SET):
        return self._real.seek(offset, whence)

    def read(self, n=-1):
        self._reads += 1
        if self._reads == 2:
            raise OSError(self._errno, os.strerror(self._errno))
        return self._real.read(n)


@pytest.mark.parametrize("existing", [False, True], ids=["new-part", "existing-part"])
def test_oserror_reading_the_source_mid_copy_is_500_and_leaves_nothing(env, monkeypatch, existing):
    client, storage, metadata, upload = env
    if existing:
        _existing(client, upload)
    before = _registry(metadata)
    real = storage.open_blob
    monkeypatch.setattr(
        storage, "open_blob", lambda blob_id: _FailingReader(real(blob_id), errno.EIO)
    )

    resp = client.put(_url(upload), headers=SOURCE)

    assert resp.status_code == 500
    monkeypatch.undo()
    _assert_nothing_left(storage, metadata, before)
    _assert_part(storage, metadata, upload, existing)
    assert client.get(f"/{BUCKET}/{SRC}").content == DATA
    _retry_works(client, storage, metadata, upload)


@pytest.mark.parametrize("where", ["first", "middle", "last"])
def test_corrupt_source_bytes_copied_whole_are_never_recorded(env, where):
    client, storage, metadata, upload = env
    _existing(client, upload)
    path = _blob_path(storage, metadata)
    offset = {"first": 0, "middle": SIZE // 2, "last": SIZE - 1}[where]
    raw = bytearray(path.read_bytes())
    raw[offset] ^= 0xFF
    path.write_bytes(bytes(raw))  # same size, so only the MD5 check can see it
    before = _registry(metadata)

    resp = client.put(_url(upload), headers=SOURCE)

    assert resp.status_code == 500
    _assert_nothing_left(storage, metadata, before)
    _assert_part(storage, metadata, upload, existing=True)


def test_a_range_has_no_digest_so_corruption_outside_it_is_not_noticed(env):
    client, storage, metadata, upload = env
    path = _blob_path(storage, metadata)
    raw = bytearray(path.read_bytes())
    raw[SIZE - 1] ^= 0xFF
    path.write_bytes(bytes(raw))

    resp = client.put(_url(upload), headers={**SOURCE, "x-amz-copy-source-range": "bytes=0-99999"})

    assert resp.status_code == 200
    assert _part_bytes(storage, metadata, upload) == DATA[:100000]


@pytest.mark.parametrize("change", ["truncated", "padded"])
@pytest.mark.parametrize("ranged", [False, True], ids=["whole", "range"])
def test_source_of_the_wrong_size_is_never_copied(env, change, ranged):
    client, storage, metadata, upload = env
    path = _blob_path(storage, metadata)
    with open(path, "r+b") as f:
        if change == "truncated":
            f.truncate(SIZE - 7)
        else:
            f.seek(0, os.SEEK_END)
            f.write(b"extra")
    before = _registry(metadata)
    headers = {**SOURCE, **({"x-amz-copy-source-range": "bytes=0-9"} if ranged else {})}

    resp = client.put(_url(upload), headers=headers)

    assert resp.status_code == 500
    # Refused before anything was registered.
    _assert_nothing_left(storage, metadata, before)
    assert metadata.get_parts(upload) == []


# --- OSError on the part ---


def test_oserror_opening_the_part_writer_is_500_and_leaves_nothing(tmp_path):
    client, storage, metadata, upload = _faulty(tmp_path, "open_writer", errno.EIO)
    before = _registry(metadata)

    resp = client.put(_url(upload), headers=SOURCE)

    assert resp.status_code == 500
    _assert_nothing_left(storage, metadata, before)
    assert metadata.get_parts(upload) == []
    _retry_works(client, storage, metadata, upload)


@pytest.mark.parametrize("existing", [False, True], ids=["new-part", "existing-part"])
@pytest.mark.parametrize("code", [errno.ENOSPC, errno.EIO])
def test_oserror_writing_the_part_mid_copy_is_500_and_leaves_nothing(
    env, monkeypatch, existing, code
):
    client, storage, metadata, upload = env
    if existing:
        _existing(client, upload)
    before = _registry(metadata)
    written = []
    real_write = BlobWriter.write

    def write(self, data):
        if written:
            raise OSError(code, os.strerror(code))
        written.append(len(data))
        real_write(self, data)

    monkeypatch.setattr(BlobWriter, "write", write)

    resp = client.put(_url(upload), headers=SOURCE)

    assert resp.status_code == 500 and written == [65536]
    monkeypatch.undo()
    # The temp file existed and held the first chunk; it is gone, and so is the row.
    _assert_nothing_left(storage, metadata, before)
    _assert_part(storage, metadata, upload, existing)
    _retry_works(client, storage, metadata, upload)


def test_oserror_on_the_part_fsync_is_500_and_leaves_nothing(env, monkeypatch):
    client, storage, metadata, upload = env
    _existing(client, upload)
    before = _registry(metadata)

    def fsync(fd):
        raise OSError(errno.EIO, "fsync")

    monkeypatch.setattr("server.storage.blobs.os.fsync", fsync)
    resp = client.put(_url(upload), headers=SOURCE)
    monkeypatch.undo()

    assert resp.status_code == 500
    _assert_nothing_left(storage, metadata, before)
    _assert_part(storage, metadata, upload, existing=True)


@pytest.mark.parametrize("existing", [False, True], ids=["new-part", "existing-part"])
def test_a_failed_part_commit_discards_the_new_blob(env, monkeypatch, existing):
    client, storage, metadata, upload = env
    if existing:
        _existing(client, upload)
    before = _registry(metadata)

    def record_part(upload_id, part):
        raise OSError(errno.EIO, "database")

    monkeypatch.setattr(metadata, "record_part", record_part)
    resp = client.put(_url(upload), headers=SOURCE)
    monkeypatch.undo()

    assert resp.status_code == 500
    _assert_nothing_left(storage, metadata, before)
    _assert_part(storage, metadata, upload, existing)
    _retry_works(client, storage, metadata, upload)
