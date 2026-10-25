"""BlobStore.open_writer: the incremental form of the durable write sequence
(docs/design/storage-layout.md §6.1, WRITE_BLOB). write_blob is built on it, so these
tests cover the one implementation of the temp file, fsync, rename and directory fsync
steps from the side of a caller that writes in pieces and can refuse at the end."""

import hashlib
import io
import os
import stat
from datetime import datetime
from pathlib import Path

import pytest

from server.metadata.sqlite import SQLiteMetadataStore
from server.storage import blobs
from server.storage.blobs import BlobStore, new_blob_id
from server.storage.lifecycle import discard_new_blob


@pytest.fixture
def store(tmp_path: Path) -> BlobStore:
    s = BlobStore(tmp_path / "blobs")
    s.create_tree()
    return s


def _files(root: Path) -> set[Path]:
    return {p for p in root.rglob("*") if p.is_file()}


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def _read(store: BlobStore, blob_id: str) -> bytes:
    with store.open_blob(blob_id) as f:
        return f.read()


@pytest.fixture
def calls(monkeypatch):
    """Record fsync (by what it was called on) and rename, in order."""
    log = []
    real_fsync, real_rename = os.fsync, os.rename

    def fsync(fd):
        log.append("fsync-dir" if stat.S_ISDIR(os.fstat(fd).st_mode) else "fsync-file")
        return real_fsync(fd)

    def rename(src, dst):
        log.append("rename")
        return real_rename(src, dst)

    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(os, "rename", rename)
    return log


class _Boom(Exception):
    pass


def test_several_writes_store_the_same_bytes_and_etag_as_one(store):
    data = bytes(range(256)) * 1000
    one, many = new_blob_id(), new_blob_id()
    with store.open_writer(one) as w:
        w.write(data)
    with store.open_writer(many) as w:
        for start in range(0, len(data), 1000):
            w.write(data[start : start + 1000])
    assert (w.etag, w.size) == (_md5(data), len(data))
    assert _read(store, many) == _read(store, one) == data


def test_writer_result_matches_write_blob(store):
    data = b"hello world, " * 5000
    via_writer, via_write_blob = new_blob_id(), new_blob_id()
    with store.open_writer(via_writer) as w:
        w.write(data[:7])
        w.write(data[7:])
    assert store.write_blob(via_write_blob, io.BytesIO(data)) == (w.etag, w.size)


def test_no_writes_and_empty_writes_store_an_empty_blob(store):
    blob_id = new_blob_id()
    with store.open_writer(blob_id) as w:
        w.write(b"")
    assert (w.etag, w.size) == (_md5(b""), 0)
    assert _read(store, blob_id) == b""


def test_durable_sequence_is_the_one_write_blob_uses(store, calls):
    with store.open_writer(new_blob_id()) as w:
        w.write(b"a")
        w.write(b"b")
    assert calls == ["fsync-file", "rename", "fsync-dir"]


def test_verify_runs_after_the_last_write_and_before_any_fsync(store, calls):
    seen = []

    def verify(writer):
        seen.append((writer.size, writer.etag, list(calls)))

    with store.open_writer(new_blob_id(), verify=verify) as w:
        w.write(b"ab")
        w.write(b"cde")
    assert seen == [(5, _md5(b"abcde"), [])]
    assert calls == ["fsync-file", "rename", "fsync-dir"]


def test_verify_that_raises_leaves_no_temp_no_blob_and_no_fsync(store, calls):
    def verify(writer):
        raise _Boom("checksum mismatch")

    blob_id = new_blob_id()
    with pytest.raises(_Boom, match="checksum mismatch"):
        with store.open_writer(blob_id, verify=verify) as w:
            w.write(b"some bytes")
    assert not _files(store.root)
    assert not store.has_files(blob_id)
    assert calls == []


def test_exception_between_writes_leaves_no_file_and_skips_verify(store, calls):
    verified = []
    blob_id = new_blob_id()
    with pytest.raises(_Boom):
        with store.open_writer(blob_id, verify=verified.append) as w:
            w.write(b"first")
            raise _Boom("client went away")
    assert verified == []
    assert not _files(store.root)
    assert not store.has_files(blob_id)
    assert calls == []


@pytest.mark.parametrize("where", ["verify", "between-writes"])
def test_failure_leaves_no_registry_row_once_the_handler_discards(tmp_path, where):
    storage = BlobStore(tmp_path / "blobs")
    storage.create_tree()
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    metadata.create_bucket("b", datetime(2024, 1, 1))

    def refuse(writer):
        raise _Boom("refused")

    blob_id = metadata.register_blob("object", "b", "k")
    assert [r.state for r in metadata.list_blob_records()] == ["pending"]
    try:
        with storage.open_writer(blob_id, verify=refuse if where == "verify" else None) as w:
            w.write(b"abc")
            if where == "between-writes":
                raise _Boom("refused")
    except _Boom:
        discard_new_blob(storage, metadata, blob_id)
    assert metadata.list_blob_records() == []
    assert not _files(storage.root)


def test_failure_after_rename_removes_the_blob(store, monkeypatch):
    def broken(directory):
        raise OSError("fsync failed")

    monkeypatch.setattr(blobs, "_fsync_dir", broken)
    with pytest.raises(OSError, match="fsync failed"):
        with store.open_writer(new_blob_id()) as w:
            w.write(b"x")
    assert not _files(store.root)


def test_existing_temp_file_is_refused_and_left(store):
    blob_id = new_blob_id()
    temp = store.temp_path(blob_id)
    temp.write_bytes(b"stray")
    with pytest.raises(FileExistsError):
        with store.open_writer(blob_id):
            raise AssertionError("body must not run")
    assert temp.read_bytes() == b"stray"


def test_mid_write_point_fires_after_the_first_write_and_before_the_second(store, monkeypatch):
    blob_id = new_blob_id()
    seen = []
    monkeypatch.setattr(
        blobs,
        "crash_point",
        lambda name: seen.append((name, store.temp_path(blob_id).stat().st_size)),
    )
    with store.open_writer(blob_id, mid_write_point="put-mid-write") as w:
        w.write(b"a" * 100)
        assert seen == [("put-mid-write", 100)]
        w.write(b"b" * 50)
        w.write(b"c" * 50)
    assert seen == [("put-mid-write", 100)]
    assert w.size == 200


def test_mid_write_point_does_not_fire_for_empty_writes_or_by_default(store, monkeypatch):
    seen = []
    monkeypatch.setattr(blobs, "crash_point", seen.append)
    with store.open_writer(new_blob_id(), mid_write_point="put-mid-write") as w:
        w.write(b"")
    with store.open_writer(new_blob_id()) as w:
        w.write(b"x")
        w.write(b"y")
    assert seen == []


def test_write_after_the_block_is_refused(store):
    with store.open_writer(new_blob_id()) as w:
        w.write(b"x")
    with pytest.raises(ValueError):
        w.write(b"y")


# --- write_blob forwards verify to open_writer ---


def test_write_blob_verify_runs_once_after_all_bytes_and_before_any_fsync(store, calls):
    seen = []

    def verify(writer):
        seen.append((writer.size, writer.etag, writer.md5_digest, list(calls)))

    blob_id = new_blob_id()
    etag, size = store.write_blob(blob_id, io.BytesIO(b"abcde" * 40000), verify=verify)
    data = b"abcde" * 40000  # more than one read of the source
    assert seen == [(len(data), _md5(data), hashlib.md5(data).digest(), [])]
    assert (etag, size) == (_md5(data), len(data))
    assert _read(store, blob_id) == data
    assert calls == ["fsync-file", "rename", "fsync-dir"]


def test_write_blob_verify_that_raises_leaves_nothing_and_does_no_fsync(store, calls):
    def verify(writer):
        raise _Boom("checksum mismatch")

    blob_id = new_blob_id()
    with pytest.raises(_Boom, match="checksum mismatch"):
        store.write_blob(blob_id, io.BytesIO(b"some bytes"), verify=verify)
    assert not _files(store.root)
    assert not store.has_files(blob_id)
    assert calls == []


def test_write_blob_without_verify_is_unchanged(store, calls):
    blob_id = new_blob_id()
    assert store.write_blob(blob_id, io.BytesIO(b"x")) == (_md5(b"x"), 1)
    assert calls == ["fsync-file", "rename", "fsync-dir"]
