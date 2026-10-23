"""The blob file layer of the new storage layout (docs/design/storage-layout.md §4,
§6.1): IDs, the directory tree, the durable write sequence, assembly, and what the
startup sweep can see. Not wired into the server yet."""

import hashlib
import io
import os
import stat
from pathlib import Path

import pytest

from server.storage import blobs
from server.storage.blobs import (
    FANOUT,
    BlobStore,
    InvalidBlobId,
    MissingPartBlob,
    is_blob_id,
    new_blob_id,
)

posix_only = pytest.mark.skipif(os.name == "nt", reason="POSIX modes and directory fsync")


@pytest.fixture
def store(tmp_path: Path) -> BlobStore:
    s = BlobStore(tmp_path / "blobs")
    s.create_tree()
    return s


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def _files(root: Path) -> set[Path]:
    return {p for p in root.rglob("*") if p.is_file()}


@pytest.fixture
def calls(monkeypatch):
    """Record fsync (by the inode it was called on) and rename, in order."""
    log = []
    real_fsync, real_rename = os.fsync, os.rename

    def fsync(fd):
        st = os.fstat(fd)
        log.append(("fsync-dir" if stat.S_ISDIR(st.st_mode) else "fsync-file", st.st_ino))
        return real_fsync(fd)

    def rename(src, dst):
        log.append(("rename", Path(src), Path(dst)))
        return real_rename(src, dst)

    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(os, "rename", rename)
    return log


# --- IDs and paths ---


def test_new_ids_are_32_lowercase_hex_and_distinct():
    ids = {new_blob_id() for _ in range(1000)}
    assert len(ids) == 1000
    assert all(is_blob_id(i) for i in ids)


@pytest.mark.parametrize(
    "bad",
    ["", "0" * 31, "0" * 33, "A" * 32, "g" * 32, "../" + "0" * 29, "0" * 30 + "/x", ".."],
)
def test_malformed_ids_are_refused_and_write_nothing(store, bad):
    with pytest.raises(InvalidBlobId):
        store.blob_path(bad)
    with pytest.raises(InvalidBlobId):
        store.write_blob(bad, io.BytesIO(b"x"))
    with pytest.raises(InvalidBlobId):
        store.open_blob(bad)
    with pytest.raises(InvalidBlobId):
        store.remove_blob(bad)
    assert not _files(store.root)


def test_blob_path_is_fanout_dir_then_id(store):
    blob_id = new_blob_id()
    assert store.blob_path(blob_id) == store.root / blob_id[:2] / blob_id
    assert store.temp_path(blob_id) == store.root / "tmp" / f"{blob_id}.tmp"


# --- directory tree ---


def test_create_tree_makes_fanout_and_tmp(tmp_path):
    s = BlobStore(tmp_path / "blobs")
    s.create_tree()
    assert sorted(p.name for p in s.root.iterdir()) == sorted([*FANOUT, "tmp"])
    assert len(FANOUT) == 256


@posix_only
def test_create_tree_directories_are_private(store):
    for name in ("00", "ff", "tmp"):
        assert stat.S_IMODE((store.root / name).stat().st_mode) & 0o077 == 0


def test_create_tree_is_idempotent_and_completes_a_partial_tree(store):
    (store.root / "7f").rmdir()
    (store.root / "tmp").rmdir()
    store.create_tree()
    store.create_tree()
    assert (store.root / "7f").is_dir() and (store.root / "tmp").is_dir()


@posix_only
def test_create_tree_fsyncs_root_parent_and_root(tmp_path, calls):
    s = BlobStore(tmp_path / "blobs")
    s.create_tree()
    dirs = [ino for kind, *rest in calls if kind == "fsync-dir" for ino in rest]
    assert dirs == [tmp_path.stat().st_ino, s.root.stat().st_ino]


@posix_only
def test_create_tree_creates_each_missing_level_and_fsyncs_its_parent(tmp_path, calls):
    """blobs/ two levels below the nearest existing directory: each level is created
    on its own and made durable in its parent, top down, then blobs/ once for the
    fanout directories."""
    s = BlobStore(tmp_path / "a" / "b")
    s.create_tree()
    dirs = [ino for kind, *rest in calls if kind == "fsync-dir" for ino in rest]
    assert dirs == [
        tmp_path.stat().st_ino,  # after creating a/
        (tmp_path / "a").stat().st_ino,  # after creating a/b/ (the blob root)
        s.root.stat().st_ino,  # after creating the fanout directories and tmp/
    ]
    assert (s.root / "00").is_dir()


@posix_only
def test_create_tree_on_complete_tree_fsyncs_nothing(store, calls):
    store.create_tree()
    assert calls == []


# --- write_blob ---


def test_write_blob_stores_bytes_and_returns_md5_and_size(store):
    data = os.urandom(200_000)
    blob_id = new_blob_id()
    assert store.write_blob(blob_id, io.BytesIO(data)) == (_md5(data), len(data))
    assert store.blob_path(blob_id).read_bytes() == data
    assert store.temp_entries() == []


def test_write_blob_empty_body(store):
    blob_id = new_blob_id()
    assert store.write_blob(blob_id, io.BytesIO(b"")) == (_md5(b""), 0)
    assert store.blob_path(blob_id).read_bytes() == b""


@posix_only
def test_write_blob_file_is_private(store):
    blob_id = new_blob_id()
    store.write_blob(blob_id, io.BytesIO(b"x"))
    assert stat.S_IMODE(store.blob_path(blob_id).stat().st_mode) == 0o600


@posix_only
def test_write_blob_order_is_fsync_file_rename_fsync_fanout(store, calls):
    blob_id = new_blob_id()
    store.write_blob(blob_id, io.BytesIO(b"payload"))
    final = store.blob_path(blob_id)
    assert calls == [
        ("fsync-file", final.stat().st_ino),
        ("rename", store.temp_path(blob_id), final),
        ("fsync-dir", final.parent.stat().st_ino),
    ]


def test_write_blob_refuses_an_existing_temp_file_and_leaves_it(store):
    blob_id = new_blob_id()
    store.temp_path(blob_id).write_bytes(b"someone else's")
    with pytest.raises(FileExistsError):
        store.write_blob(blob_id, io.BytesIO(b"new"))
    assert store.temp_path(blob_id).read_bytes() == b"someone else's"
    assert not store.blob_path(blob_id).exists()


class _FailingReader(io.RawIOBase):
    def __init__(self):
        self.calls = 0

    def read(self, n=-1):
        self.calls += 1
        if self.calls == 1:
            return b"x" * 1000
        raise OSError("client went away")


def test_failure_while_streaming_leaves_no_file(store):
    with pytest.raises(OSError, match="client went away"):
        store.write_blob(new_blob_id(), _FailingReader())
    assert not _files(store.root)


def test_failure_after_rename_removes_the_blob(store, monkeypatch):
    def broken(directory):
        raise OSError("fsync failed")

    monkeypatch.setattr(blobs, "_fsync_dir", broken)
    blob_id = new_blob_id()
    with pytest.raises(OSError, match="fsync failed"):
        store.write_blob(blob_id, io.BytesIO(b"x"))
    assert not _files(store.root)


def _unlink_fails(monkeypatch, error):
    def unlink(path, *args, **kwargs):
        raise error

    monkeypatch.setattr(os, "unlink", unlink)


def test_unlink_failure_other_than_enoent_keeps_file_and_reraises_original(store, monkeypatch):
    """Cleanup that cannot remove the temp file leaves it for the sweep, reports it
    through has_files(), and the original exception is the one that propagates."""
    blob_id = new_blob_id()
    _unlink_fails(monkeypatch, PermissionError(13, "Permission denied"))
    with pytest.raises(OSError, match="client went away"):
        store.write_blob(blob_id, _FailingReader())
    monkeypatch.undo()
    assert store.temp_path(blob_id).exists()
    assert store.has_files(blob_id) is True


def test_unlink_failure_after_rename_keeps_blob_and_reraises_original(store, monkeypatch):
    def broken(directory):
        raise OSError("fsync failed")

    monkeypatch.setattr(blobs, "_fsync_dir", broken)
    _unlink_fails(monkeypatch, PermissionError(13, "Permission denied"))
    blob_id = new_blob_id()
    with pytest.raises(OSError, match="fsync failed"):
        store.write_blob(blob_id, io.BytesIO(b"x"))
    monkeypatch.undo()
    assert store.blob_path(blob_id).exists()
    assert store.has_files(blob_id) is True


def test_has_files(store):
    blob_id = new_blob_id()
    assert store.has_files(blob_id) is False
    store.temp_path(blob_id).write_bytes(b"partial")
    assert store.has_files(blob_id) is True
    store.temp_path(blob_id).unlink()
    store.write_blob(blob_id, io.BytesIO(b"x"))
    assert store.has_files(blob_id) is True
    store.remove_blob(blob_id)
    assert store.has_files(blob_id) is False


def test_has_files_counts_a_stat_error_as_existing(store, monkeypatch):
    def stat_fails(path, *args, **kwargs):
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(os, "stat", stat_fails)
    assert store.has_files(new_blob_id()) is True


def test_mid_write_point_fires_after_first_chunk_with_bytes_on_disk(store, monkeypatch):
    blob_id = new_blob_id()
    seen = []
    monkeypatch.setattr(
        blobs,
        "crash_point",
        lambda name: seen.append((name, store.temp_path(blob_id).stat().st_size)),
    )
    data = b"a" * 200_000
    store.write_blob(blob_id, io.BytesIO(data), mid_write_point="put-mid-write")
    assert seen == [("put-mid-write", 65536)]


def test_no_mid_write_point_by_default(store, monkeypatch):
    seen = []
    monkeypatch.setattr(blobs, "crash_point", seen.append)
    store.write_blob(new_blob_id(), io.BytesIO(b"a" * 200_000))
    assert seen == []


# --- assemble_blob ---


def _parts(store, *bodies):
    ids = []
    for body in bodies:
        blob_id = new_blob_id()
        store.write_blob(blob_id, io.BytesIO(body))
        ids.append(blob_id)
    return ids


def test_assemble_concatenates_in_order(store):
    bodies = [os.urandom(70_000), b"", os.urandom(5)]
    ids = _parts(store, *bodies)
    target = new_blob_id()
    assert store.assemble_blob(target, ids) == sum(map(len, bodies))
    assert store.blob_path(target).read_bytes() == b"".join(bodies)
    for blob_id, body in zip(ids, bodies, strict=True):
        assert store.blob_path(blob_id).read_bytes() == body


@posix_only
def test_assemble_order_is_fsync_file_rename_fsync_fanout(store, calls):
    ids = _parts(store, b"one", b"two")
    calls.clear()
    target = new_blob_id()
    store.assemble_blob(target, ids)
    final = store.blob_path(target)
    assert calls == [
        ("fsync-file", final.stat().st_ino),
        ("rename", store.temp_path(target), final),
        ("fsync-dir", final.parent.stat().st_ino),
    ]


def test_assemble_with_missing_part_raises_and_leaves_no_file(store):
    ids = _parts(store, b"one")
    missing = new_blob_id()
    target = new_blob_id()
    before = _files(store.root)
    with pytest.raises(MissingPartBlob) as exc:
        store.assemble_blob(target, [ids[0], missing])
    assert exc.value.blob_id == missing
    assert isinstance(exc.value, FileNotFoundError)
    assert _files(store.root) == before


def test_assemble_mid_point_fires_after_first_part(store, monkeypatch):
    ids = _parts(store, b"a" * 10, b"b" * 20)
    target = new_blob_id()
    seen = []
    monkeypatch.setattr(
        blobs,
        "crash_point",
        lambda name: seen.append((name, store.temp_path(target).stat().st_size)),
    )
    store.assemble_blob(target, ids, mid_write_point="complete-mid-assemble")
    assert seen == [("complete-mid-assemble", 10)]


# --- reading and removing ---


def test_open_blob_and_size(store):
    (blob_id,) = _parts(store, b"hello")
    with store.open_blob(blob_id) as f:
        assert f.read() == b"hello"
    assert store.blob_size(blob_id) == 5


def test_missing_blob(store):
    blob_id = new_blob_id()
    with pytest.raises(FileNotFoundError):
        store.open_blob(blob_id)
    assert store.blob_size(blob_id) is None
    assert store.remove_blob(blob_id) is False


def test_remove_blob(store):
    (blob_id,) = _parts(store, b"x")
    assert store.remove_blob(blob_id) is True
    assert not store.blob_path(blob_id).exists()


@posix_only
def test_open_descriptor_keeps_reading_after_remove(store):
    (blob_id,) = _parts(store, b"old bytes")
    f = store.open_blob(blob_id)
    store.remove_blob(blob_id)
    with f:
        assert f.read() == b"old bytes"


# --- what the sweep sees ---


def test_temp_entries(store):
    assert store.temp_entries() == []
    leftover = store.root / "tmp" / f"{new_blob_id()}.tmp"
    leftover.write_bytes(b"partial")
    assert store.temp_entries() == [leftover]


def test_scan_on_a_clean_tree_lists_blobs_only(store):
    ids = _parts(store, b"a", b"b")
    (store.root / "LOCK").write_bytes(b"")
    (store.root / "STORE_ID").write_bytes(b"x\n")
    assert sorted(store.scan()) == sorted((store.blob_path(i), i) for i in ids)


def test_scan_reports_entries_that_cannot_be_blobs(store):
    blob_id = new_blob_id()
    wrong_dir = "00" if blob_id[:2] != "00" else "01"
    stray = [
        store.root / "stray.txt",
        store.root / "quarantine",
        store.root / wrong_dir / blob_id,
        store.root / "ab" / "not-an-id",
        store.root / "cd" / ("cd" + "0" * 30),  # directory with an ID name
    ]
    stray[0].write_bytes(b"")
    stray[1].mkdir()
    stray[2].write_bytes(b"")
    stray[3].write_bytes(b"")
    stray[4].mkdir()
    assert sorted(store.scan()) == sorted((p, None) for p in stray)


def test_scan_of_missing_root_is_empty(tmp_path):
    assert list(BlobStore(tmp_path / "nowhere").scan()) == []
