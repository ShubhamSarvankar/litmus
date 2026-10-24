"""RELEASE and GC (docs/design/storage-layout.md §6.1), and what the handlers do when
cleanup itself fails."""

import io
import os
from datetime import datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from chaos.faults.faults import RaiseOSError
from chaos.fixtures.inprocess import make_faulty_server
from chaos.framework.injector import FaultInjector
from chaos.framework.triggers import CallCountTrigger
from server.app import create_app
from server.consistency import InterruptedWrite, run_consistency_sweep
from server.metadata.base import ObjectMeta
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore
from server.storage.lifecycle import collect_garbage, discard_new_blob

_NOW = datetime(2024, 1, 1)


@pytest.fixture
def deps(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    storage.create_tree()
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    metadata.create_bucket("b", _NOW)
    return storage, metadata


def _pending(deps, body=b"x"):
    storage, metadata = deps
    blob_id = metadata.register_blob("object", "b", "k")
    storage.write_blob(blob_id, io.BytesIO(body))
    return blob_id


def _live(deps, key="k", body=b"x"):
    storage, metadata = deps
    blob_id = metadata.register_blob("object", "b", key)
    etag, size = storage.write_blob(blob_id, io.BytesIO(body))
    metadata.put_object_meta("b", key, ObjectMeta("b", key, size, etag, _NOW, "t", blob_id))
    return blob_id


def test_discard_removes_file_and_releases_row(deps):
    storage, metadata = deps
    blob_id = _pending(deps)
    discard_new_blob(storage, metadata, blob_id)
    assert not storage.has_files(blob_id)
    assert metadata.blob_state(blob_id) is None


def test_discard_keeps_row_pending_when_a_file_cannot_be_removed(deps, monkeypatch):
    storage, metadata = deps
    blob_id = _pending(deps)

    def unlink_fails(path, *args, **kwargs):
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(os, "unlink", unlink_fails)
    discard_new_blob(storage, metadata, blob_id)
    monkeypatch.undo()
    assert storage.blob_path(blob_id).exists()
    assert metadata.blob_state(blob_id) == "pending"


def test_discard_never_touches_a_live_blob(deps):
    storage, metadata = deps
    blob_id = _live(deps)
    discard_new_blob(storage, metadata, blob_id)
    assert storage.blob_path(blob_id).exists()
    assert metadata.blob_state(blob_id) == "live"


def test_collect_garbage_purges_only_what_it_unlinked(deps, monkeypatch):
    storage, metadata = deps
    first, second = _live(deps, "k1"), _live(deps, "k2")
    garbage = metadata.delete_objects("b", ["k1", "k2"])
    assert sorted(garbage) == sorted([first, second])
    real_unlink = os.unlink

    def unlink(path, *args, **kwargs):
        if Path(path).name == second:
            raise PermissionError(13, "Permission denied")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(os, "unlink", unlink)
    collect_garbage(storage, metadata, garbage)
    monkeypatch.undo()
    assert metadata.blob_state(first) is None and not storage.blob_path(first).exists()
    assert metadata.blob_state(second) == "garbage" and storage.blob_path(second).exists()


def test_failed_put_whose_temp_cannot_be_removed_leaves_it_for_the_sweep(tmp_path, monkeypatch):
    """ENOSPC mid-write, and the temp file cannot be unlinked either: the request
    fails, the row stays pending, and the startup sweep removes the file."""

    class FailingWrites:
        """write_blob writes part of the body, then the disk is full."""

        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def write_blob(self, blob_id, src, mid_write_point=None):
            class Src(io.RawIOBase):
                calls = 0

                def read(self, n=-1):
                    Src.calls += 1
                    if Src.calls == 1:
                        return b"x" * 100
                    raise OSError(28, "No space left on device")

            return self._inner.write_blob(blob_id, Src(), mid_write_point)

    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=FailingWrites(storage), metadata=metadata),
        raise_server_exceptions=False,
    )
    assert client.put("/bkt").status_code == 200
    real_unlink = os.unlink

    def unlink(path, *args, **kwargs):
        if str(path).endswith(".tmp"):
            raise PermissionError(13, "Permission denied")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(os, "unlink", unlink)
    assert client.put("/bkt/k", content=b"body").status_code == 500
    monkeypatch.undo()

    (record,) = metadata.list_blob_records()
    assert record.state == "pending"
    assert storage.temp_entries() == [storage.temp_path(record.blob_id)]
    assert client.get("/bkt/k").status_code == 404

    report = run_consistency_sweep(metadata, storage)
    assert storage.temp_entries() == []
    assert metadata.list_blob_records() == []
    assert any(
        isinstance(i, InterruptedWrite) and i.key == "k" and not i.bytes_durable
        for i in report.issues
    )


def test_enospc_on_write_releases_the_registered_row(tmp_path):
    injector = FaultInjector(
        method="write_blob",
        trigger=CallCountTrigger(fire_on=1),
        fault=RaiseOSError(28),
    )
    client, _, metadata, storage = make_faulty_server(tmp_path, injector)
    with client:
        client.put("/bucket")
        assert client.put("/bucket/key", content=b"x").status_code == 500
    assert metadata.list_blob_records() == []
    assert list(storage.scan()) == []
