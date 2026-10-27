"""
Tier B chaos: a PutObject with a checksum killed around its commit
(docs/design/storage-layout.md §6.2, §20).

The checksum is committed in the object row's transaction, so a kill at put-after-bytes
(the bytes are durable, the commit has not run) leaves no row for a new key and the old
object, bytes and checksum, for an overwrite, and a kill at put-after-commit leaves the
row with its checksum. The expected values come from hashlib and zlib.
"""

import base64
import hashlib
import os
import signal
import zlib
from pathlib import Path

import httpx
import pytest

from chaos.fixtures.subprocess import ServerProcess, _find_free_port
from server.consistency import InterruptedWrite, run_consistency_sweep
from server.crash_points import PUT_AFTER_BYTES, PUT_AFTER_COMMIT
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SIGKILL not available on Windows")

BUCKET = "cksum-crash"
KEY = "obj.bin"
URL = f"/{BUCKET}/{KEY}"
OLD = b"old body. " * 3000
NEW = b"new body! " * 4000
OLD_CRC32 = base64.b64encode(zlib.crc32(OLD).to_bytes(4, "big")).decode()
NEW_SHA256 = base64.b64encode(hashlib.sha256(NEW).digest()).decode()
OLD_CLAIM = {"x-amz-checksum-crc32": OLD_CRC32}
NEW_CLAIM = {"x-amz-checksum-sha256": NEW_SHA256}


def _start(tmp_path: Path, crash_point: str | None = None) -> tuple[ServerProcess, httpx.Client]:
    port = _find_free_port()
    server = ServerProcess(
        data_dir=tmp_path, db_path=tmp_path / "meta.db", port=port, crash_point=crash_point
    )
    server.start()
    server.wait_ready()
    return server, httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10.0)


def _request_into_crash(server: ServerProcess, send) -> None:
    with pytest.raises(httpx.TransportError):
        send()
    assert server.wait_for_exit() == -signal.SIGKILL


def _store(tmp_path: Path) -> SQLiteMetadataStore:
    return SQLiteMetadataStore(db_path=tmp_path / "meta.db")


def _sweep(tmp_path: Path):
    return run_consistency_sweep(_store(tmp_path), BlobStore(tmp_path / "blobs"))


def _columns(tmp_path: Path):
    meta = _store(tmp_path).get_object_meta(BUCKET, KEY)
    return (meta.checksum, meta.checksum_algorithm, meta.checksum_type)


def test_put_killed_after_bytes_on_a_new_key_leaves_no_row(tmp_path: Path):
    server, client = _start(tmp_path, crash_point=PUT_AFTER_BYTES)
    assert client.put(f"/{BUCKET}").status_code == 200
    _request_into_crash(server, lambda: client.put(URL, content=NEW, headers=NEW_CLAIM))
    client.close()

    assert _store(tmp_path).get_object_meta(BUCKET, KEY) is None
    assert [type(i) for i in _sweep(tmp_path).issues] == [InterruptedWrite]
    assert _sweep(tmp_path).issues == []


def test_put_killed_after_commit_leaves_the_row_with_its_checksum(tmp_path: Path):
    server, client = _start(tmp_path, crash_point=PUT_AFTER_COMMIT)
    assert client.put(f"/{BUCKET}").status_code == 200
    _request_into_crash(server, lambda: client.put(URL, content=NEW, headers=NEW_CLAIM))
    client.close()

    assert _columns(tmp_path) == (NEW_SHA256, "SHA256", "FULL_OBJECT")
    assert _store(tmp_path).get_object_meta(BUCKET, KEY).size == len(NEW)
    assert _sweep(tmp_path).issues == []


def test_overwrite_killed_after_bytes_keeps_the_old_bytes_and_checksum(tmp_path: Path):
    server, client = _start(tmp_path)
    assert client.put(f"/{BUCKET}").status_code == 200
    assert client.put(URL, content=OLD, headers=OLD_CLAIM).status_code == 200
    client.close()
    server.stop()
    old = _store(tmp_path).get_object_meta(BUCKET, KEY)
    assert _columns(tmp_path) == (OLD_CRC32, "CRC32", "FULL_OBJECT")

    server, client = _start(tmp_path, crash_point=PUT_AFTER_BYTES)
    _request_into_crash(server, lambda: client.put(URL, content=NEW, headers=NEW_CLAIM))
    client.close()

    assert _store(tmp_path).get_object_meta(BUCKET, KEY) == old
    assert _columns(tmp_path) == (OLD_CRC32, "CRC32", "FULL_OBJECT")
    with BlobStore(tmp_path / "blobs").open_blob(old.blob_id) as f:
        assert f.read() == OLD
    assert [type(i) for i in _sweep(tmp_path).issues] == [InterruptedWrite]
    assert _sweep(tmp_path).issues == []
