"""
Tier B chaos: DeleteObject with If-Match killed around its commit
(docs/design/storage-layout.md §6.3, §10).

The condition is decided in the commit. A kill at delete-before-commit leaves the object
whatever the condition would have said, and the same request then succeeds. A kill at
delete-after-commit leaves the object gone, and the same request retried is 404 NoSuchKey
(If-Match on a missing key), where an unconditional retry is 204.
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
from server.consistency import InterruptedCleanup, run_consistency_sweep
from server.crash_points import DELETE_AFTER_COMMIT, DELETE_BEFORE_COMMIT
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SIGKILL not available on Windows")

BUCKET = "cond-delete-crash"
KEY = "obj.bin"
URL = f"/{BUCKET}/{KEY}"
BODY = b"old body. " * 3000
ETAG = '"' + hashlib.md5(BODY).hexdigest() + '"'


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


def _request_into_crash(server: ServerProcess, send) -> None:
    with pytest.raises(httpx.TransportError):
        send()
    assert server.wait_for_exit() == -signal.SIGKILL


def _store(tmp_path: Path) -> SQLiteMetadataStore:
    return SQLiteMetadataStore(db_path=tmp_path / "meta.db")


def _blobs(tmp_path: Path) -> BlobStore:
    return BlobStore(tmp_path / "blobs")


def _sweep(tmp_path: Path):
    return run_consistency_sweep(_store(tmp_path), _blobs(tmp_path))


def _blob_files(tmp_path: Path) -> set[str]:
    return {blob_id for _, blob_id in _blobs(tmp_path).scan()}


def _staged(tmp_path: Path):
    """An object on a server that is then stopped. Returns its row."""
    server, client = _start(tmp_path)
    assert client.put(f"/{BUCKET}").status_code == 200
    assert client.put(URL, content=BODY).status_code == 200
    assert client.head(URL).headers["etag"] == ETAG
    client.close()
    server.stop()
    return _store(tmp_path).get_object_meta(BUCKET, KEY)


def test_delete_with_a_passing_if_match_killed_before_commit_keeps_the_object(tmp_path: Path):
    old = _staged(tmp_path)
    server, client = _start(tmp_path, crash_point=DELETE_BEFORE_COMMIT)
    _request_into_crash(server, lambda: client.delete(URL, headers={"If-Match": ETAG}))
    client.close()

    # (a) The row, its registry entry and its bytes are as they were.
    assert _store(tmp_path).get_object_meta(BUCKET, KEY) == old
    assert [r.state for r in _store(tmp_path).list_blob_records()] == ["live"]
    assert _blob_files(tmp_path) == {old.blob_id}

    # (b) Nothing for the sweep to do.
    assert _sweep(tmp_path).clean

    with _restarted(tmp_path) as client2:
        # (c) The object reads back, and the same request now succeeds.
        got = client2.get(URL)
        assert got.content == BODY and got.headers["etag"] == ETAG
        assert client2.delete(URL, headers={"If-Match": ETAG}).status_code == 204
        assert client2.get(URL).status_code == 404
    assert _blob_files(tmp_path) == set() and _store(tmp_path).list_blob_records() == []
    assert _sweep(tmp_path).clean


def test_delete_with_a_passing_if_match_killed_after_commit_removes_the_object(tmp_path: Path):
    old = _staged(tmp_path)
    server, client = _start(tmp_path, crash_point=DELETE_AFTER_COMMIT)
    _request_into_crash(server, lambda: client.delete(URL, headers={"If-Match": ETAG}))
    client.close()

    # (a) The row is gone; the blob is garbage with its file still there.
    assert _store(tmp_path).get_object_meta(BUCKET, KEY) is None
    garbage = [r.blob_id for r in _store(tmp_path).list_blob_records() if r.state == "garbage"]
    assert garbage == [old.blob_id]
    assert _blob_files(tmp_path) == {old.blob_id}

    # (b) Only cleanup is left.
    assert _sweep(tmp_path).issues == [
        InterruptedCleanup(old.blob_id, "object", BUCKET, KEY, None, None)
    ]
    assert _blob_files(tmp_path) == set() and _store(tmp_path).list_blob_records() == []

    with _restarted(tmp_path) as client2:
        # (c) The client's retry of the same request: the key is gone, so 404.
        assert client2.get(URL).status_code == 404
        retry = client2.delete(URL, headers={"If-Match": ETAG})
        assert retry.status_code == 404
        assert ET.fromstring(retry.text).findtext("Code") == "NoSuchKey"
        assert client2.get(URL).status_code == 404
        assert client2.delete(URL).status_code == 204
    assert _sweep(tmp_path).clean
