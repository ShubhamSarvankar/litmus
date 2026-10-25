"""
Tier B chaos: DeleteObjects with per key ETag conditions killed around its commit
(docs/design/storage-layout.md §6.4, §10).

The conditions are decided in the batch's one commit. A kill at delete-objects-before-commit
leaves every object whatever its condition would have said, and the same request then
succeeds. A kill at delete-objects-after-commit leaves the keys whose conditions held
gone (their blobs garbage for the sweep) and the refused key intact; the same request
retried is NoSuchKey for the deleted key and PreconditionFailed for the refused one.
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
from server.crash_points import DELETE_OBJECTS_AFTER_COMMIT, DELETE_OBJECTS_BEFORE_COMMIT
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SIGKILL not available on Windows")

BUCKET = "cond-delete-objects-crash"
BODIES = {"pass": b"pass body. " * 3000, "keep": b"keep body. " * 3000}
OTHER = '"0123456789abcdef0123456789abcdef"'


def _etag(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


# "pass" has a passing condition, "keep" a failing one.
REQUEST = (
    f"<Delete><Object><Key>pass</Key><ETag>{_etag(BODIES['pass'])}</ETag></Object>"
    f"<Object><Key>keep</Key><ETag>{OTHER}</ETag></Object></Delete>"
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


def _staged(tmp_path: Path) -> dict:
    """Both objects on a server that is then stopped. Returns their rows."""
    server, client = _start(tmp_path)
    assert client.put(f"/{BUCKET}").status_code == 200
    for key, body in BODIES.items():
        assert client.put(f"/{BUCKET}/{key}", content=body).status_code == 200
    client.close()
    server.stop()
    return {key: _store(tmp_path).get_object_meta(BUCKET, key) for key in BODIES}


def _results(resp: httpx.Response):
    assert resp.status_code == 200
    root = ET.fromstring(resp.text)
    return (
        [d.findtext("Key") for d in root.findall("Deleted")],
        [(e.findtext("Key"), e.findtext("Code")) for e in root.findall("Error")],
    )


def test_delete_objects_with_a_passing_etag_killed_before_commit_keeps_the_objects(
    tmp_path: Path,
):
    rows = _staged(tmp_path)
    server, client = _start(tmp_path, crash_point=DELETE_OBJECTS_BEFORE_COMMIT)
    _request_into_crash(server, lambda: client.post(f"/{BUCKET}?delete", content=REQUEST))
    client.close()

    # (a) Both rows, their registry entries and their bytes are as they were.
    store = _store(tmp_path)
    assert {key: store.get_object_meta(BUCKET, key) for key in BODIES} == rows
    assert [r.state for r in store.list_blob_records()] == ["live", "live"]
    assert _blob_files(tmp_path) == {row.blob_id for row in rows.values()}

    # (b) Nothing for the sweep to do.
    assert _sweep(tmp_path).clean

    with _restarted(tmp_path) as client2:
        # (c) The objects read back, and the same request now does what it says.
        for key, body in BODIES.items():
            got = client2.get(f"/{BUCKET}/{key}")
            assert got.content == body and got.headers["etag"] == _etag(body)
        assert _results(client2.post(f"/{BUCKET}?delete", content=REQUEST)) == (
            ["pass"],
            [("keep", "PreconditionFailed")],
        )
        assert client2.get(f"/{BUCKET}/pass").status_code == 404
        assert client2.get(f"/{BUCKET}/keep").content == BODIES["keep"]
    assert _blob_files(tmp_path) == {rows["keep"].blob_id}
    assert [r.blob_id for r in _store(tmp_path).list_blob_records()] == [rows["keep"].blob_id]
    assert _sweep(tmp_path).clean


def test_delete_objects_with_a_passing_etag_killed_after_commit_removes_the_object(
    tmp_path: Path,
):
    rows = _staged(tmp_path)
    server, client = _start(tmp_path, crash_point=DELETE_OBJECTS_AFTER_COMMIT)
    _request_into_crash(server, lambda: client.post(f"/{BUCKET}?delete", content=REQUEST))
    client.close()

    # (a) "pass" is gone, its blob garbage with the file still there; "keep" is intact.
    store = _store(tmp_path)
    assert store.get_object_meta(BUCKET, "pass") is None
    assert store.get_object_meta(BUCKET, "keep") == rows["keep"]
    garbage = [r.blob_id for r in store.list_blob_records() if r.state == "garbage"]
    assert garbage == [rows["pass"].blob_id]
    assert _blob_files(tmp_path) == {row.blob_id for row in rows.values()}

    # (b) Only cleanup is left.
    assert _sweep(tmp_path).issues == [
        InterruptedCleanup(rows["pass"].blob_id, "object", BUCKET, "pass", None, None)
    ]
    assert _blob_files(tmp_path) == {rows["keep"].blob_id}

    with _restarted(tmp_path) as client2:
        # (c) The client's retry of the same request: "pass" is gone, so NoSuchKey;
        # "keep" still fails its condition.
        assert client2.get(f"/{BUCKET}/pass").status_code == 404
        assert _results(client2.post(f"/{BUCKET}?delete", content=REQUEST)) == (
            [],
            [("pass", "NoSuchKey"), ("keep", "PreconditionFailed")],
        )
        got = client2.get(f"/{BUCKET}/keep")
        assert got.content == BODIES["keep"] and got.headers["etag"] == _etag(BODIES["keep"])
    assert _sweep(tmp_path).clean
