"""
Tier B chaos: a conditional PutObject killed around its commit
(docs/design/storage-layout.md §6.2, §10).

The condition is decided in the commit, so a kill at put-after-bytes (the bytes are
durable, the commit has not run) leaves the old object whatever the condition would have
said, and a kill at put-after-commit leaves the new object, after which the same
If-Match, retried, is refused. Sent plain and as an aws-chunked body.
"""

import os
import signal
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest

from chaos.fixtures.subprocess import ServerProcess, _find_free_port
from chaos.framework.framed import framed_request
from server.consistency import InterruptedCleanup, InterruptedWrite, run_consistency_sweep
from server.crash_points import PUT_AFTER_BYTES, PUT_AFTER_COMMIT
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SIGKILL not available on Windows")

BUCKET = "cond-crash"
KEY = "obj.bin"
URL = f"/{BUCKET}/{KEY}"
OLD = b"old body. " * 3000
NEW = b"new body! " * 4000


@pytest.fixture(params=[pytest.param(False, id="plain"), pytest.param(True, id="framed")])
def framed(request) -> bool:
    return request.param


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


def _put(client: httpx.Client, data: bytes, headers: dict, framed: bool) -> httpx.Response:
    if framed:
        framed_headers, body = framed_request(data)
        return client.put(URL, content=body, headers={**framed_headers, **headers})
    return client.put(URL, content=data, headers=headers)


def _request_into_crash(server: ServerProcess, send) -> None:
    with pytest.raises(httpx.TransportError):
        send()
    assert server.wait_for_exit() == -signal.SIGKILL


def _store(tmp_path: Path) -> SQLiteMetadataStore:
    return SQLiteMetadataStore(db_path=tmp_path / "meta.db")


def _sweep(tmp_path: Path):
    return run_consistency_sweep(_store(tmp_path), BlobStore(tmp_path / "blobs"))


def _etag(client: httpx.Client) -> str:
    return client.head(URL).headers["etag"]


def _old_object(tmp_path: Path, framed: bool):
    server, client = _start(tmp_path)
    assert client.put(f"/{BUCKET}").status_code == 200
    assert _put(client, OLD, {}, framed).status_code == 200
    old_etag = _etag(client)
    client.close()
    server.stop()
    return _store(tmp_path).get_object_meta(BUCKET, KEY), old_etag


# Conditions that fail against the existing object.
FAILING = [
    pytest.param({"If-Match": '"0123456789abcdef0123456789abcdef"'}, id="if-match-mismatch"),
    pytest.param({"If-None-Match": "*"}, id="if-none-match-star"),
]


@pytest.mark.parametrize("condition", FAILING)
def test_put_killed_after_bytes_with_a_failing_condition_leaves_the_old_object(
    tmp_path: Path, framed, condition
):
    old, old_etag = _old_object(tmp_path, framed)

    server, client = _start(tmp_path, crash_point=PUT_AFTER_BYTES)
    _request_into_crash(server, lambda: _put(client, NEW, condition, framed))
    client.close()

    # (a) The row is untouched; the new bytes wait under a pending registry row.
    assert _store(tmp_path).get_object_meta(BUCKET, KEY) == old
    assert [r.state for r in _store(tmp_path).list_blob_records()].count("pending") == 1

    # (b) The sweep removes the orphan as an interrupted write, and nothing else.
    report = _sweep(tmp_path)
    assert [type(i) for i in report.issues] == [InterruptedWrite]
    assert [r.state for r in _store(tmp_path).list_blob_records()] == ["live"]
    assert BlobStore(tmp_path / "blobs").temp_entries() == []

    with _restarted(tmp_path) as client2:
        got = client2.get(URL)
        assert got.content == OLD and got.headers["etag"] == old_etag
        # (c) The same request is refused now, changing nothing; a retry whose condition
        # holds works.
        assert _put(client2, NEW, condition, framed).status_code == 412
        assert client2.get(URL).content == OLD
        assert _put(client2, NEW, {"If-Match": old_etag}, framed).status_code == 200
        assert client2.get(URL).content == NEW
    assert _sweep(tmp_path).issues == []


def test_put_killed_after_commit_with_if_match_shows_the_new_object(tmp_path: Path, framed):
    old, old_etag = _old_object(tmp_path, framed)

    server, client = _start(tmp_path, crash_point=PUT_AFTER_COMMIT)
    _request_into_crash(server, lambda: _put(client, NEW, {"If-Match": old_etag}, framed))
    client.close()

    # (a) The commit stands: the row names the new blob.
    row = _store(tmp_path).get_object_meta(BUCKET, KEY)
    assert row.blob_id != old.blob_id and row.size == len(NEW)

    # (b) Only the old blob's cleanup is left.
    report = _sweep(tmp_path)
    assert report.issues == [InterruptedCleanup(old.blob_id, "object", BUCKET, KEY, None, None)]

    with _restarted(tmp_path) as client2:
        got = client2.get(URL)
        assert got.content == NEW
        new_etag = got.headers["etag"]
        # (c) The client's retry of the same request is refused: the object it asked to
        # replace is gone. Nothing changes.
        retry = _put(client2, NEW, {"If-Match": old_etag}, framed)
        assert retry.status_code == 412
        assert client2.get(URL).headers["etag"] == new_etag
    assert _sweep(tmp_path).issues == []


def test_first_put_with_if_none_match_star_killed_after_commit_refuses_the_retry(
    tmp_path: Path, framed
):
    server, client = _start(tmp_path, crash_point=PUT_AFTER_COMMIT)
    assert client.put(f"/{BUCKET}").status_code == 200
    _request_into_crash(server, lambda: _put(client, NEW, {"If-None-Match": "*"}, framed))
    client.close()

    assert _store(tmp_path).get_object_meta(BUCKET, KEY).size == len(NEW)
    assert _sweep(tmp_path).issues == []
    with _restarted(tmp_path) as client2:
        assert client2.get(URL).content == NEW
        assert _put(client2, OLD, {"If-None-Match": "*"}, framed).status_code == 412
        assert client2.get(URL).content == NEW


def test_first_put_with_if_none_match_star_killed_after_bytes_can_be_retried(
    tmp_path: Path, framed
):
    server, client = _start(tmp_path, crash_point=PUT_AFTER_BYTES)
    assert client.put(f"/{BUCKET}").status_code == 200
    _request_into_crash(server, lambda: _put(client, NEW, {"If-None-Match": "*"}, framed))
    client.close()

    assert _store(tmp_path).get_object_meta(BUCKET, KEY) is None
    assert [type(i) for i in _sweep(tmp_path).issues] == [InterruptedWrite]
    with _restarted(tmp_path) as client2:
        assert client2.get(URL).status_code == 404
        assert _put(client2, NEW, {"If-None-Match": "*"}, framed).status_code == 200
        assert client2.get(URL).content == NEW
