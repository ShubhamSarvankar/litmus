"""
Tier B chaos: user metadata (x-amz-meta-*) commits in the same transaction as the
object row, so a kill leaves the old object with its old metadata or the new object
with its new metadata, never a mix.

Each test arms one crash point, drives an overwrite (or a first PUT) into it, confirms
the server died by SIGKILL, and asserts (a) the raw rows while the server is down,
(b) what the consistency sweep reports and (c) what clients see after an unarmed
restart and that the client's retry replaces the metadata. Sent plain and as an
aws-chunked body.
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

BUCKET = "meta-crash"
KEY = "obj.bin"
URL = f"/{BUCKET}/{KEY}"
OLD = b"old body. " * 3000
NEW = b"new body! " * 4000

OLD_META = {"x-amz-meta-author": "Janet", "x-amz-meta-shared": "old", "x-amz-meta-only-old": "1"}
# The overwrite sends a different set, so a name that leaks from the old object shows
# up as a name the new PUT never sent, and a name present in both shows the new value.
NEW_META = {"x-amz-meta-shared": "new", "x-amz-meta-only-new": "2"}


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


def _setup_old(client: httpx.Client, framed: bool) -> None:
    assert client.put(f"/{BUCKET}").status_code == 200
    assert _put(client, OLD, OLD_META, framed).status_code == 200


def _request_into_crash(server: ServerProcess, send) -> None:
    with pytest.raises(httpx.TransportError):
        send()
    assert server.wait_for_exit() == -signal.SIGKILL


def _store(tmp_path: Path) -> SQLiteMetadataStore:
    return SQLiteMetadataStore(db_path=tmp_path / "meta.db")


def _sweep(tmp_path: Path):
    return run_consistency_sweep(_store(tmp_path), BlobStore(tmp_path / "blobs"))


def _wire(metadata: dict) -> dict:
    """A stored metadata dict, named as on the wire."""
    return {f"x-amz-meta-{k}": v for k, v in metadata.items()}


def _seen(client: httpx.Client) -> tuple[bytes, dict]:
    """The body GET returns and the metadata GET and HEAD agree on."""
    resp = client.get(URL)
    assert resp.status_code == 200
    got = {k: v for k, v in resp.headers.items() if k.startswith("x-amz-meta-")}
    head = client.head(URL)
    assert {k: v for k, v in head.headers.items() if k.startswith("x-amz-meta-")} == got
    return resp.content, got


def test_put_killed_after_bytes_leaves_the_old_metadata(tmp_path: Path, framed):
    server, client = _start(tmp_path)
    _setup_old(client, framed)
    client.close()
    server.stop()
    old = _store(tmp_path).get_object_meta(BUCKET, KEY)

    server, client = _start(tmp_path, crash_point=PUT_AFTER_BYTES)
    _request_into_crash(server, lambda: _put(client, NEW, NEW_META, framed))
    client.close()

    # (a) The row is untouched: the old blob and the old metadata.
    row = _store(tmp_path).get_object_meta(BUCKET, KEY)
    assert row.blob_id == old.blob_id
    assert _wire(row.user_metadata) == OLD_META

    # (b) The new bytes are an interrupted write, nothing else.
    report = _sweep(tmp_path)
    assert [type(i) for i in report.issues] == [InterruptedWrite]

    with _restarted(tmp_path) as client2:
        assert _seen(client2) == (OLD, OLD_META)
        # (c) The client's retry replaces the metadata as well as the bytes.
        assert _put(client2, NEW, NEW_META, framed).status_code == 200
        assert _seen(client2) == (NEW, NEW_META)


def test_put_killed_after_commit_shows_the_new_metadata(tmp_path: Path, framed):
    server, client = _start(tmp_path)
    _setup_old(client, framed)
    client.close()
    server.stop()
    old = _store(tmp_path).get_object_meta(BUCKET, KEY)

    server, client = _start(tmp_path, crash_point=PUT_AFTER_COMMIT)
    _request_into_crash(server, lambda: _put(client, NEW, NEW_META, framed))
    client.close()

    # (a) The new row carries only what the new PUT sent; nothing leaked from the old.
    row = _store(tmp_path).get_object_meta(BUCKET, KEY)
    assert row.blob_id != old.blob_id and row.size == len(NEW)
    assert _wire(row.user_metadata) == NEW_META

    # (b) Only the old blob's cleanup is left.
    report = _sweep(tmp_path)
    assert report.issues == [InterruptedCleanup(old.blob_id, "object", BUCKET, KEY, None, None)]

    with _restarted(tmp_path) as client2:
        assert _seen(client2) == (NEW, NEW_META)


def test_first_put_killed_after_bytes_leaves_no_object_and_no_metadata(tmp_path: Path, framed):
    server, client = _start(tmp_path, crash_point=PUT_AFTER_BYTES)
    assert client.put(f"/{BUCKET}").status_code == 200
    _request_into_crash(server, lambda: _put(client, NEW, OLD_META, framed))
    client.close()

    assert _store(tmp_path).get_object_meta(BUCKET, KEY) is None
    with _restarted(tmp_path) as client2:
        assert client2.get(URL).status_code == 404
        assert client2.head(URL).status_code == 404
        assert _put(client2, NEW, {}, framed).status_code == 200
        assert _seen(client2) == (NEW, {})


def test_first_put_killed_after_commit_shows_the_metadata(tmp_path: Path, framed):
    server, client = _start(tmp_path, crash_point=PUT_AFTER_COMMIT)
    assert client.put(f"/{BUCKET}").status_code == 200
    _request_into_crash(server, lambda: _put(client, NEW, OLD_META, framed))
    client.close()

    assert _wire(_store(tmp_path).get_object_meta(BUCKET, KEY).user_metadata) == OLD_META
    assert _sweep(tmp_path).issues == []
    with _restarted(tmp_path) as client2:
        assert _seen(client2) == (NEW, OLD_META)


def test_non_ascii_metadata_survives_a_kill_after_commit(tmp_path: Path, framed):
    sent = {"x-amz-meta-greeting": "=?UTF-8?B?Y2Fmw6k=?="}
    server, client = _start(tmp_path, crash_point=PUT_AFTER_COMMIT)
    assert client.put(f"/{BUCKET}").status_code == 200
    _request_into_crash(server, lambda: _put(client, NEW, sent, framed))
    client.close()

    # Stored as text, returned as an encoded word.
    assert _store(tmp_path).get_object_meta(BUCKET, KEY).user_metadata == {"greeting": "café"}
    with _restarted(tmp_path) as client2:
        assert _seen(client2) == (NEW, sent)
