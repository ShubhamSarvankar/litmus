"""
Tier B chaos: the stored system headers (Cache-Control, Content-Disposition,
Content-Encoding, Content-Language, Expires) commit in the same transaction as the
object row, so a kill leaves the old object with its old headers or the new object
with its new ones, never a mix.

Each test arms one crash point, drives an overwrite (or a first PUT) into it, confirms
the server died by SIGKILL, and asserts (a) the raw rows while the server is down,
(b) what clients see after an unarmed restart and (c) that the consistency sweep
reports only what the crash point implies. Sent plain and as an aws-chunked body.
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

BUCKET = "hdr-crash"
KEY = "obj.bin"
URL = f"/{BUCKET}/{KEY}"
OLD = b"old body. " * 3000
NEW = b"new body! " * 4000

OLD_HEADERS = {
    "Cache-Control": "max-age=60",
    "Content-Disposition": "attachment",
    "Content-Encoding": "gzip",
    "Content-Language": "en",
    "Expires": "Thu, 01 Dec 2033 16:00:00 GMT",
}
# The overwrite sends a different subset, so a header that leaks from the old object
# shows up as a header the new one never sent.
NEW_HEADERS = {"Cache-Control": "no-store", "Content-Language": "fr"}


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
        # Keep the framing's own Content-Encoding beside the one under test.
        encoding = ", ".join(filter(None, [headers.get("Content-Encoding"), "aws-chunked"]))
        merged = {**framed_headers, **headers, "content-encoding": encoding}
        merged.pop("Content-Encoding", None)
        return client.put(URL, content=body, headers=merged)
    return client.put(URL, content=data, headers=headers)


def _setup_old(client: httpx.Client, framed: bool) -> None:
    assert client.put(f"/{BUCKET}").status_code == 200
    assert _put(client, OLD, OLD_HEADERS, framed).status_code == 200


def _request_into_crash(server: ServerProcess, send) -> None:
    with pytest.raises(httpx.TransportError):
        send()
    assert server.wait_for_exit() == -signal.SIGKILL


def _store(tmp_path: Path) -> SQLiteMetadataStore:
    return SQLiteMetadataStore(db_path=tmp_path / "meta.db")


def _sweep(tmp_path: Path):
    return run_consistency_sweep(_store(tmp_path), BlobStore(tmp_path / "blobs"))


def _stored(meta) -> dict:
    """The row's system headers, named as on the wire, without the absent ones."""
    pairs = {
        "Cache-Control": meta.cache_control,
        "Content-Disposition": meta.content_disposition,
        "Content-Encoding": meta.content_encoding,
        "Content-Language": meta.content_language,
        "Expires": meta.expires,
    }
    return {k: v for k, v in pairs.items() if v is not None}


def _seen(client: httpx.Client) -> tuple[bytes, dict]:
    """The body GET returns, undecoded, and the system headers GET and HEAD agree on."""
    with client.stream("GET", URL) as resp:
        assert resp.status_code == 200
        body = b"".join(resp.iter_raw())
        got = {k: resp.headers[k] for k in OLD_HEADERS if k in resp.headers}
    head = client.head(URL)
    assert {k: head.headers[k] for k in OLD_HEADERS if k in head.headers} == got
    return body, got


def test_put_killed_after_bytes_leaves_the_old_headers(tmp_path: Path, framed):
    server, client = _start(tmp_path)
    _setup_old(client, framed)
    client.close()
    server.stop()
    old = _store(tmp_path).get_object_meta(BUCKET, KEY)

    server, client = _start(tmp_path, crash_point=PUT_AFTER_BYTES)
    _request_into_crash(server, lambda: _put(client, NEW, NEW_HEADERS, framed))
    client.close()

    # (a) The row is untouched: the old blob and the old headers.
    row = _store(tmp_path).get_object_meta(BUCKET, KEY)
    assert row.blob_id == old.blob_id
    assert _stored(row) == OLD_HEADERS

    # (b) The new bytes are an interrupted write, nothing else.
    report = _sweep(tmp_path)
    assert [type(i) for i in report.issues] == [InterruptedWrite]

    with _restarted(tmp_path) as client2:
        assert _seen(client2) == (OLD, OLD_HEADERS)
        # (c) The client's retry replaces the headers as well as the bytes.
        assert _put(client2, NEW, NEW_HEADERS, framed).status_code == 200
        assert _seen(client2) == (NEW, NEW_HEADERS)


def test_put_killed_after_commit_shows_the_new_headers(tmp_path: Path, framed):
    server, client = _start(tmp_path)
    _setup_old(client, framed)
    client.close()
    server.stop()
    old = _store(tmp_path).get_object_meta(BUCKET, KEY)

    server, client = _start(tmp_path, crash_point=PUT_AFTER_COMMIT)
    _request_into_crash(server, lambda: _put(client, NEW, NEW_HEADERS, framed))
    client.close()

    # (a) The new row carries only what the new PUT sent; nothing leaked from the old.
    row = _store(tmp_path).get_object_meta(BUCKET, KEY)
    assert row.blob_id != old.blob_id and row.size == len(NEW)
    assert _stored(row) == NEW_HEADERS

    # (b) Only the old blob's cleanup is left.
    report = _sweep(tmp_path)
    assert report.issues == [InterruptedCleanup(old.blob_id, "object", BUCKET, KEY, None, None)]

    with _restarted(tmp_path) as client2:
        assert _seen(client2) == (NEW, NEW_HEADERS)


def test_first_put_killed_after_bytes_leaves_no_object_and_no_headers(tmp_path: Path, framed):
    server, client = _start(tmp_path, crash_point=PUT_AFTER_BYTES)
    assert client.put(f"/{BUCKET}").status_code == 200
    _request_into_crash(server, lambda: _put(client, NEW, OLD_HEADERS, framed))
    client.close()

    assert _store(tmp_path).get_object_meta(BUCKET, KEY) is None
    with _restarted(tmp_path) as client2:
        assert client2.get(URL).status_code == 404
        assert client2.head(URL).status_code == 404
        assert _put(client2, NEW, {}, framed).status_code == 200
        assert _seen(client2) == (NEW, {})


def test_first_put_killed_after_commit_shows_the_headers(tmp_path: Path, framed):
    server, client = _start(tmp_path, crash_point=PUT_AFTER_COMMIT)
    assert client.put(f"/{BUCKET}").status_code == 200
    _request_into_crash(server, lambda: _put(client, NEW, OLD_HEADERS, framed))
    client.close()

    assert _stored(_store(tmp_path).get_object_meta(BUCKET, KEY)) == OLD_HEADERS
    assert _sweep(tmp_path).issues == []
    with _restarted(tmp_path) as client2:
        assert _seen(client2) == (NEW, OLD_HEADERS)
