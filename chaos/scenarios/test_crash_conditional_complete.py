"""
Tier B chaos: a conditional CompleteMultipartUpload killed around its commit
(docs/design/storage-layout.md §6.7, §10).

The condition is decided in the commit, so a kill at complete-after-assemble (the
assembled bytes are durable, the commit has not run) leaves the old object and the open
upload whatever the condition would have said, and a kill at complete-after-commit leaves
the new object, after which the same Complete, retried, is NoSuchUpload (the upload is
closed; re-completing it is the known gap j3 in test_crash_points.py, unchanged here).
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
from server.consistency import InterruptedCleanup, InterruptedWrite, run_consistency_sweep
from server.crash_points import COMPLETE_AFTER_ASSEMBLE, COMPLETE_AFTER_COMMIT
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SIGKILL not available on Windows")

BUCKET = "cond-complete-crash"
KEY = "obj.bin"
URL = f"/{BUCKET}/{KEY}"
OLD = b"old body. " * 3000
PARTS = [bytes([n]) * (128 * 1024) for n in (1, 2, 3)]
NEW = b"".join(PARTS)
NEW_ETAG = '"' + hashlib.md5(b"".join(hashlib.md5(p).digest() for p in PARTS)).hexdigest() + '-3"'


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


def _complete(client: httpx.Client, upload_id: str, etags: list[str], headers: dict):
    items = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>"
        for n, e in enumerate(etags, start=1)
    )
    return client.post(
        f"{URL}?uploadId={upload_id}",
        content=f"<CompleteMultipartUpload>{items}</CompleteMultipartUpload>".encode(),
        headers=headers,
    )


def _request_into_crash(server: ServerProcess, send) -> None:
    with pytest.raises(httpx.TransportError):
        send()
    assert server.wait_for_exit() == -signal.SIGKILL


def _store(tmp_path: Path) -> SQLiteMetadataStore:
    return SQLiteMetadataStore(db_path=tmp_path / "meta.db")


def _sweep(tmp_path: Path):
    return run_consistency_sweep(_store(tmp_path), BlobStore(tmp_path / "blobs"))


def _staged(tmp_path: Path):
    """An old object and an open upload with three parts, on a server that is then
    stopped. Returns the old row, its ETag, the upload id, the part ETags."""
    server, client = _start(tmp_path)
    assert client.put(f"/{BUCKET}").status_code == 200
    assert client.put(URL, content=OLD).status_code == 200
    old_etag = client.head(URL).headers["etag"]
    upload_id = ET.fromstring(client.post(f"{URL}?uploads").text).findtext("UploadId")
    etags = []
    for number, data in enumerate(PARTS, start=1):
        r = client.put(f"{URL}?partNumber={number}&uploadId={upload_id}", content=data)
        assert r.status_code == 200
        etags.append(r.headers["etag"])
    client.close()
    server.stop()
    return _store(tmp_path).get_object_meta(BUCKET, KEY), old_etag, upload_id, etags


# Conditions that fail against the existing object.
FAILING = [
    pytest.param({"If-Match": '"0123456789abcdef0123456789abcdef"'}, id="if-match-mismatch"),
    pytest.param({"If-None-Match": "*"}, id="if-none-match-star"),
]


@pytest.mark.parametrize("condition", FAILING)
def test_complete_killed_after_assemble_with_a_failing_condition_leaves_the_old_object(
    tmp_path: Path, condition
):
    old, old_etag, upload_id, etags = _staged(tmp_path)
    parts_before = _store(tmp_path).get_parts(upload_id)

    server, client = _start(tmp_path, crash_point=COMPLETE_AFTER_ASSEMBLE)
    _request_into_crash(server, lambda: _complete(client, upload_id, etags, condition))
    client.close()

    # (a) The object row is untouched, the upload is open with all its parts, and the
    # assembled bytes wait under one pending registry row.
    store = _store(tmp_path)
    assert store.get_object_meta(BUCKET, KEY) == old
    assert store.upload_exists(upload_id)
    assert store.get_parts(upload_id) == parts_before
    pending = [r for r in store.list_blob_records() if r.state == "pending"]
    assert len(pending) == 1

    # (b) The sweep reports the orphan as an interrupted write, and nothing else.
    report = _sweep(tmp_path)
    assert report.issues == [
        InterruptedWrite(pending[0].blob_id, "object", BUCKET, KEY, upload_id, None, True)
    ]
    assert [r.state for r in _store(tmp_path).list_blob_records()] == ["live"] * 4
    assert BlobStore(tmp_path / "blobs").temp_entries() == []

    with _restarted(tmp_path) as client2:
        got = client2.get(URL)
        assert got.content == OLD and got.headers["etag"] == old_etag
        # (c) The same request is refused now, changing nothing, and the upload is
        # still open; a retry whose condition holds works, with no part re-uploaded.
        refused = _complete(client2, upload_id, etags, condition)
        assert refused.status_code == 412
        assert client2.get(URL).content == OLD
        assert _store(tmp_path).upload_exists(upload_id)
        assert _complete(client2, upload_id, etags, {"If-Match": old_etag}).status_code == 200
        got = client2.get(URL)
        assert got.content == NEW and got.headers["etag"] == NEW_ETAG
    assert _sweep(tmp_path).issues == []


def test_complete_killed_after_commit_with_if_match_shows_the_new_object(tmp_path: Path):
    old, old_etag, upload_id, etags = _staged(tmp_path)
    parts = _store(tmp_path).get_parts(upload_id)

    server, client = _start(tmp_path, crash_point=COMPLETE_AFTER_COMMIT)
    _request_into_crash(server, lambda: _complete(client, upload_id, etags, {"If-Match": old_etag}))
    client.close()

    # (a) The commit stands: the row names the assembled blob, the upload is closed.
    store = _store(tmp_path)
    row = store.get_object_meta(BUCKET, KEY)
    assert row.blob_id != old.blob_id and (row.size, row.etag) == (len(NEW), NEW_ETAG)
    assert not store.upload_exists(upload_id)

    # (b) Only cleanup is left: the old object's blob and the three part blobs.
    report = _sweep(tmp_path)
    assert sorted(report.issues, key=repr) == sorted(
        [
            InterruptedCleanup(old.blob_id, "object", BUCKET, KEY, None, None),
            *(
                InterruptedCleanup(p.blob_id, "part", BUCKET, KEY, upload_id, p.part_number)
                for p in parts
            ),
        ],
        key=repr,
    )

    with _restarted(tmp_path) as client2:
        got = client2.get(URL)
        assert got.content == NEW and got.headers["etag"] == NEW_ETAG
        # (c) The client's retry of the same request is what a retried Complete gets
        # today, whatever its condition: the upload is closed, so NoSuchUpload (S3 would
        # answer 200 and the same ETag: gap j3, test_gap_retried_complete_returns_same_etag).
        # Nothing changes.
        before = (_store(tmp_path).list_objects(BUCKET), _store(tmp_path).get_parts(upload_id))
        retry = _complete(client2, upload_id, etags, {"If-Match": old_etag})
        assert retry.status_code == 404
        assert ET.fromstring(retry.text).findtext("Code") == "NoSuchUpload"
        assert client2.get(URL).headers["etag"] == NEW_ETAG
        assert (
            _store(tmp_path).list_objects(BUCKET),
            _store(tmp_path).get_parts(upload_id),
        ) == before
    assert _sweep(tmp_path).issues == []
