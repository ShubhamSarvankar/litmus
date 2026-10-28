"""
Tier B chaos: SIGKILL at exact steps of the write path, via S3_TEST_CRASH_POINT.

Each test arms one crash point (server/crash_points.py), drives a request into it,
confirms the server died by SIGKILL, restarts it unarmed on the same data, and then
asserts the exact state left behind: what clients see, what is on disk, what is in
metadata, and what the consistency sweep reports. It then checks that the client's
natural retry recovers.

The per-point tests assert only correct behavior. The two test_gap_* tests were
written as strict xfails while the consistency sweep could not see orphaned blobs;
the blob registry closed both gaps and they now run as ordinary tests. Their setup
problems use pytest.fail, so a broken setup is a real failure, never a pass.

The tests after the "write paths" marker follow docs/design/storage-layout.md §15.
Each asserts four things: (a) the raw state while the server is down, (b) the sweep
report, (c) what clients see after the restart, and (d) that the client's natural
retry succeeds. Where (c) would consume state that (d) needs (completing or aborting
the upload), the dead server's directory is copied first and (d) runs on the copy,
so both start from the same crash state.
"""

import base64
import hashlib
import os
import shutil
import signal
import xml.etree.ElementTree as ET
import zlib
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest

from chaos.fixtures.subprocess import ServerProcess, _find_free_port
from chaos.framework.framed import framed_request
from server.consistency import (
    InterruptedCleanup,
    InterruptedWrite,
    LeftoverTemp,
    MissingPart,
    OrphanedMetadata,
    run_consistency_sweep,
)
from server.crash_points import (
    ABORT_AFTER_COMMIT,
    ABORT_BEFORE_COMMIT,
    BUCKET_DELETE_AFTER_COMMIT,
    COMPLETE_AFTER_ASSEMBLE,
    COMPLETE_AFTER_COMMIT,
    COMPLETE_AFTER_GC_UNLINK,
    COMPLETE_AFTER_REGISTER,
    COMPLETE_MID_ASSEMBLE,
    DELETE_AFTER_COMMIT,
    DELETE_BEFORE_COMMIT,
    DELETE_OBJECTS_AFTER_COMMIT,
    PART_AFTER_BYTES,
    PART_AFTER_COMMIT,
    PART_AFTER_REGISTER,
    PART_MID_WRITE,
    PUT_AFTER_BYTES,
    PUT_AFTER_COMMIT,
    PUT_AFTER_GC_UNLINK,
    PUT_AFTER_REGISTER,
    PUT_MID_WRITE,
    STORE_INIT_AFTER_COMMIT,
    SWEEP_AFTER_UNLINK,
)
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SIGKILL not available on Windows")

BUCKET = "crash-bucket"
KEY = "obj.bin"


# --- helpers ---


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def _multipart_etag(parts: list[bytes]) -> str:
    digests = b"".join(hashlib.md5(p).digest() for p in parts)
    return f'"{hashlib.md5(digests).hexdigest()}-{len(parts)}"'


def _complete_xml(etags: list[str]) -> bytes:
    root = ET.Element("CompleteMultipartUpload")
    for num, etag in enumerate(etags, start=1):
        part = ET.SubElement(root, "Part")
        ET.SubElement(part, "PartNumber").text = str(num)
        ET.SubElement(part, "ETag").text = etag
    return ET.tostring(root)


def _start(tmp_path: Path, crash_point: str | None = None) -> tuple[ServerProcess, httpx.Client]:
    port = _find_free_port()
    server = ServerProcess(
        data_dir=tmp_path, db_path=tmp_path / "meta.db", port=port, crash_point=crash_point
    )
    server.start()
    server.wait_ready()
    return server, httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10.0)


def _request_into_crash(server: ServerProcess, send) -> None:
    """Send the request that reaches the armed crash point; assert the server died
    by SIGKILL rather than answering."""
    with pytest.raises(httpx.TransportError):
        send()
    assert server.wait_for_exit() == -signal.SIGKILL


def _blobs(tmp_path: Path) -> BlobStore:
    return BlobStore(tmp_path / "blobs")


def _blob_files(tmp_path: Path) -> set[str]:
    """IDs of the blob files on disk."""
    return {blob_id for _, blob_id in _blobs(tmp_path).scan()}


def _temp_files(tmp_path: Path) -> list[Path]:
    return _blobs(tmp_path).temp_entries()


def _registry(tmp_path: Path, state: str | None = None, **match):
    """Registry rows, optionally filtered by state and by field values."""
    return [
        r
        for r in _store(tmp_path).list_blob_records()
        if (state is None or r.state == state) and all(getattr(r, k) == v for k, v in match.items())
    ]


def _pending_files(tmp_path: Path, bucket: str, key: str) -> list[Path]:
    """Files of pending object blobs registered for bucket/key."""
    blobs = _blobs(tmp_path)
    return [
        blobs.blob_path(r.blob_id)
        for r in _registry(tmp_path, "pending", kind="object", bucket=bucket, key=key)
        if blobs.blob_path(r.blob_id).is_file()
    ]


def _garbage_part_files(tmp_path: Path, upload_id: str) -> list[Path]:
    """Files of garbage part blobs registered for upload_id."""
    blobs = _blobs(tmp_path)
    return [
        blobs.blob_path(r.blob_id)
        for r in _registry(tmp_path, "garbage", kind="part", upload_id=upload_id)
        if blobs.blob_path(r.blob_id).is_file()
    ]


def _store(tmp_path: Path) -> SQLiteMetadataStore:
    return SQLiteMetadataStore(db_path=tmp_path / "meta.db")


def _sweep(tmp_path: Path):
    return run_consistency_sweep(_store(tmp_path), _blobs(tmp_path))


def _complete_into_crash(server: ServerProcess, client: httpx.Client, upload_id: str, etags):
    _request_into_crash(
        server,
        lambda: client.post(f"/{BUCKET}/{KEY}?uploadId={upload_id}", content=_complete_xml(etags)),
    )


def _prepare_upload(client: httpx.Client) -> tuple[str, list[bytes], list[str]]:
    assert client.put(f"/{BUCKET}").status_code == 200
    resp = client.post(f"/{BUCKET}/{KEY}?uploads")
    upload_id = ET.fromstring(resp.text).findtext("UploadId")
    parts = [bytes([n]) * (256 * 1024) for n in (1, 2, 3)]
    etags = []
    for num, data in enumerate(parts, start=1):
        r = client.put(f"/{BUCKET}/{KEY}?partNumber={num}&uploadId={upload_id}", content=data)
        assert r.status_code == 200
        etags.append(r.headers["etag"])
    return upload_id, parts, etags


@pytest.fixture(params=[pytest.param(False, id="plain"), pytest.param(True, id="framed")])
def framed(request) -> bool:
    """Whether the request under test is sent as an aws-chunked body."""
    return request.param


@pytest.fixture(params=[pytest.param(False, id="no-claims"), pytest.param(True, id="claims")])
def claimed(request) -> bool:
    """Whether the request under test carries checksum claims, which are verified
    before the blob is made durable (server/checksums.py)."""
    return request.param


def _claims(data: bytes, framed: bool) -> dict[str, str]:
    """Headers claiming the digests of data. A framed body carries its checksum in the
    trailer, so it gets Content-MD5 and the SDK header for that trailer; a plain body
    also gets an x-amz-checksum-crc32 value."""
    claims = {"content-md5": base64.b64encode(hashlib.md5(data).digest()).decode()}
    if framed:
        claims["x-amz-sdk-checksum-algorithm"] = "CRC64NVME"
    else:
        crc32 = base64.b64encode(zlib.crc32(data).to_bytes(4, "big")).decode()
        claims["x-amz-checksum-crc32"] = crc32
        claims["x-amz-sdk-checksum-algorithm"] = "CRC32"
    return claims


def _put(
    client: httpx.Client, url: str, data: bytes, framed: bool = False, claimed: bool = False
) -> httpx.Response:
    extra = _claims(data, framed) if claimed else {}
    if framed:
        headers, body = framed_request(data)
        return client.put(url, content=body, headers=headers | extra)
    return client.put(url, content=data, headers=extra)


# --- tests ---


def test_unknown_crash_point_refuses_to_start(tmp_path: Path):
    """A misspelled crash point must not silently disarm a test."""
    server = ServerProcess(
        data_dir=tmp_path,
        db_path=tmp_path / "meta.db",
        port=_find_free_port(),
        crash_point="put-after-bytez",
    )
    server.start()
    returncode = server.wait_for_exit()
    assert returncode not in (0, -signal.SIGKILL)
    assert "not a known crash point" in server.collect_logs()


def test_put_killed_after_bytes_before_metadata(tmp_path: Path, framed):
    """PUT bytes durable at a fresh blob path, metadata not committed.

    Expected: the object is not visible; its complete bytes are on disk as a pending
    blob until the restart's sweep removes it; a retried PUT succeeds.
    """
    body = os.urandom(1024 * 1024)
    server, client = _start(tmp_path, crash_point=PUT_AFTER_BYTES)
    assert client.put(f"/{BUCKET}").status_code == 200
    _request_into_crash(server, lambda: _put(client, f"/{BUCKET}/{KEY}", body, framed))
    client.close()

    # While the server is down: no metadata row; the complete bytes are at the
    # pending blob's path; no temp file left.
    assert _store(tmp_path).get_object_meta(BUCKET, KEY) is None
    (pending,) = _pending_files(tmp_path, BUCKET, KEY)
    assert pending.read_bytes() == body
    assert _temp_files(tmp_path) == []

    # (b) The sweep report, from a copy, so that the restart below runs its own sweep
    # on the original.
    report = _sweep(_twin(tmp_path, server, "sweep"))
    assert report.issues == [
        InterruptedWrite(pending.name, "object", BUCKET, KEY, None, None, bytes_durable=True)
    ]

    server2, client2 = _start(tmp_path)
    try:
        # Not visible to clients.
        assert client2.get(f"/{BUCKET}/{KEY}").status_code == 404
        assert client2.head(f"/{BUCKET}/{KEY}").status_code == 404
        assert "<Key>" not in client2.get(f"/{BUCKET}").text

        # The restart's sweep removed the pending blob and its row.
        assert _store(tmp_path).get_object_meta(BUCKET, KEY) is None
        assert _blob_files(tmp_path) == set()
        assert _registry(tmp_path) == []

        # The client's retry succeeds and the object becomes visible.
        assert _put(client2, f"/{BUCKET}/{KEY}", body, framed).status_code == 200
        resp = client2.get(f"/{BUCKET}/{KEY}")
        assert resp.status_code == 200
        assert resp.content == body
        assert resp.headers["etag"] == _md5(body)
    finally:
        client2.close()
        server2.stop()


def test_complete_killed_after_assemble_before_metadata(tmp_path: Path):
    """Complete assembled the final file, object metadata not committed.

    Expected: the object is not visible; the assembled bytes are on disk as a
    pending blob until the restart's sweep removes it; the upload is still open with
    all parts recorded and on disk; a retried Complete succeeds.
    """
    server, client = _start(tmp_path, crash_point=COMPLETE_AFTER_ASSEMBLE)
    upload_id, parts, etags = _prepare_upload(client)
    body, etag = b"".join(parts), _multipart_etag(parts)
    _complete_into_crash(server, client, upload_id, etags)
    client.close()

    # While the server is down: the assembled blob is complete on disk; no temp
    # file left.
    (pending,) = _pending_files(tmp_path, BUCKET, KEY)
    assert pending.read_bytes() == body
    assert _temp_files(tmp_path) == []

    # (b) The sweep report, from a copy, so that the restart below runs its own sweep
    # on the original.
    report = _sweep(_twin(tmp_path, server, "sweep"))
    assert report.issues == [
        InterruptedWrite(pending.name, "object", BUCKET, KEY, upload_id, None, bytes_durable=True)
    ]

    server2, client2 = _start(tmp_path)
    try:
        # Not visible to clients.
        assert client2.get(f"/{BUCKET}/{KEY}").status_code == 404
        assert _store(tmp_path).get_object_meta(BUCKET, KEY) is None

        # The restart's sweep removed the assembled blob.
        assert _registry(tmp_path, "pending") == []
        assert not pending.exists()

        # Upload still open, every part recorded and on disk.
        store = _store(tmp_path)
        assert store.upload_exists(upload_id, bucket=BUCKET, key=KEY)
        recorded = store.get_parts(upload_id)
        assert [p.part_number for p in recorded] == [1, 2, 3]
        assert {p.blob_id for p in recorded} == _blob_files(tmp_path)

        # The client's retry completes the upload.
        resp = client2.post(f"/{BUCKET}/{KEY}?uploadId={upload_id}", content=_complete_xml(etags))
        assert resp.status_code == 200
        assert ET.fromstring(resp.text).findtext("ETag") == etag
        got = client2.get(f"/{BUCKET}/{KEY}")
        assert got.content == body
        assert got.headers["etag"] == etag
        assert not _store(tmp_path).upload_exists(upload_id, bucket=BUCKET, key=KEY)
        # The part blobs were unlinked after the commit; only the object's blob is left.
        assert _blob_files(tmp_path) == {_store(tmp_path).get_object_meta(BUCKET, KEY).blob_id}
        assert _registry(tmp_path, kind="part") == []
    finally:
        client2.close()
        server2.stop()


def test_complete_killed_after_commit_before_part_cleanup(tmp_path: Path):
    """Complete committed the object metadata and the upload completion in one
    transaction; part files not yet deleted.

    Expected: the object is visible with the full body and composite ETag; the
    upload is closed; its part files are still on disk while the server is down,
    because cleanup runs after the commit, and the restart's sweep removes them.
    """
    server, client = _start(tmp_path, crash_point=COMPLETE_AFTER_COMMIT)
    upload_id, parts, etags = _prepare_upload(client)
    body, etag = b"".join(parts), _multipart_etag(parts)
    _complete_into_crash(server, client, upload_id, etags)
    client.close()

    # While the server is down: the three part files remain, as garbage blobs.
    leftovers = _garbage_part_files(tmp_path, upload_id)
    assert len(leftovers) == 3

    # (b) The sweep report, from a copy, so that the restart below runs its own sweep
    # on the original.
    report = _sweep(_twin(tmp_path, server, "sweep"))
    garbage_parts = sorted(_registry(tmp_path, "garbage"), key=lambda r: r.part_number)
    assert sorted(report.issues, key=lambda i: i.part_number) == [
        InterruptedCleanup(r.blob_id, "part", BUCKET, KEY, upload_id, r.part_number)
        for r in garbage_parts
    ]
    assert [r.part_number for r in garbage_parts] == [1, 2, 3]

    server2, client2 = _start(tmp_path)
    try:
        # Visible with exactly the right bytes and ETag.
        resp = client2.get(f"/{BUCKET}/{KEY}")
        assert resp.status_code == 200
        assert resp.content == body
        assert resp.headers["etag"] == etag
        store = _store(tmp_path)
        meta = store.get_object_meta(BUCKET, KEY)
        assert (meta.size, meta.etag) == (len(body), etag)

        # Upload closed; the assembled blob is the only blob file left: the
        # restart's sweep removed the part files the kill left behind.
        assert not store.upload_exists(upload_id, bucket=BUCKET, key=KEY)
        assert all(u.upload_id != upload_id for u in store.list_incomplete_uploads())
        assert _blob_files(tmp_path) == {meta.blob_id}
        assert not any(p.exists() for p in leftovers)
        assert _registry(tmp_path, "garbage") == []

        # (d) A retried Complete changes nothing. S3 answers it with 200 and the same
        # ETag; see test_gap_retried_complete_returns_same_etag (j3).
        before = _snapshot(tmp_path, upload_id)
        _complete(client2, upload_id, etags)
        assert _snapshot(tmp_path, upload_id) == before
    finally:
        client2.close()
        server2.stop()


def test_no_open_upload_once_object_committed(tmp_path: Path):
    """A kill anywhere after the Complete commit must not leave the upload open
    while its object is committed. The commit is one transaction, so the last
    post-commit crash point sees both or neither."""
    server, client = _start(tmp_path, crash_point=COMPLETE_AFTER_COMMIT)
    upload_id, _parts, etags = _prepare_upload(client)
    _complete_into_crash(server, client, upload_id, etags)
    client.close()

    store = _store(tmp_path)
    assert store.get_object_meta(BUCKET, KEY) is not None
    assert not store.upload_exists(upload_id, bucket=BUCKET, key=KEY), (
        "object is committed but its multipart upload is still open"
    )


# --- formerly known gaps, closed by the blob registry: the sweep reports leftovers ---


def test_gap_sweep_reports_orphaned_file_after_put_crash(tmp_path: Path):
    body = os.urandom(64 * 1024)
    server, client = _start(tmp_path, crash_point=PUT_AFTER_BYTES)
    if client.put(f"/{BUCKET}").status_code != 200:
        pytest.fail("setup: bucket creation failed")
    try:
        _request_into_crash(server, lambda: client.put(f"/{BUCKET}/{KEY}", content=body))
    except AssertionError as e:
        pytest.fail(f"setup: server did not die at the crash point: {e}")
    client.close()
    if _store(tmp_path).get_object_meta(BUCKET, KEY) is not None:
        pytest.fail("setup: metadata was committed, so the file is not orphaned")
    if not _pending_files(tmp_path, BUCKET, KEY):
        pytest.fail("setup: no file on disk, so nothing is orphaned")

    report = _sweep(tmp_path)
    reported = [
        i
        for i in report.issues
        if not isinstance(i, OrphanedMetadata)
        and getattr(i, "bucket", None) == BUCKET
        and getattr(i, "key", None) == KEY
    ]
    assert reported, f"sweep did not report the orphaned file; issues: {report.issues}"


def test_gap_sweep_reports_part_files_of_completed_upload(tmp_path: Path):
    server, client = _start(tmp_path, crash_point=COMPLETE_AFTER_COMMIT)
    upload_id, _parts, etags = _prepare_upload(client)
    try:
        _complete_into_crash(server, client, upload_id, etags)
    except AssertionError as e:
        pytest.fail(f"setup: server did not die at the crash point: {e}")
    client.close()
    store = _store(tmp_path)
    if store.upload_exists(upload_id, bucket=BUCKET, key=KEY):
        pytest.fail("setup: upload still open, so its parts are not leftovers")
    if not _garbage_part_files(tmp_path, upload_id):
        pytest.fail("setup: no part files on disk, so nothing is left over")

    report = _sweep(tmp_path)
    reported = [
        i
        for i in report.issues
        if not isinstance(i, MissingPart) and getattr(i, "upload_id", None) == upload_id
    ]
    assert reported, f"sweep did not report the leftover part files; issues: {report.issues}"


# --- write paths: docs/design/storage-layout.md §15 ---

OLD = b"old body. " * 3000
NEW = b"new body! " * 4000


def _setup_unarmed(tmp_path: Path, setup):
    """Run setup(client) on an unarmed server, so setup requests cannot reach the
    armed point, then stop it."""
    server, client = _start(tmp_path)
    try:
        return setup(client)
    finally:
        client.close()
        server.stop()


def _twin(tmp_path: Path, dead: ServerProcess, name: str = "twin") -> Path:
    """Copy the dead server's directory: two next steps from one crash state.

    Taken only after the server process has exited. Every file is copied, the
    database's -wal and -shm files included whenever they exist, and the copy is
    checked to hold the same files with byte-identical database files.
    """
    assert dead.wait_for_exit() is not None, "the server must have exited before the copy"
    twin = tmp_path.parent / f"{tmp_path.name}-{name}"
    shutil.copytree(tmp_path, twin)

    def files(root: Path) -> set[Path]:
        return {p.relative_to(root) for p in root.rglob("*") if p.is_file()}

    assert files(twin) == files(tmp_path)
    for db_file in tmp_path.glob("meta.db*"):
        assert (twin / db_file.name).read_bytes() == db_file.read_bytes()
    return twin


def _snapshot(tmp_path: Path, upload_id: str | None = None):
    """Everything a retry must leave unchanged: buckets, objects, open uploads, the
    given upload and its parts, registry rows, blob bytes and temp files."""
    store = _store(tmp_path)
    buckets = store.list_buckets()
    return (
        buckets,
        [store.list_objects(b.name) for b in buckets],
        store.list_incomplete_uploads(),
        store.get_upload(upload_id, bucket=BUCKET, key=KEY) if upload_id else None,
        store.get_parts(upload_id) if upload_id else None,
        store.list_blob_records(),
        {blob_id: _blob_bytes(tmp_path, blob_id) for blob_id in _blob_files(tmp_path)},
        _temp_files(tmp_path),
    )


@contextmanager
def _restarted(tmp_path: Path):
    """An unarmed server on tmp_path and a client for it."""
    server, client = _start(tmp_path)
    try:
        yield client
    finally:
        client.close()
        server.stop()


def _put_old(client) -> None:
    assert client.put(f"/{BUCKET}").status_code == 200
    assert client.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200


def _open_upload(client, parts: list[bytes]) -> tuple[str, list[str]]:
    """Create an upload in an existing bucket and upload parts; (upload_id, etags)."""
    upload_id = ET.fromstring(client.post(f"/{BUCKET}/{KEY}?uploads").text).findtext("UploadId")
    etags = []
    for num, data in enumerate(parts, start=1):
        r = client.put(f"/{BUCKET}/{KEY}?partNumber={num}&uploadId={upload_id}", content=data)
        assert r.status_code == 200
        etags.append(r.headers["etag"])
    return upload_id, etags


def _complete(client, upload_id: str, etags: list[str]):
    return client.post(f"/{BUCKET}/{KEY}?uploadId={upload_id}", content=_complete_xml(etags))


def _assert_object(client, body: bytes, etag: str, key: str = KEY) -> None:
    resp = client.get(f"/{BUCKET}/{key}")
    assert resp.status_code == 200
    assert resp.content == body
    assert resp.headers["etag"] == etag


def _store_parts(tmp_path: Path, upload_id: str):
    return _store(tmp_path).get_parts(upload_id)


def _blob_bytes(tmp_path: Path, blob_id: str) -> bytes | None:
    path = _blobs(tmp_path).blob_path(blob_id)
    return path.read_bytes() if path.exists() else None


# PutObject


def test_put_killed_after_register(tmp_path: Path, framed):
    server, client = _start(tmp_path, crash_point=PUT_AFTER_REGISTER)
    assert client.put(f"/{BUCKET}").status_code == 200
    _request_into_crash(server, lambda: _put(client, f"/{BUCKET}/{KEY}", NEW, framed))
    client.close()

    # (a) A registered ID with no bytes.
    assert _store(tmp_path).get_object_meta(BUCKET, KEY) is None
    (row,) = _registry(tmp_path)
    assert (row.state, row.kind, row.bucket, row.key) == ("pending", "object", BUCKET, KEY)
    assert _blob_files(tmp_path) == set()
    assert _temp_files(tmp_path) == []

    # (b)
    report = _sweep(tmp_path)
    assert report.issues == [
        InterruptedWrite(row.blob_id, "object", BUCKET, KEY, None, None, bytes_durable=False)
    ]
    assert _registry(tmp_path) == []

    with _restarted(tmp_path) as client2:
        # (c)
        assert client2.get(f"/{BUCKET}/{KEY}").status_code == 404
        # (d)
        assert _put(client2, f"/{BUCKET}/{KEY}", NEW, framed).status_code == 200
        _assert_object(client2, NEW, _md5(NEW))


def test_put_killed_mid_write(tmp_path: Path, framed, claimed):
    body = os.urandom(200 * 1024)
    server, client = _start(tmp_path, crash_point=PUT_MID_WRITE)
    assert client.put(f"/{BUCKET}").status_code == 200
    _request_into_crash(server, lambda: _put(client, f"/{BUCKET}/{KEY}", body, framed, claimed))
    client.close()

    # (a) A partial temp file and no blob.
    (row,) = _registry(tmp_path, "pending")
    temp = _blobs(tmp_path).temp_path(row.blob_id)
    assert _temp_files(tmp_path) == [temp]
    if framed:
        # The decoder writes as each buffer arrives, not after the whole chunk: the body
        # is one chunk of 200 KiB, and the temp file holds the decoded start of it, at
        # most one write (64 KiB), not the decoded length.
        partial = temp.read_bytes()
        assert 0 < len(partial) <= 65536 < len(body) and body.startswith(partial)
    else:
        assert temp.read_bytes() == body[:65536]
    assert _blob_files(tmp_path) == set()
    assert _store(tmp_path).get_object_meta(BUCKET, KEY) is None

    # (b)
    report = _sweep(tmp_path)
    assert report.issues == [
        LeftoverTemp(temp),
        InterruptedWrite(row.blob_id, "object", BUCKET, KEY, None, None, bytes_durable=False),
    ]
    assert _temp_files(tmp_path) == [] and _registry(tmp_path) == []

    with _restarted(tmp_path) as client2:
        assert client2.get(f"/{BUCKET}/{KEY}").status_code == 404
        assert _put(client2, f"/{BUCKET}/{KEY}", body, framed, claimed).status_code == 200
        _assert_object(client2, body, _md5(body))


def test_overwrite_killed_after_bytes_keeps_old_object(tmp_path: Path, framed):
    """R2: new bytes durable at a fresh path, not committed; the old object's bytes
    and ETag survive."""
    _setup_unarmed(tmp_path, _put_old)
    old = _store(tmp_path).get_object_meta(BUCKET, KEY)
    server, client = _start(tmp_path, crash_point=PUT_AFTER_BYTES)
    _request_into_crash(server, lambda: _put(client, f"/{BUCKET}/{KEY}", NEW, framed))
    client.close()

    # (a) The row and the old blob are untouched; the new blob is pending and complete.
    assert _store(tmp_path).get_object_meta(BUCKET, KEY) == old
    assert _blob_bytes(tmp_path, old.blob_id) == OLD
    (pending,) = _registry(tmp_path, "pending")
    assert _blob_bytes(tmp_path, pending.blob_id) == NEW

    # (b)
    report = _sweep(tmp_path)
    assert report.issues == [
        InterruptedWrite(pending.blob_id, "object", BUCKET, KEY, None, None, bytes_durable=True)
    ]
    assert _blob_files(tmp_path) == {old.blob_id}
    assert _blob_bytes(tmp_path, old.blob_id) == OLD

    with _restarted(tmp_path) as client2:
        # (c) Exactly the old object.
        _assert_object(client2, OLD, _md5(OLD))
        # (d)
        assert _put(client2, f"/{BUCKET}/{KEY}", NEW, framed).status_code == 200
        _assert_object(client2, NEW, _md5(NEW))
        current = _store(tmp_path).get_object_meta(BUCKET, KEY).blob_id
        assert _blob_files(tmp_path) == {current}


def test_overwrite_killed_after_commit(tmp_path: Path, framed):
    _setup_unarmed(tmp_path, _put_old)
    old = _store(tmp_path).get_object_meta(BUCKET, KEY)
    server, client = _start(tmp_path, crash_point=PUT_AFTER_COMMIT)
    _request_into_crash(server, lambda: _put(client, f"/{BUCKET}/{KEY}", NEW, framed))
    client.close()

    # (a) The row names the new live blob; the old blob is garbage, its file present.
    new = _store(tmp_path).get_object_meta(BUCKET, KEY)
    assert (new.etag, new.size) == (_md5(NEW), len(NEW)) and new.blob_id != old.blob_id
    assert _registry(tmp_path, "live") == _registry(tmp_path, blob_id=new.blob_id)
    assert [r.blob_id for r in _registry(tmp_path, "garbage")] == [old.blob_id]
    assert _blob_bytes(tmp_path, old.blob_id) == OLD
    assert _blob_bytes(tmp_path, new.blob_id) == NEW

    # (b)
    report = _sweep(tmp_path)
    assert report.issues == [InterruptedCleanup(old.blob_id, "object", BUCKET, KEY, None, None)]
    assert _blob_files(tmp_path) == {new.blob_id}
    assert [r.blob_id for r in _registry(tmp_path)] == [new.blob_id]

    with _restarted(tmp_path) as client2:
        _assert_object(client2, NEW, _md5(NEW))
        assert _put(client2, f"/{BUCKET}/{KEY}", NEW, framed).status_code == 200
        _assert_object(client2, NEW, _md5(NEW))
        current = _store(tmp_path).get_object_meta(BUCKET, KEY).blob_id
        assert _blob_files(tmp_path) == {current}


def test_overwrite_killed_after_gc_unlink(tmp_path: Path, framed):
    _setup_unarmed(tmp_path, _put_old)
    old = _store(tmp_path).get_object_meta(BUCKET, KEY)
    server, client = _start(tmp_path, crash_point=PUT_AFTER_GC_UNLINK)
    _request_into_crash(server, lambda: _put(client, f"/{BUCKET}/{KEY}", NEW, framed))
    client.close()

    # (a) The old blob's file is unlinked; its garbage row is still there.
    new = _store(tmp_path).get_object_meta(BUCKET, KEY)
    assert [r.blob_id for r in _registry(tmp_path, "garbage")] == [old.blob_id]
    assert _blob_files(tmp_path) == {new.blob_id}

    # (b) A garbage row without a file is purged silently.
    report = _sweep(tmp_path)
    assert report.clean
    assert [r.blob_id for r in _registry(tmp_path)] == [new.blob_id]

    with _restarted(tmp_path) as client2:
        _assert_object(client2, NEW, _md5(NEW))
        assert _put(client2, f"/{BUCKET}/{KEY}", NEW, framed).status_code == 200
        _assert_object(client2, NEW, _md5(NEW))


# DeleteObject and DeleteObjects


def test_delete_killed_before_commit_keeps_object(tmp_path: Path):
    """R3: a kill before the delete's commit leaves the object and its bytes."""
    _setup_unarmed(tmp_path, _put_old)
    old = _store(tmp_path).get_object_meta(BUCKET, KEY)
    server, client = _start(tmp_path, crash_point=DELETE_BEFORE_COMMIT)
    _request_into_crash(server, lambda: client.delete(f"/{BUCKET}/{KEY}"))
    client.close()

    # (a)
    assert _store(tmp_path).get_object_meta(BUCKET, KEY) == old
    assert _registry(tmp_path, "live") == _registry(tmp_path)
    assert _blob_bytes(tmp_path, old.blob_id) == OLD

    # (b)
    report = _sweep(tmp_path)
    assert report.clean
    assert _blob_bytes(tmp_path, old.blob_id) == OLD

    with _restarted(tmp_path) as client2:
        # (c)
        _assert_object(client2, OLD, _md5(OLD))
        # (d)
        assert client2.delete(f"/{BUCKET}/{KEY}").status_code == 204
        assert client2.get(f"/{BUCKET}/{KEY}").status_code == 404
        assert _blob_files(tmp_path) == set() and _registry(tmp_path) == []


def test_delete_killed_after_commit(tmp_path: Path):
    _setup_unarmed(tmp_path, _put_old)
    old = _store(tmp_path).get_object_meta(BUCKET, KEY)
    server, client = _start(tmp_path, crash_point=DELETE_AFTER_COMMIT)
    _request_into_crash(server, lambda: client.delete(f"/{BUCKET}/{KEY}"))
    client.close()

    # (a) The row is gone; the blob is garbage with its file present.
    assert _store(tmp_path).get_object_meta(BUCKET, KEY) is None
    assert [r.blob_id for r in _registry(tmp_path, "garbage")] == [old.blob_id]
    assert _blob_bytes(tmp_path, old.blob_id) == OLD

    # (b) Never a row without a file: no OrphanedMetadata.
    report = _sweep(tmp_path)
    assert report.issues == [InterruptedCleanup(old.blob_id, "object", BUCKET, KEY, None, None)]
    assert _blob_files(tmp_path) == set() and _registry(tmp_path) == []

    with _restarted(tmp_path) as client2:
        assert client2.get(f"/{BUCKET}/{KEY}").status_code == 404
        assert client2.delete(f"/{BUCKET}/{KEY}").status_code == 204
        assert client2.get(f"/{BUCKET}/{KEY}").status_code == 404


def test_delete_objects_killed_after_commit(tmp_path: Path):
    bodies = {"k1": b"one" * 1000, "k2": b"two" * 1000, "k3": b"three" * 1000}

    def setup(client):
        assert client.put(f"/{BUCKET}").status_code == 200
        for key, body in bodies.items():
            assert client.put(f"/{BUCKET}/{key}", content=body).status_code == 200

    _setup_unarmed(tmp_path, setup)
    store = _store(tmp_path)
    rows = {key: store.get_object_meta(BUCKET, key) for key in bodies}
    # k3 carries a version ID Litmus does not implement: a per-key error, kept.
    request = (
        b"<Delete><Object><Key>k1</Key></Object><Object><Key>k2</Key></Object>"
        b"<Object><Key>k3</Key><VersionId>v1</VersionId></Object></Delete>"
    )
    server, client = _start(tmp_path, crash_point=DELETE_OBJECTS_AFTER_COMMIT)
    _request_into_crash(server, lambda: client.post(f"/{BUCKET}?delete", content=request))
    client.close()

    # (a) k1 and k2 are gone in one commit, their blobs garbage with files present.
    store = _store(tmp_path)
    assert store.get_object_meta(BUCKET, "k1") is None
    assert store.get_object_meta(BUCKET, "k2") is None
    assert store.get_object_meta(BUCKET, "k3") == rows["k3"]
    garbage = {rows["k1"].blob_id, rows["k2"].blob_id}
    assert {r.blob_id for r in _registry(tmp_path, "garbage")} == garbage
    assert _blob_files(tmp_path) == garbage | {rows["k3"].blob_id}

    # (b)
    report = _sweep(tmp_path)
    assert sorted(report.issues, key=lambda i: i.key) == [
        InterruptedCleanup(rows[k].blob_id, "object", BUCKET, k, None, None) for k in ("k1", "k2")
    ]
    assert _blob_files(tmp_path) == {rows["k3"].blob_id}

    with _restarted(tmp_path) as client2:
        # (c)
        assert client2.get(f"/{BUCKET}/k1").status_code == 404
        assert client2.get(f"/{BUCKET}/k2").status_code == 404
        _assert_object(client2, bodies["k3"], _md5(bodies["k3"]), key="k3")
        # (d)
        resp = client2.post(f"/{BUCKET}?delete", content=request)
        assert resp.status_code == 200
        root = ET.fromstring(resp.text)
        assert [d.findtext("Key") for d in root.findall("Deleted")] == ["k1", "k2"]
        assert [(e.findtext("Key"), e.findtext("Code")) for e in root.findall("Error")] == [
            ("k3", "NotImplemented")
        ]
        _assert_object(client2, bodies["k3"], _md5(bodies["k3"]), key="k3")


# UploadPart


def _part_killed_before_commit(
    tmp_path: Path, point: str, body: bytes, framed: bool = False, claimed: bool = False
):
    server, client = _start(tmp_path, crash_point=point)
    assert client.put(f"/{BUCKET}").status_code == 200
    upload_id, _ = _open_upload(client, [])
    url = f"/{BUCKET}/{KEY}?partNumber=1&uploadId={upload_id}"
    _request_into_crash(server, lambda: _put(client, url, body, framed, claimed))
    client.close()
    return upload_id, url


def _part_retry(
    tmp_path: Path,
    upload_id: str,
    url: str,
    body: bytes,
    framed: bool = False,
    claimed: bool = False,
) -> None:
    with _restarted(tmp_path) as client2:
        # (c) The part was never recorded: Complete with its ETag is refused, and the
        # upload stays open.
        resp = _complete(client2, upload_id, [_md5(body)])
        assert resp.status_code == 400
        assert ET.fromstring(resp.text).findtext("Code") == "InvalidPart"
        assert _store(tmp_path).upload_exists(upload_id, bucket=BUCKET, key=KEY)
        # (d)
        retry = _put(client2, url, body, framed, claimed)
        assert retry.status_code == 200
        assert _complete(client2, upload_id, [retry.headers["etag"]]).status_code == 200
        _assert_object(client2, body, _multipart_etag([body]))


def test_upload_part_killed_after_register(tmp_path: Path, framed):
    body = b"part body " * 1000
    upload_id, url = _part_killed_before_commit(tmp_path, PART_AFTER_REGISTER, body, framed)

    # (a)
    (row,) = _registry(tmp_path)
    assert (row.state, row.kind, row.upload_id, row.part_number) == (
        "pending",
        "part",
        upload_id,
        1,
    )
    assert _store(tmp_path).get_parts(upload_id) == []
    assert _store(tmp_path).upload_exists(upload_id, bucket=BUCKET, key=KEY)
    assert _blob_files(tmp_path) == set() and _temp_files(tmp_path) == []

    # (b)
    report = _sweep(tmp_path)
    assert report.issues == [
        InterruptedWrite(row.blob_id, "part", BUCKET, KEY, upload_id, 1, bytes_durable=False)
    ]
    assert _registry(tmp_path) == []

    _part_retry(tmp_path, upload_id, url, body, framed)


def test_upload_part_killed_mid_write(tmp_path: Path, framed, claimed):
    body = os.urandom(200 * 1024)
    upload_id, url = _part_killed_before_commit(tmp_path, PART_MID_WRITE, body, framed, claimed)

    # (a)
    (row,) = _registry(tmp_path, "pending")
    temp = _blobs(tmp_path).temp_path(row.blob_id)
    assert _temp_files(tmp_path) == [temp]
    if framed:
        # The decoder writes as each buffer arrives, not after the whole chunk: the body
        # is one chunk of 200 KiB, and the temp file holds the decoded start of it, at
        # most one write (64 KiB), not the decoded length.
        partial = temp.read_bytes()
        assert 0 < len(partial) <= 65536 < len(body) and body.startswith(partial)
    else:
        assert temp.read_bytes() == body[:65536]
    assert _blob_files(tmp_path) == set()
    assert _store(tmp_path).get_parts(upload_id) == []

    # (b)
    report = _sweep(tmp_path)
    assert report.issues == [
        LeftoverTemp(temp),
        InterruptedWrite(row.blob_id, "part", BUCKET, KEY, upload_id, 1, bytes_durable=False),
    ]
    assert _temp_files(tmp_path) == [] and _registry(tmp_path) == []

    _part_retry(tmp_path, upload_id, url, body, framed, claimed)


X = b"first attempt " * 1000
Y = b"second attempt " * 1000


def _upload_x(client) -> tuple[str, str]:
    assert client.put(f"/{BUCKET}").status_code == 200
    upload_id, (x_etag,) = _open_upload(client, [X])
    return upload_id, x_etag


def test_reupload_part_killed_after_bytes_keeps_old_part(tmp_path: Path, framed):
    upload_id, x_etag = _setup_unarmed(tmp_path, _upload_x)
    (x_part,) = _store(tmp_path).get_parts(upload_id)
    url = f"/{BUCKET}/{KEY}?partNumber=1&uploadId={upload_id}"
    server, client = _start(tmp_path, crash_point=PART_AFTER_BYTES)
    _request_into_crash(server, lambda: _put(client, url, Y, framed))
    client.close()

    # (a) Part 1 is still X, intact; Y is a pending, complete blob.
    assert _store(tmp_path).get_parts(upload_id) == [x_part]
    assert _blob_bytes(tmp_path, x_part.blob_id) == X
    (pending,) = _registry(tmp_path, "pending")
    assert _blob_bytes(tmp_path, pending.blob_id) == Y
    twin = _twin(tmp_path, server)

    # (b)
    report = _sweep(tmp_path)
    assert report.issues == [
        InterruptedWrite(pending.blob_id, "part", BUCKET, KEY, upload_id, 1, bytes_durable=True)
    ]
    assert _blob_files(tmp_path) == {x_part.blob_id}

    with _restarted(tmp_path) as client2:
        # (c) Y was never recorded; Complete with the old ETag gives the old bytes.
        refused = _complete(client2, upload_id, [_md5(Y)])
        assert ET.fromstring(refused.text).findtext("Code") == "InvalidPart"
        assert _complete(client2, upload_id, [x_etag]).status_code == 200
        _assert_object(client2, X, _multipart_etag([X]))

    with _restarted(twin) as client3:
        # (d) The client retries the re-upload, then completes with it.
        retry = _put(client3, url, Y, framed)
        assert retry.status_code == 200
        assert _complete(client3, upload_id, [retry.headers["etag"]]).status_code == 200
        _assert_object(client3, Y, _multipart_etag([Y]))


def test_reupload_part_killed_after_commit(tmp_path: Path, framed):
    upload_id, x_etag = _setup_unarmed(tmp_path, _upload_x)
    (x_part,) = _store(tmp_path).get_parts(upload_id)
    url = f"/{BUCKET}/{KEY}?partNumber=1&uploadId={upload_id}"
    server, client = _start(tmp_path, crash_point=PART_AFTER_COMMIT)
    _request_into_crash(server, lambda: _put(client, url, Y, framed))
    client.close()

    # (a) Part 1 is Y; X's blob is garbage with its file present.
    (y_part,) = _store(tmp_path).get_parts(upload_id)
    assert (y_part.etag, y_part.size) == (_md5(Y), len(Y))
    assert _blob_bytes(tmp_path, y_part.blob_id) == Y
    assert [r.blob_id for r in _registry(tmp_path, "garbage")] == [x_part.blob_id]
    assert _blob_bytes(tmp_path, x_part.blob_id) == X
    twin = _twin(tmp_path, server)

    # (b)
    report = _sweep(tmp_path)
    assert report.issues == [InterruptedCleanup(x_part.blob_id, "part", BUCKET, KEY, upload_id, 1)]
    assert _blob_files(tmp_path) == {y_part.blob_id}

    with _restarted(tmp_path) as client2:
        # (c) The new part is the recorded one.
        refused = _complete(client2, upload_id, [x_etag])
        assert ET.fromstring(refused.text).findtext("Code") == "InvalidPart"
        assert _complete(client2, upload_id, [_md5(Y)]).status_code == 200
        _assert_object(client2, Y, _multipart_etag([Y]))

    with _restarted(twin) as client3:
        # (d)
        retry = _put(client3, url, Y, framed)
        assert retry.status_code == 200
        assert _complete(client3, upload_id, [retry.headers["etag"]]).status_code == 200
        _assert_object(client3, Y, _multipart_etag([Y]))


# CompleteMultipartUpload


def _complete_retry(tmp_path: Path, upload_id: str, parts: list[bytes], etags: list[str]):
    body, etag = b"".join(parts), _multipart_etag(parts)
    with _restarted(tmp_path) as client2:
        # (c)
        assert client2.get(f"/{BUCKET}/{KEY}").status_code == 404
        assert _store(tmp_path).upload_exists(upload_id, bucket=BUCKET, key=KEY)
        # (d)
        resp = _complete(client2, upload_id, etags)
        assert resp.status_code == 200
        assert ET.fromstring(resp.text).findtext("ETag") == etag
        _assert_object(client2, body, etag)
        current = _store(tmp_path).get_object_meta(BUCKET, KEY).blob_id
        assert _blob_files(tmp_path) == {current}


def test_complete_killed_after_register(tmp_path: Path):
    server, client = _start(tmp_path, crash_point=COMPLETE_AFTER_REGISTER)
    upload_id, parts, etags = _prepare_upload(client)
    part_blobs = {p.blob_id for p in _store_parts(tmp_path, upload_id)}
    _complete_into_crash(server, client, upload_id, etags)
    client.close()

    # (a) A pending object blob with no bytes; the upload and its parts untouched.
    (row,) = _registry(tmp_path, "pending")
    assert (row.kind, row.key, row.upload_id) == ("object", KEY, upload_id)
    assert _blob_files(tmp_path) == part_blobs
    assert _temp_files(tmp_path) == []
    assert _store(tmp_path).upload_exists(upload_id, bucket=BUCKET, key=KEY)

    # (b)
    report = _sweep(tmp_path)
    assert report.issues == [
        InterruptedWrite(row.blob_id, "object", BUCKET, KEY, upload_id, None, bytes_durable=False)
    ]
    assert _registry(tmp_path, "pending") == []

    _complete_retry(tmp_path, upload_id, parts, etags)


def test_complete_killed_mid_assemble(tmp_path: Path):
    server, client = _start(tmp_path, crash_point=COMPLETE_MID_ASSEMBLE)
    upload_id, parts, etags = _prepare_upload(client)
    part_blobs = {p.blob_id for p in _store_parts(tmp_path, upload_id)}
    _complete_into_crash(server, client, upload_id, etags)
    client.close()

    # (a) The first part is in the temp file; the parts are intact.
    (row,) = _registry(tmp_path, "pending")
    temp = _blobs(tmp_path).temp_path(row.blob_id)
    assert _temp_files(tmp_path) == [temp]
    assert temp.read_bytes() == parts[0]
    assert _blob_files(tmp_path) == part_blobs
    assert _store(tmp_path).upload_exists(upload_id, bucket=BUCKET, key=KEY)

    # (b)
    report = _sweep(tmp_path)
    assert report.issues == [
        LeftoverTemp(temp),
        InterruptedWrite(row.blob_id, "object", BUCKET, KEY, upload_id, None, bytes_durable=False),
    ]
    assert _temp_files(tmp_path) == []

    _complete_retry(tmp_path, upload_id, parts, etags)


def test_complete_over_existing_killed_after_assemble_keeps_old_object(tmp_path: Path):
    """R2/R6: an existing object at the key keeps its bytes when Complete is killed
    after assembling into a fresh blob."""
    parts = [bytes([n]) * (256 * 1024) for n in (7, 8)]

    def setup(client):
        _put_old(client)
        return _open_upload(client, parts)

    upload_id, etags = _setup_unarmed(tmp_path, setup)
    old = _store(tmp_path).get_object_meta(BUCKET, KEY)
    server, client = _start(tmp_path, crash_point=COMPLETE_AFTER_ASSEMBLE)
    _complete_into_crash(server, client, upload_id, etags)
    client.close()

    # (a) The row and the old blob are untouched; the assembled blob is pending.
    assert _store(tmp_path).get_object_meta(BUCKET, KEY) == old
    assert _blob_bytes(tmp_path, old.blob_id) == OLD
    (pending,) = _registry(tmp_path, "pending")
    assert _blob_bytes(tmp_path, pending.blob_id) == b"".join(parts)
    assert _store(tmp_path).upload_exists(upload_id, bucket=BUCKET, key=KEY)

    # (b)
    report = _sweep(tmp_path)
    assert report.issues == [
        InterruptedWrite(
            pending.blob_id, "object", BUCKET, KEY, upload_id, None, bytes_durable=True
        )
    ]
    assert _blob_bytes(tmp_path, old.blob_id) == OLD

    with _restarted(tmp_path) as client2:
        # (c) Exactly the old object.
        _assert_object(client2, OLD, _md5(OLD))
        # (d)
        assert _complete(client2, upload_id, etags).status_code == 200
        _assert_object(client2, b"".join(parts), _multipart_etag(parts))
        current = _store(tmp_path).get_object_meta(BUCKET, KEY).blob_id
        assert _blob_files(tmp_path) == {current}


# AbortMultipartUpload


def test_abort_killed_before_commit_keeps_upload(tmp_path: Path):
    """R3: a kill before the abort's commit leaves the upload and its parts."""
    server, client = _start(tmp_path, crash_point=ABORT_BEFORE_COMMIT)
    upload_id, parts, etags = _prepare_upload(client)
    recorded = _store_parts(tmp_path, upload_id)
    _request_into_crash(server, lambda: client.delete(f"/{BUCKET}/{KEY}?uploadId={upload_id}"))
    client.close()

    # (a)
    assert _store(tmp_path).upload_exists(upload_id, bucket=BUCKET, key=KEY)
    assert _store_parts(tmp_path, upload_id) == recorded
    for part, data in zip(recorded, parts, strict=True):
        assert _blob_bytes(tmp_path, part.blob_id) == data
    twin = _twin(tmp_path, server)

    # (b)
    report = _sweep(tmp_path)
    assert report.clean

    with _restarted(tmp_path) as client2:
        # (c) The parts are intact and Complete still works.
        assert _complete(client2, upload_id, etags).status_code == 200
        _assert_object(client2, b"".join(parts), _multipart_etag(parts))

    with _restarted(twin) as client3:
        # (d) The client retries the abort.
        assert client3.delete(f"/{BUCKET}/{KEY}?uploadId={upload_id}").status_code == 204
        again = client3.put(f"/{BUCKET}/{KEY}?partNumber=1&uploadId={upload_id}", content=b"x")
        assert again.status_code == 404
        assert _blob_files(twin) == set() and _registry(twin) == []


# Startup sweep


def test_sweep_killed_mid_cleanup(tmp_path: Path):
    body = os.urandom(64 * 1024)
    server, client = _start(tmp_path, crash_point=PUT_AFTER_BYTES)
    assert client.put(f"/{BUCKET}").status_code == 200
    _request_into_crash(server, lambda: client.put(f"/{BUCKET}/{KEY}", content=body))
    client.close()
    (pending,) = _registry(tmp_path, "pending")

    # The next start is killed by its own sweep, after unlinking the pending blob's
    # file and before deleting its row.
    sweeper = ServerProcess(
        data_dir=tmp_path,
        db_path=tmp_path / "meta.db",
        port=_find_free_port(),
        crash_point=SWEEP_AFTER_UNLINK,
    )
    sweeper.start()
    assert sweeper.wait_for_exit() == -signal.SIGKILL

    # (a) The row survived its file.
    assert _registry(tmp_path) == [pending]
    assert _blob_files(tmp_path) == set() and _temp_files(tmp_path) == []

    # (b) The next sweep finishes the job: the same final state as one uninterrupted
    # sweep, reported once more without bytes.
    report = _sweep(tmp_path)
    assert report.issues == [
        InterruptedWrite(pending.blob_id, "object", BUCKET, KEY, None, None, bytes_durable=False)
    ]
    assert _registry(tmp_path) == [] and _blob_files(tmp_path) == set()

    with _restarted(tmp_path) as client2:
        assert client2.get(f"/{BUCKET}/{KEY}").status_code == 404
        assert client2.put(f"/{BUCKET}/{KEY}", content=body).status_code == 200
        _assert_object(client2, body, _md5(body))


def test_complete_killed_after_gc_unlink(tmp_path: Path):
    server, client = _start(tmp_path, crash_point=COMPLETE_AFTER_GC_UNLINK)
    upload_id, parts, etags = _prepare_upload(client)
    recorded = _store_parts(tmp_path, upload_id)
    body, etag = b"".join(parts), _multipart_etag(parts)
    _complete_into_crash(server, client, upload_id, etags)
    client.close()

    # (a) Committed: the object names a live blob with the full body, the upload is
    # closed, the listed parts keep their ETag and size without bytes. One part blob
    # is unlinked; the other two are garbage with their files present.
    store = _store(tmp_path)
    meta = store.get_object_meta(BUCKET, KEY)
    assert (meta.etag, meta.size, meta.upload_id) == (etag, len(body), upload_id)
    assert _blob_bytes(tmp_path, meta.blob_id) == body
    assert not store.upload_exists(upload_id, bucket=BUCKET, key=KEY)
    assert [(p.part_number, p.etag, p.size, p.blob_id) for p in store.get_parts(upload_id)] == [
        (p.part_number, p.etag, p.size, None) for p in recorded
    ]
    part_blobs = {p.blob_id for p in recorded}
    assert {r.blob_id for r in _registry(tmp_path, "garbage")} == part_blobs
    remaining = _blob_files(tmp_path) - {meta.blob_id}
    assert remaining < part_blobs and len(part_blobs - remaining) == 1

    # (b) The two files still present are reported; the unlinked one's row goes
    # silently.
    report = _sweep(tmp_path)
    part_number = {p.blob_id: p.part_number for p in recorded}
    assert sorted(report.issues, key=lambda i: i.part_number) == sorted(
        (
            InterruptedCleanup(blob_id, "part", BUCKET, KEY, upload_id, part_number[blob_id])
            for blob_id in remaining
        ),
        key=lambda i: i.part_number,
    )
    assert _blob_files(tmp_path) == {meta.blob_id}
    assert [r.blob_id for r in _registry(tmp_path)] == [meta.blob_id]

    with _restarted(tmp_path) as client2:
        # (c)
        _assert_object(client2, body, etag)
        # (d) A retried Complete changes nothing. S3 answers it with 200 and the same
        # ETag; see test_gap_retried_complete_returns_same_etag (j3).
        before = _snapshot(tmp_path, upload_id)
        _complete(client2, upload_id, etags)
        assert _snapshot(tmp_path, upload_id) == before


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="gap (j3): re-completing a completed multipart upload is not implemented; "
    "S3 answers a retried CompleteMultipartUpload with 200 and the same ETag",
)
@pytest.mark.parametrize("point", [COMPLETE_AFTER_COMMIT, COMPLETE_AFTER_GC_UNLINK])
def test_gap_retried_complete_returns_same_etag(tmp_path: Path, point: str):
    """The client's natural retry after a Complete that committed but never answered.
    Setup problems use pytest.fail, so they are real failures, not absorbed xfails."""
    server, client = _start(tmp_path, crash_point=point)
    try:
        upload_id, parts, etags = _prepare_upload(client)
        _complete_into_crash(server, client, upload_id, etags)
    except AssertionError as e:
        pytest.fail(f"setup: Complete did not die at {point}: {e}")
    client.close()
    etag = _multipart_etag(parts)

    with _restarted(tmp_path) as client2:
        if client2.get(f"/{BUCKET}/{KEY}").headers.get("etag") != etag:
            pytest.fail("setup: the object was not committed before the kill")
        resp = _complete(client2, upload_id, etags)
        assert resp.status_code == 200
        assert ET.fromstring(resp.text).findtext("ETag") == etag


def _part_cleanup_issues(recorded, upload_id: str) -> list:
    """The sweep issues for part blobs a commit made garbage, by part number."""
    return [
        InterruptedCleanup(p.blob_id, "part", BUCKET, KEY, upload_id, p.part_number)
        for p in recorded
    ]


def test_abort_killed_after_commit(tmp_path: Path):
    server, client = _start(tmp_path, crash_point=ABORT_AFTER_COMMIT)
    upload_id, _parts, _etags = _prepare_upload(client)
    recorded = _store_parts(tmp_path, upload_id)
    abort = f"/{BUCKET}/{KEY}?uploadId={upload_id}"
    _request_into_crash(server, lambda: client.delete(abort))
    client.close()

    # (a) The upload and its part rows are gone in one commit; the part blobs are
    # garbage with their files present.
    store = _store(tmp_path)
    assert store.get_upload(upload_id, bucket=BUCKET, key=KEY) is None
    assert store.get_parts(upload_id) == []
    part_blobs = {p.blob_id for p in recorded}
    assert {r.blob_id for r in _registry(tmp_path, "garbage")} == part_blobs
    assert _blob_files(tmp_path) == part_blobs

    # (b) The part files are reported as interrupted cleanup; never MissingPart.
    report = _sweep(tmp_path)
    issues = sorted(report.issues, key=lambda i: i.part_number)
    assert issues == _part_cleanup_issues(recorded, upload_id)
    assert _blob_files(tmp_path) == set() and _registry(tmp_path) == []

    with _restarted(tmp_path) as client2:
        # (c) The upload is gone.
        part = client2.put(f"/{BUCKET}/{KEY}?partNumber=1&uploadId={upload_id}", content=b"x")
        assert part.status_code == 404
        assert ET.fromstring(part.text).findtext("Code") == "NoSuchUpload"
        # (d) The retried abort finds it gone, as on S3: 404 NoSuchUpload, and nothing
        # changes.
        before = _snapshot(tmp_path, upload_id)
        retry = client2.delete(abort)
        assert retry.status_code == 404
        assert ET.fromstring(retry.text).findtext("Code") == "NoSuchUpload"
        assert _snapshot(tmp_path, upload_id) == before


def test_delete_bucket_killed_after_commit(tmp_path: Path):
    server, client = _start(tmp_path, crash_point=BUCKET_DELETE_AFTER_COMMIT)
    upload_id, _parts, _etags = _prepare_upload(client)
    recorded = _store_parts(tmp_path, upload_id)
    _request_into_crash(server, lambda: client.delete(f"/{BUCKET}"))
    client.close()

    # (a) The bucket and its open upload are gone in one commit; the part blobs are
    # garbage with their files present.
    store = _store(tmp_path)
    assert not store.bucket_exists(BUCKET)
    assert store.get_upload(upload_id, bucket=BUCKET, key=KEY) is None
    assert store.get_parts(upload_id) == []
    part_blobs = {p.blob_id for p in recorded}
    assert {r.blob_id for r in _registry(tmp_path, "garbage")} == part_blobs
    assert _blob_files(tmp_path) == part_blobs

    # (b)
    report = _sweep(tmp_path)
    issues = sorted(report.issues, key=lambda i: i.part_number)
    assert issues == _part_cleanup_issues(recorded, upload_id)
    assert _blob_files(tmp_path) == set() and _registry(tmp_path) == []

    with _restarted(tmp_path) as client2:
        # (c) The bucket is gone, and its upload with it.
        assert client2.head(f"/{BUCKET}").status_code == 404
        assert f"<Name>{BUCKET}</Name>" not in client2.get("/").text
        part = client2.put(f"/{BUCKET}/{KEY}?partNumber=1&uploadId={upload_id}", content=b"x")
        assert part.status_code == 404
        assert ET.fromstring(part.text).findtext("Code") == "NoSuchBucket"
        # (d) The retried DeleteBucket finds it gone, as on S3: 404 NoSuchBucket, and
        # nothing changes.
        before = _snapshot(tmp_path, upload_id)
        retry = client2.delete(f"/{BUCKET}")
        assert retry.status_code == 404
        assert ET.fromstring(retry.text).findtext("Code") == "NoSuchBucket"
        assert _snapshot(tmp_path, upload_id) == before
        # (e) NoSuchBucket alone does not show the upload went with the bucket: with the
        # bucket back, the same part PUT must find no upload.
        assert client2.put(f"/{BUCKET}").status_code == 200
        again = client2.put(f"/{BUCKET}/{KEY}?partNumber=1&uploadId={upload_id}", content=b"x")
        assert again.status_code == 404
        assert ET.fromstring(again.text).findtext("Code") == "NoSuchUpload"


# First start (§7 step 6)


def test_first_start_killed_after_store_row(tmp_path: Path):
    """The store row is committed as 'initializing', the marker not yet written."""
    starter = ServerProcess(
        data_dir=tmp_path,
        db_path=tmp_path / "meta.db",
        port=_find_free_port(),
        crash_point=STORE_INIT_AFTER_COMMIT,
    )
    starter.start()
    assert starter.wait_for_exit() == -signal.SIGKILL

    # (a) One side holds the ID: an initializing row for this blob directory, no
    # marker, and nothing in blobs/ but its LOCK.
    store = _store(tmp_path).get_store()
    blob_root = tmp_path / "blobs"
    assert store.state == "initializing"
    assert store.blob_dir == os.path.realpath(blob_root)
    assert not (blob_root / "STORE_ID").exists()
    assert sorted(p.name for p in blob_root.iterdir()) == ["LOCK"]

    # (b) Nothing for the sweep to do.
    assert _sweep(tmp_path).clean

    # (c) A server given this database and another blob directory refuses and
    # changes nothing (a LOCK file may appear).
    def files() -> dict[Path, bytes]:
        return {
            p.relative_to(tmp_path): p.read_bytes()
            for p in sorted(tmp_path.rglob("*"))
            if p.is_file() and p.name != "LOCK"
        }

    other = tmp_path / "other"
    (other / "blobs").mkdir(parents=True)
    before = files()
    wrong = ServerProcess(data_dir=other, db_path=tmp_path / "meta.db", port=_find_free_port())
    wrong.start()
    returncode = wrong.wait_for_exit()
    logs = wrong.collect_logs()
    assert returncode not in (0, -signal.SIGKILL), logs
    assert "was initialized for blob directory" in logs
    assert files() == before
    assert _store(tmp_path).get_store() == store

    # (d) The retry: a restart with the same blob directory finishes the first
    # start, then serves.
    with _restarted(tmp_path) as client:
        finished = _store(tmp_path).get_store()
        assert (finished.store_id, finished.state) == (store.store_id, "ready")
        assert (blob_root / "STORE_ID").read_text() == store.store_id + "\n"
        assert client.put(f"/{BUCKET}").status_code == 200
        assert client.put(f"/{BUCKET}/{KEY}", content=NEW).status_code == 200
        _assert_object(client, NEW, _md5(NEW))
