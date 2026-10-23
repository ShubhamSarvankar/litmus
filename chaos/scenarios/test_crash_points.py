"""
Tier B chaos: SIGKILL at exact steps of the write path, via S3_TEST_CRASH_POINT.

Each test arms one crash point (server/crash_points.py), drives a request into it,
confirms the server died by SIGKILL, restarts it unarmed on the same data, and then
asserts the exact state left behind: what clients see, what is on disk, what is in
metadata, and what the consistency sweep reports. It then checks that the client's
natural retry recovers.

The per-point tests assert only correct behavior. Where the current code breaks an
invariant at a crash point, the correct behavior is asserted in a separate
test_gap_* test marked xfail(strict=True, raises=AssertionError): it fails today,
and as soon as the gap is fixed it XPASSes and turns the run red until the marker
is removed. Setup problems in those tests use pytest.fail, so they surface as real
failures instead of being absorbed by the xfail.

Overwrite and DELETE crash points are deliberately not covered yet.
"""

import hashlib
import os
import signal
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx
import pytest

from chaos.fixtures.subprocess import ServerProcess, _find_free_port
from server.consistency import MissingPart, OrphanedMetadata, run_consistency_sweep
from server.crash_points import (
    COMPLETE_AFTER_ASSEMBLE,
    COMPLETE_AFTER_COMMIT,
    PUT_AFTER_BYTES,
)
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.filesystem import FilesystemBackend

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


def _object_dir(tmp_path: Path) -> Path:
    return tmp_path / "objects" / BUCKET


def _part_dir(tmp_path: Path, upload_id: str) -> Path:
    return tmp_path / "parts" / upload_id


def _store(tmp_path: Path) -> SQLiteMetadataStore:
    return SQLiteMetadataStore(db_path=tmp_path / "meta.db")


def _sweep(tmp_path: Path):
    return run_consistency_sweep(
        _store(tmp_path), FilesystemBackend(tmp_path / "objects", tmp_path / "parts")
    )


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


def test_put_killed_after_bytes_before_metadata(tmp_path: Path):
    """PUT bytes durable at the final path, metadata not committed.

    Expected: the object is not visible; its complete bytes are on disk as an
    orphaned file; a retried PUT succeeds.
    """
    body = os.urandom(1024 * 1024)
    server, client = _start(tmp_path, crash_point=PUT_AFTER_BYTES)
    assert client.put(f"/{BUCKET}").status_code == 200
    _request_into_crash(server, lambda: client.put(f"/{BUCKET}/{KEY}", content=body))
    client.close()

    server2, client2 = _start(tmp_path)
    try:
        # Not visible to clients.
        assert client2.get(f"/{BUCKET}/{KEY}").status_code == 404
        assert client2.head(f"/{BUCKET}/{KEY}").status_code == 404
        assert "<Key>" not in client2.get(f"/{BUCKET}").text

        # No metadata row; the complete bytes are at the final path; no temp file left.
        assert _store(tmp_path).get_object_meta(BUCKET, KEY) is None
        assert sorted(os.listdir(_object_dir(tmp_path))) == [KEY]
        assert (_object_dir(tmp_path) / KEY).read_bytes() == body

        # The client's retry succeeds and the object becomes visible.
        assert client2.put(f"/{BUCKET}/{KEY}", content=body).status_code == 200
        resp = client2.get(f"/{BUCKET}/{KEY}")
        assert resp.status_code == 200
        assert resp.content == body
        assert resp.headers["etag"] == _md5(body)
    finally:
        client2.close()
        server2.stop()


def test_complete_killed_after_assemble_before_metadata(tmp_path: Path):
    """Complete assembled the final file, object metadata not committed.

    Expected: the object is not visible; the assembled bytes are on disk as an
    orphaned file; the upload is still open with all parts recorded and on disk;
    a retried Complete succeeds.
    """
    server, client = _start(tmp_path, crash_point=COMPLETE_AFTER_ASSEMBLE)
    upload_id, parts, etags = _prepare_upload(client)
    body, etag = b"".join(parts), _multipart_etag(parts)
    _complete_into_crash(server, client, upload_id, etags)
    client.close()

    server2, client2 = _start(tmp_path)
    try:
        # Not visible to clients.
        assert client2.get(f"/{BUCKET}/{KEY}").status_code == 404
        assert _store(tmp_path).get_object_meta(BUCKET, KEY) is None

        # The assembled file is complete on disk; no temp file left.
        assert sorted(os.listdir(_object_dir(tmp_path))) == [KEY]
        assert (_object_dir(tmp_path) / KEY).read_bytes() == body

        # Upload still open, every part recorded and on disk.
        store = _store(tmp_path)
        assert store.upload_exists(upload_id)
        assert [p.part_number for p in store.get_parts(upload_id)] == [1, 2, 3]
        assert sorted(os.listdir(_part_dir(tmp_path, upload_id))) == ["1", "2", "3"]

        # The client's retry completes the upload.
        resp = client2.post(f"/{BUCKET}/{KEY}?uploadId={upload_id}", content=_complete_xml(etags))
        assert resp.status_code == 200
        assert ET.fromstring(resp.text).findtext("ETag") == etag
        got = client2.get(f"/{BUCKET}/{KEY}")
        assert got.content == body
        assert got.headers["etag"] == etag
        assert not _store(tmp_path).upload_exists(upload_id)
        assert not _part_dir(tmp_path, upload_id).exists()
    finally:
        client2.close()
        server2.stop()


def test_complete_killed_after_commit_before_part_cleanup(tmp_path: Path):
    """Complete committed the object metadata and the upload completion in one
    transaction; part files not yet deleted.

    Expected: the object is visible with the full body and composite ETag; the
    upload is closed; its part files are still on disk, because cleanup runs after
    the commit. Whether the sweep notices those part files is a gap; see
    test_gap_sweep_reports_part_files_of_completed_upload.
    """
    server, client = _start(tmp_path, crash_point=COMPLETE_AFTER_COMMIT)
    upload_id, parts, etags = _prepare_upload(client)
    body, etag = b"".join(parts), _multipart_etag(parts)
    _complete_into_crash(server, client, upload_id, etags)
    client.close()

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

        # Upload closed; the assembled file is the only object file; the part
        # files remain because the kill came before cleanup.
        assert not store.upload_exists(upload_id)
        assert all(u.upload_id != upload_id for u in store.list_incomplete_uploads())
        assert sorted(os.listdir(_object_dir(tmp_path))) == [KEY]
        assert sorted(os.listdir(_part_dir(tmp_path, upload_id))) == ["1", "2", "3"]
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
    assert not store.upload_exists(upload_id), (
        "object is committed but its multipart upload is still open"
    )


# --- known gaps: correct behavior, expected to fail until fixed ---


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="gap: the consistency sweep does not detect object files that have no "
    "metadata row (orphaned by a kill between the bytes and the metadata commit)",
)
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
    if not (_object_dir(tmp_path) / KEY).exists():
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


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="gap: the consistency sweep does not detect part files left on disk for a "
    "completed upload (left by a kill between the Complete commit and part cleanup)",
)
def test_gap_sweep_reports_part_files_of_completed_upload(tmp_path: Path):
    server, client = _start(tmp_path, crash_point=COMPLETE_AFTER_COMMIT)
    upload_id, _parts, etags = _prepare_upload(client)
    try:
        _complete_into_crash(server, client, upload_id, etags)
    except AssertionError as e:
        pytest.fail(f"setup: server did not die at the crash point: {e}")
    client.close()
    store = _store(tmp_path)
    if store.upload_exists(upload_id):
        pytest.fail("setup: upload still open, so its parts are not leftovers")
    if not _part_dir(tmp_path, upload_id).exists():
        pytest.fail("setup: no part files on disk, so nothing is left over")

    report = _sweep(tmp_path)
    reported = [
        i
        for i in report.issues
        if not isinstance(i, MissingPart) and getattr(i, "upload_id", None) == upload_id
    ]
    assert reported, f"sweep did not report the leftover part files; issues: {report.issues}"
