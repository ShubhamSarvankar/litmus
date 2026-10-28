"""
Tier B chaos: CompleteMultipartUpload with checksums, killed at each of its crash points
(docs/design/storage-layout.md §6.7, §20).

The object's checksum, algorithm and type are committed in the transaction that marks the
upload completed. A kill before that commit (complete-after-register, complete-mid-assemble,
complete-after-assemble) leaves the upload open with every part's checksum as it was, no
object, and a Complete that works when retried; a kill after it (complete-after-commit,
complete-after-gc-unlink) leaves the object row with its checksum beside the completed
upload, and the listed part rows with their checksums and a NULL blob_id. The expected
values come from hashlib and awscrt, called directly on the bytes.
"""

import base64
import hashlib
import os
import signal
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx
import pytest
from awscrt import checksums as crt

from chaos.fixtures.subprocess import ServerProcess, _find_free_port
from server.consistency import run_consistency_sweep
from server.crash_points import (
    COMPLETE_AFTER_ASSEMBLE,
    COMPLETE_AFTER_COMMIT,
    COMPLETE_AFTER_GC_UNLINK,
    COMPLETE_AFTER_REGISTER,
    COMPLETE_MID_ASSEMBLE,
)
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SIGKILL not available on Windows")

BUCKET = "complete-cksum-crash"
KEY = "obj.bin"
PARTS = [b"one " * 30_000, b"two! " * 20_000, b"three" * 100]
WHOLE = b"".join(PARTS)


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode()


def _crc64(data: bytes) -> str:
    return _b64(crt.crc64nvme(data).to_bytes(8, "big"))


PART_CRC64 = [_crc64(p) for p in PARTS]
OBJECT_CRC64 = _crc64(WHOLE)
PART_SHA256 = [_b64(hashlib.sha256(p).digest()) for p in PARTS]
OBJECT_SHA256 = _b64(hashlib.sha256(b"".join(hashlib.sha256(p).digest() for p in PARTS)).digest())
OBJECT_SHA256 += f"-{len(PARTS)}"


def _start(tmp_path: Path, crash_point: str | None = None) -> tuple[ServerProcess, httpx.Client]:
    port = _find_free_port()
    server = ServerProcess(
        data_dir=tmp_path, db_path=tmp_path / "meta.db", port=port, crash_point=crash_point
    )
    server.start()
    server.wait_ready()
    return server, httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10.0)


def _store(tmp_path: Path) -> SQLiteMetadataStore:
    return SQLiteMetadataStore(db_path=tmp_path / "meta.db")


def _sweep(tmp_path: Path):
    return run_consistency_sweep(_store(tmp_path), BlobStore(tmp_path / "blobs"))


def _upload(client: httpx.Client, algorithm: str | None = None) -> tuple[str, list[str]]:
    """A bucket and an open upload with three parts; the upload id and the ETags."""
    assert client.put(f"/{BUCKET}").status_code == 200
    headers = {"x-amz-checksum-algorithm": algorithm} if algorithm else {}
    created = client.post(f"/{BUCKET}/{KEY}?uploads", headers=headers)
    upload = ET.fromstring(created.text).findtext("UploadId")
    etags = []
    for number, data in enumerate(PARTS, start=1):
        r = client.put(f"/{BUCKET}/{KEY}?partNumber={number}&uploadId={upload}", content=data)
        assert r.status_code == 200
        etags.append(r.headers["etag"])
    return upload, etags


def _complete(client: httpx.Client, upload: str, etags: list[str]) -> httpx.Response:
    items = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>"
        for n, e in enumerate(etags, start=1)
    )
    return client.post(
        f"/{BUCKET}/{KEY}?uploadId={upload}",
        content=f"<CompleteMultipartUpload>{items}</CompleteMultipartUpload>".encode(),
    )


def _crashed(tmp_path: Path, point: str, algorithm: str | None = None) -> tuple[str, list[str]]:
    """Run a Complete into the crash point on a fresh store; the dead server's upload."""
    server, client = _start(tmp_path, crash_point=point)
    upload, etags = _upload(client, algorithm)
    with pytest.raises(httpx.TransportError):
        _complete(client, upload, etags)
    assert server.wait_for_exit() == -signal.SIGKILL
    client.close()
    return upload, etags


@pytest.mark.parametrize(
    "point",
    [
        pytest.param(COMPLETE_AFTER_REGISTER, id="after-register"),
        pytest.param(COMPLETE_MID_ASSEMBLE, id="mid-assemble"),
        pytest.param(COMPLETE_AFTER_ASSEMBLE, id="after-assemble"),
    ],
)
def test_a_kill_before_the_commit_leaves_the_upload_open_with_its_part_checksums(
    tmp_path: Path, point: str
):
    upload, etags = _crashed(tmp_path, point)
    store = _store(tmp_path)

    assert store.upload_exists(upload)
    assert store.get_object_meta(BUCKET, KEY) is None
    parts = store.get_parts(upload)
    assert [(p.checksum, p.checksum_algorithm) for p in parts] == [
        (value, "CRC64NVME") for value in PART_CRC64
    ]
    assert all(p.blob_id is not None for p in parts)

    # The retry completes it, with the combined checksum of the parts as uploaded.
    server, client = _start(tmp_path)
    try:
        resp = _complete(client, upload, etags)
        assert resp.status_code == 200, resp.text
        assert ET.fromstring(resp.text).findtext("ChecksumCRC64NVME") == OBJECT_CRC64
        got = client.get(f"/{BUCKET}/{KEY}", headers={"x-amz-checksum-mode": "ENABLED"})
        assert got.content == WHOLE
        assert got.headers["x-amz-checksum-crc64nvme"] == OBJECT_CRC64
    finally:
        client.close()
        server.stop()
    assert _sweep(tmp_path).issues == []


@pytest.mark.parametrize(
    "point",
    [
        pytest.param(COMPLETE_AFTER_COMMIT, id="after-commit"),
        pytest.param(COMPLETE_AFTER_GC_UNLINK, id="after-gc-unlink"),
    ],
)
def test_a_kill_after_the_commit_leaves_the_object_checksum_with_the_completed_state(
    tmp_path: Path, point: str
):
    upload, _ = _crashed(tmp_path, point)
    store = _store(tmp_path)

    meta = store.get_object_meta(BUCKET, KEY)
    assert (meta.checksum, meta.checksum_algorithm, meta.checksum_type) == (
        OBJECT_CRC64,
        "CRC64NVME",
        "FULL_OBJECT",
    )
    assert meta.size == len(WHOLE)
    assert not store.upload_exists(upload)
    assert store.get_upload(upload).state == "completed"
    parts = store.get_parts(upload)
    assert [(p.checksum, p.checksum_algorithm, p.blob_id) for p in parts] == [
        (value, "CRC64NVME", None) for value in PART_CRC64
    ]
    with BlobStore(tmp_path / "blobs").open_blob(meta.blob_id) as f:
        assert f.read() == WHOLE

    # A restart repairs what the kill left, and the checksum is still served.
    server, client = _start(tmp_path)
    try:
        got = client.get(f"/{BUCKET}/{KEY}", headers={"x-amz-checksum-mode": "ENABLED"})
        assert got.content == WHOLE
        assert got.headers["x-amz-checksum-crc64nvme"] == OBJECT_CRC64
        assert got.headers["x-amz-checksum-type"] == "FULL_OBJECT"
    finally:
        client.close()
        server.stop()
    assert _sweep(tmp_path).issues == []


def test_a_composite_upload_killed_after_the_commit_keeps_the_suffixed_checksum(
    tmp_path: Path,
):
    upload, _ = _crashed(tmp_path, COMPLETE_AFTER_COMMIT, algorithm="SHA256")
    store = _store(tmp_path)

    meta = store.get_object_meta(BUCKET, KEY)
    assert (meta.checksum, meta.checksum_algorithm, meta.checksum_type) == (
        OBJECT_SHA256,
        "SHA256",
        "COMPOSITE",
    )
    assert store.get_upload(upload).state == "completed"
    assert [(p.checksum, p.blob_id) for p in store.get_parts(upload)] == [
        (value, None) for value in PART_SHA256
    ]

    server, client = _start(tmp_path)
    try:
        head = client.head(f"/{BUCKET}/{KEY}", headers={"x-amz-checksum-mode": "ENABLED"})
        assert head.headers["x-amz-checksum-sha256"] == OBJECT_SHA256
        assert head.headers["x-amz-checksum-type"] == "COMPOSITE"
    finally:
        client.close()
        server.stop()
    assert _sweep(tmp_path).issues == []
