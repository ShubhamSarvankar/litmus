"""
Tier B chaos: a PutObject, UploadPart or UploadPartCopy with a checksum killed around its
commit (docs/design/storage-layout.md §6.2, §6.6, §20).

The checksum is committed in the object row's transaction, so a kill at put-after-bytes
(the bytes are durable, the commit has not run) leaves no row for a new key and the old
object, bytes and checksum, for an overwrite, and a kill at put-after-commit leaves the
row with its checksum. A part's checksum, algorithm and time are committed in the part
row's transaction too, so a kill at part-after-bytes or part-copy-after-bytes leaves the old
part row with its own checksum and time, and a kill after the commit leaves the new row
with all three. The expected values come from hashlib and zlib.
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
from server.crash_points import (
    PART_AFTER_BYTES,
    PART_AFTER_COMMIT,
    PART_COPY_AFTER_BYTES,
    PART_COPY_AFTER_COMMIT,
    PUT_AFTER_BYTES,
    PUT_AFTER_COMMIT,
)
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


# --- parts ---

MPU_KEY = "mpu.bin"
SOURCE = os.urandom(150_000)
OLD_PART_SHA256 = base64.b64encode(hashlib.sha256(OLD).digest()).decode()
SOURCE_CRC32 = base64.b64encode(zlib.crc32(SOURCE).to_bytes(4, "big")).decode()


def _create_upload(client: httpx.Client, algorithm: str) -> str:
    resp = client.post(
        f"/{BUCKET}/{MPU_KEY}?uploads", headers={"x-amz-checksum-algorithm": algorithm}
    )
    assert resp.status_code == 200
    return resp.text.split("<UploadId>")[1].split("</UploadId>")[0]


def _part_url(upload: str) -> str:
    return f"/{BUCKET}/{MPU_KEY}?partNumber=1&uploadId={upload}"


def _setup_part_with_old_part(tmp_path: Path, algorithm: str, old_claim: dict) -> str:
    """A bucket, a source object, and an upload that names algorithm with part 1 uploaded."""
    server, client = _start(tmp_path)
    assert client.put(f"/{BUCKET}").status_code == 200
    assert client.put(URL, content=SOURCE).status_code == 200
    upload = _create_upload(client, algorithm)
    assert client.put(_part_url(upload), content=OLD, headers=old_claim).status_code == 200
    client.close()
    server.stop()
    return upload


def _assert_old_part_kept(tmp_path: Path, upload: str, algorithm: str, checksum: str) -> None:
    (part,) = _store(tmp_path).get_parts(upload)
    assert (part.checksum, part.checksum_algorithm) == (checksum, algorithm)
    assert part.last_modified is not None
    with BlobStore(tmp_path / "blobs").open_blob(part.blob_id) as f:
        assert f.read() == OLD
    assert [type(i) for i in _sweep(tmp_path).issues] == [InterruptedWrite]
    assert _sweep(tmp_path).issues == []


def test_part_killed_after_bytes_keeps_the_old_part_row_with_its_checksum_and_time(
    tmp_path: Path,
):
    upload = _setup_part_with_old_part(
        tmp_path, "SHA256", {"x-amz-checksum-sha256": OLD_PART_SHA256}
    )
    (before,) = _store(tmp_path).get_parts(upload)

    server, client = _start(tmp_path, crash_point=PART_AFTER_BYTES)
    _request_into_crash(
        server,
        lambda: client.put(_part_url(upload), content=NEW, headers=NEW_CLAIM),
    )
    client.close()

    assert _store(tmp_path).get_parts(upload) == [before]
    _assert_old_part_kept(tmp_path, upload, "SHA256", OLD_PART_SHA256)


def test_part_killed_after_commit_has_the_new_row_with_checksum_and_time_together(
    tmp_path: Path,
):
    server, client = _start(tmp_path, crash_point=PART_AFTER_COMMIT)
    assert client.put(f"/{BUCKET}").status_code == 200
    upload = _create_upload(client, "SHA256")
    _request_into_crash(
        server,
        lambda: client.put(_part_url(upload), content=NEW, headers=NEW_CLAIM),
    )
    client.close()

    (part,) = _store(tmp_path).get_parts(upload)
    assert (part.checksum, part.checksum_algorithm) == (NEW_SHA256, "SHA256")
    assert part.last_modified is not None and part.size == len(NEW)
    assert _sweep(tmp_path).issues == []


def test_part_copy_killed_after_bytes_keeps_the_old_part_row_with_its_checksum_and_time(
    tmp_path: Path,
):
    upload = _setup_part_with_old_part(tmp_path, "CRC32", {"x-amz-checksum-crc32": OLD_CRC32})
    (before,) = _store(tmp_path).get_parts(upload)

    server, client = _start(tmp_path, crash_point=PART_COPY_AFTER_BYTES)
    _request_into_crash(
        server,
        lambda: client.put(_part_url(upload), headers={"x-amz-copy-source": f"{BUCKET}/{KEY}"}),
    )
    client.close()

    assert _store(tmp_path).get_parts(upload) == [before]
    _assert_old_part_kept(tmp_path, upload, "CRC32", OLD_CRC32)


def test_part_copy_killed_after_commit_has_the_new_row_with_checksum_and_time_together(
    tmp_path: Path,
):
    server, client = _start(tmp_path, crash_point=PART_COPY_AFTER_COMMIT)
    assert client.put(f"/{BUCKET}").status_code == 200
    assert client.put(URL, content=SOURCE).status_code == 200
    upload = _create_upload(client, "CRC32")
    _request_into_crash(
        server,
        lambda: client.put(_part_url(upload), headers={"x-amz-copy-source": f"{BUCKET}/{KEY}"}),
    )
    client.close()

    (part,) = _store(tmp_path).get_parts(upload)
    assert (part.checksum, part.checksum_algorithm) == (SOURCE_CRC32, "CRC32")
    assert part.last_modified is not None and part.size == len(SOURCE)
    assert _sweep(tmp_path).issues == []
