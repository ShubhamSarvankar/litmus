"""
Tier B chaos: the checksum of a CopyObject killed at its crash points
(docs/design/storage-layout.md decision 33, §15, §20).

The bytes are durable before the metadata, and the three checksum columns are written in the
same commit as the rest of the destination row, so a kill leaves either the destination as
it was, with its own three columns, or the new row with all three recomputed. The source is
never touched.
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
from server.crash_points import COPY_AFTER_BYTES, COPY_AFTER_COMMIT
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SIGKILL not available on Windows")

BUCKET = "copy-checksum-crash"
SRC = "src.bin"
DST = "dst.bin"
SOURCE = os.urandom(200 * 1024)  # more than three 64 KiB chunks
OLD = b"old destination. " * 3000
MODE = {"x-amz-checksum-mode": "ENABLED"}


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode()


def _crc32(data: bytes) -> str:
    return _b64(zlib.crc32(data).to_bytes(4, "big"))


def _sha256(data: bytes) -> str:
    return _b64(hashlib.sha256(data).digest())


def _start(tmp_path: Path, crash_point: str | None = None) -> tuple[ServerProcess, httpx.Client]:
    port = _find_free_port()
    server = ServerProcess(
        data_dir=tmp_path, db_path=tmp_path / "meta.db", port=port, crash_point=crash_point
    )
    server.start()
    server.wait_ready()
    return server, httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10.0)


def _setup(tmp_path: Path, destination: bytes | None) -> None:
    """The source (a CRC32 claim) and the destination (another CRC32 claim) when given,
    written by an unarmed server."""
    server, client = _start(tmp_path)
    try:
        assert client.put(f"/{BUCKET}").status_code == 200
        claim = {"x-amz-checksum-crc32": _crc32(SOURCE)}
        assert client.put(f"/{BUCKET}/{SRC}", content=SOURCE, headers=claim).status_code == 200
        if destination is not None:
            claim = {"x-amz-checksum-crc32": _crc32(destination)}
            put = client.put(f"/{BUCKET}/{DST}", content=destination, headers=claim)
            assert put.status_code == 200
    finally:
        client.close()
        server.stop()


def _copy_into_crash(tmp_path: Path, point: str, **headers) -> None:
    server, client = _start(tmp_path, crash_point=point)
    try:
        with pytest.raises(httpx.TransportError):
            client.put(
                f"/{BUCKET}/{DST}", headers={"x-amz-copy-source": f"{BUCKET}/{SRC}", **headers}
            )
        assert server.wait_for_exit() == -signal.SIGKILL
    finally:
        client.close()


def _row(tmp_path: Path, key: str):
    return SQLiteMetadataStore(db_path=tmp_path / "meta.db").get_object_meta(BUCKET, key)


def _columns(row) -> tuple:
    return row.checksum, row.checksum_algorithm, row.checksum_type


def test_copy_killed_after_bytes_leaves_no_destination_and_the_source_unchanged(tmp_path: Path):
    _setup(tmp_path, destination=None)
    source = _row(tmp_path, SRC)
    _copy_into_crash(tmp_path, COPY_AFTER_BYTES, **{"x-amz-checksum-algorithm": "SHA256"})

    assert _row(tmp_path, DST) is None
    assert _row(tmp_path, SRC) == source
    assert _columns(source) == (_crc32(SOURCE), "CRC32", "FULL_OBJECT")
    assert BlobStore(tmp_path / "blobs").blob_path(source.blob_id).read_bytes() == SOURCE
    server, client = _start(tmp_path)
    try:
        assert client.get(f"/{BUCKET}/{DST}").status_code == 404
        got = client.get(f"/{BUCKET}/{SRC}", headers=MODE)
        assert got.content == SOURCE and got.headers["x-amz-checksum-crc32"] == _crc32(SOURCE)
    finally:
        client.close()
        server.stop()


def test_copy_killed_after_commit_leaves_the_destination_with_its_recomputed_checksum(
    tmp_path: Path,
):
    _setup(tmp_path, destination=None)
    _copy_into_crash(tmp_path, COPY_AFTER_COMMIT, **{"x-amz-checksum-algorithm": "sha256"})

    row = _row(tmp_path, DST)
    assert _columns(row) == (_sha256(SOURCE), "SHA256", "FULL_OBJECT")
    assert _columns(_row(tmp_path, SRC)) == (_crc32(SOURCE), "CRC32", "FULL_OBJECT")
    server, client = _start(tmp_path)
    try:
        got = client.get(f"/{BUCKET}/{DST}", headers=MODE)
        assert got.status_code == 200 and got.content == SOURCE
        assert got.headers["x-amz-checksum-sha256"] == _sha256(SOURCE)
        assert got.headers["x-amz-checksum-type"] == "FULL_OBJECT"
    finally:
        client.close()
        server.stop()


def test_an_overwrite_killed_after_bytes_leaves_the_old_destination_with_its_old_columns(
    tmp_path: Path,
):
    _setup(tmp_path, destination=OLD)
    old = _row(tmp_path, DST)
    assert _columns(old) == (_crc32(OLD), "CRC32", "FULL_OBJECT")
    _copy_into_crash(tmp_path, COPY_AFTER_BYTES, **{"x-amz-checksum-algorithm": "SHA256"})

    assert _row(tmp_path, DST) == old
    server, client = _start(tmp_path)
    try:
        got = client.get(f"/{BUCKET}/{DST}", headers=MODE)
        assert got.content == OLD
        assert got.headers["x-amz-checksum-crc32"] == _crc32(OLD)
        assert "x-amz-checksum-sha256" not in got.headers
    finally:
        client.close()
        server.stop()
