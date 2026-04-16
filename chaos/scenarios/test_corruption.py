"""
Tier A chaos: byte corruption applied directly to stored files.

Invariant under test:
- The server must never silently serve corrupt data. Either the ETag in the
  response header must not match the MD5 of the response body, OR the server
  must return a 5xx. A 200 with a body that matches the ETag but differs from
  the original is also acceptable (the server can't detect corruption it didn't
  cause), but a 200 where ETag matches original yet body is corrupt is a failure.
"""

import hashlib
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.filesystem import FilesystemBackend


@pytest.fixture
def s3(tmp_path: Path):
    storage = FilesystemBackend(
        data_dir=tmp_path / "objects",
        parts_dir=tmp_path / "parts",
    )
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    app = create_app(storage=storage, metadata=metadata)
    client = TestClient(app, raise_server_exceptions=False)
    return client, storage, tmp_path


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def test_corrupted_object_etag_mismatch_or_error(s3):
    """Corrupt bytes on disk — server must not serve data whose MD5 matches the stored ETag."""
    client, storage, _ = s3
    original = b"the quick brown fox jumps over the lazy dog"

    client.put("/bucket")
    client.put("/bucket/doc.txt", content=original)

    obj_path = storage._object_path("bucket", "doc.txt")
    data = bytearray(obj_path.read_bytes())
    data[4] ^= 0xFF
    obj_path.write_bytes(bytes(data))

    get_resp = client.get("/bucket/doc.txt")

    if get_resp.status_code == 200:
        body_md5 = _md5(get_resp.content)
        assert get_resp.headers.get("etag") != body_md5 or get_resp.content == original, (
            "Server served corrupt data with a matching ETag — silent data corruption"
        )
    else:
        assert get_resp.status_code >= 500


def test_fully_corrupted_object_is_not_empty(s3):
    """A corrupted object must not be silently returned as an empty body."""
    client, storage, _ = s3
    original = b"important data that must not vanish"

    client.put("/bucket")
    client.put("/bucket/important.txt", content=original)

    obj_path = storage._object_path("bucket", "important.txt")
    obj_path.write_bytes(b"\x00" * len(original))

    get_resp = client.get("/bucket/important.txt")
    if get_resp.status_code == 200:
        assert len(get_resp.content) > 0


def test_missing_file_returns_error_not_crash(s3):
    """Delete file from disk after PUT — GET must return error, not crash."""
    client, storage, _ = s3

    client.put("/bucket")
    client.put("/bucket/gone.txt", content=b"data")
    storage._object_path("bucket", "gone.txt").unlink()

    resp = client.get("/bucket/gone.txt")
    assert resp.status_code in (404, 500)
    assert not (resp.status_code == 200 and resp.content == b"")


def test_missing_metadata_returns_404(s3):
    """Delete metadata row after PUT — GET must return 404."""
    client, storage, tmp_path = s3

    client.put("/bucket")
    client.put("/bucket/meta-gone.txt", content=b"data")

    conn = sqlite3.connect(str(tmp_path / "meta.db"))
    conn.execute("DELETE FROM objects WHERE bucket='bucket' AND key='meta-gone.txt'")
    conn.commit()
    conn.close()

    resp = client.get("/bucket/meta-gone.txt")
    assert resp.status_code == 404
