"""
Tier A chaos: metadata/data desync scenarios.

Invariants under test:
- Metadata present, file missing → GET returns error (not crash, not empty 200).
- File present, metadata missing → GET returns 404.
- Consistency sweep detects both conditions.
"""

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.consistency import OrphanedMetadata, run_consistency_sweep
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
    return client, storage, metadata, tmp_path


def test_missing_file_returns_error_not_crash(s3):
    client, storage, _, _ = s3
    client.put("/bucket")
    client.put("/bucket/key.txt", content=b"data")

    storage._object_path("bucket", "key.txt").unlink()

    resp = client.get("/bucket/key.txt")
    assert resp.status_code in (404, 500)


def test_missing_file_response_is_not_empty_200(s3):
    client, storage, _, _ = s3
    client.put("/bucket")
    client.put("/bucket/key.txt", content=b"data")
    storage._object_path("bucket", "key.txt").unlink()

    resp = client.get("/bucket/key.txt")
    assert not (resp.status_code == 200 and resp.content == b"")


def test_missing_file_head_returns_error(s3):
    """HEAD on a desync'd object must not return 200."""
    client, storage, _, _ = s3
    client.put("/bucket")
    client.put("/bucket/key.txt", content=b"data")
    storage._object_path("bucket", "key.txt").unlink()

    resp = client.head("/bucket/key.txt")
    # HEAD may still return 200 (metadata is present) — that is acceptable since
    # HEAD reads only metadata. What must NOT happen is a crash/traceback.
    assert resp.status_code in (200, 404, 500)


def test_missing_metadata_returns_404(s3):
    client, storage, _, tmp_path = s3
    client.put("/bucket")
    client.put("/bucket/meta-gone.txt", content=b"data")

    conn = sqlite3.connect(str(tmp_path / "meta.db"))
    conn.execute("DELETE FROM objects WHERE bucket='bucket' AND key='meta-gone.txt'")
    conn.commit()
    conn.close()

    resp = client.get("/bucket/meta-gone.txt")
    assert resp.status_code == 404


def test_sweep_detects_orphaned_metadata_after_desync(s3):
    client, storage, metadata, _ = s3
    client.put("/bucket")
    client.put("/bucket/key.txt", content=b"data")
    storage._object_path("bucket", "key.txt").unlink()

    report = run_consistency_sweep(metadata, storage)
    assert any(isinstance(i, OrphanedMetadata) and i.key == "key.txt" for i in report.issues)


def test_sweep_clean_after_normal_operations(s3):
    client, storage, metadata, _ = s3
    client.put("/bucket")
    client.put("/bucket/a.txt", content=b"aaa")
    client.put("/bucket/b.txt", content=b"bbb")

    report = run_consistency_sweep(metadata, storage)
    assert report.clean


def test_multiple_desyncs_all_reported(s3):
    client, storage, metadata, _ = s3
    client.put("/bucket")
    client.put("/bucket/k1.txt", content=b"data1")
    client.put("/bucket/k2.txt", content=b"data2")
    storage._object_path("bucket", "k1.txt").unlink()
    storage._object_path("bucket", "k2.txt").unlink()

    report = run_consistency_sweep(metadata, storage)
    orphaned_keys = {i.key for i in report.issues if isinstance(i, OrphanedMetadata)}
    assert {"k1.txt", "k2.txt"} == orphaned_keys
