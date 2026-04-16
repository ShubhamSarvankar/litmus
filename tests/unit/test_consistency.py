import io
import logging
from datetime import datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.consistency import (
    ConsistencyReport,
    MissingPart,
    OrphanedMetadata,
    run_consistency_sweep,
)
from server.metadata.base import ObjectMeta, PartMeta
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.filesystem import FilesystemBackend


@pytest.fixture
def deps(tmp_path: Path):
    storage = FilesystemBackend(
        data_dir=tmp_path / "objects",
        parts_dir=tmp_path / "parts",
    )
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    return storage, metadata


_NOW = datetime(2024, 1, 1, 12, 0, 0)


def _seed_object(deps, bucket: str, key: str, body: bytes = b"data"):
    storage, metadata = deps
    if not metadata.bucket_exists(bucket):
        metadata.create_bucket(bucket, _NOW)
    etag = storage.write_object(bucket, key, io.BytesIO(body), len(body))
    metadata.put_object_meta(
        bucket,
        key,
        ObjectMeta(
            bucket=bucket,
            key=key,
            size=len(body),
            etag=etag,
            last_modified=_NOW,
            content_type="application/octet-stream",
        ),
    )
    return etag


# --- clean state ---


def test_sweep_clean_state_no_issues(deps):
    storage, metadata = deps
    report = run_consistency_sweep(metadata, storage)
    assert report.clean
    assert report.issues == []


def test_sweep_clean_state_with_objects(deps):
    _seed_object(deps, "b", "k1")
    _seed_object(deps, "b", "k2")
    storage, metadata = deps
    report = run_consistency_sweep(metadata, storage)
    assert report.clean


# --- orphaned metadata ---


def test_sweep_detects_orphaned_metadata(deps):
    """Metadata row exists but file was deleted from disk."""
    storage, metadata = deps
    _seed_object(deps, "b", "lost.txt")
    # Delete the file directly, bypassing storage layer
    obj_path = storage._object_path("b", "lost.txt")
    obj_path.unlink()

    report = run_consistency_sweep(metadata, storage)
    assert not report.clean
    assert len(report.issues) == 1
    issue = report.issues[0]
    assert isinstance(issue, OrphanedMetadata)
    assert issue.bucket == "b"
    assert issue.key == "lost.txt"


def test_sweep_orphaned_metadata_logged_as_warning(deps, caplog):
    storage, metadata = deps
    _seed_object(deps, "b", "lost.txt")
    storage._object_path("b", "lost.txt").unlink()

    with caplog.at_level(logging.WARNING, logger="server.consistency"):
        run_consistency_sweep(metadata, storage)

    assert any("orphaned metadata" in r.message for r in caplog.records)


def test_sweep_multiple_orphans(deps):
    storage, metadata = deps
    _seed_object(deps, "b", "k1")
    _seed_object(deps, "b", "k2")
    storage._object_path("b", "k1").unlink()
    storage._object_path("b", "k2").unlink()

    report = run_consistency_sweep(metadata, storage)
    assert len(report.issues) == 2
    assert all(isinstance(i, OrphanedMetadata) for i in report.issues)


# --- missing part ---


def test_sweep_detects_missing_part(deps):
    """Part file deleted after being recorded in metadata."""
    storage, metadata = deps
    metadata.create_bucket("b", _NOW)
    metadata.create_upload("uid-1", "b", "k", _NOW)
    etag = storage.write_part("uid-1", 1, io.BytesIO(b"part-data"))
    metadata.record_part("uid-1", PartMeta("uid-1", 1, etag, 9))

    # Delete part file directly
    storage._part_path("uid-1", 1).unlink()

    report = run_consistency_sweep(metadata, storage)
    assert not report.clean
    assert len(report.issues) == 1
    issue = report.issues[0]
    assert isinstance(issue, MissingPart)
    assert issue.upload_id == "uid-1"
    assert issue.part_number == 1


def test_sweep_missing_part_logged_as_warning(deps, caplog):
    storage, metadata = deps
    metadata.create_bucket("b", _NOW)
    metadata.create_upload("uid-1", "b", "k", _NOW)
    etag = storage.write_part("uid-1", 1, io.BytesIO(b"part-data"))
    metadata.record_part("uid-1", PartMeta("uid-1", 1, etag, 9))
    storage._part_path("uid-1", 1).unlink()

    with caplog.at_level(logging.WARNING, logger="server.consistency"):
        run_consistency_sweep(metadata, storage)

    assert any("missing part" in r.message for r in caplog.records)


def test_sweep_completed_uploads_not_checked(deps):
    """Completed uploads are not in list_incomplete_uploads — parts need not exist."""
    storage, metadata = deps
    metadata.create_bucket("b", _NOW)
    metadata.create_upload("uid-done", "b", "k", _NOW)
    metadata.record_part("uid-done", PartMeta("uid-done", 1, '"etag"', 5))
    metadata.complete_upload("uid-done")
    # No part file on disk — should not flag as MissingPart since upload is complete

    report = run_consistency_sweep(metadata, storage)
    assert report.clean


# --- server starts regardless of issues ---


def test_server_starts_with_orphaned_metadata(deps):
    storage, metadata = deps
    _seed_object(deps, "b", "orphan.txt")
    storage._object_path("b", "orphan.txt").unlink()

    # Server must start and /health must return 200 even with issues
    app = create_app(storage=storage, metadata=metadata)
    with TestClient(app, raise_server_exceptions=False) as client:
        resp = client.get("/health")
    assert resp.status_code == 200


def test_server_accepts_requests_with_sweep_issues(deps):
    storage, metadata = deps
    _seed_object(deps, "b", "orphan.txt")
    storage._object_path("b", "orphan.txt").unlink()

    app = create_app(storage=storage, metadata=metadata)
    with TestClient(app, raise_server_exceptions=False) as client:
        # Bucket operations work normally despite sweep finding issues
        client.put("/new-bucket")
        resp = client.get("/")
    assert resp.status_code == 200


# --- ConsistencyReport helpers ---


def test_report_clean_property_true_when_no_issues():
    report = ConsistencyReport(issues=[])
    assert report.clean is True


def test_report_clean_property_false_when_issues():
    report = ConsistencyReport(issues=[OrphanedMetadata("b", "k")])
    assert report.clean is False
