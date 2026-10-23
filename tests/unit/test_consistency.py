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
from server.storage.blobs import BlobStore


@pytest.fixture
def deps(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    storage.create_tree()
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    return storage, metadata


_NOW = datetime(2024, 1, 1, 12, 0, 0)


def _seed_object(deps, bucket: str, key: str, body: bytes = b"data"):
    storage, metadata = deps
    if not metadata.bucket_exists(bucket):
        metadata.create_bucket(bucket, _NOW)
    blob_id = metadata.register_blob("object", bucket, key)
    etag, size = storage.write_blob(blob_id, io.BytesIO(body))
    metadata.put_object_meta(
        bucket,
        key,
        ObjectMeta(
            bucket=bucket,
            key=key,
            size=size,
            etag=etag,
            last_modified=_NOW,
            content_type="application/octet-stream",
            blob_id=blob_id,
        ),
    )
    return etag


def _first_start(deps):
    """Start and stop an app on these stores once, so the store ID exists before any
    data is seeded (a first start refuses a database that already has rows)."""
    storage, metadata = deps
    with TestClient(create_app(storage=storage, metadata=metadata)):
        pass


def _object_path(deps, bucket: str, key: str):
    """The blob file of an object, resolved through its metadata row."""
    storage, metadata = deps
    return storage.blob_path(metadata.get_object_meta(bucket, key).blob_id)


def _part_path(deps, upload_id: str, part_number: int):
    """The blob file of a part, resolved through its metadata row."""
    storage, metadata = deps
    (part,) = [p for p in metadata.get_parts(upload_id) if p.part_number == part_number]
    return storage.blob_path(part.blob_id)


def _seed_part(deps, upload_id: str, part_number: int, body: bytes):
    storage, metadata = deps
    upload = metadata.get_upload(upload_id)
    blob_id = metadata.register_blob(
        "part", upload.bucket, upload.key, upload_id=upload_id, part_number=part_number
    )
    etag, size = storage.write_blob(blob_id, io.BytesIO(body))
    metadata.record_part(upload_id, PartMeta(upload_id, part_number, etag, size, blob_id))


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
    obj_path = _object_path(deps, "b", "lost.txt")
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
    _object_path(deps, "b", "lost.txt").unlink()

    with caplog.at_level(logging.WARNING, logger="server.consistency"):
        run_consistency_sweep(metadata, storage)

    assert any("orphaned metadata" in r.message for r in caplog.records)


def test_sweep_multiple_orphans(deps):
    storage, metadata = deps
    _seed_object(deps, "b", "k1")
    _seed_object(deps, "b", "k2")
    _object_path(deps, "b", "k1").unlink()
    _object_path(deps, "b", "k2").unlink()

    report = run_consistency_sweep(metadata, storage)
    assert len(report.issues) == 2
    assert all(isinstance(i, OrphanedMetadata) for i in report.issues)


# --- missing part ---


def test_sweep_detects_missing_part(deps):
    """Part file deleted after being recorded in metadata."""
    storage, metadata = deps
    metadata.create_bucket("b", _NOW)
    metadata.create_upload("uid-1", "b", "k", _NOW)
    _seed_part(deps, "uid-1", 1, b"part-data")

    # Delete part file directly
    _part_path(deps, "uid-1", 1).unlink()

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
    _seed_part(deps, "uid-1", 1, b"part-data")
    _part_path(deps, "uid-1", 1).unlink()

    with caplog.at_level(logging.WARNING, logger="server.consistency"):
        run_consistency_sweep(metadata, storage)

    assert any("missing part" in r.message for r in caplog.records)


def test_sweep_completed_uploads_not_checked(deps):
    """Completed uploads are not in list_incomplete_uploads — parts need not exist."""
    storage, metadata = deps
    metadata.create_bucket("b", _NOW)
    metadata.create_upload("uid-done", "b", "k", _NOW)
    part_blob = metadata.register_blob("part", "b", "k", upload_id="uid-done", part_number=1)
    metadata.record_part("uid-done", PartMeta("uid-done", 1, '"etag"', 5, part_blob))
    object_blob = metadata.register_blob("object", "b", "k", upload_id="uid-done")
    etag, size = storage.write_blob(object_blob, io.BytesIO(b"assembled"))
    metadata.commit_completed_upload(
        "uid-done",
        ObjectMeta("b", "k", size, etag, _NOW, "binary/octet-stream", object_blob, "uid-done"),
        part_blob_ids={1: part_blob},
    )
    # No part file on disk — should not flag as MissingPart since upload is complete

    report = run_consistency_sweep(metadata, storage)
    assert report.clean


# --- server starts regardless of issues ---


def test_server_starts_with_orphaned_metadata(deps):
    _first_start(deps)
    storage, metadata = deps
    _seed_object(deps, "b", "orphan.txt")
    _object_path(deps, "b", "orphan.txt").unlink()

    # Server must start and /health must return 200 even with issues
    app = create_app(storage=storage, metadata=metadata)
    with TestClient(app, raise_server_exceptions=False) as client:
        resp = client.get("/health")
    assert resp.status_code == 200


def test_server_accepts_requests_with_sweep_issues(deps):
    _first_start(deps)
    storage, metadata = deps
    _seed_object(deps, "b", "orphan.txt")
    _object_path(deps, "b", "orphan.txt").unlink()

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
