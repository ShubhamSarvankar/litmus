"""
Bucket conformance tests.

boto3 requires a real TCP port; TestClient does not bind one. These tests use
httpx directly against the TestClient ASGI transport, which gives us full S3
XML protocol validation without a subprocess. The boto3 smoke tests (aws s3 mb,
aws s3 ls) are run manually against a live server — see README.
"""

import io
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.base import ObjectMeta
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


@pytest.fixture
def client(deps) -> TestClient:
    storage, metadata = deps
    app = create_app(storage=storage, metadata=metadata)
    return TestClient(app, raise_server_exceptions=False)


# --- helpers ---


def _xml(text: str) -> ET.Element:
    return ET.fromstring(text)


def _error_code(response) -> str:
    return _xml(response.text).findtext("Code")


def _seed_object(deps, bucket: str, key: str, body: bytes = b"data") -> None:
    """Directly write an object via storage+metadata layers (object route not yet wired)."""
    storage, metadata = deps
    etag = storage.write_object(bucket, key, io.BytesIO(body), len(body))
    metadata.put_object_meta(
        bucket,
        key,
        ObjectMeta(
            bucket=bucket,
            key=key,
            size=len(body),
            etag=etag,
            last_modified=datetime(2024, 1, 1),
            content_type="application/octet-stream",
        ),
    )


# --- create bucket ---


def test_create_bucket_returns_200(client):
    resp = client.put("/my-bucket")
    assert resp.status_code == 200


def test_create_bucket_appears_in_list(client):
    client.put("/my-bucket")
    resp = client.get("/")
    names = [el.text for el in _xml(resp.text).findall(".//Name")]
    assert "my-bucket" in names


def test_create_duplicate_bucket_returns_409(client):
    client.put("/dupe")
    resp = client.put("/dupe")
    assert resp.status_code == 409


def test_create_duplicate_bucket_error_code(client):
    client.put("/dupe")
    resp = client.put("/dupe")
    assert _error_code(resp) == "BucketAlreadyExists"


# --- delete bucket ---


def test_delete_empty_bucket_returns_204(client):
    client.put("/to-delete")
    resp = client.delete("/to-delete")
    assert resp.status_code == 204


def test_delete_bucket_gone_from_list(client):
    client.put("/to-delete")
    client.delete("/to-delete")
    resp = client.get("/")
    names = [el.text for el in _xml(resp.text).findall(".//Name")]
    assert "to-delete" not in names


def test_delete_nonexistent_bucket_returns_404(client):
    resp = client.delete("/no-such-bucket")
    assert resp.status_code == 404


def test_delete_nonexistent_bucket_error_code(client):
    resp = client.delete("/no-such-bucket")
    assert _error_code(resp) == "NoSuchBucket"


def test_delete_nonempty_bucket_returns_409(client, deps):
    client.put("/nonempty")
    _seed_object(deps, "nonempty", "file.txt")
    resp = client.delete("/nonempty")
    assert resp.status_code == 409


def test_delete_nonempty_bucket_error_code(client, deps):
    client.put("/nonempty")
    _seed_object(deps, "nonempty", "file.txt")
    resp = client.delete("/nonempty")
    assert _error_code(resp) == "BucketNotEmpty"


# --- list buckets ---


def test_list_buckets_returns_200(client):
    resp = client.get("/")
    assert resp.status_code == 200


def test_list_buckets_content_type_xml(client):
    resp = client.get("/")
    assert "application/xml" in resp.headers["content-type"]


def test_list_buckets_correct_names(client):
    for name in ("alpha", "beta", "gamma"):
        client.put(f"/{name}")
    resp = client.get("/")
    names = [el.text for el in _xml(resp.text).findall(".//Name")]
    assert set(names) == {"alpha", "beta", "gamma"}


def test_list_buckets_sorted(client):
    for name in ("zzz", "aaa", "mmm"):
        client.put(f"/{name}")
    resp = client.get("/")
    names = [el.text for el in _xml(resp.text).findall(".//Name")]
    assert names == sorted(names)


def test_list_buckets_has_creation_date(client):
    client.put("/dated")
    resp = client.get("/")
    dates = [el.text for el in _xml(resp.text).findall(".//CreationDate")]
    assert len(dates) == 1
    assert dates[0] is not None


def test_list_buckets_empty(client):
    resp = client.get("/")
    buckets = _xml(resp.text).findall(".//Bucket")
    assert buckets == []


# --- error responses are valid XML ---


def test_error_response_is_xml(client):
    resp = client.delete("/no-such-bucket")
    root = _xml(resp.text)
    assert root.tag == "Error"
    assert root.findtext("Code") is not None
    assert root.findtext("Message") is not None


def test_error_response_content_type(client):
    resp = client.delete("/no-such-bucket")
    assert "application/xml" in resp.headers["content-type"]
