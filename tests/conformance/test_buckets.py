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
from server.config import Settings, settings
from server.metadata.base import ObjectMeta
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore


@pytest.fixture
def deps(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
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
    """Write an object through the storage and metadata layers, bypassing the HTTP route."""
    storage, metadata = deps
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
            last_modified=datetime(2024, 1, 1),
            content_type="application/octet-stream",
            blob_id=blob_id,
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


# --- list buckets: owner ---


def _owner(resp) -> tuple[str, str]:
    root = _xml(resp.text)
    owners = root.findall("Owner")
    assert len(owners) == 1
    return owners[0].findtext("ID"), owners[0].findtext("DisplayName")


def test_list_buckets_owner_defaults(client, monkeypatch):
    monkeypatch.delenv("S3_OWNER_ID", raising=False)
    monkeypatch.delenv("S3_OWNER_DISPLAY_NAME", raising=False)
    defaults = Settings()
    monkeypatch.setattr(settings, "owner_id", defaults.owner_id)
    monkeypatch.setattr(settings, "owner_display_name", defaults.owner_display_name)
    assert _owner(client.get("/")) == ("litmus", "litmus")


def test_list_buckets_owner_precedes_buckets(client):
    client.put("/alpha")
    tags = [child.tag for child in _xml(client.get("/").text)]
    assert tags == ["Owner", "Buckets"]


def test_list_buckets_owner_from_settings(client, monkeypatch):
    monkeypatch.setattr(settings, "owner_id", "id-ünïcode")
    monkeypatch.setattr(settings, "owner_display_name", 'Name <&> "quoted"')
    assert _owner(client.get("/")) == ("id-ünïcode", 'Name <&> "quoted"')


def test_list_buckets_owner_ignores_request_identity(client, monkeypatch):
    monkeypatch.setattr(settings, "owner_id", "configured-id")
    monkeypatch.setattr(settings, "owner_display_name", "Configured Name")
    headers = {
        "Authorization": (
            "AWS4-HMAC-SHA256 Credential=someoneelse/20260101/us-east-1/s3/aws4_request, "
            "SignedHeaders=host, Signature=00"
        ),
        "x-amz-expected-bucket-owner": "someoneelse",
    }
    resp = client.get("/", headers=headers)
    assert _owner(resp) == ("configured-id", "Configured Name")
    resp = client.get("/?x-id=someoneelse&owner=someoneelse")
    assert _owner(resp) == ("configured-id", "Configured Name")


def test_owner_settings_read_from_environment(monkeypatch):
    monkeypatch.setenv("S3_OWNER_ID", "env-id")
    monkeypatch.setenv("S3_OWNER_DISPLAY_NAME", "Env Name")
    s = Settings()
    assert (s.owner_id, s.owner_display_name) == ("env-id", "Env Name")


def test_owner_settings_defaults(monkeypatch):
    monkeypatch.delenv("S3_OWNER_ID", raising=False)
    monkeypatch.delenv("S3_OWNER_DISPLAY_NAME", raising=False)
    s = Settings()
    assert (s.owner_id, s.owner_display_name) == ("litmus", "litmus")


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


# --- bucket name validation (server/bucket_names.py) ---

INVALID_NAMES = [
    "a",
    "ab",
    "a" * 64,
    "foo_bar",
    "FooBar",
    "-foo",
    "foo-",
    ".foo",
    "foo.",
    "foo..bar",
    "192.168.5.4",
    "xn--bucket",
    "sthree-bucket",
    "amzn-s3-demo-bucket",
    "bucket-s3alias",
    "bucket--ol-s3",
    "bucket.mrap",
    "bucket--x-s3",
    "bucket--table-s3",
    "bucket-an",
]


@pytest.mark.parametrize("name", INVALID_NAMES)
def test_create_invalid_name_is_400_and_creates_nothing(client, deps, name):
    resp = client.put(f"/{name}")
    assert resp.status_code == 400
    assert _error_code(resp) == "InvalidBucketName"
    _storage, metadata = deps
    assert not metadata.bucket_exists(name)
    listed = [el.text for el in _xml(client.get("/").text).findall(".//Name")]
    assert listed == []


def test_create_invalid_name_error_names_the_bucket(client):
    resp = client.put("/foo_bar")
    assert _xml(resp.text).findtext("BucketName") == "foo_bar"


@pytest.mark.parametrize("name", ["abc", "a" * 63, "my.bucket-1", "0abc", "foo.-bar"])
def test_create_valid_name_is_200(client, name):
    assert client.put(f"/{name}").status_code == 200


def test_create_invalid_name_is_refused_even_when_one_is_in_the_store(client, deps):
    """The name check comes before the existence check: a legacy bucket cannot be
    re-created, it is refused as invalid, not answered 409."""
    _storage, metadata = deps
    metadata.create_bucket("Legacy_Bucket", datetime(2017, 1, 1))
    assert client.put("/Legacy_Bucket").status_code == 400


def test_invalid_name_not_in_store_is_no_such_bucket(client):
    """Only CreateBucket validates. Elsewhere a name that breaks the rules cannot
    be a bucket Litmus holds, so it is an ordinary missing bucket."""
    assert client.head("/Foo_Bar").status_code == 404
    for resp in (client.get("/Foo_Bar"), client.delete("/Foo_Bar")):
        assert resp.status_code == 404
        assert _error_code(resp) == "NoSuchBucket"
    resp = client.put("/Foo_Bar/key", content=b"x")
    assert resp.status_code == 404
    assert _error_code(resp) == "NoSuchBucket"


def test_bucket_already_in_the_store_with_an_invalid_name_stays_reachable(client, deps):
    """A store written before the rules existed (or by AWS's pre-2018 us-east-1
    rules) holds names CreateBucket now refuses; they keep working."""
    _storage, metadata = deps
    metadata.create_bucket("Legacy_Bucket", datetime(2017, 1, 1))
    assert client.head("/Legacy_Bucket").status_code == 200
    assert client.get("/").text.count("<Name>Legacy_Bucket</Name>") == 1
    assert client.put("/Legacy_Bucket/k", content=b"hello").status_code == 200
    assert client.get("/Legacy_Bucket/k").content == b"hello"
    assert client.get("/Legacy_Bucket").status_code == 200
    assert client.delete("/Legacy_Bucket/k").status_code == 204
    assert client.delete("/Legacy_Bucket").status_code == 204
    assert client.head("/Legacy_Bucket").status_code == 404


@pytest.mark.parametrize("name", ["%2E", "%2E%2E"])
def test_create_dot_segment_name_is_400(client, name):
    """httpx resolves literal dot segments, so they are sent percent-encoded."""
    resp = client.put(f"/{name}")
    assert resp.status_code == 400
    assert _error_code(resp) == "InvalidBucketName"
