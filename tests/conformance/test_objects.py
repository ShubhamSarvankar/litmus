import hashlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.filesystem import FilesystemBackend


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    storage = FilesystemBackend(
        data_dir=tmp_path / "objects",
        parts_dir=tmp_path / "parts",
    )
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    app = create_app(storage=storage, metadata=metadata)
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def bucket(client) -> str:
    client.put("/test-bucket")
    return "test-bucket"


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


# --- PUT ---


def test_put_returns_200(client, bucket):
    resp = client.put(f"/{bucket}/key.txt", content=b"hello")
    assert resp.status_code == 200


def test_put_returns_etag_header(client, bucket):
    resp = client.put(f"/{bucket}/key.txt", content=b"hello")
    assert "etag" in resp.headers


def test_put_etag_is_md5(client, bucket):
    body = b"hello world"
    resp = client.put(f"/{bucket}/key.txt", content=body)
    assert resp.headers["etag"] == _md5(body)


def test_put_nonexistent_bucket_returns_404(client):
    resp = client.put("/no-bucket/key.txt", content=b"data")
    assert resp.status_code == 404


def test_put_nonexistent_bucket_error_code(client):
    import xml.etree.ElementTree as ET

    resp = client.put("/no-bucket/key.txt", content=b"data")
    assert ET.fromstring(resp.text).findtext("Code") == "NoSuchBucket"


# --- GET ---


def test_get_returns_200(client, bucket):
    client.put(f"/{bucket}/key.txt", content=b"hello")
    resp = client.get(f"/{bucket}/key.txt")
    assert resp.status_code == 200


def test_get_returns_correct_body(client, bucket):
    body = b"the quick brown fox"
    client.put(f"/{bucket}/key.txt", content=body)
    resp = client.get(f"/{bucket}/key.txt")
    assert resp.content == body


def test_get_etag_matches_put(client, bucket):
    body = b"consistent etag"
    put_resp = client.put(f"/{bucket}/key.txt", content=body)
    get_resp = client.get(f"/{bucket}/key.txt")
    assert get_resp.headers["etag"] == put_resp.headers["etag"]


def test_get_content_length_correct(client, bucket):
    body = b"twelve bytes"
    client.put(f"/{bucket}/key.txt", content=body)
    resp = client.get(f"/{bucket}/key.txt")
    assert int(resp.headers["content-length"]) == len(body)


def test_get_nonexistent_key_returns_404(client, bucket):
    resp = client.get(f"/{bucket}/no-such-key")
    assert resp.status_code == 404


def test_get_nonexistent_key_error_code(client, bucket):
    import xml.etree.ElementTree as ET

    resp = client.get(f"/{bucket}/no-such-key")
    assert ET.fromstring(resp.text).findtext("Code") == "NoSuchKey"


def test_get_nonexistent_bucket_returns_404(client):
    resp = client.get("/no-bucket/key.txt")
    assert resp.status_code == 404


# --- HEAD ---


def test_head_returns_200(client, bucket):
    client.put(f"/{bucket}/key.txt", content=b"data")
    resp = client.head(f"/{bucket}/key.txt")
    assert resp.status_code == 200


def test_head_has_no_body(client, bucket):
    client.put(f"/{bucket}/key.txt", content=b"data")
    resp = client.head(f"/{bucket}/key.txt")
    assert resp.content == b""


def test_head_content_length(client, bucket):
    body = b"hello"
    client.put(f"/{bucket}/key.txt", content=body)
    resp = client.head(f"/{bucket}/key.txt")
    assert int(resp.headers["content-length"]) == len(body)


def test_head_etag_matches_put(client, bucket):
    body = b"etag check"
    put_resp = client.put(f"/{bucket}/key.txt", content=body)
    head_resp = client.head(f"/{bucket}/key.txt")
    assert head_resp.headers["etag"] == put_resp.headers["etag"]


def test_head_nonexistent_key_returns_404(client, bucket):
    resp = client.head(f"/{bucket}/no-such-key")
    assert resp.status_code == 404


# --- DELETE ---


def test_delete_existing_returns_204(client, bucket):
    client.put(f"/{bucket}/key.txt", content=b"data")
    resp = client.delete(f"/{bucket}/key.txt")
    assert resp.status_code == 204


def test_delete_removes_object(client, bucket):
    client.put(f"/{bucket}/key.txt", content=b"data")
    client.delete(f"/{bucket}/key.txt")
    resp = client.get(f"/{bucket}/key.txt")
    assert resp.status_code == 404


def test_delete_nonexistent_key_returns_204(client, bucket):
    # S3 spec: deleting a non-existent key is not an error
    resp = client.delete(f"/{bucket}/no-such-key")
    assert resp.status_code == 204


def test_delete_nonexistent_bucket_returns_404(client):
    resp = client.delete("/no-bucket/key.txt")
    assert resp.status_code == 404


# --- If-Match ---


def test_if_match_matching_etag_returns_200(client, bucket):
    body = b"if-match test"
    put_resp = client.put(f"/{bucket}/key.txt", content=body)
    etag = put_resp.headers["etag"]
    resp = client.get(f"/{bucket}/key.txt", headers={"If-Match": etag})
    assert resp.status_code == 200


def test_if_match_wrong_etag_returns_412(client, bucket):
    client.put(f"/{bucket}/key.txt", content=b"data")
    resp = client.get(f"/{bucket}/key.txt", headers={"If-Match": '"wrongetag"'})
    assert resp.status_code == 412


def test_if_match_wrong_etag_error_code(client, bucket):
    import xml.etree.ElementTree as ET

    client.put(f"/{bucket}/key.txt", content=b"data")
    resp = client.get(f"/{bucket}/key.txt", headers={"If-Match": '"wrongetag"'})
    assert ET.fromstring(resp.text).findtext("Code") == "PreconditionFailed"


# --- ETag consistency across PUT / GET / HEAD ---


def test_etag_consistent_put_get_head(client, bucket):
    body = b"etag consistency check"
    put_resp = client.put(f"/{bucket}/key.txt", content=body)
    get_resp = client.get(f"/{bucket}/key.txt")
    head_resp = client.head(f"/{bucket}/key.txt")
    assert put_resp.headers["etag"] == get_resp.headers["etag"] == head_resp.headers["etag"]


# --- Overwrite ---


def test_put_overwrites_existing_object(client, bucket):
    client.put(f"/{bucket}/key.txt", content=b"original")
    client.put(f"/{bucket}/key.txt", content=b"updated")
    resp = client.get(f"/{bucket}/key.txt")
    assert resp.content == b"updated"


def test_overwrite_updates_etag(client, bucket):
    client.put(f"/{bucket}/key.txt", content=b"v1")
    client.put(f"/{bucket}/key.txt", content=b"v2")
    resp = client.head(f"/{bucket}/key.txt")
    assert resp.headers["etag"] == _md5(b"v2")
