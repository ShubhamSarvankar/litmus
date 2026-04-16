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
def obj(client):
    """PUT a 100-byte object and return (bucket, key, body)."""
    body = bytes(range(100))  # 0x00..0x63
    client.put("/range-bucket")
    client.put("/range-bucket/data.bin", content=body)
    return "range-bucket", "data.bin", body


# --- basic range ---


def test_range_returns_206(client, obj):
    bucket, key, _ = obj
    resp = client.get(f"/{bucket}/{key}", headers={"Range": "bytes=0-9"})
    assert resp.status_code == 206


def test_range_first_n_bytes(client, obj):
    bucket, key, body = obj
    resp = client.get(f"/{bucket}/{key}", headers={"Range": "bytes=0-9"})
    assert resp.content == body[0:10]


def test_range_middle_slice(client, obj):
    bucket, key, body = obj
    resp = client.get(f"/{bucket}/{key}", headers={"Range": "bytes=10-19"})
    assert resp.content == body[10:20]


def test_range_last_byte(client, obj):
    bucket, key, body = obj
    resp = client.get(f"/{bucket}/{key}", headers={"Range": "bytes=99-99"})
    assert resp.content == body[99:100]


def test_range_open_end(client, obj):
    """bytes=50- should return from byte 50 to end of file."""
    bucket, key, body = obj
    resp = client.get(f"/{bucket}/{key}", headers={"Range": "bytes=50-"})
    assert resp.status_code == 206
    assert resp.content == body[50:]


def test_range_content_range_header(client, obj):
    bucket, key, body = obj
    resp = client.get(f"/{bucket}/{key}", headers={"Range": "bytes=0-9"})
    assert resp.headers["content-range"] == f"bytes 0-9/{len(body)}"


def test_range_content_length_header(client, obj):
    bucket, key, _ = obj
    resp = client.get(f"/{bucket}/{key}", headers={"Range": "bytes=0-9"})
    assert resp.headers["content-length"] == "10"


def test_range_accept_ranges_header(client, obj):
    bucket, key, _ = obj
    resp = client.get(f"/{bucket}/{key}", headers={"Range": "bytes=0-9"})
    assert resp.headers["accept-ranges"] == "bytes"


# --- suffix range ---


def test_suffix_range_returns_206(client, obj):
    bucket, key, body = obj
    resp = client.get(f"/{bucket}/{key}", headers={"Range": "bytes=-10"})
    assert resp.status_code == 206


def test_suffix_range_last_n_bytes(client, obj):
    bucket, key, body = obj
    resp = client.get(f"/{bucket}/{key}", headers={"Range": "bytes=-10"})
    assert resp.content == body[-10:]


# --- invalid / edge cases ---


def test_range_beyond_file_size_returns_416(client, obj):
    bucket, key, body = obj
    # Start beyond end of file
    resp = client.get(f"/{bucket}/{key}", headers={"Range": "bytes=200-300"})
    assert resp.status_code == 416


def test_range_start_equals_size_returns_416(client, obj):
    bucket, key, body = obj
    size = len(body)
    resp = client.get(f"/{bucket}/{key}", headers={"Range": f"bytes={size}-{size}"})
    assert resp.status_code == 416


def test_multi_range_returns_400(client, obj):
    bucket, key, _ = obj
    resp = client.get(f"/{bucket}/{key}", headers={"Range": "bytes=0-9,20-29"})
    assert resp.status_code == 400


# --- no Range header returns 200 ---


def test_no_range_header_returns_200(client, obj):
    bucket, key, body = obj
    resp = client.get(f"/{bucket}/{key}")
    assert resp.status_code == 200
    assert resp.content == body


# --- large object range (exercises chunked streaming, simulates boto3 multipart download) ---


def test_range_large_object(client, tmp_path):
    """A 10MB object sliced via range — exercises the streaming path."""
    body = b"x" * (10 * 1024 * 1024)
    client.put("/big-bucket")
    client.put("/big-bucket/big.bin", content=body)

    # Request last 1MB
    start = 9 * 1024 * 1024
    end = len(body) - 1
    resp = client.get("/big-bucket/big.bin", headers={"Range": f"bytes={start}-{end}"})
    assert resp.status_code == 206
    assert resp.content == body[start:]
    assert resp.headers["content-range"] == f"bytes {start}-{end}/{len(body)}"
