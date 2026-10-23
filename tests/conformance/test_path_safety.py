"""Keys and bucket names can never reach a path outside their directory.

Paths use %2F / %2E%2E so the HTTP client sends them unnormalized; the server sees
the decoded path (e.g. /bucket/../../escape.txt), exactly as from a client that does
not normalize dot segments.
"""

import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "test-bucket"
OTHER = "other-bucket"

TRAVERSAL_KEYS = [
    "..%2F..%2Fescape.txt",  # out of the bucket and the objects dir, next to meta.db
    "..%2F..%2F..%2Fescape.txt",  # out of the data dir entirely
    "..%2Fother-bucket%2Fplanted.txt",  # into another bucket
    "a%2F..%2F..%2Fescape.txt",  # climbs out after a normal segment
    "a%2F..%2Fb",  # stays inside but aliases key "b"
    "%2E%2E",  # the bucket's parent directory itself
    "%2E",  # the bucket directory itself
]


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    assert client.put(f"/{BUCKET}").status_code == 200
    assert client.put(f"/{OTHER}").status_code == 200
    sentinel = tmp_path / "sentinel.txt"
    sentinel.write_bytes(b"outside storage")
    return client, tmp_path, sentinel


def _all_files(root: Path) -> set[Path]:
    return {p for p in root.rglob("*") if p.is_file()}


@pytest.mark.parametrize("key", TRAVERSAL_KEYS)
def test_put_with_traversal_key_is_rejected_and_writes_nothing(env, key):
    client, tmp_path, _ = env
    before = _all_files(tmp_path)
    resp = client.put(f"/{BUCKET}/{key}", content=b"payload")
    assert resp.status_code == 400
    assert ET.fromstring(resp.text).findtext("Code") == "InvalidArgument"
    # meta.db may grow WAL pages from unrelated reads; no new files of any kind.
    assert {p for p in _all_files(tmp_path) if p.suffix not in (".db-wal", ".db-shm")} <= before


@pytest.mark.parametrize("key", TRAVERSAL_KEYS)
@pytest.mark.parametrize("method", ["GET", "HEAD", "DELETE"])
def test_read_and_delete_with_traversal_key_are_rejected(env, key, method):
    client, tmp_path, sentinel = env
    resp = client.request(method, f"/{BUCKET}/{key}")
    assert resp.status_code == 400
    assert sentinel.read_bytes() == b"outside storage"
    assert (tmp_path / "meta.db").exists()


def test_delete_cannot_unlink_a_file_outside_storage(env):
    client, _, sentinel = env
    assert client.delete(f"/{BUCKET}/..%2F..%2Fsentinel.txt").status_code == 400
    assert sentinel.exists()


def test_delete_objects_body_cannot_unlink_files_outside_storage(env):
    client, tmp_path, sentinel = env
    assert client.put(f"/{BUCKET}/ok.txt", content=b"ok").status_code == 200
    body = (
        b"<Delete>"
        b"<Object><Key>../../sentinel.txt</Key></Object>"
        b"<Object><Key>../../meta.db</Key></Object>"
        b"<Object><Key>ok.txt</Key></Object>"
        b"</Delete>"
    )
    resp = client.post(f"/{BUCKET}?delete", content=body)
    assert resp.status_code == 200
    root = ET.fromstring(resp.text)
    errors = {e.findtext("Key"): e.findtext("Code") for e in root.findall("Error")}
    deleted = [d.findtext("Key") for d in root.findall("Deleted")]
    assert errors == {"../../sentinel.txt": "InvalidArgument", "../../meta.db": "InvalidArgument"}
    assert deleted == ["ok.txt"]
    assert sentinel.read_bytes() == b"outside storage"
    assert (tmp_path / "meta.db").exists()


def test_traversal_key_cannot_plant_an_object_in_another_bucket(env):
    client, tmp_path, _ = env
    client.put(f"/{BUCKET}/..%2F{OTHER}%2Fplanted.txt", content=b"planted")
    assert not (tmp_path / "objects" / OTHER / "planted.txt").exists()
    assert "<Key>" not in client.get(f"/{OTHER}").text


@pytest.mark.parametrize("bucket", ["%2E%2E", "%2E"])
def test_dot_bucket_names_are_rejected(env, bucket):
    client, tmp_path, _ = env
    for method in ["PUT", "GET", "HEAD", "DELETE"]:
        assert client.request(method, f"/{bucket}").status_code == 400
    resp = client.put(f"/{bucket}/escape.txt", content=b"x")
    assert resp.status_code == 400
    assert not list(tmp_path.rglob("escape.txt"))


def test_multipart_with_traversal_key_is_rejected(env):
    client, tmp_path, _ = env
    assert client.post(f"/{BUCKET}/..%2F..%2Fescape.txt?uploads").status_code == 400
    assert not list(tmp_path.rglob("escape.txt"))


def test_ordinary_keys_with_dots_and_slashes_still_work(env):
    client, _, _ = env
    for key in ["a.b", "..hidden", "dir/file..txt", "x/.../y", "a/b/c.txt"]:
        assert client.put(f"/{BUCKET}/{key}", content=key.encode()).status_code == 200
        assert client.get(f"/{BUCKET}/{key}").content == key.encode()
