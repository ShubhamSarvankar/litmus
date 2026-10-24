"""Keys and bucket names can never reach a path outside their directory.

Keys are opaque (docs/design/storage-layout.md R16): a key with "." or ".." segments
is stored and read back as exactly that string, and since no key is ever part of a
filesystem path, it cannot touch anything outside the blob store. Bucket names "."
and ".." stay rejected; S3's bucket naming rules forbid them.

httpx (behind TestClient) resolves literal dot segments before sending, so
"/bucket/../x" would go out as "/x". The tests send dot segments percent-encoded
(%2E%2E), which the client leaves alone and the server decodes back to "..", exactly
as a client that does not normalize sends them. Every round trip confirms through
ListObjects that the stored key is the literal key.
"""

import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import quote

import httpx
import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "test-bucket"
OTHER = "other-bucket"

TRAVERSAL_KEYS = [
    "../../escape.txt",  # would climb out of the bucket and the objects dir
    "../../../escape.txt",  # would climb out of the data dir entirely
    "../other-bucket/planted.txt",  # would land in another bucket
    "a/../../escape.txt",  # would climb out after a normal segment
    "a/../b",  # would alias key "b"
    "..",  # would be the bucket's parent directory itself
    ".",  # would be the bucket directory itself
]


def _path(key: str) -> str:
    """The request path for a key, with dot segments percent-encoded so the client
    sends them unnormalized."""
    segments = ["%2E" * len(s) if s in (".", "..") else quote(s, safe="") for s in key.split("/")]
    return "/".join(segments)


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


def _new_files_outside_storage(tmp_path: Path, before: set[Path]) -> set[Path]:
    """Files created since `before` anywhere but in the blob store (database WAL
    files aside)."""
    return {
        p
        for p in _all_files(tmp_path) - before
        if not p.is_relative_to(tmp_path / "blobs") and p.suffix not in (".db-wal", ".db-shm")
    }


def _listed(client, bucket: str = BUCKET) -> list[str]:
    root = ET.fromstring(client.get(f"/{bucket}").text)
    return [el.text for el in root.iter("Key")]


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


@pytest.mark.parametrize("key", TRAVERSAL_KEYS)
def test_put_with_traversal_key_stores_the_literal_key_inside_storage(env, key):
    client, tmp_path, sentinel = env
    before = _all_files(tmp_path)
    resp = client.put(f"/{BUCKET}/{_path(key)}", content=b"payload")
    assert resp.status_code == 200
    # Stored under exactly the literal key, and read back from it.
    assert _listed(client) == [key]
    got = client.get(f"/{BUCKET}/{_path(key)}")
    assert got.status_code == 200
    assert got.content == b"payload"
    assert got.headers["etag"] == _md5(b"payload")
    # No file appeared outside the blob store; nothing outside it changed.
    assert _new_files_outside_storage(tmp_path, before) == set()
    assert sentinel.read_bytes() == b"outside storage"
    assert (tmp_path / "meta.db").exists()


@pytest.mark.parametrize("key", TRAVERSAL_KEYS)
@pytest.mark.parametrize("method", ["GET", "HEAD", "DELETE"])
def test_read_and_delete_with_traversal_key_act_on_the_literal_key(env, key, method):
    client, tmp_path, sentinel = env
    assert client.put(f"/{BUCKET}/{_path(key)}", content=b"payload").status_code == 200
    assert _listed(client) == [key]
    resp = client.request(method, f"/{BUCKET}/{_path(key)}")
    if method == "DELETE":
        assert resp.status_code == 204
        assert _listed(client) == []
        assert client.get(f"/{BUCKET}/{_path(key)}").status_code == 404
    else:
        assert resp.status_code == 200
        assert resp.headers["etag"] == _md5(b"payload")
        if method == "GET":
            assert resp.content == b"payload"
        assert _listed(client) == [key]
    assert sentinel.read_bytes() == b"outside storage"
    assert (tmp_path / "meta.db").exists()


def test_delete_cannot_unlink_a_file_outside_storage(env):
    client, _, sentinel = env
    # No object has this literal key: S3 answers 204, and nothing outside changes.
    assert client.delete(f"/{BUCKET}/{_path('../../sentinel.txt')}").status_code == 204
    assert sentinel.read_bytes() == b"outside storage"


def test_delete_objects_body_cannot_unlink_files_outside_storage(env):
    client, tmp_path, sentinel = env
    assert client.put(f"/{BUCKET}/ok.txt", content=b"ok").status_code == 200
    # An object whose literal key names the sentinel's path.
    assert client.put(f"/{BUCKET}/{_path('../../sentinel.txt')}", content=b"obj").status_code == 200
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
    assert root.findall("Error") == []
    deleted = [d.findtext("Key") for d in root.findall("Deleted")]
    assert deleted == ["../../sentinel.txt", "../../meta.db", "ok.txt"]
    assert _listed(client) == []
    assert sentinel.read_bytes() == b"outside storage"
    assert (tmp_path / "meta.db").exists()


def test_traversal_key_cannot_plant_an_object_in_another_bucket(env):
    client, tmp_path, _ = env
    key = f"../{OTHER}/planted.txt"
    assert client.put(f"/{BUCKET}/{_path(key)}", content=b"planted").status_code == 200
    assert _listed(client, BUCKET) == [key]
    assert _listed(client, OTHER) == []
    assert client.get(f"/{OTHER}/planted.txt").status_code == 404
    assert not list(tmp_path.rglob("planted.txt"))


@pytest.mark.parametrize("bucket", ["%2E%2E", "%2E"])
def test_dot_bucket_names_are_rejected(env, bucket):
    client, tmp_path, _ = env
    for method in ["PUT", "GET", "HEAD", "DELETE"]:
        assert client.request(method, f"/{bucket}").status_code == 400
    resp = client.put(f"/{bucket}/escape.txt", content=b"x")
    assert resp.status_code == 400
    assert not list(tmp_path.rglob("escape.txt"))


def test_multipart_with_traversal_key_stores_the_literal_key(env):
    client, tmp_path, sentinel = env
    key = "../../escape.txt"
    before = _all_files(tmp_path)
    resp = client.post(f"/{BUCKET}/{_path(key)}?uploads")
    assert resp.status_code == 200
    upload_id = ET.fromstring(resp.text).findtext("UploadId")
    part = client.put(
        f"/{BUCKET}/{_path(key)}?partNumber=1&uploadId={upload_id}", content=b"p" * 100
    )
    assert part.status_code == 200
    complete = (
        b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"
        + part.headers["etag"].encode()
        + b"</ETag></Part></CompleteMultipartUpload>"
    )
    assert (
        client.post(f"/{BUCKET}/{_path(key)}?uploadId={upload_id}", content=complete).status_code
        == 200
    )
    assert _listed(client) == [key]
    assert client.get(f"/{BUCKET}/{_path(key)}").content == b"p" * 100
    assert not list(tmp_path.rglob("escape.txt"))
    assert _new_files_outside_storage(tmp_path, before) == set()
    assert sentinel.read_bytes() == b"outside storage"


def test_ordinary_keys_with_dots_and_slashes_still_work(env):
    client, _, _ = env
    keys = ["a.b", "..hidden", "dir/file..txt", "x/.../y", "a/b/c.txt"]
    for key in keys:
        assert client.put(f"/{BUCKET}/{key}", content=key.encode()).status_code == 200
        assert client.get(f"/{BUCKET}/{key}").content == key.encode()
    assert sorted(_listed(client)) == sorted(keys)


@pytest.mark.parametrize(
    "encoded,key",
    [
        ("..%2F..%2Fescape.txt", "../../escape.txt"),
        ("a%2F..%2Fb", "a/../b"),
        ("%2E%2E%2Fx", "../x"),
    ],
)
def test_encoded_slashes_store_the_same_literal_key(env, encoded, key):
    """%2F decodes to "/" on the server: these spellings name the same literal key."""
    client, _, _ = env
    assert client.put(f"/{BUCKET}/{encoded}", content=b"x").status_code == 200
    assert _listed(client) == [key]
    assert client.get(f"/{BUCKET}/{_path(key)}").content == b"x"


def test_the_client_sends_dot_segments_unnormalized():
    """Guard for the helper: httpx would resolve literal dot segments, not encoded ones."""
    assert httpx.URL("http://x/b/a/../c").raw_path == b"/b/c"
    assert httpx.URL(f"http://x/b/{_path('a/../c')}").raw_path == b"/b/a/%2E%2E/c"
