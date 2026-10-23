"""No client string can reach a filesystem path (docs/design/storage-layout.md R19).

The storage layer takes only blob IDs and refuses anything else before touching the
disk; buckets, keys, upload IDs and part numbers never become path components."""

import io
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore, InvalidBlobId, UnsafePathError, _contained

NOT_IDS = [
    "../escape",
    "../../escape",
    "a/../../escape",
    "..",
    ".",
    "../other/k",
    "0" * 30 + "/.",
    "../" + "0" * 29,
    "uid-1",
    "b/k",
]


@pytest.fixture
def backend(tmp_path: Path) -> BlobStore:
    store = BlobStore(tmp_path / "blobs")
    store.create_tree()
    return store


def _files(root: Path) -> set[Path]:
    return {p for p in root.rglob("*") if p.is_file()}


@pytest.mark.parametrize("name", NOT_IDS)
def test_write_refuses_a_non_id_and_writes_nothing(backend, tmp_path, name):
    with pytest.raises(InvalidBlobId):
        backend.write_blob(name, io.BytesIO(b"x"))
    assert not _files(tmp_path)


@pytest.mark.parametrize("name", NOT_IDS)
def test_read_and_remove_refuse_a_non_id(backend, name):
    with pytest.raises(InvalidBlobId):
        backend.open_blob(name)
    with pytest.raises(InvalidBlobId):
        backend.remove_blob(name)
    with pytest.raises(InvalidBlobId):
        backend.blob_size(name)


@pytest.mark.parametrize("name", NOT_IDS)
def test_assemble_refuses_a_non_id_target_or_part(backend, tmp_path, name):
    with pytest.raises(InvalidBlobId):
        backend.assemble_blob(name, [])
    with pytest.raises(InvalidBlobId):
        backend.assemble_blob("0" * 32, [name])
    assert not _files(tmp_path)


@pytest.mark.parametrize("names", [("..",), ("../x",), ("a", "..", ".."), ("",)])
def test_containment_check_still_refuses_escapes(tmp_path, names):
    with pytest.raises(UnsafePathError):
        _contained(tmp_path, *names)


def test_client_strings_never_appear_in_paths(tmp_path):
    """After PUTs and a multipart upload with awkward bucket, key and upload ID
    strings, every file under blobs/ is <xx>/<id>, and no path component is a client
    string."""
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(create_app(storage=storage, metadata=metadata))
    bucket = "my.bucket-1"
    keys = ["k", "k/", "a/b", "a/b/c", "dir/sub/file..txt", "x/.../y", "sp ace", "ünï"]
    assert client.put(f"/{bucket}").status_code == 200
    for key in keys:
        assert client.put(f"/{bucket}/{key}", content=key.encode()).status_code == 200
    resp = client.post(f"/{bucket}/mp/key?uploads")
    upload_id = ET.fromstring(resp.text).findtext("UploadId")
    assert (
        client.put(f"/{bucket}/mp/key?partNumber=1&uploadId={upload_id}", content=b"p").status_code
        == 200
    )

    entries = list(storage.scan())
    assert entries and all(blob_id is not None for _, blob_id in entries)
    client_strings = {bucket, upload_id, "mp", "1", *keys, *(s for k in keys for s in k.split("/"))}
    for path in _files(tmp_path / "blobs"):
        assert not set(path.relative_to(tmp_path / "blobs").parts) & client_strings
    for key in keys:
        assert client.get(f"/{bucket}/{key}").content == key.encode()
