"""
Tier A: the checksum of a CopyObject whose source is overwritten while it reads
(docs/design/storage-layout.md decision 33, §9, §20).

The copy holds the source blob's descriptor, so it copies the bytes of the row it opened. The
digest it stores is that of those bytes, whatever the source is now, and agrees with the
ETag and the body of the destination.
"""

import base64
import hashlib
import zlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "copy-cksum-race"
V1 = b"first version of the source. " * 500
V2 = b"SECOND version, not what was copied. " * 400


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    app = create_app(storage=storage, metadata=metadata)
    clients = [TestClient(app, raise_server_exceptions=False) for _ in range(2)]
    for c in clients:  # build the middleware stack before the requests overlap
        assert c.get("/health").status_code == 200
    assert clients[0].put(f"/{BUCKET}").status_code == 200
    return clients, storage, metadata


def _crc32_b64(data: bytes) -> str:
    return base64.b64encode(zlib.crc32(data).to_bytes(4, "big")).decode()


def test_a_source_overwritten_mid_copy_gives_a_checksum_of_the_bytes_copied(env, monkeypatch):
    (c, other), storage, metadata = env
    claim = {"x-amz-checksum-crc32": _crc32_b64(V1)}
    assert c.put(f"/{BUCKET}/src", content=V1, headers=claim).status_code == 200
    real = storage.open_blob
    fired = []

    def open_blob(blob_id):
        f = real(blob_id)  # the descriptor is held; now the source is replaced
        if not fired:
            fired.append(blob_id)
            replaced = other.put(
                f"/{BUCKET}/src", content=V2, headers={"x-amz-checksum-crc32": _crc32_b64(V2)}
            )
            assert replaced.status_code == 200
        return f

    monkeypatch.setattr(storage, "open_blob", open_blob)
    resp = c.put(f"/{BUCKET}/dst", headers={"x-amz-copy-source": f"{BUCKET}/src"})
    monkeypatch.undo()

    assert resp.status_code == 200, resp.text
    row = metadata.get_object_meta(BUCKET, "dst")
    with storage.open_blob(row.blob_id) as f:
        copied = f.read()
    assert copied == V1 and c.get(f"/{BUCKET}/src").content == V2
    assert row.etag == '"' + hashlib.md5(V1).hexdigest() + '"'
    assert (row.checksum, row.checksum_algorithm, row.checksum_type) == (
        _crc32_b64(V1),
        "CRC32",
        "FULL_OBJECT",
    )
    assert f"<ChecksumCRC32>{_crc32_b64(V1)}</ChecksumCRC32>" in resp.text
    got = c.get(f"/{BUCKET}/dst", headers={"x-amz-checksum-mode": "ENABLED"})
    assert got.content == V1 and got.headers["x-amz-checksum-crc32"] == _crc32_b64(V1)
    assert [r.state for r in metadata.list_blob_records()] == ["live"] * 2
    assert storage.temp_entries() == []
    assert run_consistency_sweep(metadata, storage).issues == []
