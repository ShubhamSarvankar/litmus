"""
Tier A: a refused aws-chunked request with a CRC32, CRC32C, SHA1 or SHA256 trailer
reaches no durable step, as the CRC64NVME one does in test_barrier_order.py
(docs/design/storage-layout.md §16): the temp file is written, but never fsynced,
renamed or made live, and nothing is left on disk.
"""

import base64
import os
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from test_barrier_order import Recorder

from chaos.framework.framed import frame, framed_headers
from server.app import create_app
from server.checksums import ALGORITHMS
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

pytestmark = pytest.mark.skipif(os.name == "nt", reason="directory fsync is POSIX-only")

BUCKET = "barrier"
KEY = "obj"


@pytest.fixture
def env(tmp_path: Path, monkeypatch):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    recorder = Recorder(monkeypatch, metadata)
    assert client.put(f"/{BUCKET}").status_code == 200
    upload = client.post(f"/{BUCKET}/{KEY}?uploads")
    upload_id = ET.fromstring(upload.text).findtext("{*}UploadId")
    return client, storage, recorder, upload_id


def _wrong_digest(name: str, data: bytes) -> str:
    hasher = ALGORITHMS[name].new()
    hasher.update(data + b"x")
    return base64.b64encode(hasher.digest()).decode("ascii")


@pytest.mark.parametrize("kind", ["put", "part"])
@pytest.mark.parametrize("name", ["crc32", "crc32c", "sha1", "sha256"])
def test_refused_framed_write_reaches_no_durable_step(env, kind, name):
    client, storage, recorder, upload_id = env
    header = ALGORITHMS[name].header
    data = b"refused body" * 1000
    body = frame(data, trailer_line=f"{header}:{_wrong_digest(name, data)}")
    headers = framed_headers(body, len(data), **{"x-amz-trailer": header})
    url = f"/{BUCKET}/{KEY}"
    if kind == "part":
        url += f"?partNumber=1&uploadId={upload_id}"
    recorder.active = True
    resp = client.put(url, content=body, headers=headers)
    recorder.active = False
    assert resp.status_code == 400
    assert ET.fromstring(resp.text).findtext("Code") == "BadDigest"
    kinds = [e[0] for e in recorder.events]
    assert "fsync-file" not in kinds and "rename" not in kinds
    assert not any(e[0] == "sql" and "SET state = 'live'" in e[1] for e in recorder.events)
    assert [e for e, _ in storage.scan()] == [] and storage.temp_entries() == []
