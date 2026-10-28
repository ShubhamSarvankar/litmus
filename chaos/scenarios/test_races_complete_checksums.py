"""
Tier A: CompleteMultipartUpload checksums under races and a full disk
(docs/design/storage-layout.md §6.7, §9, §20).

The object's checksum is computed from the part rows read before the assembly and is
committed with the completed state, so a Complete whose part changed in between is refused
(InvalidPart) and leaves the upload open with the new part's own checksum; of two Completes
of one upload the loser changes nothing and the winner's stored checksum is the checksum of
its stored bytes; and a full disk during the assembly leaves the upload open with every
part checksum as it was. The digests come from awscrt called directly on the bytes.
"""

import base64
import errno
import threading
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from awscrt import checksums as crt
from fastapi.testclient import TestClient

from chaos.faults.faults import RaiseOSError
from chaos.fixtures.inprocess import make_faulty_server
from chaos.framework.injector import FaultInjector
from chaos.framework.triggers import CallCountTrigger
from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "complete-cksum-race"
KEY = "obj"
WAIT = 10.0  # safety net only: no outcome depends on it
PARTS = [b"A" * 30_000, b"B" * 31_000]
MODE = {"x-amz-checksum-mode": "ENABLED"}


def _crc64(data: bytes) -> str:
    return base64.b64encode(crt.crc64nvme(data).to_bytes(8, "big")).decode()


def _upload(client, parts: list[bytes] = PARTS) -> tuple[str, list[str]]:
    uid = ET.fromstring(client.post(f"/{BUCKET}/{KEY}?uploads").text).findtext("UploadId")
    etags = []
    for number, data in enumerate(parts, start=1):
        r = client.put(f"/{BUCKET}/{KEY}?partNumber={number}&uploadId={uid}", content=data)
        assert r.status_code == 200
        etags.append(r.headers["etag"])
    return uid, etags


def _complete(client, uid: str, etags: list[str]):
    items = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>"
        for n, e in enumerate(etags, start=1)
    )
    return client.post(
        f"/{BUCKET}/{KEY}?uploadId={uid}",
        content=f"<CompleteMultipartUpload>{items}</CompleteMultipartUpload>".encode(),
    )


def _run(calls) -> list:
    """Run each callable on its own thread; the results in order."""
    results = [None] * len(calls)

    def work(i, call):
        results[i] = call()

    threads = [threading.Thread(target=work, args=(i, c)) for i, c in enumerate(calls)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(WAIT * 3)
    assert not any(t.is_alive() for t in threads)
    return results


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    app = create_app(storage=storage, metadata=metadata)
    clients = [TestClient(app, raise_server_exceptions=False) for _ in range(2)]
    for c in clients:  # build the middleware stack before threads can race to build it
        assert c.get("/health").status_code == 200
    assert clients[0].put(f"/{BUCKET}").status_code == 200
    return clients, storage, metadata


def test_a_part_uploaded_again_after_the_checksum_was_read_is_invalid_part(env, monkeypatch):
    (reader, writer), storage, metadata = env
    uid, etags = _upload(reader)
    changed = b"changed" * 1000
    reuploads = []
    real = metadata.commit_completed_upload

    def commit_completed_upload(*args, **kwargs):
        # The checksum was computed from the old part rows; a part changes before the commit.
        reuploads.append(
            writer.put(f"/{BUCKET}/{KEY}?partNumber=1&uploadId={uid}", content=changed)
        )
        return real(*args, **kwargs)

    monkeypatch.setattr(metadata, "commit_completed_upload", commit_completed_upload)
    resp = _complete(reader, uid, etags)
    monkeypatch.undo()

    assert [r.status_code for r in reuploads] == [200]
    assert (resp.status_code, ET.fromstring(resp.text).findtext("Code")) == (400, "InvalidPart")
    assert metadata.get_object_meta(BUCKET, KEY) is None
    # The upload stays open; part 1 is the new one, with its own checksum.
    assert metadata.upload_exists(uid, bucket=BUCKET, key=KEY)
    parts = metadata.get_parts(uid)
    assert [p.checksum for p in parts] == [_crc64(changed), _crc64(PARTS[1])]
    # Listing the new ETag completes it, and the checksum is that of the new bytes.
    done = _complete(reader, uid, [parts[0].etag, etags[1]])
    assert done.status_code == 200, done.text
    assert ET.fromstring(done.text).findtext("ChecksumCRC64NVME") == _crc64(changed + PARTS[1])
    assert run_consistency_sweep(metadata, storage).issues == []


def test_two_completes_of_one_upload_leave_one_winner_whose_checksum_matches_its_bytes(
    env, monkeypatch
):
    (first, second), storage, metadata = env
    uid, etags = _upload(first)
    barrier = threading.Barrier(2, timeout=WAIT)
    real = metadata.commit_completed_upload

    def commit_completed_upload(*args, **kwargs):
        barrier.wait()  # both have assembled their blobs
        return real(*args, **kwargs)

    monkeypatch.setattr(metadata, "commit_completed_upload", commit_completed_upload)
    responses = _run([lambda c=c: _complete(c, uid, etags) for c in (first, second)])
    monkeypatch.undo()

    statuses = sorted(r.status_code for r in responses)
    assert statuses == [200, 404]
    winner = next(r for r in responses if r.status_code == 200)
    loser = next(r for r in responses if r.status_code == 404)
    assert ET.fromstring(loser.text).findtext("Code") == "NoSuchUpload"

    meta = metadata.get_object_meta(BUCKET, KEY)
    with storage.open_blob(meta.blob_id) as f:
        stored = f.read()
    assert stored == b"".join(PARTS)
    assert (meta.checksum, meta.checksum_algorithm, meta.checksum_type) == (
        _crc64(stored),
        "CRC64NVME",
        "FULL_OBJECT",
    )
    assert ET.fromstring(winner.text).findtext("ChecksumCRC64NVME") == meta.checksum
    served = first.get(f"/{BUCKET}/{KEY}", headers=MODE)
    assert served.headers["x-amz-checksum-crc64nvme"] == _crc64(stored)
    assert [r.state for r in metadata.list_blob_records()] == ["live"]
    assert run_consistency_sweep(metadata, storage).issues == []


def test_a_full_disk_during_the_assembly_leaves_the_upload_open_with_its_checksums(
    tmp_path: Path,
):
    injector = FaultInjector(
        method="assemble_blob",
        trigger=CallCountTrigger(fire_on=1),
        fault=RaiseOSError(errno.ENOSPC),
    )
    client, _, metadata, storage = make_faulty_server(tmp_path, injector)
    with client:
        assert client.put(f"/{BUCKET}").status_code == 200
        uid, etags = _upload(client)
        refused = _complete(client, uid, etags)
        assert refused.status_code in (400, 500, 503)

        assert metadata.upload_exists(uid, bucket=BUCKET, key=KEY)
        assert metadata.get_object_meta(BUCKET, KEY) is None
        parts = metadata.get_parts(uid)
        assert [(p.checksum, p.checksum_algorithm) for p in parts] == [
            (_crc64(data), "CRC64NVME") for data in PARTS
        ]
        assert all(p.blob_id is not None for p in parts)
        assert storage.temp_entries() == []

        # The disk has room again: the same Complete works, with the combined checksum.
        done = _complete(client, uid, etags)
        assert done.status_code == 200, done.text
        assert ET.fromstring(done.text).findtext("ChecksumCRC64NVME") == _crc64(b"".join(PARTS))
    assert run_consistency_sweep(metadata, storage).issues == []
