"""
Tier A: PutObject checksums under racing writers on one key
(docs/design/storage-layout.md §9, §20).

The checksum is written in the same transaction as the rest of the row, so whichever
commit is last leaves a row whose checksum is that request's digest of that request's
bytes, never a mix. Each test drives one app through several TestClients, one thread
each, with a barrier in front of the commit so every request has written its bytes before
any commits. The digests are computed here with zlib, hashlib and a table CRC-64/NVME.
"""

import base64
import hashlib
import threading
import zlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "cksum-race"
KEY = "obj"
WAIT = 10.0  # safety net only: no outcome depends on it


def _crc64nvme(data: bytes) -> bytes:
    table = []
    for n in range(256):
        crc = n
        for _ in range(8):
            crc = (crc >> 1) ^ 0x9A6C9329AC4BC9B5 if crc & 1 else crc >> 1
        table.append(crc)
    crc = 0xFFFFFFFFFFFFFFFF
    for byte in data:
        crc = table[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return (crc ^ 0xFFFFFFFFFFFFFFFF).to_bytes(8, "big")


DIGESTS = {
    "CRC32": lambda d: zlib.crc32(d).to_bytes(4, "big"),
    "CRC64NVME": _crc64nvme,
    "SHA1": lambda d: hashlib.sha1(d).digest(),
    "SHA256": lambda d: hashlib.sha256(d).digest(),
}


def _b64(algorithm: str, data: bytes) -> str:
    return base64.b64encode(DIGESTS[algorithm](data)).decode()


def _claim(algorithm: str | None, data: bytes) -> dict[str, str]:
    if algorithm is None:
        return {}
    return {f"x-amz-checksum-{algorithm.lower()}": _b64(algorithm, data)}


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    app = create_app(storage=storage, metadata=metadata)
    clients = [TestClient(app, raise_server_exceptions=False) for _ in range(8)]
    for c in clients:  # build the middleware stack before threads can race to build it
        assert c.get("/health").status_code == 200
    assert clients[0].put(f"/{BUCKET}").status_code == 200
    return clients, storage, metadata


def _all_wrote_bytes_before_any_commit(monkeypatch, metadata, n: int) -> None:
    barrier = threading.Barrier(n, timeout=WAIT)
    real = metadata.put_object_meta

    def put_object_meta(*args, **kwargs):
        barrier.wait()
        return real(*args, **kwargs)

    monkeypatch.setattr(metadata, "put_object_meta", put_object_meta)


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


def _assert_winner_is_consistent(storage, metadata, responses, bodies) -> None:
    """One row; its checksum is the stored algorithm's digest of its bytes, and is the
    one the winning response returned."""
    meta = metadata.get_object_meta(BUCKET, KEY)
    with storage.open_blob(meta.blob_id) as f:
        stored = f.read()
    winners = [i for i, b in enumerate(bodies) if b == stored]
    assert len(winners) == 1
    assert (meta.checksum_algorithm in DIGESTS) and meta.checksum_type == "FULL_OBJECT"
    assert meta.checksum == _b64(meta.checksum_algorithm, stored)
    response = responses[winners[0]]
    assert response.status_code == 200
    assert response.headers[f"x-amz-checksum-{meta.checksum_algorithm.lower()}"] == meta.checksum
    assert [r.state for r in metadata.list_blob_records()] == ["live"]
    assert storage.temp_entries() == []
    assert run_consistency_sweep(metadata, storage).issues == []


def test_concurrent_puts_with_different_algorithms_leave_one_matching_row(env, monkeypatch):
    clients, storage, metadata = env
    algorithms = ["CRC32", "CRC64NVME", "SHA1", "SHA256", None]
    bodies = [bytes([65 + i]) * (20_000 + i) for i in range(5)]
    _all_wrote_bytes_before_any_commit(monkeypatch, metadata, 5)

    responses = _run(
        [
            lambda c=c, a=a, b=b: c.put(f"/{BUCKET}/{KEY}", content=b, headers=_claim(a, b))
            for c, a, b in zip(clients[:5], algorithms, bodies, strict=True)
        ]
    )
    monkeypatch.undo()

    assert [r.status_code for r in responses] == [200] * 5
    _assert_winner_is_consistent(storage, metadata, responses, bodies)


def test_an_overwrite_race_leaves_a_checksum_that_matches_the_winners_bytes(env, monkeypatch):
    clients, storage, metadata = env
    old = b"old" * 20_000
    assert (
        clients[0].put(f"/{BUCKET}/{KEY}", content=old, headers=_claim("SHA256", old)).status_code
        == 200
    )
    algorithms = [None, "CRC32", "SHA1", "SHA256"]
    bodies = [bytes([97 + i]) * (30_000 + i) for i in range(4)]
    _all_wrote_bytes_before_any_commit(monkeypatch, metadata, 4)

    responses = _run(
        [
            lambda c=c, a=a, b=b: c.put(f"/{BUCKET}/{KEY}", content=b, headers=_claim(a, b))
            for c, a, b in zip(clients[:4], algorithms, bodies, strict=True)
        ]
    )
    monkeypatch.undo()

    assert [r.status_code for r in responses] == [200] * 4
    _assert_winner_is_consistent(storage, metadata, responses, bodies)


# --- parts ---

PART_URL = "/{bucket}/mpu?partNumber=1&uploadId={upload}"
V1 = b"version one. " * 5000
V2 = b"version two! " * 6000


def _create_upload(client, algorithm: str) -> str:
    resp = client.post(f"/{BUCKET}/mpu?uploads", headers={"x-amz-checksum-algorithm": algorithm})
    assert resp.status_code == 200
    return resp.text.split("<UploadId>")[1].split("</UploadId>")[0]


def test_concurrent_reuploads_of_one_part_number_leave_a_checksum_that_matches_the_blob(
    env, monkeypatch
):
    clients, storage, metadata = env
    upload = _create_upload(clients[0], "SHA256")
    url = PART_URL.format(bucket=BUCKET, upload=upload)
    bodies = [bytes([65 + i]) * (20_000 + i) for i in range(5)]
    barrier = threading.Barrier(5, timeout=WAIT)
    real = metadata.record_part

    def record_part(*args, **kwargs):
        barrier.wait()  # every request has written its bytes before any commits
        return real(*args, **kwargs)

    monkeypatch.setattr(metadata, "record_part", record_part)

    responses = _run(
        [
            lambda c=c, b=b, i=i: c.put(
                url, content=b, headers=_claim("SHA256", b) if i % 2 == 0 else {}
            )
            for i, (c, b) in enumerate(zip(clients[:5], bodies, strict=True))
        ]
    )
    monkeypatch.undo()

    assert [r.status_code for r in responses] == [200] * 5
    (part,) = metadata.get_parts(upload)
    with storage.open_blob(part.blob_id) as f:
        stored = f.read()
    assert stored in bodies
    assert (part.checksum_algorithm, part.checksum) == ("SHA256", _b64("SHA256", stored))
    assert part.last_modified is not None
    assert [r.state for r in metadata.list_blob_records()] == ["live"]
    assert storage.temp_entries() == []
    assert run_consistency_sweep(metadata, storage).issues == []


def test_a_source_overwritten_while_part_copy_reads_it_gives_a_checksum_of_the_bytes_copied(
    env, monkeypatch
):
    (c, other, *_), storage, metadata = env
    assert c.put(f"/{BUCKET}/src", content=V1).status_code == 200
    upload = _create_upload(c, "CRC32")
    real = storage.open_blob
    fired = []

    def open_blob(blob_id):
        f = real(blob_id)  # the descriptor is held; now the source is replaced
        if not fired:
            fired.append(blob_id)
            assert other.put(f"/{BUCKET}/src", content=V2).status_code == 200
        return f

    monkeypatch.setattr(storage, "open_blob", open_blob)
    resp = c.put(
        PART_URL.format(bucket=BUCKET, upload=upload),
        headers={"x-amz-copy-source": f"{BUCKET}/src"},
    )
    monkeypatch.undo()

    assert resp.status_code == 200
    (part,) = metadata.get_parts(upload)
    with storage.open_blob(part.blob_id) as f:
        copied = f.read()
    assert copied == V1 and c.get(f"/{BUCKET}/src").content == V2
    assert (part.checksum_algorithm, part.checksum) == ("CRC32", _b64("CRC32", V1))
    assert f"<ChecksumCRC32>{_b64('CRC32', V1)}</ChecksumCRC32>" in resp.text
    assert [r.state for r in metadata.list_blob_records()] == ["live"] * 2
    assert storage.temp_entries() == []
    assert run_consistency_sweep(metadata, storage).issues == []
