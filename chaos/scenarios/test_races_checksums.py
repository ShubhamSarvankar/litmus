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
