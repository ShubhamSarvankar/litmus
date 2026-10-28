"""
Tier A: conditional CompleteMultipartUpload racing other writers on the same key
(docs/design/storage-layout.md §9, §10).

Every Complete assembles its blob first; the conditions are decided in the commit, in the
order the commits take SQLite's write lock. Each test drives one app through several
TestClients, one thread each. A barrier in front of the commit makes sure every request
has done its work before any of them commits, so the commits really do race. A Complete
that loses leaves its upload open with all its parts.
"""

import hashlib
import threading
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "cond-complete-race"
KEY = "obj"
WAIT = 10.0  # safety net only: no outcome depends on it
PARTS = [b"A" * 30_000, b"B" * 31_000]


def _etag(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def _composite_etag(parts: list[bytes]) -> str:
    digests = b"".join(hashlib.md5(p).digest() for p in parts)
    return f'"{hashlib.md5(digests).hexdigest()}-{len(parts)}"'


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    app = create_app(storage=storage, metadata=metadata)
    clients = [TestClient(app, raise_server_exceptions=False) for _ in range(4)]
    for c in clients:  # build the middleware stack before threads can race to build it
        assert c.get("/health").status_code == 200
    assert clients[0].put(f"/{BUCKET}").status_code == 200
    return clients, storage, metadata


def _upload(client, parts: list[bytes] = PARTS) -> tuple[str, list[str]]:
    uid = ET.fromstring(client.post(f"/{BUCKET}/{KEY}?uploads").text).findtext("UploadId")
    etags = []
    for number, data in enumerate(parts, start=1):
        r = client.put(f"/{BUCKET}/{KEY}?partNumber={number}&uploadId={uid}", content=data)
        assert r.status_code == 200
        etags.append(r.headers["etag"])
    return uid, etags


def _complete(client, uid: str, etags: list[str], headers: dict):
    items = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>"
        for n, e in enumerate(etags, start=1)
    )
    return client.post(
        f"/{BUCKET}/{KEY}?uploadId={uid}",
        content=f"<CompleteMultipartUpload>{items}</CompleteMultipartUpload>".encode(),
        headers=headers,
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


def _assert_clean(storage, metadata, live: int) -> None:
    assert [r.state for r in metadata.list_blob_records()] == ["live"] * live
    assert storage.temp_entries() == []
    assert run_consistency_sweep(metadata, storage).issues == []


def _assert_upload_open(metadata, uid: str, etags: list[str]) -> None:
    assert metadata.upload_exists(uid, bucket=BUCKET, key=KEY)
    parts = metadata.get_parts(uid)
    assert [(p.part_number, p.etag) for p in parts] == list(enumerate(etags, start=1))
    assert all(p.blob_id is not None for p in parts)


def test_two_uploads_completed_with_if_none_match_star_have_one_winner(env, monkeypatch):
    clients, storage, metadata = env
    uploads = [_upload(clients[0]), _upload(clients[0])]
    barrier = threading.Barrier(2, timeout=WAIT)
    real = metadata.commit_completed_upload

    def commit_completed_upload(*args, **kwargs):
        barrier.wait()  # both have assembled their blobs
        return real(*args, **kwargs)

    monkeypatch.setattr(metadata, "commit_completed_upload", commit_completed_upload)
    responses = _run(
        [
            lambda c=c, u=u: _complete(c, u[0], u[1], {"If-None-Match": "*"})
            for c, u in zip(clients[:2], uploads, strict=True)
        ]
    )
    monkeypatch.undo()

    assert sorted(r.status_code for r in responses) == [200, 412]
    won = 0 if responses[0].status_code == 200 else 1
    lost = 1 - won
    assert ET.fromstring(responses[won].text).findtext("ETag") == _composite_etag(PARTS)
    got = clients[0].get(f"/{BUCKET}/{KEY}")
    assert got.content == b"".join(PARTS) and got.headers["etag"] == _composite_etag(PARTS)
    assert not metadata.upload_exists(uploads[won][0], bucket=BUCKET, key=KEY)
    _assert_upload_open(metadata, *uploads[lost])
    _assert_clean(storage, metadata, live=1 + len(PARTS))


def test_complete_and_put_with_if_none_match_star_on_a_new_key_have_one_winner(env, monkeypatch):
    clients, storage, metadata = env
    uid, etags = _upload(clients[0])
    body = b"P" * 40_000
    barrier = threading.Barrier(2, timeout=WAIT)
    real_complete = metadata.commit_completed_upload
    real_put = metadata.put_object_meta

    def commit_completed_upload(*args, **kwargs):
        barrier.wait()
        return real_complete(*args, **kwargs)

    def put_object_meta(*args, **kwargs):
        barrier.wait()
        return real_put(*args, **kwargs)

    monkeypatch.setattr(metadata, "commit_completed_upload", commit_completed_upload)
    monkeypatch.setattr(metadata, "put_object_meta", put_object_meta)
    resp_complete, resp_put = _run(
        [
            lambda: _complete(clients[0], uid, etags, {"If-None-Match": "*"}),
            lambda: clients[1].put(
                f"/{BUCKET}/{KEY}", content=body, headers={"If-None-Match": "*"}
            ),
        ]
    )
    monkeypatch.undo()

    assert sorted([resp_complete.status_code, resp_put.status_code]) == [200, 412]
    got = clients[0].get(f"/{BUCKET}/{KEY}")
    if resp_complete.status_code == 200:
        assert got.content == b"".join(PARTS) and got.headers["etag"] == _composite_etag(PARTS)
        assert not metadata.upload_exists(uid, bucket=BUCKET, key=KEY)
        _assert_clean(storage, metadata, live=1)
    else:
        assert got.content == body and got.headers["etag"] == _etag(body)
        _assert_upload_open(metadata, uid, etags)
        _assert_clean(storage, metadata, live=1 + len(PARTS))


def test_if_match_complete_committing_after_a_delete_is_404_and_creates_nothing(env, monkeypatch):
    clients, storage, metadata = env
    old = b"old" * 20_000
    assert clients[0].put(f"/{BUCKET}/{KEY}", content=old).status_code == 200
    uid, etags = _upload(clients[0])
    assembled = threading.Event()
    deleted = threading.Event()
    real = metadata.commit_completed_upload

    def commit_completed_upload(*args, **kwargs):
        assembled.set()  # the blob is assembled; let the delete commit before this commit
        assert deleted.wait(WAIT)
        return real(*args, **kwargs)

    monkeypatch.setattr(metadata, "commit_completed_upload", commit_completed_upload)

    def delete():
        assert assembled.wait(WAIT)
        resp = clients[1].delete(f"/{BUCKET}/{KEY}")
        deleted.set()
        return resp

    resp_complete, resp_delete = _run(
        [lambda: _complete(clients[0], uid, etags, {"If-Match": _etag(old)}), delete]
    )
    monkeypatch.undo()

    assert resp_delete.status_code == 204
    assert resp_complete.status_code == 404
    assert ET.fromstring(resp_complete.text).findtext("Code") == "NoSuchKey"
    assert clients[0].get(f"/{BUCKET}/{KEY}").status_code == 404
    _assert_upload_open(metadata, uid, etags)
    _assert_clean(storage, metadata, live=len(PARTS))
    # The upload is not lost: with the object gone, a condition that holds completes it.
    assert _complete(clients[0], uid, etags, {"If-None-Match": "*"}).status_code == 200
    _assert_clean(storage, metadata, live=1)
