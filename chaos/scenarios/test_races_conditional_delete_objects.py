"""
Tier A: DeleteObjects with per key ETag conditions racing other writers on the same key
(docs/design/storage-layout.md §9, §10).

Each key's condition is decided in the batch's commit, in the order the commits take
SQLite's write lock, against the row the earlier commits left. Each test drives one app
through several TestClients, one thread each.
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

BUCKET = "cond-delete-objects-race"
KEY = "obj"
WAIT = 10.0  # safety net only: no outcome depends on it


def _etag(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    app = create_app(storage=storage, metadata=metadata)
    clients = [TestClient(app, raise_server_exceptions=False) for _ in range(3)]
    for c in clients:  # build the middleware stack before threads can race to build it
        assert c.get("/health").status_code == 200
    assert clients[0].put(f"/{BUCKET}").status_code == 200
    return clients, storage, metadata


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
    assert sorted(b for _, b in storage.scan()) == sorted(
        r.blob_id for r in metadata.list_blob_records()
    )
    assert storage.temp_entries() == []
    assert run_consistency_sweep(metadata, storage).issues == []


def _delete_objects(client, key: str, etag: str):
    body = f"<Delete><Object><Key>{key}</Key><ETag>{etag}</ETag></Object></Delete>".encode()
    return client.post(f"/{BUCKET}?delete", content=body)


def _results(resp) -> tuple[list[str], list[tuple[str, str]]]:
    assert resp.status_code == 200
    root = ET.fromstring(resp.text)
    return (
        [d.findtext("Key") for d in root.findall("Deleted")],
        [(e.findtext("Key"), e.findtext("Code")) for e in root.findall("Error")],
    )


def test_delete_objects_with_the_old_etag_after_an_overwrite_committed_is_refused(env, monkeypatch):
    clients, storage, metadata = env
    old, new = b"old" * 20_000, b"new" * 25_000
    assert clients[0].put(f"/{BUCKET}/{KEY}", content=old).status_code == 200
    reached = threading.Event()
    overwritten = threading.Event()
    real = metadata.delete_objects_checked

    def delete_objects_checked(*args, **kwargs):
        reached.set()  # the batch is about to take the write lock; let the PUT commit first
        assert overwritten.wait(WAIT)
        return real(*args, **kwargs)

    monkeypatch.setattr(metadata, "delete_objects_checked", delete_objects_checked)

    def overwrite():
        assert reached.wait(WAIT)
        resp = clients[1].put(f"/{BUCKET}/{KEY}", content=new)
        overwritten.set()
        return resp

    resp_delete, resp_put = _run([lambda: _delete_objects(clients[0], KEY, _etag(old)), overwrite])
    monkeypatch.undo()

    assert resp_put.status_code == 200
    assert _results(resp_delete) == ([], [(KEY, "PreconditionFailed")])
    got = clients[0].get(f"/{BUCKET}/{KEY}")
    assert got.status_code == 200 and got.content == new and got.headers["etag"] == _etag(new)
    assert metadata.get_object_meta(BUCKET, KEY).etag == _etag(new)
    _assert_clean(storage, metadata, live=1)


def test_two_delete_objects_with_the_same_correct_etag_are_one_deleted_and_one_no_such_key(
    env, monkeypatch
):
    clients, storage, metadata = env
    body = b"x" * 50_000
    assert clients[0].put(f"/{BUCKET}/{KEY}", content=body).status_code == 200
    barrier = threading.Barrier(2, timeout=WAIT)
    real = metadata.delete_objects_checked

    def delete_objects_checked(*args, **kwargs):
        barrier.wait()  # both are at the commit before either takes the lock
        return real(*args, **kwargs)

    monkeypatch.setattr(metadata, "delete_objects_checked", delete_objects_checked)
    responses = _run([lambda c=c: _delete_objects(c, KEY, _etag(body)) for c in clients[:2]])
    monkeypatch.undo()

    outcomes = sorted(_results(r) for r in responses)
    assert outcomes == [([], [(KEY, "NoSuchKey")]), ([KEY], [])]  # the loser finds no key
    assert clients[0].get(f"/{BUCKET}/{KEY}").status_code == 404
    assert metadata.get_object_meta(BUCKET, KEY) is None
    _assert_clean(storage, metadata, live=0)
