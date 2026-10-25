"""
Tier A: DeleteObject with If-Match racing other writers on the same key
(docs/design/storage-layout.md §9, §10).

The condition is decided in the delete's commit, in the order the commits take SQLite's
write lock. Each test drives one app through several TestClients, one thread each.
"""

import hashlib
import random
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "cond-delete-race"
KEY = "obj"
WAIT = 10.0  # safety net only: no outcome depends on it


def _etag(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    app = create_app(storage=storage, metadata=metadata)
    clients = [TestClient(app, raise_server_exceptions=False) for _ in range(6)]
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


def test_delete_with_the_old_etag_after_an_overwrite_committed_is_412(env, monkeypatch):
    clients, storage, metadata = env
    old, new = b"old" * 20_000, b"new" * 25_000
    assert clients[0].put(f"/{BUCKET}/{KEY}", content=old).status_code == 200
    reached = threading.Event()
    overwritten = threading.Event()
    real = metadata.delete_object_meta

    def delete_object_meta(*args, **kwargs):
        reached.set()  # the delete is about to take the write lock; let the PUT commit first
        assert overwritten.wait(WAIT)
        return real(*args, **kwargs)

    monkeypatch.setattr(metadata, "delete_object_meta", delete_object_meta)

    def overwrite():
        assert reached.wait(WAIT)
        resp = clients[1].put(f"/{BUCKET}/{KEY}", content=new)
        overwritten.set()
        return resp

    resp_delete, resp_put = _run(
        [lambda: clients[0].delete(f"/{BUCKET}/{KEY}", headers={"If-Match": _etag(old)}), overwrite]
    )
    monkeypatch.undo()

    assert resp_put.status_code == 200
    assert resp_delete.status_code == 412
    got = clients[0].get(f"/{BUCKET}/{KEY}")
    assert got.status_code == 200 and got.content == new and got.headers["etag"] == _etag(new)
    _assert_clean(storage, metadata, live=1)


def test_two_deletes_with_the_same_correct_if_match_are_one_204_and_one_404(env, monkeypatch):
    clients, storage, metadata = env
    body = b"x" * 50_000
    assert clients[0].put(f"/{BUCKET}/{KEY}", content=body).status_code == 200
    barrier = threading.Barrier(2, timeout=WAIT)
    real = metadata.delete_object_meta

    def delete_object_meta(*args, **kwargs):
        barrier.wait()  # both are at the commit before either takes the lock
        return real(*args, **kwargs)

    monkeypatch.setattr(metadata, "delete_object_meta", delete_object_meta)
    responses = _run(
        [
            lambda c=c: c.delete(f"/{BUCKET}/{KEY}", headers={"If-Match": _etag(body)})
            for c in clients[:2]
        ]
    )
    monkeypatch.undo()

    assert sorted(r.status_code for r in responses) == [204, 404]  # the loser finds no key
    assert clients[0].get(f"/{BUCKET}/{KEY}").status_code == 404
    assert metadata.get_object_meta(BUCKET, KEY) is None
    _assert_clean(storage, metadata, live=0)


def test_forty_rounds_of_if_match_deletes_racing_puts_end_consistent(env):
    clients, storage, metadata = env
    rng = random.Random(20240601)
    bodies = [bytes([n]) * (4_000 + n) for n in range(1, 9)]
    assert clients[0].put(f"/{BUCKET}/{KEY}", content=bodies[0]).status_code == 200

    for round_ in range(40):
        known = {_etag(b): b for b in bodies}
        calls = []
        for i in range(4):
            client = clients[i]
            if rng.random() < 0.5:
                data = rng.choice(bodies)
                calls.append(lambda c=client, d=data: ("put", c.put(f"/{BUCKET}/{KEY}", content=d)))
            else:
                tag = rng.choice(list(known))
                calls.append(
                    lambda c=client, t=tag: (
                        "delete",
                        c.delete(f"/{BUCKET}/{KEY}", headers={"If-Match": t}),
                    )
                )
        for kind, resp in _run(calls):
            # A PUT is always 200; a delete is 204 (it matched), 404 (the key was gone)
            # or 412 (the key held another body). Never 409 or 5xx.
            assert resp.status_code in ((200,) if kind == "put" else (204, 404, 412)), round_

        row = metadata.get_object_meta(BUCKET, KEY)
        if row is None:
            assert clients[0].get(f"/{BUCKET}/{KEY}").status_code == 404
            _assert_clean(storage, metadata, live=0)
        else:
            got = clients[0].get(f"/{BUCKET}/{KEY}")
            assert got.status_code == 200
            assert got.content == known[row.etag] and row.size == len(got.content)
            assert got.headers["etag"] == row.etag == _etag(got.content)
            _assert_clean(storage, metadata, live=1)
