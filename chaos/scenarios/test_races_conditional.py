"""
Tier A: conditional PutObject racing other writers on the same key
(docs/design/storage-layout.md §9, §10).

Every PUT writes its own bytes first; the conditions are decided in the commit, in the
order the commits take SQLite's write lock. Each test drives one app through several
TestClients, one thread each. A barrier in front of the commit makes sure every request
has written its bytes before any of them commits, so the commits really do race.
"""

import hashlib
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "cond-race"
KEY = "obj"
WAIT = 10.0  # safety net only: no outcome depends on it


def _etag(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


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


def _assert_clean(storage, metadata, live: int) -> None:
    assert [r.state for r in metadata.list_blob_records()] == ["live"] * live
    assert storage.temp_entries() == []
    assert run_consistency_sweep(metadata, storage).issues == []


def test_eight_if_none_match_star_puts_to_a_new_key_have_one_winner(env, monkeypatch):
    clients, storage, metadata = env
    bodies = [bytes([65 + i]) * (40_000 + i) for i in range(8)]
    _all_wrote_bytes_before_any_commit(monkeypatch, metadata, 8)

    responses = _run(
        [
            lambda c=c, b=b: c.put(f"/{BUCKET}/{KEY}", content=b, headers={"If-None-Match": "*"})
            for c, b in zip(clients, bodies, strict=True)
        ]
    )
    monkeypatch.undo()

    winners = [i for i, r in enumerate(responses) if r.status_code == 200]
    assert len(winners) == 1
    assert [r.status_code for i, r in enumerate(responses) if i not in winners] == [412] * 7
    won = winners[0]
    assert responses[won].headers["etag"] == _etag(bodies[won])
    got = clients[0].get(f"/{BUCKET}/{KEY}")
    assert got.content == bodies[won] and got.headers["etag"] == _etag(bodies[won])
    _assert_clean(storage, metadata, live=1)


def test_two_if_match_puts_with_the_same_etag_have_one_winner(env, monkeypatch):
    clients, storage, metadata = env
    old = b"old" * 20_000
    assert clients[0].put(f"/{BUCKET}/{KEY}", content=old).status_code == 200
    bodies = [b"A" * 30_000, b"B" * 31_000]
    _all_wrote_bytes_before_any_commit(monkeypatch, metadata, 2)

    responses = _run(
        [
            lambda c=c, b=b: c.put(f"/{BUCKET}/{KEY}", content=b, headers={"If-Match": _etag(old)})
            for c, b in zip(clients[:2], bodies, strict=True)
        ]
    )
    monkeypatch.undo()

    assert sorted(r.status_code for r in responses) == [200, 412]
    won = 0 if responses[0].status_code == 200 else 1
    got = clients[0].get(f"/{BUCKET}/{KEY}")
    assert got.content == bodies[won] and got.headers["etag"] == _etag(bodies[won])
    _assert_clean(storage, metadata, live=1)


def test_if_match_put_committing_after_a_delete_is_404_and_creates_nothing(env, monkeypatch):
    clients, storage, metadata = env
    old = b"old" * 20_000
    put, other = clients[:2]
    assert put.put(f"/{BUCKET}/{KEY}", content=old).status_code == 200
    written = threading.Event()
    deleted = threading.Event()
    real = metadata.put_object_meta

    def put_object_meta(*args, **kwargs):
        written.set()  # the bytes are down; let the delete commit before this commit
        assert deleted.wait(WAIT)
        return real(*args, **kwargs)

    monkeypatch.setattr(metadata, "put_object_meta", put_object_meta)

    def delete():
        assert written.wait(WAIT)
        resp = other.delete(f"/{BUCKET}/{KEY}")
        deleted.set()
        return resp

    resp_put, resp_delete = _run(
        [
            lambda: put.put(f"/{BUCKET}/{KEY}", content=b"new" * 20_000, headers={"If-Match": "*"}),
            delete,
        ]
    )
    monkeypatch.undo()

    assert resp_delete.status_code == 204
    assert resp_put.status_code == 404
    assert clients[0].get(f"/{BUCKET}/{KEY}").status_code == 404
    _assert_clean(storage, metadata, live=0)


def test_if_match_put_racing_a_delete_is_200_or_404_never_both_applied(env):
    clients, storage, metadata = env
    put, other = clients[:2]
    outcomes = set()
    for round_ in range(40):
        key = f"k{round_}"
        old = f"old {round_}".encode() * 5_000
        assert put.put(f"/{BUCKET}/{key}", content=old).status_code == 200
        start = threading.Barrier(2, timeout=WAIT)

        def do_put(key=key, old=old):
            start.wait()
            return put.put(
                f"/{BUCKET}/{key}", content=b"new" * 5_000, headers={"If-Match": _etag(old)}
            )

        def do_delete(key=key):
            start.wait()
            return other.delete(f"/{BUCKET}/{key}")

        resp_put, resp_delete = _run([do_put, do_delete])
        outcomes.add(resp_put.status_code)
        assert resp_delete.status_code == 204
        assert resp_put.status_code in (200, 404)
        # Whichever order they committed in, the delete's effect stands: the key is gone,
        # and a 404 PUT did not bring it back.
        assert put.get(f"/{BUCKET}/{key}").status_code == 404
    assert outcomes <= {200, 404}
    _assert_clean(storage, metadata, live=0)
