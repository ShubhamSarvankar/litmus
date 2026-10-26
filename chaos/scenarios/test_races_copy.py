"""
Tier A: CopyObject racing writers on its source and on its destination
(docs/design/storage-layout.md §8, §9, §10, §11).

The source row is read, its conditions decided and its blob opened as GET does (§8), so a
source replaced between the row read and the open is copied as it is then; one replaced
or deleted after the open is copied as it was (the descriptor pins the blob). The
destination commit is PutObject's. The windows are made exact by wrapping the store's
open_blob and put_object_meta, not by timing: the other request runs in the wrapper,
on another client of the same app.
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

BUCKET = "copy-race"
SRC = "src"
SRC2 = "src2"
DST = "dst"
V1 = b"version one. " * 5000
V2 = b"version two! " * 6000
V3 = b"version 333.. " * 7000
WAIT = 10.0  # safety net only: no outcome depends on it
COPY = {"x-amz-copy-source": f"{BUCKET}/{SRC}"}


def _etag(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    app = create_app(storage=storage, metadata=metadata)
    clients = [TestClient(app, raise_server_exceptions=False) for _ in range(4)]
    for c in clients:  # build the middleware stack before threads can race to build it
        assert c.get("/health").status_code == 200
    assert clients[0].put(f"/{BUCKET}").status_code == 200
    assert clients[0].put(f"/{BUCKET}/{SRC}", content=V1).status_code == 200
    return clients, storage, metadata


def _assert_clean(storage, metadata, live: int) -> None:
    assert [r.state for r in metadata.list_blob_records()] == ["live"] * live
    assert storage.temp_entries() == []
    assert run_consistency_sweep(metadata, storage).issues == []


def _around_open(monkeypatch, storage, actions, after_open: bool = False) -> list:
    """Run actions[n] in the n-th open_blob call of the copy: before the real open, so
    the file it names may be gone (a replace or delete unlinks the old blob), or, with
    after_open, once the descriptor is held. Returns the list of blob IDs opened."""
    real = storage.open_blob
    opened = []

    def open_blob(blob_id):
        n = len(opened)
        opened.append(blob_id)
        if after_open:
            f = real(blob_id)
            if n < len(actions):
                actions[n]()
            return f
        if n < len(actions):
            actions[n]()
        return real(blob_id)

    monkeypatch.setattr(storage, "open_blob", open_blob)
    return opened


# --- the source replaced or deleted before the open ---


def test_source_replaced_before_the_open_is_copied_as_it_then_is(env, monkeypatch):
    (c, other, *_), storage, metadata = env
    opened = _around_open(monkeypatch, storage, [lambda: other.put(f"/{BUCKET}/{SRC}", content=V2)])

    resp = c.put(f"/{BUCKET}/{DST}", headers=COPY)

    assert resp.status_code == 200
    assert len(opened) == 2 and opened[0] != opened[1]
    got = c.get(f"/{BUCKET}/{DST}")
    assert got.content == V2 and got.headers["etag"] == _etag(V2)
    monkeypatch.undo()
    _assert_clean(storage, metadata, live=2)


def test_source_conditions_are_decided_again_on_the_row_that_replaced_it(env, monkeypatch):
    (c, other, *_), storage, metadata = env
    _around_open(monkeypatch, storage, [lambda: other.put(f"/{BUCKET}/{SRC}", content=V2)])

    # True of the row first read, false of the one that replaced it.
    resp = c.put(f"/{BUCKET}/{DST}", headers={**COPY, "x-amz-copy-source-if-match": _etag(V1)})

    assert resp.status_code == 412
    monkeypatch.undo()
    assert c.get(f"/{BUCKET}/{DST}").status_code == 404
    _assert_clean(storage, metadata, live=1)


def test_source_deleted_before_the_open_is_no_such_key(env, monkeypatch):
    (c, other, *_), storage, metadata = env
    _around_open(monkeypatch, storage, [lambda: other.delete(f"/{BUCKET}/{SRC}")])

    resp = c.put(f"/{BUCKET}/{DST}", headers=COPY)

    assert resp.status_code == 404 and b"NoSuchKey" in resp.content
    monkeypatch.undo()
    assert c.get(f"/{BUCKET}/{DST}").status_code == 404
    _assert_clean(storage, metadata, live=0)


def test_lost_source_bytes_with_an_unchanged_row_are_500_and_create_nothing(env, monkeypatch):
    (c, *_), storage, metadata = env
    blob_id = metadata.get_object_meta(BUCKET, SRC).blob_id
    _around_open(monkeypatch, storage, [lambda: storage.remove_blob(blob_id)])

    resp = c.put(f"/{BUCKET}/{DST}", headers=COPY)

    assert resp.status_code == 500
    monkeypatch.undo()
    assert c.get(f"/{BUCKET}/{DST}").status_code == 404
    assert storage.temp_entries() == []
    assert [r.state for r in metadata.list_blob_records()] == ["live"]  # the source's row only


@pytest.mark.parametrize(("replacements", "status"), [(3, 200), (4, 503)])
def test_a_source_replaced_again_and_again_is_served_or_slow_down(
    env, monkeypatch, replacements, status
):
    (c, other, *_), storage, metadata = env
    versions = [bytes([65 + i]) * (30_000 + i) for i in range(replacements)]
    _around_open(
        monkeypatch,
        storage,
        [lambda v=v: other.put(f"/{BUCKET}/{SRC}", content=v) for v in versions],
    )

    resp = c.put(f"/{BUCKET}/{DST}", headers=COPY)

    assert resp.status_code == status
    monkeypatch.undo()
    if status == 200:
        assert c.get(f"/{BUCKET}/{DST}").content == versions[-1]
        _assert_clean(storage, metadata, live=2)
    else:
        assert b"SlowDown" in resp.content
        assert c.get(f"/{BUCKET}/{DST}").status_code == 404
        _assert_clean(storage, metadata, live=1)


# --- the source replaced or deleted after the open ---


def test_source_overwritten_during_the_copy_is_copied_as_it_was(env, monkeypatch):
    (c, other, *_), storage, metadata = env
    _around_open(
        monkeypatch,
        storage,
        [lambda: other.put(f"/{BUCKET}/{SRC}", content=V2)],
        after_open=True,
    )

    resp = c.put(f"/{BUCKET}/{DST}", headers=COPY)

    assert resp.status_code == 200
    monkeypatch.undo()
    got = c.get(f"/{BUCKET}/{DST}")
    assert got.content == V1 and got.headers["etag"] == _etag(V1)
    assert c.get(f"/{BUCKET}/{SRC}").content == V2
    _assert_clean(storage, metadata, live=2)


def test_source_deleted_during_the_copy_is_copied_as_it_was(env, monkeypatch):
    (c, other, *_), storage, metadata = env
    _around_open(
        monkeypatch,
        storage,
        [lambda: other.delete(f"/{BUCKET}/{SRC}")],
        after_open=True,
    )

    resp = c.put(f"/{BUCKET}/{DST}", headers=COPY)

    assert resp.status_code == 200
    monkeypatch.undo()
    assert c.get(f"/{BUCKET}/{DST}").content == V1
    assert c.get(f"/{BUCKET}/{SRC}").status_code == 404
    _assert_clean(storage, metadata, live=1)


def test_source_replaced_between_the_bytes_and_the_commit_changes_nothing(env, monkeypatch):
    (c, other, *_), storage, metadata = env
    real = metadata.put_object_meta

    def put_object_meta(bucket, key, *args, **kwargs):
        if key == DST:
            assert other.put(f"/{BUCKET}/{SRC}", content=V2).status_code == 200
        return real(bucket, key, *args, **kwargs)

    monkeypatch.setattr(metadata, "put_object_meta", put_object_meta)

    resp = c.put(f"/{BUCKET}/{DST}", headers=COPY)

    assert resp.status_code == 200
    monkeypatch.undo()
    assert c.get(f"/{BUCKET}/{DST}").content == V1
    _assert_clean(storage, metadata, live=2)


# --- racing on the destination ---


def _all_wrote_bytes_before_any_commit(monkeypatch, metadata, n: int) -> None:
    barrier = threading.Barrier(n, timeout=WAIT)
    real = metadata.put_object_meta

    def put_object_meta(*args, **kwargs):
        barrier.wait()
        return real(*args, **kwargs)

    monkeypatch.setattr(metadata, "put_object_meta", put_object_meta)


def _run(calls) -> list:
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


def test_two_copies_to_one_destination_leave_one_complete_object(env, monkeypatch):
    clients, storage, metadata = env
    assert clients[0].put(f"/{BUCKET}/{SRC2}", content=V2).status_code == 200
    _all_wrote_bytes_before_any_commit(monkeypatch, metadata, 2)

    responses = _run(
        [
            lambda: clients[0].put(f"/{BUCKET}/{DST}", headers=COPY),
            lambda: clients[1].put(
                f"/{BUCKET}/{DST}", headers={"x-amz-copy-source": f"{BUCKET}/{SRC2}"}
            ),
        ]
    )
    monkeypatch.undo()

    assert [r.status_code for r in responses] == [200, 200]
    got = clients[0].get(f"/{BUCKET}/{DST}")
    assert got.content in (V1, V2) and got.headers["etag"] == _etag(got.content)
    # The later commit won, and the other copy's blob was freed.
    _assert_clean(storage, metadata, live=3)


def test_two_copies_with_if_none_match_star_to_a_new_key_have_one_winner(env, monkeypatch):
    clients, storage, metadata = env
    assert clients[0].put(f"/{BUCKET}/{SRC2}", content=V2).status_code == 200
    _all_wrote_bytes_before_any_commit(monkeypatch, metadata, 2)

    responses = _run(
        [
            lambda: clients[0].put(f"/{BUCKET}/{DST}", headers={**COPY, "If-None-Match": "*"}),
            lambda: clients[1].put(
                f"/{BUCKET}/{DST}",
                headers={"x-amz-copy-source": f"{BUCKET}/{SRC2}", "If-None-Match": "*"},
            ),
        ]
    )
    monkeypatch.undo()

    assert sorted(r.status_code for r in responses) == [200, 412]
    won = V1 if responses[0].status_code == 200 else V2
    assert clients[0].get(f"/{BUCKET}/{DST}").content == won
    _assert_clean(storage, metadata, live=3)


def test_copy_racing_a_put_with_if_none_match_star_has_one_creator(env, monkeypatch):
    clients, storage, metadata = env
    _all_wrote_bytes_before_any_commit(monkeypatch, metadata, 2)

    responses = _run(
        [
            lambda: clients[0].put(f"/{BUCKET}/{DST}", headers={**COPY, "If-None-Match": "*"}),
            lambda: clients[1].put(f"/{BUCKET}/{DST}", content=V3, headers={"If-None-Match": "*"}),
        ]
    )
    monkeypatch.undo()

    assert sorted(r.status_code for r in responses) == [200, 412]
    won = V1 if responses[0].status_code == 200 else V3
    assert clients[0].get(f"/{BUCKET}/{DST}").content == won
    _assert_clean(storage, metadata, live=2)


def test_if_match_copy_committing_after_a_delete_is_404_and_creates_nothing(env, monkeypatch):
    (c, other, *_), storage, metadata = env
    assert c.put(f"/{BUCKET}/{DST}", content=V3).status_code == 200
    real = metadata.put_object_meta

    def put_object_meta(bucket, key, *args, **kwargs):
        if key == DST:
            assert other.delete(f"/{BUCKET}/{DST}").status_code == 204
        return real(bucket, key, *args, **kwargs)

    monkeypatch.setattr(metadata, "put_object_meta", put_object_meta)

    resp = c.put(f"/{BUCKET}/{DST}", headers={**COPY, "If-Match": _etag(V3)})

    assert resp.status_code == 404 and b"NoSuchKey" in resp.content
    monkeypatch.undo()
    assert c.get(f"/{BUCKET}/{DST}").status_code == 404
    _assert_clean(storage, metadata, live=1)


def test_copy_into_a_bucket_deleted_before_its_commit_is_no_such_bucket(env, monkeypatch):
    (c, other, *_), storage, metadata = env
    assert c.put("/second").status_code == 200
    real = metadata.put_object_meta

    def put_object_meta(bucket, key, *args, **kwargs):
        if bucket == "second":
            assert other.delete("/second").status_code == 204
        return real(bucket, key, *args, **kwargs)

    monkeypatch.setattr(metadata, "put_object_meta", put_object_meta)

    resp = c.put(f"/second/{DST}", headers=COPY)

    assert resp.status_code == 404 and b"NoSuchBucket" in resp.content
    monkeypatch.undo()
    _assert_clean(storage, metadata, live=1)
