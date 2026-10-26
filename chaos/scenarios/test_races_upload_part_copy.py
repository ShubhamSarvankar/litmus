"""
Tier A: UploadPartCopy racing writers on its source and on its upload
(docs/design/storage-layout.md §6.6, §8, §9, §11).

The source row is read, its conditions decided and its blob opened as GET does (§8), so a
source replaced between the row read and the open is copied as it is then; one replaced
or deleted after the open is copied as it was (the descriptor pins the blob). The part
commit is UploadPart's: an upload aborted or completed before it is NoSuchUpload and the
new blob is discarded, and of two commits of one part number the later wins. The windows
are made exact by wrapping the store's open_blob and record_part, not by timing: the
other request runs in the wrapper, on another client of the same app.
"""

import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "part-copy-race"
SRC = "src"
DST = "dst"
V1 = b"version one. " * 5000
V2 = b"version two! " * 6000
COPY = {"x-amz-copy-source": f"{BUCKET}/{SRC}"}


def _md5(data: bytes) -> str:
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
    upload = ET.fromstring(clients[0].post(f"/{BUCKET}/{DST}?uploads").text).findtext("UploadId")
    return clients, storage, metadata, upload


def _url(upload: str, n: int = 1) -> str:
    return f"/{BUCKET}/{DST}?partNumber={n}&uploadId={upload}"


def _assert_clean(storage, metadata, live: int) -> None:
    assert [r.state for r in metadata.list_blob_records()] == ["live"] * live
    assert storage.temp_entries() == []
    assert run_consistency_sweep(metadata, storage).issues == []


def _part_bytes(storage, metadata, upload, n=1) -> bytes | None:
    for part in metadata.get_parts(upload):
        if part.part_number == n:
            with storage.open_blob(part.blob_id) as f:
                return f.read()
    return None


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


def _before_commit(monkeypatch, metadata, action) -> None:
    """Run action once, just before the part commit of the copy."""
    real = metadata.record_part
    fired = []

    def record_part(upload_id, part):
        if not fired:  # the other request commits a part too
            fired.append(True)
            action()
        return real(upload_id, part)

    monkeypatch.setattr(metadata, "record_part", record_part)


# --- the source replaced or deleted before the open ---


def test_source_replaced_before_the_open_is_copied_as_it_then_is(env, monkeypatch):
    (c, other, *_), storage, metadata, upload = env
    opened = _around_open(monkeypatch, storage, [lambda: other.put(f"/{BUCKET}/{SRC}", content=V2)])

    resp = c.put(_url(upload), headers=COPY)

    assert resp.status_code == 200
    assert len(opened) == 2 and opened[0] != opened[1]
    assert ET.fromstring(resp.text).findtext("ETag") == _md5(V2)
    monkeypatch.undo()
    assert _part_bytes(storage, metadata, upload) == V2
    _assert_clean(storage, metadata, live=2)


def test_range_is_taken_from_the_row_that_replaced_the_source(env, monkeypatch):
    (c, other, *_), storage, metadata, upload = env
    longer = V1 + b"tail" * 1000
    _around_open(monkeypatch, storage, [lambda: other.put(f"/{BUCKET}/{SRC}", content=longer)])
    end = len(V1) + 100  # outside V1, inside its replacement

    resp = c.put(
        _url(upload), headers={**COPY, "x-amz-copy-source-range": f"bytes={end - 9}-{end}"}
    )

    assert resp.status_code == 200
    monkeypatch.undo()
    assert _part_bytes(storage, metadata, upload) == longer[end - 9 : end + 1]
    _assert_clean(storage, metadata, live=2)


def test_range_outside_the_row_that_replaced_the_source_is_416(env, monkeypatch):
    (c, other, *_), storage, metadata, upload = env
    _around_open(monkeypatch, storage, [lambda: other.put(f"/{BUCKET}/{SRC}", content=b"short")])

    resp = c.put(_url(upload), headers={**COPY, "x-amz-copy-source-range": "bytes=0-99"})

    assert resp.status_code == 416 and b"InvalidRange" in resp.content
    monkeypatch.undo()
    assert metadata.get_parts(upload) == []
    _assert_clean(storage, metadata, live=1)


def test_source_conditions_are_decided_again_on_the_row_that_replaced_it(env, monkeypatch):
    (c, other, *_), storage, metadata, upload = env
    _around_open(monkeypatch, storage, [lambda: other.put(f"/{BUCKET}/{SRC}", content=V2)])

    # True of the row first read, false of the one that replaced it.
    resp = c.put(_url(upload), headers={**COPY, "x-amz-copy-source-if-match": _md5(V1)})

    assert resp.status_code == 412
    monkeypatch.undo()
    assert metadata.get_parts(upload) == []
    _assert_clean(storage, metadata, live=1)


def test_source_deleted_before_the_open_is_no_such_key(env, monkeypatch):
    (c, other, *_), storage, metadata, upload = env
    _around_open(monkeypatch, storage, [lambda: other.delete(f"/{BUCKET}/{SRC}")])

    resp = c.put(_url(upload), headers=COPY)

    assert resp.status_code == 404 and b"NoSuchKey" in resp.content
    monkeypatch.undo()
    assert metadata.get_parts(upload) == []
    _assert_clean(storage, metadata, live=0)


@pytest.mark.parametrize(("replacements", "status"), [(3, 200), (4, 503)])
def test_a_source_replaced_again_and_again_is_served_or_slow_down(
    env, monkeypatch, replacements, status
):
    (c, other, *_), storage, metadata, upload = env
    versions = [bytes([65 + i]) * (30_000 + i) for i in range(replacements)]
    _around_open(
        monkeypatch,
        storage,
        [lambda v=v: other.put(f"/{BUCKET}/{SRC}", content=v) for v in versions],
    )

    resp = c.put(_url(upload), headers=COPY)

    assert resp.status_code == status
    monkeypatch.undo()
    if status == 200:
        assert _part_bytes(storage, metadata, upload) == versions[-1]
        _assert_clean(storage, metadata, live=2)
    else:
        assert b"SlowDown" in resp.content
        assert metadata.get_parts(upload) == []
        _assert_clean(storage, metadata, live=1)


# --- the source replaced or deleted after the open ---


def test_source_overwritten_during_the_copy_is_copied_as_it_was(env, monkeypatch):
    (c, other, *_), storage, metadata, upload = env
    _around_open(
        monkeypatch, storage, [lambda: other.put(f"/{BUCKET}/{SRC}", content=V2)], after_open=True
    )

    resp = c.put(_url(upload), headers=COPY)

    assert resp.status_code == 200
    monkeypatch.undo()
    assert _part_bytes(storage, metadata, upload) == V1
    assert c.get(f"/{BUCKET}/{SRC}").content == V2
    _assert_clean(storage, metadata, live=2)


def test_source_deleted_during_the_copy_is_copied_as_it_was(env, monkeypatch):
    (c, other, *_), storage, metadata, upload = env
    _around_open(monkeypatch, storage, [lambda: other.delete(f"/{BUCKET}/{SRC}")], after_open=True)

    resp = c.put(_url(upload), headers={**COPY, "x-amz-copy-source-range": "bytes=10-49"})

    assert resp.status_code == 200
    monkeypatch.undo()
    assert _part_bytes(storage, metadata, upload) == V1[10:50]
    assert c.get(f"/{BUCKET}/{SRC}").status_code == 404
    _assert_clean(storage, metadata, live=1)


def test_source_replaced_between_the_bytes_and_the_commit_changes_nothing(env, monkeypatch):
    (c, other, *_), storage, metadata, upload = env
    _before_commit(monkeypatch, metadata, lambda: other.put(f"/{BUCKET}/{SRC}", content=V2))

    resp = c.put(_url(upload), headers=COPY)

    assert resp.status_code == 200
    monkeypatch.undo()
    assert _part_bytes(storage, metadata, upload) == V1
    _assert_clean(storage, metadata, live=2)


# --- the upload closed during the copy ---


def test_abort_during_the_copy_is_no_such_upload_and_leaves_nothing(env, monkeypatch):
    (c, other, *_), storage, metadata, upload = env
    _around_open(
        monkeypatch,
        storage,
        [lambda: other.delete(f"/{BUCKET}/{DST}?uploadId={upload}")],
        after_open=True,
    )

    resp = c.put(_url(upload), headers=COPY)

    assert resp.status_code == 404 and b"NoSuchUpload" in resp.content
    monkeypatch.undo()
    assert not metadata.upload_exists(upload)
    _assert_clean(storage, metadata, live=1)  # the source's blob only


def test_abort_between_the_bytes_and_the_commit_discards_the_new_blob(env, monkeypatch):
    (c, other, *_), storage, metadata, upload = env
    assert c.put(_url(upload, 2), content=b"another part").status_code == 200
    _before_commit(
        monkeypatch, metadata, lambda: other.delete(f"/{BUCKET}/{DST}?uploadId={upload}")
    )

    resp = c.put(_url(upload), headers=COPY)

    assert resp.status_code == 404 and b"NoSuchUpload" in resp.content
    monkeypatch.undo()
    assert not metadata.upload_exists(upload)
    _assert_clean(storage, metadata, live=1)


def test_complete_between_the_bytes_and_the_commit_discards_the_new_blob(env, monkeypatch):
    (c, other, *_), storage, metadata, upload = env
    plain = c.put(_url(upload, 2), content=b"the only part")
    etag = plain.headers["etag"]
    body = f"<CompleteMultipartUpload><Part><PartNumber>2</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    _before_commit(
        monkeypatch,
        metadata,
        lambda: other.post(f"/{BUCKET}/{DST}?uploadId={upload}", content=body),
    )

    resp = c.put(_url(upload), headers=COPY)

    assert resp.status_code == 404 and b"NoSuchUpload" in resp.content
    monkeypatch.undo()
    assert c.get(f"/{BUCKET}/{DST}").content == b"the only part"
    _assert_clean(storage, metadata, live=2)  # the source and the completed object


# --- two writers of one part number ---


def test_of_two_copies_of_one_part_number_the_later_commit_wins(env, monkeypatch):
    (c, other, *_), storage, metadata, upload = env
    second = {}

    def second_copy():
        resp = other.put(_url(upload), headers={**COPY, "x-amz-copy-source-range": "bytes=0-99"})
        second["status"] = resp.status_code

    _before_commit(monkeypatch, metadata, second_copy)

    resp = c.put(_url(upload), headers=COPY)
    monkeypatch.undo()

    # The outer copy committed last: its whole-object part is the one recorded.
    assert resp.status_code == 200 and second["status"] == 200
    assert _part_bytes(storage, metadata, upload) == V1
    _assert_clean(storage, metadata, live=2)


def test_a_plain_upload_of_the_same_part_number_during_the_copy(env, monkeypatch):
    (c, other, *_), storage, metadata, upload = env
    _before_commit(
        monkeypatch, metadata, lambda: other.put(_url(upload), content=b"uploaded meanwhile")
    )

    resp = c.put(_url(upload), headers={**COPY, "x-amz-copy-source-range": "bytes=0-9"})
    monkeypatch.undo()

    assert resp.status_code == 200
    assert _part_bytes(storage, metadata, upload) == V1[:10]
    _assert_clean(storage, metadata, live=2)
