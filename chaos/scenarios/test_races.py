"""
Tier A: requests racing on the same key, interleaved deterministically
(docs/design/storage-layout.md §8, §9, §6.7).

Concurrency: every test drives one app through two TestClients. Each TestClient runs
the app on its own event loop in its own thread, so the blocking handler code of two
requests runs on two OS threads at once. The interleaving is never left to timing:

- GET racing an overwrite or delete: a hook right after GET reads the row (the §8
  window between the row read and the open) sends the overwrite or delete through
  the second client and waits for it to finish, GC included.
- Same-key PUTs: thread A's commit is held right after it reads the current row
  inside its transaction, until thread B's commit has issued BEGIN IMMEDIATE (seen
  through SQLite's statement trace, which fires before the statement waits for the
  write lock). B can only read the row after A commits.
- Refused Complete: a hook right after assembly aborts the upload, or re-uploads a
  part, through the second client before Complete's commit runs.
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

BUCKET = "race"
KEY = "obj"
OLD = b"old version " * 20_000  # 240 KB: several 64 KiB chunks when streamed
NEW = b"new version! " * 10_000
WAIT = 10.0  # safety net only: no outcome depends on it


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    app = create_app(storage=storage, metadata=metadata)
    reader = TestClient(app, raise_server_exceptions=False)
    writer = TestClient(app, raise_server_exceptions=False)
    # Build the middleware stack before two threads can race to build it.
    assert reader.get("/health").status_code == 200
    assert writer.get("/health").status_code == 200
    assert reader.put(f"/{BUCKET}").status_code == 200
    assert reader.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200
    return reader, writer, storage, metadata


def _after_row_read(monkeypatch, metadata, action, times: int | None = 1) -> list:
    """Run action() right after each of the first `times` row reads of the key
    (every read if None). Returns the blob IDs the reads saw, one per hook run."""
    real = metadata.get_object_meta
    seen = []

    def get_object_meta(bucket, key):
        row = real(bucket, key)
        if times is None or len(seen) < times:
            seen.append(row.blob_id if row else None)
            action()
        return row

    monkeypatch.setattr(metadata, "get_object_meta", get_object_meta)
    return seen


def _record_opens(monkeypatch, storage) -> list:
    """Record every open_blob call as (blob_id, "ok" or "ENOENT")."""
    real = storage.open_blob
    opens = []

    def open_blob(blob_id):
        try:
            f = real(blob_id)
        except FileNotFoundError:
            opens.append((blob_id, "ENOENT"))
            raise
        opens.append((blob_id, "ok"))
        return f

    monkeypatch.setattr(storage, "open_blob", open_blob)
    return opens


# --- §8: GET racing an overwrite or a delete ---


def test_get_racing_an_overwrite_serves_the_new_version(env, monkeypatch):
    reader, writer, storage, metadata = env
    old = metadata.get_object_meta(BUCKET, KEY)
    writes = []
    seen = _after_row_read(
        monkeypatch, metadata, lambda: writes.append(writer.put(f"/{BUCKET}/{KEY}", content=NEW))
    )
    opens = _record_opens(monkeypatch, storage)

    resp = reader.get(f"/{BUCKET}/{KEY}")

    # The overwrite committed and its GC unlinked the old blob between GET's row
    # read and its open, so the first open found nothing.
    assert seen == [old.blob_id]
    assert [w.status_code for w in writes] == [200]
    new = metadata.get_object_meta(BUCKET, KEY)
    assert opens == [(old.blob_id, "ENOENT"), (new.blob_id, "ok")]
    # One version, whole: body, length and ETag all from the new row.
    assert resp.status_code == 200
    assert resp.content == NEW
    assert resp.headers["etag"] == _md5(NEW)
    assert resp.headers["content-length"] == str(len(NEW))


def test_range_get_racing_an_overwrite_serves_a_range_of_the_new_version(env, monkeypatch):
    reader, writer, storage, metadata = env
    old = metadata.get_object_meta(BUCKET, KEY)
    _after_row_read(monkeypatch, metadata, lambda: writer.put(f"/{BUCKET}/{KEY}", content=NEW))
    opens = _record_opens(monkeypatch, storage)

    resp = reader.get(f"/{BUCKET}/{KEY}", headers={"Range": "bytes=100-199"})

    assert opens[0] == (old.blob_id, "ENOENT")
    assert resp.status_code == 206
    assert resp.content == NEW[100:200]
    assert resp.headers["content-range"] == f"bytes 100-199/{len(NEW)}"
    assert resp.headers["etag"] == _md5(NEW)


def test_get_racing_a_delete_returns_404(env, monkeypatch):
    reader, writer, storage, metadata = env
    old = metadata.get_object_meta(BUCKET, KEY)
    deletes = []
    _after_row_read(
        monkeypatch, metadata, lambda: deletes.append(writer.delete(f"/{BUCKET}/{KEY}"))
    )
    opens = _record_opens(monkeypatch, storage)

    resp = reader.get(f"/{BUCKET}/{KEY}")

    assert [d.status_code for d in deletes] == [204]
    assert opens == [(old.blob_id, "ENOENT")]
    assert resp.status_code == 404
    assert ET.fromstring(resp.text).findtext("Code") == "NoSuchKey"


def test_if_match_is_evaluated_again_on_the_row_a_racing_overwrite_leaves(env, monkeypatch):
    """The old row passes If-Match, the overwrite unlinks its blob before the open, and the
    retry reads the new row, which does not match: 412, never the new bytes."""
    reader, writer, storage, metadata = env
    old = metadata.get_object_meta(BUCKET, KEY)
    _after_row_read(monkeypatch, metadata, lambda: writer.put(f"/{BUCKET}/{KEY}", content=NEW))
    opens = _record_opens(monkeypatch, storage)

    resp = reader.get(f"/{BUCKET}/{KEY}", headers={"If-Match": _md5(OLD)})

    assert opens == [(old.blob_id, "ENOENT")]
    assert resp.status_code == 412
    assert ET.fromstring(resp.text).findtext("Code") == "PreconditionFailed"


def test_if_none_match_is_evaluated_again_on_the_row_a_racing_overwrite_leaves(env, monkeypatch):
    """The old row does not match the NEW ETag, so GET goes to open its blob; the overwrite
    unlinks it; the retry reads the new row, which matches: 304 with the new ETag, and the
    new blob is never opened."""
    reader, writer, storage, metadata = env
    old = metadata.get_object_meta(BUCKET, KEY)
    _after_row_read(monkeypatch, metadata, lambda: writer.put(f"/{BUCKET}/{KEY}", content=NEW))
    opens = _record_opens(monkeypatch, storage)

    resp = reader.get(f"/{BUCKET}/{KEY}", headers={"If-None-Match": _md5(NEW)})

    assert opens == [(old.blob_id, "ENOENT")]
    assert resp.status_code == 304
    assert resp.content == b""
    assert resp.headers["etag"] == _md5(NEW)


def test_a_304_answers_for_the_row_it_read_and_opens_no_blob(env, monkeypatch):
    """If-None-Match matches the row GET read; an overwrite right after changes nothing
    for this request: 304 with the old ETag, as if it had run first, and no blob open."""
    reader, writer, storage, metadata = env
    writes = []
    _after_row_read(
        monkeypatch, metadata, lambda: writes.append(writer.put(f"/{BUCKET}/{KEY}", content=NEW))
    )
    opens = _record_opens(monkeypatch, storage)

    resp = reader.get(f"/{BUCKET}/{KEY}", headers={"If-None-Match": _md5(OLD)})

    assert [w.status_code for w in writes] == [200]
    assert opens == []
    assert resp.status_code == 304
    assert resp.headers["etag"] == _md5(OLD)
    assert metadata.get_object_meta(BUCKET, KEY).etag == _md5(NEW)


def test_conditional_get_racing_a_delete_returns_404(env, monkeypatch):
    """A condition that passes on the old row does not outlive the delete: the retry finds
    no row and the answer is NoSuchKey, not a 412 or a 304."""
    reader, writer, storage, metadata = env
    _after_row_read(monkeypatch, metadata, lambda: writer.delete(f"/{BUCKET}/{KEY}"))

    resp = reader.get(f"/{BUCKET}/{KEY}", headers={"If-None-Match": '"other"'})

    assert resp.status_code == 404
    assert ET.fromstring(resp.text).findtext("Code") == "NoSuchKey"


def test_open_descriptor_keeps_streaming_the_old_version_after_an_overwrite(env, monkeypatch):
    """Overwrite and GC run after the first chunk was read: the descriptor pins the
    old blob, and the whole old version is sent with its own ETag."""
    reader, writer, storage, metadata = env
    old = metadata.get_object_meta(BUCKET, KEY)
    real_open = storage.open_blob
    writes = []

    class OverwriteAfterFirstRead:
        def __init__(self, f):
            self._f, self._reads = f, 0

        def read(self, n=-1):
            chunk = self._f.read(n)
            self._reads += 1
            if self._reads == 1:
                writes.append(writer.put(f"/{BUCKET}/{KEY}", content=NEW))
            return chunk

        def __getattr__(self, name):
            return getattr(self._f, name)

    monkeypatch.setattr(
        storage, "open_blob", lambda blob_id: OverwriteAfterFirstRead(real_open(blob_id))
    )

    resp = reader.get(f"/{BUCKET}/{KEY}")

    assert [w.status_code for w in writes] == [200]
    assert not storage.blob_path(old.blob_id).exists(), "the old blob was not unlinked mid-stream"
    assert resp.status_code == 200
    assert resp.content == OLD
    assert resp.headers["etag"] == _md5(OLD)
    assert metadata.get_object_meta(BUCKET, KEY).etag == _md5(NEW)


def test_get_retries_while_commits_keep_replacing_the_key(env, monkeypatch):
    """Two commits race the GET: it retries twice and serves the version that stayed."""
    reader, writer, storage, metadata = env
    bodies = iter([b"version 2" * 100, b"version 3" * 100])
    _after_row_read(
        monkeypatch,
        metadata,
        lambda: writer.put(f"/{BUCKET}/{KEY}", content=next(bodies)),
        times=2,
    )
    opens = _record_opens(monkeypatch, storage)

    resp = reader.get(f"/{BUCKET}/{KEY}")

    assert [outcome for _, outcome in opens] == ["ENOENT", "ENOENT", "ok"]
    assert resp.status_code == 200
    assert resp.content == b"version 3" * 100
    assert resp.headers["etag"] == _md5(b"version 3" * 100)


def test_get_gives_up_with_503_when_every_read_is_raced(env, monkeypatch):
    """A commit replaces the key after every row read: the retries are bounded and
    the answer is 503 SlowDown (decision 14), never a missing file or a mix."""
    reader, writer, storage, metadata = env
    counter = iter(range(1000))
    _after_row_read(
        monkeypatch,
        metadata,
        lambda: writer.put(f"/{BUCKET}/{KEY}", content=b"v%d" % next(counter)),
        times=None,
    )
    opens = _record_opens(monkeypatch, storage)

    resp = reader.get(f"/{BUCKET}/{KEY}")

    assert resp.status_code == 503
    assert ET.fromstring(resp.text).findtext("Code") == "SlowDown"
    assert len(opens) == 4
    assert all(outcome == "ENOENT" for _, outcome in opens)


def _racing_commits(env, monkeypatch, commits: int):
    """GET with `commits` overwrites, one right after each of its first row reads."""
    reader, writer, storage, metadata = env
    bodies = iter([b"version %d" % n * 100 for n in range(2, 2 + commits)])
    _after_row_read(
        monkeypatch,
        metadata,
        lambda: writer.put(f"/{BUCKET}/{KEY}", content=next(bodies)),
        times=commits,
    )
    opens = _record_opens(monkeypatch, storage)
    return reader.get(f"/{BUCKET}/{KEY}"), opens


def test_get_three_racing_commits_are_served_on_the_fourth_open(env, monkeypatch):
    """Decision 14's boundary: three ENOENT retries are allowed."""
    resp, opens = _racing_commits(env, monkeypatch, 3)
    assert [outcome for _, outcome in opens] == ["ENOENT", "ENOENT", "ENOENT", "ok"]
    assert resp.status_code == 200
    assert resp.content == b"version 4" * 100
    assert resp.headers["etag"] == _md5(b"version 4" * 100)


def test_get_four_racing_commits_get_503_after_four_opens(env, monkeypatch):
    """Decision 14's boundary: the fourth failed open answers 503."""
    resp, opens = _racing_commits(env, monkeypatch, 4)
    assert [outcome for _, outcome in opens] == ["ENOENT"] * 4
    assert resp.status_code == 503
    assert ET.fromstring(resp.text).findtext("Code") == "SlowDown"


# --- §9: concurrent PUTs to the same key ---


def test_same_key_puts_commit_one_after_the_other_and_the_later_wins(env, monkeypatch):
    reader, writer, storage, metadata = env
    old = metadata.get_object_meta(BUCKET, KEY)
    body_a, body_b = b"A" * 50_000, b"B" * 60_000
    a_read_row, b_began_commit = threading.Event(), threading.Event()
    both_wrote_bytes = threading.Barrier(2, timeout=WAIT)
    in_commit = threading.local()
    role = {}  # thread ident -> "A" or "B"

    real_commit = metadata.put_object_meta
    real_current = metadata._current
    real_connect = metadata._connect

    def put_object_meta(bucket, key, meta):
        me = "A" if meta.etag == _md5(body_a) else "B"
        role[threading.get_ident()] = me
        both_wrote_bytes.wait()
        if me == "B":
            assert a_read_row.wait(WAIT), "A never read the row inside its commit"
        in_commit.active = True
        try:
            return real_commit(bucket, key, meta)
        finally:
            in_commit.active = False

    def current(conn, bucket, key):
        row = real_current(conn, bucket, key)
        if role.get(threading.get_ident()) == "A" and not a_read_row.is_set():
            # A holds the write lock and has read the row; let B start its commit.
            a_read_row.set()
            assert b_began_commit.wait(WAIT), "B never began its commit"
        return row

    def connect():
        conn = real_connect()
        if getattr(in_commit, "active", False) and role.get(threading.get_ident()) == "B":

            def trace(sql):
                if sql.startswith("BEGIN IMMEDIATE"):
                    b_began_commit.set()

            conn.set_trace_callback(trace)
        return conn

    monkeypatch.setattr(metadata, "put_object_meta", put_object_meta)
    monkeypatch.setattr(metadata, "_current", current)
    monkeypatch.setattr(metadata, "_connect", connect)

    results = {}

    def put(client, name, body):
        results[name] = client.put(f"/{BUCKET}/{KEY}", content=body)

    threads = [
        threading.Thread(target=put, args=(reader, "A", body_a)),
        threading.Thread(target=put, args=(writer, "B", body_b)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(WAIT * 3)
    assert not any(t.is_alive() for t in threads)
    monkeypatch.undo()

    # Both writers were told 200; B committed last and wins.
    assert results["A"].status_code == 200 and results["B"].status_code == 200
    assert a_read_row.is_set() and b_began_commit.is_set()
    final = metadata.get_object_meta(BUCKET, KEY)
    assert final.etag == _md5(body_b)
    resp = reader.get(f"/{BUCKET}/{KEY}")
    assert resp.content == body_b and resp.headers["etag"] == _md5(body_b)
    # Every blob was superseded exactly once: A's commit retired the old blob, B's
    # commit (reading the row only after A committed) retired A's. Nothing is left
    # but B's blob.
    records = metadata.list_blob_records()
    assert [(r.blob_id, r.state) for r in records] == [(final.blob_id, "live")]
    assert {blob_id for _, blob_id in storage.scan()} == {final.blob_id}
    assert old.blob_id != final.blob_id
    assert run_consistency_sweep(metadata, storage).clean


# --- §6.7 / R6: a Complete refused after assembly leaves the object untouched ---


def _upload(client, parts: list[bytes]) -> tuple[str, list[str]]:
    upload_id = ET.fromstring(client.post(f"/{BUCKET}/{KEY}?uploads").text).findtext("UploadId")
    etags = []
    for n, data in enumerate(parts, start=1):
        r = client.put(f"/{BUCKET}/{KEY}?partNumber={n}&uploadId={upload_id}", content=data)
        assert r.status_code == 200
        etags.append(r.headers["etag"])
    return upload_id, etags


def _complete_xml(etags: list[str]) -> bytes:
    root = ET.Element("CompleteMultipartUpload")
    for n, etag in enumerate(etags, start=1):
        part = ET.SubElement(root, "Part")
        ET.SubElement(part, "PartNumber").text = str(n)
        ET.SubElement(part, "ETag").text = etag
    return ET.tostring(root)


def _after_assembly(monkeypatch, storage, action) -> list:
    real = storage.assemble_blob
    assembled = []

    def assemble_blob(blob_id, part_ids, mid_write_point=None):
        size = real(blob_id, part_ids, mid_write_point)
        assembled.append(blob_id)
        action()
        return size

    monkeypatch.setattr(storage, "assemble_blob", assemble_blob)
    return assembled


def test_complete_refused_after_abort_leaves_the_existing_object(env, monkeypatch):
    reader, writer, storage, metadata = env
    old = metadata.get_object_meta(BUCKET, KEY)
    upload_id, etags = _upload(reader, [b"p1" * 3000, b"p2" * 3000])
    aborts = []
    assembled = _after_assembly(
        monkeypatch,
        storage,
        lambda: aborts.append(writer.delete(f"/{BUCKET}/{KEY}?uploadId={upload_id}")),
    )

    resp = reader.post(f"/{BUCKET}/{KEY}?uploadId={upload_id}", content=_complete_xml(etags))

    assert [a.status_code for a in aborts] == [204]
    assert resp.status_code == 404
    assert ET.fromstring(resp.text).findtext("Code") == "NoSuchUpload"
    # The existing object is untouched: same row, same bytes, served as before.
    assert metadata.get_object_meta(BUCKET, KEY) == old
    got = reader.get(f"/{BUCKET}/{KEY}")
    assert got.content == OLD and got.headers["etag"] == _md5(OLD)
    # The assembled blob was discarded; the aborted parts are gone.
    (assembled_id,) = assembled
    assert metadata.blob_state(assembled_id) is None
    assert [(r.blob_id, r.state) for r in metadata.list_blob_records()] == [(old.blob_id, "live")]
    assert {blob_id for _, blob_id in storage.scan()} == {old.blob_id}
    assert storage.temp_entries() == []


def test_complete_refused_when_a_part_changed_during_assembly(env, monkeypatch):
    reader, writer, storage, metadata = env
    old = metadata.get_object_meta(BUCKET, KEY)
    upload_id, etags = _upload(reader, [b"p1" * 3000, b"p2" * 3000])
    reuploads = []
    assembled = _after_assembly(
        monkeypatch,
        storage,
        lambda: reuploads.append(
            writer.put(
                f"/{BUCKET}/{KEY}?partNumber=1&uploadId={upload_id}", content=b"changed" * 1000
            )
        ),
    )

    resp = reader.post(f"/{BUCKET}/{KEY}?uploadId={upload_id}", content=_complete_xml(etags))

    assert [r.status_code for r in reuploads] == [200]
    assert resp.status_code == 400
    assert ET.fromstring(resp.text).findtext("Code") == "InvalidPart"
    assert metadata.get_object_meta(BUCKET, KEY) == old
    got = reader.get(f"/{BUCKET}/{KEY}")
    assert got.content == OLD and got.headers["etag"] == _md5(OLD)
    # The upload stays open with the new part 1; the assembled blob is gone.
    assert metadata.upload_exists(upload_id)
    parts = metadata.get_parts(upload_id)
    assert [p.etag for p in parts] == [_md5(b"changed" * 1000), etags[1]]
    (assembled_id,) = assembled
    assert metadata.blob_state(assembled_id) is None
    live = {old.blob_id} | {p.blob_id for p in parts}
    assert {r.blob_id for r in metadata.list_blob_records()} == live
    assert {blob_id for _, blob_id in storage.scan()} == live
