"""
Tier A: barrier order on the write paths (docs/design/storage-layout.md §16).

Under SIGKILL the page cache survives, so no Tier B test can tell whether an fsync
was actually called. These tests record the calls themselves, in process, and fail
if one is missing or out of order:

    fsync of the new blob's file -> rename tmp/<id>.tmp -> blobs/<xx>/<id>
    -> fsync of blobs/<xx> -> the commit that makes the blob visible

Each fsync is identified by os.fstat(fd), (device, inode), compared with the blob
that ends up visible and its fanout directory. The visibility commit is the COMMIT
closing the transaction that runs UPDATE blobs SET state = 'live' for that blob.
Each test also checks that the registration commit comes before the temp file is
opened (I2) and that the replaced blobs are unlinked only after the visibility
commit (I4).

Covered: the per-write file and directory fsyncs on PutObject, UploadPart and
CompleteMultipartUpload, and the commit that publishes each. Not covered: the WAL
fsync inside SQLite's commit, startup fsyncs, and paths without a file barrier.
"""

import os
import stat
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from chaos.framework.framed import frame, framed_headers, framed_request
from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

pytestmark = pytest.mark.skipif(os.name == "nt", reason="directory fsync is POSIX-only")

BUCKET = "barrier"
KEY = "obj"


class Recorder:
    """Records fsync, rename, open, unlink and SQL statements, in call order."""

    def __init__(self, monkeypatch, metadata: SQLiteMetadataStore):
        self.events: list[tuple] = []
        self.active = False
        real_fsync, real_rename, real_replace = os.fsync, os.rename, os.replace
        real_open, real_unlink = os.open, os.unlink
        real_connect = metadata._connect

        def fsync(fd):
            result = real_fsync(fd)
            if self.active:
                st = os.fstat(fd)
                kind = "fsync-dir" if stat.S_ISDIR(st.st_mode) else "fsync-file"
                self.events.append((kind, (st.st_dev, st.st_ino)))
            return result

        def rename(src, dst, *args, **kwargs):
            result = real_rename(src, dst, *args, **kwargs)
            if self.active:
                self.events.append(("rename", Path(src), Path(dst)))
            return result

        def replace(src, dst, *args, **kwargs):
            result = real_replace(src, dst, *args, **kwargs)
            if self.active:
                self.events.append(("rename", Path(src), Path(dst)))
            return result

        def open_(path, *args, **kwargs):
            fd = real_open(path, *args, **kwargs)
            if self.active:
                self.events.append(("open", Path(path)))
            return fd

        def unlink(path, *args, **kwargs):
            result = real_unlink(path, *args, **kwargs)
            if self.active:
                self.events.append(("unlink", Path(path)))
            return result

        def connect():
            conn = real_connect()
            conn.set_trace_callback(
                lambda sql: self.events.append(("sql", sql)) if self.active else None
            )
            return conn

        monkeypatch.setattr(os, "fsync", fsync)
        monkeypatch.setattr(os, "rename", rename)
        monkeypatch.setattr(os, "replace", replace)
        monkeypatch.setattr(os, "open", open_)
        monkeypatch.setattr(os, "unlink", unlink)
        monkeypatch.setattr(metadata, "_connect", connect)


def _inode(path: Path) -> tuple[int, int]:
    st = os.stat(path)
    return st.st_dev, st.st_ino


def _transactions(events):
    """(index of BEGIN, index of COMMIT or ROLLBACK, closing word, statements)."""
    txns, begin, statements = [], None, []
    for i, event in enumerate(events):
        if event[0] != "sql":
            continue
        sql = event[1].strip()
        if sql.startswith("BEGIN"):
            begin, statements = i, []
        elif sql in ("COMMIT", "ROLLBACK") and begin is not None:
            txns.append((begin, i, sql, statements))
            begin = None
        elif begin is not None:
            statements.append(sql)
    return txns


def barrier_problems(events, storage: BlobStore, blob_id: str, replaced: list[str]) -> list[str]:
    """Every way the recorded calls break the barrier for blob_id; empty if none."""
    final, temp = storage.blob_path(blob_id), storage.temp_path(blob_id)
    blob_inode, dir_inode = _inode(final), _inode(final.parent)

    def first(pred, after=-1):
        return next((i for i, e in enumerate(events) if i > after and pred(e)), None)

    txns = _transactions(events)
    register = next(
        (t for t in txns if any("INSERT INTO blobs" in s and blob_id in s for s in t[3])), None
    )
    visible = next(
        (t for t in txns if any("SET state = 'live'" in s and blob_id in s for s in t[3])), None
    )
    opened = first(lambda e: e == ("open", temp))
    file_fsync = first(lambda e: e == ("fsync-file", blob_inode))
    renamed = first(lambda e: e == ("rename", temp, final))
    dir_fsync = first(lambda e: e == ("fsync-dir", dir_inode), after=renamed if renamed else -1)

    problems = []
    for name, index in [
        ("registration commit", register),
        ("temp file open", opened),
        ("fsync of the blob file", file_fsync),
        ("rename tmp -> blob path", renamed),
        ("fsync of the fanout directory after the rename", dir_fsync),
        ("visibility commit", visible),
    ]:
        if index is None:
            problems.append(f"missing: {name}")
    if problems:
        return problems
    if visible[2] != "COMMIT":
        problems.append("the visibility transaction did not commit")
    if register[2] != "COMMIT" or not register[1] < opened:
        problems.append("the registration commit does not precede opening the temp file")
    if not file_fsync < renamed:
        problems.append("the blob file is fsynced after the rename, not before")
    if not renamed < dir_fsync < visible[0]:
        problems.append("the visibility commit begins before the fanout directory fsync")
    for old in replaced:
        unlinked = first(lambda e, old=old: e == ("unlink", storage.blob_path(old)))
        if unlinked is not None and unlinked < visible[1]:
            problems.append(f"replaced blob {old} is unlinked before the visibility commit")
        if unlinked is None:
            problems.append(f"replaced blob {old} is never unlinked")
    return problems


@pytest.fixture(params=[pytest.param(False, id="plain"), pytest.param(True, id="framed")])
def framed(request) -> bool:
    """Whether the write under test is sent as an aws-chunked body."""
    return request.param


def _put(client, url: str, data: bytes, framed: bool = False):
    if framed:
        headers, body = framed_request(data)
        return client.put(url, content=body, headers=headers)
    return client.put(url, content=data)


@pytest.fixture
def env(tmp_path: Path, monkeypatch):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    recorder = Recorder(monkeypatch, metadata)
    assert client.put(f"/{BUCKET}").status_code == 200
    return client, storage, metadata, recorder


def _put_overwrite(env, framed: bool = False):
    client, storage, metadata, recorder = env
    assert client.put(f"/{BUCKET}/{KEY}", content=b"old body").status_code == 200
    old = metadata.get_object_meta(BUCKET, KEY).blob_id
    recorder.active = True
    assert _put(client, f"/{BUCKET}/{KEY}", b"new body", framed).status_code == 200
    recorder.active = False
    return metadata.get_object_meta(BUCKET, KEY).blob_id, [old]


def test_put_object_barrier_order(env, framed):
    client, storage, _, recorder = env
    blob_id, replaced = _put_overwrite(env, framed)
    assert barrier_problems(recorder.events, storage, blob_id, replaced) == []


def test_upload_part_barrier_order(env, framed):
    client, storage, metadata, recorder = env
    upload_id = ET.fromstring(client.post(f"/{BUCKET}/{KEY}?uploads").text).findtext("UploadId")
    url = f"/{BUCKET}/{KEY}?partNumber=1&uploadId={upload_id}"
    assert client.put(url, content=b"first attempt").status_code == 200
    (old,) = [p.blob_id for p in metadata.get_parts(upload_id)]
    recorder.active = True
    assert _put(client, url, b"second attempt", framed).status_code == 200
    recorder.active = False
    (blob_id,) = [p.blob_id for p in metadata.get_parts(upload_id)]
    assert barrier_problems(recorder.events, storage, blob_id, [old]) == []


def test_complete_barrier_order(env):
    client, storage, metadata, recorder = env
    assert client.put(f"/{BUCKET}/{KEY}", content=b"existing object").status_code == 200
    old_object = metadata.get_object_meta(BUCKET, KEY).blob_id
    upload_id = ET.fromstring(client.post(f"/{BUCKET}/{KEY}?uploads").text).findtext("UploadId")
    etags = []
    for n, body in enumerate([b"part one", b"part two"], start=1):
        r = client.put(f"/{BUCKET}/{KEY}?partNumber={n}&uploadId={upload_id}", content=body)
        etags.append(r.headers["etag"])
    part_blobs = [p.blob_id for p in metadata.get_parts(upload_id)]
    complete = ET.Element("CompleteMultipartUpload")
    for n, etag in enumerate(etags, start=1):
        part = ET.SubElement(complete, "Part")
        ET.SubElement(part, "PartNumber").text = str(n)
        ET.SubElement(part, "ETag").text = etag
    recorder.active = True
    resp = client.post(f"/{BUCKET}/{KEY}?uploadId={upload_id}", content=ET.tostring(complete))
    recorder.active = False
    assert resp.status_code == 200
    meta = metadata.get_object_meta(BUCKET, KEY)
    assert meta.upload_id == upload_id
    problems = barrier_problems(recorder.events, storage, meta.blob_id, [old_object, *part_blobs])
    assert problems == []
    # One transaction carries the object row and the upload completion (R5).
    (visible,) = [
        t
        for t in _transactions(recorder.events)
        if any("SET state = 'live'" in s and meta.blob_id in s for s in t[3])
    ]
    assert any("INSERT INTO objects" in s for s in visible[3])
    assert any("SET state = 'completed'" in s for s in visible[3])


# --- the checker itself must fail on a broken order ---


def test_checker_reports_a_missing_directory_fsync(env):
    _, storage, _, recorder = env
    blob_id, replaced = _put_overwrite(env)
    dir_inode = _inode(storage.blob_path(blob_id).parent)
    events = [e for e in recorder.events if e != ("fsync-dir", dir_inode)]
    assert barrier_problems(events, storage, blob_id, replaced) == [
        "missing: fsync of the fanout directory after the rename"
    ]


def test_checker_reports_a_missing_file_fsync(env):
    _, storage, _, recorder = env
    blob_id, replaced = _put_overwrite(env)
    blob_inode = _inode(storage.blob_path(blob_id))
    events = [e for e in recorder.events if e != ("fsync-file", blob_inode)]
    assert barrier_problems(events, storage, blob_id, replaced) == [
        "missing: fsync of the blob file"
    ]


def test_checker_reports_a_commit_before_the_directory_fsync(env):
    _, storage, _, recorder = env
    blob_id, replaced = _put_overwrite(env)
    dir_inode = _inode(storage.blob_path(blob_id).parent)
    events = list(recorder.events)
    i = events.index(("fsync-dir", dir_inode))
    moved = events.pop(i)
    events.append(moved)  # now after the visibility commit
    problems = barrier_problems(events, storage, blob_id, replaced)
    assert "the visibility commit begins before the fanout directory fsync" in problems


# --- a refused framed request never reaches the barrier ---


def test_refused_framed_put_reaches_no_durable_step(env):
    client, storage, _, recorder = env
    data = b"refused body" * 1000
    body = frame(data, trailer_line="x-amz-checksum-crc64nvme:AAAAAAAAAAA=")
    recorder.active = True
    resp = client.put(f"/{BUCKET}/{KEY}", content=body, headers=framed_headers(body, len(data)))
    recorder.active = False
    assert resp.status_code == 400
    assert ET.fromstring(resp.text).findtext("Code") == "BadDigest"
    # The temp file was written, but never fsynced, renamed, or made live.
    kinds = [e[0] for e in recorder.events]
    assert "fsync-file" not in kinds and "rename" not in kinds
    assert not any(e[0] == "sql" and "SET state = 'live'" in e[1] for e in recorder.events)
    assert [e for e, _ in storage.scan()] == [] and storage.temp_entries() == []
