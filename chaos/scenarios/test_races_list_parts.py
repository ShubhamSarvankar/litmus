"""
Tier A: ListParts racing Complete, Abort and UploadPart on its upload
(docs/design/storage-layout.md §21).

ListParts reads the upload row and one page of its parts in one read transaction, so what
it answers is one committed state: the whole part list of an upload that was open, or
404 NoSuchUpload. The windows are made exact by running the other request inside the
store's read, on another client of the same app, not by timing:

  before  the racing request runs, then the read starts;
  inside  the racing request runs after the upload row is read and before the parts are;
  after   the read has finished, then the racing request runs.

A WAL reader sees the state at its first read, so "inside" must answer for the state before
the racing request, and a read that took the upload and the parts in two transactions
would show the parts of the later state.
"""

import base64
import hashlib
import xml.etree.ElementTree as ET
import zlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata import sqlite
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "list-parts-race"
KEY = "obj"
OLD = b"old version of part one"
NEW = b"the replacement of part one, longer"
TWO = b"part two"
WINDOWS = ["before", "inside", "after"]


def _etag(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    app = create_app(storage=storage, metadata=metadata)
    clients = [TestClient(app, raise_server_exceptions=False) for _ in range(3)]
    for c in clients:  # build the middleware stack before the requests can race
        assert c.get("/health").status_code == 200
    assert clients[0].put(f"/{BUCKET}").status_code == 200
    upload = ET.fromstring(
        clients[0]
        .post(f"/{BUCKET}/{KEY}?uploads", headers={"x-amz-checksum-algorithm": "CRC32"})
        .text
    ).findtext("UploadId")
    for number, data in ((1, OLD), (2, TWO)):
        resp = clients[0].put(
            f"/{BUCKET}/{KEY}?partNumber={number}&uploadId={upload}", content=data
        )
        assert resp.status_code == 200
    return clients, storage, metadata, upload


def _list(client, upload: str):
    return client.get(f"/{BUCKET}/{KEY}?uploadId={upload}")


def _complete(client, upload: str):
    body = (
        "<CompleteMultipartUpload>"
        + "".join(
            f"<Part><PartNumber>{n}</PartNumber><ETag>{_etag(d)}</ETag></Part>"
            for n, d in ((1, OLD), (2, TWO))
        )
        + "</CompleteMultipartUpload>"
    )
    resp = client.post(f"/{BUCKET}/{KEY}?uploadId={upload}", content=body)
    assert resp.status_code == 200
    return resp


def _abort(client, upload: str):
    resp = client.delete(f"/{BUCKET}/{KEY}?uploadId={upload}")
    assert resp.status_code == 204
    return resp


def _replace_part_one(client, upload: str):
    resp = client.put(f"/{BUCKET}/{KEY}?partNumber=1&uploadId={upload}", content=NEW)
    assert resp.status_code == 200
    return resp


def _race(monkeypatch, metadata, window: str, racing) -> list:
    """Run racing() in the given window of the store's list_parts_page. Returns a list the
    racing call's result is appended to."""
    ran = []

    def once():
        ran.append(racing())

    if window == "inside":
        real_upload = sqlite._upload

        def upload_then_race(row):
            built = real_upload(row)
            monkeypatch.setattr(sqlite, "_upload", real_upload)  # only the first read
            once()
            return built

        monkeypatch.setattr(sqlite, "_upload", upload_then_race)
        return ran
    real = metadata.list_parts_page

    def list_parts_page(**kwargs):
        if window == "before":
            once()
        try:
            return real(**kwargs)
        finally:
            if window == "after":
                once()

    monkeypatch.setattr(metadata, "list_parts_page", list_parts_page)
    return ran


def _numbers(resp) -> list[int]:
    return [int(p.findtext("PartNumber")) for p in ET.fromstring(resp.text).findall("Part")]


def _assert_whole_list(resp) -> None:
    """The full part list of the upload, as it was before anything ran against it."""
    assert resp.status_code == 200
    parts = ET.fromstring(resp.text).findall("Part")
    assert [int(p.findtext("PartNumber")) for p in parts] == [1, 2]
    assert [p.findtext("ETag") for p in parts] == [_etag(OLD), _etag(TWO)]
    assert [p.findtext("Size") for p in parts] == [str(len(OLD)), str(len(TWO))]


def _assert_no_such_upload(resp) -> None:
    assert resp.status_code == 404
    assert ET.fromstring(resp.text).findtext("Code") == "NoSuchUpload"


@pytest.mark.parametrize("window", WINDOWS)
def test_listparts_racing_complete_answers_the_whole_list_or_no_such_upload(
    env, monkeypatch, window
):
    clients, storage, metadata, upload = env
    ran = _race(monkeypatch, metadata, window, lambda: _complete(clients[1], upload))

    resp = _list(clients[0], upload)
    monkeypatch.undo()

    assert len(ran) == 1
    if window == "before":
        _assert_no_such_upload(resp)
    else:  # inside: the read began while the upload was open
        _assert_whole_list(resp)
    _assert_no_such_upload(_list(clients[0], upload))
    assert run_consistency_sweep(metadata, storage).issues == []


@pytest.mark.parametrize("window", WINDOWS)
def test_listparts_racing_abort_answers_the_whole_list_or_no_such_upload(env, monkeypatch, window):
    clients, storage, metadata, upload = env
    ran = _race(monkeypatch, metadata, window, lambda: _abort(clients[1], upload))

    resp = _list(clients[0], upload)
    monkeypatch.undo()

    assert len(ran) == 1
    if window == "before":
        _assert_no_such_upload(resp)
    else:
        _assert_whole_list(resp)
    _assert_no_such_upload(_list(clients[0], upload))
    assert run_consistency_sweep(metadata, storage).issues == []


@pytest.mark.parametrize("window", WINDOWS)
def test_listparts_racing_a_replaced_part_shows_one_upload_of_it(env, monkeypatch, window):
    clients, storage, metadata, upload = env
    ran = _race(monkeypatch, metadata, window, lambda: _replace_part_one(clients[1], upload))

    resp = _list(clients[0], upload)
    monkeypatch.undo()

    assert len(ran) == 1
    assert resp.status_code == 200
    first = ET.fromstring(resp.text).findall("Part")[0]
    # ETag, Size and checksum are all of the old upload of part 1, or all of the new one.
    shown = (first.findtext("ETag"), first.findtext("Size"), first.findtext("ChecksumCRC32"))
    old = (_etag(OLD), str(len(OLD)), _checksum(OLD))
    new = (_etag(NEW), str(len(NEW)), _checksum(NEW))
    assert shown == (new if window == "before" else old), (window, shown)
    # Whatever the window, part 1 ends as the new upload.
    final = ET.fromstring(_list(clients[0], upload).text).findall("Part")[0]
    assert (final.findtext("ETag"), final.findtext("Size")) == new[:2]
    assert _numbers(resp) == [1, 2]
    assert run_consistency_sweep(metadata, storage).issues == []


def _checksum(data: bytes) -> str:
    return base64.b64encode(zlib.crc32(data).to_bytes(4, "big")).decode()
