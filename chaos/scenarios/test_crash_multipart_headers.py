"""
Tier B chaos: the headers and user metadata given to CreateMultipartUpload reach the
object in the same transaction that completes the upload (commit_completed_upload), so
a kill leaves the old object with its old headers and the upload open, or the new
object with the upload's headers and the upload closed, never a mix.

Each test arms one Complete crash point, confirms the server died by SIGKILL, and
asserts (a) the raw rows while the server is down, (b) what clients see after an
unarmed restart and (c) that the client's retry gives the object the upload's headers.
"""

import os
import signal
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import httpx
import pytest

from chaos.fixtures.subprocess import ServerProcess, _find_free_port
from server.crash_points import COMPLETE_AFTER_ASSEMBLE, COMPLETE_AFTER_COMMIT
from server.metadata.sqlite import SQLiteMetadataStore

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SIGKILL not available on Windows")

BUCKET = "mp-hdr-crash"
KEY = "obj.bin"
URL = f"/{BUCKET}/{KEY}"
OLD = b"old body. " * 3000
PARTS = [bytes([n]) * (64 * 1024) for n in (1, 2, 3)]
NEW = b"".join(PARTS)

OLD_HEADERS = {
    "Content-Type": "text/old",
    "Cache-Control": "max-age=60",
    "Content-Language": "en",
    "x-amz-meta-shared": "old",
    "x-amz-meta-only-old": "1",
}
# The upload's set differs from the object's, so a header that leaks from the old
# object shows up as one the upload never had, and a shared name shows the new value.
UPLOAD_HEADERS = {
    "Content-Type": "text/new",
    "Content-Disposition": "attachment",
    "Content-Encoding": "gzip",
    "Expires": "Thu, 01 Dec 2033 16:00:00 GMT",
    "x-amz-meta-shared": "new",
    "x-amz-meta-only-new": "2",
}
SEEN = [
    "content-type",
    "cache-control",
    "content-disposition",
    "content-encoding",
    "content-language",
    "expires",
]


def _start(tmp_path: Path, crash_point: str | None = None) -> tuple[ServerProcess, httpx.Client]:
    port = _find_free_port()
    server = ServerProcess(
        data_dir=tmp_path, db_path=tmp_path / "meta.db", port=port, crash_point=crash_point
    )
    server.start()
    server.wait_ready()
    return server, httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10.0)


@contextmanager
def _restarted(tmp_path: Path):
    server, client = _start(tmp_path)
    try:
        yield client
    finally:
        client.close()
        server.stop()


def _store(tmp_path: Path) -> SQLiteMetadataStore:
    return SQLiteMetadataStore(db_path=tmp_path / "meta.db")


def _complete_xml(etags: list[str]) -> bytes:
    root = ET.Element("CompleteMultipartUpload")
    for num, etag in enumerate(etags, start=1):
        part = ET.SubElement(root, "Part")
        ET.SubElement(part, "PartNumber").text = str(num)
        ET.SubElement(part, "ETag").text = etag
    return ET.tostring(root)


def _setup(tmp_path: Path) -> tuple[str, list[str]]:
    """An old object with OLD_HEADERS, and an open upload of three parts created with
    UPLOAD_HEADERS, on an unarmed server that is then stopped."""
    server, client = _start(tmp_path)
    try:
        assert client.put(f"/{BUCKET}").status_code == 200
        assert client.put(URL, content=OLD, headers=OLD_HEADERS).status_code == 200
        created = client.post(f"{URL}?uploads", headers=UPLOAD_HEADERS)
        assert created.status_code == 200
        upload_id = ET.fromstring(created.text).findtext("UploadId")
        etags = []
        for num, data in enumerate(PARTS, start=1):
            part = client.put(f"{URL}?partNumber={num}&uploadId={upload_id}", content=data)
            assert part.status_code == 200
            etags.append(part.headers["etag"])
        return upload_id, etags
    finally:
        client.close()
        server.stop()


def _complete_into_crash(server: ServerProcess, client: httpx.Client, upload_id: str, etags):
    with pytest.raises(httpx.TransportError):
        client.post(f"{URL}?uploadId={upload_id}", content=_complete_xml(etags))
    assert server.wait_for_exit() == -signal.SIGKILL


def _wire(headers: dict[str, str]) -> dict[str, str]:
    return {k.lower(): v for k, v in headers.items()}


def _row_headers(meta) -> dict[str, str]:
    """An object or upload row's headers and metadata, named as on the wire."""
    stored = {
        "content-type": meta.content_type,
        "cache-control": meta.cache_control,
        "content-disposition": meta.content_disposition,
        "content-encoding": meta.content_encoding,
        "content-language": meta.content_language,
        "expires": meta.expires,
    }
    stored = {k: v for k, v in stored.items() if v is not None}
    return stored | {f"x-amz-meta-{k}": v for k, v in meta.user_metadata.items()}


def _seen(client: httpx.Client) -> tuple[bytes, dict[str, str]]:
    """The body GET returns (undecoded) and the headers and metadata GET and HEAD agree on."""
    with client.stream("GET", URL) as resp:
        assert resp.status_code == 200
        body = b"".join(resp.iter_raw())
        got = {k: v for k, v in resp.headers.items() if k in SEEN or k.startswith("x-amz-meta-")}
        last_modified = resp.headers["last-modified"]
    head = client.head(URL)
    assert {
        k: v for k, v in head.headers.items() if k in SEEN or k.startswith("x-amz-meta-")
    } == got
    assert head.headers["last-modified"] == last_modified
    return body, got


def test_complete_killed_after_assemble_leaves_the_old_headers_and_the_upload_open(tmp_path: Path):
    upload_id, etags = _setup(tmp_path)
    old = _store(tmp_path).get_object_meta(BUCKET, KEY)
    server, client = _start(tmp_path, crash_point=COMPLETE_AFTER_ASSEMBLE)
    _complete_into_crash(server, client, upload_id, etags)
    client.close()

    # (a) The object row is untouched; the upload is open with the headers it was given.
    store = _store(tmp_path)
    row = store.get_object_meta(BUCKET, KEY)
    assert (row.blob_id, row.last_modified) == (old.blob_id, old.last_modified)
    assert _row_headers(row) == _wire(OLD_HEADERS)
    upload = store.get_upload(upload_id)
    assert upload.state == "open"
    assert _row_headers(upload) == _wire(UPLOAD_HEADERS)

    # (b) After a restart: the old object, headers and Last-Modified included.
    with _restarted(tmp_path) as client2:
        body, got = _seen(client2)
        assert (body, got) == (OLD, _wire(OLD_HEADERS))
        # (c) The retry completes the upload, and its headers replace the old ones.
        resp = client2.post(f"{URL}?uploadId={upload_id}", content=_complete_xml(etags))
        assert resp.status_code == 200
        body, got = _seen(client2)
        assert (body, got) == (NEW, _wire(UPLOAD_HEADERS))
    final = _store(tmp_path).get_object_meta(BUCKET, KEY)
    assert final.last_modified == upload.initiated
    assert final.last_modified != old.last_modified


def test_complete_killed_after_commit_shows_the_new_object_with_the_upload_headers(tmp_path: Path):
    upload_id, etags = _setup(tmp_path)
    old = _store(tmp_path).get_object_meta(BUCKET, KEY)
    initiated = _store(tmp_path).get_upload(upload_id).initiated
    server, client = _start(tmp_path, crash_point=COMPLETE_AFTER_COMMIT)
    _complete_into_crash(server, client, upload_id, etags)
    client.close()

    # (a) One transaction: the new object carries the upload's headers, metadata and
    # initiation time, nothing from the old object, and the upload is closed.
    store = _store(tmp_path)
    row = store.get_object_meta(BUCKET, KEY)
    assert row.blob_id != old.blob_id and row.size == len(NEW)
    assert _row_headers(row) == _wire(UPLOAD_HEADERS)
    assert row.last_modified == initiated
    assert row.upload_id == upload_id
    assert store.get_upload(upload_id).state == "completed"

    # (b) The same after a restart.
    with _restarted(tmp_path) as client2:
        body, got = _seen(client2)
        assert (body, got) == (NEW, _wire(UPLOAD_HEADERS))
        listing = client2.get(f"/{BUCKET}?list-type=2")
        stamp = ET.fromstring(listing.text)
        stamps = [e.text for e in stamp.iter() if e.tag.rsplit("}", 1)[-1] == "LastModified"]
        assert [datetime.fromisoformat(s.rstrip("Z")) for s in stamps] == [
            initiated.replace(microsecond=0)
        ]
