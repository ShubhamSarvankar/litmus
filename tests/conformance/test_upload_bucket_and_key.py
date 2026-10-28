"""UploadPart, UploadPartCopy, CompleteMultipartUpload and AbortMultipartUpload check that the
upload belongs to the request's bucket and key (docs/design/storage-layout.md decision 34).

A request for a bucket that does not exist is 404 NoSuchBucket. Otherwise an upload that does
not exist for that bucket and key is 404 NoSuchUpload, and an upload of another bucket or key
gets exactly the answer of an unknown upload ID. Keys compare as stored, so they are case
sensitive. Both checks come before any byte of the body is read and before any blob is
written; for UploadPartCopy they concern the destination. A refused request changes nothing:
no part row, no blob, no object row, and the upload stays open and usable through its own
bucket and key.

The AWS API reference pages for the four operations say nothing about a request whose bucket
or key differs from the upload's; the codes are the documented NoSuchUpload (404) and, from
the list of error codes, NoSuchBucket (404).
"""

import hashlib
import socket
import sqlite3
import threading
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "bka"
OTHER_BUCKET = "bkb"
KEY = "k1"
PART = b"P" * 16
SOURCE = b"S" * 10
# What the objects the refused requests aim at hold, to show they are never overwritten.
EXISTING = {
    (OTHER_BUCKET, KEY): b"existing in bkb",
    (BUCKET, "k2"): b"existing at k2",
    (BUCKET, "K1"): b"existing at K1",
}

# The request's target, and the answer: a bucket that does not exist, another existing bucket
# with the same key, the same bucket with another key, and the same key in other case.
CASES = {
    "missing-bucket": ("no-such-bucket", KEY, "NoSuchBucket"),
    "other-bucket": (OTHER_BUCKET, KEY, "NoSuchUpload"),
    "other-key": (BUCKET, "k2", "NoSuchUpload"),
    "case-differs": (BUCKET, "K1", "NoSuchUpload"),
}
case = pytest.mark.parametrize("target", CASES.values(), ids=CASES.keys())


def code(resp) -> str | None:
    try:
        return ET.fromstring(resp.text).findtext("Code")
    except ET.ParseError:
        return None


def complete_body(entries: list[tuple[int, str]]) -> bytes:
    items = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>" for n, e in entries
    )
    return f"<CompleteMultipartUpload>{items}</CompleteMultipartUpload>".encode()


@dataclass
class Env:
    client: TestClient
    tmp: Path
    uid: str  # the open upload of BUCKET/KEY, holding part 1
    etag: str  # the ETag of its part 1

    def snapshot(self) -> dict:
        """Every upload, part, object and registry row, and every file under the blob
        directory with its size and digest."""
        db = sqlite3.connect(f"file:{self.tmp / 'meta.db'}?mode=ro", uri=True)
        try:
            rows = {
                table: sorted(map(repr, db.execute(f"SELECT * FROM {table}").fetchall()))
                for table in ("multipart_uploads", "multipart_parts", "objects", "blobs")
            }
        finally:
            db.close()
        blobs = self.tmp / "blobs"
        rows["files"] = {
            str(f.relative_to(blobs)): (
                f.stat().st_size,
                hashlib.sha256(f.read_bytes()).hexdigest(),
            )
            for f in sorted(blobs.rglob("*"))
            if f.is_file()
        }
        return rows


def build_app(tmp: Path):
    return create_app(
        storage=BlobStore(tmp / "blobs"),
        metadata=SQLiteMetadataStore(db_path=tmp / "meta.db"),
        min_part_size=0,
    )


def populate(client) -> tuple[str, str]:
    """The buckets, objects and open upload every test starts from, made through `client`
    (a TestClient, or an httpx client of a running server)."""
    for bucket in (BUCKET, OTHER_BUCKET):
        assert client.put(f"/{bucket}").status_code == 200
    for (bucket, key), data in EXISTING.items():
        assert client.put(f"/{bucket}/{key}", content=data).status_code == 200
    assert client.put(f"/{BUCKET}/src", content=SOURCE).status_code == 200
    uid = ET.fromstring(client.post(f"/{BUCKET}/{KEY}?uploads").text).findtext("UploadId")
    r = client.put(f"/{BUCKET}/{KEY}?partNumber=1&uploadId={uid}", content=PART)
    assert r.status_code == 200
    return uid, r.headers["etag"]


@pytest.fixture
def env(tmp_path: Path) -> Env:
    client = TestClient(build_app(tmp_path), raise_server_exceptions=False)
    uid, etag = populate(client)
    return Env(client, tmp_path, uid, etag)


def upload_part(e: Env, bucket: str, key: str):
    return e.client.put(f"/{bucket}/{key}?partNumber=2&uploadId={e.uid}", content=b"Q" * 8)


def upload_part_copy(e: Env, bucket: str, key: str):
    return e.client.put(
        f"/{bucket}/{key}?partNumber=2&uploadId={e.uid}",
        headers={"x-amz-copy-source": f"/{BUCKET}/src"},
    )


def complete(e: Env, bucket: str, key: str):
    return e.client.post(f"/{bucket}/{key}?uploadId={e.uid}", content=complete_body([(1, e.etag)]))


def abort(e: Env, bucket: str, key: str):
    return e.client.delete(f"/{bucket}/{key}?uploadId={e.uid}")


OPERATIONS = {
    "UploadPart": upload_part,
    "UploadPartCopy": upload_part_copy,
    "CompleteMultipartUpload": complete,
    "AbortMultipartUpload": abort,
}
operation = pytest.mark.parametrize("call", OPERATIONS.values(), ids=OPERATIONS.keys())


def assert_completes_through_its_own_bucket_and_key(e: Env) -> None:
    r = complete(e, BUCKET, KEY)
    assert r.status_code == 200
    got = e.client.get(f"/{BUCKET}/{KEY}")
    assert (got.status_code, got.content) == (200, PART)


@operation
@case
def test_refused_request_is_answered_and_changes_nothing(env, call, target):
    bucket, key, expected = target
    before = env.snapshot()

    resp = call(env, bucket, key)

    assert (resp.status_code, code(resp)) == (404, expected)
    assert env.snapshot() == before


@operation
@case
def test_upload_still_completes_through_its_own_bucket_and_key_after_a_refusal(env, call, target):
    bucket, key, _ = target
    assert call(env, bucket, key).status_code == 404

    assert_completes_through_its_own_bucket_and_key(env)

    for (other_bucket, other_key), data in EXISTING.items():
        got = env.client.get(f"/{other_bucket}/{other_key}")
        assert (got.status_code, got.content) == (200, data)
    # Nothing was created at the refused target.
    if (bucket, key) not in EXISTING:
        assert env.client.head(f"/{bucket}/{key}").status_code == 404


@pytest.mark.parametrize("bucket, key", [(BUCKET, "k2"), (OTHER_BUCKET, KEY), (BUCKET, "K1")])
def test_completing_through_the_key_of_an_existing_object_leaves_it_intact(env, bucket, key):
    before = env.client.get(f"/{bucket}/{key}")
    assert before.content == EXISTING[(bucket, key)]

    resp = complete(env, bucket, key)

    assert (resp.status_code, code(resp)) == (404, "NoSuchUpload")
    after = env.client.get(f"/{bucket}/{key}")
    assert after.content == EXISTING[(bucket, key)]
    assert after.headers["etag"] == before.headers["etag"]
    assert after.headers["last-modified"] == before.headers["last-modified"]


def test_completing_through_a_key_that_does_not_exist_creates_nothing(env):
    resp = complete(env, OTHER_BUCKET, "wrong")

    assert (resp.status_code, code(resp)) == (404, "NoSuchUpload")
    assert env.client.head(f"/{OTHER_BUCKET}/wrong").status_code == 404


def test_abort_through_the_wrong_key_leaves_the_upload_open(env):
    resp = abort(env, BUCKET, "k2")

    assert (resp.status_code, code(resp)) == (404, "NoSuchUpload")
    state = sqlite3.connect(f"file:{env.tmp / 'meta.db'}?mode=ro", uri=True)
    try:
        rows = state.execute("SELECT state FROM multipart_uploads").fetchall()
    finally:
        state.close()
    assert rows == [("open",)]
    # It still takes a part, and an Abort through its own bucket and key ends it.
    assert upload_part(env, BUCKET, KEY).status_code == 200
    assert abort(env, BUCKET, KEY).status_code == 204
    assert complete(env, BUCKET, KEY).status_code == 404


# --- the destination is what is checked; the source is handled as before ---


def copy_from(e: Env, bucket: str, key: str, source: str):
    return e.client.put(
        f"/{bucket}/{key}?partNumber=2&uploadId={e.uid}", headers={"x-amz-copy-source": source}
    )


@case
def test_upload_part_copy_decides_the_destination_before_the_source(env, target):
    bucket, key, expected = target

    resp = copy_from(env, bucket, key, f"/{BUCKET}/no-such-source")

    assert (resp.status_code, code(resp)) == (404, expected)


def test_upload_part_copy_with_a_missing_source_is_still_no_such_key(env):
    resp = copy_from(env, BUCKET, KEY, f"/{BUCKET}/no-such-source")

    assert (resp.status_code, code(resp)) == (404, "NoSuchKey")


def test_upload_part_copy_may_read_a_source_in_another_bucket(env):
    resp = copy_from(env, BUCKET, KEY, f"/{OTHER_BUCKET}/{KEY}")

    assert resp.status_code == 200


# --- keys are compared as stored, after the decoding object keys get ---


def test_keys_compare_after_the_decoding_of_object_keys(tmp_path):
    client = TestClient(build_app(tmp_path), raise_server_exceptions=False)
    populate(client)
    uid = ET.fromstring(client.post(f"/{BUCKET}/a+b/%C3%BC?uploads").text).findtext("UploadId")

    def put(path: str):
        return client.put(f"/{BUCKET}/{path}?partNumber=1&uploadId={uid}", content=b"x")

    assert put("a+b/%C3%BC").status_code == 200
    assert put("a%2Bb/%C3%BC").status_code == 200  # decodes to the same key
    for other in ("a%20b/%C3%BC", "A+b/%C3%BC", "a+b/%C3%9C", "a+b/u"):
        resp = put(other)
        assert (resp.status_code, code(resp)) == (404, "NoSuchUpload"), other


# --- nothing of the body is read before the checks ---


@pytest.fixture(scope="module")
def live(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("upload-bucket-and-key")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(build_app(tmp), host="127.0.0.1", port=port, ws="none", log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("uvicorn did not start")
        time.sleep(0.02)
    # Through the running server: its start has to see an empty store.
    with httpx.Client(base_url=f"http://127.0.0.1:{port}") as client:
        uid, _ = populate(client)
    yield port, uid
    server.should_exit = True
    thread.join(timeout=10)


def answer_without_a_body(port: int, method: str, target: str) -> str:
    """Send the head of a request that promises a 1 MiB body and never sends it, and return
    what the server answers. A server that waits for the body times out here."""
    head = (
        f"{method} {target} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
        "Content-Length: 1048576\r\nConnection: close\r\n\r\n"
    )
    data = b""
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.sendall(head.encode())
        while b"</Error>" not in data:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
    return data.decode()


@pytest.mark.parametrize(
    "method, query",
    [("PUT", "partNumber=2&uploadId={uid}"), ("POST", "uploadId={uid}")],
    ids=["UploadPart", "CompleteMultipartUpload"],
)
@case
def test_the_checks_come_before_the_body_is_read(live, method, query, target):
    port, uid = live
    bucket, key, expected = target

    answered = answer_without_a_body(port, method, f"/{bucket}/{key}?{query.format(uid=uid)}")

    assert answered.startswith("HTTP/1.1 404"), answered
    assert f"<Code>{expected}</Code>" in answered
