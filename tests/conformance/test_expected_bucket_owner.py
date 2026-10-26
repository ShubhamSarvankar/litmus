"""x-amz-expected-bucket-owner and x-amz-source-expected-bucket-owner.

S3 User Guide, "Verifying bucket ownership with bucket owner condition": a request that
names the wrong bucket owner fails with AccessDenied (403), and CreateBucket and
ListBuckets ignore the parameter. The API reference gives x-amz-expected-bucket-owner to
17 operations, and x-amz-source-expected-bucket-owner to CopyObject and UploadPartCopy.

Litmus compares the stripped value to S3_OWNER_ID in the request guard, after every 501
refusal and before routing, so before the body is read and before any existence check.
"""

import asyncio
import re
import socket
import sqlite3
import threading
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

import boto3
import pytest
import uvicorn
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient

from server.api import request_guard
from server.app import create_app
from server.config import settings
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

OWNER = "owner-id"
WRONG = "wrong-owner"
OWNER_HEADER = "x-amz-expected-bucket-owner"
SOURCE_HEADER = "x-amz-source-expected-bucket-owner"
BUCKET = "bkt"
SRC_BUCKET = "src"
EMPTY_BUCKET = "empty"
KEY = "k"
BODY = b"hello"
SRC_BODY = b"0123456789"
PART = b"part-one"


@pytest.fixture(autouse=True)
def owner(monkeypatch):
    monkeypatch.setattr(settings, "owner_id", OWNER)


@dataclass
class Env:
    client: TestClient
    root: Path
    upload_id: str
    part_etag: str


def _new_env(root: Path) -> Env:
    """Buckets bkt (object k, open upload of mp holding part 1), src (s.bin) and empty."""
    root.mkdir()
    app = create_app(
        storage=BlobStore(root / "blobs"), metadata=SQLiteMetadataStore(db_path=root / "meta.db")
    )
    client = TestClient(app, raise_server_exceptions=False)
    for bucket in (BUCKET, SRC_BUCKET, EMPTY_BUCKET):
        assert client.put(f"/{bucket}").status_code == 200
    assert client.put(f"/{BUCKET}/{KEY}", content=BODY).status_code == 200
    assert client.put(f"/{SRC_BUCKET}/s.bin", content=SRC_BODY).status_code == 200
    started = client.post(f"/{BUCKET}/mp?uploads")
    upload_id = ET.fromstring(started.text).findtext("{*}UploadId")
    part = client.put(f"/{BUCKET}/mp?partNumber=1&uploadId={upload_id}", content=PART)
    assert part.status_code == 200
    return Env(client, root, upload_id, part.headers["etag"])


@pytest.fixture
def env(tmp_path) -> Env:
    return _new_env(tmp_path / "env")


def _state(e: Env, ids: bool = True) -> dict:
    """What the store holds. With ids false, blob file names are counted, not listed."""
    con = sqlite3.connect(e.root / "meta.db")
    try:

        def rows(sql: str) -> list:
            return sorted(con.execute(sql).fetchall())

        files = sorted(p.name for p in (e.root / "blobs").rglob("*") if p.is_file())
        return {
            "buckets": rows("SELECT name FROM buckets"),
            "objects": rows("SELECT bucket, key, size, etag FROM objects"),
            "uploads": rows("SELECT bucket, key, state FROM multipart_uploads"),
            "parts": rows(
                "SELECT u.bucket, u.key, p.part_number, p.etag, p.size FROM multipart_parts p "
                "JOIN multipart_uploads u USING (upload_id)"
            ),
            "blobs": rows("SELECT state, kind, bucket, key, part_number FROM blobs"),
            "files": files if ids else len(files),
        }
    finally:
        con.close()


_DELETE_KEY = b"<Delete><Object><Key>k</Key></Object></Delete>"


def _complete_body(e: Env) -> bytes:
    return (
        b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"
        + e.part_etag.encode()
        + b"</ETag></Part></CompleteMultipartUpload>"
    )


# The 17 operations, each as a request that succeeds in a fresh Env.
OPERATIONS = {
    "HeadBucket": lambda e, h: e.client.head(f"/{BUCKET}", headers=h),
    "DeleteBucket": lambda e, h: e.client.delete(f"/{EMPTY_BUCKET}", headers=h),
    "ListObjects": lambda e, h: e.client.get(f"/{BUCKET}", headers=h),
    "ListObjectsV2": lambda e, h: e.client.get(f"/{BUCKET}?list-type=2", headers=h),
    "ListObjectVersions": lambda e, h: e.client.get(f"/{BUCKET}?versions", headers=h),
    "PutObject": lambda e, h: e.client.put(f"/{BUCKET}/new", content=b"data", headers=h),
    "GetObject": lambda e, h: e.client.get(f"/{BUCKET}/{KEY}", headers=h),
    "HeadObject": lambda e, h: e.client.head(f"/{BUCKET}/{KEY}", headers=h),
    "DeleteObject": lambda e, h: e.client.delete(f"/{BUCKET}/{KEY}", headers=h),
    "DeleteObjects": lambda e, h: e.client.post(
        f"/{BUCKET}?delete", content=_DELETE_KEY, headers=h
    ),
    "GetObjectTagging": lambda e, h: e.client.get(f"/{BUCKET}/{KEY}?tagging", headers=h),
    "CreateMultipartUpload": lambda e, h: e.client.post(f"/{BUCKET}/new?uploads", headers=h),
    "UploadPart": lambda e, h: e.client.put(
        f"/{BUCKET}/mp?partNumber=2&uploadId={e.upload_id}", content=b"two", headers=h
    ),
    "UploadPartCopy": lambda e, h: e.client.put(
        f"/{BUCKET}/mp?partNumber=2&uploadId={e.upload_id}",
        headers={"x-amz-copy-source": f"{SRC_BUCKET}/s.bin", **h},
    ),
    "CompleteMultipartUpload": lambda e, h: e.client.post(
        f"/{BUCKET}/mp?uploadId={e.upload_id}", content=_complete_body(e), headers=h
    ),
    "AbortMultipartUpload": lambda e, h: e.client.delete(
        f"/{BUCKET}/mp?uploadId={e.upload_id}", headers=h
    ),
    "CopyObject": lambda e, h: e.client.put(
        f"/{BUCKET}/copy", headers={"x-amz-copy-source": f"{SRC_BUCKET}/s.bin", **h}
    ),
}
OPERATION_NAMES = list(OPERATIONS)
COPIES = ["CopyObject", "UploadPartCopy"]


def _shape(resp) -> tuple:
    """A response as a client sees it, without what differs between two fresh stores."""
    body = resp.text
    for pattern in (r"<UploadId>[^<]*</UploadId>", r"<LastModified>[^<]*</LastModified>"):
        body = re.sub(pattern, "", body)
    names = {k.lower() for k in resp.headers} - {"date", "x-amz-request-id", "last-modified"}
    return (
        resp.status_code,
        resp.headers.get("content-type"),
        resp.headers.get("etag"),
        sorted(names),
        body,
    )


def _assert_denied(resp) -> None:
    assert resp.status_code == 403
    assert resp.headers["x-amz-request-id"]
    if resp.request.method != "HEAD":
        root = ET.fromstring(resp.text)
        assert root.tag == "Error"
        assert root.findtext("Code") == "AccessDenied"
        assert root.findtext("Message") == "Access Denied"


@pytest.fixture
def pair(tmp_path) -> tuple[Env, Env]:
    return _new_env(tmp_path / "a"), _new_env(tmp_path / "b")


# --- the 17 operations ---


@pytest.mark.parametrize("value", [OWNER, f"  {OWNER}  "])
@pytest.mark.parametrize("name", OPERATION_NAMES)
def test_matching_owner_behaves_like_no_header(pair, name, value):
    bare, owned = pair
    expected = OPERATIONS[name](bare, {})
    got = OPERATIONS[name](owned, {OWNER_HEADER: value})
    assert expected.status_code in (200, 204)
    assert _shape(got) == _shape(expected)
    assert _state(owned, ids=False) == _state(bare, ids=False)


@pytest.mark.parametrize("name", OPERATION_NAMES)
def test_wrong_owner_is_access_denied_and_changes_nothing(env, name):
    before = _state(env)
    _assert_denied(OPERATIONS[name](env, {OWNER_HEADER: WRONG}))
    assert _state(env) == before


@pytest.mark.parametrize("name", OPERATION_NAMES)
def test_owner_differing_only_in_case_is_a_mismatch(env, name):
    before = _state(env)
    _assert_denied(OPERATIONS[name](env, {OWNER_HEADER: OWNER.upper()}))
    assert _state(env) == before


@pytest.mark.parametrize("value", ["", "   "])
@pytest.mark.parametrize("name", OPERATION_NAMES)
def test_empty_or_blank_owner_is_absent(pair, name, value):
    bare, blank = pair
    expected = OPERATIONS[name](bare, {})
    got = OPERATIONS[name](blank, {OWNER_HEADER: value})
    assert _shape(got) == _shape(expected)
    assert _state(blank, ids=False) == _state(bare, ids=False)


def test_the_owner_is_not_validated_as_an_account_id(env):
    # A 12 digit account ID that is not S3_OWNER_ID is just a mismatch, and S3_OWNER_ID
    # itself is accepted whatever it looks like.
    _assert_denied(env.client.get(f"/{BUCKET}", headers={OWNER_HEADER: "111122223333"}))
    settings.owner_id = "Not 12 digits!"
    resp = env.client.get(f"/{BUCKET}", headers={OWNER_HEADER: "Not 12 digits!"})
    assert resp.status_code == 200


# --- the source header on the two copy operations ---


@pytest.mark.parametrize("name", COPIES)
def test_matching_source_owner_behaves_like_no_header(pair, name):
    bare, owned = pair
    expected = OPERATIONS[name](bare, {})
    got = OPERATIONS[name](owned, {SOURCE_HEADER: OWNER})
    assert _shape(got) == _shape(expected)
    assert _state(owned, ids=False) == _state(bare, ids=False)
    both = _new_env(pair[0].root.parent / "both")
    got = OPERATIONS[name](both, {OWNER_HEADER: OWNER, SOURCE_HEADER: OWNER})
    assert _shape(got) == _shape(expected)


@pytest.mark.parametrize("name", COPIES)
@pytest.mark.parametrize(
    "headers",
    [
        {SOURCE_HEADER: WRONG},
        {SOURCE_HEADER: OWNER.upper()},
        {SOURCE_HEADER: WRONG, OWNER_HEADER: OWNER},
        {SOURCE_HEADER: OWNER, OWNER_HEADER: WRONG},
        {SOURCE_HEADER: WRONG, OWNER_HEADER: WRONG},
    ],
)
def test_copy_with_either_owner_wrong_is_access_denied(env, name, headers):
    before = _state(env)
    _assert_denied(OPERATIONS[name](env, headers))
    assert _state(env) == before


@pytest.mark.parametrize("name", COPIES)
@pytest.mark.parametrize("value", ["", "   "])
def test_empty_or_blank_source_owner_is_absent(pair, name, value):
    bare, blank = pair
    expected = OPERATIONS[name](bare, {})
    got = OPERATIONS[name](blank, {SOURCE_HEADER: value})
    assert _shape(got) == _shape(expected)


def test_destination_header_is_checked_before_the_source_header(monkeypatch):
    copy = {"x-amz-copy-source": "src/s.bin"}

    def run(mismatching: set[str]) -> list[str]:
        seen = []

        def record(headers, name):
            seen.append(name)
            return name in mismatching

        monkeypatch.setattr(request_guard, "_owner_mismatch", record)
        try:
            request_guard._check_expected_owner("PUT", "copy", set(), copy)
        except request_guard.AccessDenied:
            pass
        return seen

    assert run({OWNER_HEADER, SOURCE_HEADER}) == [OWNER_HEADER]
    assert run({SOURCE_HEADER}) == [OWNER_HEADER, SOURCE_HEADER]
    assert run(set()) == [OWNER_HEADER, SOURCE_HEADER]


@pytest.mark.parametrize(
    "request_for",
    [
        lambda e, h: e.client.put(f"/{BUCKET}/new", content=b"data", headers=h),
        lambda e, h: e.client.put(
            f"/{BUCKET}/mp?partNumber=2&uploadId={e.upload_id}", content=b"two", headers=h
        ),
        lambda e, h: e.client.post(f"/{BUCKET}/new?uploads", headers=h),
        lambda e, h: e.client.get(f"/{BUCKET}/{KEY}", headers=h),
        lambda e, h: e.client.get(f"/{BUCKET}", headers=h),
        lambda e, h: e.client.delete(f"/{BUCKET}/{KEY}", headers=h),
        lambda e, h: e.client.delete(f"/{BUCKET}/mp?uploadId={e.upload_id}", headers=h),
        lambda e, h: e.client.post(f"/{BUCKET}?delete", content=_DELETE_KEY, headers=h),
    ],
)
def test_source_owner_is_ignored_outside_the_copy_operations(pair, request_for):
    bare, wrong = pair
    expected = request_for(bare, {})
    got = request_for(wrong, {SOURCE_HEADER: WRONG})
    assert expected.status_code in (200, 204)
    assert _shape(got) == _shape(expected)
    assert _state(wrong, ids=False) == _state(bare, ids=False)


# --- ListBuckets, CreateBucket and /health ignore both ---


@pytest.mark.parametrize("header", [OWNER_HEADER, SOURCE_HEADER])
def test_list_buckets_ignores_the_headers(env, header):
    resp = env.client.get("/", headers={header: WRONG})
    assert resp.status_code == 200
    assert {b.text for b in ET.fromstring(resp.text).iter("Name")} >= {BUCKET}


@pytest.mark.parametrize("header", [OWNER_HEADER, SOURCE_HEADER])
@pytest.mark.parametrize("path", ["/fresh", "/fresh/", "/fresh?x-id=CreateBucket"])
def test_create_bucket_ignores_the_headers(env, header, path):
    assert env.client.put(path, headers={header: WRONG}).status_code == 200
    assert env.client.head("/fresh").status_code == 200


def test_create_bucket_ignores_both_headers_together(env):
    resp = env.client.put("/fresh", headers={OWNER_HEADER: WRONG, SOURCE_HEADER: WRONG})
    assert resp.status_code == 200
    assert ("fresh",) in _state(env)["buckets"]


def test_health_ignores_the_headers(env):
    assert env.client.get("/health", headers={OWNER_HEADER: WRONG}).status_code == 200


@pytest.mark.parametrize(
    "request_for",
    [
        lambda c, h: c.head(f"/{BUCKET}/", headers=h),
        lambda c, h: c.get(f"/{BUCKET}/", headers=h),
        lambda c, h: c.get(f"/{BUCKET}/?list-type=2", headers=h),
        lambda c, h: c.delete(f"/{EMPTY_BUCKET}/", headers=h),
        lambda c, h: c.post(f"/{BUCKET}/?delete", content=_DELETE_KEY, headers=h),
    ],
)
def test_trailing_slash_bucket_routes_check_the_owner(env, request_for):
    before = _state(env)
    _assert_denied(request_for(env.client, {OWNER_HEADER: WRONG}))
    assert _state(env) == before
    assert request_for(env.client, {OWNER_HEADER: OWNER}).status_code in (200, 204)


# --- order: after the 501 refusals, before existence checks and the body ---


@pytest.mark.parametrize(
    "request_for",
    [
        lambda c, h: c.head("/nobucket", headers=h),
        lambda c, h: c.delete("/nobucket", headers=h),
        lambda c, h: c.get("/nobucket", headers=h),
        lambda c, h: c.get("/nobucket?list-type=2", headers=h),
        lambda c, h: c.get("/nobucket?versions", headers=h),
        lambda c, h: c.put("/nobucket/k", content=b"x", headers=h),
        lambda c, h: c.get("/nobucket/k", headers=h),
        lambda c, h: c.head("/nobucket/k", headers=h),
        lambda c, h: c.delete("/nobucket/k", headers=h),
        lambda c, h: c.post("/nobucket?delete", content=_DELETE_KEY, headers=h),
        lambda c, h: c.get("/nobucket/k?tagging", headers=h),
        lambda c, h: c.post("/nobucket/k?uploads", headers=h),
        lambda c, h: c.put("/nobucket/k?partNumber=1&uploadId=u", content=b"x", headers=h),
        lambda c, h: c.post("/nobucket/k?uploadId=u", content=b"<x/>", headers=h),
        lambda c, h: c.delete("/nobucket/k?uploadId=u", headers=h),
        lambda c, h: c.put("/nobucket/k", headers={"x-amz-copy-source": "src/s.bin", **h}),
    ],
)
def test_wrong_owner_on_a_missing_bucket_is_access_denied(env, request_for):
    _assert_denied(request_for(env.client, {OWNER_HEADER: WRONG}))
    # without the header the same request is the usual error
    assert request_for(env.client, {}).status_code in (404, 400)


def test_wrong_owner_beats_no_such_key_and_no_such_upload(env):
    _assert_denied(env.client.get(f"/{BUCKET}/missing", headers={OWNER_HEADER: WRONG}))
    _assert_denied(env.client.head(f"/{BUCKET}/missing", headers={OWNER_HEADER: WRONG}))
    _assert_denied(env.client.get(f"/{BUCKET}/missing?tagging", headers={OWNER_HEADER: WRONG}))
    _assert_denied(env.client.delete(f"/{BUCKET}/mp?uploadId=nope", headers={OWNER_HEADER: WRONG}))
    _assert_denied(
        env.client.put(
            f"/{BUCKET}/copy",
            headers={"x-amz-copy-source": f"{SRC_BUCKET}/missing", OWNER_HEADER: WRONG},
        )
    )
    _assert_denied(
        env.client.put(
            f"/{BUCKET}/copy",
            headers={"x-amz-copy-source": f"{SRC_BUCKET}/missing", SOURCE_HEADER: WRONG},
        )
    )
    _assert_denied(
        env.client.put(
            f"/{BUCKET}/mp?partNumber=2&uploadId=nope",
            headers={"x-amz-copy-source": f"{SRC_BUCKET}/missing", OWNER_HEADER: WRONG},
        )
    )


@pytest.mark.parametrize(
    "method, path",
    [
        ("PUT", f"/{BUCKET}?acl"),
        ("GET", f"/{BUCKET}?acl"),
        ("GET", f"/{BUCKET}?cors"),
        ("DELETE", f"/{BUCKET}?cors"),
        ("PUT", f"/{BUCKET}?versioning"),
        ("GET", f"/{BUCKET}?uploads"),
        ("PUT", f"/{BUCKET}/{KEY}?acl"),
        ("GET", f"/{BUCKET}/{KEY}?acl"),
        ("PUT", f"/{BUCKET}/{KEY}?tagging"),
        ("DELETE", f"/{BUCKET}/{KEY}?tagging"),
        ("GET", f"/{BUCKET}/{KEY}?attributes"),
        ("GET", f"/{BUCKET}/{KEY}?versionId=other"),
        ("GET", f"/{BUCKET}/{KEY}?uploadId=u"),
        ("PUT", f"/{BUCKET}/{KEY}?partNumber=1"),
    ],
)
def test_wrong_owner_on_an_unimplemented_request_is_still_501(env, method, path):
    before = _state(env)
    resp = env.client.request(method, path, headers={OWNER_HEADER: WRONG})
    assert resp.status_code == 501
    assert ET.fromstring(resp.text).findtext("Code") == "NotImplemented"
    assert _state(env) == before


@pytest.mark.parametrize(
    "headers",
    [
        {"x-amz-tagging": "a=b"},
        {"x-amz-write-offset-bytes": "0"},
        {"x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"},
    ],
)
def test_wrong_owner_on_a_refused_put_header_is_still_501(env, headers):
    resp = env.client.put(f"/{BUCKET}/x", content=b"x", headers={OWNER_HEADER: WRONG, **headers})
    assert resp.status_code == 501


def test_wrong_owner_on_a_refused_copy_is_still_501(env):
    resp = env.client.put(
        f"/{BUCKET}/x",
        headers={
            OWNER_HEADER: WRONG,
            SOURCE_HEADER: WRONG,
            "x-amz-copy-source": f"{SRC_BUCKET}/s.bin?versionId=other",
        },
    )
    assert resp.status_code == 501


def test_wrong_owner_on_a_conditional_delete_header_is_still_501(env):
    resp = env.client.delete(
        f"/{BUCKET}/mp?uploadId={env.upload_id}",
        headers={OWNER_HEADER: WRONG, "x-amz-if-match-initiated-time": "x"},
    )
    assert resp.status_code == 501


def test_invalid_bucket_name_beats_the_owner_check(env):
    resp = env.client.get("/%2E%2E", headers={OWNER_HEADER: WRONG})
    assert resp.status_code == 400
    assert ET.fromstring(resp.text).findtext("Code") == "InvalidBucketName"


def test_wrong_owner_does_not_read_the_put_object_body(env):
    async def receive():
        raise AssertionError("the request body was read")

    sent = []

    async def send(message):
        sent.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "PUT",
        "scheme": "http",
        "path": f"/{BUCKET}/streamed",
        "raw_path": f"/{BUCKET}/streamed".encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [
            (b"host", b"testserver"),
            (b"content-length", b"1048576"),
            (OWNER_HEADER.encode(), WRONG.encode()),
        ],
        "client": ("127.0.0.1", 1),
        "server": ("testserver", 80),
        "state": {},
    }
    before = _state(env)
    asyncio.run(env.client.app(scope, receive, send))
    start = next(m for m in sent if m["type"] == "http.response.start")
    assert start["status"] == 403
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    assert b"<Code>AccessDenied</Code>" in body
    assert _state(env) == before


# --- every registered route is accounted for ---

# Every (path, methods) the app registers, each with the operations it carries. The guard
# decides by method, key and query (request_guard._takes_owner_condition); a new route
# fails this test until someone decides whether its operation takes the header.
KNOWN_ROUTES = {
    "/health": ({"GET"}, "not an S3 operation"),
    "/": ({"GET"}, "ListBuckets"),
    "/{bucket}": (
        {"GET", "HEAD", "PUT", "DELETE", "POST"},
        "ListObjects, ListObjectsV2, ListObjectVersions, HeadBucket, CreateBucket, "
        "DeleteBucket, DeleteObjects",
    ),
    "/{bucket}/": (
        {"GET", "HEAD", "PUT", "DELETE", "POST"},
        "the same as /{bucket}",
    ),
    "/{bucket}/{key:path}": (
        {"GET", "HEAD", "PUT", "DELETE", "POST"},
        "GetObject, GetObjectTagging, HeadObject, PutObject, CopyObject, UploadPart, "
        "UploadPartCopy, DeleteObject, AbortMultipartUpload, CreateMultipartUpload, "
        "CompleteMultipartUpload",
    ),
}


def test_every_registered_route_is_a_known_operation(env):
    registered: dict[str, set[str]] = {}
    for route in env.client.app.routes:
        methods = getattr(route, "methods", None)
        if methods:
            registered.setdefault(route.path, set()).update(methods - {"OPTIONS"})
    unknown = {
        path: methods - KNOWN_ROUTES.get(path, (set(), ""))[0]
        for path, methods in registered.items()
    }
    unknown = {path: methods for path, methods in unknown.items() if methods}
    assert not unknown, f"routes with no decision about x-amz-expected-bucket-owner: {unknown}"


def test_the_guard_covers_exactly_the_17_operations(env):
    """The 17 requests above are the whole set the guard checks: every other request that
    the routes above can carry is CreateBucket or ListBuckets."""
    takes = request_guard._takes_owner_condition
    assert not takes("PUT", "", set())  # CreateBucket
    assert not takes("PUT", "", {"x-id"})
    assert not takes("OPTIONS", "k", set())
    assert not takes("PATCH", "", set())
    assert takes("PUT", "k", set())
    assert takes("PUT", "k", {"partNumber", "uploadId"})
    assert takes("POST", "", {"delete"})
    assert takes("POST", "k", {"uploads"})
    assert takes("POST", "k", {"uploadId"})
    assert takes("DELETE", "", set())
    assert takes("DELETE", "k", set())
    assert takes("DELETE", "k", {"uploadId"})
    assert takes("GET", "k", {"tagging"})
    assert takes("HEAD", "", set())


# --- boto3 ---


@pytest.fixture(scope="module")
def s3(tmp_path_factory):
    patch = pytest.MonkeyPatch()
    patch.setattr(settings, "owner_id", OWNER)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    tmp: Path = tmp_path_factory.mktemp("owner-boto3")
    app = create_app(
        storage=BlobStore(tmp / "blobs"), metadata=SQLiteMetadataStore(db_path=tmp / "meta.db")
    )
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, ws="none", log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("uvicorn did not start")
        time.sleep(0.02)
    client = boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{port}",
        aws_access_key_id="key",
        aws_secret_access_key="secret",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 0}),
    )
    client.create_bucket(Bucket="boto")
    client.put_object(Bucket="boto", Key="src", Body=b"source")
    yield client
    server.should_exit = True
    thread.join(timeout=10)
    patch.undo()


def _denied(error: ClientError) -> None:
    assert error.value.response["Error"]["Code"] == "AccessDenied"
    assert error.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403


def test_boto3_wrong_owner_raises_access_denied(s3):
    with pytest.raises(ClientError) as e:
        s3.put_object(Bucket="boto", Key="denied", Body=b"x", ExpectedBucketOwner=WRONG)
    _denied(e)
    with pytest.raises(ClientError) as e:
        s3.list_objects_v2(Bucket="boto", ExpectedBucketOwner=WRONG)
    _denied(e)
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket="boto", Key="src", ExpectedBucketOwner=WRONG)
    _denied(e)
    with pytest.raises(ClientError) as e:
        s3.create_multipart_upload(Bucket="boto", Key="denied", ExpectedBucketOwner=WRONG)
    _denied(e)
    with pytest.raises(ClientError) as e:
        s3.copy_object(
            Bucket="boto",
            Key="denied",
            CopySource={"Bucket": "boto", "Key": "src"},
            ExpectedSourceBucketOwner=WRONG,
        )
    _denied(e)
    with pytest.raises(ClientError) as e:
        s3.head_bucket(Bucket="boto", ExpectedBucketOwner=WRONG)
    assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket="boto")["Contents"]]
    assert "denied" not in keys


def test_boto3_right_owner_succeeds(s3):
    s3.put_object(Bucket="boto", Key="granted", Body=b"x", ExpectedBucketOwner=OWNER)
    assert (
        s3.get_object(Bucket="boto", Key="granted", ExpectedBucketOwner=OWNER)["Body"].read()
        == b"x"
    )
    s3.head_bucket(Bucket="boto", ExpectedBucketOwner=OWNER)
    listed = s3.list_objects_v2(Bucket="boto", ExpectedBucketOwner=OWNER)
    assert "granted" in [o["Key"] for o in listed["Contents"]]
    s3.copy_object(
        Bucket="boto",
        Key="copied",
        CopySource={"Bucket": "boto", "Key": "src"},
        ExpectedBucketOwner=OWNER,
        ExpectedSourceBucketOwner=OWNER,
    )
    s3.delete_object(Bucket="boto", Key="granted", ExpectedBucketOwner=OWNER)
    s3.delete_object(Bucket="boto", Key="copied")


def test_boto3_list_and_create_ignore_a_wrong_owner(s3):
    s3.list_buckets()
    s3.create_bucket(Bucket="boto-other")
