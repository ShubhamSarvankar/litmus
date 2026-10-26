"""Request headers for features Litmus does not have are refused with 501 NotImplemented,
before any body byte is read (server/api/request_guard.py): server-side encryption,
SSE-C, website redirects, Object Lock and storage classes, and three CreateBucket options.
Litmus stores plaintext, has no Object Lock and no website hosting, and has one storage
class, so honoring none of them would succeed without doing what the client asked.

S3 API reference, PutObject, CreateMultipartUpload and CopyObject: x-amz-server-side-
encryption, -aws-kms-key-id, -context, -bucket-key-enabled, the three SSE-C headers,
x-amz-storage-class, x-amz-website-redirect-location and the six x-amz-object-lock-*
headers. UploadPart and UploadPartCopy: the SSE-C headers (the destination side of the
copy). CreateBucket: x-amz-bucket-object-lock-enabled, x-amz-bucket-namespace and
x-amz-object-ownership.

A value that is empty after stripping whitespace counts as absent. x-amz-storage-class is
accepted only as STANDARD, x-amz-bucket-object-lock-enabled only as false (any case),
x-amz-bucket-namespace only as global and x-amz-object-ownership only as
BucketOwnerEnforced. The ACL and grant headers are answered by the handlers, not refused
here (tests/conformance/test_acl_headers.py); x-amz-object-ownership on an object write is
not defined and is left alone, and so are the checksum headers."""

import socket
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import boto3
import pytest
import uvicorn
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient

from server.app import create_app
from server.config import settings
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "test-bucket"
NEW_BUCKET = "new-bucket"
KEY = "obj.txt"
NEW = "new.txt"
COPY = "copy.txt"
OLD = b"old content"
BODY = b"content that must never be stored"
PART = b"part one"

SSE = "x-amz-server-side-encryption"
KMS_KEY = "x-amz-server-side-encryption-aws-kms-key-id"
CONTEXT = "x-amz-server-side-encryption-context"
BUCKET_KEY = "x-amz-server-side-encryption-bucket-key-enabled"
REDIRECT = "x-amz-website-redirect-location"
LOCK_HEADERS = {
    "x-amz-object-lock-mode": "GOVERNANCE",
    "x-amz-object-lock-retain-until-date": "2030-01-01T00:00:00Z",
    "x-amz-object-lock-legal-hold": "ON",
    "x-amz-object-lock-event-hold": "ON",
    "x-amz-object-lock-event-hold-duration-days": "3",
    "x-amz-object-lock-event-hold-duration-years": "1",
}
FEATURE_HEADERS = {
    SSE: "AES256",
    KMS_KEY: "key-id",
    CONTEXT: "e30=",
    BUCKET_KEY: "true",
    REDIRECT: "/elsewhere",
    **LOCK_HEADERS,
}
SSE_C_HEADERS = {
    "x-amz-server-side-encryption-customer-algorithm": "AES256",
    "x-amz-server-side-encryption-customer-key": "a2V5",
    "x-amz-server-side-encryption-customer-key-MD5": "bWQ1",
}
STORAGE_CLASS = "x-amz-storage-class"
LOCK_ENABLED = "x-amz-bucket-object-lock-enabled"
NAMESPACE = "x-amz-bucket-namespace"
BLANK = ["", " ", " \t "]


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    assert client.put(f"/{BUCKET}").status_code == 200
    assert client.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200
    started = client.post(f"/{BUCKET}/mp?uploads")
    upload_id = ET.fromstring(started.text).findtext("{*}UploadId")
    part = client.put(f"/{BUCKET}/mp?partNumber=1&uploadId={upload_id}", content=PART)
    assert part.status_code == 200
    client.upload_id = upload_id
    return client, storage, metadata


def _state(storage, metadata, upload_id: str) -> tuple:
    """Everything a refused request must leave alone: buckets, the objects any operation
    below could write, the blob registry, the uploads and the recorded parts."""
    return (
        sorted(b.name for b in metadata.list_buckets()),
        [metadata.get_object_meta(BUCKET, k) for k in (KEY, NEW, COPY, "mp")],
        sorted((r.blob_id, r.state) for r in metadata.list_blob_records()),
        sorted(u.upload_id for u in metadata.list_incomplete_uploads()),
        [(p.part_number, p.etag, p.blob_id) for p in metadata.get_parts(upload_id)],
        storage.temp_entries(),
    )


def _assert_clean(storage, metadata) -> None:
    assert run_consistency_sweep(metadata, storage).issues == []
    assert storage.temp_entries() == []
    assert {r.state for r in metadata.list_blob_records()} <= {"live"}


def _assert_not_implemented(resp, header: str) -> None:
    assert resp.status_code == 501
    assert resp.headers["content-type"].startswith("application/xml")
    root = ET.fromstring(resp.text)
    assert root.findtext("Code") == "NotImplemented"
    assert root.findtext("Message")
    assert root.findtext("RequestId")
    assert resp.headers["x-amz-request-id"] == root.findtext("RequestId")
    assert root.findtext("Header") == header


# The operations, as (client, headers) -> response. The upload ID is on the client.
def _put_object(c, h):
    return c.put(f"/{BUCKET}/{NEW}", content=BODY, headers=h)


def _create_multipart_upload(c, h):
    return c.post(f"/{BUCKET}/{NEW}?uploads", headers=h)


def _copy_object(c, h):
    return c.put(f"/{BUCKET}/{COPY}", headers={"x-amz-copy-source": f"/{BUCKET}/{KEY}", **h})


def _upload_part(c, h):
    return c.put(f"/{BUCKET}/mp?partNumber=2&uploadId={c.upload_id}", content=BODY, headers=h)


def _upload_part_copy(c, h):
    return c.put(
        f"/{BUCKET}/mp?partNumber=2&uploadId={c.upload_id}",
        headers={"x-amz-copy-source": f"/{BUCKET}/{KEY}", **h},
    )


NEW_OBJECT_OPS = [_put_object, _create_multipart_upload, _copy_object]
PART_OPS = [_upload_part, _upload_part_copy]
ALL_OPS = NEW_OBJECT_OPS + PART_OPS


def ids(op) -> str:
    return op.__name__.lstrip("_")


def _cases(ops, headers):
    return [
        pytest.param(op, name, value, id=f"{ids(op)}-{name}")
        for op in ops
        for name, value in headers.items()
    ]


# --- each header on each operation it applies to ---


@pytest.mark.parametrize("op,name,value", _cases(NEW_OBJECT_OPS, FEATURE_HEADERS))
def test_feature_header_is_501_and_writes_nothing(env, op, name, value):
    client, storage, metadata = env
    before = _state(storage, metadata, client.upload_id)
    _assert_not_implemented(op(client, {name: value}), name)
    assert _state(storage, metadata, client.upload_id) == before
    _assert_clean(storage, metadata)


@pytest.mark.parametrize("op,name,value", _cases(ALL_OPS, SSE_C_HEADERS))
def test_sse_c_header_is_501_and_writes_nothing(env, op, name, value):
    client, storage, metadata = env
    before = _state(storage, metadata, client.upload_id)
    _assert_not_implemented(op(client, {name: value}), name.lower())
    assert _state(storage, metadata, client.upload_id) == before
    _assert_clean(storage, metadata)


@pytest.mark.parametrize("op", NEW_OBJECT_OPS, ids=ids)
@pytest.mark.parametrize("value", ["STANDARD_IA", "GLACIER", "standard", "REDUCED_REDUNDANCY", "x"])
def test_storage_class_other_than_standard_is_501_and_writes_nothing(env, op, value):
    client, storage, metadata = env
    before = _state(storage, metadata, client.upload_id)
    _assert_not_implemented(op(client, {STORAGE_CLASS: value}), STORAGE_CLASS)
    assert _state(storage, metadata, client.upload_id) == before
    _assert_clean(storage, metadata)


def test_put_object_over_an_existing_key_leaves_the_old_object(env):
    client, storage, metadata = env
    meta = metadata.get_object_meta(BUCKET, KEY)
    resp = client.put(f"/{BUCKET}/{KEY}", content=BODY, headers={SSE: "AES256"})
    _assert_not_implemented(resp, SSE)
    assert client.get(f"/{BUCKET}/{KEY}").content == OLD
    assert metadata.get_object_meta(BUCKET, KEY) == meta
    _assert_clean(storage, metadata)


def test_a_repeated_header_with_one_refused_value_is_refused(env):
    client, _, _ = env
    resp = client.put(
        f"/{BUCKET}/{NEW}",
        content=BODY,
        headers=[(STORAGE_CLASS, "STANDARD"), (STORAGE_CLASS, "GLACIER")],
    )
    _assert_not_implemented(resp, STORAGE_CLASS)


def test_the_refusal_names_the_header_with_an_sdk_operation_hint(env):
    client, _, _ = env
    resp = client.put(f"/{BUCKET}/{NEW}?x-id=PutObject", content=BODY, headers={SSE: "aws:kms"})
    _assert_not_implemented(resp, SSE)
    resp = client.post(f"/{BUCKET}/{NEW}?uploads&x-id=CreateMultipartUpload", headers={SSE: "x"})
    _assert_not_implemented(resp, SSE)


# --- empty and whitespace only values are absent ---


@pytest.mark.parametrize("value", BLANK)
@pytest.mark.parametrize("op,name", [(o, n) for o in NEW_OBJECT_OPS for n in FEATURE_HEADERS])
def test_blank_feature_header_is_accepted(env, op, name, value):
    client, storage, metadata = env
    assert op(client, {name: value}).status_code == 200
    _assert_clean(storage, metadata)


@pytest.mark.parametrize("value", BLANK)
@pytest.mark.parametrize("op,name", [(o, n) for o in ALL_OPS for n in SSE_C_HEADERS])
def test_blank_sse_c_header_is_accepted(env, op, name, value):
    client, _, _ = env
    assert op(client, {name: value}).status_code == 200


@pytest.mark.parametrize("value", BLANK)
@pytest.mark.parametrize("op", NEW_OBJECT_OPS, ids=ids)
def test_blank_storage_class_is_accepted(env, op, value):
    client, _, _ = env
    assert op(client, {STORAGE_CLASS: value}).status_code == 200


# --- x-amz-storage-class ---


@pytest.mark.parametrize("op", NEW_OBJECT_OPS, ids=ids)
def test_storage_class_standard_is_accepted(env, op):
    client, storage, metadata = env
    assert op(client, {STORAGE_CLASS: "STANDARD"}).status_code == 200
    _assert_clean(storage, metadata)


def test_put_object_with_storage_class_standard_stores_the_object(env):
    client, _, _ = env
    assert _put_object(client, {STORAGE_CLASS: "STANDARD"}).status_code == 200
    assert client.get(f"/{BUCKET}/{NEW}").content == BODY


# --- CreateBucket ---


def _create_bucket(client, headers, path=f"/{NEW_BUCKET}"):
    return client.put(path, headers=headers)


@pytest.mark.parametrize("value", ["false", "FALSE", "False", " false "])
def test_create_bucket_with_object_lock_false_is_accepted(env, value):
    client, _, metadata = env
    assert _create_bucket(client, {LOCK_ENABLED: value}).status_code == 200
    assert metadata.bucket_exists(NEW_BUCKET)


@pytest.mark.parametrize("value", ["true", "TRUE", "True", "yes", "1", "falsey"])
@pytest.mark.parametrize(
    "path", [f"/{NEW_BUCKET}", f"/{NEW_BUCKET}/", f"/{NEW_BUCKET}?x-id=CreateBucket"]
)
def test_create_bucket_with_object_lock_other_than_false_is_501_and_creates_nothing(
    env, value, path
):
    client, storage, metadata = env
    before = _state(storage, metadata, client.upload_id)
    _assert_not_implemented(_create_bucket(client, {LOCK_ENABLED: value}, path), LOCK_ENABLED)
    assert not metadata.bucket_exists(NEW_BUCKET)
    assert _state(storage, metadata, client.upload_id) == before


def test_create_bucket_with_namespace_global_is_accepted(env):
    client, _, metadata = env
    assert _create_bucket(client, {NAMESPACE: "global"}).status_code == 200
    assert metadata.bucket_exists(NEW_BUCKET)


@pytest.mark.parametrize("value", ["account-regional", "GLOBAL", "x"])
def test_create_bucket_with_namespace_other_than_global_is_501_and_creates_nothing(env, value):
    client, storage, metadata = env
    before = _state(storage, metadata, client.upload_id)
    _assert_not_implemented(_create_bucket(client, {NAMESPACE: value}), NAMESPACE)
    assert not metadata.bucket_exists(NEW_BUCKET)
    assert _state(storage, metadata, client.upload_id) == before


@pytest.mark.parametrize("name", [LOCK_ENABLED, NAMESPACE])
@pytest.mark.parametrize("value", BLANK)
def test_create_bucket_with_a_blank_header_is_accepted(env, name, value):
    client, _, metadata = env
    assert _create_bucket(client, {name: value}).status_code == 200
    assert metadata.bucket_exists(NEW_BUCKET)


def test_create_bucket_with_both_accepted_values_is_created(env):
    client, _, metadata = env
    resp = _create_bucket(client, {LOCK_ENABLED: "false", NAMESPACE: "global"})
    assert resp.status_code == 200
    assert metadata.bucket_exists(NEW_BUCKET)


def test_create_bucket_with_object_ownership_is_left_alone(env):
    client, _, metadata = env
    resp = _create_bucket(client, {"x-amz-object-ownership": "BucketOwnerEnforced"})
    assert resp.status_code == 200
    assert metadata.bucket_exists(NEW_BUCKET)


# --- the body is not read ---


def _unreadable_body():
    def body():
        raise AssertionError("the request body was read")
        yield b""

    return body()


def test_put_object_is_refused_before_the_body_is_read(env):
    client, storage, metadata = env
    resp = client.put(f"/{BUCKET}/{NEW}", content=_unreadable_body(), headers={SSE: "AES256"})
    _assert_not_implemented(resp, SSE)
    _assert_clean(storage, metadata)


def test_put_object_sse_c_is_refused_before_the_body_is_read(env):
    client, _, _ = env
    name = "x-amz-server-side-encryption-customer-key"
    resp = client.put(f"/{BUCKET}/{NEW}", content=_unreadable_body(), headers={name: "a2V5"})
    _assert_not_implemented(resp, name)


def test_put_object_storage_class_is_refused_before_the_body_is_read(env):
    client, _, _ = env
    resp = client.put(
        f"/{BUCKET}/{NEW}", content=_unreadable_body(), headers={STORAGE_CLASS: "GLACIER"}
    )
    _assert_not_implemented(resp, STORAGE_CLASS)


def test_upload_part_is_refused_before_the_body_is_read(env):
    client, storage, metadata = env
    name = "x-amz-server-side-encryption-customer-algorithm"
    before = _state(storage, metadata, client.upload_id)
    resp = client.put(
        f"/{BUCKET}/mp?partNumber=2&uploadId={client.upload_id}",
        content=_unreadable_body(),
        headers={name: "AES256"},
    )
    _assert_not_implemented(resp, name)
    assert _state(storage, metadata, client.upload_id) == before
    _assert_clean(storage, metadata)


# --- the refusal comes before the owner check ---


@pytest.fixture
def owner(monkeypatch):
    monkeypatch.setattr(settings, "owner_id", "owner-id")


@pytest.mark.parametrize(
    "op,name,value",
    [pytest.param(op, SSE, "AES256", id=ids(op)) for op in NEW_OBJECT_OPS]
    + [
        pytest.param(op, "x-amz-server-side-encryption-customer-key", "a2V5", id=f"{ids(op)}-sse-c")
        for op in ALL_OPS
    ]
    + [pytest.param(op, STORAGE_CLASS, "GLACIER", id=f"{ids(op)}-class") for op in NEW_OBJECT_OPS],
)
def test_a_refused_header_beats_a_wrong_expected_owner(env, owner, op, name, value):
    client, _, _ = env
    wrong = {"x-amz-expected-bucket-owner": "someone-else"}
    assert op(client, wrong).status_code == 403
    _assert_not_implemented(op(client, {**wrong, name: value}), name.lower())


def test_a_refused_header_beats_a_wrong_source_expected_owner(env, owner):
    client, _, _ = env
    headers = {"x-amz-source-expected-bucket-owner": "someone-else"}
    assert _copy_object(client, headers).status_code == 403
    _assert_not_implemented(_copy_object(client, {**headers, SSE: "AES256"}), SSE)


def test_a_wrong_expected_owner_alone_is_still_403(env, owner):
    client, _, _ = env
    headers = {"x-amz-expected-bucket-owner": "someone-else", STORAGE_CLASS: "STANDARD"}
    assert _put_object(client, headers).status_code == 403


# --- the headers are ignored where the operation does not define them ---


def test_feature_headers_on_upload_part_are_ignored(env):
    """Encryption, website redirect, storage class and Object Lock are set when the upload
    is created; UploadPart and UploadPartCopy define only the SSE-C headers."""
    client, _, _ = env
    headers = {**FEATURE_HEADERS, STORAGE_CLASS: "GLACIER"}
    assert _upload_part(client, headers).status_code == 200
    assert _upload_part_copy(client, headers).status_code == 200


@pytest.mark.parametrize(
    "headers",
    [
        {STORAGE_CLASS: "GLACIER"},
        {SSE: "AES256"},
        {**LOCK_HEADERS},
        {REDIRECT: "/elsewhere"},
    ],
    ids=["storage-class", "sse", "object-lock", "redirect"],
)
def test_headers_are_ignored_on_operations_that_do_not_define_them(env, headers):
    client, _, _ = env
    assert client.get(f"/{BUCKET}/{KEY}", headers=headers).status_code == 200
    assert client.head(f"/{BUCKET}/{KEY}", headers=headers).status_code == 200
    assert client.get(f"/{BUCKET}?list-type=2", headers=headers).status_code == 200
    assert client.head(f"/{BUCKET}", headers=headers).status_code == 200
    assert client.get(f"/{BUCKET}/{KEY}?tagging", headers=headers).status_code == 200
    abort = client.delete(f"/{BUCKET}/mp?uploadId={client.upload_id}", headers=headers)
    assert abort.status_code == 204
    assert client.delete(f"/{BUCKET}/{KEY}", headers=headers).status_code == 204


def test_create_bucket_headers_are_ignored_on_object_operations(env):
    client, _, _ = env
    headers = {LOCK_ENABLED: "true", NAMESPACE: "account-regional"}
    assert _put_object(client, headers).status_code == 200
    assert _copy_object(client, headers).status_code == 200
    assert _create_multipart_upload(client, headers).status_code == 200


def test_object_headers_are_ignored_on_create_bucket(env):
    client, _, metadata = env
    resp = _create_bucket(client, {STORAGE_CLASS: "GLACIER", SSE: "AES256", **LOCK_HEADERS})
    assert resp.status_code == 200
    assert metadata.bucket_exists(NEW_BUCKET)


def test_headers_left_alone_are_still_accepted(env):
    client, _, _ = env
    headers = {"x-amz-acl": "private", "x-amz-object-ownership": "ObjectWriter"}
    assert _put_object(client, headers).status_code == 200


def test_complete_multipart_upload_ignores_the_object_headers(env):
    client, _, _ = env
    etag = client.put(
        f"/{BUCKET}/mp?partNumber=1&uploadId={client.upload_id}", content=PART
    ).headers["etag"]
    xml = (
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>"
        f"<ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    )
    resp = client.post(
        f"/{BUCKET}/mp?uploadId={client.upload_id}",
        content=xml,
        headers={STORAGE_CLASS: "GLACIER", SSE: "AES256"},
    )
    assert resp.status_code == 200


# --- boto3 ---


@pytest.fixture(scope="module")
def s3(tmp_path_factory):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    tmp: Path = tmp_path_factory.mktemp("feature-headers")
    metadata = SQLiteMetadataStore(db_path=tmp / "meta.db")
    app = create_app(storage=BlobStore(tmp / "blobs"), metadata=metadata)
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
    client.create_bucket(Bucket="boto3-headers")
    client.metadata_store = metadata
    yield client
    server.should_exit = True
    thread.join(timeout=10)


SSE_C = {"SSECustomerAlgorithm": "AES256", "SSECustomerKey": "k" * 32}


def _assert_client_error(info) -> None:
    assert info.value.response["Error"]["Code"] == "NotImplemented"
    assert info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 501


@pytest.mark.parametrize(
    "params",
    [
        {"ServerSideEncryption": "AES256"},
        {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "key"},
        SSE_C,
        {"StorageClass": "GLACIER"},
        {"WebsiteRedirectLocation": "/elsewhere"},
        {"ObjectLockMode": "GOVERNANCE"},
    ],
    ids=["sse", "kms", "sse-c", "glacier", "redirect", "object-lock"],
)
def test_boto3_put_object_raises_not_implemented(s3, params):
    with pytest.raises(ClientError) as info:
        s3.put_object(Bucket="boto3-headers", Key="refused", Body=b"x", **params)
    _assert_client_error(info)
    assert "Contents" not in s3.list_objects_v2(Bucket="boto3-headers", Prefix="refused")


def test_boto3_other_operations_raise_not_implemented(s3):
    s3.put_object(Bucket="boto3-headers", Key="source", Body=b"x")
    started = s3.create_multipart_upload(Bucket="boto3-headers", Key="mp")
    upload_id = started["UploadId"]
    calls = [
        lambda: s3.create_multipart_upload(Bucket="boto3-headers", Key="r", StorageClass="GLACIER"),
        lambda: s3.copy_object(
            Bucket="boto3-headers",
            Key="r",
            CopySource={"Bucket": "boto3-headers", "Key": "source"},
            ServerSideEncryption="AES256",
        ),
        lambda: s3.upload_part(
            Bucket="boto3-headers", Key="mp", UploadId=upload_id, PartNumber=1, Body=b"x", **SSE_C
        ),
        lambda: s3.upload_part_copy(
            Bucket="boto3-headers",
            Key="mp",
            UploadId=upload_id,
            PartNumber=1,
            CopySource={"Bucket": "boto3-headers", "Key": "source"},
            **SSE_C,
        ),
        lambda: s3.create_bucket(Bucket="boto3-lock", ObjectLockEnabledForBucket=True),
        lambda: s3.create_bucket(Bucket="boto3-ns", BucketNamespace="account-regional"),
    ]
    for call in calls:
        with pytest.raises(ClientError) as info:
            call()
        _assert_client_error(info)
    assert s3.metadata_store.get_parts(upload_id) == []
    names = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    assert "boto3-lock" not in names and "boto3-ns" not in names
    s3.abort_multipart_upload(Bucket="boto3-headers", Key="mp", UploadId=upload_id)


def test_boto3_accepts_standard_and_object_lock_false(s3):
    s3.put_object(Bucket="boto3-headers", Key="std", Body=b"x", StorageClass="STANDARD")
    assert s3.get_object(Bucket="boto3-headers", Key="std")["Body"].read() == b"x"
    s3.create_bucket(Bucket="boto3-unlocked", ObjectLockEnabledForBucket=False)
    assert "boto3-unlocked" in [b["Name"] for b in s3.list_buckets()["Buckets"]]
