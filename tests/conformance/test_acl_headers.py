"""ACLs are disabled: Litmus behaves as a bucket whose Object Ownership setting is
BucketOwnerEnforced, the default for a new bucket (server/api/acl_headers.py).

Object writes (PutObject, CreateMultipartUpload, CopyObject): x-amz-acl absent, private or
bucket-owner-full-control is accepted; any other x-amz-acl value and any non-blank
x-amz-grant-full-control, -read, -read-acp or -write-acp is 400 AccessControlListNotSupported.
The check runs in the handler right after the bucket lookup, before the body is read and
before a copy source is parsed.

CreateBucket: any x-amz-object-ownership but BucketOwnerEnforced is 501 NotImplemented, in
the request guard; any x-amz-acl but private and any non-blank grant header (the four
above and x-amz-grant-write) is 400 InvalidBucketAclWithObjectOwnership, after the name is
validated and before the existence check.

A value that is empty after stripping whitespace counts as absent. Values are compared
exactly, case sensitive."""

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

ACL = "x-amz-acl"
OWNERSHIP = "x-amz-object-ownership"
OBJECT_GRANTS = [
    "x-amz-grant-full-control",
    "x-amz-grant-read",
    "x-amz-grant-read-acp",
    "x-amz-grant-write-acp",
]
BUCKET_GRANTS = [*OBJECT_GRANTS, "x-amz-grant-write"]
# Owner only grants are refused too: a grant is an ACL.
GRANT_VALUES = ['id="owner-id"', 'uri="http://acs.amazonaws.com/groups/global/AllUsers"', "x"]
ACCEPTED_ACLS = ["private", "bucket-owner-full-control"]
BLANK = ["", " ", " \t "]
# The canned ACLs the API reference lists for PutObject, minus the two accepted ones, a
# value that is not a canned ACL, and case variants.
REFUSED_OBJECT_ACLS = [
    "public-read",
    "public-read-write",
    "authenticated-read",
    "aws-exec-read",
    "bucket-owner-read",
    "not-a-canned-acl",
    "Private",
    "PRIVATE",
    "Bucket-Owner-Full-Control",
    "private,public-read",
]
REFUSED_BUCKET_ACLS = [
    "public-read",
    "public-read-write",
    "authenticated-read",
    "bucket-owner-full-control",
    "not-a-canned-acl",
    "Private",
]


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
    """Everything a refused request must leave alone."""
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


def _assert_error(resp, status: int, code: str) -> None:
    assert resp.status_code == status
    assert resp.headers["content-type"].startswith("application/xml")
    root = ET.fromstring(resp.text)
    assert root.findtext("Code") == code
    assert root.findtext("Message")
    assert root.findtext("RequestId")
    assert resp.headers["x-amz-request-id"] == root.findtext("RequestId")


def _assert_not_supported(resp) -> None:
    _assert_error(resp, 400, "AccessControlListNotSupported")
    assert ET.fromstring(resp.text).findtext("Message") == "The bucket does not allow ACLs"


def _assert_invalid_bucket_acl(resp) -> None:
    _assert_error(resp, 400, "InvalidBucketAclWithObjectOwnership")
    assert (
        ET.fromstring(resp.text).findtext("Message")
        == "Bucket cannot have ACLs set with ObjectOwnership's BucketOwnerEnforced setting"
    )


def _put_object(c, h, bucket=BUCKET):
    return c.put(f"/{bucket}/{NEW}", content=BODY, headers=h)


def _create_multipart_upload(c, h, bucket=BUCKET):
    return c.post(f"/{bucket}/{NEW}?uploads", headers=h)


def _copy_object(c, h, bucket=BUCKET):
    return c.put(f"/{bucket}/{COPY}", headers={"x-amz-copy-source": f"/{BUCKET}/{KEY}", **h})


def _upload_part(c, h):
    return c.put(f"/{BUCKET}/mp?partNumber=2&uploadId={c.upload_id}", content=BODY, headers=h)


def _create_bucket(c, h, path=f"/{NEW_BUCKET}"):
    return c.put(path, headers=h)


OPS = [_put_object, _create_multipart_upload, _copy_object]


def ids(op) -> str:
    return op.__name__.lstrip("_")


def _written(op, metadata, upload_id=None) -> bool:
    """Whether a successful op left its mark."""
    if op is _put_object:
        return metadata.get_object_meta(BUCKET, NEW) is not None
    if op is _copy_object:
        return metadata.get_object_meta(BUCKET, COPY) is not None
    return len(metadata.list_incomplete_uploads()) == 2


# --- object writes: accepted ---


@pytest.mark.parametrize("op", OPS, ids=ids)
@pytest.mark.parametrize("headers", [{}, *({ACL: v} for v in [*BLANK, *ACCEPTED_ACLS])], ids=repr)
def test_object_write_with_an_accepted_or_absent_acl_writes(env, op, headers):
    client, _, metadata = env
    assert not _written(op, metadata)
    assert op(client, headers).status_code == 200
    assert _written(op, metadata)


@pytest.mark.parametrize("op", OPS, ids=ids)
@pytest.mark.parametrize("name", OBJECT_GRANTS)
@pytest.mark.parametrize("value", BLANK)
def test_object_write_with_a_blank_grant_writes(env, op, name, value):
    client, _, metadata = env
    assert op(client, {name: value}).status_code == 200
    assert _written(op, metadata)


@pytest.mark.parametrize("op", OPS, ids=ids)
def test_object_write_with_an_accepted_acl_and_blank_grants_writes(env, op):
    client, _, metadata = env
    headers = {ACL: "bucket-owner-full-control", **dict.fromkeys(OBJECT_GRANTS, " ")}
    assert op(client, headers).status_code == 200
    assert _written(op, metadata)


@pytest.mark.parametrize("op", OPS, ids=ids)
@pytest.mark.parametrize("value", ["BucketOwnerEnforced", "ObjectWriter", "junk"])
def test_object_ownership_on_an_object_write_is_ignored(env, op, value):
    client, _, metadata = env
    assert op(client, {OWNERSHIP: value}).status_code == 200
    assert _written(op, metadata)


# --- object writes: refused ---


def _refused_acl_cases():
    return [
        pytest.param(op, {ACL: v}, id=f"{ids(op)}-{v}") for op in OPS for v in REFUSED_OBJECT_ACLS
    ]


def _refused_grant_cases():
    return [
        pytest.param(op, {name: v}, id=f"{ids(op)}-{name}-{i}")
        for op in OPS
        for name in OBJECT_GRANTS
        for i, v in enumerate(GRANT_VALUES)
    ]


@pytest.mark.parametrize("op,headers", _refused_acl_cases() + _refused_grant_cases())
def test_object_write_with_an_acl_is_refused_and_writes_nothing(env, op, headers):
    client, storage, metadata = env
    before = _state(storage, metadata, client.upload_id)
    _assert_not_supported(op(client, headers))
    assert _state(storage, metadata, client.upload_id) == before
    _assert_clean(storage, metadata)


@pytest.mark.parametrize("op", OPS, ids=ids)
def test_object_write_with_a_refused_acl_beside_an_accepted_one_is_refused(env, op):
    client, storage, metadata = env
    before = _state(storage, metadata, client.upload_id)
    resp = op(client, {ACL: "bucket-owner-full-control", "x-amz-grant-read": 'id="owner-id"'})
    _assert_not_supported(resp)
    assert _state(storage, metadata, client.upload_id) == before


def test_put_object_over_an_existing_key_with_a_refused_acl_leaves_the_old_object(env):
    client, _, metadata = env
    meta = metadata.get_object_meta(BUCKET, KEY)
    _assert_not_supported(
        client.put(f"/{BUCKET}/{KEY}", content=BODY, headers={ACL: "public-read"})
    )
    assert metadata.get_object_meta(BUCKET, KEY) == meta
    assert client.get(f"/{BUCKET}/{KEY}").content == OLD


# --- object writes: where the check sits ---


@pytest.mark.parametrize("op", OPS, ids=ids)
@pytest.mark.parametrize("headers", [{ACL: "public-read"}, {"x-amz-grant-read": "x"}], ids=repr)
def test_a_refused_acl_on_a_missing_bucket_is_no_such_bucket(env, op, headers):
    client, _, _ = env
    resp = op(client, headers, bucket="missing-bucket")
    _assert_error(resp, 404, "NoSuchBucket")


@pytest.fixture
def owner(monkeypatch):
    monkeypatch.setattr(settings, "owner_id", "owner-id")


@pytest.mark.parametrize("op", OPS, ids=ids)
def test_a_refused_acl_with_a_wrong_expected_owner_is_403(env, owner, op):
    client, storage, metadata = env
    before = _state(storage, metadata, client.upload_id)
    headers = {ACL: "public-read", "x-amz-expected-bucket-owner": "someone-else"}
    _assert_error(op(client, headers), 403, "AccessDenied")
    assert _state(storage, metadata, client.upload_id) == before


@pytest.mark.parametrize("op", OPS, ids=ids)
def test_a_refused_acl_with_the_right_expected_owner_is_refused(env, owner, op):
    client, _, _ = env
    headers = {ACL: "public-read", "x-amz-expected-bucket-owner": "owner-id"}
    _assert_not_supported(op(client, headers))


@pytest.mark.parametrize("op", OPS, ids=ids)
def test_a_refused_acl_with_a_refused_feature_header_is_501(env, op):
    client, storage, metadata = env
    before = _state(storage, metadata, client.upload_id)
    resp = op(client, {ACL: "public-read", "x-amz-server-side-encryption": "AES256"})
    _assert_error(resp, 501, "NotImplemented")
    assert ET.fromstring(resp.text).findtext("Header") == "x-amz-server-side-encryption"
    assert _state(storage, metadata, client.upload_id) == before


def _unreadable_body():
    def body():
        raise AssertionError("the request body was read")
        yield b""

    return body()


def test_put_object_with_a_refused_acl_is_refused_before_the_body_is_read(env):
    client, storage, metadata = env
    resp = client.put(f"/{BUCKET}/{NEW}", content=_unreadable_body(), headers={ACL: "public-read"})
    _assert_not_supported(resp)
    _assert_clean(storage, metadata)
    assert metadata.get_object_meta(BUCKET, NEW) is None


def test_put_object_with_a_refused_grant_is_refused_before_the_body_is_read(env):
    client, _, _ = env
    headers = {"x-amz-grant-full-control": 'id="owner-id"'}
    resp = client.put(f"/{BUCKET}/{NEW}", content=_unreadable_body(), headers=headers)
    _assert_not_supported(resp)


def test_copy_object_with_a_refused_acl_and_a_missing_source_is_not_no_such_key(env):
    client, _, metadata = env
    resp = client.put(
        f"/{BUCKET}/{COPY}",
        headers={"x-amz-copy-source": f"/{BUCKET}/missing-key", ACL: "public-read"},
    )
    _assert_not_supported(resp)
    resp = client.put(f"/{BUCKET}/{COPY}", headers={"x-amz-copy-source": f"/{BUCKET}/missing-key"})
    _assert_error(resp, 404, "NoSuchKey")
    assert metadata.get_object_meta(BUCKET, COPY) is None


@pytest.mark.parametrize("source", ["/missing-bucket/missing-key", "no-slash", "/", "/b/%zz"])
def test_copy_object_with_a_refused_acl_does_not_parse_the_source(env, source):
    client, _, _ = env
    resp = client.put(
        f"/{BUCKET}/{COPY}", headers={"x-amz-copy-source": source, ACL: "public-read"}
    )
    _assert_not_supported(resp)


def test_copy_object_with_a_refused_acl_does_not_copy_onto_itself(env):
    client, _, metadata = env
    meta = metadata.get_object_meta(BUCKET, KEY)
    resp = client.put(
        f"/{BUCKET}/{KEY}",
        headers={"x-amz-copy-source": f"/{BUCKET}/{KEY}", ACL: "public-read"},
    )
    _assert_not_supported(resp)
    assert metadata.get_object_meta(BUCKET, KEY) == meta


def test_copy_object_with_a_refused_acl_does_not_evaluate_the_source_conditions(env):
    client, _, _ = env
    resp = client.put(
        f"/{BUCKET}/{COPY}",
        headers={
            "x-amz-copy-source": f"/{BUCKET}/{KEY}",
            "x-amz-copy-source-if-match": '"nope"',
            ACL: "public-read",
        },
    )
    _assert_not_supported(resp)


# --- other operations are unaffected ---


@pytest.mark.parametrize("value", ["public-read", "private"])
def test_upload_part_ignores_the_acl_header(env, value):
    client, _, metadata = env
    assert _upload_part(client, {ACL: value, "x-amz-grant-read": "x"}).status_code == 200
    assert [p.part_number for p in metadata.get_parts(client.upload_id)] == [1, 2]


def test_upload_part_copy_ignores_the_acl_header(env):
    client, _, _ = env
    resp = client.put(
        f"/{BUCKET}/mp?partNumber=2&uploadId={client.upload_id}",
        headers={"x-amz-copy-source": f"/{BUCKET}/{KEY}", ACL: "public-read"},
    )
    assert resp.status_code == 200


def test_complete_multipart_upload_ignores_the_acl_header(env):
    client, _, _ = env
    etag = client.put(
        f"/{BUCKET}/mp?partNumber=1&uploadId={client.upload_id}", content=PART
    ).headers["etag"]
    xml = (
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>"
        f"<ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    )
    resp = client.post(
        f"/{BUCKET}/mp?uploadId={client.upload_id}", content=xml, headers={ACL: "public-read"}
    )
    assert resp.status_code == 200


# --- CreateBucket ---


@pytest.mark.parametrize(
    "path", [f"/{NEW_BUCKET}", f"/{NEW_BUCKET}/", f"/{NEW_BUCKET}?x-id=CreateBucket"]
)
@pytest.mark.parametrize("headers", [{}, *({ACL: v} for v in [*BLANK, "private"])], ids=repr)
def test_create_bucket_with_an_accepted_or_absent_acl_creates_it(env, path, headers):
    client, _, metadata = env
    assert _create_bucket(client, headers, path).status_code == 200
    assert metadata.bucket_exists(NEW_BUCKET)


@pytest.mark.parametrize("name", BUCKET_GRANTS)
@pytest.mark.parametrize("value", BLANK)
def test_create_bucket_with_a_blank_grant_creates_it(env, name, value):
    client, _, metadata = env
    assert _create_bucket(client, {name: value}).status_code == 200
    assert metadata.bucket_exists(NEW_BUCKET)


@pytest.mark.parametrize("value", REFUSED_BUCKET_ACLS)
def test_create_bucket_with_a_refused_acl_creates_nothing(env, value):
    client, storage, metadata = env
    before = _state(storage, metadata, client.upload_id)
    _assert_invalid_bucket_acl(_create_bucket(client, {ACL: value}))
    assert not metadata.bucket_exists(NEW_BUCKET)
    assert _state(storage, metadata, client.upload_id) == before


@pytest.mark.parametrize("name", BUCKET_GRANTS)
@pytest.mark.parametrize("value", GRANT_VALUES)
def test_create_bucket_with_a_grant_creates_nothing(env, name, value):
    client, storage, metadata = env
    before = _state(storage, metadata, client.upload_id)
    _assert_invalid_bucket_acl(_create_bucket(client, {name: value}))
    assert not metadata.bucket_exists(NEW_BUCKET)
    assert _state(storage, metadata, client.upload_id) == before


def test_create_bucket_with_a_refused_acl_beside_private_is_refused(env):
    client, _, metadata = env
    _assert_invalid_bucket_acl(_create_bucket(client, {ACL: "private", "x-amz-grant-write": "x"}))
    assert not metadata.bucket_exists(NEW_BUCKET)


def test_create_bucket_with_object_ownership_bucket_owner_enforced_is_accepted(env):
    client, _, metadata = env
    assert _create_bucket(client, {OWNERSHIP: "BucketOwnerEnforced"}).status_code == 200
    assert metadata.bucket_exists(NEW_BUCKET)


@pytest.mark.parametrize("value", BLANK)
def test_create_bucket_with_a_blank_object_ownership_is_accepted(env, value):
    client, _, metadata = env
    assert _create_bucket(client, {OWNERSHIP: value}).status_code == 200
    assert metadata.bucket_exists(NEW_BUCKET)


@pytest.mark.parametrize(
    "value", ["BucketOwnerPreferred", "ObjectWriter", "junk", "bucketownerenforced"]
)
def test_create_bucket_with_another_object_ownership_is_501_and_creates_nothing(env, value):
    client, storage, metadata = env
    before = _state(storage, metadata, client.upload_id)
    resp = _create_bucket(client, {OWNERSHIP: value})
    _assert_error(resp, 501, "NotImplemented")
    assert ET.fromstring(resp.text).findtext("Header") == OWNERSHIP
    assert not metadata.bucket_exists(NEW_BUCKET)
    assert _state(storage, metadata, client.upload_id) == before


def test_create_bucket_with_object_writer_and_public_read_is_501(env):
    client, _, metadata = env
    resp = _create_bucket(client, {OWNERSHIP: "ObjectWriter", ACL: "public-read"})
    _assert_error(resp, 501, "NotImplemented")
    assert not metadata.bucket_exists(NEW_BUCKET)


def test_create_bucket_with_an_invalid_name_and_a_refused_acl_is_invalid_bucket_name(env):
    client, _, _ = env
    resp = client.put("/Bad_Name", headers={ACL: "public-read"})
    _assert_error(resp, 400, "InvalidBucketName")


def test_create_bucket_that_exists_with_a_refused_acl_is_the_acl_error(env):
    client, _, metadata = env
    _assert_invalid_bucket_acl(client.put(f"/{BUCKET}", headers={ACL: "public-read"}))
    assert client.put(f"/{BUCKET}").status_code == 409
    assert metadata.bucket_exists(BUCKET)


def test_create_bucket_ignores_the_expected_owner_before_the_acl_check(env, owner):
    client, _, _ = env
    headers = {ACL: "public-read", "x-amz-expected-bucket-owner": "someone-else"}
    _assert_invalid_bucket_acl(_create_bucket(client, headers))


# --- boto3 ---


@pytest.fixture(scope="module")
def s3(tmp_path_factory):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    tmp: Path = tmp_path_factory.mktemp("acl-headers")
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
    client.create_bucket(Bucket="boto3-acl")
    yield client
    server.should_exit = True
    thread.join(timeout=10)


def test_boto3_put_object_with_public_read_raises_access_control_list_not_supported(s3):
    with pytest.raises(ClientError) as info:
        s3.put_object(Bucket="boto3-acl", Key="refused", Body=b"x", ACL="public-read")
    assert info.value.response["Error"]["Code"] == "AccessControlListNotSupported"
    assert info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert "Contents" not in s3.list_objects_v2(Bucket="boto3-acl", Prefix="refused")


def test_boto3_put_object_with_bucket_owner_full_control_succeeds(s3):
    s3.put_object(Bucket="boto3-acl", Key="accepted", Body=b"x", ACL="bucket-owner-full-control")
    assert s3.get_object(Bucket="boto3-acl", Key="accepted")["Body"].read() == b"x"


def test_boto3_grant_read_raises_access_control_list_not_supported(s3):
    with pytest.raises(ClientError) as info:
        s3.put_object(Bucket="boto3-acl", Key="refused-grant", Body=b"x", GrantRead='id="owner"')
    assert info.value.response["Error"]["Code"] == "AccessControlListNotSupported"
    assert info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_boto3_create_bucket_with_public_read_raises_invalid_bucket_acl(s3):
    with pytest.raises(ClientError) as info:
        s3.create_bucket(Bucket="boto3-public", ACL="public-read")
    assert info.value.response["Error"]["Code"] == "InvalidBucketAclWithObjectOwnership"
    assert info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    names = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    assert "boto3-public" not in names


def test_boto3_create_bucket_with_object_ownership_enforced_succeeds(s3):
    s3.create_bucket(Bucket="boto3-enforced", ObjectOwnership="BucketOwnerEnforced")
    s3.create_bucket(Bucket="boto3-private", ACL="private")


def test_boto3_create_bucket_with_object_writer_raises_not_implemented(s3):
    with pytest.raises(ClientError) as info:
        s3.create_bucket(Bucket="boto3-writer", ObjectOwnership="ObjectWriter")
    assert info.value.response["Error"]["Code"] == "NotImplemented"
    assert info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 501
