"""CompleteMultipartUpload request headers: x-amz-mp-object-size is verified, and the
checksum headers are decided against an upload that named no algorithm (decisions 26 and
32 of docs/design/storage-layout.md).

x-amz-mp-object-size must be a plain non-negative decimal integer (a malformed value is
400 InvalidArgument, a choice: the S3 API reference documents only the mismatch) and
equal the total of the stored sizes of exactly the listed parts (400 InvalidRequest, S3
API reference, "CompleteMultipartUpload"). Both are decided before a blob is registered,
so a refusal leaves the upload open with all its parts.

The uploads here name no algorithm, so x-amz-checksum-crc32, -crc32c, -sha1, -sha256 and
x-amz-checksum-type are accepted and not validated, x-amz-checksum-crc64nvme is validated
against the combined CRC64NVME Litmus stores (400 BadDigest on a mismatch), and
x-amz-checksum-sha512, -md5 and the three XXHash headers are 400 InvalidRequest (S3 User
Guide, "Checking object integrity for data uploads"). A wrong ChecksumCRC64NVME element of a
Part is 400 BadDigest. The uploads that name an algorithm are in test_complete_checksums.py.
"""

import hashlib
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
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "complete-headers"
KEY = "obj.bin"
SIZE_HEADER = "x-amz-mp-object-size"
# Parts of different sizes.
PARTS = [b"a" * 10, b"b" * 3000, b"c" * 1]
TOTAL = sum(len(p) for p in PARTS)


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    assert client.put(f"/{BUCKET}").status_code == 200
    return client, storage, metadata


def _code(resp) -> str:
    return ET.fromstring(resp.text).findtext("Code")


def _upload(client, key: str = KEY, parts: list[bytes] = PARTS) -> tuple[str, list[str]]:
    """An open upload with the given parts; its id and the parts' ETags."""
    uid = ET.fromstring(client.post(f"/{BUCKET}/{key}?uploads").text).findtext("UploadId")
    etags = []
    for number, data in enumerate(parts, start=1):
        r = client.put(f"/{BUCKET}/{key}?partNumber={number}&uploadId={uid}", content=data)
        assert r.status_code == 200
        etags.append(r.headers["etag"])
    return uid, etags


def _body(etags: list[str]) -> bytes:
    items = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>"
        for n, e in enumerate(etags, start=1)
    )
    return f"<CompleteMultipartUpload>{items}</CompleteMultipartUpload>".encode()


def _complete(client, uid: str, etags: list[str], headers: dict | None = None, key: str = KEY):
    return client.post(
        f"/{BUCKET}/{key}?uploadId={uid}", content=_body(etags), headers=headers or {}
    )


def _state(storage, metadata, uid: str) -> tuple:
    """Everything the store holds that a refused Complete must leave alone."""
    return (
        metadata.list_objects(BUCKET),
        metadata.list_blob_records(),
        storage.temp_entries(),
        sorted(storage.scan()),
        metadata.get_upload(uid),
        metadata.get_parts(uid),
        metadata.list_incomplete_uploads(),
    )


def assert_store_clean(storage, metadata, live: int) -> None:
    """Only live blobs, no pending or garbage one, an empty tmp/ and a quiet sweep."""
    records = metadata.list_blob_records()
    assert [r.state for r in records] == ["live"] * live
    assert storage.temp_entries() == []
    assert run_consistency_sweep(metadata, storage).issues == []


def assert_upload_open(metadata, uid: str, etags: list[str]) -> None:
    assert metadata.upload_exists(uid)
    assert [u.upload_id for u in metadata.list_incomplete_uploads()] == [uid]
    parts = metadata.get_parts(uid)
    assert [(p.part_number, p.etag) for p in parts] == list(enumerate(etags, start=1))
    assert all(p.blob_id is not None for p in parts)


def assert_refused_and_open(env, uid, etags, before, resp, status, code) -> None:
    """The refusal, and that it left the upload open with all its parts and the store as
    it was: no blob was registered for the object."""
    client, storage, metadata = env
    assert (resp.status_code, _code(resp)) == (status, code)
    assert _state(storage, metadata, uid) == before
    assert_upload_open(metadata, uid, etags)
    assert_store_clean(storage, metadata, live=len(etags))
    assert client.get(f"/{BUCKET}/{KEY}").status_code == 404


# --- a right size completes ---


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(str(TOTAL), id="plain"),
        pytest.param(f" {TOTAL} ", id="spaces-around"),
        pytest.param(f"\t{TOTAL}\t", id="tabs-around"),
        pytest.param(f"{TOTAL:08d}", id="leading-zeros"),
    ],
)
def test_a_matching_size_over_parts_of_different_sizes_completes(env, value):
    client, storage, metadata = env
    uid, etags = _upload(client)
    resp = _complete(client, uid, etags, {SIZE_HEADER: value})
    assert resp.status_code == 200
    got = client.get(f"/{BUCKET}/{KEY}")
    assert got.content == b"".join(PARTS)
    assert len(got.content) == TOTAL
    assert not metadata.list_incomplete_uploads()


@pytest.mark.parametrize("value", [None, "", "   ", "\t"])
def test_a_blank_or_absent_size_completes_without_a_check(env, value):
    client, storage, metadata = env
    uid, etags = _upload(client)
    headers = {} if value is None else {SIZE_HEADER: value}
    assert _complete(client, uid, etags, headers).status_code == 200
    assert client.get(f"/{BUCKET}/{KEY}").content == b"".join(PARTS)


def test_a_zero_size_is_a_value_and_does_not_match_nonempty_parts(env):
    client, storage, metadata = env
    uid, etags = _upload(client)
    before = _state(storage, metadata, uid)
    resp = _complete(client, uid, etags, {SIZE_HEADER: "0"})
    assert_refused_and_open(env, uid, etags, before, resp, 400, "InvalidRequest")


# --- a malformed size ---


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("-1", id="minus-one"),
        pytest.param("1.5", id="decimal"),
        pytest.param("0x10", id="hex"),
        pytest.param("+5", id="plus-sign"),
        pytest.param("+3011", id="plus-sign-and-right-digits"),
        pytest.param("abc", id="letters"),
        pytest.param("12abc", id="digits-then-letters"),
        pytest.param("1 2", id="embedded-space"),
        pytest.param(" 3 011 ", id="embedded-space-and-outer-spaces"),
        pytest.param("1e3", id="exponent"),
        pytest.param("1_000", id="underscore"),
        pytest.param(b"\xb2", id="superscript-two"),
        pytest.param("3011,3011", id="list"),
    ],
)
def test_a_malformed_size_is_invalid_argument_and_leaves_the_upload_open(env, value):
    client, storage, metadata = env
    uid, etags = _upload(client)
    before = _state(storage, metadata, uid)
    resp = _complete(client, uid, etags, {SIZE_HEADER: value})
    assert_refused_and_open(env, uid, etags, before, resp, 400, "InvalidArgument")
    assert ET.fromstring(resp.text).findtext("ArgumentName") == SIZE_HEADER
    # The same upload completes once the header is right.
    assert _complete(client, uid, etags, {SIZE_HEADER: str(TOTAL)}).status_code == 200


# --- a size that does not match ---


@pytest.mark.parametrize("delta", [1, -1])
def test_a_size_one_byte_off_is_invalid_request_and_leaves_the_upload_open(env, delta):
    client, storage, metadata = env
    uid, etags = _upload(client)
    before = _state(storage, metadata, uid)
    resp = _complete(client, uid, etags, {SIZE_HEADER: str(TOTAL + delta)})
    assert_refused_and_open(env, uid, etags, before, resp, 400, "InvalidRequest")
    # A following Complete with the right size succeeds, with all the parts.
    ok = _complete(client, uid, etags, {SIZE_HEADER: str(TOTAL)})
    assert ok.status_code == 200
    assert client.get(f"/{BUCKET}/{KEY}").content == b"".join(PARTS)
    assert not metadata.list_incomplete_uploads()


def test_a_size_with_more_digits_than_any_object_can_have_is_a_mismatch(env):
    client, storage, metadata = env
    uid, etags = _upload(client)
    before = _state(storage, metadata, uid)
    resp = _complete(client, uid, etags, {SIZE_HEADER: "9" * 5000})
    assert_refused_and_open(env, uid, etags, before, resp, 400, "InvalidRequest")


def test_the_size_counts_only_the_parts_listed(env):
    client, storage, metadata = env
    uid, etags = _upload(client)  # three parts uploaded, two listed
    listed = [etags[0], etags[2]]
    two = len(PARTS[0]) + len(PARTS[2])
    # The part numbers must be the listed ones, so build the body by hand.
    body = (
        "<CompleteMultipartUpload>"
        f"<Part><PartNumber>1</PartNumber><ETag>{listed[0]}</ETag></Part>"
        f"<Part><PartNumber>3</PartNumber><ETag>{listed[1]}</ETag></Part>"
        "</CompleteMultipartUpload>"
    ).encode()
    url = f"/{BUCKET}/{KEY}?uploadId={uid}"
    # The total of all three uploaded parts is not the size of the two listed.
    resp = client.post(url, content=body, headers={SIZE_HEADER: str(TOTAL)})
    assert (resp.status_code, _code(resp)) == (400, "InvalidRequest")
    assert metadata.upload_exists(uid)
    resp = client.post(url, content=body, headers={SIZE_HEADER: str(two)})
    assert resp.status_code == 200
    assert client.get(f"/{BUCKET}/{KEY}").content == PARTS[0] + PARTS[2]


# --- the order of the errors ---


def test_a_malformed_size_on_a_missing_upload_is_no_such_upload(env):
    client, _, _ = env
    resp = client.post(
        f"/{BUCKET}/{KEY}?uploadId=missing", content=b"<x/>", headers={SIZE_HEADER: "abc"}
    )
    assert (resp.status_code, _code(resp)) == (404, "NoSuchUpload")


def test_a_malformed_size_comes_before_a_malformed_body(env):
    client, storage, metadata = env
    uid, etags = _upload(client)
    resp = client.post(
        f"/{BUCKET}/{KEY}?uploadId={uid}", content=b"not xml", headers={SIZE_HEADER: "abc"}
    )
    assert (resp.status_code, _code(resp)) == (400, "InvalidArgument")
    assert_upload_open(metadata, uid, etags)


def test_a_wrong_size_does_not_hide_a_malformed_body(env):
    client, storage, metadata = env
    uid, etags = _upload(client)
    resp = client.post(
        f"/{BUCKET}/{KEY}?uploadId={uid}", content=b"not xml", headers={SIZE_HEADER: "1"}
    )
    assert (resp.status_code, _code(resp)) == (400, "MalformedXML")


def test_a_wrong_size_does_not_hide_a_part_out_of_order(env):
    client, storage, metadata = env
    uid, etags = _upload(client)
    body = (
        "<CompleteMultipartUpload>"
        f"<Part><PartNumber>2</PartNumber><ETag>{etags[1]}</ETag></Part>"
        f"<Part><PartNumber>1</PartNumber><ETag>{etags[0]}</ETag></Part>"
        "</CompleteMultipartUpload>"
    ).encode()
    resp = client.post(f"/{BUCKET}/{KEY}?uploadId={uid}", content=body, headers={SIZE_HEADER: "1"})
    assert (resp.status_code, _code(resp)) == (400, "InvalidPartOrder")


def test_a_wrong_size_does_not_hide_an_invalid_part(env):
    client, storage, metadata = env
    uid, etags = _upload(client)
    wrong = ['"' + "0" * 32 + '"', *etags[1:]]
    resp = _complete(client, uid, wrong, {SIZE_HEADER: str(TOTAL + 1)})
    assert (resp.status_code, _code(resp)) == (400, "InvalidPart")
    assert_upload_open(metadata, uid, etags)


def test_a_wrong_size_comes_before_a_failing_condition(env):
    client, storage, metadata = env
    assert client.put(f"/{BUCKET}/{KEY}", content=b"old").status_code == 200
    uid, etags = _upload(client)
    resp = _complete(client, uid, etags, {SIZE_HEADER: str(TOTAL + 1), "If-None-Match": "*"})
    assert (resp.status_code, _code(resp)) == (400, "InvalidRequest")
    assert_upload_open(metadata, uid, etags)
    assert client.get(f"/{BUCKET}/{KEY}").content == b"old"


def test_a_right_size_is_followed_by_the_conditions(env):
    client, storage, metadata = env
    assert client.put(f"/{BUCKET}/{KEY}", content=b"old").status_code == 200
    uid, etags = _upload(client)
    resp = _complete(client, uid, etags, {SIZE_HEADER: str(TOTAL), "If-None-Match": "*"})
    assert (resp.status_code, _code(resp)) == (412, "PreconditionFailed")
    assert_upload_open(metadata, uid, etags)
    assert client.get(f"/{BUCKET}/{KEY}").content == b"old"


# --- checksum headers ---

CHECKSUM_HEADERS = [
    "x-amz-checksum-crc32",
    "x-amz-checksum-crc32c",
    "x-amz-checksum-crc64nvme",
    "x-amz-checksum-sha1",
    "x-amz-checksum-sha256",
    "x-amz-checksum-sha512",
    "x-amz-checksum-md5",
    "x-amz-checksum-xxhash64",
    "x-amz-checksum-xxhash3",
    "x-amz-checksum-xxhash128",
    "x-amz-checksum-type",
]


def _value(header: str) -> str:
    return "COMPOSITE" if header.endswith("type") else "AAAAAAAAAAA="


UNNAMED_ACCEPTED = [
    "x-amz-checksum-crc32",
    "x-amz-checksum-crc32c",
    "x-amz-checksum-sha1",
    "x-amz-checksum-sha256",
    "x-amz-checksum-type",
]
UNNAMED_INVALID_REQUEST = [
    "x-amz-checksum-sha512",
    "x-amz-checksum-md5",
    "x-amz-checksum-xxhash64",
    "x-amz-checksum-xxhash3",
    "x-amz-checksum-xxhash128",
]


@pytest.mark.parametrize("header", UNNAMED_ACCEPTED)
def test_an_unvalidated_checksum_header_on_an_unnamed_upload_completes(env, header):
    client, storage, metadata = env
    uid, etags = _upload(client)
    resp = _complete(client, uid, etags, {header: _value(header)})
    assert resp.status_code == 200
    assert client.get(f"/{BUCKET}/{KEY}").content == b"".join(PARTS)
    assert not metadata.list_incomplete_uploads()


def test_a_wrong_crc64nvme_header_on_an_unnamed_upload_is_bad_digest_and_leaves_it_open(env):
    client, storage, metadata = env
    uid, etags = _upload(client)
    before = _state(storage, metadata, uid)
    resp = _complete(client, uid, etags, {"x-amz-checksum-crc64nvme": "AAAAAAAAAAA="})
    assert_refused_and_open(env, uid, etags, before, resp, 400, "BadDigest")
    assert _complete(client, uid, etags).status_code == 200


@pytest.mark.parametrize("header", UNNAMED_INVALID_REQUEST)
def test_an_md5_sha512_or_xxhash_header_on_an_unnamed_upload_is_invalid_request(env, header):
    client, storage, metadata = env
    uid, etags = _upload(client)
    before = _state(storage, metadata, uid)
    resp = _complete(client, uid, etags, {header: _value(header)})
    assert_refused_and_open(env, uid, etags, before, resp, 400, "InvalidRequest")
    assert _complete(client, uid, etags).status_code == 200


@pytest.mark.parametrize("header", CHECKSUM_HEADERS)
def test_a_checksum_header_on_a_missing_upload_is_no_such_upload(env, header):
    client, _, _ = env
    resp = client.post(
        f"/{BUCKET}/{KEY}?uploadId=missing", content=b"<x/>", headers={header: _value(header)}
    )
    assert (resp.status_code, _code(resp)) == (404, "NoSuchUpload")


@pytest.mark.parametrize("header", CHECKSUM_HEADERS)
@pytest.mark.parametrize("blank", ["", "  "])
def test_a_blank_checksum_header_counts_as_absent(env, header, blank):
    client, storage, metadata = env
    uid, etags = _upload(client)
    assert _complete(client, uid, etags, {header: blank}).status_code == 200


def test_the_checksum_header_is_accepted_whatever_the_case_of_its_name(env):
    client, storage, metadata = env
    uid, etags = _upload(client)
    resp = _complete(client, uid, etags, {"X-Amz-Checksum-SHA256": "AAAA"})
    assert resp.status_code == 200


# --- the per part checksum elements of the body ---


def test_a_wrong_crc64nvme_part_element_on_an_unnamed_upload_is_bad_digest(env):
    client, storage, metadata = env
    uid, etags = _upload(client)
    # The shape the AWS CLI sends: an XML declaration, unquoted ETags, one checksum
    # element per part. The values are not any part's checksum: the CRC64NVME one is
    # compared with the stored part checksum and refused.
    items = "".join(
        f"<Part><ETag>{e.strip(chr(34))}</ETag><PartNumber>{n}</PartNumber>"
        f"<ChecksumCRC64NVME>bV69mNWvFC0=</ChecksumCRC64NVME>"
        f"<ChecksumCRC32>NeBCjA==</ChecksumCRC32></Part>"
        for n, e in enumerate(etags, start=1)
    )
    body = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"{items}</CompleteMultipartUpload>"
    ).encode()
    resp = client.post(
        f"/{BUCKET}/{KEY}?uploadId={uid}", content=body, headers={SIZE_HEADER: str(TOTAL)}
    )
    assert (resp.status_code, _code(resp)) == (400, "BadDigest")
    assert_upload_open(metadata, uid, etags)
    assert client.get(f"/{BUCKET}/{KEY}").status_code == 404


# --- boto3 ---


@pytest.fixture(scope="module")
def s3(tmp_path_factory):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    tmp: Path = tmp_path_factory.mktemp("complete-headers")
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
    client.create_bucket(Bucket=BUCKET)
    yield client
    server.should_exit = True
    thread.join(timeout=10)


def _boto3_upload(s3, key: str) -> tuple[str, list[dict]]:
    uid = s3.create_multipart_upload(Bucket=BUCKET, Key=key)["UploadId"]
    parts = []
    for number, data in enumerate(PARTS, start=1):
        etag = s3.upload_part(Bucket=BUCKET, Key=key, UploadId=uid, PartNumber=number, Body=data)[
            "ETag"
        ]
        parts.append({"ETag": etag, "PartNumber": number})
    return uid, parts


def test_boto3_complete_with_the_right_mpu_object_size_succeeds(s3):
    uid, parts = _boto3_upload(s3, "boto3-right")
    s3.complete_multipart_upload(
        Bucket=BUCKET,
        Key="boto3-right",
        UploadId=uid,
        MultipartUpload={"Parts": parts},
        MpuObjectSize=TOTAL,
    )
    body = s3.get_object(Bucket=BUCKET, Key="boto3-right")["Body"].read()
    assert hashlib.sha256(body).digest() == hashlib.sha256(b"".join(PARTS)).digest()


def test_boto3_complete_with_the_wrong_mpu_object_size_is_invalid_request(s3):
    uid, parts = _boto3_upload(s3, "boto3-wrong")
    with pytest.raises(ClientError) as raised:
        s3.complete_multipart_upload(
            Bucket=BUCKET,
            Key="boto3-wrong",
            UploadId=uid,
            MultipartUpload={"Parts": parts},
            MpuObjectSize=TOTAL + 1,
        )
    error = raised.value.response
    assert error["Error"]["Code"] == "InvalidRequest"
    assert error["ResponseMetadata"]["HTTPStatusCode"] == 400
    # The upload is still open: the right size completes it.
    s3.complete_multipart_upload(
        Bucket=BUCKET,
        Key="boto3-wrong",
        UploadId=uid,
        MultipartUpload={"Parts": parts},
        MpuObjectSize=TOTAL,
    )
    assert s3.head_object(Bucket=BUCKET, Key="boto3-wrong")["ContentLength"] == TOTAL
