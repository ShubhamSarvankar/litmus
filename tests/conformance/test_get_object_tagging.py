"""GetObjectTagging: GET /{bucket}/{key}?tagging answers with an empty TagSet.

Litmus stores no tags: PutObject and CreateMultipartUpload refuse a non-empty
x-amz-tagging with 501 and CopyObject refuses a tag set it would have to store, so
the true tag set of every object is empty. This is an answer from a store that cannot
hold tags, not tagging support: PUT ?tagging, DELETE ?tagging and GET /{bucket}?tagging
stay 501. The request takes ?tagging alone or with versionId, handled as GetObject
handles it; no Range, conditional header or response-* override applies.
"""

import socket
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import boto3
import pytest
import uvicorn
from botocore.config import Config
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "tag-bucket"
KEY = "obj.txt"
BODY = b"hello tagging"
NS = "http://s3.amazonaws.com/doc/2006-03-01/"
EMPTY = f'<?xml version="1.0" encoding="UTF-8"?>\n<Tagging xmlns="{NS}"><TagSet/></Tagging>'


@pytest.fixture
def client(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    c = TestClient(create_app(storage=storage, metadata=metadata), raise_server_exceptions=False)
    assert c.put(f"/{BUCKET}").status_code == 200
    return c


def _tagging(client, key=KEY, bucket=BUCKET, query="tagging", **kwargs):
    return client.get(f"/{bucket}/{key}?{query}", **kwargs)


def _assert_empty(resp) -> None:
    assert resp.status_code == 200
    assert resp.text == EMPTY
    root = ET.fromstring(resp.text)
    assert root.tag == f"{{{NS}}}Tagging"
    (tag_set,) = list(root)
    assert tag_set.tag == f"{{{NS}}}TagSet"
    assert list(tag_set) == []


def _assert_code(resp, status: int, code: str) -> ET.Element:
    assert resp.status_code == status
    root = ET.fromstring(resp.text)
    assert root.findtext("Code") == code
    return root


# --- an empty TagSet for every way an object is made ---


def test_put_object(client):
    client.put(f"/{BUCKET}/{KEY}", content=BODY)
    _assert_empty(_tagging(client))


def test_complete_multipart_upload(client):
    upload = ET.fromstring(client.post(f"/{BUCKET}/mp?uploads").text).findtext("UploadId")
    part = client.put(f"/{BUCKET}/mp?partNumber=1&uploadId={upload}", content=BODY)
    assert part.status_code == 200
    body = (
        f"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>"
        f"<ETag>{part.headers['etag']}</ETag></Part></CompleteMultipartUpload>"
    )
    assert client.post(f"/{BUCKET}/mp?uploadId={upload}", content=body).status_code == 200
    _assert_empty(_tagging(client, key="mp"))


def test_copy_object(client):
    client.put(f"/{BUCKET}/{KEY}", content=BODY)
    resp = client.put(f"/{BUCKET}/copy", headers={"x-amz-copy-source": f"{BUCKET}/{KEY}"})
    assert resp.status_code == 200
    _assert_empty(_tagging(client, key="copy"))


def test_copy_object_with_tagging_directive_copy(client):
    client.put(f"/{BUCKET}/{KEY}", content=BODY)
    resp = client.put(
        f"/{BUCKET}/copy",
        headers={
            "x-amz-copy-source": f"{BUCKET}/{KEY}",
            "x-amz-tagging-directive": "COPY",
            "x-amz-tagging": "a=b",
        },
    )
    assert resp.status_code == 200
    _assert_empty(_tagging(client, key="copy"))


def test_upload_part_copy(client):
    client.put(f"/{BUCKET}/{KEY}", content=BODY)
    upload = ET.fromstring(client.post(f"/{BUCKET}/mpc?uploads").text).findtext("UploadId")
    part = client.put(
        f"/{BUCKET}/mpc?partNumber=1&uploadId={upload}",
        headers={"x-amz-copy-source": f"{BUCKET}/{KEY}"},
    )
    assert part.status_code == 200
    etag = ET.fromstring(part.text).findtext("ETag")
    body = (
        f"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>"
        f"<ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    )
    assert client.post(f"/{BUCKET}/mpc?uploadId={upload}", content=body).status_code == 200
    _assert_empty(_tagging(client, key="mpc"))


def test_key_with_slashes_and_special_characters(client):
    key = "dir/sub dir/a+b.txt"
    client.put(f"/{BUCKET}/{key}", content=BODY)
    _assert_empty(client.get(f"/{BUCKET}/dir/sub%20dir/a%2Bb.txt?tagging"))


# --- the raw response ---


def test_raw_response(client):
    client.put(f"/{BUCKET}/{KEY}", content=BODY)
    resp = _tagging(client)
    assert resp.status_code == 200
    assert resp.content == EMPTY.encode()
    assert resp.headers["content-type"] == "application/xml"
    assert resp.headers["x-amz-request-id"]
    assert resp.headers["x-amz-id-2"]
    # Not GetObject: no object headers, and no version ID, as GetObject sends none.
    for header in ("etag", "last-modified", "accept-ranges", "x-amz-version-id"):
        assert header not in resp.headers


def test_boto3_get_object_tagging(tmp_path_factory):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    tmp = tmp_path_factory.mktemp("tagging-boto3")
    app = create_app(
        storage=BlobStore(tmp / "blobs"), metadata=SQLiteMetadataStore(db_path=tmp / "meta.db")
    )
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, ws="none", log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 10
        while not server.started:
            if time.monotonic() > deadline:
                raise RuntimeError("uvicorn did not start")
            time.sleep(0.02)
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://127.0.0.1:{port}",
            aws_access_key_id="key",
            aws_secret_access_key="secret",
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 0}),
        )
        s3.create_bucket(Bucket=BUCKET)
        s3.put_object(Bucket=BUCKET, Key=KEY, Body=BODY)
        resp = s3.get_object_tagging(Bucket=BUCKET, Key=KEY)
        assert resp["TagSet"] == []
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # The same call with a version ID of "null", as an SDK sends for an unversioned object.
        assert s3.get_object_tagging(Bucket=BUCKET, Key=KEY, VersionId="null")["TagSet"] == []
    finally:
        server.should_exit = True
        thread.join(timeout=10)


# --- errors ---


def test_no_such_bucket(client):
    root = _assert_code(_tagging(client, bucket="no-such-bucket"), 404, "NoSuchBucket")
    assert root.findtext("BucketName") == "no-such-bucket"


def test_no_such_key(client):
    root = _assert_code(_tagging(client, key="missing"), 404, "NoSuchKey")
    assert root.findtext("Key") == "missing"


def test_bucket_is_checked_before_key(client):
    _assert_code(_tagging(client, bucket="no-such-bucket", key="missing"), 404, "NoSuchBucket")


def test_deleted_key_is_no_such_key(client):
    client.put(f"/{BUCKET}/{KEY}", content=BODY)
    assert client.delete(f"/{BUCKET}/{KEY}").status_code == 204
    _assert_code(_tagging(client), 404, "NoSuchKey")


def test_put_with_tags_is_501_and_the_key_does_not_exist(client):
    resp = client.put(f"/{BUCKET}/{KEY}", content=BODY, headers={"x-amz-tagging": "a=b"})
    _assert_code(resp, 501, "NotImplemented")
    _assert_code(_tagging(client), 404, "NoSuchKey")


# --- versionId, as GetObject handles it ---


def test_version_id_null_names_the_current_object(client):
    client.put(f"/{BUCKET}/{KEY}", content=BODY)
    _assert_empty(_tagging(client, query="tagging&versionId=null"))
    _assert_empty(_tagging(client, query="versionId=null&tagging"))


def test_version_id_null_on_a_missing_key(client):
    _assert_code(_tagging(client, key="missing", query="tagging&versionId=null"), 404, "NoSuchKey")


@pytest.mark.parametrize("version_id", ["abc", "", "NULL", "3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY"])
def test_other_version_id_is_501_as_for_get_object(client, version_id):
    client.put(f"/{BUCKET}/{KEY}", content=BODY)
    get = client.get(f"/{BUCKET}/{KEY}", params={"versionId": version_id})
    resp = client.get(f"/{BUCKET}/{KEY}", params={"tagging": "", "versionId": version_id})
    assert get.status_code == resp.status_code == 501
    assert ET.fromstring(resp.text).findtext("Parameter") == "versionId"


def test_version_id_null_and_other_is_501(client):
    client.put(f"/{BUCKET}/{KEY}", content=BODY)
    _assert_code(
        _tagging(client, query="tagging&versionId=null&versionId=abc"), 501, "NotImplemented"
    )


def test_x_id_is_ignored(client):
    client.put(f"/{BUCKET}/{KEY}", content=BODY)
    _assert_empty(_tagging(client, query="tagging&x-id=GetObjectTagging"))
    _assert_empty(_tagging(client, query="tagging&versionId=null&x-id=GetObjectTagging"))


# --- nothing of GetObject applies ---


def test_headers_that_shape_get_object_do_not_apply(client):
    client.put(f"/{BUCKET}/{KEY}", content=BODY)
    etag = client.head(f"/{BUCKET}/{KEY}").headers["etag"]
    for headers in (
        {"Range": "bytes=0-3"},
        {"If-None-Match": etag},
        {"If-Match": '"nope"'},
        {"If-Modified-Since": "Wed, 01 Jan 2100 00:00:00 GMT"},
        {"If-Unmodified-Since": "Wed, 01 Jan 2000 00:00:00 GMT"},
        {"x-amz-expected-bucket-owner": "someone-else", "x-amz-request-payer": "requester"},
    ):
        _assert_empty(_tagging(client, headers=headers))


@pytest.mark.parametrize(
    "extra",
    [
        "partNumber=1",
        "response-content-type=text/plain",
        "response-cache-control=no-cache",
        "uploadId=abc",
        "uploads",
        "acl",
        "attributes",
        "foo=bar",
        "versionId=null&partNumber=1",
        "versionId=null&response-expires=x",
    ],
)
def test_extra_query_parameter_is_501(client, extra):
    client.put(f"/{BUCKET}/{KEY}", content=BODY)
    _assert_code(_tagging(client, query=f"tagging&{extra}"), 501, "NotImplemented")


# --- everything else about tagging stays 501 ---


@pytest.mark.parametrize("method", ["PUT", "DELETE"])
def test_put_and_delete_object_tagging_are_501(client, method):
    client.put(f"/{BUCKET}/{KEY}", content=BODY)
    body = f'<Tagging xmlns="{NS}"><TagSet/></Tagging>' if method == "PUT" else None
    resp = client.request(method, f"/{BUCKET}/{KEY}?tagging", content=body)
    root = _assert_code(resp, 501, "NotImplemented")
    assert root.findtext("Subresource") == "tagging"
    # Nothing was changed: the object is intact.
    assert client.get(f"/{BUCKET}/{KEY}").content == BODY


@pytest.mark.parametrize("method", ["GET", "PUT", "DELETE"])
def test_bucket_tagging_is_501(client, method):
    resp = client.request(method, f"/{BUCKET}?tagging")
    _assert_code(resp, 501, "NotImplemented")
    assert client.head(f"/{BUCKET}").status_code == 200


def test_head_object_tagging_is_501(client):
    client.put(f"/{BUCKET}/{KEY}", content=BODY)
    assert client.head(f"/{BUCKET}/{KEY}?tagging").status_code == 501


def test_plain_get_object_is_unchanged(client):
    client.put(f"/{BUCKET}/{KEY}", content=BODY)
    assert client.get(f"/{BUCKET}/{KEY}").content == BODY
