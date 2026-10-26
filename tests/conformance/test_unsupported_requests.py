"""Requests for S3 operations Litmus does not implement get 501 NotImplemented and
change nothing (server/api/request_guard.py)."""

import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.api.request_guard import BUCKET_SUBRESOURCES, OBJECT_SUBRESOURCES
from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "test-bucket"
KEY = "obj.txt"
BODY = b"precious content"
METHODS = ["GET", "HEAD", "PUT", "POST", "DELETE"]

# Requests that are implemented, so they are not expected to return 501.
IMPLEMENTED_BUCKET = {("GET", "versions"), ("POST", "delete")}
IMPLEMENTED_OBJECT = {
    ("POST", "uploadId"),
    ("DELETE", "uploadId"),
    ("POST", "uploads"),
    ("GET", "tagging"),
}


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    return TestClient(create_app(storage=storage, metadata=metadata), raise_server_exceptions=False)


@pytest.fixture
def populated(client) -> TestClient:
    assert client.put(f"/{BUCKET}").status_code == 200
    assert client.put(f"/{BUCKET}/{KEY}", content=BODY).status_code == 200
    return client


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def _assert_not_implemented(resp, method: str) -> None:
    assert resp.status_code == 501
    if method != "HEAD":
        assert resp.headers["content-type"].startswith("application/xml")
        root = ET.fromstring(resp.text)
        assert root.findtext("Code") == "NotImplemented"
        assert root.findtext("Message")
        assert root.findtext("RequestId")
        assert BODY not in resp.content


def _assert_unchanged(client: TestClient) -> None:
    assert client.head(f"/{BUCKET}").status_code == 200
    resp = client.get(f"/{BUCKET}/{KEY}")
    assert resp.status_code == 200
    assert resp.content == BODY
    assert resp.headers["etag"] == _md5(BODY)
    listing = ET.fromstring(client.get(f"/{BUCKET}").text)
    keys = [el.text for el in listing.iter() if el.tag.endswith("Key")]
    assert keys == [KEY]


# --- the two cases that used to destroy data ---


def test_put_object_acl_returns_501_and_leaves_object_intact(populated):
    resp = populated.put(
        f"/{BUCKET}/{KEY}?acl", headers={"x-amz-acl": "private"}, content=b"<AccessControlPolicy/>"
    )
    _assert_not_implemented(resp, "PUT")
    _assert_unchanged(populated)


def test_delete_bucket_cors_returns_501_and_leaves_bucket_intact(client):
    assert client.put(f"/{BUCKET}").status_code == 200
    _assert_not_implemented(client.delete(f"/{BUCKET}?cors"), "DELETE")
    assert client.head(f"/{BUCKET}").status_code == 200
    assert f"<Name>{BUCKET}</Name>" in client.get("/").text


def test_get_object_acl_does_not_return_object_body(populated):
    _assert_not_implemented(populated.get(f"/{BUCKET}/{KEY}?acl"), "GET")


# --- every listed subresource, every method ---


def _unimplemented(subresources, implemented):
    return [
        (method, name)
        for name in sorted(subresources)
        for method in METHODS
        if (method, name) not in implemented
    ]


@pytest.mark.parametrize(
    ("method", "subresource"), _unimplemented(BUCKET_SUBRESOURCES, IMPLEMENTED_BUCKET)
)
def test_bucket_subresource_not_implemented(populated, method, subresource):
    resp = populated.request(method, f"/{BUCKET}?{subresource}", content=b"<x/>")
    _assert_not_implemented(resp, method)
    _assert_unchanged(populated)


@pytest.mark.parametrize(
    ("method", "subresource"), _unimplemented(OBJECT_SUBRESOURCES, IMPLEMENTED_OBJECT)
)
def test_object_subresource_not_implemented(populated, method, subresource):
    resp = populated.request(method, f"/{BUCKET}/{KEY}?{subresource}", content=b"<x/>")
    _assert_not_implemented(resp, method)
    _assert_unchanged(populated)


# --- unlisted operations on mutating methods ---


def test_bucket_post_without_delete_returns_501(populated):
    _assert_not_implemented(populated.post(f"/{BUCKET}", content=b"form"), "POST")
    _assert_unchanged(populated)


@pytest.mark.parametrize("method", ["PUT", "DELETE", "POST"])
def test_unknown_bucket_query_parameter_on_mutating_method_returns_501(populated, method):
    _assert_not_implemented(populated.request(method, f"/{BUCKET}?someFutureConfig"), method)
    _assert_unchanged(populated)


@pytest.mark.parametrize("method", ["PUT", "DELETE", "POST"])
def test_unknown_object_query_parameter_on_mutating_method_returns_501(populated, method):
    resp = populated.request(method, f"/{BUCKET}/{KEY}?someFutureOp", content=b"x")
    _assert_not_implemented(resp, method)
    _assert_unchanged(populated)


def test_put_part_number_without_upload_id_returns_501(populated):
    resp = populated.put(f"/{BUCKET}/{KEY}?partNumber=1", content=b"overwrite")
    _assert_not_implemented(resp, "PUT")
    _assert_unchanged(populated)


def _create_upload(client: TestClient) -> str:
    resp = client.post(f"/{BUCKET}/dest?uploads")
    assert resp.status_code == 200
    return ET.fromstring(resp.text).findtext("UploadId")


@pytest.mark.parametrize(
    "source",
    [
        "arn:aws:s3:us-west-2:123456789012:accesspoint/ap/object/obj.txt",
        "arn%3Aaws%3As3%3Aus-west-2%3A123456789012%3Aaccesspoint/ap/object/obj.txt",
        "/arn:aws:s3-outposts:us-west-2:123456789012:outpost/op/object/obj.txt",
    ],
)
def test_upload_part_copy_from_an_arn_returns_501_and_records_no_part(populated, source):
    upload_id = _create_upload(populated)
    resp = populated.put(
        f"/{BUCKET}/dest?partNumber=1&uploadId={upload_id}",
        headers={"x-amz-copy-source": source},
    )
    _assert_not_implemented(resp, "PUT")
    assert populated.app.state.metadata.get_parts(upload_id) == []
    assert populated.get(f"/{BUCKET}/dest").status_code == 404
    _assert_unchanged(populated)


@pytest.mark.parametrize("version_id", ["3HL4kqtJlcpXroDTDmJ", "", "null2", "NULL"])
def test_upload_part_copy_with_a_non_null_version_id_returns_501_and_records_no_part(
    populated, version_id
):
    upload_id = _create_upload(populated)
    resp = populated.put(
        f"/{BUCKET}/dest?partNumber=1&uploadId={upload_id}",
        headers={
            "x-amz-copy-source": f"/{BUCKET}/{KEY}?versionId={version_id}",
            "x-amz-copy-source-range": "bytes=0-3",
        },
    )
    _assert_not_implemented(resp, "PUT")
    assert populated.app.state.metadata.get_parts(upload_id) == []
    _assert_unchanged(populated)


@pytest.mark.parametrize("version_id", ["3HL4kqtJlcpXroDTDmJ", "", "null2", "NULL"])
def test_copy_with_a_non_null_version_id_returns_501_and_creates_nothing(populated, version_id):
    resp = populated.put(
        f"/{BUCKET}/new.txt",
        headers={"x-amz-copy-source": f"/{BUCKET}/{KEY}?versionId={version_id}"},
    )
    _assert_not_implemented(resp, "PUT")
    assert populated.get(f"/{BUCKET}/new.txt").status_code == 404
    _assert_unchanged(populated)


@pytest.mark.parametrize(
    "source",
    [
        "arn:aws:s3:us-west-2:123456789012:accesspoint/ap/object/obj.txt",
        "arn%3Aaws%3As3%3Aus-west-2%3A123456789012%3Aaccesspoint/ap/object/obj.txt",
        "/arn:aws:s3-outposts:us-west-2:123456789012:outpost/op/object/obj.txt",
    ],
)
def test_copy_from_an_arn_returns_501_and_creates_nothing(populated, source):
    resp = populated.put(f"/{BUCKET}/new.txt", headers={"x-amz-copy-source": source})
    _assert_not_implemented(resp, "PUT")
    assert populated.get(f"/{BUCKET}/new.txt").status_code == 404
    _assert_unchanged(populated)


@pytest.mark.parametrize(
    ("method", "query"),
    [
        ("POST", "?uploads"),
        ("POST", "?uploadId=x"),
        ("POST", ""),
        ("PUT", "?uploadId=x"),
        ("PUT", "?partNumber=1"),
        ("PUT", "?acl"),
        ("PUT", "?tagging"),
        ("PUT", "?someFutureOp"),
        ("DELETE", ""),
        ("GET", ""),
        ("HEAD", ""),
    ],
)
def test_copy_source_header_outside_a_plain_put_returns_501_and_changes_nothing(
    populated, method, query
):
    resp = populated.request(
        method, f"/{BUCKET}/{KEY}{query}", headers={"x-amz-copy-source": f"/{BUCKET}/src.txt"}
    )
    _assert_not_implemented(resp, method)
    _assert_unchanged(populated)


def test_copy_with_replaced_tags_returns_501_and_creates_nothing(populated):
    resp = populated.put(
        f"/{BUCKET}/new.txt",
        headers={
            "x-amz-copy-source": f"/{BUCKET}/{KEY}",
            "x-amz-tagging-directive": "REPLACE",
            "x-amz-tagging": "a=b",
        },
    )
    _assert_not_implemented(resp, "PUT")
    assert populated.get(f"/{BUCKET}/new.txt").status_code == 404
    _assert_unchanged(populated)


# --- implemented requests still work ---


def test_sdk_operation_hint_is_ignored(populated):
    assert populated.put(f"/{BUCKET}/hinted?x-id=PutObject", content=b"h").status_code == 200
    assert populated.get(f"/{BUCKET}/hinted?x-id=GetObject").content == b"h"


def test_list_objects_parameters_are_not_rejected(populated):
    resp = populated.get(f"/{BUCKET}?list-type=2&prefix=o&max-keys=10")
    assert resp.status_code == 200
    assert f"<Key>{KEY}</Key>" in resp.text


def test_list_object_versions_still_served(populated):
    resp = populated.get(f"/{BUCKET}?versions")
    assert resp.status_code == 200
    assert f"<Key>{KEY}</Key>" in resp.text
