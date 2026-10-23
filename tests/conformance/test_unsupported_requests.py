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
from server.storage.filesystem import FilesystemBackend

BUCKET = "test-bucket"
KEY = "obj.txt"
BODY = b"precious content"
METHODS = ["GET", "HEAD", "PUT", "POST", "DELETE"]

# Requests that are implemented, so they are not expected to return 501.
IMPLEMENTED_BUCKET = {("GET", "versions"), ("POST", "delete")}
IMPLEMENTED_OBJECT = {("POST", "uploadId"), ("DELETE", "uploadId"), ("POST", "uploads")}


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    storage = FilesystemBackend(data_dir=tmp_path / "objects", parts_dir=tmp_path / "parts")
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


def test_delete_object_with_version_id_returns_501(populated):
    _assert_not_implemented(populated.delete(f"/{BUCKET}/{KEY}?versionId=abc"), "DELETE")
    _assert_unchanged(populated)


def test_put_part_number_without_upload_id_returns_501(populated):
    resp = populated.put(f"/{BUCKET}/{KEY}?partNumber=1", content=b"overwrite")
    _assert_not_implemented(resp, "PUT")
    _assert_unchanged(populated)


def test_copy_object_onto_existing_key_returns_501_and_leaves_it_intact(populated):
    assert populated.put(f"/{BUCKET}/src.txt", content=b"source").status_code == 200
    resp = populated.put(f"/{BUCKET}/{KEY}", headers={"x-amz-copy-source": f"/{BUCKET}/src.txt"})
    _assert_not_implemented(resp, "PUT")
    assert populated.get(f"/{BUCKET}/{KEY}").content == BODY


def test_copy_object_to_new_key_returns_501_and_creates_nothing(populated):
    resp = populated.put(f"/{BUCKET}/new.txt", headers={"x-amz-copy-source": f"/{BUCKET}/{KEY}"})
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
