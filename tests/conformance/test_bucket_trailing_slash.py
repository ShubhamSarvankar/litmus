"""
/{bucket}/ is the bucket, like /{bucket}.

The key of a path-style request is the rest of the Request-URI after the bucket
(S3 User Guide, "Virtual hosting of general purpose buckets", HTTP Host header bucket
specification). With nothing after the slash there is no key, and the request is the
bucket-level one, as GET / on a virtual-hosted request is. Litmus used to route
/{bucket}/ to the object handlers with an empty key: PUT stored an object under "".
"""

import io
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.base import ObjectMeta
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

DELETE_EMPTY_KEY = b"<Delete><Object><Key></Key></Object></Delete>"


@pytest.fixture
def deps(tmp_path: Path):
    return BlobStore(tmp_path / "blobs"), SQLiteMetadataStore(db_path=tmp_path / "meta.db")


@pytest.fixture
def client(deps) -> TestClient:
    storage, metadata = deps
    return TestClient(create_app(storage=storage, metadata=metadata), raise_server_exceptions=False)


def _code(resp) -> str | None:
    return ET.fromstring(resp.text).findtext("Code")


def _seed_empty_key(deps, bucket: str) -> None:
    """An object under the empty key, as the old PUT /{bucket}/ left one."""
    storage, metadata = deps
    blob_id = metadata.register_blob("object", bucket, "")
    etag, size = storage.write_blob(blob_id, io.BytesIO(b"old"))
    metadata.put_object_meta(
        bucket,
        "",
        ObjectMeta(
            bucket=bucket,
            key="",
            size=size,
            etag=etag,
            last_modified=datetime(2024, 1, 1),
            content_type="application/octet-stream",
            blob_id=blob_id,
        ),
    )


# --- GET: ListObjects ---


def test_get_lists_the_bucket_like_the_path_without_the_slash(client):
    client.put("/bkt")
    client.put("/bkt/one", content=b"1")
    client.put("/bkt/two", content=b"2")
    plain, slashed = client.get("/bkt"), client.get("/bkt/")
    assert slashed.status_code == 200
    assert slashed.text.count("<Key>") == 2
    assert slashed.text == plain.text
    assert [p.text for p in _keys(slashed)] == [p.text for p in _keys(plain)] == ["one", "two"]


def _keys(resp):
    return ET.fromstring(resp.text).findall("Contents/Key")


def test_get_takes_listing_parameters(client):
    client.put("/bkt")
    for key in ("a/1", "a/2", "b/1"):
        client.put(f"/bkt/{key}", content=b"x")
    resp = client.get("/bkt/", params={"prefix": "a/", "max-keys": "1"})
    assert resp.status_code == 200
    assert [k.text for k in _keys(resp)] == ["a/1"]
    assert "<IsTruncated>true</IsTruncated>" in resp.text


def test_get_missing_bucket_is_no_such_bucket(client):
    resp = client.get("/nope/")
    assert (resp.status_code, _code(resp)) == (404, "NoSuchBucket")


def test_get_is_not_an_object_get_of_the_empty_key(client, deps):
    client.put("/bkt")
    _seed_empty_key(deps, "bkt")
    resp = client.get("/bkt/")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/xml"
    assert b"old" not in resp.content


def test_get_unimplemented_subresource_is_501(client):
    client.put("/bkt")
    resp = client.get("/bkt/?acl")
    assert (resp.status_code, _code(resp)) == (501, "NotImplemented")


# --- HEAD ---


def test_head_existing_bucket_is_200(client):
    client.put("/bkt")
    assert client.head("/bkt/").status_code == 200


def test_head_missing_bucket_is_404(client):
    assert client.head("/nope/").status_code == 404


# --- PUT: CreateBucket ---


def test_put_creates_the_bucket(client):
    assert client.put("/bkt/").status_code == 200
    assert client.head("/bkt").status_code == 200
    assert client.get("/").text.count("<Name>bkt</Name>") == 1


def test_put_creates_no_object(client):
    client.put("/bkt/", content=b"payload")
    assert _keys(client.get("/bkt")) == []


def test_put_existing_bucket_is_bucket_already_exists(client, deps):
    client.put("/bkt")
    resp = client.put("/bkt/")
    assert (resp.status_code, _code(resp)) == (409, "BucketAlreadyExists")
    assert _keys(client.get("/bkt")) == []  # and no empty-key object was written
    assert deps[1].get_object_meta("bkt", "") is None


def test_put_invalid_name_is_400(client):
    resp = client.put("/Foo_Bar/")
    assert (resp.status_code, _code(resp)) == (400, "InvalidBucketName")


def test_put_unimplemented_subresource_is_501(client):
    client.put("/bkt")
    resp = client.put("/bkt/?versioning", content=b"<x/>")
    assert (resp.status_code, _code(resp)) == (501, "NotImplemented")


# --- DELETE: DeleteBucket ---


def test_delete_removes_an_empty_bucket(client):
    client.put("/bkt")
    assert client.delete("/bkt/").status_code == 204
    assert client.head("/bkt").status_code == 404


def test_delete_non_empty_bucket_is_bucket_not_empty(client):
    client.put("/bkt")
    client.put("/bkt/k", content=b"x")
    resp = client.delete("/bkt/")
    assert (resp.status_code, _code(resp)) == (409, "BucketNotEmpty")
    assert client.get("/bkt/k").content == b"x"


def test_delete_missing_bucket_is_no_such_bucket(client):
    resp = client.delete("/nope/")
    assert (resp.status_code, _code(resp)) == (404, "NoSuchBucket")


# --- POST: DeleteObjects ---


def test_post_delete_deletes_the_listed_keys(client):
    client.put("/bkt")
    client.put("/bkt/one", content=b"1")
    client.put("/bkt/two", content=b"2")
    body = b"<Delete><Object><Key>one</Key></Object></Delete>"
    resp = client.post("/bkt/?delete", content=body)
    assert resp.status_code == 200
    assert "<Deleted><Key>one</Key></Deleted>" in resp.text
    assert [k.text for k in _keys(client.get("/bkt"))] == ["two"]


def test_post_delete_missing_bucket_is_no_such_bucket(client):
    resp = client.post("/nope/?delete", content=DELETE_EMPTY_KEY)
    assert (resp.status_code, _code(resp)) == (404, "NoSuchBucket")


@pytest.mark.parametrize("query", ["", "?uploads", "?uploadId=u"])
def test_post_without_delete_is_501(client, query):
    client.put("/bkt")
    resp = client.post(f"/bkt/{query}", content=b"x")
    assert (resp.status_code, _code(resp)) == (501, "NotImplemented")
    assert _keys(client.get("/bkt")) == []


# --- keys that merely contain a slash are still keys ---


def test_a_key_made_of_one_slash_is_still_an_object(client):
    client.put("/bkt")
    assert client.put("/bkt//", content=b"slash").status_code == 200  # key "/"
    assert [k.text for k in _keys(client.get("/bkt"))] == ["/"]
    assert client.get("/bkt//").content == b"slash"
    assert client.delete("/bkt//").status_code == 204


def test_a_key_with_a_trailing_slash_is_still_an_object(client):
    client.put("/bkt")
    assert client.put("/bkt/dir/", content=b"d").status_code == 200
    assert client.get("/bkt/dir/").content == b"d"
    assert client.head("/bkt/dir/").status_code == 200
    assert client.delete("/bkt/dir/").status_code == 204


# --- an object already stored under the empty key ---


def test_an_empty_key_object_is_listed_blocks_delete_bucket_and_goes_by_delete_objects(
    client, deps
):
    """A store written before this fix can hold an object under "" (PUT /{bucket}/
    created one). It is no longer addressable as /{bucket}/, so nothing orphans it:
    ListObjects still shows it, DeleteBucket refuses while it exists, and DeleteObjects
    with an empty <Key> removes it."""
    client.put("/bkt")
    _seed_empty_key(deps, "bkt")
    assert [k.text for k in _keys(client.get("/bkt"))] == [None]

    resp = client.delete("/bkt/")
    assert (resp.status_code, _code(resp)) == (409, "BucketNotEmpty")
    assert deps[1].get_object_meta("bkt", "") is not None

    resp = client.post("/bkt?delete", content=DELETE_EMPTY_KEY)
    assert resp.status_code == 200
    assert "<Deleted><Key /></Deleted>" in resp.text
    assert deps[1].get_object_meta("bkt", "") is None
    assert deps[1].list_blob_records() == []
    assert client.delete("/bkt/").status_code == 204


def test_an_empty_key_object_goes_by_delete_objects_on_the_slash_path_too(client, deps):
    client.put("/bkt")
    _seed_empty_key(deps, "bkt")
    assert client.post("/bkt/?delete", content=DELETE_EMPTY_KEY).status_code == 200
    assert deps[1].get_object_meta("bkt", "") is None
