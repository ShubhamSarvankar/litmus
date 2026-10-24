"""Listings paged by boto3 itself, against the real app under uvicorn.

boto3's paginators carry NextMarker, NextContinuationToken and NextKeyMarker /
NextVersionIdMarker from page to page, and ceph/s3-tests deletes every test bucket's
objects by paging ListObjectVersions with MaxKeys=128 (s3tests/functional/__init__.py,
list_versions). Both must see every key exactly once.
"""

import socket
import threading
import time
from pathlib import Path

import boto3
import pytest
import uvicorn
from botocore.config import Config

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "paged"
KEYS = sorted(
    {f"d{i % 7}/k{i:03}" for i in range(150)} | {f"top{i:02}" for i in range(30)} | {"é/1", "日"},
    key=lambda k: k.encode("utf-8"),
)


@pytest.fixture(scope="module")
def s3(tmp_path_factory):
    tmp: Path = tmp_path_factory.mktemp("boto3")
    app = create_app(
        storage=BlobStore(tmp / "blobs"), metadata=SQLiteMetadataStore(db_path=tmp / "meta.db")
    )
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
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
    yield client
    server.should_exit = True
    thread.join(timeout=10)


@pytest.fixture(scope="module")
def bucket(s3):
    s3.create_bucket(Bucket=BUCKET)
    for key in KEYS:
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"x")
    yield BUCKET
    for key in KEYS:
        s3.delete_object(Bucket=BUCKET, Key=key)
    s3.delete_bucket(Bucket=BUCKET)


def _top_level():
    entries = {k.split("/", 1)[0] + "/" if "/" in k else k for k in KEYS}
    return sorted(entries, key=lambda k: k.encode("utf-8"))


@pytest.mark.parametrize("page_size", [1, 7, 128])
def test_paginators_list_every_key_once(s3, bucket, page_size):
    for operation, items in [
        ("list_objects", "Contents"),
        ("list_objects_v2", "Contents"),
        ("list_object_versions", "Versions"),
    ]:
        pages = s3.get_paginator(operation).paginate(
            Bucket=bucket, PaginationConfig={"PageSize": page_size}
        )
        got = [item["Key"] for page in pages for item in page.get(items, [])]
        assert got == KEYS, operation


@pytest.mark.parametrize("page_size", [1, 3, 128])
def test_paginators_with_delimiter(s3, bucket, page_size):
    for operation, items in [
        ("list_objects", "Contents"),
        ("list_objects_v2", "Contents"),
        ("list_object_versions", "Versions"),
    ]:
        pages = s3.get_paginator(operation).paginate(
            Bucket=bucket, Delimiter="/", PaginationConfig={"PageSize": page_size}
        )
        entries = []
        for page in pages:
            entries += [item["Key"] for item in page.get(items, [])]
            entries += [p["Prefix"] for p in page.get("CommonPrefixes", [])]
        assert sorted(entries, key=lambda k: k.encode("utf-8")) == _top_level(), operation
        assert len(entries) == len(set(entries)), operation


def test_ceph_teardown_loop_deletes_everything(s3):
    # s3tests/functional/__init__.py list_versions and nuke_bucket, at the pinned commit:
    # page ListObjectVersions with MaxKeys=128 and delete each batch before the next.
    s3.create_bucket(Bucket="teardown")
    # The keys of test_bucket_list_delimiter_not_skip_special, cut to 305: three pages.
    keys = [f"0/{i}" for i in range(1000, 1300)] + ["0/", "1999", "1999#", "1999+", "2000"]
    for key in keys:
        s3.put_object(Bucket="teardown", Key=key, Body=b"")
    kwargs = {"Bucket": "teardown", "MaxKeys": 128}
    deleted, truncated = [], True
    while truncated:
        listing = s3.list_object_versions(**kwargs)
        kwargs["KeyMarker"] = listing.get("NextKeyMarker")
        kwargs["VersionIdMarker"] = listing.get("NextVersionIdMarker")
        truncated = listing["IsTruncated"]
        objs = listing.get("Versions", []) + listing.get("DeleteMarkers", [])
        if objs:
            batch = [{"Key": o["Key"], "VersionId": o["VersionId"]} for o in objs]
            s3.delete_objects(Bucket="teardown", Delete={"Objects": batch})
            deleted += [o["Key"] for o in batch]
    assert sorted(deleted) == sorted(keys)
    assert s3.list_objects_v2(Bucket="teardown")["KeyCount"] == 0
    s3.delete_bucket(Bucket="teardown")
