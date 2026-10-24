"""Buckets named like FastAPI's default routes behave like any other bucket.

FastAPI registers /docs, /docs/oauth2-redirect, /redoc and /openapi.json by default.
Litmus turns them off; with them on, GET /docs returned the API docs page instead of
ListObjects for a bucket named "docs".
"""

import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

FORMER_ROUTE_BUCKETS = ["docs", "redoc", "openapi.json"]


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    return TestClient(create_app(storage=storage, metadata=metadata), raise_server_exceptions=False)


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def _listed(client, bucket: str) -> list[str]:
    resp = client.get(f"/{bucket}")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/xml")
    root = ET.fromstring(resp.text)
    assert root.tag == "ListBucketResult"
    assert root.findtext("Name") == bucket
    return [el.text for el in root.iter("Key")]


def test_framework_default_routes_are_not_registered():
    paths = {route.path for route in create_app().routes}
    assert not paths & {"/docs", "/docs/oauth2-redirect", "/redoc", "/openapi.json"}


@pytest.mark.parametrize("bucket", FORMER_ROUTE_BUCKETS)
def test_bucket_named_like_a_framework_route_round_trips(client, bucket):
    assert client.put(f"/{bucket}").status_code == 200
    assert client.head(f"/{bucket}").status_code == 200
    assert _listed(client, bucket) == []

    body = b"stored in " + bucket.encode()
    assert client.put(f"/{bucket}/obj.txt", content=body).status_code == 200
    assert _listed(client, bucket) == ["obj.txt"]
    got = client.get(f"/{bucket}/obj.txt")
    assert got.status_code == 200
    assert got.content == body
    assert got.headers["etag"] == _md5(body)

    assert client.delete(f"/{bucket}/obj.txt").status_code == 204
    assert client.delete(f"/{bucket}").status_code == 204
    assert client.head(f"/{bucket}").status_code == 404


def test_key_named_like_the_oauth2_redirect_route_round_trips(client):
    """/docs/oauth2-redirect was a route of its own; now it is key "oauth2-redirect"
    in bucket "docs"."""
    assert client.put("/docs").status_code == 200
    assert client.put("/docs/oauth2-redirect", content=b"redirect").status_code == 200
    assert _listed(client, "docs") == ["oauth2-redirect"]
    got = client.get("/docs/oauth2-redirect")
    assert got.status_code == 200
    assert got.content == b"redirect"
    assert client.head("/docs/oauth2-redirect").status_code == 200


@pytest.mark.parametrize("bucket", FORMER_ROUTE_BUCKETS)
def test_missing_bucket_named_like_a_framework_route_is_no_such_bucket(client, bucket):
    resp = client.get(f"/{bucket}")
    assert resp.status_code == 404
    assert ET.fromstring(resp.text).findtext("Code") == "NoSuchBucket"
