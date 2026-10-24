"""Responses that FastAPI or Starlette would produce on their own are S3 XML errors.

Without handlers, an unmatched path got Starlette's JSON 404, a method no route
accepts got a JSON 405 whose Allow header named one route's methods, a validation
error got FastAPI's JSON 422, and an unhandled exception got a plain-text 500.
"""

import xml.etree.ElementTree as ET
from pathlib import Path

import anyio
import pytest
from fastapi import HTTPException
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

ALL_S3_METHODS = "DELETE, GET, HEAD, POST, PUT"


@pytest.fixture
def stores(tmp_path: Path):
    return BlobStore(tmp_path / "blobs"), SQLiteMetadataStore(db_path=tmp_path / "meta.db")


@pytest.fixture
def app(stores):
    storage, metadata = stores
    return create_app(storage=storage, metadata=metadata)


@pytest.fixture
def client(app) -> TestClient:
    c = TestClient(app, raise_server_exceptions=False)
    assert c.put("/b").status_code == 200
    assert c.put("/b/k", content=b"body").status_code == 200
    return c


def _error(resp) -> ET.Element:
    assert resp.headers["content-type"] == "application/xml"
    root = ET.fromstring(resp.text)
    assert root.tag == "Error"
    return root


def _asgi(app, method: str, path: str) -> tuple[int, dict[str, str], bytes]:
    """Call the app with an exact path, which an HTTP client may rewrite."""
    sent = {"status": None, "headers": {}, "body": b""}

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        if message["type"] == "http.response.start":
            sent["status"] = message["status"]
            sent["headers"] = {k.decode(): v.decode() for k, v in message["headers"]}
        elif message["type"] == "http.response.body":
            sent["body"] += message.get("body", b"")

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [(b"host", b"testserver")],
        "client": ("127.0.0.1", 1),
        "server": ("testserver", 80),
        "root_path": "",
    }
    anyio.run(app, scope, receive, send)
    return sent["status"], sent["headers"], sent["body"]


# --- unmatched paths ---


@pytest.mark.parametrize("path", ["//x", "//", "//x/y", "*"])
def test_unmatched_path_is_invalid_uri(app, path):
    status, headers, body = _asgi(app, "GET", path)
    assert status == 400
    assert headers["content-type"] == "application/xml"
    root = ET.fromstring(body)
    assert root.findtext("Code") == "InvalidURI"
    assert root.findtext("Message") == "Couldn't parse the specified URI."


# --- methods no route accepts ---


@pytest.mark.parametrize(
    "method,path,allowed",
    [
        ("POST", "/", "GET"),
        ("PUT", "/", "GET"),
        ("DELETE", "/", "GET"),
        ("PATCH", "/", "GET"),
        ("PATCH", "/b", ALL_S3_METHODS),
        ("PATCH", "/b/k", ALL_S3_METHODS),
        ("TRACE", "/b/k", ALL_S3_METHODS),
        ("PATCH", "/health", ALL_S3_METHODS),
    ],
)
def test_unsupported_method_is_method_not_allowed_with_full_allow(client, method, path, allowed):
    resp = client.request(method, path)
    assert resp.status_code == 405
    assert resp.headers["allow"] == allowed
    root = _error(resp)
    assert root.findtext("Code") == "MethodNotAllowed"
    assert root.findtext("Method") == method


def test_head_on_service_is_method_not_allowed_without_body(client):
    resp = client.head("/")
    assert resp.status_code == 405
    assert resp.headers["allow"] == "GET"
    assert resp.headers["content-type"] == "application/xml"
    assert resp.content == b""


def test_unsupported_method_changes_nothing(client):
    assert client.request("PATCH", "/b/k", content=b"new").status_code == 405
    assert client.request("DELETE", "/").status_code == 405
    assert client.get("/b/k").content == b"body"
    assert client.head("/b").status_code == 200


# --- OPTIONS (CORS preflight, not implemented) ---


@pytest.mark.parametrize("path", ["/", "/b", "/b/k"])
def test_options_is_not_implemented(client, path):
    resp = client.options(path, headers={"Origin": "http://example.com"})
    assert resp.status_code == 501
    root = _error(resp)
    assert root.findtext("Code") == "NotImplemented"
    assert root.findtext("Method") == "OPTIONS"
    assert client.get("/b/k").content == b"body"


# --- validation errors ---


def _with_probe_route(app, endpoint, path="/__probe/{n}"):
    """A route of the test's own, ahead of the S3 routes, to reach a handler that
    the S3 routes cannot trigger."""
    app.router.routes.insert(0, APIRoute(path, endpoint, methods=["GET"]))
    return TestClient(app, raise_server_exceptions=False)


def test_validation_error_is_invalid_argument(app):
    def typed(n: int):
        return {"n": n}

    client = _with_probe_route(app, typed)
    assert client.get("/__probe/7").status_code == 200
    resp = client.get("/__probe/not-a-number")
    assert resp.status_code == 400
    assert _error(resp).findtext("Code") == "InvalidArgument"


@pytest.mark.parametrize(
    "raised,status,code", [(418, 400, "InvalidRequest"), (502, 500, "InternalError")]
)
def test_other_framework_http_errors_are_xml(app, raised, status, code):
    def fails(n: int):
        raise HTTPException(status_code=raised)

    resp = _with_probe_route(app, fails).get("/__probe/1")
    assert resp.status_code == status
    assert _error(resp).findtext("Code") == code


# --- unhandled exceptions ---


def test_unhandled_exception_is_internal_error(app, stores, monkeypatch):
    _, metadata = stores
    client = TestClient(app, raise_server_exceptions=False)

    def broken(*args, **kwargs):
        raise RuntimeError("unexpected")

    monkeypatch.setattr(metadata, "list_buckets", broken)
    resp = client.get("/")
    assert resp.status_code == 500
    root = _error(resp)
    assert root.findtext("Code") == "InternalError"
    assert "unexpected" not in resp.text


def test_unhandled_exception_on_head_has_no_body(app, stores, monkeypatch):
    _, metadata = stores
    client = TestClient(app, raise_server_exceptions=False)
    monkeypatch.setattr(metadata, "bucket_exists", lambda bucket: 1 / 0)
    resp = client.head("/b")
    assert resp.status_code == 500
    assert resp.headers["content-type"] == "application/xml"
    assert resp.content == b""
