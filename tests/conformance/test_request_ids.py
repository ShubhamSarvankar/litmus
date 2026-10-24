"""Every response carries x-amz-request-id and x-amz-id-2, fresh per request, and
every XML error body repeats them as RequestId and HostId."""

import base64
import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import anyio
import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.request_ids import new_request_ids
from server.storage.blobs import BlobStore

REQUEST_ID = re.compile(r"[A-Z0-9]{16}")


@pytest.fixture
def stores(tmp_path: Path):
    return BlobStore(tmp_path / "blobs"), SQLiteMetadataStore(db_path=tmp_path / "meta.db")


@pytest.fixture
def app(stores):
    storage, metadata = stores
    application = create_app(storage=storage, metadata=metadata)
    client = TestClient(application, raise_server_exceptions=False)
    assert client.put("/b").status_code == 200
    assert client.put("/b/k", content=b"0123456789" * 10).status_code == 200
    return application


def _asgi(app, method: str, path: str, headers=()):
    """Call the app with an exact path and headers, which an HTTP client may rewrite.
    Returns (status, headers, body); the request body is never sent."""
    sent = {"status": None, "headers": {}, "body": b""}

    async def receive():
        await anyio.sleep_forever()  # the client withholds the body

    async def send(message):
        if message["type"] == "http.response.start":
            sent["status"] = message["status"]
            sent["headers"] = {k.decode().lower(): v.decode() for k, v in message["headers"]}
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
        "headers": [(b"host", b"testserver"), *headers],
        "client": ("127.0.0.1", 1),
        "server": ("testserver", 80),
        "root_path": "",
    }

    async def run():
        with anyio.fail_after(5):
            await app(scope, receive, send)

    anyio.run(run)
    return sent["status"], sent["headers"], sent["body"]


def _client(app):
    return TestClient(app, raise_server_exceptions=False)


def _via_client(method, path, **kwargs):
    def call(app, stores, monkeypatch):
        r = _client(app).request(method, path, **kwargs)
        return r.status_code, {k.lower(): v for k, v in r.headers.items()}, r.content

    return call


def _unmatched_path(app, stores, monkeypatch):
    return _asgi(app, "GET", "//x")


def _validation_error(app, stores, monkeypatch):
    def typed(n: int):
        return {"n": n}

    app.router.routes.insert(0, APIRoute("/__probe/{n}", typed, methods=["GET"]))
    return _via_client("GET", "/__probe/not-a-number")(app, stores, monkeypatch)


def _unhandled_exception(app, stores, monkeypatch):
    _, metadata = stores

    def broken(*args, **kwargs):
        raise RuntimeError("unexpected")

    monkeypatch.setattr(metadata, "list_buckets", broken)
    return _via_client("GET", "/")(app, stores, monkeypatch)


def _expect_continue_early_error(app, stores, monkeypatch):
    status, headers, body = _asgi(
        app,
        "PUT",
        "/no-such-bucket/k",
        headers=[(b"content-length", b"5"), (b"expect", b"100-continue")],
    )
    assert headers.get("connection") == "close", "the early-error path did not close"
    return status, headers, body


# (name, call, expected status, has an XML error body)
RESPONSE_PATHS = [
    ("handler success: PutObject", _via_client("PUT", "/b/new", content=b"x"), 200, False),
    ("handler success: ListObjects", _via_client("GET", "/b"), 200, False),
    ("handler success: DeleteObject", _via_client("DELETE", "/b/k"), 204, False),
    (
        "handler success: DeleteObjects",
        _via_client("POST", "/b?delete", content=b"<Delete><Object><Key>k</Key></Object></Delete>"),
        200,
        False,
    ),
    ("handler success: CreateMultipartUpload", _via_client("POST", "/b/mp?uploads"), 200, False),
    ("streaming GET", _via_client("GET", "/b/k"), 200, False),
    ("streaming range GET", _via_client("GET", "/b/k", headers={"Range": "bytes=0-9"}), 206, False),
    ("HEAD, no body", _via_client("HEAD", "/b/k"), 200, False),
    ("HEAD error, no body", _via_client("HEAD", "/b/missing"), 404, False),
    ("handler error: NoSuchKey", _via_client("GET", "/b/missing"), 404, True),
    ("handler error: NoSuchBucket", _via_client("GET", "/missing/k"), 404, True),
    ("request guard 501", _via_client("PUT", "/b/k?acl", content=b""), 501, True),
    ("framework: InvalidURI", _unmatched_path, 400, True),
    ("framework: MethodNotAllowed", _via_client("PATCH", "/b"), 405, True),
    ("framework: OPTIONS NotImplemented", _via_client("OPTIONS", "/b"), 501, True),
    ("framework: validation InvalidArgument", _validation_error, 400, True),
    ("framework: unhandled InternalError", _unhandled_exception, 500, True),
    ("Expect: 100-continue early error", _expect_continue_early_error, 404, True),
]


@pytest.mark.parametrize(
    "call,status,error_body",
    [pytest.param(c, s, e, id=n) for n, c, s, e in RESPONSE_PATHS],
)
def test_every_response_carries_request_ids(app, stores, monkeypatch, call, status, error_body):
    got_status, headers, body = call(app, stores, monkeypatch)
    assert got_status == status
    request_id, host_id = headers.get("x-amz-request-id"), headers.get("x-amz-id-2")
    assert request_id is not None and REQUEST_ID.fullmatch(request_id), request_id
    assert host_id is not None and len(base64.b64decode(host_id, validate=True)) == 56
    if error_body:
        root = ET.fromstring(body)
        assert root.tag == "Error"
        assert root.findtext("RequestId") == request_id
        assert root.findtext("HostId") == host_id
    else:
        assert not body.startswith(b"<?xml") or b"<Error>" not in body


def test_two_requests_never_share_an_id(app):
    client = _client(app)
    seen_request_ids, seen_host_ids = set(), set()
    for _ in range(100):
        for method, path in [
            ("GET", "/b/k"),
            ("GET", "/b/missing"),
            ("PATCH", "/b"),
            ("HEAD", "/b"),
        ]:
            r = client.request(method, path)
            seen_request_ids.add(r.headers["x-amz-request-id"])
            seen_host_ids.add(r.headers["x-amz-id-2"])
    assert len(seen_request_ids) == 400
    assert len(seen_host_ids) == 400


def test_generated_ids_are_distinct_and_well_formed():
    pairs = [new_request_ids() for _ in range(10_000)]
    assert len({p.request_id for p in pairs}) == 10_000
    assert len({p.host_id for p in pairs}) == 10_000
    assert all(REQUEST_ID.fullmatch(p.request_id) for p in pairs)


def test_error_responses_are_logged_with_their_request_id(app, caplog):
    with caplog.at_level(logging.INFO, logger="server.request_ids"):
        r = _client(app).get("/b/missing")
    request_id = r.headers["x-amz-request-id"]
    lines = [rec.getMessage() for rec in caplog.records if rec.name == "server.request_ids"]
    assert any(request_id in line and "404" in line for line in lines), lines


def test_unhandled_exception_is_logged_with_request_id_and_traceback(
    app, stores, monkeypatch, caplog
):
    with caplog.at_level(logging.ERROR, logger="server.app"):
        status, headers, _ = _unhandled_exception(app, stores, monkeypatch)
    assert status == 500
    request_id = headers["x-amz-request-id"]
    records = [
        rec for rec in caplog.records if rec.name == "server.app" and request_id in rec.getMessage()
    ]
    assert records and records[0].exc_info is not None
