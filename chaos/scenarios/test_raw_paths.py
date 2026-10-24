"""
Raw request paths with dot segments, sent to a real server process over a socket
(docs/design/storage-layout.md R16).

HTTP clients such as httpx resolve "/bucket/../x" before sending it, so these
requests are written by hand: uvicorn passes the path through without resolving dot
segments, the server percent-decodes it, and everything after the bucket is the key,
stored as exactly that string. A "." or ".." in the bucket position is rejected.
"""

import os
import socket
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from chaos.fixtures.subprocess import ServerProcess, _find_free_port

pytestmark = pytest.mark.skipif(os.name == "nt", reason="server processes are POSIX-only here")


def _raw(port: int, method: str, target: str, body: bytes = b"") -> tuple[int, bytes]:
    request = (
        f"{method} {target} HTTP/1.1\r\nHost: localhost\r\n"
        f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
    ).encode() + body
    with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
        sock.sendall(request)
        data = b""
        while chunk := sock.recv(65536):
            data += chunk
    head, _, payload = data.partition(b"\r\n\r\n")
    return int(head.split(b" ")[1]), payload


@pytest.fixture
def server(tmp_path: Path):
    srv = ServerProcess(data_dir=tmp_path, db_path=tmp_path / "meta.db", port=_find_free_port())
    srv.start()
    srv.wait_ready()
    yield srv
    srv.stop()


def _listed(port: int) -> list[str]:
    status, payload = _raw(port, "GET", "/bkt")
    assert status == 200
    return sorted(el.text for el in ET.fromstring(payload).iter("Key"))


@pytest.mark.parametrize(
    "target,key",
    [
        ("/bkt/../x", "../x"),
        ("/bkt/a/../b", "a/../b"),
        ("/bkt/./c", "./c"),
        ("/bkt/..", ".."),
        ("/bkt/%2E%2E/d", "../d"),
        ("/bkt/..%2Fe", "../e"),
    ],
)
def test_raw_dot_segment_path_stores_the_literal_key(server, tmp_path, target, key):
    assert _raw(server.port, "PUT", "/bkt")[0] == 200
    status, _ = _raw(server.port, "PUT", target, target.encode())
    assert status == 200
    assert _listed(server.port) == [key]
    status, payload = _raw(server.port, "GET", target)
    assert (status, payload) == (200, target.encode())
    outside = [
        p
        for p in tmp_path.rglob("*")
        if p.is_file()
        and not p.is_relative_to(tmp_path / "blobs")
        and not p.name.startswith("meta.db")
    ]
    assert outside == []


@pytest.mark.parametrize("target", ["/../bkt/x", "/./bkt/x", "/%2E%2E/bkt/x"])
def test_raw_dot_segment_bucket_is_rejected(server, target):
    assert _raw(server.port, "PUT", "/bkt")[0] == 200
    status, payload = _raw(server.port, "PUT", target, b"x")
    assert status == 400
    assert ET.fromstring(payload).findtext("Code") == "InvalidBucketName"
    assert _listed(server.port) == []


# --- framework responses through a real server: always S3 XML errors ---


def _raw_response(port: int, method: str, target: str) -> tuple[int, dict[str, str], bytes]:
    request = (
        f"{method} {target} HTTP/1.1\r\nHost: localhost\r\n"
        "Content-Length: 0\r\nConnection: close\r\n\r\n"
    ).encode()
    with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
        sock.sendall(request)
        data = b""
        while chunk := sock.recv(65536):
            data += chunk
    head, _, payload = data.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    headers = {k.strip().lower(): v.strip() for k, v in (h.split(":", 1) for h in lines[1:])}
    return int(lines[0].split(" ")[1]), headers, payload


@pytest.mark.parametrize("target", ["//x", "//", "*"])
def test_raw_unmatched_path_is_invalid_uri(server, target):
    status, headers, payload = _raw_response(server.port, "GET", target)
    assert status == 400
    assert headers["content-type"] == "application/xml"
    assert ET.fromstring(payload).findtext("Code") == "InvalidURI"


@pytest.mark.parametrize(
    "method,target,allowed",
    [("PATCH", "/bkt/k", "DELETE, GET, HEAD, POST, PUT"), ("POST", "/", "GET")],
)
def test_raw_unsupported_method_is_method_not_allowed(server, method, target, allowed):
    status, headers, payload = _raw_response(server.port, method, target)
    assert status == 405
    assert headers["allow"] == allowed
    assert headers["content-type"] == "application/xml"
    assert ET.fromstring(payload).findtext("Code") == "MethodNotAllowed"


def test_raw_options_is_not_implemented(server):
    status, headers, payload = _raw_response(server.port, "OPTIONS", "/bkt/k?X-Amz-Expires=100")
    assert status == 501
    assert headers["content-type"] == "application/xml"
    assert ET.fromstring(payload).findtext("Code") == "NotImplemented"
