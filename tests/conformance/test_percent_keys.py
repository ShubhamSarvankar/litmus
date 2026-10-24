"""Keys that contain "%" followed by two hex digits round trip literally.

A client percent-encodes the key once ("a%41" is sent as "a%2541"), and the server
must decode it once. Starlette's TestClient decodes the request path a second time,
so in-process tests cannot send these keys the way a real client does. These tests run
the real app under uvicorn (both HTTP implementations) and talk to it over raw
sockets, so a second decode anywhere in Litmus would store, find or list a different
key.
"""

import socket
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import quote

import pytest
import uvicorn

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "percent"
HTTP_IMPLEMENTATIONS = ["httptools", "h11"]

# Each decodes once to itself; a second decode would give the value in the comment.
KEYS = [
    "a%41",  # aA
    "a%2F",  # a/
    "%25",  # %
    "%2541",  # %41
    "a%2Fb/c%3F",  # a/b/c?
    "%E6%97%A5",  # the character U+65E5
    "100%",  # not an escape: stays as it is either way
    "%zz",  # not an escape
]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(params=HTTP_IMPLEMENTATIONS)
def port(request, tmp_path: Path):
    app = create_app(
        storage=BlobStore(tmp_path / "blobs"),
        metadata=SQLiteMetadataStore(db_path=tmp_path / "meta.db"),
    )
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            app, host="127.0.0.1", port=port, http=request.param, ws="none", log_level="warning"
        )
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("uvicorn did not start")
        time.sleep(0.02)
    assert _request(port, "PUT", f"/{BUCKET}")[0] == 200
    yield port
    server.should_exit = True
    thread.join(timeout=10)


def _request(port: int, method: str, target: str, body: bytes = b"") -> tuple[int, bytes]:
    """Send target exactly as given (no client-side encoding or normalization) and
    return (status, body)."""
    head = (
        f"{method} {target} HTTP/1.1\r\nHost: x\r\nContent-Length: {len(body)}\r\n"
        "Connection: close\r\n\r\n"
    )
    with socket.create_connection(("127.0.0.1", port)) as s:
        s.settimeout(5)
        s.sendall(head.encode("ascii") + body)
        data = b""
        while chunk := s.recv(65536):
            data += chunk
    headers, _, payload = data.partition(b"\r\n\r\n")
    status = int(headers.split(b" ", 2)[1])
    if b"transfer-encoding: chunked" in headers.lower():
        payload = _unchunk(payload)
    return status, payload


def _unchunk(data: bytes) -> bytes:
    out = b""
    while True:
        size_line, _, data = data.partition(b"\r\n")
        size = int(size_line, 16)
        if size == 0:
            return out
        out, data = out + data[:size], data[size + 2 :]


def _object_path(key: str) -> str:
    return f"/{BUCKET}/{quote(key, safe='')}"


def _listed_keys(port: int, query: str = "") -> list[str]:
    status, body = _request(port, "GET", f"/{BUCKET}{query}")
    assert status == 200, body
    return [el.text for el in ET.fromstring(body).iter("Key")]


@pytest.mark.parametrize("key", KEYS)
def test_percent_key_round_trips(port, key):
    assert _object_path(key).count("%25") >= key.count("%")
    assert _request(port, "PUT", _object_path(key), key.encode())[0] == 200
    assert _request(port, "GET", _object_path(key)) == (200, key.encode())
    assert _listed_keys(port) == [key]
    assert _listed_keys(port, "?list-type=2") == [key]
    assert _listed_keys(port, "?versions") == [key]
    assert _request(port, "DELETE", _object_path(key))[0] == 204
    assert _listed_keys(port) == []


def test_percent_keys_are_distinct_from_their_decoded_form(port):
    pairs = [("a%41", "aA"), ("a%2F", "a/"), ("%2541", "%41"), ("%E6%97%A5", "日")]
    for encoded, decoded in pairs:
        assert _request(port, "PUT", _object_path(encoded), b"encoded")[0] == 200
        assert _request(port, "PUT", _object_path(decoded), b"decoded")[0] == 200
    for encoded, decoded in pairs:
        assert _request(port, "GET", _object_path(encoded)) == (200, b"encoded")
        assert _request(port, "GET", _object_path(decoded)) == (200, b"decoded")
    expected = sorted({k for pair in pairs for k in pair}, key=lambda k: k.encode("utf-8"))
    assert _listed_keys(port) == expected


def test_percent_in_prefix_marker_and_start_after(port):
    for key in ["a%41", "a%42", "aA", "a%2F/x"]:
        assert _request(port, "PUT", _object_path(key), b"x")[0] == 200
    assert _listed_keys(port, "?prefix=a%25") == ["a%2F/x", "a%41", "a%42"]
    assert _listed_keys(port, "?prefix=a%254") == ["a%41", "a%42"]
    assert _listed_keys(port, "?marker=a%2541") == ["a%42", "aA"]
    assert _listed_keys(port, "?list-type=2&start-after=a%2541") == ["a%42", "aA"]
