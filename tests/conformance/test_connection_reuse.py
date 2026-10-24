"""Early error responses must not corrupt the next request on a kept-alive connection.

These run the real app under uvicorn (both HTTP implementations) and talk to it over
raw sockets, because the failure is in HTTP framing, which the in-process test client
does not exercise.

Case 1, Expect: 100-continue: the client sends headers, waits for 100 Continue, and
withholds the body if a final response comes first. The server must then close the
connection rather than wait for that body and misparse the next request as it.

Case 2, body sent immediately: the client sends headers and body together, and the
server answers before reading the body. The unread body must be discarded so the next
request on the same connection is parsed correctly.
"""

import socket
import threading
import time
from pathlib import Path

import pytest
import uvicorn

from server.app import LitmusApp, create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "test-bucket"
HTTP_IMPLEMENTATIONS = ["httptools", "h11"]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _serve(app, http: str):
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, http=http, ws="none", log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("uvicorn did not start")
        time.sleep(0.02)
    return server, thread, port


@pytest.fixture(params=HTTP_IMPLEMENTATIONS)
def litmus(request, tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    server, thread, port = _serve(create_app(storage=storage, metadata=metadata), request.param)
    with socket.create_connection(("127.0.0.1", port)) as s:
        s.sendall(f"PUT /{BUCKET} HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n".encode())
        assert _read_response(s)[0] == 200
    yield port
    server.should_exit = True
    thread.join(timeout=10)


class _Closed(Exception):
    pass


def _read_response(sock: socket.socket, timeout: float = 5.0):
    """Read one response; return (status, headers). Raises _Closed on EOF/reset."""
    sock.settimeout(timeout)
    buf = b""
    while b"\r\n\r\n" not in buf:
        try:
            chunk = sock.recv(65536)
        except (ConnectionResetError, BrokenPipeError) as e:
            raise _Closed() from e
        if not chunk:
            raise _Closed()
        buf += chunk
    head, _, rest = buf.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split()[1])
    if status == 100:  # interim response; the final one follows
        return _read_response_from(sock, rest, timeout)
    headers = {k.strip().lower(): v.strip() for k, v in (h.split(":", 1) for h in lines[1:])}
    length = int(headers.get("content-length", "0"))
    while len(rest) < length:
        chunk = sock.recv(65536)
        if not chunk:
            break
        rest += chunk
    return status, headers


def _read_response_from(sock, already: bytes, timeout: float):
    class _Prefixed:
        def __init__(self):
            self.pending = already

        def settimeout(self, t):
            sock.settimeout(t)

        def recv(self, n):
            if self.pending:
                out, self.pending = self.pending, b""
                return out
            return sock.recv(n)

    return _read_response(_Prefixed(), timeout)


def _next_request_outcome(sock: socket.socket) -> str:
    """Send GET /health on the same connection: 'ok', 'closed', or the bad status."""
    try:
        sock.sendall(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n")
        status, _ = _read_response(sock)
    except (_Closed, BrokenPipeError, ConnectionResetError):
        return "closed"
    return "ok" if status == 200 else f"status {status}"


EARLY_ERRORS = [
    pytest.param("/no-such-bucket/k", 404, id="404-no-such-bucket"),
    pytest.param(f"/{BUCKET}/k?acl", 501, id="501-unimplemented"),
    pytest.param("/%2E%2E/k", 400, id="400-invalid-bucket-name"),
]


# --- case 1: Expect: 100-continue, body withheld ---


@pytest.mark.parametrize(("path", "expected"), EARLY_ERRORS)
def test_expect_continue_early_error_closes_instead_of_misparsing(litmus, path, expected):
    with socket.create_connection(("127.0.0.1", litmus)) as s:
        s.sendall(
            f"PUT {path} HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n"
            "Expect: 100-continue\r\n\r\n".encode()
        )
        status, headers = _read_response(s)
        assert status == expected
        assert headers.get("connection", "").lower() == "close"
        # The body was never sent. The next request must not be read as that body.
        assert _next_request_outcome(s) == "closed"
    # A new connection works.
    with socket.create_connection(("127.0.0.1", litmus)) as s:
        assert _next_request_outcome(s) == "ok"


def test_expect_continue_read_body_keeps_connection_alive(litmus):
    with socket.create_connection(("127.0.0.1", litmus)) as s:
        s.sendall(
            f"PUT /{BUCKET}/k HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n"
            "Expect: 100-continue\r\n\r\n".encode()
        )
        s.settimeout(5)
        interim = s.recv(65536)
        assert interim.startswith(b"HTTP/1.1 100")
        s.sendall(b"hello")
        status, headers = _read_response_from(s, interim.partition(b"\r\n\r\n")[2], 5)
        assert status == 200
        assert headers.get("connection", "").lower() != "close"
        assert _next_request_outcome(s) == "ok"


# --- case 2: no Expect, body sent with the headers ---


@pytest.mark.parametrize("size", [5, 1024 * 1024])
@pytest.mark.parametrize(("path", "expected"), EARLY_ERRORS)
def test_body_sent_early_error_next_request_parsed_correctly(litmus, path, expected, size):
    with socket.create_connection(("127.0.0.1", litmus)) as s:
        s.sendall(f"PUT {path} HTTP/1.1\r\nHost: x\r\nContent-Length: {size}\r\n\r\n".encode())
        s.sendall(b"x" * size)
        status, headers = _read_response(s)
        assert status == expected
        assert headers.get("connection", "").lower() != "close"
        assert _next_request_outcome(s) == "ok"


@pytest.mark.parametrize(("path", "expected"), EARLY_ERRORS)
def test_chunked_body_sent_early_error_next_request_parsed_correctly(litmus, path, expected):
    with socket.create_connection(("127.0.0.1", litmus)) as s:
        s.sendall(
            f"PUT {path} HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"
            "5\r\nhello\r\n0\r\n\r\n".encode()
        )
        assert _read_response(s)[0] == expected
        assert _next_request_outcome(s) == "ok"


# --- the wrapper also covers 500s sent by Starlette's error middleware ---


@pytest.mark.parametrize("http", HTTP_IMPLEMENTATIONS)
def test_expect_continue_unhandled_error_also_closes(http):
    app = LitmusApp()

    @app.put("/boom")
    async def boom():
        raise RuntimeError("fails before reading the body")

    server, thread, port = _serve(app, http)
    try:
        with socket.create_connection(("127.0.0.1", port)) as s:
            s.sendall(
                b"PUT /boom HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n"
                b"Expect: 100-continue\r\n\r\n"
            )
            status, headers = _read_response(s)
            assert status == 500
            assert headers.get("connection", "").lower() == "close"
            assert _next_request_outcome(s) == "closed"
    finally:
        server.should_exit = True
        thread.join(timeout=10)
