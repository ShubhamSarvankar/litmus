"""Chunk-framed (aws-chunked) bodies are refused, not stored.

botocore 1.36+ (and AWS CLI 2.23+) send PutObject and UploadPart bodies to an https
endpoint as aws-chunked with a trailing checksum. Litmus does not decode the framing,
and used to store it as the object's bytes with 200. It now answers 501 before any
byte is written or any blob registered.

The botocore tests let botocore build the exact request it would send over TLS
(endpoint https://..., its own AwsChunkedWrapper and headers), capture it before it is
sent, and deliver it over a plain socket to the real app under uvicorn, as a
TLS-terminating proxy would.
"""

import base64
import socket
import threading
import time
import xml.etree.ElementTree as ET
import zlib
from pathlib import Path

import boto3
import pytest
import uvicorn
from botocore.config import Config
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "chunked"
BODY = b"hello world, " * 1000
HTTP_IMPLEMENTATIONS = ["httptools", "h11"]


class _Captured(Exception):
    def __init__(self, request):
        self.request = request


def _botocore_https_request(operation: str, **params):
    """The request botocore builds for an https endpoint, captured before sending."""
    client = boto3.client(
        "s3",
        endpoint_url="https://litmus.invalid",
        aws_access_key_id="key",
        aws_secret_access_key="secret",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 0}),
    )

    def capture(request, **kwargs):
        raise _Captured(request)

    client.meta.events.register("before-send.s3.*", capture)
    with pytest.raises(_Captured) as info:
        getattr(client, operation)(**params)
    request = info.value.request
    body = request.body.read() if hasattr(request.body, "read") else request.body
    headers = {k: v.decode() if isinstance(v, bytes) else v for k, v in request.headers.items()}
    path = request.url.split("litmus.invalid", 1)[1]
    return path, headers, body


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(params=HTTP_IMPLEMENTATIONS)
def server(request, tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    port = _free_port()
    uv = uvicorn.Server(
        uvicorn.Config(
            create_app(storage=storage, metadata=metadata),
            host="127.0.0.1",
            port=port,
            http=request.param,
            ws="none",
            log_level="warning",
        )
    )
    thread = threading.Thread(target=uv.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not uv.started:
        if time.monotonic() > deadline:
            raise RuntimeError("uvicorn did not start")
        time.sleep(0.02)
    assert _send(port, "PUT", f"/{BUCKET}", {}, b"")[0] == 200
    yield port, metadata, tmp_path / "blobs"
    uv.should_exit = True
    thread.join(timeout=10)


def _send(port, method, target, headers, body: bytes):
    """One request over a fresh socket; a Transfer-Encoding: chunked request gets its
    body in HTTP chunked framing, as an HTTP client would send it."""
    headers = {k: v for k, v in headers.items() if k.lower() not in ("host", "connection")}
    if headers.get("Transfer-Encoding", "").lower() == "chunked":
        wire_body = (
            f"{len(body):x}\r\n".encode() + body + b"\r\n0\r\n\r\n" if body else b"0\r\n\r\n"
        )
    else:
        headers["Content-Length"] = str(len(body))
        wire_body = body
    head = f"{method} {target} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n"
    head += "".join(f"{k}: {v}\r\n" for k, v in headers.items()) + "\r\n"
    with socket.create_connection(("127.0.0.1", port)) as s:
        s.settimeout(5)
        s.sendall(head.encode("latin-1") + wire_body)
        data = b""
        while chunk := s.recv(65536):
            data += chunk
    status_line, _, rest = data.partition(b"\r\n")
    _, _, payload = rest.partition(b"\r\n\r\n")
    return int(status_line.split()[1]), payload


def _assert_nothing_stored(port, metadata, blob_dir: Path):
    status, body = _send(port, "GET", f"/{BUCKET}/k", {}, b"")
    assert status == 404
    assert ET.fromstring(body).findtext("Code") == "NoSuchKey"
    status, body = _send(port, "GET", f"/{BUCKET}", {}, b"")
    assert status == 200
    assert ET.fromstring(body).find("Contents") is None
    assert metadata.list_blob_records() == []
    assert [p for p in blob_dir.rglob("*") if p.is_file() and p.parent != blob_dir] == []


def test_botocore_request_is_chunk_framed():
    # Guards the premise: if botocore stopped framing, the tests below would prove nothing.
    _, headers, body = _botocore_https_request("put_object", Bucket=BUCKET, Key="k", Body=BODY)
    # With awscrt installed botocore signs through the CRT signer, which names this header
    # in lower case; header names are case insensitive.
    content_sha256 = {k.lower(): v for k, v in headers.items()}["x-amz-content-sha256"]
    assert content_sha256 == "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
    assert headers["X-Amz-Decoded-Content-Length"] == str(len(BODY))
    assert headers["Content-Encoding"] == "aws-chunked"
    assert body != BODY and BODY in body and b"x-amz-checksum-crc32:" in body


def test_botocore_chunked_put_object_is_refused(server):
    port, metadata, blob_dir = server
    path, headers, body = _botocore_https_request("put_object", Bucket=BUCKET, Key="k", Body=BODY)
    assert headers["X-Amz-Trailer"] == "x-amz-checksum-crc32"
    headers["X-Amz-Trailer"] = "x-amz-checksum-sha512"  # a trailer that stays refused
    status, payload = _send(port, "PUT", path, headers, body)
    assert status == 501
    assert ET.fromstring(payload).findtext("Code") == "NotImplemented"
    _assert_nothing_stored(port, metadata, blob_dir)


def test_botocore_chunked_upload_part_is_refused(server):
    port, metadata, blob_dir = server
    status, payload = _send(port, "POST", f"/{BUCKET}/k?uploads", {}, b"")
    assert status == 200
    upload_id = ET.fromstring(payload).findtext("UploadId")
    path, headers, body = _botocore_https_request(
        "upload_part", Bucket=BUCKET, Key="k", UploadId=upload_id, PartNumber=1, Body=BODY
    )
    assert headers["X-Amz-Trailer"] == "x-amz-checksum-crc32"
    headers["X-Amz-Trailer"] = "x-amz-checksum-sha512"  # a trailer that stays refused
    status, payload = _send(port, "PUT", path, headers, body)
    assert status == 501
    assert ET.fromstring(payload).findtext("Code") == "NotImplemented"
    assert metadata.get_parts(upload_id) == []
    _assert_nothing_stored(port, metadata, blob_dir)


def test_plain_put_object_still_stored(server):
    # The same client over plain http sends an ordinary body with a checksum header,
    # which is verified against the body (tests/conformance/test_upload_checksums.py).
    port, metadata, _ = server
    crc32 = base64.b64encode(zlib.crc32(BODY).to_bytes(4, "big")).decode()
    status, _ = _send(port, "PUT", f"/{BUCKET}/k", {"x-amz-checksum-crc32": crc32}, BODY)
    assert status == 200
    assert _send(port, "GET", f"/{BUCKET}/k", {}, b"") == (200, BODY)


# --- the detection rule, in process ---


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    app = create_app(
        storage=BlobStore(tmp_path / "blobs"),
        metadata=SQLiteMetadataStore(db_path=tmp_path / "meta.db"),
    )
    client = TestClient(app, raise_server_exceptions=False)
    assert client.put(f"/{BUCKET}").status_code == 200
    return client


@pytest.mark.parametrize(
    "headers",
    [
        {"x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER"},
        {"x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"},
        {"x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"},
        {"x-amz-content-sha256": "streaming-unsigned-payload-trailer"},
        {"x-amz-decoded-content-length": "5"},
        {"x-amz-decoded-content-length": "5", "content-encoding": "aws-chunked"},
    ],
)
def test_chunk_framed_put_refused(client, headers):
    for target in (f"/{BUCKET}/k", f"/{BUCKET}/k?partNumber=1&uploadId=u"):
        resp = client.put(target, content=b"5\r\nhello\r\n0\r\n\r\n", headers=headers)
        assert resp.status_code == 501, target
        assert ET.fromstring(resp.text).findtext("Code") == "NotImplemented"
    assert client.get(f"/{BUCKET}/k").status_code == 404


@pytest.mark.parametrize(
    "encoding",
    ["aws-chunked", "gzip, aws-chunked", "aws-chunked, gzip", "aws-chunked, aws-chunked"],
)
def test_content_encoding_alone_is_not_chunk_framing(client, encoding):
    # S3 does not require Content-Encoding: aws-chunked on a chunked upload, and does
    # not treat it as one without the other headers: the body is stored as sent.
    resp = client.put(f"/{BUCKET}/k", content=b"plain body", headers={"content-encoding": encoding})
    assert resp.status_code == 200
    # Raw bytes: GET returns the remaining encoding (gzip), which the client would undo.
    with client.stream("GET", f"/{BUCKET}/k") as got:
        assert b"".join(got.iter_raw()) == b"plain body"


def test_ordinary_content_sha256_values_accepted(client):
    for value in (
        "UNSIGNED-PAYLOAD",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    ):
        resp = client.put(f"/{BUCKET}/k", content=b"", headers={"x-amz-content-sha256": value})
        assert resp.status_code == 200, value


def test_chunk_framing_headers_on_other_methods_refused(client):
    # Framing is decoded for PutObject and UploadPart alone; on every other request the
    # headers are refused, GET and HEAD included (test_framing_other_requests.py).
    client.put(f"/{BUCKET}/k", content=b"x")
    headers = {"x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER"}
    assert client.get(f"/{BUCKET}/k", headers=headers).status_code == 501
    assert client.head(f"/{BUCKET}/k", headers=headers).status_code == 501
    assert client.get(f"/{BUCKET}/k").content == b"x"


def test_trailer_checksum_is_base64():
    # The captured trailer carries a base64 CRC32, as the AWS user guide describes.
    _, _, body = _botocore_https_request("put_object", Bucket=BUCKET, Key="k", Body=BODY)
    trailer = body.split(b"x-amz-checksum-crc32:", 1)[1].split(b"\r\n", 1)[0].strip()
    assert len(base64.b64decode(trailer)) == 4


# --- the connection after a refused framed body ---


class _Closed(Exception):
    pass


def _read_one_response(sock: socket.socket, buffered: bytes = b""):
    """Read exactly one response from a kept-alive socket; return (status, headers,
    body, bytes left over). Raises _Closed if the server closed the connection."""
    data = buffered
    while b"\r\n\r\n" not in data:
        try:
            chunk = sock.recv(65536)
        except (ConnectionResetError, BrokenPipeError) as e:
            raise _Closed() from e
        if not chunk:
            raise _Closed()
        data += chunk
    head, _, rest = data.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split()[1])
    headers = {k.strip().lower(): v.strip() for k, v in (h.split(":", 1) for h in lines[1:])}

    def more():
        chunk = sock.recv(65536)
        if not chunk:
            raise _Closed()
        return chunk

    if headers.get("transfer-encoding", "").lower() == "chunked":
        body = b""
        while True:
            while b"\r\n" not in rest:
                rest += more()
            size_line, _, rest = rest.partition(b"\r\n")
            size = int(size_line.split(b";")[0], 16)
            while len(rest) < size + 2:
                rest += more()
            body, rest = body + rest[:size], rest[size + 2 :]
            if size == 0:
                return status, headers, body, rest
    length = int(headers.get("content-length", "0"))
    while len(rest) < length:
        rest += more()
    return status, headers, rest[:length], rest[length:]


def _framed_put(port, metadata, expect: bool):
    """botocore's framed PutObject for an existing bucket: (head bytes, body bytes)."""
    path, headers, body = _botocore_https_request("put_object", Bucket=BUCKET, Key="k", Body=BODY)
    assert headers.get("Expect") == "100-continue"  # botocore always asks
    if not expect:
        del headers["Expect"]
    assert headers["Transfer-Encoding"] == "chunked"
    head = f"PUT {path} HTTP/1.1\r\nHost: x\r\n"
    head += "".join(f"{k}: {v}\r\n" for k, v in headers.items() if k.lower() != "host")
    wire_body = f"{len(body):x}\r\n".encode() + body + b"\r\n0\r\n\r\n"
    return (head + "\r\n").encode("latin-1"), wire_body


def _seed_and_get_request(port) -> bytes:
    assert _send(port, "PUT", f"/{BUCKET}/other", {}, b"other body")[0] == 200
    return f"GET /{BUCKET}/other HTTP/1.1\r\nHost: x\r\n\r\n".encode()


def test_refused_framed_put_keeps_connection_usable(server):
    # botocore's framed request without Expect: the body follows the headers at once.
    # The 501 comes before the body is read; uvicorn must discard the unread body so
    # the next request on the same connection gets its own response.
    port, metadata, blob_dir = server
    get_request = _seed_and_get_request(port)
    head, body = _framed_put(port, metadata, expect=False)
    # botocore's CRC32 trailer is accepted now; a SHA512 one stays refused.
    assert b"X-Amz-Trailer: x-amz-checksum-crc32\r\n" in head
    head = head.replace(
        b"X-Amz-Trailer: x-amz-checksum-crc32", b"X-Amz-Trailer: x-amz-checksum-sha512"
    )
    with socket.create_connection(("127.0.0.1", port)) as s:
        s.settimeout(5)
        s.sendall(head + body)
        status, headers, payload, left = _read_one_response(s)
        assert status == 501
        assert ET.fromstring(payload).findtext("Code") == "NotImplemented"
        assert headers.get("connection", "").lower() != "close"
        s.sendall(get_request)
        status, headers, payload, left = _read_one_response(s, left)
        assert (status, payload, left) == (200, b"other body", b"")
    assert metadata.list_blob_records()[0].key == "other"
    assert len(metadata.list_blob_records()) == 1


def test_refused_framed_put_with_expect_continue_closes(server):
    # The exact botocore request, Expect: 100-continue included. The client waits for
    # 100 Continue and withholds the body; Litmus answers 501 without asking for the
    # body, so no 100 is sent and the response closes the connection: the next
    # request on it must not be read as the withheld body.
    port, metadata, blob_dir = server
    get_request = _seed_and_get_request(port)
    head, _ = _framed_put(port, metadata, expect=True)
    # botocore's CRC32 trailer is accepted now; a SHA512 one stays refused.
    assert b"X-Amz-Trailer: x-amz-checksum-crc32\r\n" in head
    head = head.replace(
        b"X-Amz-Trailer: x-amz-checksum-crc32", b"X-Amz-Trailer: x-amz-checksum-sha512"
    )
    with socket.create_connection(("127.0.0.1", port)) as s:
        s.settimeout(5)
        s.sendall(head)
        status, headers, payload, left = _read_one_response(s)
        assert status == 501  # not 100: the body was never asked for
        assert ET.fromstring(payload).findtext("Code") == "NotImplemented"
        assert headers.get("connection", "").lower() == "close"
        with pytest.raises((_Closed, BrokenPipeError, ConnectionResetError)):
            s.sendall(get_request)
            _read_one_response(s, left)
    with socket.create_connection(("127.0.0.1", port)) as s:
        s.settimeout(5)
        s.sendall(get_request)
        assert _read_one_response(s)[:3:2] == (200, b"other body")
    assert len(metadata.list_blob_records()) == 1


def test_refused_framed_put_with_expect_and_body_sent_anyway_closes(server):
    # A client that sends the body without waiting for 100 Continue (botocore does
    # after its Expect timeout), followed at once by another request.
    port, metadata, blob_dir = server
    get_request = _seed_and_get_request(port)
    head, body = _framed_put(port, metadata, expect=True)
    # botocore's CRC32 trailer is accepted now; a SHA512 one stays refused.
    assert b"X-Amz-Trailer: x-amz-checksum-crc32\r\n" in head
    head = head.replace(
        b"X-Amz-Trailer: x-amz-checksum-crc32", b"X-Amz-Trailer: x-amz-checksum-sha512"
    )
    with socket.create_connection(("127.0.0.1", port)) as s:
        s.settimeout(5)
        s.sendall(head + body + get_request)
        status, headers, payload, left = _read_one_response(s)
        assert status == 501
        assert headers.get("connection", "").lower() == "close"
        # The GET is not answered on this connection, and nothing was misparsed:
        # the connection ends after the 501.
        with pytest.raises(_Closed):
            _read_one_response(s, left)
    assert len(metadata.list_blob_records()) == 1
