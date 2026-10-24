"""aws-chunked request bodies with an unsigned payload and a trailing CRC64NVME
(server/api/aws_chunked.py), for PutObject and UploadPart.

The app is driven through its ASGI interface, so each test decides exactly how the body
is split across receive() calls, whether the client disconnects, and how many times the
app asked for the body. Every refusal is checked for the status and S3 code, and then
for what must not exist: no registry row, an empty tmp/, no object or part, and a
consistency sweep with nothing to report.
"""

import base64
import errno
import hashlib
import os
import random
import socket
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
import uvicorn

from chaos.framework.framed import SHA, TRAILER, crc_b64, frame, framed_headers
from server.api import aws_chunked
from server.api.aws_chunked import ChunkDecoder, preflight
from server.app import create_app
from server.config import settings
from server.consistency import run_consistency_sweep
from server.errors.s3errors import InvalidRequest, MissingContentLength
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage import blobs
from server.storage.blobs import BlobStore

BUCKET = "framed"
KEY = "obj"
DATA = bytes(range(256)) * 20  # 5120 bytes


# --- building requests ---


def split(body: bytes, how: str) -> list[bytes]:
    if how == "whole":
        return [body]
    if how == "bytes":
        return [body[i : i + 1] for i in range(len(body))]
    rng = random.Random(how)
    cuts = sorted(rng.sample(range(1, len(body)), 12))
    return [body[a:b] for a, b in zip([0, *cuts], [*cuts, len(body)], strict=True)]


# --- driving the app ---


class Result:
    def __init__(self):
        self.status = None
        self.headers = {}
        self.body = b""
        self.received = 0
        self.raised = None

    @property
    def code(self) -> str | None:
        return ET.fromstring(self.body).findtext("Code") if self.status != 200 else None


async def call(
    app, method: str, target: str, headers, chunks, disconnect=False, result=None
) -> Result:
    """One request. headers is a dict or a list of pairs. The body arrives as the given
    pieces, one per receive(); then the end of the body, or a disconnect."""
    pairs = list(headers.items()) if isinstance(headers, dict) else list(headers)
    path, _, query = target.partition("?")
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": query.encode(),
        "root_path": "",
        "headers": [(b"host", b"x")] + [(k.encode(), v.encode()) for k, v in pairs],
        "server": ("x", 80),
        "client": ("c", 1),
    }
    messages = [{"type": "http.request", "body": c, "more_body": True} for c in chunks]
    messages.append(
        {"type": "http.disconnect"}
        if disconnect
        else {"type": "http.request", "body": b"", "more_body": False}
    )
    result = result or Result()

    async def receive():
        if result.received < len(messages):
            message = messages[result.received]
            result.received += 1
            return message
        return {"type": "http.disconnect"}

    async def send(message):
        if message["type"] == "http.response.start" and result.status is None:
            result.status = message["status"]
            result.headers = {k.decode(): v.decode() for k, v in message["headers"]}
        elif message["type"] == "http.response.body":
            result.body += message.get("body", b"")

    try:
        await app(scope, receive, send)
    except Exception as exc:  # a 500 is re-raised by the server error middleware
        result.raised = exc
    return result


class Env:
    def __init__(self, tmp_path: Path):
        self.storage = BlobStore(tmp_path / "blobs")
        self.metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
        self.app = create_app(storage=self.storage, metadata=self.metadata)
        self.upload_id = None

    async def setup(self):
        r = await call(self.app, "PUT", f"/{BUCKET}", {}, [])
        assert r.status == 200
        r = await call(self.app, "POST", f"/{BUCKET}/{KEY}?uploads", {}, [])
        self.upload_id = ET.fromstring(r.body).findtext("UploadId")

    def target(self, kind: str) -> str:
        if kind == "put":
            return f"/{BUCKET}/{KEY}"
        return f"/{BUCKET}/{KEY}?partNumber=1&uploadId={self.upload_id}"

    async def send(self, kind: str, headers: dict, chunks, **kw) -> Result:
        return await call(self.app, "PUT", self.target(kind), headers, chunks, **kw)

    def stored_bytes(self, kind: str) -> tuple[bytes, str]:
        if kind == "put":
            meta = self.metadata.get_object_meta(BUCKET, KEY)
        else:
            (meta,) = self.metadata.get_parts(self.upload_id)
        with self.storage.open_blob(meta.blob_id) as f:
            return f.read(), meta.etag

    def assert_nothing_left(self, kind: str):
        if kind == "put":
            assert self.metadata.get_object_meta(BUCKET, KEY) is None
        else:
            assert self.metadata.get_parts(self.upload_id) == []
        assert self.metadata.list_blob_records() == []
        assert self.storage.temp_entries() == []
        assert [e for e, _ in self.storage.scan()] == []
        assert run_consistency_sweep(self.metadata, self.storage).issues == []


@pytest.fixture
async def env(tmp_path: Path) -> Env:
    e = Env(tmp_path)
    await e.setup()
    return e


KINDS = ["put", "part"]


def md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


# --- success ---


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("how", ["whole", "bytes", "seed-1", "seed-2", "seed-3"])
async def test_single_chunk_split_across_receives(env, kind, how):
    body = frame(DATA)
    r = await env.send(kind, framed_headers(body, len(DATA)), split(body, how))
    assert r.status == 200, r.body
    assert env.stored_bytes(kind) == (DATA, md5(DATA))
    assert r.received >= 1


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("how", ["whole", "bytes", "seed-1", "seed-2", "seed-3", "seed-4"])
async def test_multi_chunk_split_across_receives(env, kind, how):
    # Size lines, data and CRLFs fall on and across receive() boundaries.
    body = frame(DATA, sizes=[1, 4095, 16, 1008])
    r = await env.send(kind, framed_headers(body, len(DATA)), split(body, how))
    assert r.status == 200, r.body
    assert env.stored_bytes(kind) == (DATA, md5(DATA))


@pytest.mark.parametrize("kind", KINDS)
async def test_response_carries_the_md5_of_the_decoded_bytes(env, kind):
    body = frame(DATA)
    r = await env.send(kind, framed_headers(body, len(DATA)), [body])
    assert r.status == 200
    assert r.headers["etag"] == md5(DATA)


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("sizes", [None, [1000, 4120], [1] * 5 + [5115]])
async def test_framed_and_plain_uploads_store_the_same(tmp_path, kind, sizes):
    framed, plain = Env(tmp_path / "framed"), Env(tmp_path / "plain")
    await framed.setup()
    await plain.setup()
    body = frame(DATA, sizes=sizes)
    assert (await framed.send(kind, framed_headers(body, len(DATA)), [body])).status == 200
    assert (await plain.send(kind, {"content-length": str(len(DATA))}, [DATA])).status == 200
    assert framed.stored_bytes(kind) == plain.stored_bytes(kind) == (DATA, md5(DATA))


@pytest.mark.parametrize("kind", KINDS)
async def test_empty_body(env, kind):
    body = f"0\r\n{TRAILER}:{crc_b64(b'')}\r\n\r\n".encode()
    r = await env.send(kind, framed_headers(body, 0), [body])
    assert r.status == 200, r.body
    assert env.stored_bytes(kind) == (b"", md5(b""))


@pytest.mark.parametrize("kind", KINDS)
async def test_known_crc64nvme_value_end_to_end(env, kind):
    data = b"A" * 1024
    body = frame(data, trailer_line=f"{TRAILER}:Qeh8oXvGiSo=")
    assert (await env.send(kind, framed_headers(body, 1024), [body])).status == 200


@pytest.mark.parametrize("kind", KINDS)
async def test_trailer_name_and_header_are_case_insensitive(env, kind):
    body = frame(DATA, trailer_line=f"X-Amz-Checksum-CRC64NVME:{crc_b64(DATA)}")
    headers = framed_headers(body, len(DATA), **{"x-amz-trailer": "X-Amz-Checksum-Crc64nvme"})
    assert (await env.send(kind, headers, [body])).status == 200


# --- Tier A refusals ---


def _bad_checksum():
    body = frame(DATA, trailer_line=f"{TRAILER}:{crc_b64(DATA + b'x')}")
    return framed_headers(body, len(DATA)), [body], 400, "BadDigest"


def _malformed_value(value):
    def case():
        body = frame(DATA, trailer_line=f"{TRAILER}:{value}")
        return framed_headers(body, len(DATA)), [body], 400, "InvalidDigest"

    return case


def _wrong_trailer_name():
    body = frame(DATA, trailer_line="x-amz-checksum-crc32:AAAAAA==")
    return framed_headers(body, len(DATA)), [body], 400, "InvalidRequest"


def _decoded_short():  # the header promises more than the body holds
    body = frame(DATA)
    return framed_headers(body, len(DATA) + 5), [body], 400, "IncompleteBody"


def _decoded_long():  # the body holds more than the header promises
    body = frame(DATA)
    return framed_headers(body, len(DATA) - 5), [body], 400, "IncompleteBody"


def _content_length_larger():
    body = frame(DATA)
    headers = framed_headers(body, len(DATA), **{"content-length": str(len(body) + 7)})
    return headers, [body], 400, "InvalidRequest"


def _content_length_smaller():
    body = frame(DATA)
    headers = framed_headers(body, len(DATA), **{"content-length": str(len(body) - 7)})
    return headers, [body], 400, "InvalidRequest"


def _framing(**kw):
    def case():
        body = frame(DATA, **kw)
        return framed_headers(body, len(DATA)), [body], 400, "InvalidRequest"

    return case


def _raw(body: bytes, code="InvalidRequest"):
    def case():
        return framed_headers(body, len(DATA)), [body], 400, code

    return case


_FRAMED = frame(DATA, sizes=[2560, 2560])
REFUSALS = {
    "bad-checksum": _bad_checksum,
    "malformed-not-base64": _malformed_value("not*base64!"),
    "malformed-4-bytes": _malformed_value("AAAAAA=="),
    "malformed-empty": _malformed_value(""),
    "malformed-space": _malformed_value(" " + crc_b64(DATA)),
    "wrong-trailer-name": _wrong_trailer_name,
    "decoded-length-short": _decoded_short,
    "decoded-length-long": _decoded_long,
    "content-length-larger": _content_length_larger,
    "content-length-smaller": _content_length_smaller,
    "bad-hex": _framing(size_line=lambda n: "zz"),
    "sign-in-size": _framing(size_line=lambda n: f"+{n:x}"),
    "space-in-size": _framing(size_line=lambda n: f" {n:x}"),
    "empty-size": _framing(size_line=lambda n: ""),
    "chunk-extension": _framing(size_line=lambda n: f"{n:x};ext=1"),
    "huge-size-line": _framing(size_line=lambda n: "0" * 40 + f"{n:x}"),
    "bytes-after-final-crlf": _framing(tail=b"extra"),
    "bare-lf-after-size": _raw(_FRAMED.replace(b"a00\r\n", b"a00\n", 1)),
    "bare-lf-after-data": _raw(frame(DATA).replace(DATA + b"\r\n", DATA + b"\n", 1)),
    "bare-lf-after-trailer": _raw(frame(DATA)[:-4] + b"\n\r\n"),
    "bare-lf-at-end": _raw(frame(DATA)[:-2] + b"\n"),
    "no-trailer": _raw(
        frame(DATA, sizes=[5120])[: -len(f"{TRAILER}:{crc_b64(DATA)}\r\n\r\n")] + b"\r\n"
    ),
    "two-trailer-lines": _raw(frame(DATA)[:-2] + f"{TRAILER}:{crc_b64(DATA)}\r\n\r\n".encode()),
    "trailer-line-too-long": _framing(trailer_line=f"{TRAILER}:" + "A" * 4000),
    "trailer-without-colon": _framing(trailer_line=TRAILER),
    "truncated-in-data": _raw(frame(DATA)[:3000]),
    "truncated-after-zero": _raw(frame(DATA)[: 4 + 2 + len(DATA) + 2 + 3]),
}


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("name", sorted(REFUSALS))
async def test_refused_leaves_nothing(env, kind, name):
    headers, chunks, status, code = REFUSALS[name]()
    r = await env.send(kind, headers, chunks)
    assert (r.status, r.code) == (status, code), r.body
    env.assert_nothing_left(kind)


@pytest.mark.parametrize("kind", KINDS)
async def test_enospc_mid_stream_leaves_nothing(env, kind, monkeypatch):
    real_write, calls = blobs.BlobWriter.write, []

    def write(self, data):
        calls.append(len(data))
        if len(calls) == 3:
            raise OSError(errno.ENOSPC, "No space left on device")
        return real_write(self, data)

    monkeypatch.setattr(blobs.BlobWriter, "write", write)
    body = frame(DATA)
    r = await env.send(kind, framed_headers(body, len(DATA)), split(body, "seed-5"))
    assert (r.status, r.code) == (500, "InternalError")
    assert len(calls) == 3  # the stream was abandoned at the failure
    env.assert_nothing_left(kind)


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("after", [0, 1, 3])
async def test_client_disconnect_mid_body_leaves_nothing(env, kind, after):
    body = frame(DATA)
    chunks = split(body, "seed-6")[: after + 1]  # the body stops short, then the client goes
    r = await env.send(kind, framed_headers(body, len(DATA)), chunks, disconnect=True)
    assert r.status != 200
    env.assert_nothing_left(kind)


async def test_reupload_over_a_part_with_a_bad_checksum_keeps_the_old_part(env):
    old = b"the first attempt" * 100
    body = frame(old)
    assert (await env.send("part", framed_headers(body, len(old)), [body])).status == 200
    (before,) = env.metadata.get_parts(env.upload_id)
    records = env.metadata.list_blob_records()

    headers, chunks, status, code = _bad_checksum()
    r = await env.send("part", headers, chunks)
    assert (r.status, r.code) == (400, "BadDigest")

    (after,) = env.metadata.get_parts(env.upload_id)
    assert after == before and after.etag == md5(old)
    with env.storage.open_blob(after.blob_id) as f:
        assert f.read() == old
    assert env.metadata.list_blob_records() == records
    assert env.storage.temp_entries() == []
    assert run_consistency_sweep(env.metadata, env.storage).issues == []


class _Stop(Exception):
    pass


@pytest.mark.parametrize("kind, point", [("put", "put-mid-write"), ("part", "part-mid-write")])
async def test_mid_write_point_is_reached_after_the_first_buffer(env, kind, point, monkeypatch):
    """One 300 KB chunk arrives as six 50 KB buffers. The mid-write point fires after
    the first write: by then the app has received one buffer, and the temp file holds
    that buffer's decoded bytes, far fewer than the decoded length."""
    monkeypatch.setattr(settings, "test_crash_point", point)
    data = os.urandom(300_000)
    body = frame(data)  # a single chunk
    pieces = [body[i : i + 50_000] for i in range(0, len(body), 50_000)]
    assert len(pieces) == 7 and body.startswith(b"493e0\r\n")
    result = Result()
    seen = []

    def hook(name):
        (temp,) = env.storage.temp_entries()
        seen.append((name, result.received, temp.read_bytes()))
        raise _Stop

    monkeypatch.setattr(blobs, "crash_point", hook)
    r = await env.send(kind, framed_headers(body, len(data)), pieces, result=result)
    assert (r.status, r.code) == (500, "InternalError")
    assert isinstance(r.raised, _Stop)
    ((name, received, on_disk),) = seen
    assert name == point and received == 1
    assert len(on_disk) == 50_000 - len(b"493e0\r\n") < len(data)
    assert data.startswith(on_disk)
    env.assert_nothing_left(kind)


# --- preflight and the guard: refused before the body is read ---


def _no_blob_registered(env):
    assert env.metadata.list_blob_records() == []
    assert env.storage.temp_entries() == []


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    "override, status, code",
    [
        ({"x-amz-decoded-content-length": None}, 400, "InvalidRequest"),
        ({"x-amz-decoded-content-length": "abc"}, 400, "InvalidRequest"),
        ({"x-amz-decoded-content-length": "-5120"}, 400, "InvalidRequest"),
        ({"x-amz-decoded-content-length": "+5120"}, 400, "InvalidRequest"),
        ({"x-amz-decoded-content-length": " 5120"}, 400, "InvalidRequest"),
        ({"x-amz-decoded-content-length": "5120 "}, 400, "InvalidRequest"),
        ({"x-amz-decoded-content-length": "5e3"}, 400, "InvalidRequest"),
        ({"x-amz-decoded-content-length": "٥١٢٠"}, 400, "InvalidRequest"),
        ({"x-amz-decoded-content-length": str(5 * 1024**3 + 1)}, 400, "InvalidRequest"),
        ({"content-length": None}, 411, "MissingContentLength"),
    ],
)
async def test_preflight_refusals_register_nothing_and_read_nothing(
    env, kind, override, status, code
):
    body = frame(DATA)
    r = await env.send(kind, framed_headers(body, len(DATA), **override), [body])
    assert (r.status, r.code) == (status, code)
    assert r.received == 0
    env.assert_nothing_left(kind)


def _headers(**kw):
    from starlette.datastructures import Headers

    return Headers({k: v for k, v in kw.items() if v is not None})


def test_preflight_accepts_exactly_5_gib():
    framed = preflight(
        _headers(
            **{
                "x-amz-decoded-content-length": str(5 * 1024**3),
                "content-length": "5400000000",
                "x-amz-trailer": TRAILER,
            }
        )
    )
    assert (framed.decoded_length, framed.content_length) == (5 * 1024**3, 5400000000)


def test_preflight_refuses_a_trailer_other_than_the_checksum():
    for value in (None, "", "x-amz-checksum-crc32", f"{TRAILER},{TRAILER}"):
        with pytest.raises(InvalidRequest):
            preflight(
                _headers(
                    **{
                        "x-amz-decoded-content-length": "1",
                        "content-length": "9",
                        "x-amz-trailer": value,
                    }
                )
            )
    with pytest.raises(MissingContentLength):
        preflight(_headers(**{"x-amz-decoded-content-length": "1", "x-amz-trailer": TRAILER}))


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    "override",
    [
        {"x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"},
        {"x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"},
        {"x-amz-content-sha256": "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER"},
        {"x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD"},
        {"x-amz-content-sha256": "streaming-unsigned-payload-trailer"},
        {"x-amz-content-sha256": "UNSIGNED-PAYLOAD"},
        {"x-amz-content-sha256": None},
        {"x-amz-trailer": None},
        {"x-amz-trailer": "x-amz-checksum-crc32"},
        {"x-amz-trailer": "x-amz-checksum-sha256"},
        {"x-amz-trailer": "x-amz-checksum-crc64nvme,x-amz-checksum-crc32"},
        {"x-amz-trailer": "x-amz-checksum-crc64nvme,x-amz-checksum-crc64nvme"},
        {"x-amz-trailer": "x-amz-checksum-crc64nvme,"},
        {"x-amz-trailer": "x-amz-checksum-crc64nvme-x"},
        {"x-amz-trailer": ""},
    ],
)
async def test_everything_else_stays_501_before_the_body_is_read(env, kind, override):
    body = frame(DATA)
    r = await env.send(kind, framed_headers(body, len(DATA), **override), [body])
    assert (r.status, r.code) == (501, "NotImplemented"), r.body
    assert r.received == 0
    env.assert_nothing_left(kind)


@pytest.mark.parametrize("kind", KINDS)
async def test_repeated_trailer_header_is_501(env, kind):
    body = frame(DATA)
    pairs = [*framed_headers(body, len(DATA)).items(), ("x-amz-trailer", TRAILER)]
    r = await env.send(kind, pairs, [body])
    assert (r.status, r.code) == (501, "NotImplemented")
    assert r.received == 0
    env.assert_nothing_left(kind)


async def test_framed_create_bucket_is_501_and_creates_nothing(env):
    # The supported framing is decoded for PutObject and UploadPart only. CreateBucket
    # ignores its body, so a framed one would silently succeed; it is refused instead.
    body = frame(DATA)
    r = await call(env.app, "PUT", "/another", framed_headers(body, len(DATA)), [body])
    assert (r.status, r.code) == (501, "NotImplemented")
    assert r.received == 0
    assert not env.metadata.bucket_exists("another")
    plain = await call(env.app, "PUT", "/another", {"content-length": "0"}, [])
    assert plain.status == 200  # the same bucket, unframed, is created


@pytest.mark.parametrize(
    "target",
    [
        f"/{BUCKET}/{KEY}?uploadId=u",  # uploadId without partNumber
        f"/{BUCKET}/{KEY}?partNumber=1",  # partNumber without uploadId
        f"/{BUCKET}/{KEY}?partNumber=1&uploadId=u&acl",
        f"/{BUCKET}/{KEY}?acl",
        f"/{BUCKET}/{KEY}?tagging",
        f"/{BUCKET}?acl",
        f"/{BUCKET}?versioning",
        f"/{BUCKET}/",
    ],
)
async def test_framed_other_puts_are_501_before_the_body_is_read(env, target):
    body = frame(DATA)
    r = await call(env.app, "PUT", target, framed_headers(body, len(DATA)), [body])
    assert (r.status, r.code) == (501, "NotImplemented"), r.body
    assert r.received == 0
    assert env.metadata.list_blob_records() == []
    assert env.metadata.get_object_meta(BUCKET, KEY) is None


# --- the decoder: bounded buffers and early stop ---


def test_decoder_buffers_stay_small():
    decoder = ChunkDecoder(10**9)
    with pytest.raises(InvalidRequest):
        decoder.feed(b"f" * 1000)  # no CR within 32 digits
    decoder = ChunkDecoder(10**9)
    decoder.feed(b"0\r\n")
    with pytest.raises(InvalidRequest):
        decoder.feed(b"x" * 1000)  # no CR within the trailer line limit


def test_decoder_holds_nothing_back():
    decoder = ChunkDecoder(100)
    pieces = decoder.feed(b"a\r\n01234")
    assert b"".join(pieces) == b"01234"
    assert b"".join(decoder.feed(b"56789\r\n")) == b"56789"
    assert not decoder.done


async def test_reading_stops_once_the_decoded_length_is_exceeded(env):
    body = frame(DATA)
    headers = framed_headers(body, 100)
    chunks = split(body, "bytes")
    r = await env.send("put", headers, chunks)
    assert (r.status, r.code) == (400, "IncompleteBody")
    assert r.received < 200  # not the 5000+ buffers of the whole body
    env.assert_nothing_left("put")


async def test_each_received_buffer_is_written_as_it_arrives(env, monkeypatch):
    log = []
    real_write = blobs.BlobWriter.write

    def write(self, data):
        log.append(("write", len(data)))
        return real_write(self, data)

    monkeypatch.setattr(blobs.BlobWriter, "write", write)
    data = b"z" * 4000
    body = frame(data)  # a single chunk, delivered in four buffers
    pieces = [body[i : i + 1100] for i in range(0, len(body), 1100)]
    assert len(pieces) == 4

    r = await env.send("put", framed_headers(body, len(data)), pieces)
    assert r.status == 200
    sizes = [n for _, n in log]
    assert len(sizes) == 4 and sum(sizes) == len(data)  # one write per buffer, 1 chunk


def test_module_exposes_the_one_supported_framing():
    assert aws_chunked.STREAMING_UNSIGNED_PAYLOAD_TRAILER == SHA
    assert aws_chunked.CHECKSUM_TRAILER == TRAILER
    assert aws_chunked.MAX_DECODED_LENGTH == 5 * 1024**3
    assert base64.b64decode(crc_b64(b"123456789")).hex() == "ae8b14860a799888"


# --- over a real socket, under both HTTP implementations ---


@pytest.fixture(params=["httptools", "h11"])
def live(request, tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(storage=storage, metadata=metadata),
            host="127.0.0.1",
            port=port,
            http=request.param,
            log_level="critical",
        )
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.02)
    assert server.started
    yield port, metadata, storage
    server.should_exit = True
    thread.join(timeout=10)


def _wire(method, target, headers, body: bytes) -> bytes:
    head = f"{method} {target} HTTP/1.1\r\nHost: x\r\n"
    head += "".join(f"{k}: {v}\r\n" for k, v in headers.items()) + "\r\n"
    return head.encode() + body


def _status_and_code(sock: socket.socket) -> tuple[int, str | None]:
    sock.settimeout(10)
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(65536)
        assert chunk, "connection closed without a response"
        data += chunk
    head, _, rest = data.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split()[1])
    fields = {k.strip().lower(): v.strip() for k, v in (line.split(":", 1) for line in lines[1:])}
    length = int(fields.get("content-length", "0"))
    while len(rest) < length:
        rest += sock.recv(65536)
    return status, (ET.fromstring(rest).findtext("Code") if rest else None)


def _live_send(port, method, target, headers, body) -> socket.socket:
    s = socket.create_connection(("127.0.0.1", port))
    s.sendall(_wire(method, target, headers, body))
    return s


def _live_bucket(port):
    s = _live_send(port, "PUT", f"/{BUCKET}", {"content-length": "0"}, b"")
    assert _status_and_code(s) == (200, None)
    s.close()


def _assert_live_clean(metadata, storage):
    deadline = time.monotonic() + 5  # the handler may still be unwinding after a disconnect
    while (metadata.list_blob_records() or storage.temp_entries()) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert metadata.list_blob_records() == []
    assert storage.temp_entries() == []
    assert run_consistency_sweep(metadata, storage).issues == []


def test_live_put_stored_in_several_chunks(live):
    port, metadata, storage = live
    _live_bucket(port)
    data = os.urandom(3 * 1024 * 1024 + 17)
    body = frame(data, sizes=[len(data) // 2, len(data) - len(data) // 2])
    s = _live_send(port, "PUT", f"/{BUCKET}/k", framed_headers(body, len(data)), body)
    assert _status_and_code(s)[0] == 200
    meta = metadata.get_object_meta(BUCKET, "k")
    with storage.open_blob(meta.blob_id) as f:
        assert f.read() == data
    assert meta.etag == md5(data)


def test_live_bytes_after_the_final_crlf_within_content_length(live):
    port, metadata, storage = live
    _live_bucket(port)
    body = frame(DATA, tail=b"junk")
    s = _live_send(port, "PUT", f"/{BUCKET}/k", framed_headers(body, len(DATA)), body)
    assert _status_and_code(s) == (400, "InvalidRequest")
    assert metadata.get_object_meta(BUCKET, "k") is None
    _assert_live_clean(metadata, storage)


def test_live_refusal_with_a_large_unread_body_keeps_the_connection(live):
    port, metadata, storage = live
    _live_bucket(port)
    data = os.urandom(8 * 1024 * 1024)
    body = frame(data, trailer_line=f"{TRAILER}:AAAAAAAAAAA=")
    s = _live_send(port, "PUT", f"/{BUCKET}/k", framed_headers(body, len(data)), body)
    assert _status_and_code(s) == (400, "BadDigest")
    s.sendall(_wire("GET", f"/{BUCKET}/k", {}, b""))
    assert _status_and_code(s)[0] == 404  # the same connection answers the next request
    _assert_live_clean(metadata, storage)


def test_live_decoded_length_exceeded_stops_reading(live):
    port, metadata, storage = live
    _live_bucket(port)
    data = os.urandom(8 * 1024 * 1024)
    body = frame(data)
    start = time.monotonic()
    s = _live_send(port, "PUT", f"/{BUCKET}/k", framed_headers(body, 1000), body)
    assert _status_and_code(s) == (400, "IncompleteBody")
    assert time.monotonic() - start < 5
    _assert_live_clean(metadata, storage)


def test_live_client_disconnect_mid_body_leaves_nothing(live):
    port, metadata, storage = live
    _live_bucket(port)
    data = os.urandom(8 * 1024 * 1024)
    body = frame(data)
    s = _live_send(
        port, "PUT", f"/{BUCKET}/k", framed_headers(body, len(data)), body[: 3 * 1024 * 1024]
    )
    time.sleep(0.5)
    s.close()
    assert metadata.get_object_meta(BUCKET, "k") is None
    _assert_live_clean(metadata, storage)
