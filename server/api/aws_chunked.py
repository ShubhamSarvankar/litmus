"""Decoding of aws-chunked request bodies with an unsigned payload and a trailing
CRC64NVME checksum, for PutObject and UploadPart.

This is the one framing the AWS CLI sends over http by default:

    x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER
    x-amz-decoded-content-length: <size without framing>
    x-amz-trailer: x-amz-checksum-crc64nvme
    Content-Length: <framed size>

    <hex size> CRLF <data> CRLF   (any number of chunks)
    0 CRLF
    x-amz-checksum-crc64nvme:<base64 of the 8 byte big endian CRC> CRLF
    CRLF

There are no chunk signatures and no chunk extensions. The signed STREAMING-AWS4-*
variants and every other trailer stay NotImplemented (server/api/request_guard.py).

Memory is bounded: the decoder is fed whatever the server received, keeps at most a
size line or the trailer line (a few hundred bytes), and hands the decoded bytes
back at once, so the body is written to the blob as it arrives.
"""

import re
from dataclasses import dataclass

from fastapi import Request
from starlette.datastructures import Headers

from server.crc64nvme import Crc64Nvme, decode_b64_digest
from server.errors.s3errors import (
    BadDigest,
    IncompleteBody,
    InvalidDigest,
    InvalidRequest,
    MissingContentLength,
)
from server.storage.blobs import BlobStore, BlobWriter

STREAMING_UNSIGNED_PAYLOAD_TRAILER = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
CONTENT_SHA256_HEADER = "x-amz-content-sha256"
DECODED_LENGTH_HEADER = "x-amz-decoded-content-length"
TRAILER_HEADER = "x-amz-trailer"
CHECKSUM_TRAILER = "x-amz-checksum-crc64nvme"

# S3's largest object (PutObject) and largest part (UploadPart) are both 5 GiB.
MAX_DECODED_LENGTH = 5 * 1024**3
_MAX_SIZE_DIGITS = 32
_MAX_TRAILER_LINE = 256
# The most one write to the blob carries, so a large received buffer is not written in
# one piece and the mid-write crash point lands at a bounded offset (as in write_blob).
_MAX_WRITE = 65536

_DIGITS = re.compile(r"[0-9]+")
_HEX = frozenset(b"0123456789abcdefABCDEF")

_CR, _LF = 0x0D, 0x0A


def is_framed(headers: Headers) -> bool:
    """Whether the request carries the one supported framing. The request guard lets
    no other STREAMING- value through."""
    return headers.get(CONTENT_SHA256_HEADER, "") == STREAMING_UNSIGNED_PAYLOAD_TRAILER


def trailer_is_supported(headers: Headers) -> bool:
    """x-amz-trailer names exactly x-amz-checksum-crc64nvme, once, in any case."""
    entries = [
        entry.strip().lower()
        for value in headers.getlist(TRAILER_HEADER)
        for entry in value.split(",")
    ]
    return entries == [CHECKSUM_TRAILER]


@dataclass(frozen=True)
class FramedRequest:
    decoded_length: int
    content_length: int


def preflight(headers: Headers) -> FramedRequest:
    """The checks that need no body, run before anything is registered or written."""
    declared = headers.get(DECODED_LENGTH_HEADER)
    if declared is None:
        raise InvalidRequest(extra={"Header": DECODED_LENGTH_HEADER})
    if not _DIGITS.fullmatch(declared) or int(declared) > MAX_DECODED_LENGTH:
        raise InvalidRequest(extra={"Header": DECODED_LENGTH_HEADER})
    content_length = headers.get("content-length")
    if content_length is None:
        raise MissingContentLength()
    if not _DIGITS.fullmatch(content_length):
        raise InvalidRequest(extra={"Header": "Content-Length"})
    if not trailer_is_supported(headers):
        raise InvalidRequest(extra={"Header": TRAILER_HEADER})
    return FramedRequest(int(declared), int(content_length))


def _framing(reason: str) -> InvalidRequest:
    return InvalidRequest(extra={"Reason": reason})


class ChunkDecoder:
    """Byte fed state machine for the framing above. feed() returns the decoded data
    found in the bytes given, as a list of slices; anything wrong with the framing
    raises. Strict: lower or upper case hex digits only, no sign, whitespace or chunk
    extension, CRLF exactly (a bare LF is an error), nothing after the final CRLF."""

    _SIZE, _SIZE_LF, _DATA, _DATA_CR, _DATA_LF = range(5)
    _TRAILER, _TRAILER_LF, _END_CR, _END_LF, _DONE = range(5, 10)

    def __init__(self, decoded_length: int):
        self._limit = decoded_length
        self._state = self._SIZE
        self._line = bytearray()
        self._remaining = 0
        self.decoded = 0
        self.framed = 0
        self.trailer: bytes | None = None

    @property
    def done(self) -> bool:
        return self._state == self._DONE

    def feed(self, data: bytes) -> list[bytes]:
        out: list[bytes] = []
        self.framed += len(data)
        view = memoryview(data)
        i, n = 0, len(data)
        while i < n:
            state = self._state
            if state == self._DATA:
                take = min(self._remaining, n - i, _MAX_WRITE)
                self.decoded += take
                if self.decoded > self._limit:
                    raise IncompleteBody(extra={"Reason": "more data than the decoded length"})
                out.append(bytes(view[i : i + take]))
                i += take
                self._remaining -= take
                if self._remaining == 0:
                    self._state = self._DATA_CR
                continue
            b = data[i]
            i += 1
            if state == self._SIZE:
                if b == _CR:
                    if not self._line:
                        raise _framing("empty chunk size")
                    self._remaining = int(self._line, 16)
                    self._line.clear()
                    self._state = self._SIZE_LF
                elif b in _HEX and len(self._line) < _MAX_SIZE_DIGITS:
                    self._line.append(b)
                else:
                    raise _framing("bad chunk size line")
            elif state == self._SIZE_LF:
                if b != _LF:
                    raise _framing("chunk size line not ended by CRLF")
                self._state = self._DATA if self._remaining else self._TRAILER
            elif state == self._DATA_CR:
                if b != _CR:
                    raise _framing("chunk data not followed by CRLF")
                self._state = self._DATA_LF
            elif state == self._DATA_LF:
                if b != _LF:
                    raise _framing("chunk data not followed by CRLF")
                self._state = self._SIZE
            elif state == self._TRAILER:
                if b == _CR:
                    if not self._line:
                        raise _framing("missing trailer")
                    self.trailer = bytes(self._line)
                    self._line.clear()
                    self._state = self._TRAILER_LF
                elif b == _LF or len(self._line) >= _MAX_TRAILER_LINE:
                    raise _framing("bad trailer line")
                else:
                    self._line.append(b)
            elif state == self._TRAILER_LF:
                if b != _LF:
                    raise _framing("trailer line not ended by CRLF")
                self._state = self._END_CR
            elif state == self._END_CR:
                if b != _CR:
                    raise _framing("more than one trailer line")
                self._state = self._END_LF
            elif state == self._END_LF:
                if b != _LF:
                    raise _framing("body not ended by CRLF")
                self._state = self._DONE
            else:  # _DONE
                raise _framing("bytes after the final CRLF")
        return out

    def trailer_checksum(self) -> bytes:
        """The 8 checksum bytes of the trailer line. The line must be
        x-amz-checksum-crc64nvme:<base64>, the name in any case."""
        name, sep, value = (self.trailer or b"").decode("latin-1").partition(":")
        if not sep:
            raise _framing("trailer line has no colon")
        if name.lower() != CHECKSUM_TRAILER:
            raise InvalidRequest(extra={"Reason": "trailer name does not match x-amz-trailer"})
        raw = decode_b64_digest(value)
        if raw is None:
            raise InvalidDigest(extra={"Header": CHECKSUM_TRAILER})
        return raw


async def write_framed_body(
    request: Request,
    storage: BlobStore,
    blob_id: str,
    framed: FramedRequest,
    mid_write_point: str | None,
) -> tuple[str, int]:
    """Decode request.stream() into the blob, writing each received buffer as it
    arrives. Returns (ETag, size) of the decoded bytes. Nothing is made durable unless
    the framing, both lengths and the checksum all check out; on any failure no file of
    the blob is left (BlobStore.open_writer)."""
    decoder = ChunkDecoder(framed.decoded_length)
    crc = Crc64Nvme()

    def verify(writer: BlobWriter) -> None:
        if not decoder.done:
            raise _framing("body ended inside the framing")
        if decoder.framed != framed.content_length:
            raise _framing("framed body does not match Content-Length")
        if decoder.decoded != framed.decoded_length:
            raise IncompleteBody(extra={"Reason": "decoded length differs from the header"})
        expected = decoder.trailer_checksum()
        if crc.digest() != expected:
            raise BadDigest(extra={"Header": CHECKSUM_TRAILER})

    with storage.open_writer(blob_id, mid_write_point=mid_write_point, verify=verify) as writer:
        async for received in request.stream():
            for piece in decoder.feed(received):
                writer.write(piece)
                crc.update(piece)
    return writer.etag, writer.size
