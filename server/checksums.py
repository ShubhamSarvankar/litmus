"""Checksums a PutObject, UploadPart or DeleteObjects request claims for its body, and
their check.

A request can claim a checksum of the object bytes in three ways, all verified here
before the blob is made durable:

  Content-MD5                      base64 of the 16 byte MD5 (RFC 1864)
  x-amz-checksum-<algorithm>       base64 of the digest, for crc32, crc32c, crc64nvme,
                                   sha1 or sha256
  x-amz-trailer / the trailer      CRC64NVME in an aws-chunked body (server/api/aws_chunked.py)

Content-MD5 may accompany one checksum, and both are checked. x-amz-sdk-checksum-algorithm
names the algorithm the SDK used and must agree with the checksum that is sent.

The S3 API reference documents BadDigest for a value that does not match, and for an
x-amz-sdk-checksum-algorithm that differs from the checksum header. It documents "400 Bad
Request" for the SDK header without a matching checksum header or trailer, and no error at
all for the rest of the rules below; those are Litmus's own choices, listed in
docs/compatibility.md:

  a value that is not strict base64 of the right size   400 InvalidDigest
  the SDK header with no checksum header or trailer     400 InvalidRequest
  more than one x-amz-checksum-<algorithm> header, or one together with a trailer
                                                         400 InvalidRequest
  x-amz-checksum-<other algorithm>                       501 NotImplemented, before the
                                                         body is read (request_guard)

DeleteObjects takes the same claims over its XML body (UploadChecks.verify_body); none is
required, though the S3 API reference says Content-MD5 is (docs/compatibility.md).

Nothing is stored or returned yet.
"""

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from awscrt import checksums as crt
from starlette.datastructures import Headers

from server.crc64nvme import Crc64Nvme, decode_b64_digest
from server.errors.s3errors import (
    BadDigest,
    InvalidDigest,
    InvalidRequest,
    NotImplementedOperation,
)
from server.storage.blobs import BlobWriter

CONTENT_MD5_HEADER = "content-md5"
SDK_ALGORITHM_HEADER = "x-amz-sdk-checksum-algorithm"
VALUE_HEADER_PREFIX = "x-amz-checksum-"
# x-amz-checksum-mode, -algorithm and -type share the prefix but carry no checksum value.
_NOT_VALUE_HEADERS = frozenset({"mode", "algorithm", "type"})
_MD5_SIZE = 16


class Hasher(Protocol):
    def update(self, data: bytes) -> None: ...

    def digest(self) -> bytes: ...


class _Crc:
    """An awscrt CRC32 or CRC32C, whose wire form is the 4 byte big endian value."""

    def __init__(self, func: Callable[[bytes, int], int]) -> None:
        self._func = func
        self._crc = 0

    def update(self, data: bytes) -> None:
        self._crc = self._func(data, self._crc)

    def digest(self) -> bytes:
        return self._crc.to_bytes(4, "big")


@dataclass(frozen=True)
class Algorithm:
    name: str  # as x-amz-sdk-checksum-algorithm spells it
    size: int  # digest bytes
    new: Callable[[], Hasher]

    @property
    def header(self) -> str:
        return VALUE_HEADER_PREFIX + self.name.lower()


ALGORITHMS: dict[str, Algorithm] = {
    a.name.lower(): a
    for a in (
        Algorithm("CRC32", 4, lambda: _Crc(crt.crc32)),
        Algorithm("CRC32C", 4, lambda: _Crc(crt.crc32c)),
        Algorithm("CRC64NVME", 8, Crc64Nvme),
        Algorithm("SHA1", 20, hashlib.sha1),
        Algorithm("SHA256", 32, hashlib.sha256),
    )
}
CRC64NVME = ALGORITHMS["crc64nvme"]


def value_headers(headers: Headers) -> list[str]:
    """The names, lower case and once per occurrence, of the x-amz-checksum-<name>
    headers that carry a checksum value."""
    return [
        name
        for name in map(str.lower, headers.keys())
        if name.startswith(VALUE_HEADER_PREFIX)
        and name[len(VALUE_HEADER_PREFIX) :] not in _NOT_VALUE_HEADERS
    ]


def unsupported_value_header(headers: Headers) -> str | None:
    """The first checksum value header naming an algorithm Litmus does not check."""
    for name in value_headers(headers):
        if name[len(VALUE_HEADER_PREFIX) :] not in ALGORITHMS:
            return name
    return None


class UploadChecks:
    """The claims of one request and their verification. feed() takes the object bytes
    as they are written; verify() is the BlobStore verify callback and runs before the
    blob is made durable."""

    def __init__(
        self,
        algorithm: Algorithm | None,
        expected: bytes | None,
        content_md5: bytes | None,
    ) -> None:
        self._algorithm = algorithm
        self._expected = expected  # None: the value arrives later, in the trailer
        self._content_md5 = content_md5
        self._hasher = algorithm.new() if algorithm else None

    @classmethod
    def from_headers(cls, headers: Headers, trailer: Algorithm | None = None) -> "UploadChecks":
        """Read and check the claims in the headers; raises InvalidRequest,
        InvalidDigest or BadDigest (the SDK header disagreeing with the checksum), and
        NotImplementedOperation for a checksum header of another algorithm.
        trailer is the algorithm of the aws-chunked trailer, if the body has one."""
        if unsupported := unsupported_value_header(headers):
            # The request guard answers this first; kept so no caller can reach the
            # lookup below with an algorithm Litmus does not check.
            raise NotImplementedOperation(extra={"Header": unsupported})
        names = value_headers(headers)
        if len(names) > 1 or (names and trailer):
            raise InvalidRequest(extra={"Header": names[-1]})
        algorithm, expected = trailer, None
        if names:
            algorithm = ALGORITHMS[names[0][len(VALUE_HEADER_PREFIX) :]]
        sdk = headers.getlist(SDK_ALGORITHM_HEADER)
        if len(sdk) > 1 or (sdk and algorithm is None):
            raise InvalidRequest(extra={"Header": SDK_ALGORITHM_HEADER})
        if sdk and sdk[0].strip().upper() != algorithm.name:
            raise BadDigest(extra={"Header": SDK_ALGORITHM_HEADER})
        if names:
            expected = decode_b64_digest(headers[names[0]], algorithm.size)
            if expected is None:
                raise InvalidDigest(extra={"Header": names[0]})
        content_md5 = None
        if CONTENT_MD5_HEADER in headers:
            # Repeated headers read as one comma separated value, which is not base64.
            value = ", ".join(headers.getlist(CONTENT_MD5_HEADER))
            content_md5 = decode_b64_digest(value, _MD5_SIZE)
            if content_md5 is None:
                raise InvalidDigest(extra={"Header": "Content-MD5"})
        return cls(algorithm, expected, content_md5)

    def feed(self, data: bytes) -> None:
        if self._hasher is not None:
            self._hasher.update(data)

    def verify(self, writer: BlobWriter, trailer_value: bytes | None = None) -> None:
        """BadDigest unless what was written matches every claim. trailer_value is the
        decoded trailer, for a body that has one."""
        self._check(writer.md5_digest, trailer_value)

    def verify_body(self, body: bytes) -> None:
        """feed() and verify() for a request body held whole in memory."""
        self.feed(body)
        self._check(hashlib.md5(body).digest(), None)

    def _check(self, md5_digest: bytes, trailer_value: bytes | None) -> None:
        if self._content_md5 is not None and md5_digest != self._content_md5:
            raise BadDigest(extra={"Header": "Content-MD5"})
        if self._hasher is not None:
            expected = self._expected if self._expected is not None else trailer_value
            if self._hasher.digest() != expected:
                raise BadDigest(extra={"Header": self._algorithm.header})
