"""The x-amz-copy-source header of CopyObject (S3 API reference, CopyObject).

The value is bucket/key, URL-encoded, with an optional leading slash, and may end in
?versionId=<id>. The "?" that starts the version is the first one in the raw value: a
"?" in a key arrives as %3F. The bucket and the key are each percent-decoded once
(never "+" as a space), so a key holding "%25" names a key with a "%", not "%25".

Litmus has no versioning, so versionId=null names the current object (as in
request_guard.NULL_VERSION_ID) and any other version ID, like an access point or
Outposts ARN source, is refused with 501. The reference gives no error code for a
malformed value; it gets 400 InvalidArgument, naming the header.
"""

from dataclasses import dataclass
from urllib.parse import unquote_to_bytes

from server.errors.s3errors import InvalidArgument, NotImplementedOperation

COPY_SOURCE_HEADER = "x-amz-copy-source"
NULL_VERSION_ID = "null"
_ARN_PREFIX = "arn:"


@dataclass(frozen=True)
class CopySource:
    bucket: str
    key: str


def _malformed() -> InvalidArgument:
    return InvalidArgument(extra={"ArgumentName": COPY_SOURCE_HEADER})


def _unsupported() -> NotImplementedOperation:
    return NotImplementedOperation(extra={"Header": COPY_SOURCE_HEADER})


def _decode(part: str) -> str:
    """Percent-decode once. The header arrives as ISO-8859-1 text, so a client that sent
    raw UTF-8 bytes instead of escapes is read back from those bytes."""
    try:
        return unquote_to_bytes(part.encode("latin-1")).decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        raise _malformed() from None


def parse_copy_source(value: str) -> CopySource:
    """The source bucket and key. Raises NotImplementedOperation for an ARN or a version
    ID other than "null", InvalidArgument for anything that is not bucket/key."""
    path, mark, query = value.strip().partition("?")
    path = path.removeprefix("/")
    # An ARN is sent URL-encoded as a whole, colons included.
    if _decode(path).lower().startswith(_ARN_PREFIX):
        raise _unsupported()
    if mark:
        name, eq, version = query.partition("=")
        if name != "versionId" or not eq or "&" in version:
            raise _malformed()
        if _decode(version) != NULL_VERSION_ID:
            raise _unsupported()
    bucket, slash, key = path.partition("/")
    if not slash or not bucket or not key:
        raise _malformed()
    return CopySource(_decode(bucket), _decode(key))
