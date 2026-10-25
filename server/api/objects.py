import io
import os
import re
from datetime import UTC, datetime

from fastapi import Request
from fastapi.responses import Response, StreamingResponse
from starlette.background import BackgroundTask

from server.api.aws_chunked import is_framed, preflight, write_framed_body
from server.api.conditions import evaluate_read_conditions
from server.api.user_metadata import parse_user_metadata, response_headers
from server.checksums import CRC64NVME, UploadChecks
from server.crash_points import (
    DELETE_AFTER_COMMIT,
    DELETE_BEFORE_COMMIT,
    PUT_AFTER_BYTES,
    PUT_AFTER_COMMIT,
    PUT_AFTER_GC_UNLINK,
    PUT_AFTER_REGISTER,
    PUT_MID_WRITE,
    armed_point,
    crash_point,
)
from server.errors.s3errors import (
    InternalError,
    InvalidArgument,
    InvalidRange,
    NoSuchBucket,
    NoSuchKey,
    SlowDown,
)
from server.metadata.base import BucketMissing, ObjectMeta
from server.storage.lifecycle import collect_garbage, discard_new_blob

_CHUNK = 65536
# GET re-reads the row when the blob it named was already replaced and unlinked;
# after this many retries in a row it answers 503 (docs/design/storage-layout.md §8).
_GET_RETRIES = 3


_BYTE_RANGE = re.compile(r"(?:(\d+)-(\d*)|-(\d+))", re.ASCII)
# A numeral this long is not a client asking for bytes; it is read as an invalid
# specifier, which is ignored, rather than handed to int() (RFC 9110 14.1.2).
_MAX_DIGITS = 4000


def _resolve_range(range_header: str, total: int) -> tuple[int, int] | None:
    """Resolve a Range header against an object of total bytes (RFC 9110 14.1.2, 14.2).

    Returns the inclusive (start, end) to send as a 206, with end clamped to the last
    byte, or None when the header is to be ignored and the whole object served: a unit
    other than bytes, a set of ranges, a specifier that is not valid, and a suffix range
    on an empty object. Raises InvalidRange when the one range is valid and no byte of
    the object satisfies it."""
    unit, eq, spec = range_header.partition("=")
    if not eq or unit.lower() != "bytes" or "," in spec:
        return None
    m = _BYTE_RANGE.fullmatch(spec)
    if m is None or any(g is not None and len(g) > _MAX_DIGITS for g in m.groups()):
        return None
    unsatisfied = InvalidRange(headers={"Content-Range": f"bytes */{total}"})
    if m.group(3) is not None:  # suffix-range: the last N bytes
        n = int(m.group(3))
        if n == 0:
            raise unsatisfied
        if total == 0:
            return None
        return max(0, total - n), total - 1
    first = int(m.group(1))
    last = int(m.group(2)) if m.group(2) else None
    if last is not None and last < first:
        return None
    if first >= total:
        raise unsatisfied
    return first, total - 1 if last is None else min(last, total - 1)


def _stream(f, start: int, length: int):
    """Stream length bytes from offset start of an open blob, closing it at the end
    (or when the client goes away)."""
    try:
        f.seek(start)
        remaining = length
        while remaining > 0:
            chunk = f.read(min(_CHUNK, remaining))
            if not chunk:
                break
            remaining -= len(chunk)
            yield chunk
    finally:
        f.close()


def _http_date(dt: datetime) -> str:
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")


_AWS_CHUNKED = "aws-chunked"


def _header_value(headers, name: str) -> str | None:
    """The request header as RFC 9110 5.3 reads repeated fields, joined with ", ";
    None when it is absent or blank, so a blank value is stored as no value."""
    values = [v.strip() for v in headers.getlist(name) if v.strip()]
    return ", ".join(values) or None


def _stored_content_encoding(headers) -> str | None:
    """Content-Encoding without its aws-chunked tokens, which name the request's
    framing, not an encoding of the object; None when nothing else is left."""
    tokens = [t.strip() for v in headers.getlist("content-encoding") for t in v.split(",")]
    return ", ".join(t for t in tokens if t and t.lower() != _AWS_CHUNKED) or None


# GetObject and HeadObject: the query parameters that set a response header for one
# request (S3 API reference, GetObject "Overriding response header values").
_RESPONSE_OVERRIDES = {
    "response-cache-control": "Cache-Control",
    "response-content-disposition": "Content-Disposition",
    "response-content-encoding": "Content-Encoding",
    "response-content-language": "Content-Language",
    "response-content-type": "Content-Type",
    "response-expires": "Expires",
}


def _response_overrides(request: Request) -> dict[str, str]:
    """The response headers this request overrides: header name to the value to send.

    A repeated parameter reads as its last value, as every other query parameter does
    here, and an empty value is no override. A value with a control character other
    than a tab (RFC 9110 5.5: a field value must not hold CR, LF or NUL) is refused with
    400 InvalidArgument, which names the parameter and not the value. Anything else is
    sent as the UTF-8 bytes of the decoded value. Parameters not listed are ignored."""
    params = dict(request.query_params)
    overrides = {}
    for param, header in _RESPONSE_OVERRIDES.items():
        value = params.get(param)
        if not value:
            continue
        if any((c < " " and c != "\t") or c == "\x7f" for c in value):
            raise InvalidArgument(extra={"ArgumentName": param})
        # Starlette writes header values as ISO-8859-1: pass the UTF-8 bytes through it.
        overrides[header] = value.encode("utf-8").decode("latin-1")
    return overrides


def request_object_headers(headers) -> dict:
    """What PutObject and CreateMultipartUpload store with an object, from a request's
    headers: Content-Type, the system headers and the user metadata, keyed by the
    ObjectMeta and UploadMeta field names. Raises MetadataTooLarge."""
    return {
        "content_type": headers.get("content-type", "binary/octet-stream"),
        "cache_control": _header_value(headers, "cache-control"),
        "content_disposition": _header_value(headers, "content-disposition"),
        "content_encoding": _stored_content_encoding(headers),
        "content_language": _header_value(headers, "content-language"),
        "expires": _header_value(headers, "expires"),
        "user_metadata": parse_user_metadata(headers),
    }


def _stored_headers(meta: ObjectMeta) -> dict[str, str]:
    """The stored system headers and user metadata GET and HEAD return, besides
    Content-Type."""
    pairs = (
        ("Cache-Control", meta.cache_control),
        ("Content-Disposition", meta.content_disposition),
        ("Content-Encoding", meta.content_encoding),
        ("Content-Language", meta.content_language),
        ("Expires", meta.expires),
    )
    stored = {name: value for name, value in pairs if value is not None}
    return stored | response_headers(meta.user_metadata)


# --- shared handler implementations (called by multipart router too) ---


async def _put_object(bucket: str, key: str, request: Request) -> Response:
    """PutObject, new key or overwrite (docs/design/storage-layout.md §6.2)."""
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})

    # Refused before the body is read and before any blob is registered.
    stored = request_object_headers(request.headers)

    # A framed body is decoded straight into the blob; the plain path buffers it whole.
    framed = preflight(request.headers) if is_framed(request.headers) else None
    checks = UploadChecks.from_headers(request.headers, trailer=CRC64NVME if framed else None)
    body = b"" if framed else await request.body()

    blob_id = metadata.register_blob("object", bucket, key)
    crash_point(PUT_AFTER_REGISTER)
    try:
        if framed:
            etag, size = await write_framed_body(
                request, storage, blob_id, framed, armed_point(PUT_MID_WRITE), checks
            )
        else:
            checks.feed(body)
            etag, size = storage.write_blob(
                blob_id,
                io.BytesIO(body),
                mid_write_point=armed_point(PUT_MID_WRITE),
                verify=checks.verify,
            )
    except BaseException:
        discard_new_blob(storage, metadata, blob_id)
        raise
    crash_point(PUT_AFTER_BYTES)

    meta = ObjectMeta(
        bucket=bucket,
        key=key,
        size=size,
        etag=etag,
        last_modified=datetime.now(UTC).replace(tzinfo=None),
        blob_id=blob_id,
        **stored,
    )
    try:
        replaced = metadata.put_object_meta(bucket, key, meta)
    except BucketMissing:
        discard_new_blob(storage, metadata, blob_id)
        raise NoSuchBucket(extra={"BucketName": bucket}) from None
    except BaseException:
        discard_new_blob(storage, metadata, blob_id)
        raise
    crash_point(PUT_AFTER_COMMIT)
    if replaced:
        collect_garbage(storage, metadata, [replaced], after_unlink_point=PUT_AFTER_GC_UNLINK)
    return Response(status_code=200, headers={"ETag": etag})


def _not_modified(meta: ObjectMeta) -> Response:
    """304 for GET and HEAD (RFC 9110 15.4.5): the ETag and, when stored, Cache-Control
    and Expires, which a 200 would carry, and no body. Last-Modified is left out: the
    RFC asks for no other representation metadata, and the ETag is present."""
    headers = {"ETag": meta.etag}
    if meta.cache_control is not None:
        headers["Cache-Control"] = meta.cache_control
    if meta.expires is not None:
        headers["Expires"] = meta.expires
    return Response(status_code=304, headers=headers)


def _open_current(metadata, storage, bucket: str, key: str, request_headers):
    """Read the key's row, evaluate the request's conditions against it, and open the
    blob it names (§8 steps 1 to 3). Returns (row, open file), or (row, None) when the
    conditions make the answer 304; the headers must come from that row. Raises
    PreconditionFailed. The conditions are evaluated again on every re-read row."""
    meta = metadata.get_object_meta(bucket, key)
    if meta is None:
        raise NoSuchKey(extra={"Key": key})
    retries = 0
    while True:
        if evaluate_read_conditions(request_headers, meta.etag, meta.last_modified):
            return meta, None
        try:
            return meta, storage.open_blob(meta.blob_id)
        except FileNotFoundError:
            # Unlinked only after a later commit replaced or deleted the key (I4),
            # unless the bytes are lost.
            again = metadata.get_object_meta(bucket, key)
            if again is None:
                raise NoSuchKey(extra={"Key": key}) from None
            if again.blob_id == meta.blob_id:
                raise InternalError() from None
            retries += 1
            if retries > _GET_RETRIES:
                raise SlowDown() from None
            meta = again


async def _get_object(bucket: str, key: str, request: Request) -> Response:
    """GetObject (docs/design/storage-layout.md §8)."""
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})

    overrides = _response_overrides(request)
    meta, f = _open_current(metadata, storage, bucket, key, request.headers)
    if f is None:
        return _not_modified(meta)
    try:
        if os.fstat(f.fileno()).st_size != meta.size:
            # Never send a truncated or padded body under this row's ETag.
            raise InternalError()

        # The stored headers, with this request's overrides over them; never stored.
        headers = {"Content-Type": meta.content_type, **_stored_headers(meta), **overrides}
        range_header = request.headers.get("range")
        resolved = _resolve_range(range_header, meta.size) if range_header else None
        if resolved is not None:
            start, end = resolved
            length = end - start + 1
            return StreamingResponse(
                _stream(f, start, length),
                status_code=206,
                headers={
                    "Content-Range": f"bytes {start}-{end}/{meta.size}",
                    "Content-Length": str(length),
                    "Accept-Ranges": "bytes",
                    "ETag": meta.etag,
                    "Last-Modified": _http_date(meta.last_modified),
                    **headers,
                },
                media_type=headers["Content-Type"],
                background=BackgroundTask(f.close),
            )

        return StreamingResponse(
            _stream(f, 0, meta.size),
            status_code=200,
            headers={
                "Content-Length": str(meta.size),
                "Accept-Ranges": "bytes",
                "ETag": meta.etag,
                "Last-Modified": _http_date(meta.last_modified),
                **headers,
            },
            media_type=headers["Content-Type"],
            background=BackgroundTask(f.close),
        )
    except BaseException:
        f.close()
        raise


async def _head_object(bucket: str, key: str, request: Request) -> Response:
    metadata = request.app.state.metadata

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})

    overrides = _response_overrides(request)
    meta = metadata.get_object_meta(bucket, key)
    if meta is None:
        raise NoSuchKey(extra={"Key": key})

    if evaluate_read_conditions(request.headers, meta.etag, meta.last_modified):
        return _not_modified(meta)

    range_header = request.headers.get("range")
    if range_header:
        # Raises InvalidRange for a range that GET would refuse. A range GET would
        # serve is ignored here, as before: HEAD does not narrow Content-Length.
        _resolve_range(range_header, meta.size)

    return Response(
        status_code=200,
        headers={
            "Content-Length": str(meta.size),
            "ETag": meta.etag,
            "Last-Modified": _http_date(meta.last_modified),
            "Content-Type": meta.content_type,
            "Accept-Ranges": "bytes",
            **_stored_headers(meta),
            **overrides,
        },
    )


async def _delete_object(bucket: str, key: str, request: Request) -> Response:
    """DeleteObject (docs/design/storage-layout.md §6.3): the row goes in one
    commit, the blob file after it."""
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})

    crash_point(DELETE_BEFORE_COMMIT)
    garbage = metadata.delete_object_meta(bucket, key)
    crash_point(DELETE_AFTER_COMMIT)
    collect_garbage(storage, metadata, garbage)
    return Response(status_code=204)
