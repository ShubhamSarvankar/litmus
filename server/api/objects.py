import io
import os
from datetime import UTC, datetime

from fastapi import Request
from fastapi.responses import Response, StreamingResponse
from starlette.background import BackgroundTask

from server.api.aws_chunked import is_framed, preflight, write_framed_body
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
    NoSuchBucket,
    NoSuchKey,
    PreconditionFailed,
    SlowDown,
)
from server.metadata.base import BucketMissing, ObjectMeta
from server.storage.lifecycle import collect_garbage, discard_new_blob

_CHUNK = 65536
# GET re-reads the row when the blob it named was already replaced and unlinked;
# after this many retries in a row it answers 503 (docs/design/storage-layout.md §8).
_GET_RETRIES = 3


def _parse_range(range_header: str, total: int) -> tuple[int, int]:
    """Parse 'bytes=start-end' → (start, end) inclusive. Raises ValueError on bad input."""
    if not range_header.startswith("bytes="):
        raise ValueError("malformed Range header")
    spec = range_header[6:]
    if "," in spec:
        raise ValueError("multi-range not supported")
    if spec.startswith("-"):
        n = int(spec[1:])
        if n <= 0:
            raise ValueError("invalid suffix range")
        start = max(0, total - n)
        end = total - 1
    else:
        parts = spec.split("-", 1)
        start = int(parts[0])
        end = int(parts[1]) if parts[1] else total - 1
    if start > end or end >= total:
        raise ValueError("range out of bounds")
    return start, end


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


# --- shared handler implementations (called by multipart router too) ---


async def _put_object(bucket: str, key: str, request: Request) -> Response:
    """PutObject, new key or overwrite (docs/design/storage-layout.md §6.2)."""
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})

    # A framed body is decoded straight into the blob; the plain path buffers it whole.
    framed = preflight(request.headers) if is_framed(request.headers) else None
    body = b"" if framed else await request.body()
    content_type = request.headers.get("content-type", "binary/octet-stream")

    blob_id = metadata.register_blob("object", bucket, key)
    crash_point(PUT_AFTER_REGISTER)
    try:
        if framed:
            etag, size = await write_framed_body(
                request, storage, blob_id, framed, armed_point(PUT_MID_WRITE)
            )
        else:
            etag, size = storage.write_blob(
                blob_id, io.BytesIO(body), mid_write_point=armed_point(PUT_MID_WRITE)
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
        content_type=content_type,
        blob_id=blob_id,
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


def _open_current(metadata, storage, bucket: str, key: str, if_match: str | None):
    """Read the key's row and open the blob it names (§8 steps 1 to 3). Returns
    (row, open file); the headers must come from that row."""
    meta = metadata.get_object_meta(bucket, key)
    if meta is None:
        raise NoSuchKey(extra={"Key": key})
    retries = 0
    while True:
        if if_match and if_match != meta.etag:
            raise PreconditionFailed()
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

    meta, f = _open_current(metadata, storage, bucket, key, request.headers.get("if-match"))
    try:
        if os.fstat(f.fileno()).st_size != meta.size:
            # Never send a truncated or padded body under this row's ETag.
            raise InternalError()

        range_header = request.headers.get("range")
        if range_header:
            try:
                start, end = _parse_range(range_header, meta.size)
            except ValueError as e:
                f.close()
                if "multi-range" in str(e):
                    return Response(status_code=400, content="Multi-range not supported")
                return Response(
                    status_code=416,
                    headers={"Content-Range": f"bytes */{meta.size}"},
                )
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
                    "Content-Type": meta.content_type,
                },
                media_type=meta.content_type,
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
                "Content-Type": meta.content_type,
            },
            media_type=meta.content_type,
            background=BackgroundTask(f.close),
        )
    except BaseException:
        f.close()
        raise


async def _head_object(bucket: str, key: str, request: Request) -> Response:
    metadata = request.app.state.metadata

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})

    meta = metadata.get_object_meta(bucket, key)
    if meta is None:
        raise NoSuchKey(extra={"Key": key})

    return Response(
        status_code=200,
        headers={
            "Content-Length": str(meta.size),
            "ETag": meta.etag,
            "Last-Modified": _http_date(meta.last_modified),
            "Content-Type": meta.content_type,
            "Accept-Ranges": "bytes",
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
