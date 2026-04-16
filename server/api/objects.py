import io
from datetime import UTC, datetime

from fastapi import APIRouter, Request
from fastapi.responses import Response, StreamingResponse

from server.errors.s3errors import InternalError, NoSuchBucket, NoSuchKey, PreconditionFailed
from server.metadata.base import ObjectMeta

router = APIRouter()

_CHUNK = 65536


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


def _stream_range(path, start: int, end: int):
    with open(path, "rb") as f:
        f.seek(start)
        remaining = end - start + 1
        while remaining > 0:
            chunk = f.read(min(_CHUNK, remaining))
            if not chunk:
                break
            remaining -= len(chunk)
            yield chunk


def _http_date(dt: datetime) -> str:
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")


# --- shared handler implementations (called by multipart router too) ---


async def _put_object(bucket: str, key: str, request: Request) -> Response:
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})

    body = await request.body()
    content_type = request.headers.get("content-type", "binary/octet-stream")
    etag = storage.write_object(bucket, key, io.BytesIO(body), len(body))
    metadata.put_object_meta(
        bucket,
        key,
        ObjectMeta(
            bucket=bucket,
            key=key,
            size=len(body),
            etag=etag,
            last_modified=datetime.now(UTC).replace(tzinfo=None),
            content_type=content_type,
        ),
    )
    return Response(status_code=200, headers={"ETag": etag})


async def _get_object(bucket: str, key: str, request: Request) -> Response:
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})

    meta = metadata.get_object_meta(bucket, key)
    if meta is None:
        raise NoSuchKey(extra={"Key": key})

    if_match = request.headers.get("if-match")
    if if_match and if_match != meta.etag:
        raise PreconditionFailed()

    if not storage.object_exists(bucket, key):
        raise InternalError()

    range_header = request.headers.get("range")

    if range_header:
        try:
            start, end = _parse_range(range_header, meta.size)
        except ValueError as e:
            if "multi-range" in str(e):
                return Response(status_code=400, content="Multi-range not supported")
            return Response(
                status_code=416,
                headers={"Content-Range": f"bytes */{meta.size}"},
            )

        length = end - start + 1
        obj_path = storage._object_path(bucket, key)
        return StreamingResponse(
            _stream_range(obj_path, start, end),
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
        )

    def _stream_full():
        with storage.read_object(bucket, key) as f:
            while True:
                chunk = f.read(_CHUNK)
                if not chunk:
                    break
                yield chunk

    return StreamingResponse(
        _stream_full(),
        status_code=200,
        headers={
            "Content-Length": str(meta.size),
            "Accept-Ranges": "bytes",
            "ETag": meta.etag,
            "Last-Modified": _http_date(meta.last_modified),
            "Content-Type": meta.content_type,
        },
        media_type=meta.content_type,
    )


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
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})

    storage.delete_object(bucket, key)
    metadata.delete_object_meta(bucket, key)
    return Response(status_code=204)


# --- router entry points (thin wrappers) ---


@router.put("/{bucket}/{key:path}")
async def put_object(bucket: str, key: str, request: Request) -> Response:
    return await _put_object(bucket, key, request)


@router.get("/{bucket}/{key:path}")
async def get_object(bucket: str, key: str, request: Request) -> Response:
    return await _get_object(bucket, key, request)


@router.head("/{bucket}/{key:path}")
async def head_object(bucket: str, key: str, request: Request) -> Response:
    return await _head_object(bucket, key, request)


@router.delete("/{bucket}/{key:path}")
async def delete_object(bucket: str, key: str, request: Request) -> Response:
    return await _delete_object(bucket, key, request)
