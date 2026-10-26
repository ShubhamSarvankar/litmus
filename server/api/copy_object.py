"""CopyObject (docs/design/storage-layout.md §6.10, §11): PutObject whose bytes come from
another object's blob.

The source row is read once, its x-amz-copy-source-if-* conditions are decided against
it and its blob is opened (GetObject's sequence, §8), so the headers, the conditions and
the bytes all belong to one row. The bytes are then written to a new blob through the
durable write sequence, with the source's size and, for a single part source, its MD5
checked before they are made durable, and the destination row is committed like
PutObject's (§6.2 P4), with the destination's If-Match and If-None-Match.
"""

import logging
import os
import xml.etree.ElementTree as ET

from fastapi import Request
from fastapi.responses import Response

from server.api.conditions import CopySourceConditions, write_precondition
from server.api.copy_source import COPY_SOURCE_HEADER, parse_copy_source
from server.api.objects import _CHUNK, _open_current, request_object_headers
from server.crash_points import (
    COPY_AFTER_BYTES,
    COPY_AFTER_COMMIT,
    COPY_AFTER_GC_UNLINK,
    COPY_AFTER_REGISTER,
    COPY_MID_WRITE,
    armed_point,
    crash_point,
)
from server.errors.s3errors import (
    CopyOntoItself,
    InternalError,
    InvalidArgument,
    NoSuchBucket,
    NoSuchKey,
    PreconditionFailed,
)
from server.metadata.base import BucketMissing, ObjectMeta
from server.storage.lifecycle import collect_garbage, discard_new_blob

logger = logging.getLogger(__name__)

_METADATA_DIRECTIVE = "x-amz-metadata-directive"
_TAGGING_DIRECTIVE = "x-amz-tagging-directive"
_COPY = "COPY"
_REPLACE = "REPLACE"


def _directive(headers, name: str) -> str:
    """COPY (also when absent) or REPLACE; anything else is 400 InvalidArgument."""
    values = headers.getlist(name)
    if not values:
        return _COPY
    if len(values) != 1 or values[0] not in (_COPY, _REPLACE):
        raise InvalidArgument(extra={"ArgumentName": name})
    return values[0]


def _stored_headers(source: ObjectMeta) -> dict:
    """What a COPY keeps of the source: Content-Type, the system headers and the user
    metadata, keyed like request_object_headers()."""
    return {
        "content_type": source.content_type,
        "cache_control": source.cache_control,
        "content_disposition": source.content_disposition,
        "content_encoding": source.content_encoding,
        "content_language": source.content_language,
        "expires": source.expires,
        "user_metadata": dict(source.user_metadata),
    }


def _result_xml(etag: str, last_modified) -> str:
    root = ET.Element("CopyObjectResult")
    ET.SubElement(root, "ETag").text = etag
    ET.SubElement(root, "LastModified").text = last_modified.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


async def copy_object(bucket: str, key: str, request: Request) -> Response:
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})

    values = request.headers.getlist(COPY_SOURCE_HEADER)
    if len(values) != 1:
        raise InvalidArgument(extra={"ArgumentName": COPY_SOURCE_HEADER})
    source = parse_copy_source(values[0])
    directive = _directive(request.headers, _METADATA_DIRECTIVE)
    _directive(request.headers, _TAGGING_DIRECTIVE)
    # Under REPLACE the destination's headers are the request's, refused before any blob
    # is registered; under COPY the request's are ignored.
    replaced_headers = request_object_headers(request.headers) if directive == _REPLACE else None

    if not metadata.bucket_exists(source.bucket):
        raise NoSuchBucket(extra={"BucketName": source.bucket})
    if metadata.get_object_meta(source.bucket, source.key) is None:
        raise NoSuchKey(extra={"Key": source.key})
    # Decided before the source conditions: a copy that changes nothing is refused
    # whatever they say. Storage class and encryption headers are not honored, so
    # REPLACE is the only way to change something.
    if (source.bucket, source.key) == (bucket, key) and directive != _REPLACE:
        raise CopyOntoItself()

    src, f = _open_current(
        metadata, storage, source.bucket, source.key, CopySourceConditions(request.headers)
    )
    if f is None:
        # A condition that failed on its own: evaluate_read_conditions answers "not
        # modified" for If-None-Match and If-Modified-Since, which a copy reports as 412.
        raise PreconditionFailed()

    with f:
        if os.fstat(f.fileno()).st_size != src.size:
            # Never copy a truncated or padded blob under this row's ETag.
            raise InternalError()

        def verify(writer) -> None:
            # Before the bytes are made durable. A multipart source's ETag is not the MD5
            # of its bytes, so only its size is checked.
            if writer.size != src.size or (src.upload_id is None and writer.etag != src.etag):
                logger.error(
                    "copy of %s/%s: blob %s read back as %d bytes %s, row says %d bytes %s",
                    source.bucket,
                    source.key,
                    src.blob_id,
                    writer.size,
                    writer.etag,
                    src.size,
                    src.etag,
                )
                raise InternalError()

        blob_id = metadata.register_blob("object", bucket, key)
        crash_point(COPY_AFTER_REGISTER)
        try:
            with storage.open_writer(
                blob_id, mid_write_point=armed_point(COPY_MID_WRITE), verify=verify
            ) as writer:
                while chunk := f.read(_CHUNK):
                    writer.write(chunk)
        except BaseException:
            discard_new_blob(storage, metadata, blob_id)
            raise
    crash_point(COPY_AFTER_BYTES)

    meta = ObjectMeta(
        bucket=bucket,
        key=key,
        size=writer.size,
        etag=writer.etag,
        last_modified=None,  # stamped inside the commit (§9)
        blob_id=blob_id,
        **(replaced_headers if replaced_headers is not None else _stored_headers(src)),
    )
    precondition = write_precondition(request.headers, key)
    try:
        replaced = metadata.put_object_meta(bucket, key, meta, precondition=precondition)
    except BucketMissing:
        discard_new_blob(storage, metadata, blob_id)
        raise NoSuchBucket(extra={"BucketName": bucket}) from None
    except BaseException:
        discard_new_blob(storage, metadata, blob_id)
        raise
    crash_point(COPY_AFTER_COMMIT)
    if replaced:
        collect_garbage(storage, metadata, [replaced], after_unlink_point=COPY_AFTER_GC_UNLINK)
    return Response(
        content=_result_xml(meta.etag, meta.last_modified),
        status_code=200,
        media_type="application/xml",
    )
