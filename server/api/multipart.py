import re
import uuid
import xml.etree.ElementTree as ET
from datetime import UTC, datetime

from fastapi import APIRouter, Request
from fastapi.responses import Response

from server.api.aws_chunked import is_framed, preflight, write_framed_body
from server.api.conditions import write_precondition
from server.api.request_xml import parse_request_xml
from server.checksums import CRC64NVME, UploadChecks
from server.crash_points import (
    ABORT_AFTER_COMMIT,
    ABORT_BEFORE_COMMIT,
    PART_AFTER_BYTES,
    PART_AFTER_COMMIT,
    PART_AFTER_REGISTER,
    PART_MID_WRITE,
    armed_point,
    crash_point,
)
from server.errors.s3errors import InvalidPart, MalformedXML, NoSuchBucket, NoSuchUpload
from server.metadata.base import BucketMissing, PartMeta, UploadNotOpen
from server.multipart.state_machine import complete_multipart_upload
from server.storage.lifecycle import collect_garbage, discard_new_blob

router = APIRouter()


def _initiate_xml(bucket: str, key: str, upload_id: str) -> str:
    root = ET.Element("InitiateMultipartUploadResult")
    ET.SubElement(root, "Bucket").text = bucket
    ET.SubElement(root, "Key").text = key
    ET.SubElement(root, "UploadId").text = upload_id
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _complete_xml(bucket: str, key: str, etag: str) -> str:
    root = ET.Element("CompleteMultipartUploadResult")
    ET.SubElement(root, "Bucket").text = bucket
    ET.SubElement(root, "Key").text = key
    ET.SubElement(root, "ETag").text = etag
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


_S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"
_INTEGER = re.compile(r"-?[0-9]+")
# Past this many digits the number is far outside 1 to 10000, and is not worth
# converting: int() refuses a string of more than 4300 digits.
_MAX_DIGITS = 18


def _child_text(element: ET.Element, tag: str) -> str:
    """The text of the child `tag`, with the S3 namespace or without; MalformedXML when
    the child is absent or blank."""
    text = element.findtext(f"{{{_S3_NS}}}{tag}") or element.findtext(tag)
    if text is None or not text.strip():
        raise MalformedXML()
    return text.strip()


def _part_number(text: str) -> int:
    """The integer in a PartNumber; MalformedXML when the text is not one. A value
    outside 1 to 10000 is returned as it is: it is InvalidPart, decided with the
    other part errors."""
    if _INTEGER.fullmatch(text) is None:
        raise MalformedXML()
    digits = text.lstrip("-")
    if len(digits) > _MAX_DIGITS:
        return -(10**_MAX_DIGITS) if text.startswith("-") else 10**_MAX_DIGITS
    return int(text)


def _parse_complete_body(body: bytes) -> list[dict]:
    """The Part elements of a CompleteMultipartUpload document, in document order.

    MalformedXML (400) for every body that is not one: empty, not UTF-8, not XML, a
    root other than CompleteMultipartUpload, no Part, or a Part without a PartNumber
    that is an integer or without an ETag. Nothing is sorted or checked against the
    upload here.
    """
    root = parse_request_xml(body)
    # boto3 sends the S3 namespace; handle both namespaced and plain.
    if root.tag not in ("CompleteMultipartUpload", f"{{{_S3_NS}}}CompleteMultipartUpload"):
        raise MalformedXML()
    parts = [
        {"PartNumber": _part_number(_child_text(el, "PartNumber")), "ETag": _child_text(el, "ETag")}
        for el in root
        if el.tag in ("Part", f"{{{_S3_NS}}}Part")
    ]
    if not parts:
        raise MalformedXML()
    return parts


@router.post("/{bucket}/{key:path}")
async def post_object(bucket: str, key: str, request: Request) -> Response:
    """Dispatch: ?uploads → initiate, ?uploadId → complete."""
    params = dict(request.query_params)

    if "uploads" in params:
        return await _initiate(bucket, key, request)
    if "uploadId" in params:
        return await _complete(bucket, key, params["uploadId"], request)

    return Response(status_code=400)


@router.delete("/{bucket}/{key:path}")
async def delete_object_or_abort(bucket: str, key: str, request: Request) -> Response:
    """Dispatch: ?uploadId → abort multipart, otherwise → delete object."""
    params = dict(request.query_params)

    if "uploadId" in params:
        return await _abort(bucket, key, params["uploadId"], request)

    # Plain object delete
    from server.api.objects import _delete_object

    return await _delete_object(bucket, key, request)


@router.put("/{bucket}/{key:path}")
async def put_object_or_part(bucket: str, key: str, request: Request) -> Response:
    """Dispatch: ?partNumber&uploadId → upload part, otherwise → put object."""
    params = dict(request.query_params)

    if "partNumber" in params and "uploadId" in params:
        # UploadPartCopy: the guard lets x-amz-copy-source through on this query only.
        if "x-amz-copy-source" in request.headers:
            from server.api.upload_part_copy import upload_part_copy

            return await upload_part_copy(bucket, key, params, request)
        return await _upload_part(bucket, key, params, request)

    # CopyObject: the guard lets x-amz-copy-source through on a PUT with no query only.
    if "x-amz-copy-source" in request.headers:
        from server.api.copy_object import copy_object

        return await copy_object(bucket, key, request)

    # Plain object put
    from server.api.objects import _put_object

    return await _put_object(bucket, key, request)


@router.get("/{bucket}/{key:path}")
async def get_object_proxy(bucket: str, key: str, request: Request) -> Response:
    """Proxy to objects router — multipart has no GET of its own."""
    from server.api.objects import _get_object

    return await _get_object(bucket, key, request)


@router.head("/{bucket}/{key:path}")
async def head_object_proxy(bucket: str, key: str, request: Request) -> Response:
    from server.api.objects import _head_object

    return await _head_object(bucket, key, request)


# --- private handlers ---


async def _initiate(bucket: str, key: str, request: Request) -> Response:
    metadata = request.app.state.metadata
    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})
    # The same headers and rules as PutObject; refused before the upload is created.
    from server.api.objects import request_object_headers

    stored = request_object_headers(request.headers)
    upload_id = uuid.uuid4().hex
    try:
        metadata.create_upload(
            upload_id, bucket, key, datetime.now(UTC).replace(tzinfo=None), **stored
        )
    except BucketMissing:
        raise NoSuchBucket(extra={"BucketName": bucket}) from None
    return Response(
        content=_initiate_xml(bucket, key, upload_id),
        status_code=200,
        media_type="application/xml",
    )


async def _upload_part(bucket: str, key: str, params: dict, request: Request) -> Response:
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    upload_id = params["uploadId"]
    try:
        part_number = int(params["partNumber"])
    except ValueError:
        raise InvalidPart()

    if not metadata.upload_exists(upload_id):
        raise NoSuchUpload(extra={"UploadId": upload_id})

    if not (1 <= part_number <= 10000):
        raise InvalidPart(extra={"PartNumber": str(part_number)})

    import io

    framed = preflight(request.headers) if is_framed(request.headers) else None
    checks = UploadChecks.from_headers(request.headers, trailer=CRC64NVME if framed else None)
    body = b"" if framed else await request.body()
    # docs/design/storage-layout.md §6.6: a fresh blob per attempt; the previous
    # part stays recorded and intact until the commit that replaces it.
    blob_id = metadata.register_blob(
        "part", bucket, key, upload_id=upload_id, part_number=part_number
    )
    crash_point(PART_AFTER_REGISTER)
    try:
        if framed:
            part_etag, size = await write_framed_body(
                request, storage, blob_id, framed, armed_point(PART_MID_WRITE), checks
            )
        else:
            checks.feed(body)
            part_etag, size = storage.write_blob(
                blob_id,
                io.BytesIO(body),
                mid_write_point=armed_point(PART_MID_WRITE),
                verify=checks.verify,
            )
    except BaseException:
        discard_new_blob(storage, metadata, blob_id)
        raise
    crash_point(PART_AFTER_BYTES)
    part = PartMeta(
        upload_id=upload_id, part_number=part_number, etag=part_etag, size=size, blob_id=blob_id
    )
    try:
        replaced = metadata.record_part(upload_id, part)
    except UploadNotOpen:
        discard_new_blob(storage, metadata, blob_id)
        raise NoSuchUpload(extra={"UploadId": upload_id}) from None
    except BaseException:
        discard_new_blob(storage, metadata, blob_id)
        raise
    crash_point(PART_AFTER_COMMIT)
    if replaced:
        collect_garbage(storage, metadata, [replaced])
    return Response(status_code=200, headers={"ETag": part_etag})


async def _complete(bucket: str, key: str, upload_id: str, request: Request) -> Response:
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    if not metadata.upload_exists(upload_id):
        raise NoSuchUpload(extra={"UploadId": upload_id})

    body = await request.body()
    requested_parts = _parse_complete_body(body)

    composite_etag = complete_multipart_upload(
        bucket=bucket,
        key=key,
        upload_id=upload_id,
        requested_parts=requested_parts,
        storage=storage,
        metadata=metadata,
        # Decided in the commit, after every other Complete error (§10); no early check.
        precondition=write_precondition(request.headers, key),
    )
    return Response(
        content=_complete_xml(bucket, key, composite_etag),
        status_code=200,
        media_type="application/xml",
    )


async def _abort(bucket: str, key: str, upload_id: str, request: Request) -> Response:
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    if not metadata.upload_exists(upload_id):
        raise NoSuchUpload(extra={"UploadId": upload_id})

    # docs/design/storage-layout.md §6.8: the rows go in one commit, the part blobs
    # after it.
    crash_point(ABORT_BEFORE_COMMIT)
    garbage = metadata.abort_upload(upload_id)
    if garbage is None:
        raise NoSuchUpload(extra={"UploadId": upload_id})
    crash_point(ABORT_AFTER_COMMIT)
    collect_garbage(storage, metadata, garbage)
    return Response(status_code=204)
