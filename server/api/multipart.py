import re
import uuid
import xml.etree.ElementTree as ET
from datetime import UTC, datetime

from fastapi import APIRouter, Request
from fastapi.responses import Response

from server.api.acl_headers import check_object_write
from server.api.aws_chunked import is_framed, preflight, write_framed_body
from server.api.checksum_headers import complete_claims, create_checksum, response_header
from server.api.conditions import write_precondition
from server.api.request_xml import parse_request_xml
from server.checksums import CRC64NVME, Algorithm, UploadChecks
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
from server.errors.s3errors import (
    InvalidArgument,
    InvalidPart,
    MalformedXML,
    NoSuchBucket,
    NoSuchUpload,
)
from server.metadata.base import BucketMissing, PartMeta, UploadNotOpen
from server.multipart.checksums import PART_ELEMENTS, ObjectChecksum
from server.multipart.state_machine import complete_multipart_upload
from server.storage.lifecycle import collect_garbage, discard_new_blob

router = APIRouter()


def _initiate_xml(bucket: str, key: str, upload_id: str) -> str:
    root = ET.Element("InitiateMultipartUploadResult")
    ET.SubElement(root, "Bucket").text = bucket
    ET.SubElement(root, "Key").text = key
    ET.SubElement(root, "UploadId").text = upload_id
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _complete_xml(bucket: str, key: str, etag: str, checksum: ObjectChecksum | None) -> str:
    root = ET.Element("CompleteMultipartUploadResult")
    ET.SubElement(root, "Bucket").text = bucket
    ET.SubElement(root, "Key").text = key
    ET.SubElement(root, "ETag").text = etag
    # S3 API reference, "CompleteMultipartUpload": Checksum<ALG> follows ETag, and
    # ChecksumType comes last.
    if checksum is not None:
        ET.SubElement(root, f"Checksum{checksum.algorithm}").text = checksum.value
        ET.SubElement(root, "ChecksumType").text = checksum.type
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


_OBJECT_SIZE_HEADER = "x-amz-mp-object-size"
_DIGITS = re.compile(r"[0-9]+")


def _expected_object_size(headers) -> int | None:
    """The x-amz-mp-object-size of a Complete, or None when the header is absent or
    blank. The value must be a plain non-negative decimal integer, digits only after
    stripping whitespace; anything else is InvalidArgument. The API reference documents
    only the mismatch (InvalidRequest), so the error for a malformed value is a choice.
    A value past _MAX_DIGITS significant digits is returned as 10**_MAX_DIGITS, which no
    upload's size can equal."""
    text = ", ".join(headers.getlist(_OBJECT_SIZE_HEADER)).strip()
    if not text:
        return None
    if _DIGITS.fullmatch(text) is None:
        raise InvalidArgument(extra={"ArgumentName": _OBJECT_SIZE_HEADER, "ArgumentValue": text})
    digits = text.lstrip("0")
    return 10**_MAX_DIGITS if len(digits) > _MAX_DIGITS else int(digits or "0")


def _part_checksums(element: ET.Element) -> dict[str, str]:
    """The non-blank Checksum* elements of a Part, by algorithm name, with the S3
    namespace or without. A blank element counts as absent."""
    found = {}
    for name in PART_ELEMENTS:
        text = element.findtext(f"{{{_S3_NS}}}Checksum{name}") or element.findtext(
            f"Checksum{name}"
        )
        if text is not None and text.strip():
            found[name] = text.strip()
    return found


def _parse_complete_body(body: bytes) -> list[dict]:
    """The Part elements of a CompleteMultipartUpload document, in document order, each
    with its PartNumber, ETag and Checksums (the non-blank Checksum* elements).

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
        {
            "PartNumber": _part_number(_child_text(el, "PartNumber")),
            "ETag": _child_text(el, "ETag"),
            "Checksums": _part_checksums(el),
        }
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
    """Proxy to objects router. ?uploadId is ListParts and ?tagging is GetObjectTagging; the
    guard has refused every other query alongside either."""
    if "uploadId" in request.query_params:
        from server.api.list_parts import list_parts

        return await list_parts(bucket, key, request.query_params["uploadId"], request)
    if "tagging" in request.query_params:
        from server.api.objects import _get_object_tagging

        return await _get_object_tagging(bucket, key, request)
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
    check_object_write(request.headers)
    # The same headers and rules as PutObject; refused before the upload is created.
    from server.api.objects import request_object_headers

    stored = request_object_headers(request.headers)
    # Refused before the upload is created; an upload that names no algorithm stores NULL
    # in both columns and echoes neither header.
    named = create_checksum(request.headers)
    algorithm, type_ = named or (None, None)
    upload_id = uuid.uuid4().hex
    try:
        metadata.create_upload(
            upload_id,
            bucket,
            key,
            datetime.now(UTC).replace(tzinfo=None),
            **stored,
            checksum_algorithm=algorithm,
            checksum_type=type_,
        )
    except BucketMissing:
        raise NoSuchBucket(extra={"BucketName": bucket}) from None
    return Response(
        content=_initiate_xml(bucket, key, upload_id),
        status_code=200,
        media_type="application/xml",
        headers={"x-amz-checksum-algorithm": algorithm, "x-amz-checksum-type": type_}
        if named
        else None,
    )


async def _upload_part(bucket: str, key: str, params: dict, request: Request) -> Response:
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    upload_id = params["uploadId"]
    try:
        part_number = int(params["partNumber"])
    except ValueError:
        raise InvalidPart()

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})
    # Decision 34: an upload of another bucket or key is an unknown upload.
    upload = metadata.get_open_upload(upload_id, bucket=bucket, key=key)
    if upload is None:
        raise NoSuchUpload(extra={"UploadId": upload_id})

    if not (1 <= part_number <= 10000):
        raise InvalidPart(extra={"PartNumber": str(part_number)})

    import io

    # The effective algorithm: the one the upload named, else CRC64NVME.
    named = upload.checksum_algorithm is not None
    effective = Algorithm.by_name(upload.checksum_algorithm) if named else CRC64NVME
    framed = preflight(request.headers) if is_framed(request.headers) else None
    checks = UploadChecks.from_headers(
        request.headers,
        trailer=framed.algorithm if framed else None,
        store_as=effective,
        named=named,
    )
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
    stored_algorithm, stored_checksum = checks.stored_checksum()
    part = PartMeta(
        upload_id=upload_id,
        part_number=part_number,
        etag=part_etag,
        size=size,
        blob_id=blob_id,
        checksum=stored_checksum,
        checksum_algorithm=stored_algorithm.name,
        bucket=bucket,
        key=key,
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
    # API_UploadPart: the checksum response headers are present only if the checksum was
    # provided in the request, so only the claim's header goes out, with its verified value.
    headers = {"ETag": part_etag}
    if claimed := checks.claimed_checksum():
        headers[response_header(claimed[0])] = claimed[1]
    return Response(status_code=200, headers=headers)


async def _complete(bucket: str, key: str, upload_id: str, request: Request) -> Response:
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})
    upload = metadata.get_open_upload(upload_id, bucket=bucket, key=key)
    if upload is None:
        raise NoSuchUpload(extra={"UploadId": upload_id})

    expected_size = _expected_object_size(request.headers)
    claims = complete_claims(request.headers, upload.checksum_algorithm)
    body = await request.body()
    requested_parts = _parse_complete_body(body)

    completed = complete_multipart_upload(
        bucket=bucket,
        key=key,
        upload_id=upload_id,
        requested_parts=requested_parts,
        upload=upload,
        claims=claims,
        storage=storage,
        metadata=metadata,
        # Decided in the commit, after every other Complete error (§10); no early check.
        precondition=write_precondition(request.headers, key),
        expected_size=expected_size,
        min_part_size=request.app.state.min_part_size,
    )
    return Response(
        content=_complete_xml(bucket, key, completed.etag, completed.checksum),
        status_code=200,
        media_type="application/xml",
    )


async def _abort(bucket: str, key: str, upload_id: str, request: Request) -> Response:
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})
    if not metadata.upload_exists(upload_id, bucket=bucket, key=key):
        raise NoSuchUpload(extra={"UploadId": upload_id})

    # docs/design/storage-layout.md §6.8: the rows go in one commit, the part blobs
    # after it.
    crash_point(ABORT_BEFORE_COMMIT)
    garbage = metadata.abort_upload(upload_id, bucket=bucket, key=key)
    if garbage is None:
        raise NoSuchUpload(extra={"UploadId": upload_id})
    crash_point(ABORT_AFTER_COMMIT)
    collect_garbage(storage, metadata, garbage)
    return Response(status_code=204)
