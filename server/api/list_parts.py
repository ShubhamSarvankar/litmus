"""ListParts: GET /{bucket}/{key}?uploadId= (docs/design/storage-layout.md §21, decision 36).

A read: it changes no row and no file."""

import xml.etree.ElementTree as ET

from fastapi import Request
from fastapi.responses import Response

from server.config import settings
from server.errors.s3errors import InvalidArgument, NoSuchBucket, NoSuchUpload
from server.metadata.base import PartMeta, PartsPage
from server.numerals import parse_decimal

# The API reference: "The ListParts request returns a maximum of 1,000 uploaded parts. The
# limit of 1,000 parts is also the default value."
MAX_PARTS = 1000

# The query parameters ListParts takes besides uploadId. "x-id" is the SDKs' operation
# name hint and never selects anything (server/api/request_guard.py).
QUERY_PARAMETERS = frozenset({"uploadId", "max-parts", "part-number-marker"})


def _decimal(params, name: str) -> tuple[int, str] | None:
    """A decimal integer parameter as (value, its digits without leading zeros), None when
    it is absent. The value is parse_decimal's, so a long one is SATURATED; the digits are
    what the response echoes. Anything but ASCII digits (a sign, a space, a fraction) is
    InvalidArgument."""
    raw = params.get(name)
    if raw is None:
        return None
    value = parse_decimal(raw)
    if value is None:
        raise InvalidArgument(extra={"ArgumentName": name, "ArgumentValue": raw})
    return value, raw.lstrip("0") or "0"


def _max_parts(params) -> int:
    """max-parts as _max_keys reads max-keys: default 1000, larger values capped at 1000,
    0 allowed, anything but a decimal integer InvalidArgument."""
    parsed = _decimal(params, "max-parts")
    return MAX_PARTS if parsed is None else min(parsed[0], MAX_PARTS)


def _part_number_marker(params) -> tuple[int, str]:
    """part-number-marker as (value, digits to echo): absent is 0, anything but a decimal
    integer is InvalidArgument. A marker above every part number is accepted and lists no
    parts."""
    return _decimal(params, "part-number-marker") or (0, "0")


def _timestamp(part: PartMeta) -> str:
    # An open part always has its time (v3 migration backfills it), so no NULL is handled.
    return part.last_modified.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _identity(parent: ET.Element, tag: str) -> None:
    element = ET.SubElement(parent, tag)
    ET.SubElement(element, "ID").text = settings.owner_id
    ET.SubElement(element, "DisplayName").text = settings.owner_display_name


def _list_parts_xml(
    bucket: str, key: str, upload_id: str, marker: str, max_parts: int, page: PartsPage
) -> str:
    """The elements in the order of the API reference's response syntax. Each Part is
    ordered as its sample response (PartNumber, LastModified, ETag, Size), with the
    checksum last."""
    root = ET.Element("ListPartsResult")
    ET.SubElement(root, "Bucket").text = bucket
    ET.SubElement(root, "Key").text = key
    ET.SubElement(root, "UploadId").text = upload_id
    ET.SubElement(root, "PartNumberMarker").text = marker
    if page.is_truncated:
        ET.SubElement(root, "NextPartNumberMarker").text = str(page.parts[-1].part_number)
    ET.SubElement(root, "MaxParts").text = str(max_parts)
    ET.SubElement(root, "IsTruncated").text = "true" if page.is_truncated else "false"
    for part in page.parts:
        element = ET.SubElement(root, "Part")
        ET.SubElement(element, "PartNumber").text = str(part.part_number)
        ET.SubElement(element, "LastModified").text = _timestamp(part)
        ET.SubElement(element, "ETag").text = part.etag
        ET.SubElement(element, "Size").text = str(part.size)
        # NULL on a part uploaded before the v3 migration.
        if part.checksum is not None and part.checksum_algorithm is not None:
            ET.SubElement(element, f"Checksum{part.checksum_algorithm}").text = part.checksum
    _identity(root, "Initiator")
    _identity(root, "Owner")
    ET.SubElement(root, "StorageClass").text = "STANDARD"
    if page.upload.checksum_algorithm is not None:
        ET.SubElement(root, "ChecksumAlgorithm").text = page.upload.checksum_algorithm
        ET.SubElement(root, "ChecksumType").text = page.upload.checksum_type
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


async def list_parts(bucket: str, key: str, upload_id: str, request: Request) -> Response:
    """The guard has answered 501 and AccessDenied. Then: InvalidArgument for a bad
    max-parts or part-number-marker, NoSuchBucket, NoSuchUpload (unknown, completed,
    aborted, or of another bucket or key)."""
    metadata = request.app.state.metadata
    params = dict(request.query_params)
    max_parts = _max_parts(params)
    marker, marker_digits = _part_number_marker(params)

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})
    page = metadata.list_parts_page(
        upload_id=upload_id,
        bucket=bucket,
        key=key,
        part_number_marker=marker,
        max_parts=max_parts,
    )
    if page is None:
        raise NoSuchUpload(extra={"UploadId": upload_id})
    return Response(
        content=_list_parts_xml(bucket, key, upload_id, marker_digits, max_parts, page),
        status_code=200,
        media_type="application/xml",
    )
