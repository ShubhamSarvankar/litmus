"""UploadPartCopy (docs/design/storage-layout.md §6.6, §11): UploadPart whose bytes come
from a byte range of another object's blob.

The source row is read once, its x-amz-copy-source-if-* conditions are decided against
it and its blob is opened (GetObject's sequence, §8), exactly as for CopyObject, so the
conditions, the size and the bytes belong to one row. The range is checked against that
row's size. The bytes are then written to a fresh part blob through the durable write
sequence of UploadPart, with the length (and, for the whole of a single part source, the
MD5) checked before they are made durable, and the part row replaces any earlier row for
the part number in one commit. The copied bytes are hashed in the upload's effective
algorithm (the one CreateMultipartUpload named, else CRC64NVME) and the digest and the commit
time are stored with the part; CopyPartResult returns that time, and the checksum only when
the upload named an algorithm (S3 API reference, CopyPartResult: "present if the multipart
upload request was created with the <algorithm> checksum algorithm").
"""

import base64
import logging
import os
import re
import xml.etree.ElementTree as ET

from fastapi import Request
from fastapi.responses import Response

from server.api.conditions import CopySourceConditions
from server.api.copy_source import COPY_SOURCE_HEADER, parse_copy_source
from server.api.objects import _CHUNK, _open_current
from server.checksums import CRC64NVME, Algorithm
from server.crash_points import (
    PART_COPY_AFTER_BYTES,
    PART_COPY_AFTER_COMMIT,
    PART_COPY_AFTER_REGISTER,
    PART_COPY_MID_WRITE,
    armed_point,
    crash_point,
)
from server.errors.s3errors import (
    InternalError,
    InvalidArgument,
    InvalidPart,
    InvalidRange,
    NoSuchBucket,
    NoSuchKey,
    NoSuchUpload,
    PreconditionFailed,
)
from server.metadata.base import PartMeta, UploadNotOpen
from server.numerals import SATURATED, numeral_text, parse_decimal, parse_signed_decimal
from server.storage.lifecycle import collect_garbage, discard_new_blob

logger = logging.getLogger(__name__)

COPY_SOURCE_RANGE_HEADER = "x-amz-copy-source-range"
# bytes=first-last, both decimal: the only form the S3 API reference gives. No open ended
# range, no suffix range and no list.
_RANGE = re.compile(r"bytes=([0-9]+)-([0-9]+)", re.ASCII)


def parse_copy_source_range(headers) -> tuple[int, int] | None:
    """(first, last) of the x-amz-copy-source-range header, or None when it is absent.
    Anything that is not bytes=<first>-<last> with first <= last is 400 InvalidArgument
    naming the header, a reversed range included (Litmus's choice: the reference gives no
    error for it). Whether the range lies inside the source is decided later, against the
    source row."""
    values = headers.getlist(COPY_SOURCE_RANGE_HEADER)
    if not values:
        return None
    match = _RANGE.fullmatch(values[0]) if len(values) == 1 else None
    if match is None:
        raise InvalidArgument(extra={"ArgumentName": COPY_SOURCE_RANGE_HEADER})
    first, last = parse_decimal(match[1]), parse_decimal(match[2])
    if _is_reversed(match[1], match[2], first, last):
        raise InvalidArgument(extra={"ArgumentName": COPY_SOURCE_RANGE_HEADER})
    return first, last


def _is_reversed(first_digits: str, last_digits: str, first: int, last: int) -> bool:
    """Whether first is greater than last. Two values that both saturate are ordered by
    their digits, so a reversed range of very long numerals is still reversed."""
    if first == last == SATURATED:
        a, b = first_digits.lstrip("0"), last_digits.lstrip("0")
        return (len(a), a) > (len(b), b)
    return first > last


def _result_xml(part: PartMeta, named: bool) -> str:
    root = ET.Element("CopyPartResult")
    ET.SubElement(root, "ETag").text = part.etag
    ET.SubElement(root, "LastModified").text = part.last_modified.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    if named:
        ET.SubElement(root, f"Checksum{part.checksum_algorithm}").text = part.checksum
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


async def upload_part_copy(bucket: str, key: str, params: dict, request: Request) -> Response:
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})

    # The checks of UploadPart, in its order.
    upload_id = params["uploadId"]
    part_number = parse_signed_decimal(params["partNumber"])
    if part_number is None:
        raise InvalidPart()
    part_number_text = numeral_text(params["partNumber"])
    upload = metadata.get_open_upload(upload_id, bucket=bucket, key=key)
    if upload is None:
        raise NoSuchUpload(extra={"UploadId": upload_id})
    if not (1 <= part_number <= 10000):
        raise InvalidPart(extra={"PartNumber": part_number_text})

    named = upload.checksum_algorithm is not None
    effective = Algorithm.by_name(upload.checksum_algorithm) if named else CRC64NVME

    values = request.headers.getlist(COPY_SOURCE_HEADER)
    if len(values) != 1:
        raise InvalidArgument(extra={"ArgumentName": COPY_SOURCE_HEADER})
    source = parse_copy_source(values[0])
    byte_range = parse_copy_source_range(request.headers)

    if not metadata.bucket_exists(source.bucket):
        raise NoSuchBucket(extra={"BucketName": source.bucket})
    if metadata.get_object_meta(source.bucket, source.key) is None:
        raise NoSuchKey(extra={"Key": source.key})

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
        if byte_range is None:
            first, length = 0, src.size
        else:
            first, last = byte_range
            if last >= src.size:
                # Also every range of an empty source.
                raise InvalidRange(extra={"ActualObjectSize": str(src.size)})
            length = last - first + 1
        whole = first == 0 and length == src.size

        def verify(writer) -> None:
            # Before the bytes are made durable. Only the whole of a single part source
            # has an ETag that is the MD5 of the bytes copied; for a range, or a multipart
            # source, the length is all there is to check.
            if writer.size != length or (
                whole and src.upload_id is None and writer.etag != src.etag
            ):
                logger.error(
                    "part copy of %s/%s [%d, +%d): blob %s read back as %d bytes %s, "
                    "row says %d bytes %s",
                    source.bucket,
                    source.key,
                    first,
                    length,
                    src.blob_id,
                    writer.size,
                    writer.etag,
                    src.size,
                    src.etag,
                )
                raise InternalError()

        # docs/design/storage-layout.md §6.6: a fresh blob per attempt; the previous part
        # stays recorded and intact until the commit that replaces it.
        blob_id = metadata.register_blob(
            "part", bucket, key, upload_id=upload_id, part_number=part_number
        )
        crash_point(PART_COPY_AFTER_REGISTER)
        hasher = effective.new()
        try:
            with storage.open_writer(
                blob_id, mid_write_point=armed_point(PART_COPY_MID_WRITE), verify=verify
            ) as writer:
                f.seek(first)
                remaining = length
                while remaining and (chunk := f.read(min(_CHUNK, remaining))):
                    writer.write(chunk)
                    hasher.update(chunk)
                    remaining -= len(chunk)
        except BaseException:
            discard_new_blob(storage, metadata, blob_id)
            raise
    crash_point(PART_COPY_AFTER_BYTES)

    part = PartMeta(
        upload_id=upload_id,
        part_number=part_number,
        etag=writer.etag,
        size=writer.size,
        blob_id=blob_id,
        checksum=base64.b64encode(hasher.digest()).decode("ascii"),
        checksum_algorithm=effective.name,
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
    crash_point(PART_COPY_AFTER_COMMIT)
    if replaced:
        collect_garbage(storage, metadata, [replaced])
    return Response(
        content=_result_xml(part, named),
        status_code=200,
        media_type="application/xml",
    )
