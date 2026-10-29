from collections.abc import Callable
from dataclasses import dataclass

from server.checksums import CRC64NVME, Algorithm
from server.crash_points import (
    COMPLETE_AFTER_ASSEMBLE,
    COMPLETE_AFTER_COMMIT,
    COMPLETE_AFTER_GC_UNLINK,
    COMPLETE_AFTER_REGISTER,
    COMPLETE_MID_ASSEMBLE,
    armed_point,
    crash_point,
)
from server.errors.s3errors import (
    EntityTooSmall,
    InternalError,
    InvalidPart,
    InvalidPartOrder,
    InvalidRequest,
    NoSuchUpload,
)
from server.metadata.base import ObjectMeta, PartsChanged, UploadMeta
from server.multipart.checksums import (
    COMPOSITE,
    FULL_OBJECT,
    CompleteClaims,
    ObjectChecksum,
    PartRef,
    object_checksum,
    verify_claims,
    verify_part_elements,
)
from server.multipart.etag import composite_etag
from server.numerals import numeral_key
from server.storage.blobs import MissingPartBlob
from server.storage.lifecycle import collect_garbage, discard_new_blob

MAX_PART_NUMBER = 10000


def _normalize_etag(e: str) -> str:
    # boto3 may send ETags with or without surrounding quotes.
    return e.strip('"')


def _raise_for_missing_part(metadata, bucket: str, key: str, upload_id: str, blob_id: str) -> None:
    """A part file was missing during assembly (§6.7 C3): decide why."""
    upload = metadata.get_upload(upload_id, bucket=bucket, key=key)
    if upload is None or upload.state != "open":
        raise NoSuchUpload(extra={"UploadId": upload_id})
    for part in metadata.get_parts(upload_id):
        if part.blob_id == blob_id:
            # The part row still names the file: the bytes are lost.
            raise InternalError()
    # The part was re-uploaded during assembly.
    raise InvalidPart()


@dataclass(frozen=True)
class CompletedUpload:
    """What a Complete answers: the composite ETag, and the object's checksum, None when
    a listed part stored none."""

    etag: str
    checksum: ObjectChecksum | None


def complete_multipart_upload(
    bucket: str,
    key: str,
    upload_id: str,
    # [{"PartNumber": int, "ETag": str, "Checksums": {algorithm name: value}}, ...]
    requested_parts: list[dict],
    upload: UploadMeta,
    claims: CompleteClaims,
    storage,
    metadata,
    precondition: Callable[[str | None], None] | None = None,
    expected_size: int | None = None,
    min_part_size: int = 0,
) -> CompletedUpload:
    """CompleteMultipartUpload (docs/design/storage-layout.md §6.7, §20).

    Validates parts, assembles them into a fresh blob, then commits the object row
    and the upload completion in a single SQLite transaction, then unlinks the part
    blobs and any replaced object blob. Returns the composite ETag and the object's
    checksum. upload is the open upload the handler read, claims its checksum headers.

    precondition (the If-Match and If-None-Match check, §10) runs inside the commit
    transaction against the object row it replaces. If it raises, the transaction rolls
    back, so the upload is still open with all its parts; the assembled blob is
    discarded and the error propagates.

    Raises InvalidPartOrder unless the part numbers are strictly ascending (a repeat is
    out of order too), then InvalidPart if any listed part is outside 1 to 10000,
    missing or has a mismatched ETag, then InvalidPartOrder again if the upload is
    COMPOSITE and the listed numbers are not exactly 1 to N, then EntityTooSmall if a
    listed part other than the last listed one is stored smaller than min_part_size
    bytes, then the Part checksum elements (InvalidRequest, BadDigest), then
    InvalidRequest if expected_size (the x-amz-mp-object-size header) is not the total of
    the stored sizes of exactly the listed parts, then BadDigest for the type header and
    the checksum header. All of it is decided before a blob is registered.
    """
    previous = None
    for req in requested_parts:
        order = numeral_key(req.get("PartNumberText") or str(req["PartNumber"]))
        if previous is not None and order <= previous:
            raise InvalidPartOrder()
        previous = order

    recorded = {p.part_number: p for p in metadata.get_parts(upload_id)}

    for req in requested_parts:
        num = req["PartNumber"]
        recorded_part = recorded.get(num)
        if (
            not 1 <= num <= MAX_PART_NUMBER
            or recorded_part is None
            or recorded_part.blob_id is None
            or _normalize_etag(recorded_part.etag) != _normalize_etag(req["ETag"])
        ):
            raise InvalidPart(extra={"PartNumber": req.get("PartNumberText") or str(num)})

    # S3 API reference, "CompletedPart": with part checksums the numbers start at 1 and
    # are consecutive, else 400 InvalidPartOrder. Only COMPOSITE uploads need it.
    if upload.checksum_type == COMPOSITE and [r["PartNumber"] for r in requested_parts] != list(
        range(1, len(requested_parts) + 1)
    ):
        raise InvalidPartOrder()

    for req in requested_parts[:-1]:
        if recorded[req["PartNumber"]].size < min_part_size:
            raise EntityTooSmall()

    named = upload.checksum_algorithm
    listed = [recorded[r["PartNumber"]] for r in requested_parts]
    verify_part_elements(requested_parts, [p.checksum for p in listed], named)

    if expected_size is not None and expected_size != sum(
        recorded[r["PartNumber"]].size for r in requested_parts
    ):
        raise InvalidRequest(extra={"Header": "x-amz-mp-object-size"})

    # An unnamed upload stores CRC64NVME, whole object, like PutObject.
    algorithm = Algorithm.by_name(named) if named else CRC64NVME
    type_ = upload.checksum_type if named else FULL_OBJECT
    value = object_checksum(algorithm, type_, [PartRef(p.checksum, p.size) for p in listed])
    verify_claims(claims, upload.checksum_type, named is not None, value)
    checksum = ObjectChecksum(algorithm.name, value, type_) if value is not None else None

    part_blob_ids = {r["PartNumber"]: recorded[r["PartNumber"]].blob_id for r in requested_parts}
    etag = composite_etag([r["ETag"] for r in requested_parts])

    current = metadata.get_upload(upload_id, bucket=bucket, key=key)
    if current is None or current.state != "open":
        raise NoSuchUpload(extra={"UploadId": upload_id})

    # C2, C3: register a fresh blob and assemble the parts into it.
    blob_id = metadata.register_blob("object", bucket, key, upload_id=upload_id)
    crash_point(COMPLETE_AFTER_REGISTER)
    try:
        size = storage.assemble_blob(
            blob_id,
            [part_blob_ids[r["PartNumber"]] for r in requested_parts],
            mid_write_point=armed_point(COMPLETE_MID_ASSEMBLE),
        )
    except MissingPartBlob as missing:
        discard_new_blob(storage, metadata, blob_id)
        _raise_for_missing_part(metadata, bucket, key, upload_id, missing.blob_id)
        raise
    except BaseException:
        discard_new_blob(storage, metadata, blob_id)
        raise
    crash_point(COMPLETE_AFTER_ASSEMBLE)

    # C4: one transaction for the object row and the upload completion.
    released: list[str] = []
    meta = ObjectMeta(
        bucket=bucket,
        key=key,
        size=size,
        etag=etag,
        # S3 User Guide, "System-defined object metadata": for a multipart upload, the
        # object's date is the date the upload was initiated.
        last_modified=current.initiated,
        content_type=current.content_type,
        cache_control=current.cache_control,
        content_disposition=current.content_disposition,
        content_encoding=current.content_encoding,
        content_language=current.content_language,
        expires=current.expires,
        user_metadata=current.user_metadata,
        blob_id=blob_id,
        upload_id=upload_id,
        checksum=checksum.value if checksum else None,
        checksum_algorithm=checksum.algorithm if checksum else None,
        checksum_type=checksum.type if checksum else None,
    )
    try:
        committed = metadata.commit_completed_upload(
            upload_id,
            meta,
            part_blob_ids=part_blob_ids,
            released=released,
            precondition=precondition,
        )
    except PartsChanged as changed:
        discard_new_blob(storage, metadata, blob_id)
        raise InvalidPart(extra={"PartNumber": str(changed.part_number)}) from None
    except BaseException:
        discard_new_blob(storage, metadata, blob_id)
        raise
    if not committed:
        # Refused: nothing outside the fresh blob was written, so the existing
        # object is untouched (R6).
        discard_new_blob(storage, metadata, blob_id)
        raise NoSuchUpload(extra={"UploadId": upload_id})

    crash_point(COMPLETE_AFTER_COMMIT)

    # C5: unlink the part blobs and the replaced object blob.
    collect_garbage(storage, metadata, released, after_unlink_point=COMPLETE_AFTER_GC_UNLINK)
    return CompletedUpload(etag, checksum)
