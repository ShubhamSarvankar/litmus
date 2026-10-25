from collections.abc import Callable

from server.crash_points import (
    COMPLETE_AFTER_ASSEMBLE,
    COMPLETE_AFTER_COMMIT,
    COMPLETE_AFTER_GC_UNLINK,
    COMPLETE_AFTER_REGISTER,
    COMPLETE_MID_ASSEMBLE,
    armed_point,
    crash_point,
)
from server.errors.s3errors import InternalError, InvalidPart, InvalidPartOrder, NoSuchUpload
from server.metadata.base import ObjectMeta, PartsChanged
from server.multipart.etag import composite_etag
from server.storage.blobs import MissingPartBlob
from server.storage.lifecycle import collect_garbage, discard_new_blob

MAX_PART_NUMBER = 10000


def _normalize_etag(e: str) -> str:
    # boto3 may send ETags with or without surrounding quotes.
    return e.strip('"')


def _raise_for_missing_part(metadata, upload_id: str, blob_id: str) -> None:
    """A part file was missing during assembly (§6.7 C3): decide why."""
    upload = metadata.get_upload(upload_id)
    if upload is None or upload.state != "open":
        raise NoSuchUpload(extra={"UploadId": upload_id})
    for part in metadata.get_parts(upload_id):
        if part.blob_id == blob_id:
            # The part row still names the file: the bytes are lost.
            raise InternalError()
    # The part was re-uploaded during assembly.
    raise InvalidPart()


def complete_multipart_upload(
    bucket: str,
    key: str,
    upload_id: str,
    requested_parts: list[dict],  # [{"PartNumber": int, "ETag": str}, ...]
    storage,
    metadata,
    precondition: Callable[[str | None], None] | None = None,
) -> str:
    """CompleteMultipartUpload (docs/design/storage-layout.md §6.7).

    Validates parts, assembles them into a fresh blob, then commits the object row
    and the upload completion in a single SQLite transaction, then unlinks the part
    blobs and any replaced object blob. Returns the composite ETag.

    precondition (the If-Match and If-None-Match check, §10) runs inside the commit
    transaction against the object row it replaces. If it raises, the transaction rolls
    back, so the upload is still open with all its parts; the assembled blob is
    discarded and the error propagates.

    Raises InvalidPartOrder unless the part numbers are strictly ascending (a repeat is
    out of order too), then InvalidPart if any listed part is outside 1 to 10000,
    missing or has a mismatched ETag. All of it is decided before a blob is registered.
    """
    previous = None
    for req in requested_parts:
        if previous is not None and req["PartNumber"] <= previous:
            raise InvalidPartOrder()
        previous = req["PartNumber"]

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
            raise InvalidPart(extra={"PartNumber": str(num)})

    part_blob_ids = {r["PartNumber"]: recorded[r["PartNumber"]].blob_id for r in requested_parts}
    etag = composite_etag([r["ETag"] for r in requested_parts])

    upload = metadata.get_upload(upload_id)
    if upload is None or upload.state != "open":
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
        _raise_for_missing_part(metadata, upload_id, missing.blob_id)
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
        last_modified=upload.initiated,
        content_type=upload.content_type,
        cache_control=upload.cache_control,
        content_disposition=upload.content_disposition,
        content_encoding=upload.content_encoding,
        content_language=upload.content_language,
        expires=upload.expires,
        user_metadata=upload.user_metadata,
        blob_id=blob_id,
        upload_id=upload_id,
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
    return etag
