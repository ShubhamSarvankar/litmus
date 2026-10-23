from datetime import UTC, datetime

from server.crash_points import (
    COMPLETE_AFTER_ASSEMBLE,
    COMPLETE_AFTER_COMMIT,
    COMPLETE_AFTER_GC_UNLINK,
    COMPLETE_AFTER_REGISTER,
    COMPLETE_MID_ASSEMBLE,
    armed_point,
    crash_point,
)
from server.errors.s3errors import InternalError, InvalidPart, NoSuchUpload
from server.metadata.base import ObjectMeta, PartsChanged
from server.multipart.etag import composite_etag
from server.storage.blobs import MissingPartBlob
from server.storage.lifecycle import collect_garbage, discard_new_blob


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
) -> str:
    """CompleteMultipartUpload (docs/design/storage-layout.md §6.7).

    Validates parts, assembles them into a fresh blob, then commits the object row
    and the upload completion in a single SQLite transaction, then unlinks the part
    blobs and any replaced object blob. Returns the composite ETag.

    Raises InvalidPart if any listed part is missing or has a mismatched ETag.
    """
    recorded = {p.part_number: p for p in metadata.get_parts(upload_id)}

    for req in requested_parts:
        num = req["PartNumber"]
        recorded_part = recorded.get(num)
        if (
            recorded_part is None
            or recorded_part.blob_id is None
            or _normalize_etag(recorded_part.etag) != _normalize_etag(req["ETag"])
        ):
            raise InvalidPart(extra={"PartNumber": str(num)})

    ordered = sorted(requested_parts, key=lambda r: r["PartNumber"])
    part_blob_ids = {r["PartNumber"]: recorded[r["PartNumber"]].blob_id for r in ordered}
    etag = composite_etag([r["ETag"] for r in ordered])

    upload = metadata.get_upload(upload_id)
    if upload is None or upload.state != "open":
        raise NoSuchUpload(extra={"UploadId": upload_id})

    # C2, C3: register a fresh blob and assemble the parts into it.
    blob_id = metadata.register_blob("object", bucket, key, upload_id=upload_id)
    crash_point(COMPLETE_AFTER_REGISTER)
    try:
        size = storage.assemble_blob(
            blob_id,
            [part_blob_ids[r["PartNumber"]] for r in ordered],
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
        last_modified=datetime.now(UTC).replace(tzinfo=None),
        content_type=upload.content_type,
        blob_id=blob_id,
        upload_id=upload_id,
    )
    try:
        committed = metadata.commit_completed_upload(
            upload_id, meta, part_blob_ids=part_blob_ids, released=released
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
