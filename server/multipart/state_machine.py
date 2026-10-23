from datetime import UTC

from server.crash_points import COMPLETE_AFTER_ASSEMBLE, COMPLETE_AFTER_COMMIT, crash_point
from server.errors.s3errors import InvalidPart, NoSuchUpload
from server.metadata.base import ObjectMeta
from server.storage.base import PartSpec


def complete_multipart_upload(
    bucket: str,
    key: str,
    upload_id: str,
    requested_parts: list[dict],  # [{"PartNumber": int, "ETag": str}, ...]
    storage,
    metadata,
    content_type: str = "binary/octet-stream",
) -> str:
    """Execute the COMPLETING transition atomically.

    Validates parts, assembles the file, commits the object metadata and the upload
    completion in a single SQLite transaction, then cleans up part files. Returns
    the composite ETag.

    Raises InvalidPart if any listed part is missing or has a mismatched ETag.
    """
    recorded = {p.part_number: p for p in metadata.get_parts(upload_id)}

    # Validate every requested part exists with a matching ETag.
    # Normalize quotes: boto3 may send ETags with or without surrounding quotes.
    def _normalize_etag(e: str) -> str:
        return e.strip('"')

    for req in requested_parts:
        num = req["PartNumber"]
        etag = req["ETag"]
        recorded_part = recorded.get(num)
        if recorded_part is None or _normalize_etag(recorded_part.etag) != _normalize_etag(etag):
            raise InvalidPart(extra={"PartNumber": str(num)})

    # Build ordered PartSpec list for assembly
    parts = [
        PartSpec(part_number=req["PartNumber"], etag=req["ETag"])
        for req in sorted(requested_parts, key=lambda r: r["PartNumber"])
    ]

    # 1. Assemble parts into final object file (durable write)
    composite_etag = storage.assemble_parts(bucket, key, upload_id, parts)
    crash_point(COMPLETE_AFTER_ASSEMBLE)

    # 2. Compute total size from recorded part metadata
    total_size = sum(recorded[p.part_number].size for p in parts)

    from datetime import datetime

    # 3. Commit object metadata and mark the upload complete in one SQLite
    # transaction — bytes are already on disk
    committed = metadata.commit_completed_upload(
        upload_id,
        ObjectMeta(
            bucket=bucket,
            key=key,
            size=total_size,
            etag=composite_etag,
            last_modified=datetime.now(UTC).replace(tzinfo=None),
            content_type=content_type,
        ),
    )
    if not committed:
        raise NoSuchUpload(extra={"UploadId": upload_id})

    crash_point(COMPLETE_AFTER_COMMIT)

    # 4. Delete part files
    storage.delete_parts(upload_id)

    return composite_etag
