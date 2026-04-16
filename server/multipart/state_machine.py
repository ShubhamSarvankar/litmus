from datetime import UTC

from server.errors.s3errors import InvalidPart
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

    Validates parts, assembles the file, commits object metadata, marks upload
    complete, and cleans up part files. Returns the composite ETag.

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

    # 2. Compute total size from recorded part metadata
    total_size = sum(recorded[p.part_number].size for p in parts)

    from datetime import datetime

    # 3. Commit object metadata — bytes are already on disk
    metadata.put_object_meta(
        bucket,
        key,
        ObjectMeta(
            bucket=bucket,
            key=key,
            size=total_size,
            etag=composite_etag,
            last_modified=datetime.now(UTC).replace(tzinfo=None),
            content_type=content_type,
        ),
    )

    # 4. Mark upload complete
    metadata.complete_upload(upload_id)

    # 5. Delete part files
    storage.delete_parts(upload_id)

    return composite_etag
