from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass
class BucketMeta:
    name: str
    created_at: datetime


@dataclass
class ObjectMeta:
    bucket: str
    key: str
    size: int
    etag: str
    last_modified: datetime
    content_type: str
    blob_id: str
    # Set when the object was written by CompleteMultipartUpload.
    upload_id: str | None = None


@dataclass
class PartMeta:
    upload_id: str
    part_number: int
    etag: str
    size: int
    # NULL once Complete released the part's bytes (docs/design/storage-layout.md §5).
    blob_id: str | None = None


@dataclass
class UploadMeta:
    upload_id: str
    bucket: str
    key: str
    initiated: datetime
    state: str = "open"
    content_type: str = "binary/octet-stream"


@dataclass
class BlobRecord:
    """One row of the blob registry (docs/design/storage-layout.md §3, §5)."""

    blob_id: str
    state: str  # 'pending', 'live' or 'garbage'
    kind: str  # 'object' or 'part'
    bucket: str
    key: str
    upload_id: str | None
    part_number: int | None


@dataclass
class StoreRecord:
    """The store identity row (docs/design/storage-layout.md I7, §5)."""

    store_id: str
    state: str  # 'initializing' or 'ready'
    blob_dir: str


# --- refused commits ---


class BucketMissing(Exception):
    """The bucket does not exist (any more)."""


class BucketHasObjects(Exception):
    """DeleteBucket on a bucket that still has objects."""


class UploadNotOpen(Exception):
    """The multipart upload is unknown, aborted or completed."""


class PartsChanged(Exception):
    """A part named in Complete was re-uploaded after it was assembled."""

    def __init__(self, part_number: int):
        super().__init__(f"part {part_number} changed")
        self.part_number = part_number


class MetadataStore(Protocol):
    def create_bucket(self, bucket: str, created_at: datetime) -> None: ...

    def delete_bucket(self, bucket: str) -> list[str]: ...

    # Aborts the bucket's uploads in the same transaction; returns their part blobs,
    # now garbage. Raises BucketMissing or BucketHasObjects.

    def bucket_exists(self, bucket: str) -> bool: ...

    def list_buckets(self) -> list[BucketMeta]: ...

    def register_blob(
        self,
        kind: str,
        bucket: str,
        key: str,
        upload_id: str | None = None,
        part_number: int | None = None,
    ) -> str: ...

    # Commits a pending registry row for a new blob ID and returns the ID.

    def release_blob(self, blob_id: str) -> None: ...

    def blob_state(self, blob_id: str) -> str | None: ...

    def purge_garbage(self, blob_ids: list[str]) -> None: ...

    def put_object_meta(self, bucket: str, key: str, meta: ObjectMeta) -> str | None: ...

    # Points the key at meta.blob_id, makes it live, and returns the replaced blob
    # (now garbage), if any. Raises BucketMissing.

    def get_object_meta(self, bucket: str, key: str) -> ObjectMeta | None: ...

    def delete_object_meta(self, bucket: str, key: str) -> list[str]: ...

    def delete_objects(self, bucket: str, keys: list[str]) -> list[str]: ...

    def list_objects(self, bucket: str, prefix: str | None = None) -> list[ObjectMeta]: ...

    def create_upload(self, upload_id: str, bucket: str, key: str, initiated: datetime) -> None: ...

    def get_upload(self, upload_id: str) -> UploadMeta | None: ...

    def record_part(self, upload_id: str, part: PartMeta) -> str | None: ...

    # Returns the replaced part blob (now garbage), if any. Raises UploadNotOpen.

    def get_parts(self, upload_id: str) -> list[PartMeta]: ...

    def commit_completed_upload(
        self,
        upload_id: str,
        meta: ObjectMeta,
        part_blob_ids: dict[int, str] | None = None,
        released: list[str] | None = None,
    ) -> bool: ...

    # Marks the open upload complete and commits its object row in one transaction.
    # Returns False, changing nothing, if the upload is not open; raises PartsChanged
    # if a listed part no longer names the blob that was assembled. Blobs it makes
    # garbage are appended to released.

    def abort_upload(self, upload_id: str) -> list[str] | None: ...

    # Returns the upload's part blobs (now garbage), or None if it was not open.

    def list_incomplete_uploads(self) -> list[UploadMeta]: ...

    def upload_exists(self, upload_id: str) -> bool: ...
