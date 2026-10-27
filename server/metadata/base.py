from collections.abc import Callable
from dataclasses import dataclass, field
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
    # Stored system headers, as the client sent them; None when the PUT had none.
    cache_control: str | None = None
    content_disposition: str | None = None
    content_encoding: str | None = None
    content_language: str | None = None
    expires: str | None = None
    # x-amz-meta-* of the PUT: lower case name, without the prefix, to text.
    user_metadata: dict[str, str] = field(default_factory=dict)


@dataclass
class ListPage:
    """One page of a listing: objects and common prefixes, each in key order."""

    objects: list[ObjectMeta] = field(default_factory=list)
    common_prefixes: list[str] = field(default_factory=list)
    is_truncated: bool = False
    # The last key or common prefix on the page (V1's NextMarker).
    next_marker: str | None = None
    # Where the next page starts: after this key, or at this key (inclusive) when the
    # page ended on a common prefix, whose successor it is. Exactly one is set on a
    # truncated page.
    restart_after: str | None = None
    restart_at: str | None = None

    @property
    def count(self) -> int:
        return len(self.objects) + len(self.common_prefixes)


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
    # What CreateMultipartUpload was given; Complete copies it onto the object.
    content_type: str = "binary/octet-stream"
    cache_control: str | None = None
    content_disposition: str | None = None
    content_encoding: str | None = None
    content_language: str | None = None
    expires: str | None = None
    user_metadata: dict[str, str] = field(default_factory=dict)


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
    def migrate(self) -> None:
        """Bring the store to the current schema version, in place.

        Called once at startup, after the blob directory lock and the store identity
        check (docs/design/storage-layout.md §7, §20). A no-op when the store is
        current. Constructing a store only inspects an existing database; it never
        migrates one.
        """
        ...

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

    def put_object_meta(
        self,
        bucket: str,
        key: str,
        meta: ObjectMeta,
        precondition: Callable[[str | None], None] | None = None,
    ) -> str | None: ...

    # Points the key at meta.blob_id, makes it live, and returns the replaced blob
    # (now garbage), if any. Raises BucketMissing. precondition is called inside the
    # transaction with the ETag of the row being replaced (None if none); what it raises
    # rolls the commit back. A meta with last_modified None is stamped inside the commit.

    def get_object_meta(self, bucket: str, key: str) -> ObjectMeta | None: ...

    def delete_object_meta(
        self,
        bucket: str,
        key: str,
        precondition: Callable[[str | None], None] | None = None,
    ) -> list[str]: ...

    # Deletes the row and returns its blob (now garbage); no row is no error.
    # precondition is called inside the transaction with the ETag of the row being
    # deleted (None if none); what it raises rolls the commit back.

    def delete_objects(self, bucket: str, keys: list[str]) -> list[str]: ...

    def delete_objects_checked(
        self,
        bucket: str,
        entries: list[tuple[str, Callable[[str | None], Exception | None] | None]],
    ) -> tuple[list[str], list[Exception | None]]: ...

    # delete_objects with a condition per key, in one transaction. Each entry's
    # precondition is called after the write lock, in request order, with the ETag of
    # that key's row (None if none). Unlike put_object_meta's, it returns the refusal
    # instead of raising it: a refused key is left untouched and the others still
    # commit. Returns the blobs that became garbage and, per entry, its refusal (None
    # when the key was deleted or had no row and no condition).

    def list_objects(
        self,
        bucket: str,
        prefix: str | None = None,
        start_after: str | None = None,
        limit: int | None = None,
    ) -> list[ObjectMeta]: ...

    # Keys that start with prefix (exact bytes, case sensitive) and sort after
    # start_after, in key order, at most limit of them.

    def list_page(
        self,
        bucket: str,
        prefix: str,
        delimiter: str,
        marker: str,
        max_keys: int,
        start_at: str | None = None,
    ) -> ListPage: ...

    # One page of keys and common prefixes after marker (or from start_at, inclusive),
    # read in one read transaction.

    def create_upload(
        self,
        upload_id: str,
        bucket: str,
        key: str,
        initiated: datetime,
        *,
        content_type: str = "binary/octet-stream",
        cache_control: str | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        content_language: str | None = None,
        expires: str | None = None,
        user_metadata: dict[str, str] | None = None,
    ) -> None: ...

    # Creates the open upload with the headers and metadata it will give the object.

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
        precondition: Callable[[str | None], None] | None = None,
    ) -> bool: ...

    # Marks the open upload complete and commits its object row in one transaction.
    # Returns False, changing nothing, if the upload is not open; raises PartsChanged
    # if a listed part no longer names the blob that was assembled. Blobs it makes
    # garbage are appended to released. precondition is called inside the transaction,
    # after those two checks, with the ETag of the row being replaced (None if none);
    # what it raises rolls the whole commit back, the upload's state change included,
    # so the upload stays open with its parts.

    def abort_upload(self, upload_id: str) -> list[str] | None: ...

    # Returns the upload's part blobs (now garbage), or None if it was not open.

    def list_incomplete_uploads(self) -> list[UploadMeta]: ...

    def upload_exists(self, upload_id: str) -> bool: ...
