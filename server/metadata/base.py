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


@dataclass
class PartMeta:
    upload_id: str
    part_number: int
    etag: str
    size: int


@dataclass
class UploadMeta:
    upload_id: str
    bucket: str
    key: str
    initiated: datetime


class MetadataStore(Protocol):
    def create_bucket(self, bucket: str, created_at: datetime) -> None: ...

    def delete_bucket(self, bucket: str) -> None: ...

    def bucket_exists(self, bucket: str) -> bool: ...

    def list_buckets(self) -> list[BucketMeta]: ...

    def put_object_meta(self, bucket: str, key: str, meta: ObjectMeta) -> None: ...

    def get_object_meta(self, bucket: str, key: str) -> ObjectMeta | None: ...

    def delete_object_meta(self, bucket: str, key: str) -> None: ...

    def list_objects(self, bucket: str, prefix: str | None = None) -> list[ObjectMeta]: ...

    def create_upload(self, upload_id: str, bucket: str, key: str, initiated: datetime) -> None: ...

    def record_part(self, upload_id: str, part: PartMeta) -> None: ...

    def get_parts(self, upload_id: str) -> list[PartMeta]: ...

    def complete_upload(self, upload_id: str) -> None: ...

    def abort_upload(self, upload_id: str) -> None: ...

    def list_incomplete_uploads(self) -> list[UploadMeta]: ...

    def upload_exists(self, upload_id: str) -> bool: ...
