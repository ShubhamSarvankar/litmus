from dataclasses import dataclass
from typing import IO, Protocol


@dataclass
class PartSpec:
    part_number: int
    etag: str


class StorageBackend(Protocol):
    def write_object(self, bucket: str, key: str, data: IO[bytes], size: int) -> str: ...

    # Returns quoted ETag e.g. '"abc123..."'

    def read_object(self, bucket: str, key: str) -> IO[bytes]: ...

    def delete_object(self, bucket: str, key: str) -> None: ...

    def object_exists(self, bucket: str, key: str) -> bool: ...

    def write_part(self, upload_id: str, part_number: int, data: IO[bytes]) -> str: ...

    # Returns part ETag

    def assemble_parts(
        self, bucket: str, key: str, upload_id: str, parts: list[PartSpec]
    ) -> str: ...

    # Returns composite ETag

    def delete_parts(self, upload_id: str) -> None: ...

    def list_part_files(self, upload_id: str) -> list[str]: ...

    def part_exists(self, upload_id: str, part_number: int) -> bool: ...
