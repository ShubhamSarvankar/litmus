from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, Protocol


@dataclass
class PartSpec:
    part_number: int
    etag: str


class StorageBackend(Protocol):
    """Blob files (docs/design/storage-layout.md §4, §6.1). Every method takes a
    blob ID; no client string is ever part of a path."""

    def create_tree(self) -> None: ...

    def write_blob(
        self,
        blob_id: str,
        src: IO[bytes],
        mid_write_point: str | None = None,
        verify: Callable[[Any], None] | None = None,
    ) -> tuple[str, int]: ...

    # Returns (quoted MD5 ETag, size). verify(writer) runs after the last byte is
    # written and before any fsync; if it raises, no file of the blob is left.

    def assemble_blob(
        self, blob_id: str, part_ids: Iterable[str], mid_write_point: str | None = None
    ) -> int: ...

    # Returns the size; raises MissingPartBlob if a part file is missing

    def open_blob(self, blob_id: str) -> IO[bytes]: ...

    def blob_path(self, blob_id: str) -> Path: ...

    def blob_size(self, blob_id: str) -> int | None: ...

    def has_files(self, blob_id: str) -> bool: ...

    def remove_blob(self, blob_id: str) -> bool: ...

    def temp_entries(self) -> list[Path]: ...

    def scan(self) -> Iterator[tuple[Path, str | None]]: ...
