"""Blob files for the storage layout in docs/design/storage-layout.md (§4, §6.1).

Every object version and every uploaded part is one immutable file
blobs/<xx>/<id>, where <id> is 32 random lowercase hex characters and <xx> its first
two. No bucket, key, upload ID or part number is ever part of a path. Bytes are
written to tmp/<id>.tmp, fsynced, renamed to the blob path, and the fanout directory
is fsynced; after that the file is only read and, at the end, unlinked.

This module only handles files. Which blobs exist, and which may be removed, is
recorded in the metadata registry (the blobs table).
"""

import hashlib
import logging
import os
import re
import secrets
import shutil
import time
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import IO

from server.crash_points import crash_point

logger = logging.getLogger(__name__)

FANOUT = tuple(f"{n:02x}" for n in range(256))
TMP_DIR = "tmp"
# Top-level entries of blobs/ other than the fanout directories and tmp/ (§4).
LOCK_NAME = "LOCK"
MARKER_NAME = "STORE_ID"

_ID = re.compile(r"[0-9a-f]{32}")
_CHUNK = 65536


class UnsafePathError(ValueError):
    """A name would map outside its directory."""


class InvalidBlobId(ValueError):
    """A string that is not a blob ID was used where a blob ID is required."""


class MissingPartBlob(FileNotFoundError):
    """A part blob named for assembly does not exist."""

    def __init__(self, blob_id: str):
        super().__init__(f"part blob {blob_id} is missing")
        self.blob_id = blob_id


def new_blob_id() -> str:
    return secrets.token_hex(16)


def is_blob_id(name: str) -> bool:
    return _ID.fullmatch(name) is not None


def _contained(base: Path, *names: str) -> Path:
    """Join names under base and refuse the result unless it is strictly inside base.

    Lexical check (os.path.normpath), so it does not depend on what exists on disk.
    """
    root = os.path.abspath(base)
    path = os.path.normpath(os.path.join(root, *names))
    if os.path.commonpath([root, path]) != root or path == root:
        raise UnsafePathError(f"{'/'.join(names)!r} resolves outside {root}")
    return Path(path)


def _fsync_dir(directory: Path) -> None:
    # Windows does not support fsync on directory fds; the durability guarantee
    # in crash-model.md is scoped to process-kill on POSIX only.
    if os.name == "nt":
        return
    dir_fd = os.open(str(directory), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


class BlobStore:
    def __init__(self, blob_dir: Path, write_delay_ms: int = 0):
        self._root = Path(os.path.abspath(blob_dir))
        self._write_delay_ms = write_delay_ms

    @property
    def root(self) -> Path:
        return self._root

    # --- paths ---

    def blob_path(self, blob_id: str) -> Path:
        if not is_blob_id(blob_id):
            raise InvalidBlobId(f"{blob_id!r} is not a blob ID")
        return _contained(self._root, blob_id[:2], blob_id)

    def temp_path(self, blob_id: str) -> Path:
        if not is_blob_id(blob_id):
            raise InvalidBlobId(f"{blob_id!r} is not a blob ID")
        return _contained(self._root, TMP_DIR, f"{blob_id}.tmp")

    # --- directory tree ---

    def create_tree(self) -> None:
        """Create blobs/, its 256 fanout directories and tmp/ where missing, and
        make the new directory entries durable. Idempotent; also completes a tree
        left half-created by a kill."""
        missing = []
        level = self._root
        while not level.is_dir():
            missing.append(level)
            level = level.parent
        # One level at a time, top down, each new entry made durable in its parent.
        for directory in reversed(missing):
            directory.mkdir(mode=0o700)
            _fsync_dir(directory.parent)
        created = False
        for name in (*FANOUT, TMP_DIR):
            path = self._root / name
            if not path.is_dir():
                path.mkdir(mode=0o700)
                created = True
        if created:
            _fsync_dir(self._root)

    # --- writing ---

    def write_blob(
        self, blob_id: str, src: IO[bytes], mid_write_point: str | None = None
    ) -> tuple[str, int]:
        """Stream src into the blob: temp file, fsync, rename, fsync of the fanout
        directory. Returns (quoted MD5 ETag, size). On any failure no file of this
        blob is left behind and the exception propagates."""
        md5 = hashlib.md5()
        size = 0

        def copy(out: IO[bytes]) -> None:
            nonlocal size
            if self._write_delay_ms:
                time.sleep(self._write_delay_ms / 1000)
            first = True
            while chunk := src.read(_CHUNK):
                out.write(chunk)
                md5.update(chunk)
                size += len(chunk)
                if first and mid_write_point:
                    out.flush()
                    crash_point(mid_write_point)
                first = False

        self._place(blob_id, copy)
        return f'"{md5.hexdigest()}"', size

    def assemble_blob(
        self, blob_id: str, part_ids: Iterable[str], mid_write_point: str | None = None
    ) -> int:
        """Concatenate the part blobs, in order, into a new blob with the same
        durable sequence as write_blob. Returns the size. Raises MissingPartBlob,
        leaving no file of the new blob, if a part file does not exist."""
        size = 0

        def copy(out: IO[bytes]) -> None:
            nonlocal size
            first = True
            for part_id in part_ids:
                try:
                    part = self.open_blob(part_id)
                except FileNotFoundError:
                    raise MissingPartBlob(part_id) from None
                with part:
                    shutil.copyfileobj(part, out, _CHUNK)
                    size += part.tell()
                if first and mid_write_point:
                    out.flush()
                    crash_point(mid_write_point)
                first = False

        self._place(blob_id, copy)
        return size

    def _place(self, blob_id: str, copy) -> None:
        temp = self.temp_path(blob_id)
        final = self.blob_path(blob_id)
        # O_EXCL: the ID is registered and tmp/ is emptied at startup, so an
        # existing temp file is a bug; never write into it, and never remove it.
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0), 0o600)
        renamed = False
        try:
            with os.fdopen(fd, "wb") as out:
                copy(out)
                out.flush()
                os.fsync(out.fileno())
            os.rename(temp, final)
            renamed = True
            _fsync_dir(final.parent)
        except BaseException:
            leftover = final if renamed else temp
            try:
                os.unlink(leftover)
            except FileNotFoundError:
                pass
            except OSError as unlink_error:
                # The file stays for the startup sweep. The caller sees it through
                # has_files() and keeps the registry row pending; the original
                # exception is the one that propagates.
                logger.warning(
                    "could not remove %s after a failed write: %s", leftover, unlink_error
                )
            raise

    # --- reading and removing ---

    def open_blob(self, blob_id: str) -> IO[bytes]:
        """Open a blob for reading. Raises FileNotFoundError if it does not exist."""
        return open(self.blob_path(blob_id), "rb")

    def blob_size(self, blob_id: str) -> int | None:
        try:
            return os.stat(self.blob_path(blob_id)).st_size
        except FileNotFoundError:
            return None

    def has_files(self, blob_id: str) -> bool:
        """Whether the blob's temp file or blob file may still exist. A stat that
        fails for any reason other than ENOENT counts as existing."""
        for path in (self.temp_path(blob_id), self.blob_path(blob_id)):
            try:
                os.stat(path)
            except FileNotFoundError:
                continue
            except OSError:
                return True
            return True
        return False

    def remove_blob(self, blob_id: str) -> bool:
        """Unlink a blob. Returns False if it was already gone; other errors
        propagate."""
        try:
            os.unlink(self.blob_path(blob_id))
        except FileNotFoundError:
            return False
        return True

    # --- for the startup sweep ---

    def temp_entries(self) -> list[Path]:
        tmp = self._root / TMP_DIR
        if not tmp.is_dir():
            return []
        return sorted(tmp.iterdir())

    def scan(self) -> Iterator[tuple[Path, str | None]]:
        """Every entry under blobs/ that is not part of the fixed tree, as
        (path, blob ID). The ID is None for an entry that cannot be a blob: a
        top-level entry other than the fanout directories, tmp/, LOCK and STORE_ID,
        or a fanout entry whose name is not an ID belonging to that directory."""
        if not self._root.is_dir():
            return
        fixed = {*FANOUT, TMP_DIR, LOCK_NAME, MARKER_NAME}
        for entry in sorted(self._root.iterdir()):
            if entry.name not in fixed:
                yield entry, None
        for name in FANOUT:
            directory = self._root / name
            if not directory.is_dir():
                continue
            for entry in sorted(directory.iterdir()):
                blob_id = entry.name
                ok = is_blob_id(blob_id) and blob_id[:2] == name and entry.is_file()
                yield entry, blob_id if ok else None
