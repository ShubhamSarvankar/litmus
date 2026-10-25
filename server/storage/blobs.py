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
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
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


class StoreInUse(RuntimeError):
    """Another process holds the lock on this blob directory."""


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


class BlobWriter:
    """The write side of BlobStore.open_writer: appends to the temp file and keeps the
    MD5 and size of what was written."""

    def __init__(self, out: IO[bytes], mid_write_point: str | None):
        self._out = out
        self._mid_write_point = mid_write_point
        self._md5 = hashlib.md5()
        self._first = True
        self.size = 0

    @property
    def etag(self) -> str:
        return f'"{self._md5.hexdigest()}"'

    @property
    def md5_digest(self) -> bytes:
        """The MD5 of what was written so far, as the 16 raw bytes."""
        return self._md5.digest()

    def write(self, data: bytes) -> None:
        if self._out is None:
            raise ValueError("write to a closed blob writer")
        if not data:
            return
        self._out.write(data)
        self._md5.update(data)
        self.size += len(data)
        if self._first and self._mid_write_point:
            self._out.flush()
            crash_point(self._mid_write_point)
        self._first = False

    def _close(self) -> None:
        self._out = None


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

    def create_root(self) -> None:
        """Create blobs/ and any missing parent, one level at a time, top down, each
        new entry made durable in its parent."""
        missing = []
        level = self._root
        while not level.is_dir():
            missing.append(level)
            level = level.parent
        # One level at a time, top down, each new entry made durable in its parent.
        for directory in reversed(missing):
            directory.mkdir(mode=0o700)
            _fsync_dir(directory.parent)

    def create_tree(self) -> None:
        """Create blobs/, its 256 fanout directories and tmp/ where missing, and
        make the new directory entries durable. Idempotent; also completes a tree
        left half-created by a kill."""
        self.create_root()
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
        self,
        blob_id: str,
        src: IO[bytes],
        mid_write_point: str | None = None,
        verify: Callable[["BlobWriter"], None] | None = None,
    ) -> tuple[str, int]:
        """Stream src into the blob: temp file, verify(writer) if given, fsync, rename,
        fsync of the fanout directory. Returns (quoted MD5 ETag, size). On any failure,
        including a verify that raises, no file of this blob is left behind and the
        exception propagates; verify runs before any fsync (see open_writer)."""
        with self.open_writer(blob_id, mid_write_point=mid_write_point, verify=verify) as writer:
            while chunk := src.read(_CHUNK):
                writer.write(chunk)
        return writer.etag, writer.size

    @contextmanager
    def open_writer(
        self,
        blob_id: str,
        mid_write_point: str | None = None,
        verify: Callable[["BlobWriter"], None] | None = None,
    ) -> Iterator["BlobWriter"]:
        """Write a blob in several steps. The body of the with statement calls
        writer.write(data) any number of times; leaving it normally runs verify(writer)
        if given, then the durable sequence of write_blob (fsync, rename, fsync of the
        fanout directory). If the body or verify raises, no file of this blob is left
        behind and the exception propagates; verify runs before any fsync, so a
        refusal costs no durable step. mid_write_point fires after the first write,
        with those bytes flushed to the temp file. After a normal exit writer.etag
        and writer.size describe the stored blob."""

        def run_verify() -> None:
            if verify is not None:
                verify(writer)

        with self._place(blob_id, before_sync=run_verify) as out:
            if self._write_delay_ms:
                time.sleep(self._write_delay_ms / 1000)
            writer = BlobWriter(out, mid_write_point)
            try:
                yield writer
            finally:
                writer._close()

    def assemble_blob(
        self, blob_id: str, part_ids: Iterable[str], mid_write_point: str | None = None
    ) -> int:
        """Concatenate the part blobs, in order, into a new blob with the same
        durable sequence as write_blob. Returns the size. Raises MissingPartBlob,
        leaving no file of the new blob, if a part file does not exist."""
        size = 0
        with self._place(blob_id) as out:
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
        return size

    @contextmanager
    def _place(
        self, blob_id: str, before_sync: Callable[[], None] | None = None
    ) -> Iterator[IO[bytes]]:
        """The one durable write sequence: open tmp/<id>.tmp, yield it for the caller
        to fill, then before_sync, fsync, rename to the blob path, fsync of the fanout
        directory. On any exception no file of the blob is left."""
        temp = self.temp_path(blob_id)
        final = self.blob_path(blob_id)
        # O_EXCL: the ID is registered and tmp/ is emptied at startup, so an
        # existing temp file is a bug; never write into it, and never remove it.
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0), 0o600)
        renamed = False
        try:
            with os.fdopen(fd, "wb") as out:
                yield out
                if before_sync is not None:
                    before_sync()
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

    # --- startup: lock and store marker (docs/design/storage-layout.md §7) ---

    @property
    def marker_path(self) -> Path:
        return self._root / MARKER_NAME

    @property
    def marker_temp_path(self) -> Path:
        return self._root / TMP_DIR / f"{MARKER_NAME}.tmp"

    def acquire_lock(self) -> int:
        """Take an exclusive, non-blocking lock on blobs/LOCK and return its file
        descriptor; held until release_lock. Raises StoreInUse if another process
        holds it. blobs/ must exist."""
        fd = os.open(self._root / LOCK_NAME, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            raise StoreInUse(f"{self._root} is in use by another server") from None
        return fd

    @staticmethod
    def release_lock(fd: int) -> None:
        os.close(fd)

    def read_marker(self) -> str | None:
        """The content of blobs/STORE_ID without its trailing newline, or None if the
        file does not exist."""
        try:
            return self.marker_path.read_text().rstrip("\n")
        except FileNotFoundError:
            return None

    def write_marker(self, store_id: str) -> None:
        """Write blobs/STORE_ID: tmp/STORE_ID.tmp, fsync, rename, fsync blobs/."""
        tmp = self._root / TMP_DIR
        if not tmp.is_dir():
            tmp.mkdir(mode=0o700)
            _fsync_dir(self._root)
        temp = self.marker_temp_path
        try:
            os.unlink(temp)  # left by a start killed while writing it
        except FileNotFoundError:
            pass
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as out:
            out.write(store_id + "\n")
            out.flush()
            os.fsync(out.fileno())
        os.rename(temp, self.marker_path)
        _fsync_dir(self._root)

    def unexpected_entries(
        self, tmp_holds_only_marker_temp: bool, marker: str | None
    ) -> list[Path]:
        """Entries under blobs/ that mean it has held data: anything but LOCK, the
        fanout directories (which must be empty) and tmp/. With
        tmp_holds_only_marker_temp, tmp/ may hold nothing but tmp/STORE_ID.tmp.
        STORE_ID is expected only if marker is given and the file holds exactly it."""
        if not self._root.is_dir():
            return []
        found = []
        for entry in sorted(self._root.iterdir()):
            name = entry.name
            if name == LOCK_NAME and entry.is_file():
                continue
            if name == MARKER_NAME and marker is not None and self.read_marker() == marker:
                continue
            if name in FANOUT and entry.is_dir():
                found += sorted(entry.iterdir())
                continue
            if name == TMP_DIR and entry.is_dir():
                if tmp_holds_only_marker_temp:
                    found += [e for e in sorted(entry.iterdir()) if e != self.marker_temp_path]
                continue
            found.append(entry)
        return found

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
