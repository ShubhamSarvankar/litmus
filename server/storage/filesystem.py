import hashlib
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import IO

from server.storage.base import PartSpec


class UnsafePathError(ValueError):
    """A bucket, key or upload ID would map outside its directory."""


def _contained(base: Path, *names: str) -> Path:
    """Join names under base and refuse the result unless it is strictly inside base.

    Lexical check (os.path.normpath), so it does not depend on what exists on disk.
    """
    root = os.path.abspath(base)
    path = os.path.normpath(os.path.join(root, *names))
    if os.path.commonpath([root, path]) != root or path == root:
        raise UnsafePathError(f"{'/'.join(names)!r} resolves outside {root}")
    return Path(path)


class FilesystemBackend:
    def __init__(self, data_dir: Path, parts_dir: Path, write_delay_ms: int = 0):
        self._objects = data_dir
        self._parts = parts_dir
        self._write_delay_ms = write_delay_ms
        self._objects.mkdir(parents=True, exist_ok=True)
        self._parts.mkdir(parents=True, exist_ok=True)

    # --- internal helpers ---

    def _object_path(self, bucket: str, key: str) -> Path:
        # The key must stay inside its own bucket directory, and the bucket inside
        # the objects directory: never another bucket, the data dir, or beyond.
        return _contained(_contained(self._objects, bucket), key)

    def _upload_dir(self, upload_id: str) -> Path:
        return _contained(self._parts, upload_id)

    def _part_path(self, upload_id: str, part_number: int) -> Path:
        return _contained(self._upload_dir(upload_id), str(part_number))

    def _durable_write(self, src: IO[bytes], final_path: Path) -> str:
        """Stream src to a temp file in the same directory as final_path,
        fsync, rename, fsync parent dir. Returns quoted MD5 ETag."""
        final_path.parent.mkdir(parents=True, exist_ok=True)

        md5 = hashlib.md5()
        tmp_fd, tmp_name = tempfile.mkstemp(dir=final_path.parent)
        try:
            with os.fdopen(tmp_fd, "wb") as tmp_file:
                if self._write_delay_ms:
                    time.sleep(self._write_delay_ms / 1000)
                while True:
                    chunk = src.read(65536)
                    if not chunk:
                        break
                    tmp_file.write(chunk)
                    md5.update(chunk)
                tmp_file.flush()
                os.fsync(tmp_file.fileno())
            # tmp_fd is closed by fdopen context manager
            os.replace(tmp_name, final_path)
            self._fsync_dir(final_path.parent)
        except Exception:
            # Clean up temp file on any failure
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

        return f'"{md5.hexdigest()}"'

    def _fsync_dir(self, directory: Path) -> None:
        # Windows does not support fsync on directory fds; the durability guarantee
        # in crash-model.md is scoped to process-kill on POSIX only.
        if os.name == "nt":
            return
        dir_fd = os.open(str(directory), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    # --- StorageBackend implementation ---

    def write_object(self, bucket: str, key: str, data: IO[bytes], size: int) -> str:
        return self._durable_write(data, self._object_path(bucket, key))

    def read_object(self, bucket: str, key: str) -> IO[bytes]:
        return open(self._object_path(bucket, key), "rb")

    def delete_object(self, bucket: str, key: str) -> None:
        path = self._object_path(bucket, key)
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    def object_exists(self, bucket: str, key: str) -> bool:
        # Used by the startup sweep over every metadata row. A row written before keys
        # were checked may name an unsafe path; there is no object file for it inside
        # storage, and it must not stop the server from starting.
        try:
            return self._object_path(bucket, key).exists()
        except UnsafePathError:
            return False

    def write_part(self, upload_id: str, part_number: int, data: IO[bytes]) -> str:
        return self._durable_write(data, self._part_path(upload_id, part_number))

    def assemble_parts(self, bucket: str, key: str, upload_id: str, parts: list[PartSpec]) -> str:
        final_path = self._object_path(bucket, key)
        final_path.parent.mkdir(parents=True, exist_ok=True)

        # Build composite ETag from raw MD5 bytes of each part
        raw_md5_bytes = b"".join(bytes.fromhex(p.etag.strip('"')) for p in parts)
        composite_md5 = hashlib.md5(raw_md5_bytes).hexdigest()
        composite_etag = f'"{composite_md5}-{len(parts)}"'

        # Concatenate all part files into a single temp file, then durable-rename
        tmp_fd, tmp_name = tempfile.mkstemp(dir=final_path.parent)
        try:
            with os.fdopen(tmp_fd, "wb") as tmp_file:
                for part in parts:
                    part_path = self._part_path(upload_id, part.part_number)
                    with open(part_path, "rb") as pf:
                        shutil.copyfileobj(pf, tmp_file)
                tmp_file.flush()
                os.fsync(tmp_file.fileno())
            os.replace(tmp_name, final_path)
            self._fsync_dir(final_path.parent)
        except Exception:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

        return composite_etag

    def delete_parts(self, upload_id: str) -> None:
        part_dir = self._upload_dir(upload_id)
        if part_dir.exists():
            shutil.rmtree(part_dir)

    def list_part_files(self, upload_id: str) -> list[str]:
        part_dir = self._upload_dir(upload_id)
        if not part_dir.exists():
            return []
        return [str(p) for p in part_dir.iterdir()]

    def part_exists(self, upload_id: str, part_number: int) -> bool:
        return self._part_path(upload_id, part_number).exists()

    def get_object_size(self, bucket: str, key: str) -> int:
        return self._object_path(bucket, key).stat().st_size
