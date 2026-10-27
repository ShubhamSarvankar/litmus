import os
import re
from dataclasses import dataclass, field
from pathlib import Path


def _env_path(key: str, default: str) -> Path:
    return Path(os.environ.get(key, default))


def _env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, default))


# S3 User Guide, "Amazon S3 multipart upload limits": a part is 5 MiB to 5 GiB, with no
# minimum for the last part.
DEFAULT_MIN_PART_SIZE = 5 * 1024 * 1024

_DIGITS = re.compile(r"[0-9]+")


def _env_min_part_size() -> int:
    """S3_MIN_PART_SIZE in bytes: a non-negative decimal integer, or the server does not
    start."""
    text = os.environ.get("S3_MIN_PART_SIZE")
    if text is None:
        return DEFAULT_MIN_PART_SIZE
    if _DIGITS.fullmatch(text.strip()) is None:
        raise ValueError(f"S3_MIN_PART_SIZE must be a non-negative integer, got {text!r}")
    return int(text)


@dataclass
class Settings:
    # Root of the blob tree (docs/design/storage-layout.md §4). S3_DATA_DIR and
    # S3_PARTS_DIR belonged to the old layout; the server refuses to start if either
    # is set.
    blob_dir: Path = field(default_factory=lambda: _env_path("S3_BLOB_DIR", "data/blobs"))
    db_path: Path = field(default_factory=lambda: _env_path("S3_DB_PATH", "data/litmus.db"))
    port: int = field(default_factory=lambda: _env_int("S3_PORT", 8000))
    host: str = "0.0.0.0"
    # crash_model: only process-kill (SIGKILL) durability is guaranteed
    crash_model: str = "process-kill"
    # The one identity every Owner element reports (ListBuckets, ListObjects). Litmus
    # has no authentication and one account, so the owner never comes from the request.
    owner_id: str = field(default_factory=lambda: os.environ.get("S3_OWNER_ID", "litmus"))
    owner_display_name: str = field(
        default_factory=lambda: os.environ.get("S3_OWNER_DISPLAY_NAME", "litmus")
    )
    # The smallest stored size CompleteMultipartUpload accepts for a listed part other than
    # the last listed one (400 EntityTooSmall). Lowering it diverges from AWS and is meant
    # for tests; 0 disables the check.
    min_part_size: int = field(default_factory=_env_min_part_size)
    # S3_TEST_WRITE_DELAY_MS: Tier B chaos tests only. One sleep of this many ms in
    # BlobStore.write_blob (PUT and UploadPart), after the temp file is created and
    # before any bytes are written to it. A plain request body is already
    # fully buffered by then; a framed (aws-chunked) body is not yet read. Not applied
    # per chunk, and not applied to CompleteMultipartUpload assembly.
    test_write_delay_ms: int = field(default_factory=lambda: _env_int("S3_TEST_WRITE_DELAY_MS", 0))
    # S3_TEST_CRASH_POINT: Tier B chaos tests only. Names a step in the write path at
    # which the server SIGKILLs itself; see server/crash_points.py. Empty = inert.
    test_crash_point: str = field(default_factory=lambda: os.environ.get("S3_TEST_CRASH_POINT", ""))


settings = Settings()
