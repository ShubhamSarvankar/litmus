"""Deterministic crash points for Tier B chaos tests.

Setting S3_TEST_CRASH_POINT to one of the names below makes the server SIGKILL
itself when a write reaches that step, so a test can stop the write path at an
exact place instead of racing a timer. When the variable is unset (always, outside
Tier B tests; ceph runs refuse to start with it set) crash_point() does nothing.
"""

import os
import signal

from server.config import settings

# Names follow docs/design/storage-layout.md §6 and §15. "after-register": the
# registry row for a new blob is committed, no bytes written. "mid-write" /
# "mid-assemble": the first chunk (or part) is in the temp file. "after-bytes" /
# "after-assemble": the new blob is durable at its fresh path; the commit that makes
# it visible has not run. "after-commit": that commit ran; nothing is unlinked yet.
# "after-gc-unlink": the first replaced blob is unlinked; its row is not purged.

# PutObject (§6.2)
PUT_AFTER_REGISTER = "put-after-register"
PUT_MID_WRITE = "put-mid-write"
PUT_AFTER_BYTES = "put-after-bytes"
PUT_AFTER_COMMIT = "put-after-commit"
PUT_AFTER_GC_UNLINK = "put-after-gc-unlink"
# DeleteObject (§6.3) and DeleteObjects (§6.4)
DELETE_BEFORE_COMMIT = "delete-before-commit"
DELETE_AFTER_COMMIT = "delete-after-commit"
DELETE_OBJECTS_BEFORE_COMMIT = "delete-objects-before-commit"
DELETE_OBJECTS_AFTER_COMMIT = "delete-objects-after-commit"
# UploadPart (§6.6)
PART_AFTER_REGISTER = "part-after-register"
PART_MID_WRITE = "part-mid-write"
PART_AFTER_BYTES = "part-after-bytes"
PART_AFTER_COMMIT = "part-after-commit"
# CompleteMultipartUpload (§6.7): the commit carries the object row and the upload
# completion in one transaction.
COMPLETE_AFTER_REGISTER = "complete-after-register"
COMPLETE_MID_ASSEMBLE = "complete-mid-assemble"
COMPLETE_AFTER_ASSEMBLE = "complete-after-assemble"
COMPLETE_AFTER_COMMIT = "complete-after-commit"
COMPLETE_AFTER_GC_UNLINK = "complete-after-gc-unlink"
# AbortMultipartUpload (§6.8)
ABORT_BEFORE_COMMIT = "abort-before-commit"
ABORT_AFTER_COMMIT = "abort-after-commit"
# DeleteBucket (§6.9): the bucket row and its open uploads are gone in one commit.
BUCKET_DELETE_AFTER_COMMIT = "bucket-delete-after-commit"
# Startup sweep (§7): a file of a pending or garbage row is unlinked; its row is not
# deleted yet.
SWEEP_AFTER_UNLINK = "sweep-after-unlink"
# First start (§7 step 6): the store row is committed as 'initializing'; the
# STORE_ID marker is not written yet.
STORE_INIT_AFTER_COMMIT = "store-init-after-commit"

CRASH_POINTS = frozenset(
    {
        PUT_AFTER_REGISTER,
        PUT_MID_WRITE,
        PUT_AFTER_BYTES,
        PUT_AFTER_COMMIT,
        PUT_AFTER_GC_UNLINK,
        DELETE_BEFORE_COMMIT,
        DELETE_AFTER_COMMIT,
        DELETE_OBJECTS_BEFORE_COMMIT,
        DELETE_OBJECTS_AFTER_COMMIT,
        PART_AFTER_REGISTER,
        PART_MID_WRITE,
        PART_AFTER_BYTES,
        PART_AFTER_COMMIT,
        COMPLETE_AFTER_REGISTER,
        COMPLETE_MID_ASSEMBLE,
        COMPLETE_AFTER_ASSEMBLE,
        COMPLETE_AFTER_COMMIT,
        COMPLETE_AFTER_GC_UNLINK,
        ABORT_BEFORE_COMMIT,
        ABORT_AFTER_COMMIT,
        BUCKET_DELETE_AFTER_COMMIT,
        SWEEP_AFTER_UNLINK,
        STORE_INIT_AFTER_COMMIT,
    }
)


def validate_configured_crash_point() -> None:
    """Refuse to start with an unknown crash point name, so a typo cannot make a
    crash test pass by never crashing."""
    name = settings.test_crash_point
    if name and name not in CRASH_POINTS:
        raise ValueError(
            f"S3_TEST_CRASH_POINT={name!r} is not a known crash point; "
            f"expected one of {sorted(CRASH_POINTS)}"
        )


def crash_point(name: str) -> None:
    """SIGKILL this process if S3_TEST_CRASH_POINT names this step."""
    if name not in CRASH_POINTS:
        raise ValueError(f"unknown crash point {name!r}")
    if settings.test_crash_point == name:
        os.kill(os.getpid(), signal.SIGKILL)


def armed_point(name: str) -> str | None:
    """name if S3_TEST_CRASH_POINT names it, else None. For points that need extra
    work before they fire (flushing the temp file at a mid-write point): an unarmed
    server passes None and does exactly what it does without crash points."""
    if name not in CRASH_POINTS:
        raise ValueError(f"unknown crash point {name!r}")
    return name if settings.test_crash_point == name else None
