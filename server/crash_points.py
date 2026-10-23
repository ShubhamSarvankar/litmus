"""Deterministic crash points for Tier B chaos tests.

Setting S3_TEST_CRASH_POINT to one of the names below makes the server SIGKILL
itself when a write reaches that step, so a test can stop the write path at an
exact place instead of racing a timer. When the variable is unset (always, outside
Tier B tests; ceph runs refuse to start with it set) crash_point() does nothing.
"""

import os
import signal

from server.config import settings

# PUT: the new blob is durable at its fresh path; the commit that makes it visible
# has not run.
PUT_AFTER_BYTES = "put-after-bytes"
# CompleteMultipartUpload: parts assembled into a fresh blob; the commit that makes
# it visible has not run.
COMPLETE_AFTER_ASSEMBLE = "complete-after-assemble"
# CompleteMultipartUpload: object row and upload completion committed (one
# transaction); the part blobs and any replaced object blob are not yet unlinked.
COMPLETE_AFTER_COMMIT = "complete-after-commit"

CRASH_POINTS = frozenset({PUT_AFTER_BYTES, COMPLETE_AFTER_ASSEMBLE, COMPLETE_AFTER_COMMIT})


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
