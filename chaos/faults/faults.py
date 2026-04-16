import time


class RaiseOSError:
    """Raise an OSError with the given errno code."""

    def __init__(self, errno_code: int):
        self._errno_code = errno_code

    def execute(self) -> None:
        import errno as _errno

        raise OSError(self._errno_code, _errno.errorcode.get(self._errno_code, "OSError"))


class CorruptBytes:
    """Record that a corruption should occur at the given offset with the given pattern.

    Tier A only — actual byte corruption is applied by test code directly to
    the stored file, not via this fault's execute(). execute() is a no-op here;
    the class exists as a marker for the injector framework.
    """

    def __init__(self, offset: int, pattern: bytes):
        self.offset = offset
        self.pattern = pattern

    def execute(self) -> None:
        # Corruption is applied directly to the file by test code after the write.
        pass


class HangForever:
    """Sleep indefinitely — simulates a hung write."""

    def execute(self) -> None:
        time.sleep(float("inf"))


class PartialWrite:
    """Raise OSError after max_bytes have been written, simulating a partial write."""

    def __init__(self, max_bytes: int):
        self._max_bytes = max_bytes

    def execute(self) -> None:
        import errno

        raise OSError(errno.EIO, "Simulated partial write")
