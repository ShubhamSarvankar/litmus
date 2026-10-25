"""Incremental CRC64NVME, the checksum of an aws-chunked trailer
(x-amz-checksum-crc64nvme), computed by awscrt.

The wire form is the 8 byte big endian CRC, base64 encoded.
"""

import base64
import binascii

from awscrt import checksums

DIGEST_SIZE = 8


class Crc64Nvme:
    def __init__(self) -> None:
        self._crc = 0

    def update(self, data: bytes) -> None:
        self._crc = checksums.crc64nvme(data, self._crc)

    def digest(self) -> bytes:
        return self._crc.to_bytes(DIGEST_SIZE, "big")

    def b64digest(self) -> str:
        return base64.b64encode(self.digest()).decode("ascii")


def decode_b64_digest(value: str, size: int = DIGEST_SIZE) -> bytes | None:
    """The size bytes a checksum value encodes (8 for a CRC64NVME trailer), or None if
    it is not strict base64 of exactly that many bytes."""
    try:
        raw = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return None
    return raw if len(raw) == size else None
