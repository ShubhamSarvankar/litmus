"""Builders for aws-chunked request bodies (STREAMING-UNSIGNED-PAYLOAD-TRAILER with a
CRC64NVME trailer), shared by the framed tests."""

from collections.abc import Callable

from server.crc64nvme import Crc64Nvme

TRAILER = "x-amz-checksum-crc64nvme"
SHA = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"


def crc_b64(data: bytes) -> str:
    crc = Crc64Nvme()
    crc.update(data)
    return crc.b64digest()


def frame(
    data: bytes,
    sizes: list[int] | None = None,
    trailer_line: str | None = None,
    size_line: Callable[[int], str] = lambda n: f"{n:x}",
    tail: bytes = b"",
) -> bytes:
    """The framed form of data: one chunk by default, else chunks of the given sizes."""
    sizes = sizes or [len(data)]
    assert sum(sizes) == len(data)
    out, pos = b"", 0
    for size in sizes:
        out += size_line(size).encode() + b"\r\n" + data[pos : pos + size] + b"\r\n"
        pos += size
    if trailer_line is None:
        trailer_line = f"{TRAILER}:{crc_b64(data)}"
    return out + b"0\r\n" + trailer_line.encode() + b"\r\n\r\n" + tail


def framed_headers(body: bytes, decoded: int, **override) -> dict:
    """The headers of the AWS CLI's framed request; an override of None drops one."""
    headers = {
        "x-amz-content-sha256": SHA,
        "content-encoding": "aws-chunked",
        "x-amz-decoded-content-length": str(decoded),
        "x-amz-trailer": TRAILER,
        "content-length": str(len(body)),
    }
    headers.update(override)
    return {k: v for k, v in headers.items() if v is not None}


def framed_request(data: bytes, **kw) -> tuple[dict, bytes]:
    """(headers, body) of a one chunk framed upload of data."""
    body = frame(data, **kw)
    return framed_headers(body, len(data)), body
