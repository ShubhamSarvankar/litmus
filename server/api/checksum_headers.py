"""The checksum headers of a response (S3 API reference, PutObject, response syntax)."""

from server.checksums import Algorithm


def response_header(algorithm: Algorithm) -> str:
    """The response header that carries the checksum of algorithm, in lower case, for
    example x-amz-checksum-crc64nvme."""
    return algorithm.header
