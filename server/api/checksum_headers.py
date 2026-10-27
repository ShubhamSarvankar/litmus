"""The checksum headers of a response (S3 API reference, PutObject, response syntax) and
the x-amz-checksum-mode request header of GetObject and HeadObject."""

from server.checksums import Algorithm
from server.errors.s3errors import InvalidArgument

CHECKSUM_MODE_HEADER = "x-amz-checksum-mode"


def response_header(algorithm: Algorithm) -> str:
    """The response header that carries the checksum of algorithm, in lower case, for
    example x-amz-checksum-crc64nvme."""
    return algorithm.header


def checksum_mode_enabled(headers) -> bool:
    """Whether a GetObject or HeadObject request asks for the stored checksum (S3 API
    reference, GetObject: "To retrieve the checksum, this mode must be enabled"; the
    valid value is ENABLED).

    Absent, or blank once stripped, is not enabled. ENABLED in any letter case, stripped,
    is enabled; the CLI sends it in lower case. Any other value, including a repeated
    header, which arrives joined with ", ", is 400 InvalidArgument. The reference names
    no error for a value other than ENABLED: refusing it is a choice."""
    values = [v.strip() for v in headers.getlist(CHECKSUM_MODE_HEADER) if v.strip()]
    if not values:
        return False
    value = ", ".join(values)
    if value.upper() != "ENABLED":
        raise InvalidArgument(extra={"ArgumentName": CHECKSUM_MODE_HEADER, "ArgumentValue": value})
    return True
