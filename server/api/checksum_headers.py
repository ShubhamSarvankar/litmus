"""The checksum headers of a response (S3 API reference, PutObject, response syntax), the
x-amz-checksum-mode request header of GetObject and HeadObject, and the
x-amz-checksum-algorithm and x-amz-checksum-type headers of CreateMultipartUpload."""

from server.checksums import ALGORITHMS, UNCOMPUTED_NAMES, Algorithm
from server.errors.s3errors import InvalidArgument, InvalidRequest, NotImplementedOperation

CHECKSUM_MODE_HEADER = "x-amz-checksum-mode"
CREATE_ALGORITHM_HEADER = "x-amz-checksum-algorithm"
CREATE_TYPE_HEADER = "x-amz-checksum-type"

COMPOSITE = "COMPOSITE"
FULL_OBJECT = "FULL_OBJECT"
# The type of each computed algorithm when only the algorithm is given, and the types it
# allows (S3 User Guide, "Checking object integrity for data uploads in Amazon S3",
# multipart uploads). The reference names no default type: these are choices.
_DEFAULT_TYPE = {
    "CRC64NVME": FULL_OBJECT,
    "CRC32": COMPOSITE,
    "CRC32C": COMPOSITE,
    "SHA1": COMPOSITE,
    "SHA256": COMPOSITE,
}
_ALLOWED_TYPES = {
    "CRC64NVME": {FULL_OBJECT},
    "CRC32": {FULL_OBJECT, COMPOSITE},
    "CRC32C": {FULL_OBJECT, COMPOSITE},
    "SHA1": {COMPOSITE},
    "SHA256": {COMPOSITE},
}


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


def _create_value(headers, name: str) -> str | None:
    """The stripped value of a CreateMultipartUpload checksum header, None when absent
    or blank. A repeated header arrives joined with ", "."""
    values = [v.strip() for v in headers.getlist(name) if v.strip()]
    return ", ".join(values) if values else None


def create_checksum(headers) -> tuple[str, str] | None:
    """The algorithm and type a CreateMultipartUpload names, in canonical upper case, or
    None when it names no algorithm (S3 API reference, CreateMultipartUpload:
    x-amz-checksum-algorithm and x-amz-checksum-type, the latter COMPOSITE or
    FULL_OBJECT). Both are matched case insensitively after stripping, and blank counts
    as absent. A type alone is 400 InvalidRequest; so is a pair the user guide does not
    allow. A value that is not an AWS algorithm or type name is 400 InvalidArgument, naming
    the header. With no type, CRC64NVME gets FULL_OBJECT and the other four COMPOSITE. The
    reference gives no error or default for any of these: they are choices. The five AWS
    names Litmus does not compute are 501; the request guard answers them first."""
    algorithm = _create_value(headers, CREATE_ALGORITHM_HEADER)
    type_ = _create_value(headers, CREATE_TYPE_HEADER)
    if algorithm is not None:
        given, algorithm = algorithm, algorithm.upper()
        if algorithm in UNCOMPUTED_NAMES:
            raise NotImplementedOperation(extra={"Header": CREATE_ALGORITHM_HEADER})
        if algorithm.lower() not in ALGORITHMS:
            raise InvalidArgument(
                extra={"ArgumentName": CREATE_ALGORITHM_HEADER, "ArgumentValue": given}
            )
    if type_ is not None:
        given, type_ = type_, type_.upper()
        if type_ not in (COMPOSITE, FULL_OBJECT):
            raise InvalidArgument(
                extra={"ArgumentName": CREATE_TYPE_HEADER, "ArgumentValue": given}
            )
    if algorithm is None:
        if type_ is not None:
            raise InvalidRequest(extra={"Header": CREATE_TYPE_HEADER})
        return None
    if type_ is None:
        return algorithm, _DEFAULT_TYPE[algorithm]
    if type_ not in _ALLOWED_TYPES[algorithm]:
        raise InvalidRequest(extra={"Header": CREATE_TYPE_HEADER})
    return algorithm, type_
