"""The checksum headers of a response (S3 API reference, PutObject, response syntax), the
x-amz-checksum-mode request header of GetObject and HeadObject, and the
x-amz-checksum-algorithm and x-amz-checksum-type headers of CreateMultipartUpload, and the
checksum headers of CompleteMultipartUpload."""

from server.checksums import ALGORITHMS, UNCOMPUTED_NAMES, VALUE_HEADER_PREFIX, Algorithm
from server.errors.s3errors import InvalidArgument, InvalidRequest, NotImplementedOperation
from server.multipart.checksums import CompleteClaims

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


# The checksum value headers of CompleteMultipartUpload (S3 API reference,
# "CompleteMultipartUpload"): the five computed algorithms and the five Litmus does not
# compute.
_COMPLETE_VALUE_ALGORITHMS = (
    *(a.name for a in ALGORITHMS.values()),
    *sorted(UNCOMPUTED_NAMES),
)


def complete_claims(headers, named: str | None) -> CompleteClaims:
    """The checksum headers of a CompleteMultipartUpload, checked for shape; blank counts
    as absent. named is the algorithm the upload named, or None.

    400 InvalidRequest for more than one value header, for a value header in an algorithm
    other than a named upload's, and for MD5, SHA512 or an XXHash on an unnamed upload (S3
    User Guide, "Checking object integrity for data uploads": without the algorithm named
    at CreateMultipartUpload the request "will fail with an InvalidRequest error"). The
    reference documents neither the several headers nor another algorithm on a named
    upload: those are choices. 400 InvalidArgument for a type that is neither COMPOSITE nor
    FULL_OBJECT, in any letter case; whether it equals the upload's is decided later,
    with the values, by verify_claims."""
    given = [
        (name, value)
        for name in _COMPLETE_VALUE_ALGORITHMS
        if (value := _create_value(headers, VALUE_HEADER_PREFIX + name.lower())) is not None
    ]
    if len(given) > 1:
        raise InvalidRequest(extra={"Header": VALUE_HEADER_PREFIX + given[1][0].lower()})
    if given:
        name = given[0][0]
        if (named is not None and name != named) or (named is None and name in UNCOMPUTED_NAMES):
            raise InvalidRequest(extra={"Header": VALUE_HEADER_PREFIX + name.lower()})
    type_ = _create_value(headers, CREATE_TYPE_HEADER)
    if type_ is not None:
        given_type, type_ = type_, type_.upper()
        if type_ not in (COMPOSITE, FULL_OBJECT):
            raise InvalidArgument(
                extra={"ArgumentName": CREATE_TYPE_HEADER, "ArgumentValue": given_type}
            )
    return CompleteClaims(
        algorithm=given[0][0] if given else None,
        value=given[0][1] if given else None,
        type=type_,
    )
