"""The CreateMultipartUpload checksum headers (server/api/checksum_headers.py,
create_checksum): x-amz-checksum-algorithm and x-amz-checksum-type, parsed to the
algorithm and type an upload stores.

The allowed pairs are the S3 User Guide's, "Checking object integrity for data uploads in
Amazon S3": CRC64NVME with FULL_OBJECT; CRC32 and CRC32C with either; SHA1 and SHA256 with
COMPOSITE. The default types and every error are Litmus's choices (docs/compatibility.md).
"""

import pytest
from starlette.datastructures import Headers

from server.api.checksum_headers import create_checksum
from server.errors.s3errors import InvalidArgument, InvalidRequest

ALGORITHMS = ["CRC64NVME", "CRC32", "CRC32C", "SHA1", "SHA256"]
TYPE_FORMS = [None, "COMPOSITE", "FULL_OBJECT"]
ALLOWED = {
    "CRC64NVME": {"FULL_OBJECT"},
    "CRC32": {"COMPOSITE", "FULL_OBJECT"},
    "CRC32C": {"COMPOSITE", "FULL_OBJECT"},
    "SHA1": {"COMPOSITE"},
    "SHA256": {"COMPOSITE"},
}
DEFAULT = {
    "CRC64NVME": "FULL_OBJECT",
    "CRC32": "COMPOSITE",
    "CRC32C": "COMPOSITE",
    "SHA1": "COMPOSITE",
    "SHA256": "COMPOSITE",
}


def headers(algorithm=None, type_=None) -> Headers:
    pairs = []
    if algorithm is not None:
        pairs.append((b"x-amz-checksum-algorithm", algorithm.encode()))
    if type_ is not None:
        pairs.append((b"x-amz-checksum-type", type_.encode()))
    return Headers(raw=pairs)


@pytest.mark.parametrize("type_", TYPE_FORMS, ids=lambda t: t or "no-type")
@pytest.mark.parametrize("algorithm", ALGORITHMS)
def test_each_algorithm_with_each_type_form(algorithm, type_):
    if type_ is not None and type_ not in ALLOWED[algorithm]:
        with pytest.raises(InvalidRequest):
            create_checksum(headers(algorithm, type_))
    else:
        assert create_checksum(headers(algorithm, type_)) == (
            algorithm,
            type_ or DEFAULT[algorithm],
        )


@pytest.mark.parametrize("type_", ["COMPOSITE", "FULL_OBJECT"])
def test_a_type_without_an_algorithm_is_invalid_request(type_):
    with pytest.raises(InvalidRequest):
        create_checksum(headers(type_=type_))


def test_an_unknown_algorithm_is_invalid_argument():
    with pytest.raises(InvalidArgument) as error:
        create_checksum(headers("CRC16"))
    assert error.value.extra == {
        "ArgumentName": "x-amz-checksum-algorithm",
        "ArgumentValue": "CRC16",
    }


@pytest.mark.parametrize("type_", ["PARTIAL", "COMPOSITE, FULL_OBJECT"])
def test_a_value_that_is_not_a_type_is_invalid_argument(type_):
    with pytest.raises(InvalidArgument) as error:
        create_checksum(headers("CRC32", type_))
    assert error.value.extra["ArgumentName"] == "x-amz-checksum-type"


@pytest.mark.parametrize(
    ("algorithm", "type_", "want"),
    [
        ("crc32", "composite", ("CRC32", "COMPOSITE")),
        ("Crc32c", "Full_Object", ("CRC32C", "FULL_OBJECT")),
        ("  sha1 ", " composite  ", ("SHA1", "COMPOSITE")),
        ("sha256", None, ("SHA256", "COMPOSITE")),
        ("crc64nvme", "full_object", ("CRC64NVME", "FULL_OBJECT")),
        ("  ", "   ", None),
        ("crc32", "", ("CRC32", "COMPOSITE")),
    ],
    ids=["lower", "mixed", "padded", "lower-no-type", "lower-crc64", "blank-both", "blank-type"],
)
def test_values_are_case_insensitive_and_blank_is_absent(algorithm, type_, want):
    assert create_checksum(headers(algorithm, type_)) == want
