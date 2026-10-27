"""server/checksums.py: the five algorithms against published check values, and the
rules for reading a request's checksum claims from its headers.

Check values (digest of the bytes b"123456789" for the CRCs, b"abc" for the SHAs):
  CRC-32    0xCBF43926  reveng CRC catalogue, CRC-32/ISO-HDLC "check"
  CRC-32C   0xE3069283  reveng CRC catalogue, CRC-32/ISCSI "check"
  CRC-64    0xAE8B14860A799888  reveng CRC catalogue, CRC-64/NVME "check" (NVM Express
            NVM Command Set specification)
  SHA-1     a9993e36...  FIPS 180-4 example, "abc"
  SHA-256   ba7816bf...  FIPS 180-4 example, "abc"
and, for the empty input, the well known digests of nothing.
"""

import base64
import hashlib
import zlib

import pytest
from starlette.datastructures import Headers

from server.checksums import (
    ALGORITHMS,
    CRC64NVME,
    Algorithm,
    UploadChecks,
    unsupported_value_header,
    value_headers,
)
from server.errors.s3errors import (
    BadDigest,
    InvalidDigest,
    InvalidRequest,
    NotImplementedOperation,
)


def b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode()


def digest(name: str, data: bytes, pieces=None) -> bytes:
    h = ALGORITHMS[name].new()
    for piece in pieces or [data]:
        h.update(piece)
    return h.digest()


CHECK = b"123456789"
VECTORS = [
    # name, input, digest as hex
    ("crc32", CHECK, "cbf43926"),
    ("crc32c", CHECK, "e3069283"),
    ("crc64nvme", CHECK, "ae8b14860a799888"),
    ("sha1", b"abc", "a9993e364706816aba3e25717850c26c9cd0d89d"),
    ("sha256", b"abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
    ("crc32", b"", "00000000"),
    ("crc32c", b"", "00000000"),
    ("crc64nvme", b"", "0000000000000000"),
    ("sha1", b"", "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
    ("sha256", b"", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
]


@pytest.mark.parametrize("name, data, expected", VECTORS)
def test_published_check_values(name, data, expected):
    assert digest(name, data).hex() == expected
    assert len(digest(name, data)) == ALGORITHMS[name].size


@pytest.mark.parametrize("name", sorted(ALGORITHMS))
def test_incremental_equals_one_shot(name):
    data = bytes(range(256)) * 50
    pieces = [data[:1], data[1:7], b"", data[7:4096], data[4096:]]
    assert digest(name, data, pieces) == digest(name, data)


def test_wire_forms_from_published_sources():
    assert b64(digest("crc32", CHECK)) == "y/Q5Jg=="
    assert b64(digest("crc32c", CHECK)) == "4waSgw=="
    assert b64(digest("crc64nvme", CHECK)) == "rosUhgp5mIg="
    assert b64(digest("sha1", b"abc")) == "qZk+NkcGgWq6PiVxeFDCbJzQ2J0="
    assert b64(digest("sha256", b"abc")) == "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="


def test_crc32_matches_zlib_and_crc32c_matches_rfc_3720_zeros():
    data = bytes(range(256)) * 100
    assert digest("crc32", data) == zlib.crc32(data).to_bytes(4, "big")
    # RFC 3720 B.4: 32 bytes of zeros have CRC32C 0x8A9136AA.
    assert digest("crc32c", bytes(32)).hex() == "8a9136aa"


def test_values_from_the_ceph_suite():
    # s3-tests test_s3.py test_object_checksum_sha256 / _crc64nvme: 1024 bytes of "A".
    data = b"A" * 1024
    assert b64(digest("sha256", data)) == "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0="
    assert b64(digest("crc64nvme", data)) == "Qeh8oXvGiSo="


def test_algorithm_table():
    assert sorted(ALGORITHMS) == ["crc32", "crc32c", "crc64nvme", "sha1", "sha256"]
    assert {a.name: a.header for a in ALGORITHMS.values()} == {
        "CRC32": "x-amz-checksum-crc32",
        "CRC32C": "x-amz-checksum-crc32c",
        "CRC64NVME": "x-amz-checksum-crc64nvme",
        "SHA1": "x-amz-checksum-sha1",
        "SHA256": "x-amz-checksum-sha256",
    }


# --- which headers are checksum value headers ---


def h(*pairs, **kw) -> Headers:
    return Headers(raw=[(k.lower().encode(), v.encode()) for k, v in [*pairs, *kw.items()]])


def test_mode_algorithm_and_type_are_not_value_headers():
    headers = h(
        ("x-amz-checksum-mode", "ENABLED"),
        ("x-amz-checksum-algorithm", "CRC32"),
        ("x-amz-checksum-type", "COMPOSITE"),
        ("x-amz-sdk-checksum-algorithm", "CRC32"),
        ("x-amz-checksum", "x"),
    )
    assert value_headers(headers) == []
    assert unsupported_value_header(headers) is None


def test_value_headers_are_found_in_any_letter_case_and_once_per_occurrence():
    headers = Headers(
        raw=[(b"X-Amz-Checksum-CRC32", b"a"), (b"x-amz-checksum-crc32", b"a"), (b"etag", b"x")]
    )
    assert value_headers(headers) == ["x-amz-checksum-crc32", "x-amz-checksum-crc32"]


@pytest.mark.parametrize(
    "name", ["md5", "sha512", "xxhash64", "xxhash3", "xxhash128", "crc32x", "foo", "crc32 "]
)
def test_other_algorithms_are_unsupported(name):
    headers = h(("x-amz-checksum-crc32", "AAAAAA=="), (f"x-amz-checksum-{name}", "x"))
    assert unsupported_value_header(headers) == f"x-amz-checksum-{name}"


def test_the_five_are_supported():
    headers = h(*((a.header, "x") for a in ALGORITHMS.values()))
    assert unsupported_value_header(headers) is None


# --- from_headers ---

DATA = b"hello world"


def claim(name: str, data: bytes = DATA) -> tuple[str, str]:
    return ALGORITHMS[name].header, b64(digest(name, data))


class FakeWriter:
    def __init__(self, data: bytes):
        self.md5_digest = hashlib.md5(data).digest()


def run(headers: Headers, data: bytes = DATA, trailer=None, trailer_value=None) -> None:
    checks = UploadChecks.from_headers(headers, trailer=trailer)
    checks.feed(data[:3])
    checks.feed(data[3:])
    checks.verify(FakeWriter(data), trailer_value=trailer_value)


def test_no_claims_means_nothing_to_check():
    run(h())
    run(h(("content-type", "text/plain")))


@pytest.mark.parametrize("name", sorted(ALGORITHMS))
def test_a_matching_header_passes_and_a_wrong_one_is_a_bad_digest(name):
    run(h(claim(name)))
    run(h(claim(name), ("x-amz-sdk-checksum-algorithm", name.upper())))
    run(h(claim(name), ("x-amz-sdk-checksum-algorithm", name)))  # any letter case
    with pytest.raises(BadDigest) as caught:
        run(h(claim(name, DATA + b"x")))
    assert caught.value.extra == {"Header": f"x-amz-checksum-{name}"}


def test_content_md5_alone_and_with_one_checksum_both_checked():
    md5 = ("Content-MD5", b64(hashlib.md5(DATA).digest()))
    wrong_md5 = ("Content-MD5", b64(hashlib.md5(b"other").digest()))
    run(h(md5))
    run(h(md5, claim("sha256")))
    for headers in (h(wrong_md5), h(wrong_md5, claim("sha256")), h(md5, claim("sha256", b"x"))):
        with pytest.raises(BadDigest):
            run(headers)


@pytest.mark.parametrize("name", sorted(ALGORITHMS))
@pytest.mark.parametrize(
    "value",
    [
        "not*base64!",
        "",
        " ",
        "AAAA",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==",
    ],
)
def test_a_malformed_value_is_an_invalid_digest(name, value):
    with pytest.raises(InvalidDigest) as caught:
        run(h((f"x-amz-checksum-{name}", value)))
    assert caught.value.extra == {"Header": f"x-amz-checksum-{name}"}


def test_a_value_is_base64_of_the_right_size_only():
    good = claim("crc32")[1]
    for value in (good.rstrip("="), " " + good, good + " ", good[:2], b64(bytes(5)), b64(bytes(8))):
        with pytest.raises(InvalidDigest):
            run(h(("x-amz-checksum-crc32", value)))


@pytest.mark.parametrize(
    "value", ["YWJyYWNhZGFicmE=", "", "AWS HAHAHA", b64(bytes(15)), b64(bytes(17)), "not*base64!"]
)
def test_a_malformed_content_md5_is_an_invalid_digest(value):
    # The first is the short digest the ceph suite sends (test_object_create_bad_md5_invalid_short).
    with pytest.raises(InvalidDigest) as caught:
        run(h(("Content-MD5", value)))
    assert caught.value.extra == {"Header": "Content-MD5"}


def test_repeated_content_md5_is_not_one_valid_value():
    good = b64(hashlib.md5(DATA).digest())
    with pytest.raises(InvalidDigest):
        run(h(("Content-MD5", good), ("Content-MD5", good)))


def test_the_sdk_header_needs_a_checksum_header_or_trailer():
    sdk = ("x-amz-sdk-checksum-algorithm", "CRC32")
    for headers in (h(sdk), h(sdk, ("Content-MD5", b64(hashlib.md5(DATA).digest())))):
        with pytest.raises(InvalidRequest) as caught:
            run(headers)
        assert caught.value.extra == {"Header": "x-amz-sdk-checksum-algorithm"}
    with pytest.raises(InvalidRequest):
        run(h(claim("crc32"), sdk, sdk))


@pytest.mark.parametrize("sdk", ["SHA256", "CRC32C", "CRC64NVME", "MD5", "", "nonsense"])
def test_the_sdk_header_naming_another_algorithm_is_a_bad_digest(sdk):
    with pytest.raises(BadDigest) as caught:
        run(h(claim("crc32"), ("x-amz-sdk-checksum-algorithm", sdk)))
    assert caught.value.extra == {"Header": "x-amz-sdk-checksum-algorithm"}


def test_more_than_one_checksum_header_is_an_invalid_request():
    for headers in (
        h(claim("crc32"), claim("sha256")),
        h(claim("crc32"), claim("crc32")),
        h(claim("crc32"), claim("crc32c"), claim("sha1")),
    ):
        with pytest.raises(InvalidRequest):
            run(headers)


def test_an_unsupported_algorithm_is_not_implemented():
    # request_guard answers this before the body is read; from_headers is the backstop.
    for name in ("sha512", "md5", "xxhash3"):
        with pytest.raises(NotImplementedOperation) as caught:
            run(h(claim("crc32"), (f"x-amz-checksum-{name}", "x")))
        assert caught.value.extra == {"Header": f"x-amz-checksum-{name}"}


def test_non_value_headers_are_ignored():
    run(h(("x-amz-checksum-mode", "ENABLED"), ("x-amz-checksum-type", "FULL_OBJECT")))
    run(h(claim("crc32"), ("x-amz-checksum-algorithm", "SHA256")))


# --- a trailer ---


def test_trailer_value_is_checked_against_the_body():
    good = digest("crc64nvme", DATA)
    run(h(), trailer=CRC64NVME, trailer_value=good)
    with pytest.raises(BadDigest):
        run(h(), trailer=CRC64NVME, trailer_value=digest("crc64nvme", b"other"))


def test_trailer_with_content_md5_checks_both():
    good = digest("crc64nvme", DATA)
    md5 = ("Content-MD5", b64(hashlib.md5(DATA).digest()))
    run(h(md5), trailer=CRC64NVME, trailer_value=good)
    wrong = ("Content-MD5", b64(hashlib.md5(b"x").digest()))
    with pytest.raises(BadDigest):
        run(h(wrong), trailer=CRC64NVME, trailer_value=good)


def test_a_value_header_together_with_a_trailer_is_an_invalid_request():
    for name in sorted(ALGORITHMS):
        with pytest.raises(InvalidRequest):
            run(h(claim(name)), trailer=CRC64NVME, trailer_value=digest("crc64nvme", DATA))


def test_the_sdk_header_is_compared_with_the_trailer_algorithm():
    good = digest("crc64nvme", DATA)
    run(h(("x-amz-sdk-checksum-algorithm", "CRC64NVME")), trailer=CRC64NVME, trailer_value=good)
    with pytest.raises(BadDigest):
        run(h(("x-amz-sdk-checksum-algorithm", "CRC32")), trailer=CRC64NVME, trailer_value=good)


# --- the stored checksum ---


def stored(headers: Headers, data: bytes = DATA, trailer=None, trailer_value=None):
    checks = UploadChecks.from_headers(headers, trailer=trailer)
    checks.feed(data[:3])
    checks.feed(data[3:])
    checks.verify(FakeWriter(data), trailer_value=trailer_value)
    return checks.stored_checksum()


def test_the_stored_algorithm_defaults_to_crc64nvme():
    for headers in (h(), h(("Content-MD5", b64(hashlib.md5(DATA).digest())))):
        algorithm, value = stored(headers)
        assert algorithm is CRC64NVME
        assert value == b64(digest("crc64nvme", DATA))


@pytest.mark.parametrize("name", sorted(ALGORITHMS))
def test_the_stored_algorithm_is_the_claimed_one(name):
    algorithm, _ = stored(h(claim(name)))
    assert algorithm is ALGORITHMS[name]
    assert Algorithm.by_name(name.upper()) is algorithm
    assert Algorithm.by_name(name.capitalize()) is algorithm


@pytest.mark.parametrize("name", sorted(ALGORITHMS))
def test_the_stored_value_is_the_base64_of_the_computed_digest(name):
    # A trailer: the value stored is the digest computed here, as canonical base64.
    _, value = stored(h(), trailer=ALGORITHMS[name], trailer_value=digest(name, DATA))
    assert value == b64(digest(name, DATA))
    assert base64.b64decode(value, validate=True) == digest(name, DATA)


def test_delete_objects_stores_nothing():
    checks = UploadChecks.from_headers(h(claim("sha256")))
    checks.verify_body(DATA)
    assert checks.stored_checksum() is None
    checks = UploadChecks.from_headers(h())
    checks.verify_body(DATA)
    assert checks.stored_checksum() is None
