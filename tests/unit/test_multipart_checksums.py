"""server/multipart/checksums.py: the checksum of a completed multipart upload, built from
the stored part checksums (docs/design/storage-layout.md §20).

COMPOSITE is the digest of the concatenated raw part digests plus "-N"; FULL_OBJECT is
the CRC of the whole object folded from the part CRCs and sizes. The first six cases are
the vectors of ceph/s3-tests, as literals, with the part values the tests upload. The
expected values of the rest come from hashlib, zlib and awscrt called directly on the
bytes, never from the module under test.
"""

import base64
import hashlib
import random
import zlib

import pytest
from awscrt import checksums as crt

from server.checksums import ALGORITHMS
from server.multipart.checksums import COMPOSITE, FULL_OBJECT, PartRef, object_checksum

MIB = 1024 * 1024


def b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


# Each: algorithm, type, the part checksums and sizes the test uploads, the vector.
VECTORS = [
    pytest.param(
        "sha256",
        COMPOSITE,
        [("arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=", 1024)],
        "Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1",
        id="sha256-composite-1-part",
    ),  # .s3tests/s3tests/functional/test_s3.py, test_multipart_checksum_sha256
    pytest.param(
        "sha256",
        COMPOSITE,
        [
            ("275VF5loJr1YYawit0XSHREhkFXYkkPKGuoK0x9VKxI=", 5 * MIB),
            ("mrHwOfjTL5Zwfj74F05HOQGLdUb7E5szdCbxgUSq6NM=", 5 * MIB),
            ("Vw7oB/nKQ5xWb3hNgbyfkvDiivl+U+/Dft48nfJfDow=", 5 * MIB),
        ],
        "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3",
        id="sha256-composite-3-parts",
    ),  # test_s3.py, test_multipart_reupload_checksum_and_etag (and ..._use_cksum_helper_sha256)
    pytest.param(
        "sha1",
        COMPOSITE,
        [
            ("iIaTCGbm+vdVjNqIMF2S0T7ibMk=", 5 * MIB),
            ("LS/TJ32bAVKEwRu+sE3X7awh/lk=", 5 * MIB),
            ("6DDwovUaHwrKNXDMzOGbuvj9kxI=", 5 * MIB),
        ],
        "sizjvY4eud3MrcHdZM3cQ/ol39o=-3",
        id="sha1-composite-3-parts",
    ),  # test_s3.py, test_multipart_use_cksum_helper_sha1
    pytest.param(
        "crc32",
        FULL_OBJECT,
        [("JRTCyQ==", 5 * MIB), ("QoZTGg==", 5 * MIB), ("YAgjqw==", 5 * MIB)],
        "WgDhBQ==",
        id="crc32-full-object",
    ),  # test_s3.py, test_multipart_use_cksum_helper_crc32
    pytest.param(
        "crc32c",
        FULL_OBJECT,
        [("MDaLrw==", 5 * MIB), ("TH4EZg==", 5 * MIB), ("Z7mBIQ==", 5 * MIB)],
        "xU+Krw==",
        id="crc32c-full-object",
    ),  # test_s3.py, test_multipart_use_cksum_helper_crc32c
    pytest.param(
        "crc64nvme",
        FULL_OBJECT,
        [("L/E4WYn8v98=", 5 * MIB), ("xW1l19VobYM=", 5 * MIB), ("cK5MnNaWrW4=", 5 * MIB)],
        "i+6LR0y3eFo=",
        id="crc64nvme-full-object",
    ),  # test_s3.py, test_multipart_use_cksum_helper_crc64nvme
]


@pytest.mark.parametrize("name, type_, parts, expected", VECTORS)
def test_the_ceph_vectors(name, type_, parts, expected):
    refs = [PartRef(checksum, size) for checksum, size in parts]
    assert object_checksum(ALGORITHMS[name], type_, refs) == expected


def direct(name: str, data: bytes) -> bytes:
    """The digest of data in algorithm name, from the libraries directly."""
    if name == "crc32":
        return zlib.crc32(data).to_bytes(4, "big")
    if name == "crc32c":
        return crt.crc32c(data).to_bytes(4, "big")
    if name == "crc64nvme":
        return crt.crc64nvme(data).to_bytes(8, "big")
    return hashlib.new(name, data).digest()


COMPOSITE_ALGORITHMS = ["crc32", "crc32c", "sha1", "sha256"]


@pytest.mark.parametrize("name", COMPOSITE_ALGORITHMS)
@pytest.mark.parametrize("count", [1, 2, 3, 10])
def test_the_composite_suffix_is_the_listed_count_and_the_digest_is_of_the_part_digests(
    name, count
):
    rng = random.Random(f"{name}{count}")
    chunks = [rng.randbytes(rng.randrange(1, 300)) for _ in range(count)]
    digests = [direct(name, chunk) for chunk in chunks]
    parts = [PartRef(b64(d), len(c)) for d, c in zip(digests, chunks, strict=True)]
    value = object_checksum(ALGORITHMS[name], COMPOSITE, parts)
    assert value == b64(direct(name, b"".join(digests))) + f"-{count}"
    assert value.rsplit("-", 1)[1] == str(count)


# Part sizes of a whole object: a zero length part, 1 byte parts and the 64 KiB boundaries
# of the CRC implementations' block sizes.
PATTERNS = [
    (0,),
    (1,),
    (2,),
    (65535,),
    (65536,),
    (65537,),
    (131072,),
    (1, 1),
    (1, 2),
    (2, 1),
    (10, 3000),
    (3000, 10),
    (65535, 1),
    (65536, 1),
    (65537, 1),
    (1, 65535),
    (1, 65536),
    (1, 65537),
    (65536, 65536),
    (65535, 65535),
    (65537, 65537),
    (131071, 131073),
    (100, 0),
    (65536, 0),
    (1, 0),
    (0, 1),
    (0, 0),
    (3000, 10, 0),
    (1, 1, 1),
    (10, 3000, 1),
    (65536, 65536, 65536),
    (65536, 65536, 1),
    (1, 65536, 65536),
    (65535, 65536, 65537),
    (0, 65536, 0),
    (7, 13, 29, 31),
    (1, 2, 3, 4, 5),
    (65536, 1, 65536, 1),
    (1, 0, 1, 0, 1),
    (4096, 4096, 4096, 4096, 4096, 4097),
]


@pytest.mark.parametrize("name", ["crc32", "crc32c", "crc64nvme"])
@pytest.mark.parametrize("sizes", PATTERNS, ids=lambda sizes: "-".join(map(str, sizes)))
def test_the_combined_crc_equals_the_crc_of_the_whole_bytes(name, sizes):
    rng = random.Random(f"{name}{sizes}")
    chunks = [rng.randbytes(size) for size in sizes]
    parts = [PartRef(b64(direct(name, chunk)), len(chunk)) for chunk in chunks]
    value = object_checksum(ALGORITHMS[name], FULL_OBJECT, parts)
    assert value == b64(direct(name, b"".join(chunks)))


def test_a_part_without_a_checksum_gives_no_object_checksum():
    algorithm = ALGORITHMS["crc32"]
    parts = [PartRef(b64(direct("crc32", b"a")), 1), PartRef(None, 1)]
    assert object_checksum(algorithm, FULL_OBJECT, parts[:1]) is not None
    assert object_checksum(algorithm, FULL_OBJECT, parts) is None


def test_the_unlisted_parts_do_not_take_part():
    stored = {n: bytes([n]) * n for n in (1, 2, 3)}
    refs = {n: PartRef(b64(direct("crc64nvme", data)), len(data)) for n, data in stored.items()}
    listed = [refs[1], refs[3]]
    value = object_checksum(ALGORITHMS["crc64nvme"], FULL_OBJECT, listed)
    assert value == b64(direct("crc64nvme", stored[1] + stored[3]))
    assert value != b64(direct("crc64nvme", stored[1] + stored[2] + stored[3]))
