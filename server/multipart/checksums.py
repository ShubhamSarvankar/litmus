"""The checksum of a completed multipart upload, and the checks CompleteMultipartUpload
makes against the checksums the client claims (docs/design/storage-layout.md §20).

An upload is named when CreateMultipartUpload sent a non-blank x-amz-checksum-algorithm;
E is the named algorithm, else CRC64NVME. UploadPart stores the part's checksum in E, so
the object's checksum is built from the stored part values and never from the bytes:

  COMPOSITE    the digest, in the upload's algorithm, of the concatenated raw part
               digests in listed order, base64 encoded, plus "-N" for the N listed parts.
               That wire form, with the suffix, is what is stored and returned.
  FULL_OBJECT  the CRC of the whole object, folded from the part CRCs and part sizes with
               awscrt's combine functions.

A listed part whose stored checksum is NULL (a part uploaded before schema version 3)
gives no object checksum at all. Parts that are not listed take no part.
"""

import base64
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from awscrt import checksums as crt

from server.checksums import CRC64NVME, Algorithm
from server.errors.s3errors import BadDigest, InvalidRequest

COMPOSITE = "COMPOSITE"
FULL_OBJECT = "FULL_OBJECT"
TYPE_HEADER = "x-amz-checksum-type"

# The ten Checksum* elements of a CompletedPart (S3 API reference, "CompletedPart").
PART_ELEMENTS = (
    "CRC32",
    "CRC32C",
    "CRC64NVME",
    "MD5",
    "SHA1",
    "SHA256",
    "SHA512",
    "XXHASH128",
    "XXHASH3",
    "XXHASH64",
)

_COMBINE: dict[str, Callable[[int, int, int], int]] = {
    "CRC32": crt.combine_crc32,
    "CRC32C": crt.combine_crc32c,
    "CRC64NVME": crt.combine_crc64nvme,
}


@dataclass(frozen=True)
class CompleteClaims:
    """The checksum headers of a Complete, already checked for shape: at most one value
    header, in an algorithm the upload allows, and a type that is a valid name. algorithm
    and value are None without a value header; type is None without a type header."""

    algorithm: str | None = None
    value: str | None = None
    type: str | None = None


@dataclass(frozen=True)
class ObjectChecksum:
    algorithm: str
    value: str
    type: str


@dataclass(frozen=True)
class PartRef:
    """A listed part as the upload stored it: its checksum, or None, and its size."""

    checksum: str | None
    size: int


def object_checksum(algorithm: Algorithm, type_: str, parts: Sequence[PartRef]) -> str | None:
    """The wire form of the object's checksum for the listed parts in order, or None when
    any of them has no stored checksum."""
    if any(part.checksum is None for part in parts):
        return None
    if type_ == COMPOSITE:
        hasher = algorithm.new()
        for part in parts:
            hasher.update(base64.b64decode(part.checksum))
        return f"{base64.b64encode(hasher.digest()).decode('ascii')}-{len(parts)}"
    combine = _COMBINE[algorithm.name]
    value = int.from_bytes(base64.b64decode(parts[0].checksum), "big")
    for part in parts[1:]:
        value = combine(value, int.from_bytes(base64.b64decode(part.checksum), "big"), part.size)
    return base64.b64encode(value.to_bytes(algorithm.size, "big")).decode("ascii")


def verify_part_elements(
    listed: Sequence[dict], stored: Sequence[str | None], named: str | None
) -> None:
    """The Part elements of a Complete against the stored part checksums. Each listed
    part is a dict with its PartNumber and its Checksums, the non-blank Checksum*
    elements by algorithm name.

    A named upload refuses (InvalidRequest) an element in any other algorithm, in any
    Part, before it compares any value, and then refuses (BadDigest) the first element in
    its own algorithm that differs from the stored value. An unnamed upload compares
    ChecksumCRC64NVME, the checksum it stores, skips a part that stored none, and accepts
    the elements of every other algorithm unverified."""
    if named is not None:
        for part in listed:
            if others := sorted(part["Checksums"].keys() - {named}):
                raise InvalidRequest(extra={"Element": f"Checksum{others[0]}"})
    compared = named or CRC64NVME.name
    for part, value in zip(listed, stored, strict=True):
        claimed = part["Checksums"].get(compared)
        if claimed is not None and value is not None and claimed != value:
            raise BadDigest(
                extra={"Element": f"Checksum{compared}", "PartNumber": str(part["PartNumber"])}
            )


def verify_claims(
    claims: CompleteClaims, upload_type: str | None, named: bool, checksum: str | None
) -> None:
    """BadDigest unless the type header and the value header agree with the upload and
    the object checksum. A named upload checks the type, then the value, which is
    compared in wire form. An unnamed upload ignores a type and validates only
    x-amz-checksum-crc64nvme, which names the checksum Litmus stores; the other value
    headers it accepts are not validated, and an object without a checksum has nothing to
    compare."""
    if named:
        if claims.type is not None and claims.type != upload_type:
            raise BadDigest(extra={"Header": TYPE_HEADER})
        if claims.value is not None and claims.value != checksum:
            raise BadDigest(extra={"Header": f"x-amz-checksum-{claims.algorithm.lower()}"})
    elif claims.algorithm == CRC64NVME.name and checksum is not None and claims.value != checksum:
        raise BadDigest(extra={"Header": CRC64NVME.header})
