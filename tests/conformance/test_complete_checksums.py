"""CompleteMultipartUpload and checksums (docs/design/storage-layout.md decision 32, §20).

An upload is named when CreateMultipartUpload sent a non blank x-amz-checksum-algorithm,
and E is the named algorithm, else CRC64NVME. UploadPart stores E and the part's digest;
Complete checks the Part elements and the checksum headers against them, stores the
object's checksum and returns it (S3 API reference, "CompleteMultipartUpload",
"CompletedPart"; S3 User Guide, "Checking object integrity for data uploads").

COMPOSITE: the digest of the concatenated raw part digests in the upload's algorithm, plus
"-N". FULL_OBJECT: the CRC of the whole listed bytes. The expectations are computed here
from zlib, hashlib and awscrt on the bytes, with no import of server.multipart, so the file
also runs against a server that does not have the feature.

The groups: the Part elements, the headers and the type, the part number rules, the order
of the errors (adjacent pairs), the values on the wire and on GET and HEAD, and boto3.
"""

import base64
import hashlib
import socket
import sqlite3
import threading
import time
import xml.etree.ElementTree as ET
import zlib
from dataclasses import dataclass
from pathlib import Path

import boto3
import pytest
import uvicorn
from awscrt import checksums as crt
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient

from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "complete-checksums"
KEY = "obj.bin"
NS = "http://s3.amazonaws.com/doc/2006-03-01/"
# Parts of different sizes; the fixture of test_complete_multipart_headers.py.
PARTS = [b"a" * 10, b"b" * 3000, b"c" * 1]
SIZES = {"crc32": 4, "crc32c": 4, "crc64nvme": 8, "sha1": 20, "sha256": 32}
MODE = {"x-amz-checksum-mode": "ENABLED"}


# --- the expected values, from the libraries directly ---


def raw(name: str, data: bytes) -> bytes:
    if name == "crc32":
        return zlib.crc32(data).to_bytes(4, "big")
    if name == "crc32c":
        return crt.crc32c(data).to_bytes(4, "big")
    if name == "crc64nvme":
        return crt.crc64nvme(data).to_bytes(8, "big")
    return hashlib.new(name, data).digest()


def b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def part_value(name: str, data: bytes) -> str:
    return b64(raw(name, data))


def object_value(name: str, type_: str, datas: list[bytes]) -> str:
    """The object checksum in wire form for the listed parts' bytes."""
    if type_ == "COMPOSITE":
        return b64(raw(name, b"".join(raw(name, d) for d in datas))) + f"-{len(datas)}"
    return b64(raw(name, b"".join(datas)))


def wrong(name: str) -> str:
    """A well formed value that is no object's or part's checksum."""
    return b64(bytes(SIZES[name]))


JUNK = "!!not base64!!"


# --- the store and the uploads ---


def build(tmp_path: Path, min_part_size: int = 0):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata, min_part_size=min_part_size),
        raise_server_exceptions=False,
    )
    assert client.put(f"/{BUCKET}").status_code == 200
    return client, storage, metadata, tmp_path / "meta.db"


@pytest.fixture
def env(tmp_path: Path):
    return build(tmp_path)


def code(resp) -> str | None:
    return ET.fromstring(resp.text).findtext("Code")


@dataclass
class Up:
    uid: str
    name: str | None  # the named algorithm, lower case, or None
    type_: str | None  # the type the upload has: named, or None
    numbers: list[int]
    etags: dict[int, str]
    data: dict[int, bytes]

    @property
    def e(self) -> str:
        return self.name or "crc64nvme"

    @property
    def effective_type(self) -> str:
        return self.type_ or "FULL_OBJECT"

    def value(self, listed: list[int] | None = None) -> str:
        """The object checksum for the listed part numbers (all by default)."""
        listed = self.numbers if listed is None else listed
        return object_value(self.e, self.effective_type, [self.data[n] for n in listed])

    def part(self, number: int, name: str | None = None) -> str:
        return part_value(name or self.e, self.data[number])


def make(
    env,
    name: str | None = None,
    type_: str | None = None,
    numbers: tuple[int, ...] = (1, 2, 3),
    datas: list[bytes] | None = None,
    key: str = KEY,
) -> Up:
    """An open upload that names name and type_ (or neither), with one part per number."""
    client = env[0]
    headers = {}
    if name:
        headers["x-amz-checksum-algorithm"] = name.upper()
    if type_:
        headers["x-amz-checksum-type"] = type_
    created = client.post(f"/{BUCKET}/{key}?uploads", headers=headers)
    assert created.status_code == 200, created.text
    uid = ET.fromstring(created.text).findtext("UploadId")
    datas = datas or PARTS
    etags, data = {}, {}
    for number, content in zip(numbers, datas, strict=False):
        r = client.put(f"/{BUCKET}/{key}?partNumber={number}&uploadId={uid}", content=content)
        assert r.status_code == 200, r.text
        etags[number] = r.headers["etag"]
        data[number] = content
    stored_type = None
    if name:
        stored_type = type_ or ("FULL_OBJECT" if name == "crc64nvme" else "COMPOSITE")
    return Up(uid, name, stored_type, list(numbers), etags, data)


def element(name: str, value: str) -> str:
    return f"<Checksum{name}>{value}</Checksum{name}>"


def body(up: Up, listed: list[int] | None = None, elements: dict | None = None, ns: bool = False):
    """A Complete body listing the part numbers in order; elements maps a part number to
    the raw XML of its Checksum* elements."""
    elements = elements or {}
    items = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{up.etags[n]}</ETag>{elements.get(n, '')}</Part>"
        for n in (up.numbers if listed is None else listed)
    )
    root = f' xmlns="{NS}"' if ns else ""
    return f"<CompleteMultipartUpload{root}>{items}</CompleteMultipartUpload>".encode()


def complete(env, up: Up, listed=None, elements=None, headers=None, ns=False, key=KEY):
    return env[0].post(
        f"/{BUCKET}/{key}?uploadId={up.uid}",
        content=body(up, listed, elements, ns),
        headers=headers or {},
    )


def state(env, up: Up) -> tuple:
    """Everything a refused Complete must leave alone."""
    _, storage, metadata, _ = env
    return (
        metadata.list_objects(BUCKET),
        metadata.list_blob_records(),
        storage.temp_entries(),
        sorted(storage.scan()),
        metadata.get_upload(up.uid, bucket=BUCKET, key=KEY),
        metadata.get_parts(up.uid),
    )


def assert_refused(env, up: Up, before: tuple, resp, status: int, error: str) -> None:
    """The refusal, and that the upload is open with all its parts, the store as it was,
    and no object written."""
    client, storage, metadata, _ = env
    assert (resp.status_code, code(resp)) == (status, error), resp.text
    assert state(env, up) == before
    assert metadata.upload_exists(up.uid, bucket=BUCKET, key=KEY)
    parts = metadata.get_parts(up.uid)
    assert [p.part_number for p in parts] == up.numbers
    assert all(p.blob_id is not None for p in parts)
    assert run_consistency_sweep(metadata, storage).issues == []
    assert client.get(f"/{BUCKET}/{KEY}").status_code == 404


def result(resp) -> tuple[list[str], dict[str, str]]:
    """The child tags of a CompleteMultipartUploadResult, in order, and their texts."""
    root = ET.fromstring(resp.text)
    return [c.tag for c in root], {c.tag: c.text for c in root}


def assert_completed(env, up: Up, resp, listed: list[int] | None = None) -> None:
    """A 200 whose bytes and checksum are those of the listed parts."""
    listed = up.numbers if listed is None else listed
    assert resp.status_code == 200, resp.text
    tags, texts = result(resp)
    value = up.value(listed)
    assert tags == ["Bucket", "Key", "ETag", f"Checksum{up.e.upper()}", "ChecksumType"]
    assert texts[f"Checksum{up.e.upper()}"] == value
    assert texts["ChecksumType"] == up.effective_type
    got = env[0].get(f"/{BUCKET}/{KEY}", headers=MODE)
    assert got.content == b"".join(up.data[n] for n in listed)
    assert got.headers[f"x-amz-checksum-{up.e}"] == value
    assert got.headers["x-amz-checksum-type"] == up.effective_type
    assert not env[2].list_incomplete_uploads()


# --- the Part elements (41) ---

# (id, algorithm, type, the elements by part number, expected: 200 or an error code)
BAD = "BadDigest"
REQ = "InvalidRequest"


def all_parts(make_xml):
    """Elements for every part, make_xml(up, number) giving the raw XML of one part."""
    return lambda up: {n: make_xml(up, n) for n in up.numbers}


def right(name=None, **kw):
    return all_parts(lambda up, n: element((name or up.e).upper(), up.part(n, name)))


def at(number, make_xml):
    return lambda up: {number: make_xml(up, number)}


ELEMENT_CASES = [
    # a named CRC32 COMPOSITE upload
    ("crc32-right", "crc32", "COMPOSITE", right(), 200),
    ("crc32-none", "crc32", "COMPOSITE", lambda up: {}, 200),
    (
        "crc32-wrong-on-2",
        "crc32",
        "COMPOSITE",
        at(2, lambda up, n: element("CRC32", wrong("crc32"))),
        BAD,
    ),
    ("crc32-junk-on-1", "crc32", "COMPOSITE", at(1, lambda up, n: element("CRC32", JUNK)), BAD),
    (
        "crc32-wrong-on-1-and-3",
        "crc32",
        "COMPOSITE",
        lambda up: {1: element("CRC32", wrong("crc32")), 3: element("CRC32", JUNK)},
        BAD,
    ),
    (
        "crc32-sha256-element",
        "crc32",
        "COMPOSITE",
        at(2, lambda up, n: element("SHA256", up.part(n, "sha256"))),
        REQ,
    ),
    (
        "crc32-crc32c-element",
        "crc32",
        "COMPOSITE",
        at(1, lambda up, n: element("CRC32C", up.part(n, "crc32c"))),
        REQ,
    ),
    (
        "crc32-crc64nvme-element",
        "crc32",
        "COMPOSITE",
        at(3, lambda up, n: element("CRC64NVME", up.part(n, "crc64nvme"))),
        REQ,
    ),
    (
        "crc32-md5-element",
        "crc32",
        "COMPOSITE",
        at(1, lambda up, n: element("MD5", "AAAAAAAAAAAAAAAAAAAAAA==")),
        REQ,
    ),
    (
        "crc32-other-algorithm-before-any-value",
        "crc32",
        "COMPOSITE",
        lambda up: {
            1: element("CRC32", wrong("crc32")),
            3: element("CRC32C", up.part(3, "crc32c")),
        },
        REQ,
    ),
    (
        "crc32-blank-element",
        "crc32",
        "COMPOSITE",
        at(2, lambda up, n: "<ChecksumCRC32></ChecksumCRC32>"),
        200,
    ),
    (
        "crc32-blank-other-algorithm-element",
        "crc32",
        "COMPOSITE",
        at(2, lambda up, n: "<ChecksumSHA256/>"),
        200,
    ),
    (
        "crc32-padded-right-value",
        "crc32",
        "COMPOSITE",
        at(2, lambda up, n: element("CRC32", f"  {up.part(n)}\n")),
        200,
    ),
    (
        "crc32-right-on-one-part-only",
        "crc32",
        "COMPOSITE",
        at(3, lambda up, n: element("CRC32", up.part(n))),
        200,
    ),
    # the other named algorithms
    ("sha256-right", "sha256", None, right(), 200),
    ("sha256-wrong", "sha256", None, at(2, lambda up, n: element("SHA256", wrong("sha256"))), BAD),
    (
        "sha256-crc32-element",
        "sha256",
        None,
        at(2, lambda up, n: element("CRC32", up.part(n, "crc32"))),
        REQ,
    ),
    ("crc64nvme-right", "crc64nvme", None, right(), 200),
    (
        "crc64nvme-wrong",
        "crc64nvme",
        None,
        at(1, lambda up, n: element("CRC64NVME", wrong("crc64nvme"))),
        BAD,
    ),
    (
        "crc64nvme-crc32-element",
        "crc64nvme",
        None,
        at(1, lambda up, n: element("CRC32", up.part(n, "crc32"))),
        REQ,
    ),
    ("crc32c-full-right", "crc32c", "FULL_OBJECT", right(), 200),
    (
        "crc32c-full-wrong",
        "crc32c",
        "FULL_OBJECT",
        at(3, lambda up, n: element("CRC32C", wrong("crc32c"))),
        BAD,
    ),
    ("sha1-right", "sha1", None, right(), 200),
    ("sha1-wrong", "sha1", None, at(2, lambda up, n: element("SHA1", wrong("sha1"))), BAD),
    ("crc32-full-right", "crc32", "FULL_OBJECT", right(), 200),
    (
        "crc32-full-wrong",
        "crc32",
        "FULL_OBJECT",
        at(2, lambda up, n: element("CRC32", wrong("crc32"))),
        BAD,
    ),
    # an upload that named no algorithm: CRC64NVME is compared, the rest is not
    ("unnamed-none", None, None, lambda up: {}, 200),
    ("unnamed-crc64nvme-right", None, None, right("crc64nvme"), 200),
    (
        "unnamed-crc64nvme-wrong",
        None,
        None,
        at(2, lambda up, n: element("CRC64NVME", wrong("crc64nvme"))),
        BAD,
    ),
    ("unnamed-crc64nvme-junk", None, None, at(1, lambda up, n: element("CRC64NVME", JUNK)), BAD),
    (
        "unnamed-crc32-only",
        None,
        None,
        all_parts(lambda up, n: element("CRC32", wrong("crc32"))),
        200,
    ),
    (
        "unnamed-crc32-wrong-and-crc64nvme-right",
        None,
        None,
        all_parts(
            lambda up, n: element("CRC32", wrong("crc32")) + element("CRC64NVME", up.part(n))
        ),
        200,
    ),
    (
        "unnamed-sha256-wrong",
        None,
        None,
        at(2, lambda up, n: element("SHA256", wrong("sha256"))),
        200,
    ),
    (
        "unnamed-md5",
        None,
        None,
        at(2, lambda up, n: element("MD5", "AAAAAAAAAAAAAAAAAAAAAA==")),
        200,
    ),
    ("unnamed-sha512", None, None, at(1, lambda up, n: element("SHA512", JUNK)), 200),
    ("unnamed-xxhash64", None, None, at(1, lambda up, n: element("XXHASH64", JUNK)), 200),
    (
        "unnamed-blank-crc64nvme",
        None,
        None,
        at(1, lambda up, n: "<ChecksumCRC64NVME> </ChecksumCRC64NVME>"),
        200,
    ),
    (
        "unnamed-crc64nvme-wrong-on-3-only",
        None,
        None,
        at(3, lambda up, n: element("CRC64NVME", wrong("crc64nvme"))),
        BAD,
    ),
]


@pytest.mark.parametrize("namespaced", [False, True])
def test_the_namespace_of_the_body_does_not_change_the_verdict(env, namespaced):
    up = make(env, "crc32")
    elements = {n: element("CRC32", up.part(n)) for n in up.numbers}
    assert_completed(env, up, complete(env, up, elements=elements, ns=namespaced))


@pytest.mark.parametrize(
    "algorithm, type_, elements, expected",
    [pytest.param(*c[1:], id=c[0]) for c in ELEMENT_CASES],
)
def test_the_part_elements(env, algorithm, type_, elements, expected):
    up = make(env, algorithm, type_)
    before = state(env, up)
    resp = complete(env, up, elements=elements(up))
    if expected == 200:
        assert_completed(env, up, resp)
    else:
        assert_refused(env, up, before, resp, 400, expected)
        # The same upload completes once the elements are right.
        assert_completed(env, up, complete(env, up))


def test_a_part_element_on_a_legacy_part_is_accepted_unverified(env):
    """A part stored before schema version 3 has no checksum: an unnamed upload accepts
    its ChecksumCRC64NVME element unverified, and the object gets no checksum."""
    up = make(env)
    with sqlite3.connect(env[3]) as db:
        db.execute(
            "UPDATE multipart_parts SET checksum = NULL, checksum_algorithm = NULL "
            "WHERE upload_id = ? AND part_number = 2",
            (up.uid,),
        )
    elements = {2: element("CRC64NVME", wrong("crc64nvme")), 1: element("CRC64NVME", up.part(1))}
    resp = complete(env, up, elements=elements)
    assert resp.status_code == 200, resp.text
    assert result(resp)[0] == ["Bucket", "Key", "ETag"]
    got = env[0].get(f"/{BUCKET}/{KEY}", headers=MODE)
    assert got.content == b"".join(PARTS)
    assert "x-amz-checksum-crc64nvme" not in got.headers
    assert "x-amz-checksum-type" not in got.headers
    meta = env[2].get_object_meta(BUCKET, KEY)
    assert (meta.checksum, meta.checksum_algorithm, meta.checksum_type) == (None, None, None)


# --- the headers and the type (58) ---


def hdr(name: str):
    """The right value for the object, in wire form."""
    return lambda up: up.value()


def no_suffix(up: Up) -> str:
    return up.value().rsplit("-", 1)[0]


def w(name: str):
    return lambda up: wrong(name)


def junk(up: Up) -> str:
    return JUNK


H = "x-amz-checksum-"
T = "x-amz-checksum-type"
OK = 200
INVALID_ARGUMENT = "InvalidArgument"

# (id, algorithm, type, the headers: name to a string or a function of the upload, expected)
HEADER_CASES = [
    # a named SHA256 upload (COMPOSITE)
    ("sha256-right", "sha256", None, {H + "sha256": hdr("sha256")}, OK),
    ("sha256-without-the-suffix", "sha256", None, {H + "sha256": no_suffix}, BAD),
    ("sha256-wrong", "sha256", None, {H + "sha256": w("sha256")}, BAD),
    ("sha256-junk", "sha256", None, {H + "sha256": junk}, BAD),
    ("sha256-upload-sha1-header", "sha256", None, {H + "sha1": w("sha1")}, REQ),
    ("sha256-upload-crc32-header", "sha256", None, {H + "crc32": w("crc32")}, REQ),
    ("sha256-upload-crc64nvme-header", "sha256", None, {H + "crc64nvme": w("crc64nvme")}, REQ),
    ("sha256-upload-md5-header", "sha256", None, {H + "md5": "AAAAAAAAAAAAAAAAAAAAAA=="}, REQ),
    (
        "sha256-two-headers",
        "sha256",
        None,
        {H + "sha256": hdr("sha256"), H + "crc32": w("crc32")},
        REQ,
    ),
    ("sha256-type-composite", "sha256", None, {T: "COMPOSITE"}, OK),
    ("sha256-type-lower-case", "sha256", None, {T: "composite"}, OK),
    ("sha256-type-full-object", "sha256", None, {T: "FULL_OBJECT"}, BAD),
    ("sha256-type-bogus", "sha256", None, {T: "bogus"}, INVALID_ARGUMENT),
    ("sha256-right-with-type", "sha256", None, {H + "sha256": hdr("sha256"), T: "COMPOSITE"}, OK),
    (
        "sha256-blank-type-and-blank-other-header",
        "sha256",
        None,
        {T: "  ", H + "crc32": " ", H + "sha256": hdr("sha256")},
        OK,
    ),
    # CRC32 and CRC32C, composite and full object
    ("crc32-composite-right", "crc32", "COMPOSITE", {H + "crc32": hdr("crc32")}, OK),
    ("crc32-composite-type-full-object", "crc32", "COMPOSITE", {T: "FULL_OBJECT"}, BAD),
    ("crc32-full-right", "crc32", "FULL_OBJECT", {H + "crc32": hdr("crc32")}, OK),
    ("crc32-full-type-composite", "crc32", "FULL_OBJECT", {T: "COMPOSITE"}, BAD),
    ("crc32-full-wrong", "crc32", "FULL_OBJECT", {H + "crc32": w("crc32")}, BAD),
    ("crc32-full-type-full-object", "crc32", "FULL_OBJECT", {T: "FULL_OBJECT"}, OK),
    (
        "crc32-full-the-composite-form-is-wrong",
        "crc32",
        "FULL_OBJECT",
        {H + "crc32": lambda up: up.value() + "-3"},
        BAD,
    ),
    ("crc32c-composite-right", "crc32c", "COMPOSITE", {H + "crc32c": hdr("crc32c")}, OK),
    ("crc32c-full-right", "crc32c", "FULL_OBJECT", {H + "crc32c": hdr("crc32c")}, OK),
    ("crc32c-full-wrong", "crc32c", "FULL_OBJECT", {H + "crc32c": w("crc32c")}, BAD),
    # CRC64NVME and SHA1
    ("crc64nvme-right", "crc64nvme", None, {H + "crc64nvme": hdr("crc64nvme")}, OK),
    ("crc64nvme-wrong", "crc64nvme", None, {H + "crc64nvme": w("crc64nvme")}, BAD),
    ("crc64nvme-type-full-object", "crc64nvme", None, {T: "FULL_OBJECT"}, OK),
    ("crc64nvme-type-composite", "crc64nvme", None, {T: "COMPOSITE"}, BAD),
    ("sha1-right", "sha1", None, {H + "sha1": hdr("sha1")}, OK),
    ("sha1-wrong", "sha1", None, {H + "sha1": w("sha1")}, BAD),
    ("sha1-type-composite", "sha1", None, {T: "COMPOSITE"}, OK),
    (
        "crc32-right-value-with-spaces",
        "crc32",
        "COMPOSITE",
        {H + "crc32": lambda up: f" {up.value()} "},
        OK,
    ),
    (
        "all-eleven-headers-blank",
        "crc32",
        "COMPOSITE",
        {
            h: ""
            for h in (
                H + n
                for n in (
                    "crc32",
                    "crc32c",
                    "crc64nvme",
                    "sha1",
                    "sha256",
                    "sha512",
                    "md5",
                    "xxhash64",
                    "xxhash3",
                    "xxhash128",
                    "type",
                )
            )
        },
        OK,
    ),
    # an upload that named no algorithm
    ("unnamed-crc64nvme-right", None, None, {H + "crc64nvme": hdr("crc64nvme")}, OK),
    ("unnamed-crc64nvme-wrong", None, None, {H + "crc64nvme": w("crc64nvme")}, BAD),
    ("unnamed-crc64nvme-junk", None, None, {H + "crc64nvme": junk}, BAD),
    ("unnamed-crc32", None, None, {H + "crc32": w("crc32")}, OK),
    ("unnamed-sha1", None, None, {H + "sha1": w("sha1")}, OK),
    ("unnamed-sha256", None, None, {H + "sha256": w("sha256")}, OK),
    ("unnamed-sha512", None, None, {H + "sha512": junk}, REQ),
    ("unnamed-md5", None, None, {H + "md5": junk}, REQ),
    ("unnamed-xxhash64", None, None, {H + "xxhash64": junk}, REQ),
    ("unnamed-xxhash3", None, None, {H + "xxhash3": junk}, REQ),
    ("unnamed-xxhash128", None, None, {H + "xxhash128": junk}, REQ),
    ("unnamed-two-headers", None, None, {H + "crc32": junk, H + "sha256": junk}, REQ),
    (
        "unnamed-crc64nvme-right-and-crc32",
        None,
        None,
        {H + "crc64nvme": hdr("crc64nvme"), H + "crc32": junk},
        REQ,
    ),
    ("unnamed-type-composite", None, None, {T: "COMPOSITE"}, OK),
    ("unnamed-type-full-object", None, None, {T: "FULL_OBJECT"}, OK),
    ("unnamed-type-lower-case", None, None, {T: "full_object"}, OK),
    ("unnamed-type-padded", None, None, {T: " COMPOSITE "}, OK),
    ("unnamed-type-bogus", None, None, {T: "bogus"}, INVALID_ARGUMENT),
    (
        "unnamed-right-header-and-type",
        None,
        None,
        {H + "crc64nvme": hdr("crc64nvme"), T: "COMPOSITE"},
        OK,
    ),
    (
        "unnamed-wrong-header-and-bogus-type",
        None,
        None,
        {H + "crc64nvme": w("crc64nvme"), T: "bogus"},
        INVALID_ARGUMENT,
    ),
    (
        "unnamed-header-name-in-mixed-case",
        None,
        None,
        {"X-Amz-Checksum-CRC64NVME": hdr("crc64nvme")},
        OK,
    ),
    (
        "unnamed-right-header-in-padding",
        None,
        None,
        {H + "crc64nvme": lambda up: f"  {up.value()}  "},
        OK,
    ),
    ("unnamed-crc32-and-md5-blank", None, None, {H + "md5": "", H + "crc32": w("crc32")}, OK),
]


@pytest.mark.parametrize(
    "algorithm, type_, headers, expected",
    [pytest.param(*c[1:], id=c[0]) for c in HEADER_CASES],
)
def test_the_checksum_headers_and_the_type(env, algorithm, type_, headers, expected):
    up = make(env, algorithm, type_)
    before = state(env, up)
    sent = {k: (v(up) if callable(v) else v) for k, v in headers.items()}
    resp = complete(env, up, headers=sent)
    if expected == OK:
        assert_completed(env, up, resp)
    else:
        assert_refused(env, up, before, resp, 400, expected)
        assert_completed(env, up, complete(env, up))


def test_a_wrong_crc64nvme_header_on_a_legacy_upload_is_accepted_and_stores_nothing(env):
    up = make(env)
    with sqlite3.connect(env[3]) as db:
        db.execute(
            "UPDATE multipart_parts SET checksum = NULL, checksum_algorithm = NULL "
            "WHERE upload_id = ? AND part_number = 3",
            (up.uid,),
        )
    resp = complete(env, up, headers={H + "crc64nvme": wrong("crc64nvme")})
    assert resp.status_code == 200, resp.text
    assert result(resp)[0] == ["Bucket", "Key", "ETag"]
    meta = env[2].get_object_meta(BUCKET, KEY)
    assert (meta.checksum, meta.checksum_algorithm, meta.checksum_type) == (None, None, None)


# --- the part number rules (19) ---

IPO = "InvalidPartOrder"

# (id, algorithm, type, the numbers uploaded, the numbers listed, expected)
GAP_CASES = [
    ("crc32-composite-1-3-of-1-2-3", "crc32", "COMPOSITE", (1, 2, 3), [1, 3], IPO),
    ("crc32c-composite-1-3-of-1-2-3", "crc32c", "COMPOSITE", (1, 2, 3), [1, 3], IPO),
    ("sha1-composite-1-3-of-1-2-3", "sha1", "COMPOSITE", (1, 2, 3), [1, 3], IPO),
    ("sha256-composite-1-3-of-1-2-3", "sha256", "COMPOSITE", (1, 2, 3), [1, 3], IPO),
    ("crc32-composite-2-3-of-1-2-3", "crc32", "COMPOSITE", (1, 2, 3), [2, 3], IPO),
    ("sha256-composite-2-3-of-1-2-3", "sha256", "COMPOSITE", (1, 2, 3), [2, 3], IPO),
    ("sha256-composite-1-2-of-1-2-3-ignores-part-3", "sha256", "COMPOSITE", (1, 2, 3), [1, 2], OK),
    ("crc32-composite-uploaded-2-3-4", "crc32", "COMPOSITE", (2, 3, 4), [2, 3, 4], IPO),
    ("crc32c-composite-uploaded-1-3-5", "crc32c", "COMPOSITE", (1, 3, 5), [1, 3, 5], IPO),
    ("sha256-composite-single-part-10000", "sha256", "COMPOSITE", (10000,), [10000], IPO),
    ("crc32-composite-single-part-1", "crc32", "COMPOSITE", (1,), [1], OK),
    ("crc32-full-1-3-of-1-2-3", "crc32", "FULL_OBJECT", (1, 2, 3), [1, 3], OK),
    ("crc32c-full-1-3-of-1-2-3", "crc32c", "FULL_OBJECT", (1, 2, 3), [1, 3], OK),
    ("crc64nvme-1-3-of-1-2-3", "crc64nvme", None, (1, 2, 3), [1, 3], OK),
    ("crc32-full-2-3-of-1-2-3", "crc32", "FULL_OBJECT", (1, 2, 3), [2, 3], OK),
    ("crc64nvme-single-part-2", "crc64nvme", None, (2,), [2], OK),
    ("unnamed-1-3-of-1-2-3", None, None, (1, 2, 3), [1, 3], OK),
    ("unnamed-5-7-9", None, None, (5, 7, 9), [5, 7, 9], OK),
    ("unnamed-single-part-10000", None, None, (10000,), [10000], OK),
]


@pytest.mark.parametrize(
    "algorithm, type_, uploaded, listed, expected",
    [pytest.param(*c[1:], id=c[0]) for c in GAP_CASES],
)
def test_the_part_numbers_of_a_composite_upload_are_consecutive_from_1(
    env, algorithm, type_, uploaded, listed, expected
):
    up = make(env, algorithm, type_, uploaded)
    before = state(env, up)
    resp = complete(env, up, listed=listed)
    if expected == OK:
        assert_completed(env, up, resp, listed)
    else:
        assert_refused(env, up, before, resp, 400, expected)


# --- the order of the errors: each case breaks two adjacent steps of decision 32 ---


def test_order_2_3_no_such_upload_before_a_malformed_size(env):
    resp = env[0].post(
        f"/{BUCKET}/{KEY}?uploadId=missing",
        content=b"<x/>",
        headers={"x-amz-mp-object-size": "abc"},
    )
    assert (resp.status_code, code(resp)) == (404, "NoSuchUpload")


def test_order_3_4_a_malformed_size_before_the_shape_of_the_checksum_headers(env):
    up = make(env, "crc32")
    before = state(env, up)
    headers = {"x-amz-mp-object-size": "abc", H + "crc32": junk(up), H + "sha256": junk(up)}
    assert_refused(env, up, before, complete(env, up, headers=headers), 400, "InvalidArgument")


def test_order_4_5_the_shape_of_the_checksum_headers_before_malformed_xml(env):
    up = make(env, "crc32")
    before = state(env, up)
    resp = env[0].post(
        f"/{BUCKET}/{KEY}?uploadId={up.uid}", content=b"not xml", headers={H + "sha1": junk(up)}
    )
    assert_refused(env, up, before, resp, 400, REQ)


def test_order_5_6_malformed_xml_before_parts_out_of_order(env):
    up = make(env, "crc32")
    before = state(env, up)
    items = (
        f"<Part><PartNumber>2</PartNumber><ETag>{up.etags[2]}</ETag></Part>"
        f"<Part><PartNumber>1</PartNumber><ETag>{up.etags[1]}</ETag></Part>"
        "<Part><PartNumber>3</PartNumber></Part>"
    )
    resp = env[0].post(
        f"/{BUCKET}/{KEY}?uploadId={up.uid}",
        content=f"<CompleteMultipartUpload>{items}</CompleteMultipartUpload>".encode(),
    )
    assert_refused(env, up, before, resp, 400, "MalformedXML")


def test_order_6_7_parts_out_of_order_before_an_invalid_part(env):
    up = make(env, "crc32")
    before = state(env, up)
    items = (
        f"<Part><PartNumber>2</PartNumber><ETag>{up.etags[2]}</ETag></Part>"
        '<Part><PartNumber>1</PartNumber><ETag>"00000000000000000000000000000000"</ETag></Part>'
    )
    resp = env[0].post(
        f"/{BUCKET}/{KEY}?uploadId={up.uid}",
        content=f"<CompleteMultipartUpload>{items}</CompleteMultipartUpload>".encode(),
    )
    assert_refused(env, up, before, resp, 400, IPO)


def test_order_7_8_an_invalid_part_before_a_composite_gap(env):
    up = make(env, "crc32", "COMPOSITE")
    before = state(env, up)
    up.etags[3] = '"00000000000000000000000000000000"'
    assert_refused(env, up, before, complete(env, up, listed=[1, 3]), 400, "InvalidPart")


def test_order_8_9_a_composite_gap_before_a_part_that_is_too_small(tmp_path):
    env = build(tmp_path, min_part_size=100)
    up = make(env, "crc32", "COMPOSITE")  # every part but the last listed is under 100
    before = state(env, up)
    assert_refused(env, up, before, complete(env, up, listed=[1, 3]), 400, IPO)


def test_order_9_10_a_part_too_small_before_an_element_in_another_algorithm(tmp_path):
    env = build(tmp_path, min_part_size=100)
    up = make(env, "crc32", "COMPOSITE")
    before = state(env, up)
    elements = {1: element("SHA256", up.part(1, "sha256"))}
    assert_refused(env, up, before, complete(env, up, elements=elements), 400, "EntityTooSmall")


def test_order_10_11_an_element_in_another_algorithm_before_a_wrong_value(env):
    up = make(env, "crc32")
    before = state(env, up)
    elements = {1: element("CRC32", wrong("crc32")), 2: element("SHA1", up.part(2, "sha1"))}
    assert_refused(env, up, before, complete(env, up, elements=elements), 400, REQ)


def test_order_11_12_a_wrong_part_element_before_a_wrong_object_size(env):
    up = make(env, "crc32")
    before = state(env, up)
    elements = {2: element("CRC32", wrong("crc32"))}
    headers = {"x-amz-mp-object-size": "1"}
    assert_refused(env, up, before, complete(env, up, elements=elements, headers=headers), 400, BAD)


def test_order_12_13_a_wrong_object_size_before_a_type_that_differs(env):
    up = make(env, "crc32", "FULL_OBJECT")
    before = state(env, up)
    headers = {"x-amz-mp-object-size": "1", T: "COMPOSITE"}
    assert_refused(env, up, before, complete(env, up, headers=headers), 400, REQ)


def test_order_13_14_a_type_that_differs_before_a_wrong_checksum_value(env):
    up = make(env, "crc32", "FULL_OBJECT")
    before = state(env, up)
    headers = {T: "COMPOSITE", H + "crc32": wrong("crc32")}
    resp = complete(env, up, headers=headers)
    assert_refused(env, up, before, resp, 400, BAD)
    assert ET.fromstring(resp.text).findtext("Header") == T


def test_order_14_16_a_wrong_checksum_value_before_a_failing_condition(env):
    client = env[0]
    assert client.put(f"/{BUCKET}/{KEY}", content=b"old").status_code == 200
    up = make(env, "crc32", "FULL_OBJECT")
    resp = complete(env, up, headers={H + "crc32": wrong("crc32"), "If-None-Match": "*"})
    assert (resp.status_code, code(resp)) == (400, BAD)
    assert ET.fromstring(resp.text).findtext("Header") == H + "crc32"
    assert env[2].upload_exists(up.uid, bucket=BUCKET, key=KEY)
    assert client.get(f"/{BUCKET}/{KEY}").content == b"old"


# --- the values on the wire, on GET and on HEAD (18) ---

END_TO_END = [
    ("unnamed", None, None, [1, 2, 3]),
    ("crc32-composite", "crc32", "COMPOSITE", [1, 2, 3]),
    ("crc32-full", "crc32", "FULL_OBJECT", [1, 2, 3]),
    ("crc32c-composite", "crc32c", "COMPOSITE", [1, 2, 3]),
    ("crc32c-full", "crc32c", "FULL_OBJECT", [1, 2, 3]),
    ("crc64nvme", "crc64nvme", None, [1, 2, 3]),
    ("sha1-composite", "sha1", "COMPOSITE", [1, 2, 3]),
    ("sha256-composite", "sha256", "COMPOSITE", [1, 2, 3]),
    ("crc32-full-listing-1-3", "crc32", "FULL_OBJECT", [1, 3]),
]


@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize(
    "algorithm, type_, listed",
    [pytest.param(*c[1:], id=c[0]) for c in END_TO_END],
)
def test_the_completed_object_returns_its_checksum(env, algorithm, type_, listed, method):
    up = make(env, algorithm, type_)
    resp = complete(env, up, listed=listed)
    tags, texts = result(resp)
    assert resp.status_code == 200, resp.text
    assert tags == [
        "Bucket",
        "Key",
        "ETag",
        f"Checksum{up.e.upper()}",
        "ChecksumType",
    ]
    value = up.value(listed)
    assert texts[f"Checksum{up.e.upper()}"] == value
    plain = env[0].request(method, f"/{BUCKET}/{KEY}")
    assert not [h for h in plain.headers if h.startswith("x-amz-checksum")]
    got = env[0].request(method, f"/{BUCKET}/{KEY}", headers=MODE)
    assert got.status_code == 200
    assert got.headers[f"x-amz-checksum-{up.e}"] == value
    assert got.headers["x-amz-checksum-type"] == up.effective_type
    checksum_headers = {h for h in got.headers if h.startswith("x-amz-checksum")}
    assert checksum_headers == {f"x-amz-checksum-{up.e}", "x-amz-checksum-type"}
    if method == "GET":
        assert got.content == b"".join(up.data[n] for n in listed)


# --- boto3 (4) ---


@pytest.fixture(scope="module")
def live(tmp_path_factory):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    tmp: Path = tmp_path_factory.mktemp("complete-checksums")
    app = create_app(
        storage=BlobStore(tmp / "blobs"), metadata=SQLiteMetadataStore(db_path=tmp / "meta.db")
    )
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, ws="none", log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("uvicorn did not start")
        time.sleep(0.02)
    client = boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{port}",
        aws_access_key_id="key",
        aws_secret_access_key="secret",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 0}),
    )
    client.create_bucket(Bucket=BUCKET)
    yield client
    server.should_exit = True
    thread.join(timeout=10)


MIB = 1024 * 1024


def boto3_parts(s3, key: str, algorithm: str | None, numbers=(1, 2, 3), type_=None):
    """A boto3 upload: its id and the parts as Complete lists them, each with the
    checksum the UploadPart response echoed."""
    args = {}
    if algorithm:
        args["ChecksumAlgorithm"] = algorithm.upper()
    if type_:
        args["ChecksumType"] = type_
    uid = s3.create_multipart_upload(Bucket=BUCKET, Key=key, **args)["UploadId"]
    parts = []
    for number, data in zip(numbers, PARTS, strict=False):
        sent = {"ChecksumAlgorithm": algorithm.upper()} if algorithm else {}
        r = s3.upload_part(
            Bucket=BUCKET, Key=key, UploadId=uid, PartNumber=number, Body=data, **sent
        )
        part = {"ETag": r["ETag"], "PartNumber": number}
        for field in (
            "ChecksumCRC32",
            "ChecksumCRC32C",
            "ChecksumCRC64NVME",
            "ChecksumSHA1",
            "ChecksumSHA256",
        ):
            if field in r:
                part[field] = r[field]
        parts.append(part)
    return uid, parts


def test_boto3_upload_file_by_default_completes_a_named_crc32_composite(live, tmp_path):
    data = (b"x" * (5 * MIB)) + (b"y" * (5 * MIB)) + b"z" * 100
    source = tmp_path / "source.bin"
    source.write_bytes(data)
    live.upload_file(
        str(source),
        BUCKET,
        "upload-file",
        Config=TransferConfig(multipart_threshold=5 * MIB, multipart_chunksize=5 * MIB),
    )
    head = live.head_object(Bucket=BUCKET, Key="upload-file", ChecksumMode="ENABLED")
    chunks = [data[: 5 * MIB], data[5 * MIB : 10 * MIB], data[10 * MIB :]]
    assert head["ChecksumType"] == "COMPOSITE"
    assert head["ChecksumCRC32"] == object_value("crc32", "COMPOSITE", chunks)
    assert head["ChecksumCRC32"].endswith("-3")
    # botocore checks a whole download against the stored checksum.
    got = live.get_object(Bucket=BUCKET, Key="upload-file", ChecksumMode="ENABLED")
    assert got["Body"].read() == data


def test_boto3_sha256_composite_with_the_part_elements_and_the_header(live):
    uid, parts = boto3_parts(live, "sha256-flow", "sha256")
    assert all("ChecksumSHA256" in p for p in parts)
    value = object_value("sha256", "COMPOSITE", PARTS)
    done = live.complete_multipart_upload(
        Bucket=BUCKET,
        Key="sha256-flow",
        UploadId=uid,
        MultipartUpload={"Parts": parts},
        ChecksumSHA256=value,
    )
    assert (done["ChecksumSHA256"], done["ChecksumType"]) == (value, "COMPOSITE")
    got = live.get_object(Bucket=BUCKET, Key="sha256-flow", ChecksumMode="ENABLED")
    assert got["ChecksumSHA256"] == value
    assert got["Body"].read() == b"".join(PARTS)


def test_boto3_crc64nvme_full_object_with_a_gap_between_the_part_numbers(live):
    uid, parts = boto3_parts(live, "full-object-gap", "crc64nvme", numbers=(1, 4, 9))
    listed = [parts[0], parts[2]]
    value = object_value("crc64nvme", "FULL_OBJECT", [PARTS[0], PARTS[2]])
    done = live.complete_multipart_upload(
        Bucket=BUCKET,
        Key="full-object-gap",
        UploadId=uid,
        MultipartUpload={"Parts": listed},
        ChecksumCRC64NVME=value,
        ChecksumType="FULL_OBJECT",
    )
    assert (done["ChecksumCRC64NVME"], done["ChecksumType"]) == (value, "FULL_OBJECT")
    got = live.get_object(Bucket=BUCKET, Key="full-object-gap", ChecksumMode="ENABLED")
    assert got["Body"].read() == PARTS[0] + PARTS[2]


def test_boto3_a_wrong_crc64nvme_header_on_an_unnamed_upload_is_bad_digest(live):
    uid, parts = boto3_parts(live, "unnamed-flow", None)
    with pytest.raises(ClientError) as raised:
        live.complete_multipart_upload(
            Bucket=BUCKET,
            Key="unnamed-flow",
            UploadId=uid,
            MultipartUpload={"Parts": parts},
            ChecksumCRC64NVME=wrong("crc64nvme"),
        )
    assert raised.value.response["Error"]["Code"] == "BadDigest"
    value = object_value("crc64nvme", "FULL_OBJECT", PARTS)
    done = live.complete_multipart_upload(
        Bucket=BUCKET,
        Key="unnamed-flow",
        UploadId=uid,
        MultipartUpload={"Parts": parts},
        ChecksumCRC64NVME=value,
    )
    assert (done["ChecksumCRC64NVME"], done["ChecksumType"]) == (value, "FULL_OBJECT")
    assert live.get_object(Bucket=BUCKET, Key="unnamed-flow", ChecksumMode="ENABLED")[
        "Body"
    ].read() == b"".join(PARTS)
