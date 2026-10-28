"""CopyObject stores and returns a checksum (docs/design/storage-layout.md decision 33, §20).

The copy hashes the bytes it copies, in the algorithm x-amz-checksum-algorithm names, else
the source row's, else CRC64NVME, and always as a FULL_OBJECT checksum (S3 User Guide,
"Copying, moving, and renaming objects": "Amazon S3 calculates a new checksum value after
the object is copied. Amazon S3 doesn't directly copy the value of the checksum";
"Checking object integrity for data uploads in Amazon S3": "With a copy command, the
checksum of the object is a direct checksum of the full object"). CopyObjectResult carries
ChecksumType and Checksum<ALG> after ETag and LastModified (S3 API reference, CopyObject).

The expected digests come from the reference implementations in test_upload_checksums.py
(zlib, hashlib and two table CRCs), none of which uses awscrt or server/.

The cases (33):
  the header names the algorithm                               5
  the header is case insensitive                               3
  no header: the source's algorithm is inherited               5
  a source with NULL columns gives CRC64NVME                   1
  a composite source gives a direct full object value          1
  the type is always FULL_OBJECT                               3
  the five uncomputed names are 501                            5
  an unknown name is 400 before the source lookup              1
  an unknown name is 400 and creates nothing                   2
  a blank header counts as absent                              2
  a copy onto itself, without and with REPLACE                 2
  the values on GET and HEAD with the checksum mode            2
  an overwrite replaces all three columns                      1
"""

import sqlite3
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from test_upload_checksums import ALGOS, b64, claim, digest

from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "copy-checksums"
DATA = b"copy me, and hash what is copied. " * 40
OTHER = b"the destination before the copy. " * 7
PARTS = [b"a" * 10, b"b" * 3000]
ALGORITHM = "x-amz-checksum-algorithm"
MODE = {"x-amz-checksum-mode": "ENABLED"}
UNCOMPUTED = ["SHA512", "MD5", "xxhash64", "XXHash3", "XXHASH128"]


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    assert client.put(f"/{BUCKET}").status_code == 200
    return client, storage, metadata, tmp_path / "meta.db"


def put_source(env, key: str = "src", algorithm: str | None = None, data: bytes = DATA):
    headers = dict([claim(algorithm, data)]) if algorithm else {}
    assert env[0].put(f"/{BUCKET}/{key}", content=data, headers=headers).status_code == 200


def composite_source(env, key: str = "src", algorithm: str = "SHA256") -> None:
    """A multipart object of a named upload: a COMPOSITE checksum with its "-2" suffix."""
    client = env[0]
    created = client.post(f"/{BUCKET}/{key}?uploads", headers={ALGORITHM: algorithm})
    assert created.status_code == 200, created.text
    uid = next(e.text for e in ET.fromstring(created.text).iter() if e.tag.endswith("UploadId"))
    items = ""
    for number, data in enumerate(PARTS, 1):
        part = client.put(f"/{BUCKET}/{key}?partNumber={number}&uploadId={uid}", content=data)
        assert part.status_code == 200, part.text
        items += (
            f"<Part><PartNumber>{number}</PartNumber><ETag>{part.headers['etag']}</ETag></Part>"
        )
    done = client.post(
        f"/{BUCKET}/{key}?uploadId={uid}",
        content=f"<CompleteMultipartUpload>{items}</CompleteMultipartUpload>",
    )
    assert done.status_code == 200, done.text
    meta = env[2].get_object_meta(BUCKET, key)
    assert (meta.checksum_algorithm, meta.checksum_type) == (algorithm, "COMPOSITE")
    assert meta.checksum.endswith("-2")


def copy(env, key: str = "dst", source: str = "src", **headers):
    return env[0].put(
        f"/{BUCKET}/{key}", headers={"x-amz-copy-source": f"{BUCKET}/{source}", **headers}
    )


def columns(env, key: str = "dst") -> tuple:
    meta = env[2].get_object_meta(BUCKET, key)
    return meta.checksum, meta.checksum_algorithm, meta.checksum_type


def expected(algorithm: str, data: bytes = DATA) -> tuple:
    return b64(digest(algorithm.lower(), data)), algorithm.upper(), "FULL_OBJECT"


def result(resp) -> list[tuple[str, str]]:
    """The children of CopyObjectResult, in order."""
    return [(e.tag, e.text) for e in ET.fromstring(resp.text)]


def assert_result(resp, algorithm: str, data: bytes = DATA) -> None:
    assert resp.status_code == 200, resp.text
    tags = [t for t, _ in result(resp)]
    assert tags == ["ETag", "LastModified", "ChecksumType", f"Checksum{algorithm.upper()}"]
    assert dict(result(resp))["ChecksumType"] == "FULL_OBJECT"
    assert dict(result(resp))[f"Checksum{algorithm.upper()}"] == b64(
        digest(algorithm.lower(), data)
    )


def code(resp) -> str:
    return ET.fromstring(resp.text).findtext("Code")


def assert_nothing_created(env, key: str = "dst") -> None:
    assert env[2].get_object_meta(BUCKET, key) is None
    assert env[1].temp_entries() == []
    assert run_consistency_sweep(env[2], env[1]).issues == []


# --- the header names the algorithm ---


@pytest.mark.parametrize("algorithm", ALGOS)
def test_the_header_names_the_algorithm(env, algorithm):
    # A source in another algorithm, so the header is what decides.
    put_source(env, algorithm="sha1" if algorithm == "crc32" else "crc32")
    resp = copy(env, **{ALGORITHM: algorithm.upper()})
    assert_result(resp, algorithm)
    assert columns(env) == expected(algorithm)


@pytest.mark.parametrize("given", ["sha256", "Sha256", "CRC32c"])
def test_the_header_is_case_insensitive_and_stored_in_upper_case(env, given):
    put_source(env)
    resp = copy(env, **{ALGORITHM: given})
    assert_result(resp, given)
    assert columns(env) == expected(given)


# --- no header ---


@pytest.mark.parametrize("algorithm", ALGOS)
def test_no_header_inherits_the_algorithm_of_the_source(env, algorithm):
    put_source(env, algorithm=algorithm)
    resp = copy(env)
    assert_result(resp, algorithm)
    assert columns(env) == expected(algorithm)


def test_a_source_with_null_columns_gives_crc64nvme(env):
    put_source(env)
    # An object migrated from schema version 2 has no checksum columns.
    with sqlite3.connect(env[3]) as db:
        db.execute(
            "UPDATE objects SET checksum = NULL, checksum_algorithm = NULL, checksum_type = NULL"
            " WHERE key = 'src'"
        )
    assert columns(env, "src") == (None, None, None)
    resp = copy(env)
    assert_result(resp, "CRC64NVME")
    assert columns(env) == expected("CRC64NVME")


def test_a_composite_source_gives_a_direct_full_object_value(env):
    composite_source(env)
    resp = copy(env)
    whole = b"".join(PARTS)
    assert_result(resp, "SHA256", whole)
    assert columns(env) == expected("SHA256", whole)
    assert not columns(env)[0].endswith("-2")
    # The source keeps its composite value.
    assert columns(env, "src")[2] == "COMPOSITE"


# --- the type ---


@pytest.mark.parametrize("source", ["put", "composite", "null"])
def test_the_type_is_always_full_object(env, source):
    if source == "composite":
        composite_source(env, algorithm="CRC32")
    else:
        put_source(env, algorithm="sha1")
    if source == "null":
        with sqlite3.connect(env[3]) as db:
            db.execute(
                "UPDATE objects SET checksum = NULL, checksum_algorithm = NULL,"
                " checksum_type = NULL WHERE key = 'src'"
            )
    resp = copy(env, **({ALGORITHM: "SHA256"} if source == "put" else {}))
    assert resp.status_code == 200, resp.text
    assert dict(result(resp))["ChecksumType"] == "FULL_OBJECT"
    assert columns(env)[2] == "FULL_OBJECT"
    head = env[0].head(f"/{BUCKET}/dst", headers=MODE)
    assert head.headers["x-amz-checksum-type"] == "FULL_OBJECT"


# --- the names ---


@pytest.mark.parametrize("name", UNCOMPUTED)
def test_an_uncomputed_name_is_501_and_creates_nothing(env, name):
    put_source(env)
    resp = copy(env, **{ALGORITHM: name})
    assert (resp.status_code, code(resp)) == (501, "NotImplemented")
    assert_nothing_created(env)


def test_an_unknown_name_is_400_before_the_source_lookup(env):
    resp = copy(env, source="missing", **{ALGORITHM: "CRC16"})
    assert (resp.status_code, code(resp)) == (400, "InvalidArgument")
    # The same copy with a good name finds no source.
    again = copy(env, source="missing", **{ALGORITHM: "CRC32"})
    assert (again.status_code, code(again)) == (404, "NoSuchKey")


@pytest.mark.parametrize("given", ["MD6", "CRC32, SHA1"])
def test_an_unknown_name_is_400_naming_the_header_and_creates_nothing(env, given):
    put_source(env)
    resp = copy(env, **{ALGORITHM: given})
    assert (resp.status_code, code(resp)) == (400, "InvalidArgument")
    assert ET.fromstring(resp.text).findtext("ArgumentName") == ALGORITHM
    assert_nothing_created(env)


@pytest.mark.parametrize("blank", ["", "   "])
def test_a_blank_header_counts_as_absent(env, blank):
    put_source(env, algorithm="sha1")
    resp = copy(env, **{ALGORITHM: blank})
    # Absent, so the source's SHA1 is inherited and not the CRC64NVME default.
    assert_result(resp, "SHA1")
    assert columns(env) == expected("SHA1")


# --- a copy onto itself ---


def test_a_copy_onto_itself_that_changes_only_the_algorithm_is_400(env):
    put_source(env, algorithm="crc32")
    before = columns(env, "src")
    resp = copy(env, key="src", **{ALGORITHM: "SHA256"})
    assert (resp.status_code, code(resp)) == (400, "InvalidRequest")
    assert columns(env, "src") == before == expected("CRC32")


def test_a_copy_onto_itself_with_replace_recomputes_in_the_named_algorithm(env):
    put_source(env, algorithm="crc32")
    resp = copy(env, key="src", **{ALGORITHM: "SHA256", "x-amz-metadata-directive": "REPLACE"})
    assert_result(resp, "SHA256")
    assert columns(env, "src") == expected("SHA256")


# --- the values on GET and HEAD ---


@pytest.mark.parametrize("method", ["GET", "HEAD"])
def test_the_checksum_of_a_copy_is_returned_with_the_mode(env, method):
    put_source(env, algorithm="crc32c")
    assert copy(env, **{ALGORITHM: "SHA1"}).status_code == 200
    got = env[0].request(method, f"/{BUCKET}/dst", headers=MODE)
    assert got.status_code == 200
    assert got.headers["x-amz-checksum-sha1"] == b64(digest("sha1", DATA))
    assert got.headers["x-amz-checksum-type"] == "FULL_OBJECT"
    assert not [h for h in got.headers if h.startswith("x-amz-checksum-crc")]
    if method == "GET":
        assert got.content == DATA
    # Without the mode no checksum header is sent.
    plain = env[0].request(method, f"/{BUCKET}/dst")
    assert not [h for h in plain.headers if h.startswith("x-amz-checksum")]


# --- an overwrite ---


def test_an_overwrite_replaces_all_three_columns(env):
    # The destination is a composite SHA256 object: all three columns will change.
    composite_source(env, key="dst", algorithm="SHA256")
    old = columns(env)
    put_source(env, algorithm="crc32")
    resp = copy(env)
    assert_result(resp, "CRC32")
    assert columns(env) == expected("CRC32")
    assert all(new != was for new, was in zip(columns(env), old, strict=True))
    assert env[0].get(f"/{BUCKET}/dst").content == DATA
    assert run_consistency_sweep(env[2], env[1]).issues == []
