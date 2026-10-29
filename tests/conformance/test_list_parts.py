"""ListParts: GET /{bucket}/{key}?uploadId= (docs/design/storage-layout.md §21, decision 36).

The response is the elements of the S3 API reference's ListParts response syntax, in its
order, with each Part in the order of the reference's sample response and its checksum
last. The error order is the guard (501, then AccessDenied), InvalidArgument for a bad
max-parts or part-number-marker, NoSuchBucket, NoSuchUpload. ListParts is a read: it
changes no row and no file.
"""

import base64
import hashlib
import re
import sqlite3
import xml.etree.ElementTree as ET
import zlib
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote

import pytest
from awscrt import checksums
from fastapi.testclient import TestClient

from server.app import create_app
from server.config import settings
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

OWNER = "owner-id"
DISPLAY = "Owner Name"
BUCKET = "lp-bucket"
OTHER_BUCKET = "lp-other"
KEY = "dir/object.bin"
TOP_LEVEL = [
    "Bucket",
    "Key",
    "UploadId",
    "PartNumberMarker",
    "MaxParts",
    "IsTruncated",
    "Part",
    "Initiator",
    "Owner",
    "StorageClass",
]
TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.000Z")


@pytest.fixture(autouse=True)
def owner(monkeypatch):
    monkeypatch.setattr(settings, "owner_id", OWNER)
    monkeypatch.setattr(settings, "owner_display_name", DISPLAY)


@dataclass
class Env:
    client: TestClient
    root: Path


@pytest.fixture
def env(tmp_path) -> Env:
    app = create_app(
        storage=BlobStore(tmp_path / "blobs"),
        metadata=SQLiteMetadataStore(db_path=tmp_path / "meta.db"),
    )
    client = TestClient(app, raise_server_exceptions=False)
    for bucket in (BUCKET, OTHER_BUCKET):
        assert client.put(f"/{bucket}").status_code == 200
    return Env(client, tmp_path)


# --- helpers ---


def _digest(algorithm: str, data: bytes) -> str:
    """The base64 checksum of data, from libraries other than the server's own code."""
    raw = {
        "CRC32": lambda: zlib.crc32(data).to_bytes(4, "big"),
        "CRC32C": lambda: checksums.crc32c(data).to_bytes(4, "big"),
        "CRC64NVME": lambda: checksums.crc64nvme(data).to_bytes(8, "big"),
        "SHA1": lambda: hashlib.sha1(data).digest(),
        "SHA256": lambda: hashlib.sha256(data).digest(),
    }[algorithm]()
    return base64.b64encode(raw).decode()


def _etag(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def _start(client, key: str = KEY, bucket: str = BUCKET, headers: dict | None = None) -> str:
    resp = client.post(f"/{bucket}/{key}?uploads", headers=headers or {})
    assert resp.status_code == 200
    return ET.fromstring(resp.text).findtext("UploadId")


def _put(client, uid: str, number: int, data: bytes, key: str = KEY, bucket: str = BUCKET) -> str:
    resp = client.put(f"/{bucket}/{key}?partNumber={number}&uploadId={uid}", content=data)
    assert resp.status_code == 200
    return resp.headers["etag"]


def _data(number: int) -> bytes:
    return bytes([64 + number % 26]) * (number * 3)


def _list(client, uid: str, key: str = KEY, bucket: str = BUCKET, headers=None, **query):
    parts = [f"uploadId={uid}", *(f"{name}={value}" for name, value in query.items())]
    return client.get(f"/{bucket}/{key}?" + "&".join(parts), headers=headers or {})


def _ok(resp) -> ET.Element:
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith("application/xml")
    root = ET.fromstring(resp.text)
    assert root.tag == "ListPartsResult"
    return root


def _numbers(root: ET.Element) -> list[int]:
    return [int(p.findtext("PartNumber")) for p in root.findall("Part")]


def _error(resp, status: int, code: str) -> ET.Element:
    assert resp.status_code == status, resp.text
    root = ET.fromstring(resp.text)
    assert root.tag == "Error"
    assert root.findtext("Code") == code
    assert resp.headers["x-amz-request-id"]
    return root


def _raw(env: Env, sql: str, args: tuple = ()) -> list:
    con = sqlite3.connect(env.root / "meta.db")
    try:
        rows = con.execute(sql, args).fetchall()
        con.commit()
        return rows
    finally:
        con.close()


def _insert_part(env: Env, uid: str, number: int, **columns) -> None:
    """A part row written with raw sqlite3, as no request can write it. blob_id is NULL,
    as on a part whose bytes were released."""
    row = {
        "etag": '"raw"',
        "size": 1,
        "checksum": None,
        "checksum_algorithm": None,
        "last_modified": "2026-01-02T03:04:05.678901",
        **columns,
    }
    _raw(
        env,
        "INSERT INTO multipart_parts (upload_id, part_number, blob_id, etag, size, checksum,"
        " checksum_algorithm, last_modified) VALUES (?, ?, NULL, ?, ?, ?, ?, ?)",
        (
            uid,
            number,
            row["etag"],
            row["size"],
            row["checksum"],
            row["checksum_algorithm"],
            row["last_modified"],
        ),
    )


def _dump(env: Env) -> tuple:
    """Every row of every table and every file with its size: all the store holds."""
    con = sqlite3.connect(env.root / "meta.db")
    try:
        tables = [
            r[0]
            for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY 1")
        ]
        rows = {t: sorted(con.execute(f"SELECT * FROM {t}").fetchall(), key=repr) for t in tables}
    finally:
        con.close()
    files = sorted(
        (str(p.relative_to(env.root)), p.stat().st_size)
        for p in (env.root / "blobs").rglob("*")
        if p.is_file()
    )
    return rows, files


# --- the elements and their order ---


def test_an_unnamed_upload_lists_every_element_in_order(env):
    client = env.client
    uid = _start(client)
    etags = {n: _put(client, uid, n, _data(n)) for n in (1, 2)}

    resp = _list(client, uid)
    root = _ok(resp)

    assert [child.tag for child in root] == [*TOP_LEVEL[:6], "Part", "Part", *TOP_LEVEL[7:]]
    assert root.findtext("Bucket") == BUCKET
    assert root.findtext("Key") == KEY
    assert root.findtext("UploadId") == uid
    assert root.findtext("PartNumberMarker") == "0"
    assert root.findtext("MaxParts") == "1000"
    assert root.findtext("IsTruncated") == "false"
    assert root.find("NextPartNumberMarker") is None
    assert root.findtext("StorageClass") == "STANDARD"
    for who in ("Initiator", "Owner"):
        element = root.find(who)
        assert [c.tag for c in element] == ["ID", "DisplayName"]
        assert element.findtext("ID") == OWNER
        assert element.findtext("DisplayName") == DISPLAY
    # An unnamed upload names no algorithm, but its parts carry the default CRC64NVME.
    assert root.find("ChecksumAlgorithm") is None
    assert root.find("ChecksumType") is None
    for part in root.findall("Part"):
        number = int(part.findtext("PartNumber"))
        assert [c.tag for c in part] == [
            "PartNumber",
            "LastModified",
            "ETag",
            "Size",
            "ChecksumCRC64NVME",
        ]
        assert TIMESTAMP.fullmatch(part.findtext("LastModified"))
        assert part.findtext("ETag") == etags[number] == _etag(_data(number))
        assert part.findtext("Size") == str(len(_data(number)))
        assert part.findtext("ChecksumCRC64NVME") == _digest("CRC64NVME", _data(number))
    assert "x-amz-abort-date" not in resp.headers
    assert "x-amz-abort-rule-id" not in resp.headers


def test_last_modified_is_the_time_the_part_was_committed(env):
    client = env.client
    uid = _start(client)
    _put(client, uid, 1, b"one")
    stored = _raw(env, "SELECT last_modified FROM multipart_parts WHERE upload_id = ?", (uid,))
    part = _ok(_list(client, uid)).find("Part")
    assert part.findtext("LastModified") == stored[0][0][:19] + ".000Z"


def test_an_upload_with_no_parts_lists_none(env):
    uid = _start(env.client)
    root = _ok(_list(env.client, uid))
    assert [child.tag for child in root] == [
        "Bucket",
        "Key",
        "UploadId",
        "PartNumberMarker",
        "MaxParts",
        "IsTruncated",
        "Initiator",
        "Owner",
        "StorageClass",
    ]
    assert root.findtext("IsTruncated") == "false"


def test_the_key_is_echoed_decoded(env):
    encoded = "a%20b/%C3%BC%2B%3F.txt"
    uid = _start(env.client, key=encoded)
    root = _ok(_list(env.client, uid, key=encoded))
    assert root.findtext("Key") == "a b/ü+?.txt"


# --- checksums ---


@pytest.mark.parametrize(
    ("algorithm", "type_"),
    [
        ("CRC32", "COMPOSITE"),
        ("CRC32C", "COMPOSITE"),
        ("SHA1", "COMPOSITE"),
        ("SHA256", "COMPOSITE"),
        ("CRC32", "FULL_OBJECT"),
        ("CRC32C", "FULL_OBJECT"),
        ("CRC64NVME", "FULL_OBJECT"),
    ],
)
def test_a_named_upload_lists_its_algorithm_type_and_part_checksums(env, algorithm, type_):
    client = env.client
    uid = _start(
        client, headers={"x-amz-checksum-algorithm": algorithm, "x-amz-checksum-type": type_}
    )
    for number in (1, 2):
        _put(client, uid, number, _data(number))

    root = _ok(_list(client, uid))

    assert [child.tag for child in root][-2:] == ["ChecksumAlgorithm", "ChecksumType"]
    assert root.findtext("ChecksumAlgorithm") == algorithm
    assert root.findtext("ChecksumType") == type_
    for part in root.findall("Part"):
        number = int(part.findtext("PartNumber"))
        assert [c.tag for c in part][-1] == f"Checksum{algorithm}"
        assert part.findtext(f"Checksum{algorithm}") == _digest(algorithm, _data(number))
        assert len([c for c in part if c.tag.startswith("Checksum")]) == 1


def test_a_part_with_a_null_checksum_omits_the_element_beside_one_that_has_it(env):
    # State of an upload that was open when the store was migrated from schema version 2
    # to 3 (docs/design/storage-layout.md §20): its parts hold bytes and a last_modified
    # taken from their blob, but no checksum, so both checksum columns are NULL. No
    # request can write that row, so it is made with raw sqlite3.
    client = env.client
    uid = _start(client)
    _put(client, uid, 1, b"new part")
    _insert_part(env, uid, 2, etag='"old-part"', size=8)

    root = _ok(_list(client, uid))

    first, second = root.findall("Part")
    assert first.findtext("ChecksumCRC64NVME") == _digest("CRC64NVME", b"new part")
    assert [c.tag for c in second] == ["PartNumber", "LastModified", "ETag", "Size"]
    assert second.findtext("ETag") == '"old-part"'
    assert second.findtext("LastModified") == "2026-01-02T03:04:05.000Z"


def test_a_replaced_part_shows_only_the_new_etag_size_and_checksum(env):
    client = env.client
    uid = _start(client, headers={"x-amz-checksum-algorithm": "SHA256"})
    _put(client, uid, 1, b"first version")
    _put(client, uid, 2, b"two")
    new = _put(client, uid, 1, b"replacement!!")

    root = _ok(_list(client, uid))

    assert _numbers(root) == [1, 2]
    part = root.find("Part")
    assert part.findtext("ETag") == new == _etag(b"replacement!!")
    assert part.findtext("Size") == "13"
    assert part.findtext("ChecksumSHA256") == _digest("SHA256", b"replacement!!")
    stored = _raw(
        env,
        "SELECT last_modified FROM multipart_parts WHERE upload_id = ? AND part_number = 1",
        (uid,),
    )
    assert part.findtext("LastModified") == stored[0][0][:19] + ".000Z"


# --- pagination ---


def _gappy(env) -> str:
    uid = _start(env.client)
    for number in (1, 3, 4, 7, 9):
        _put(env.client, uid, number, _data(number))
    return uid


def _walk(client, uid: str, max_parts: int) -> list[ET.Element]:
    pages = []
    marker = 0
    for _ in range(50):
        root = _ok(_list(client, uid, **{"max-parts": max_parts, "part-number-marker": marker}))
        pages.append(root)
        if root.findtext("IsTruncated") != "true":
            return pages
        marker = int(root.findtext("NextPartNumberMarker"))
    raise AssertionError("the listing did not end")


def test_pages_follow_next_part_number_marker_across_gaps(env):
    uid = _gappy(env)

    pages = _walk(env.client, uid, 2)

    assert [_numbers(p) for p in pages] == [[1, 3], [4, 7], [9]]
    assert [p.findtext("IsTruncated") for p in pages] == ["true", "true", "false"]
    assert [p.findtext("PartNumberMarker") for p in pages] == ["0", "3", "7"]
    assert [p.findtext("NextPartNumberMarker") for p in pages] == ["3", "7", None]
    assert {p.findtext("MaxParts") for p in pages} == {"2"}


def test_a_page_that_ends_exactly_on_the_last_part_is_not_truncated(env):
    uid = _gappy(env)
    pages = _walk(env.client, uid, 5)
    assert [_numbers(p) for p in pages] == [[1, 3, 4, 7, 9]]
    assert pages[0].findtext("IsTruncated") == "false"
    assert pages[0].find("NextPartNumberMarker") is None


@pytest.mark.parametrize(
    ("marker", "expected"),
    [
        ("0", [1, 3, 4, 7, 9]),
        ("1", [3, 4, 7, 9]),
        ("2", [3, 4, 7, 9]),  # between part numbers
        ("3", [4, 7, 9]),  # a part number: only higher parts are listed
        ("5", [7, 9]),
        ("8", [9]),
        ("9", []),  # the highest part
        ("10", []),  # above the highest part
        ("10000", []),
        ("10001", []),
        ("99999999999999999999", []),
        ("9" * 5000, []),
    ],
)
def test_only_parts_above_the_marker_are_listed(env, marker, expected):
    uid = _gappy(env)
    root = _ok(_list(env.client, uid, **{"part-number-marker": marker}))
    assert _numbers(root) == expected
    assert root.findtext("PartNumberMarker") == marker
    assert root.findtext("IsTruncated") == "false"
    assert root.find("NextPartNumberMarker") is None


def test_the_marker_is_echoed_as_a_number(env):
    uid = _gappy(env)
    root = _ok(_list(env.client, uid, **{"part-number-marker": "0003"}))
    assert root.findtext("PartNumberMarker") == "3"
    assert _numbers(root) == [4, 7, 9]


def test_a_marker_inside_a_truncated_listing_gives_the_next_marker(env):
    uid = _gappy(env)
    root = _ok(_list(env.client, uid, **{"part-number-marker": 2, "max-parts": 2}))
    assert _numbers(root) == [3, 4]
    assert root.findtext("IsTruncated") == "true"
    assert root.findtext("NextPartNumberMarker") == "4"
    assert [c.tag for c in root][:7] == [
        "Bucket",
        "Key",
        "UploadId",
        "PartNumberMarker",
        "NextPartNumberMarker",
        "MaxParts",
        "IsTruncated",
    ]


# --- max-parts and part-number-marker values ---


def test_max_parts_one_lists_the_first_part_and_is_truncated(env):
    uid = _gappy(env)
    root = _ok(_list(env.client, uid, **{"max-parts": 1}))
    assert _numbers(root) == [1]
    assert root.findtext("MaxParts") == "1"
    assert root.findtext("IsTruncated") == "true"
    assert root.findtext("NextPartNumberMarker") == "1"


def test_max_parts_zero_lists_no_parts_and_is_not_truncated(env):
    # As ListObjects does for max-keys=0. The API reference does not say (not found).
    uid = _gappy(env)
    root = _ok(_list(env.client, uid, **{"max-parts": 0}))
    assert _numbers(root) == []
    assert root.findtext("MaxParts") == "0"
    assert root.findtext("IsTruncated") == "false"
    assert root.find("NextPartNumberMarker") is None


@pytest.mark.parametrize("value", ["1001", "5000", "99999999999999999999", "9" * 5000])
def test_max_parts_above_the_cap_is_the_cap(env, value):
    uid = _gappy(env)
    root = _ok(_list(env.client, uid, **{"max-parts": value}))
    assert root.findtext("MaxParts") == "1000"
    assert _numbers(root) == [1, 3, 4, 7, 9]


def test_max_parts_is_read_as_a_number(env):
    uid = _gappy(env)
    root = _ok(_list(env.client, uid, **{"max-parts": "002"}))
    assert root.findtext("MaxParts") == "2"
    assert _numbers(root) == [1, 3]


def test_the_cap_and_the_default_are_1000_parts(env):
    uid = _start(env.client)
    con = sqlite3.connect(env.root / "meta.db")
    try:
        con.executemany(
            "INSERT INTO multipart_parts (upload_id, part_number, blob_id, etag, size,"
            " last_modified) VALUES (?, ?, NULL, '\"e\"', 1, '2026-01-01T00:00:00.000000')",
            [(uid, n) for n in range(1, 1006)],
        )
        con.commit()
    finally:
        con.close()

    for query in ({}, {"max-parts": 1000}, {"max-parts": 5000}):
        root = _ok(_list(env.client, uid, **query))
        assert _numbers(root) == list(range(1, 1001))
        assert root.findtext("MaxParts") == "1000"
        assert root.findtext("IsTruncated") == "true"
        assert root.findtext("NextPartNumberMarker") == "1000"
    rest = _ok(_list(env.client, uid, **{"part-number-marker": 1000}))
    assert _numbers(rest) == [1001, 1002, 1003, 1004, 1005]
    assert rest.findtext("IsTruncated") == "false"


@pytest.mark.parametrize("value", ["-1", "1.5", "abc", "", " 1", "1 ", "%2B1", "1e3", "0x10", "１"])
@pytest.mark.parametrize("name", ["max-parts", "part-number-marker"])
def test_a_value_that_is_not_a_decimal_integer_is_invalid_argument(env, name, value):
    uid = _gappy(env)
    before = _dump(env)
    error = _error(_list(env.client, uid, **{name: value}), 400, "InvalidArgument")
    assert error.findtext("ArgumentName") == name
    assert error.findtext("ArgumentValue") == unquote(value)
    assert _dump(env) == before


# --- the upload must be open, and be the request's ---


def test_a_completed_upload_is_no_such_upload(env):
    client = env.client
    uid = _start(client)
    etag = _put(client, uid, 1, b"data")
    body = (
        f"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part>"
        "</CompleteMultipartUpload>"
    )
    assert client.post(f"/{BUCKET}/{KEY}?uploadId={uid}", content=body).status_code == 200

    error = _error(_list(client, uid), 404, "NoSuchUpload")
    assert error.findtext("UploadId") == uid


def test_an_aborted_upload_is_no_such_upload(env):
    client = env.client
    uid = _start(client)
    _put(client, uid, 1, b"data")
    assert client.delete(f"/{BUCKET}/{KEY}?uploadId={uid}").status_code == 204
    _error(_list(client, uid), 404, "NoSuchUpload")


def test_an_unknown_upload_is_no_such_upload(env):
    _error(_list(env.client, "no-such-upload"), 404, "NoSuchUpload")


def test_a_blank_upload_id_is_no_such_upload_as_for_complete_and_abort(env):
    client = env.client
    _start(client)
    complete = client.post(f"/{BUCKET}/{KEY}?uploadId=", content=b"<CompleteMultipartUpload/>")
    abort = client.delete(f"/{BUCKET}/{KEY}?uploadId=")
    assert (complete.status_code, abort.status_code) == (404, 404)
    error = _error(client.get(f"/{BUCKET}/{KEY}?uploadId="), 404, "NoSuchUpload")
    assert error.findtext("UploadId") == ""


@pytest.mark.parametrize(
    ("bucket", "key"),
    [
        (OTHER_BUCKET, KEY),  # an upload of another bucket
        (BUCKET, "dir/other.bin"),  # of another key
        (BUCKET, "DIR/OBJECT.BIN"),  # of a key that differs in case
        (BUCKET, "dir/object.bin/"),
        (BUCKET, "dir/object.bi"),
    ],
)
def test_an_upload_of_another_bucket_or_key_is_no_such_upload(env, bucket, key):
    client = env.client
    uid = _start(client)
    _put(client, uid, 1, b"data")
    # The same key exists in the other places, so only the upload differs.
    for other_bucket, other_key in ((OTHER_BUCKET, KEY), (BUCKET, "dir/other.bin")):
        _start(client, key=other_key, bucket=other_bucket)

    _error(_list(client, uid, key=key, bucket=bucket), 404, "NoSuchUpload")
    assert _numbers(_ok(_list(client, uid))) == [1]


def test_a_missing_bucket_is_no_such_bucket(env):
    error = _error(_list(env.client, "u", bucket="no-bucket"), 404, "NoSuchBucket")
    assert error.findtext("BucketName") == "no-bucket"


# --- the order of the errors ---


def test_invalid_argument_beats_no_such_bucket_and_no_such_upload(env):
    _error(
        _list(env.client, "u", bucket="no-bucket", **{"max-parts": "-1"}), 400, "InvalidArgument"
    )
    _error(_list(env.client, "u", **{"part-number-marker": "x"}), 400, "InvalidArgument")


def test_max_parts_is_checked_before_the_marker(env):
    error = _error(
        _list(env.client, "u", **{"part-number-marker": "x", "max-parts": "y"}),
        400,
        "InvalidArgument",
    )
    assert error.findtext("ArgumentName") == "max-parts"


def test_no_such_bucket_beats_no_such_upload(env):
    _error(_list(env.client, "unknown", bucket="no-bucket"), 404, "NoSuchBucket")


def test_a_wrong_owner_beats_invalid_argument_and_every_lookup(env):
    wrong = {"x-amz-expected-bucket-owner": "someone-else"}
    uid = _start(env.client)
    for kwargs in (
        {"uid": uid, "max-parts": "-1"},
        {"uid": "unknown"},
        {"uid": uid, "bucket": "no-bucket"},
    ):
        uid_ = kwargs.pop("uid")
        resp = _list(env.client, uid_, headers=wrong, **kwargs)
        _error(resp, 403, "AccessDenied")


def test_not_implemented_beats_a_wrong_owner_and_invalid_argument(env):
    wrong = {"x-amz-expected-bucket-owner": "someone-else"}
    uid = _start(env.client)
    resp = _list(env.client, uid, headers=wrong, partNumber=1, **{"max-parts": "-1"})
    _error(resp, 501, "NotImplemented")


def test_the_expected_owner_is_accepted_when_it_matches(env):
    uid = _gappy(env)
    root = _ok(_list(env.client, uid, headers={"x-amz-expected-bucket-owner": OWNER}))
    assert _numbers(root) == [1, 3, 4, 7, 9]


# --- requests that are still not ListParts ---


@pytest.mark.parametrize(
    "extra",
    [
        "versionId=null",
        "versionId=other",
        "partNumber=1",
        "response-content-type=text/plain",
        "tagging",
        "acl",
        "uploads",
        "attributes",
        "encoding-type=url",
        "prefix=a",
        "delimiter=/",
        "max-keys=1",
        "list-type=2",
        "max-uploads=1",
        "key-marker=a",
        "upload-id-marker=a",
        "continuation-token=a",
    ],
)
def test_a_listparts_request_with_another_query_parameter_is_501(env, extra):
    uid = _gappy(env)
    before = _dump(env)
    error = _error(env.client.get(f"/{BUCKET}/{KEY}?uploadId={uid}&{extra}"), 501, "NotImplemented")
    assert error.findtext("Method") == "GET"
    assert _dump(env) == before
    assert _numbers(_ok(_list(env.client, uid))) == [1, 3, 4, 7, 9]


def test_head_with_an_upload_id_is_501(env):
    uid = _gappy(env)
    assert env.client.head(f"/{BUCKET}/{KEY}?uploadId={uid}").status_code == 501


def test_the_sdk_operation_hint_is_not_a_query_parameter(env):
    uid = _gappy(env)
    root = _ok(_list(env.client, uid, **{"x-id": "ListParts", "max-parts": 2}))
    assert _numbers(root) == [1, 3]


def test_request_payer_is_ignored(env):
    # Ignored as on every operation, and no x-amz-request-charged is sent.
    uid = _gappy(env)
    plain = _list(env.client, uid)
    asked = _list(env.client, uid, headers={"x-amz-request-payer": "requester"})
    assert asked.status_code == 200
    assert asked.text == plain.text
    assert "x-amz-request-charged" not in asked.headers


SSE_C = {
    "x-amz-server-side-encryption-customer-algorithm": "AES256",
    "x-amz-server-side-encryption-customer-key": "a" * 44,
    "x-amz-server-side-encryption-customer-key-MD5": "b" * 24,
}


@pytest.mark.parametrize("name", list(SSE_C))
def test_an_sse_c_header_is_501_as_for_upload_part(env, name):
    uid = _gappy(env)
    before = _dump(env)
    error = _error(_list(env.client, uid, headers={name: SSE_C[name]}), 501, "NotImplemented")
    assert error.findtext("Header") == name.lower()
    assert _dump(env) == before


def test_the_sse_c_501_beats_a_wrong_expected_owner(env):
    uid = _gappy(env)
    headers = {"x-amz-expected-bucket-owner": "someone-else", **SSE_C}
    _error(_list(env.client, uid, headers=headers), 501, "NotImplemented")


@pytest.mark.parametrize("value", ["", "   "])
def test_a_blank_sse_c_header_is_absent(env, value):
    uid = _gappy(env)
    plain = _list(env.client, uid)
    asked = _list(env.client, uid, headers={name: value for name in SSE_C})
    assert asked.status_code == 200
    assert asked.text == plain.text


# --- a read changes nothing ---


def test_listparts_changes_no_row_and_no_file(env):
    client = env.client
    uid = _start(client, headers={"x-amz-checksum-algorithm": "CRC32"})
    for number in (1, 2, 3):
        _put(client, uid, number, _data(number))
    done = _start(client, key="done")
    etag = _put(client, done, 1, b"x", key="done")
    body = (
        f"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part>"
        "</CompleteMultipartUpload>"
    )
    assert client.post(f"/{BUCKET}/done?uploadId={done}", content=body).status_code == 200
    before = _dump(env)

    assert _list(client, uid).status_code == 200
    assert _list(client, uid, **{"max-parts": 1}).status_code == 200
    assert _list(client, uid, **{"part-number-marker": 1, "max-parts": 1}).status_code == 200
    assert _list(client, uid, **{"max-parts": 0}).status_code == 200
    assert _list(client, uid, **{"part-number-marker": 50}).status_code == 200
    assert _list(client, done, key="done").status_code == 404
    assert _list(client, "unknown").status_code == 404
    assert _list(client, uid, bucket="no-bucket").status_code == 404
    assert _list(client, uid, **{"max-parts": "-1"}).status_code == 400
    assert _list(client, uid, partNumber=1).status_code == 501

    assert _dump(env) == before
    # The upload is still open and complete-able after being listed.
    assert _numbers(_ok(_list(client, uid))) == [1, 2, 3]
