"""Listings paged by boto3 itself, against the real app under uvicorn.

boto3's paginators carry NextMarker, NextContinuationToken and NextKeyMarker /
NextVersionIdMarker from page to page, and ceph/s3-tests deletes every test bucket's
objects by paging ListObjectVersions with MaxKeys=128 (s3tests/functional/__init__.py,
list_versions). Both must see every key exactly once.

botocore adds encoding-type=url to every ListObjects, ListObjectsV2 and
ListObjectVersions request it builds and, when the response echoes EncodingType,
decodes the key-valued elements with unquote_plus (botocore/handlers.py,
set_list_objects_encoding_type_url and decode_list_object*). So every key here,
including ones with a space, "+", "%", control characters and code points XML 1.0
does not allow, must come back through boto3 exactly as it was put.
"""

import socket
import threading
import time
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import quote

import boto3
import pytest
import uvicorn
from botocore.config import Config

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "paged"
# Keys botocore's decoding must leave unchanged: a space (%20, not "+"), "+" (%2B,
# which unquote_plus would otherwise turn into a space), "%" and "%XX" sequences,
# "/", non-ASCII, AWS's own example, every reserved URL character, control
# characters, CR (which an unencoded XML response would turn into LF) and the two
# code points XML 1.0 forbids outright.
SPECIAL_KEYS = [
    "sp ace/x y",
    "plus+plus",
    "a+b c/d+e",
    "a+b c/f",
    "100%",
    "pct%41",
    "%2F",
    "test_file(3).png",
    "-_.~!*'",
    "q?x#y&z=w",
    "<>\"'\\{}^`][|",
    "ctrl\x01\x1f\x7f",
    "tab\tcr\r",
    "\ufffe\uffff",
]
KEYS = sorted(
    {f"d{i % 7}/k{i:03}" for i in range(150)}
    | {f"top{i:02}" for i in range(30)}
    | {"é/1", "日"}
    | set(SPECIAL_KEYS),
    key=lambda k: k.encode("utf-8"),
)


@pytest.fixture(scope="module")
def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def s3(tmp_path_factory, port):
    tmp: Path = tmp_path_factory.mktemp("boto3")
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
    yield client
    server.should_exit = True
    thread.join(timeout=10)


@pytest.fixture(scope="module")
def bucket(s3):
    s3.create_bucket(Bucket=BUCKET)
    for key in KEYS:
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"x")
    yield BUCKET
    for key in KEYS:
        s3.delete_object(Bucket=BUCKET, Key=key)
    s3.delete_bucket(Bucket=BUCKET)


def _top_level():
    entries = {k.split("/", 1)[0] + "/" if "/" in k else k for k in KEYS}
    return sorted(entries, key=lambda k: k.encode("utf-8"))


OPERATIONS = [
    ("list_objects", "Contents"),
    ("list_objects_v2", "Contents"),
    ("list_object_versions", "Versions"),
]


@pytest.mark.parametrize("operation,items", OPERATIONS)
def test_default_client_round_trips_every_key(s3, bucket, operation, items):
    # No EncodingType given: botocore adds encoding-type=url and decodes the keys.
    response = getattr(s3, operation)(Bucket=bucket)
    assert [item["Key"] for item in response[items]] == KEYS
    assert response["EncodingType"] == "url"


@pytest.mark.parametrize("operation,items", OPERATIONS)
def test_explicit_encoding_type_returns_encoded_keys(s3, bucket, operation, items):
    # EncodingType given by the caller: botocore does not decode, and the keys are
    # S3's url encoding (ceph/s3-tests test_bucket_list_encoding_basic).
    response = getattr(s3, operation)(Bucket=bucket, EncodingType="url", Delimiter="/")
    assert response["EncodingType"] == "url"
    assert response["Delimiter"] == "/"
    entries = [item["Key"] for item in response.get(items, [])]
    entries += [p["Prefix"] for p in response.get("CommonPrefixes", [])]
    assert sorted(entries) == sorted(quote(e, safe="/") for e in _top_level())
    assert "plus%2Bplus" in entries and "sp%20ace/" in entries


def test_default_client_decodes_echoed_parameters(s3, bucket):
    # The echoed parameters botocore decodes come back as sent, element by element.
    v1 = s3.list_objects(Bucket=bucket, Prefix="a+b c", Delimiter="+", Marker="a+a", MaxKeys=1)
    assert v1["Marker"] == "a+a" and v1["Delimiter"] == "+"
    assert v1["Prefix"] == "a+b c"
    assert v1["IsTruncated"] and v1["NextMarker"] == "a+b c/d+"
    assert v1["CommonPrefixes"] == [{"Prefix": "a+b c/d+"}]
    v2 = s3.list_objects_v2(
        Bucket=bucket, Prefix="a+b c", Delimiter="+", StartAfter="a+a", MaxKeys=1
    )
    assert v2["StartAfter"] == "a+a" and v2["Delimiter"] == "+" and v2["Prefix"] == "a+b c"
    assert v2["CommonPrefixes"] == [{"Prefix": "a+b c/d+"}]
    nxt = s3.list_objects_v2(Bucket=bucket, ContinuationToken=v2["NextContinuationToken"])
    assert nxt["ContinuationToken"] == v2["NextContinuationToken"]
    assert nxt["Contents"][0]["Key"] == "a+b c/f"
    versions = s3.list_object_versions(
        Bucket=bucket, Prefix="a+b c", Delimiter="+", KeyMarker="a+a", MaxKeys=1
    )
    assert versions["KeyMarker"] == "a+a" and versions["Delimiter"] == "+"
    assert versions["Prefix"] == "a+b c" and versions["NextKeyMarker"] == "a+b c/d+"
    assert versions["CommonPrefixes"] == [{"Prefix": "a+b c/d+"}]
    versions = s3.list_object_versions(Bucket=bucket, Prefix="sp ace", MaxKeys=1)
    assert versions["Versions"][0]["Key"] == "sp ace/x y"


def test_v1_prefix_is_not_decoded_by_botocore(s3, bucket):
    # botocore decodes Delimiter, Marker, NextMarker, Key and CommonPrefixes/Prefix
    # for ListObjects but not Prefix (botocore/handlers.py decode_list_object), so
    # Litmus does not encode it: this is the raw value because it was never encoded.
    assert s3.list_objects(Bucket=bucket, Prefix="sp ace/x y")["Prefix"] == "sp ace/x y"
    assert (
        s3.list_objects(Bucket=bucket, Prefix="sp ace/x y", EncodingType="url")["Prefix"]
        == "sp ace/x y"
    )


@pytest.mark.parametrize("query", ["", "?list-type=2", "?versions"])
def test_without_encoding_type_the_response_is_not_xml(port, bucket, query):
    # The keys with U+0001 and U+FFFE make the unencoded response ill-formed XML
    # (docs/compatibility.md, "Characters XML 1.0 does not allow"); with
    # encoding-type=url it parses and every key decodes.
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/{bucket}{query}") as resp:
        body = resp.read()
    with pytest.raises(ET.ParseError):
        ET.fromstring(body)
    sep = "&" if query else "?"
    url = f"http://127.0.0.1:{port}/{bucket}{query}{sep}encoding-type=url"
    with urllib.request.urlopen(url) as resp:
        root = ET.fromstring(resp.read())
    element = "Version" if query == "?versions" else "Contents"
    assert [el.findtext("Key") for el in root.findall(element)] == [
        quote(k, safe="/") for k in KEYS
    ]


@pytest.mark.parametrize("page_size", [1, 7, 128])
def test_paginators_list_every_key_once(s3, bucket, page_size):
    for operation, items in [
        ("list_objects", "Contents"),
        ("list_objects_v2", "Contents"),
        ("list_object_versions", "Versions"),
    ]:
        pages = s3.get_paginator(operation).paginate(
            Bucket=bucket, PaginationConfig={"PageSize": page_size}
        )
        got = [item["Key"] for page in pages for item in page.get(items, [])]
        assert got == KEYS, operation


@pytest.mark.parametrize("page_size", [1, 3, 128])
def test_paginators_with_delimiter(s3, bucket, page_size):
    for operation, items in [
        ("list_objects", "Contents"),
        ("list_objects_v2", "Contents"),
        ("list_object_versions", "Versions"),
    ]:
        pages = s3.get_paginator(operation).paginate(
            Bucket=bucket, Delimiter="/", PaginationConfig={"PageSize": page_size}
        )
        entries = []
        for page in pages:
            entries += [item["Key"] for item in page.get(items, [])]
            entries += [p["Prefix"] for p in page.get("CommonPrefixes", [])]
        assert sorted(entries, key=lambda k: k.encode("utf-8")) == _top_level(), operation
        assert len(entries) == len(set(entries)), operation


def test_ceph_teardown_loop_deletes_everything(s3):
    # s3tests/functional/__init__.py list_versions and nuke_bucket, at the pinned commit:
    # page ListObjectVersions with MaxKeys=128 and delete each batch before the next.
    s3.create_bucket(Bucket="teardown")
    # The keys of test_bucket_list_delimiter_not_skip_special, cut to 305, plus the
    # special keys: three pages, and every key must decode to the one that was put
    # or DeleteObjects deletes a different key and the bucket cannot be emptied.
    keys = [f"0/{i}" for i in range(1000, 1300)] + ["0/", "1999", "1999#", "1999+", "2000"]
    # DeleteObjects carries keys in an XML body, so a key with a character XML 1.0
    # forbids cannot be deleted that way, and botocore writes a CR literally, which
    # the parser turns into LF (docs/compatibility.md, "Characters XML 1.0 does not
    # allow"). Those keys are listed but left out here.
    keys += [k for k in SPECIAL_KEYS if k not in ("ctrl\x01\x1f\x7f", "tab\tcr\r", "\ufffe\uffff")]
    for key in keys:
        s3.put_object(Bucket="teardown", Key=key, Body=b"")
    kwargs = {"Bucket": "teardown", "MaxKeys": 128}
    deleted, truncated = [], True
    while truncated:
        listing = s3.list_object_versions(**kwargs)
        kwargs["KeyMarker"] = listing.get("NextKeyMarker")
        kwargs["VersionIdMarker"] = listing.get("NextVersionIdMarker")
        truncated = listing["IsTruncated"]
        objs = listing.get("Versions", []) + listing.get("DeleteMarkers", [])
        if objs:
            batch = [{"Key": o["Key"], "VersionId": o["VersionId"]} for o in objs]
            s3.delete_objects(Bucket="teardown", Delete={"Objects": batch})
            deleted += [o["Key"] for o in batch]
    assert sorted(deleted) == sorted(keys)
    assert s3.list_objects_v2(Bucket="teardown")["KeyCount"] == 0
    s3.delete_bucket(Bucket="teardown")
