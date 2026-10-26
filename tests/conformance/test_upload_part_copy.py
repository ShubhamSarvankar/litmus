"""UploadPartCopy: PUT with x-amz-copy-source, partNumber and uploadId
(docs/design/storage-layout.md §6.6, §11).

The bytes of the part are copied from a range of the source blob into a part blob of its
own, so the part and its source share nothing. Every refusal leaves the store as it was:
the part rows, the blob registry, the blob files and tmp/ (checked with the consistency
sweep).
"""

import hashlib
import os
import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"
SRC = "src.bin"
DST = "dest.bin"
BODY = bytes(range(256)) * 4  # 1024 bytes, every offset distinct within 256
HTTP_DATE = "%a, %d %b %Y %H:%M:%S GMT"


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def _composite(*parts: bytes) -> str:
    digest = hashlib.md5(b"".join(hashlib.md5(p).digest() for p in parts)).hexdigest()
    return f'"{digest}-{len(parts)}"'


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    for bucket in (SRC_BUCKET, DST_BUCKET):
        assert client.put(f"/{bucket}").status_code == 200
    assert client.put(f"/{SRC_BUCKET}/{SRC}", content=BODY).status_code == 200
    upload = ET.fromstring(client.post(f"/{DST_BUCKET}/{DST}?uploads").text).findtext("UploadId")
    return client, storage, metadata, upload


def _copy_part(client, upload, n=1, src=f"{SRC_BUCKET}/{SRC}", bucket=DST_BUCKET, **headers):
    return client.put(
        f"/{bucket}/{DST}?partNumber={n}&uploadId={upload}",
        headers={"x-amz-copy-source": src, **headers},
    )


def _code(resp) -> str:
    return ET.fromstring(resp.text).findtext("Code")


def _part_bytes(storage, metadata, upload, n=1) -> bytes:
    (part,) = [p for p in metadata.get_parts(upload) if p.part_number == n]
    with storage.open_blob(part.blob_id) as f:
        return f.read()


def _complete(client, upload, parts: list[tuple[int, str]]):
    body = "".join(f"<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>" for n, e in parts)
    return client.post(
        f"/{DST_BUCKET}/{DST}?uploadId={upload}",
        content=f"<CompleteMultipartUpload>{body}</CompleteMultipartUpload>",
    )


def _state(storage, metadata, upload) -> tuple:
    """Everything a refused request must leave alone."""
    parts = metadata.get_parts(upload)
    registry = sorted((r.blob_id, r.state) for r in metadata.list_blob_records())
    return parts, registry, storage.temp_entries()


def _assert_clean(storage, metadata) -> None:
    assert run_consistency_sweep(metadata, storage).issues == []
    assert storage.temp_entries() == []
    assert {r.state for r in metadata.list_blob_records()} <= {"live"}


def _http_date(dt: datetime) -> str:
    return dt.strftime(HTTP_DATE)


# --- the part and its response ---


def test_copy_without_a_range_copies_the_whole_source(env):
    client, storage, metadata, upload = env

    resp = _copy_part(client, upload)

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/xml")
    root = ET.fromstring(resp.text)
    assert root.tag == "CopyPartResult"
    assert root.findtext("ETag") == _md5(BODY)
    last_modified = datetime.strptime(root.findtext("LastModified"), "%Y-%m-%dT%H:%M:%S.000Z")
    assert abs(datetime.now(UTC).replace(tzinfo=None) - last_modified) < timedelta(minutes=1)
    (part,) = metadata.get_parts(upload)
    assert (part.etag, part.size) == (_md5(BODY), len(BODY))
    assert _part_bytes(storage, metadata, upload) == BODY
    _assert_clean(storage, metadata)


@pytest.mark.parametrize(
    ("first", "last"),
    [(0, 0), (0, 9), (5, 99), (255, 256), (1023, 1023), (0, 1022), (1, 1023), (0, 1023)],
)
def test_copy_of_a_range_copies_exactly_those_bytes(env, first, last):
    client, storage, metadata, upload = env
    want = BODY[first : last + 1]

    resp = _copy_part(client, upload, **{"x-amz-copy-source-range": f"bytes={first}-{last}"})

    assert resp.status_code == 200
    assert ET.fromstring(resp.text).findtext("ETag") == _md5(want)
    assert _part_bytes(storage, metadata, upload) == want
    assert metadata.get_parts(upload)[0].size == len(want)
    _assert_clean(storage, metadata)


def test_copy_of_an_empty_source_is_an_empty_part(env):
    client, storage, metadata, upload = env
    assert client.put(f"/{SRC_BUCKET}/empty", content=b"").status_code == 200

    resp = _copy_part(client, upload, src=f"{SRC_BUCKET}/empty")

    assert resp.status_code == 200
    assert ET.fromstring(resp.text).findtext("ETag") == _md5(b"")
    assert _part_bytes(storage, metadata, upload) == b""
    _assert_clean(storage, metadata)


def test_the_copy_does_not_touch_its_source(env):
    client, storage, metadata, upload = env
    before = metadata.get_object_meta(SRC_BUCKET, SRC)

    assert _copy_part(client, upload).status_code == 200

    assert metadata.get_object_meta(SRC_BUCKET, SRC) == before
    assert client.get(f"/{SRC_BUCKET}/{SRC}").content == BODY
    (part,) = metadata.get_parts(upload)
    assert part.blob_id != before.blob_id


def test_version_id_null_names_the_current_object(env):
    client, storage, metadata, upload = env

    resp = _copy_part(client, upload, src=f"{SRC_BUCKET}/{SRC}?versionId=null")

    assert resp.status_code == 200
    assert _part_bytes(storage, metadata, upload) == BODY


def test_leading_slash_of_the_source_is_optional(env):
    client, _, _, upload = env
    assert _copy_part(client, upload, 1, src=f"/{SRC_BUCKET}/{SRC}").status_code == 200
    assert _copy_part(client, upload, 2, src=f"{SRC_BUCKET}/{SRC}").status_code == 200


def test_the_source_may_be_the_key_the_upload_will_create(env):
    client, storage, metadata, upload = env
    assert client.put(f"/{DST_BUCKET}/{DST}", content=BODY[:100]).status_code == 200

    resp = _copy_part(client, upload, src=f"{DST_BUCKET}/{DST}")

    assert resp.status_code == 200
    assert _part_bytes(storage, metadata, upload) == BODY[:100]
    assert client.get(f"/{DST_BUCKET}/{DST}").content == BODY[:100]


def test_percent_encoded_source_is_decoded_once(env):
    client, storage, metadata, upload = env
    # The key holds a literal "%25"; the header names it as "%2525".
    key = "anyfilename%25.txt"
    stored = quote(quote(key, safe="/"), safe="/")  # TestClient decodes the path again
    assert client.put(f"/{SRC_BUCKET}/{stored}", content=b"foo").status_code == 200

    # What ceph's test_upload_part_copy_percent_encoded_key sends for the key
    # "anyfilename%.txt": it is a different key, and it is not there.
    missing = _copy_part(client, upload, src=f"{SRC_BUCKET}/anyfilename%25.txt")
    assert (missing.status_code, _code(missing)) == (404, "NoSuchKey")
    assert metadata.get_parts(upload) == []

    found = _copy_part(client, upload, src=f"{SRC_BUCKET}/{quote(key, safe='/')}")
    assert found.status_code == 200
    assert _part_bytes(storage, metadata, upload) == b"foo"


@pytest.mark.parametrize("name", [" ", "_", "__", "?versionId", "a+b c", "é/ü"])
def test_special_source_names(env, name):
    client, storage, metadata, upload = env
    path = quote(quote(name, safe="/"), safe="/")
    assert client.put(f"/{SRC_BUCKET}/{path}", content=b"special").status_code == 200

    resp = _copy_part(
        client,
        upload,
        src=f"{SRC_BUCKET}/{quote(name, safe='/')}",
        **{"x-amz-copy-source-range": "bytes=0-3"},
    )

    assert resp.status_code == 200
    assert _part_bytes(storage, metadata, upload) == b"spec"


def test_a_multipart_source_is_read_as_its_bytes(env):
    client, storage, metadata, upload = env
    part1, part2 = os.urandom(3000), os.urandom(1000)
    mp = ET.fromstring(client.post(f"/{SRC_BUCKET}/mp?uploads").text).findtext("UploadId")
    etags = [
        client.put(f"/{SRC_BUCKET}/mp?partNumber={n}&uploadId={mp}", content=p).headers["etag"]
        for n, p in ((1, part1), (2, part2))
    ]
    body = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>" for n, e in enumerate(etags, 1)
    )
    assert (
        client.post(
            f"/{SRC_BUCKET}/mp?uploadId={mp}",
            content=f"<CompleteMultipartUpload>{body}</CompleteMultipartUpload>",
        ).status_code
        == 200
    )
    data = part1 + part2

    whole = _copy_part(client, upload, 1, src=f"{SRC_BUCKET}/mp")
    across = _copy_part(
        client, upload, 2, src=f"{SRC_BUCKET}/mp", **{"x-amz-copy-source-range": "bytes=2990-3009"}
    )

    assert ET.fromstring(whole.text).findtext("ETag") == _md5(data)
    assert ET.fromstring(across.text).findtext("ETag") == _md5(data[2990:3010])
    assert _part_bytes(storage, metadata, upload, 1) == data
    assert _part_bytes(storage, metadata, upload, 2) == data[2990:3010]
    _assert_clean(storage, metadata)


# --- the range ---


@pytest.mark.parametrize(
    "value",
    [
        "0-2",
        "bytes=0",
        "bytes=hello-world",
        "bytes=0-bar",
        "bytes=hello-",
        "bytes=0-2,3-5",
        "bytes=0-",
        "bytes=-5",
        "bytes=-1-2",
        "bytes=+1-2",
        "bytes= 1-2",
        "bytes=1 -2",
        "Bytes=1-2",
        "bytes=1-2 ",
        "bytes=",
        "",
        "bytes=5-2",
        "bytes=1-0",
        "bytes=1023-0",
    ],
)
def test_malformed_or_reversed_range_is_400_invalid_argument(env, value):
    client, storage, metadata, upload = env
    before = _state(storage, metadata, upload)

    resp = _copy_part(client, upload, **{"x-amz-copy-source-range": value})

    assert (resp.status_code, _code(resp)) == (400, "InvalidArgument")
    assert ET.fromstring(resp.text).findtext("ArgumentName") == "x-amz-copy-source-range"
    assert _state(storage, metadata, upload) == before
    _assert_clean(storage, metadata)


@pytest.mark.parametrize(
    "value", ["bytes=0-1024", "bytes=0-21000", "bytes=1024-1024", "bytes=5000-6000"]
)
def test_range_outside_the_source_is_416_invalid_range(env, value):
    client, storage, metadata, upload = env
    before = _state(storage, metadata, upload)

    resp = _copy_part(client, upload, **{"x-amz-copy-source-range": value})

    assert (resp.status_code, _code(resp)) == (416, "InvalidRange")
    assert _state(storage, metadata, upload) == before
    _assert_clean(storage, metadata)


def test_any_range_of_an_empty_source_is_416(env):
    client, storage, metadata, upload = env
    assert client.put(f"/{SRC_BUCKET}/empty", content=b"").status_code == 200

    resp = _copy_part(
        client, upload, src=f"{SRC_BUCKET}/empty", **{"x-amz-copy-source-range": "bytes=0-0"}
    )

    assert (resp.status_code, _code(resp)) == (416, "InvalidRange")
    assert metadata.get_parts(upload) == []


def test_a_repeated_range_header_is_400(env):
    client, _, metadata, upload = env
    resp = client.put(
        f"/{DST_BUCKET}/{DST}?partNumber=1&uploadId={upload}",
        headers=[
            ("x-amz-copy-source", f"{SRC_BUCKET}/{SRC}"),
            ("x-amz-copy-source-range", "bytes=0-1"),
            ("x-amz-copy-source-range", "bytes=2-3"),
        ],
    )
    assert (resp.status_code, _code(resp)) == (400, "InvalidArgument")
    assert metadata.get_parts(upload) == []


# --- the source conditions ---


def test_each_source_condition_failing_alone_is_412(env):
    client, storage, metadata, upload = env
    now = datetime.now(UTC)
    failing = [
        {"x-amz-copy-source-if-match": '"nope"'},
        {"x-amz-copy-source-if-none-match": _md5(BODY)},
        {"x-amz-copy-source-if-modified-since": _http_date(now + timedelta(days=1))},
        {"x-amz-copy-source-if-unmodified-since": _http_date(now - timedelta(days=1))},
    ]
    for headers in failing:
        resp = _copy_part(client, upload, **headers)
        assert (resp.status_code, _code(resp)) == (412, "PreconditionFailed"), headers
    assert metadata.get_parts(upload) == []
    _assert_clean(storage, metadata)


def test_each_source_condition_holding_alone_copies(env):
    client, _, metadata, upload = env
    now = datetime.now(UTC)
    holding = [
        {"x-amz-copy-source-if-match": _md5(BODY)},
        {"x-amz-copy-source-if-none-match": '"nope"'},
        {"x-amz-copy-source-if-modified-since": _http_date(now - timedelta(days=1))},
        {"x-amz-copy-source-if-unmodified-since": _http_date(now + timedelta(days=1))},
    ]
    for n, headers in enumerate(holding, 1):
        assert _copy_part(client, upload, n, **headers).status_code == 200, headers
    assert [p.part_number for p in metadata.get_parts(upload)] == [1, 2, 3, 4]


def test_the_two_combinations_the_reference_documents(env):
    client, _, metadata, upload = env
    past = _http_date(datetime.now(UTC) - timedelta(days=1))

    # If-Match true with If-Unmodified-Since false: 200 and the data is copied.
    ok = _copy_part(
        client,
        upload,
        **{"x-amz-copy-source-if-match": _md5(BODY), "x-amz-copy-source-if-unmodified-since": past},
    )
    assert ok.status_code == 200 and len(metadata.get_parts(upload)) == 1

    # If-None-Match false with If-Modified-Since true: 412.
    refused = _copy_part(
        client,
        upload,
        2,
        **{
            "x-amz-copy-source-if-none-match": _md5(BODY),
            "x-amz-copy-source-if-modified-since": past,
        },
    )
    assert refused.status_code == 412 and len(metadata.get_parts(upload)) == 1


def test_the_requests_own_if_headers_are_not_source_conditions(env):
    client, _, metadata, upload = env
    # UploadPartCopy has no destination conditions either: both are ignored, as on UploadPart.
    resp = _copy_part(client, upload, **{"If-Match": '"nope"', "If-None-Match": "*"})
    assert resp.status_code == 200
    assert len(metadata.get_parts(upload)) == 1


def test_a_failed_condition_beats_a_range_outside_the_source(env):
    client, _, metadata, upload = env
    resp = _copy_part(
        client,
        upload,
        **{"x-amz-copy-source-if-match": '"nope"', "x-amz-copy-source-range": "bytes=0-9999"},
    )
    assert (resp.status_code, _code(resp)) == (412, "PreconditionFailed")
    assert metadata.get_parts(upload) == []


# --- the order of the errors ---


def test_error_order(env):
    client, storage, metadata, upload = env
    bad_src = "no-such-bucket/key"
    bad_range = {"x-amz-copy-source-range": "bytes=nope"}

    # The destination bucket, then the upload.
    resp = _copy_part(client, upload, bucket="no-bucket", src=bad_src)
    assert (resp.status_code, _code(resp)) == (404, "NoSuchBucket")
    resp = _copy_part(client, "no-upload", src=bad_src, **bad_range)
    assert (resp.status_code, _code(resp)) == (404, "NoSuchUpload")

    # The part number: not a number, then the upload, then the interval.
    resp = _copy_part(client, upload, n="x", src=bad_src)
    assert (resp.status_code, _code(resp)) == (400, "InvalidPart")
    resp = _copy_part(client, "no-upload", n=0, src=bad_src)
    assert (resp.status_code, _code(resp)) == (404, "NoSuchUpload")
    for n in (0, 10001, -1):
        resp = _copy_part(client, upload, n=n, src=bad_src, **bad_range)
        assert (resp.status_code, _code(resp)) == (400, "InvalidPart"), n

    # The source value and the range syntax, before the source is looked up.
    resp = _copy_part(client, upload, src="no-slash", **bad_range)
    assert (resp.status_code, _code(resp), ET.fromstring(resp.text).findtext("ArgumentName")) == (
        400,
        "InvalidArgument",
        "x-amz-copy-source",
    )
    resp = _copy_part(client, upload, src=bad_src, **bad_range)
    assert (resp.status_code, _code(resp)) == (400, "InvalidArgument")
    assert ET.fromstring(resp.text).findtext("ArgumentName") == "x-amz-copy-source-range"

    # The source bucket, then its key, before the conditions.
    resp = _copy_part(client, upload, src=bad_src, **{"x-amz-copy-source-if-match": '"x"'})
    assert (resp.status_code, _code(resp)) == (404, "NoSuchBucket")
    resp = _copy_part(
        client, upload, src=f"{SRC_BUCKET}/nope", **{"x-amz-copy-source-if-match": '"x"'}
    )
    assert (resp.status_code, _code(resp)) == (404, "NoSuchKey")

    assert metadata.get_parts(upload) == []
    _assert_clean(storage, metadata)


def test_a_missing_source_key_is_no_such_key_and_registers_nothing(env):
    client, storage, metadata, upload = env
    before = _state(storage, metadata, upload)

    resp = _copy_part(client, upload, src=f"{SRC_BUCKET}/missing")

    assert (resp.status_code, _code(resp)) == (404, "NoSuchKey")
    assert _state(storage, metadata, upload) == before


@pytest.mark.parametrize("how", ["abort", "complete"])
def test_a_closed_upload_is_no_such_upload(env, how):
    client, storage, metadata, upload = env
    if how == "abort":
        assert client.delete(f"/{DST_BUCKET}/{DST}?uploadId={upload}").status_code == 204
    else:
        etag = ET.fromstring(_copy_part(client, upload).text).findtext("ETag")
        assert _complete(client, upload, [(1, etag)]).status_code == 200
    registry = sorted((r.blob_id, r.state) for r in metadata.list_blob_records())

    resp = _copy_part(client, upload, 2)

    assert (resp.status_code, _code(resp)) == (404, "NoSuchUpload")
    assert sorted((r.blob_id, r.state) for r in metadata.list_blob_records()) == registry
    assert storage.temp_entries() == []


# --- replacing a part, and Complete over copied parts ---


def test_copying_over_a_part_number_replaces_it(env):
    client, storage, metadata, upload = env
    first = _copy_part(client, upload, **{"x-amz-copy-source-range": "bytes=0-99"})
    (old,) = metadata.get_parts(upload)
    assert _part_bytes(storage, metadata, upload) == BODY[:100]

    second = _copy_part(client, upload, **{"x-amz-copy-source-range": "bytes=100-299"})

    assert second.status_code == 200
    (new,) = metadata.get_parts(upload)
    assert new.blob_id != old.blob_id
    assert _part_bytes(storage, metadata, upload) == BODY[100:300]
    assert not storage.has_files(old.blob_id)  # its bytes were collected
    _assert_clean(storage, metadata)
    # Complete knows only the new ETag.
    old_etag = ET.fromstring(first.text).findtext("ETag")
    refused = _complete(client, upload, [(1, old_etag)])
    assert (refused.status_code, _code(refused)) == (400, "InvalidPart")
    done = _complete(client, upload, [(1, _md5(BODY[100:300]))])
    assert done.status_code == 200
    assert client.get(f"/{DST_BUCKET}/{DST}").content == BODY[100:300]


def test_a_copy_replaces_an_uploaded_part_and_the_other_way_round(env):
    client, storage, metadata, upload = env
    plain = client.put(f"/{DST_BUCKET}/{DST}?partNumber=1&uploadId={upload}", content=b"plain")
    assert plain.status_code == 200
    assert _copy_part(client, upload, **{"x-amz-copy-source-range": "bytes=0-3"}).status_code == 200
    assert _part_bytes(storage, metadata, upload) == BODY[:4]

    again = client.put(f"/{DST_BUCKET}/{DST}?partNumber=1&uploadId={upload}", content=b"plain")
    assert again.status_code == 200
    assert _part_bytes(storage, metadata, upload) == b"plain"
    _assert_clean(storage, metadata)


def test_complete_over_copied_and_uploaded_parts_gives_their_bytes_and_composite_etag(env):
    client, storage, metadata, upload = env
    pieces = {1: BODY[:300], 2: b"uploaded in between", 3: BODY[700:], 4: BODY}
    etags = {}
    resp = _copy_part(client, upload, 1, **{"x-amz-copy-source-range": "bytes=0-299"})
    etags[1] = ET.fromstring(resp.text).findtext("ETag")
    put = client.put(f"/{DST_BUCKET}/{DST}?partNumber=2&uploadId={upload}", content=pieces[2])
    etags[2] = put.headers["etag"]
    resp = _copy_part(client, upload, 3, **{"x-amz-copy-source-range": "bytes=700-1023"})
    etags[3] = ET.fromstring(resp.text).findtext("ETag")
    etags[4] = ET.fromstring(_copy_part(client, upload, 4).text).findtext("ETag")

    done = _complete(client, upload, sorted(etags.items()))

    assert done.status_code == 200
    data = b"".join(pieces[n] for n in sorted(pieces))
    got = client.get(f"/{DST_BUCKET}/{DST}")
    assert got.content == data
    expected = _composite(*(pieces[n] for n in sorted(pieces)))
    assert got.headers["etag"] == expected
    assert ET.fromstring(done.text).findtext("ETag") == expected
    _assert_clean(storage, metadata)
    assert client.get(f"/{SRC_BUCKET}/{SRC}").content == BODY


def test_copies_are_listed_after_complete(env):
    client, _, _, upload = env
    etag = ET.fromstring(_copy_part(client, upload).text).findtext("ETag")
    assert _complete(client, upload, [(1, etag)]).status_code == 200
    listing = client.get(f"/{DST_BUCKET}?list-type=2")
    assert f"<Key>{DST}</Key>" in listing.text


# --- what stays unimplemented ---

SSE_C_COPY_SOURCE = {
    "x-amz-copy-source-server-side-encryption-customer-algorithm": "AES256",
    "x-amz-copy-source-server-side-encryption-customer-key": "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
    "x-amz-copy-source-server-side-encryption-customer-key-MD5": "JK1b8Xz0Ihb1Zbs2d5+4pw==",
}


def _assert_not_implemented(resp, header: str | None = None) -> None:
    assert resp.status_code == 501
    root = ET.fromstring(resp.text)
    assert root.findtext("Code") == "NotImplemented"
    if header:
        assert root.findtext("Header") == header.lower()


@pytest.mark.parametrize("name", sorted(SSE_C_COPY_SOURCE))
def test_copy_source_sse_c_headers_are_refused_on_upload_part_copy(env, name):
    client, storage, metadata, upload = env
    before = _state(storage, metadata, upload)

    resp = _copy_part(client, upload, **{name: SSE_C_COPY_SOURCE[name]})

    _assert_not_implemented(resp, name)
    assert _state(storage, metadata, upload) == before
    assert _copy_part(client, upload).status_code == 200  # without them it works


@pytest.mark.parametrize("name", sorted(SSE_C_COPY_SOURCE))
def test_copy_source_sse_c_headers_are_refused_on_copy_object(env, name):
    client, storage, metadata, _ = env
    registry = sorted((r.blob_id, r.state) for r in metadata.list_blob_records())

    resp = client.put(
        f"/{DST_BUCKET}/new.bin",
        headers={"x-amz-copy-source": f"{SRC_BUCKET}/{SRC}", name: SSE_C_COPY_SOURCE[name]},
    )

    _assert_not_implemented(resp, name)
    assert client.get(f"/{DST_BUCKET}/new.bin").status_code == 404
    assert sorted((r.blob_id, r.state) for r in metadata.list_blob_records()) == registry


def test_all_three_copy_source_sse_c_headers_together_are_refused_on_both_paths(env):
    client, _, metadata, upload = env
    part = _copy_part(client, upload, **SSE_C_COPY_SOURCE)
    copy = client.put(
        f"/{DST_BUCKET}/new.bin",
        headers={"x-amz-copy-source": f"{SRC_BUCKET}/{SRC}", **SSE_C_COPY_SOURCE},
    )
    _assert_not_implemented(part)
    _assert_not_implemented(copy)
    assert metadata.get_parts(upload) == []


def test_a_copy_source_on_a_query_that_is_not_upload_part_stays_501(env):
    client, _, metadata, upload = env
    src = {"x-amz-copy-source": f"{SRC_BUCKET}/{SRC}"}
    for query in (
        f"?uploadId={upload}",
        "?partNumber=1",
        f"?partNumber=1&uploadId={upload}&acl",
        f"?partNumber=1&uploadId={upload}&versionId=null",
    ):
        _assert_not_implemented(client.put(f"/{DST_BUCKET}/{DST}{query}", headers=src))
    assert metadata.get_parts(upload) == []
