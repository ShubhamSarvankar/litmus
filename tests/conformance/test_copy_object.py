"""CopyObject: PUT with x-amz-copy-source and no query (docs/design/storage-layout.md §6.10,
§11).

The bytes are copied into a new blob, so the copy and its source share nothing. Every
refusal leaves the store as it was: the object rows, the blob registry, the blob files
and tmp/ (checked with the consistency sweep).
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
DST = "dst.bin"
BODY = b"copy me. " * 20
OLD = b"previous destination. " * 9
HTTP_DATE = "%a, %d %b %Y %H:%M:%S GMT"


def _etag(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


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
    return client, storage, metadata


def _copy(client, dst_key=DST, src=f"{SRC_BUCKET}/{SRC}", bucket=DST_BUCKET, **headers):
    return client.put(f"/{bucket}/{dst_key}", headers={"x-amz-copy-source": src, **headers})


def _code(resp) -> str:
    return ET.fromstring(resp.text).findtext("Code")


def _state(storage, metadata) -> tuple:
    """Everything a refused request must leave alone."""
    rows = {
        (b, k): metadata.get_object_meta(b, k) for b in (SRC_BUCKET, DST_BUCKET) for k in (SRC, DST)
    }
    registry = sorted((r.blob_id, r.state) for r in metadata.list_blob_records())
    return rows, registry, storage.temp_entries()


def _assert_clean(storage, metadata) -> None:
    assert run_consistency_sweep(metadata, storage).issues == []
    assert storage.temp_entries() == []
    assert {r.state for r in metadata.list_blob_records()} <= {"live"}


def _path(key: str) -> str:
    """The request path that stores key. Starlette's TestClient decodes the path a second
    time (tests/conformance/test_percent_keys.py), so the key is quoted twice here. The
    x-amz-copy-source header is not decoded by the client and is quoted once."""
    return quote(quote(key, safe="/"), safe="/")


def _http_date(dt: datetime) -> str:
    return dt.strftime(HTTP_DATE)


# --- the copy itself ---


@pytest.mark.parametrize("size", [0, 1, 65535, 65536, 65537, 3 * 65536 + 5])
def test_copy_has_the_sources_bytes_and_etag(env, size):
    client, storage, metadata = env
    data = os.urandom(size)
    client.put(f"/{SRC_BUCKET}/sized", content=data)

    resp = _copy(client, src=f"{SRC_BUCKET}/sized")

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/xml")
    result = ET.fromstring(resp.text)
    assert result.tag == "CopyObjectResult"
    assert result.findtext("ETag") == _etag(data)
    got = client.get(f"/{DST_BUCKET}/{DST}")
    assert got.content == data and got.headers["etag"] == _etag(data)
    assert client.get(f"/{SRC_BUCKET}/sized").content == data
    _assert_clean(storage, metadata)


def test_copy_result_last_modified_is_the_commit_time(env):
    client, _, _ = env
    before = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    resp = _copy(client)
    stamped = datetime.strptime(
        ET.fromstring(resp.text).findtext("LastModified"), "%Y-%m-%dT%H:%M:%S.000Z"
    )
    head = client.head(f"/{DST_BUCKET}/{DST}").headers["last-modified"]
    assert datetime.strptime(head, HTTP_DATE) == stamped
    assert stamped >= before


def test_copy_within_one_bucket(env):
    client, storage, metadata = env
    assert _copy(client, bucket=SRC_BUCKET).status_code == 200
    assert client.get(f"/{SRC_BUCKET}/{DST}").content == BODY
    _assert_clean(storage, metadata)


def test_copy_shares_no_blob_with_its_source(env):
    client, storage, metadata = env
    assert _copy(client).status_code == 200
    src = metadata.get_object_meta(SRC_BUCKET, SRC)
    dst = metadata.get_object_meta(DST_BUCKET, DST)
    assert src.blob_id != dst.blob_id
    assert client.delete(f"/{SRC_BUCKET}/{SRC}").status_code == 204
    assert client.get(f"/{DST_BUCKET}/{DST}").content == BODY
    client.put(f"/{DST_BUCKET}/{DST}", content=b"overwritten")
    assert client.put(f"/{SRC_BUCKET}/{SRC}", content=BODY).status_code == 200
    assert _copy(client, dst_key="again").status_code == 200
    assert client.get(f"/{DST_BUCKET}/again").content == BODY
    _assert_clean(storage, metadata)


def test_copy_over_an_existing_key_replaces_it_and_frees_its_blob(env):
    client, storage, metadata = env
    client.put(f"/{DST_BUCKET}/{DST}", content=OLD)
    old_blob = metadata.get_object_meta(DST_BUCKET, DST).blob_id

    assert _copy(client).status_code == 200

    assert client.get(f"/{DST_BUCKET}/{DST}").content == BODY
    assert not storage.has_files(old_blob)
    assert metadata.blob_state(old_blob) is None
    _assert_clean(storage, metadata)


def test_leading_slash_of_the_source_is_optional(env):
    client, _, _ = env
    assert _copy(client, src=f"/{SRC_BUCKET}/{SRC}").status_code == 200
    assert _copy(client, dst_key="two", src=f"{SRC_BUCKET}/{SRC}").status_code == 200


def test_version_id_null_names_the_current_object(env):
    client, _, _ = env
    assert _copy(client, src=f"{SRC_BUCKET}/{SRC}?versionId=null").status_code == 200
    assert client.get(f"/{DST_BUCKET}/{DST}").content == BODY


def test_copy_of_a_multipart_object_has_a_plain_md5_etag(env):
    client, storage, metadata = env
    part1, part2 = os.urandom(5 * 1024 * 1024), os.urandom(1000)
    upload = ET.fromstring(client.post(f"/{SRC_BUCKET}/mp?uploads").text).findtext("UploadId")
    etags = [
        client.put(f"/{SRC_BUCKET}/mp?partNumber={n}&uploadId={upload}", content=p).headers["etag"]
        for n, p in ((1, part1), (2, part2))
    ]
    body = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>" for n, e in enumerate(etags, 1)
    )
    done = client.post(
        f"/{SRC_BUCKET}/mp?uploadId={upload}",
        content=f"<CompleteMultipartUpload>{body}</CompleteMultipartUpload>",
    )
    assert done.status_code == 200
    source_etag = client.head(f"/{SRC_BUCKET}/mp").headers["etag"]
    assert "-" in source_etag

    resp = _copy(client, src=f"{SRC_BUCKET}/mp")

    assert resp.status_code == 200
    data = part1 + part2
    assert ET.fromstring(resp.text).findtext("ETag") == _etag(data)
    got = client.get(f"/{DST_BUCKET}/{DST}")
    assert got.content == data and got.headers["etag"] == _etag(data) != source_etag
    assert metadata.get_object_meta(DST_BUCKET, DST).upload_id is None
    _assert_clean(storage, metadata)


# --- the metadata directive ---

SOURCE_HEADERS = {
    "Content-Type": "text/source",
    "Cache-Control": "max-age=1",
    "Content-Disposition": "attachment; filename=source",
    "Content-Encoding": "identity",
    "Content-Language": "en",
    "Expires": "Thu, 01 Dec 2094 16:00:00 GMT",
    "x-amz-meta-from": "source",
    "x-amz-meta-shared": "source",
}
REQUEST_HEADERS = {
    "Content-Type": "text/request",
    "Cache-Control": "no-cache",
    "Content-Disposition": "inline",
    "Content-Encoding": "x-request",
    "Content-Language": "de",
    "Expires": "Fri, 02 Dec 2095 16:00:00 GMT",
    "x-amz-meta-from": "request",
    "x-amz-meta-extra": "request",
}


def _stored(client, bucket, key) -> dict:
    headers = client.head(f"/{bucket}/{key}").headers
    return {k: headers.get(k) for k in [*SOURCE_HEADERS, *REQUEST_HEADERS]}


@pytest.mark.parametrize("directive", [None, "COPY"])
def test_copy_directive_takes_every_header_from_the_source(env, directive):
    client, _, _ = env
    client.put(f"/{SRC_BUCKET}/headed", content=BODY, headers=SOURCE_HEADERS)
    extra = {"x-amz-metadata-directive": directive} if directive else {}

    resp = _copy(client, src=f"{SRC_BUCKET}/headed", **REQUEST_HEADERS, **extra)

    assert resp.status_code == 200
    assert _stored(client, DST_BUCKET, DST) == _stored(client, SRC_BUCKET, "headed")
    head = client.head(f"/{DST_BUCKET}/{DST}").headers
    assert head["content-type"] == "text/source"
    assert head["x-amz-meta-from"] == "source" and "x-amz-meta-extra" not in head


def test_replace_directive_takes_every_header_from_the_request(env):
    client, _, _ = env
    client.put(f"/{SRC_BUCKET}/headed", content=BODY, headers=SOURCE_HEADERS)

    resp = _copy(
        client,
        src=f"{SRC_BUCKET}/headed",
        **{"x-amz-metadata-directive": "REPLACE"},
        **REQUEST_HEADERS,
    )

    assert resp.status_code == 200
    head = client.head(f"/{DST_BUCKET}/{DST}").headers
    assert head["content-type"] == "text/request"
    assert head["cache-control"] == "no-cache" and head["content-language"] == "de"
    assert head["content-disposition"] == "inline" and head["content-encoding"] == "x-request"
    assert head["x-amz-meta-from"] == "request" and head["x-amz-meta-extra"] == "request"
    assert "x-amz-meta-shared" not in head
    assert client.get(f"/{DST_BUCKET}/{DST}").content == BODY


def test_replace_without_headers_drops_the_sources_and_defaults_like_put_object(env):
    client, _, _ = env
    client.put(f"/{SRC_BUCKET}/headed", content=BODY, headers=SOURCE_HEADERS)
    client.put(f"/{DST_BUCKET}/plain", content=BODY)

    resp = _copy(client, src=f"{SRC_BUCKET}/headed", **{"x-amz-metadata-directive": "REPLACE"})

    assert resp.status_code == 200
    head = client.head(f"/{DST_BUCKET}/{DST}").headers
    assert head["content-type"] == client.head(f"/{DST_BUCKET}/plain").headers["content-type"]
    assert head["content-type"] == "binary/octet-stream"
    for name in ("cache-control", "content-disposition", "content-encoding", "content-language"):
        assert name not in head
    assert "expires" not in head and not any(h.startswith("x-amz-meta-") for h in head)


@pytest.mark.parametrize("name", ["x-amz-metadata-directive", "x-amz-tagging-directive"])
@pytest.mark.parametrize("value", ["replace", "copy", "MERGE", "", " REPLACE"])
def test_an_invalid_directive_is_invalid_argument_and_changes_nothing(env, name, value):
    client, storage, metadata = env
    before = _state(storage, metadata)
    resp = _copy(client, **{name: value})
    assert resp.status_code == 400 and _code(resp) == "InvalidArgument"
    assert ET.fromstring(resp.text).findtext("ArgumentName") == name
    assert _state(storage, metadata) == before


def test_tagging_directive_copy_and_empty_replace_proceed(env):
    client, _, _ = env
    assert _copy(client, **{"x-amz-tagging-directive": "COPY"}).status_code == 200
    assert (
        _copy(client, **{"x-amz-tagging-directive": "COPY", "x-amz-tagging": "a=b"}).status_code
        == 200
    )
    resp = _copy(client, **{"x-amz-tagging-directive": "REPLACE", "x-amz-tagging": ""})
    assert resp.status_code == 200
    assert _copy(client, **{"x-amz-tagging-directive": "REPLACE"}).status_code == 200


# --- copying onto itself ---


@pytest.mark.parametrize("extra", [{}, {"x-amz-metadata-directive": "COPY"}])
@pytest.mark.parametrize("suffix", ["", "?versionId=null"])
def test_copy_onto_itself_without_replace_is_invalid_request(env, extra, suffix):
    client, storage, metadata = env
    before = _state(storage, metadata)
    resp = _copy(client, dst_key=SRC, bucket=SRC_BUCKET, src=f"{SRC_BUCKET}/{SRC}{suffix}", **extra)
    assert resp.status_code == 400 and _code(resp) == "InvalidRequest"
    assert _state(storage, metadata) == before


def test_copy_onto_itself_with_replace_changes_the_headers_and_keeps_the_bytes(env):
    client, storage, metadata = env
    client.put(f"/{SRC_BUCKET}/headed", content=BODY, headers=SOURCE_HEADERS)
    old_blob = metadata.get_object_meta(SRC_BUCKET, "headed").blob_id

    resp = _copy(
        client,
        dst_key="headed",
        bucket=SRC_BUCKET,
        src=f"{SRC_BUCKET}/headed",
        **{"x-amz-metadata-directive": "REPLACE", "x-amz-meta-foo": "bar"},
    )

    assert resp.status_code == 200
    got = client.get(f"/{SRC_BUCKET}/headed")
    assert got.content == BODY and got.headers["etag"] == _etag(BODY)
    assert got.headers["x-amz-meta-foo"] == "bar" and "x-amz-meta-from" not in got.headers
    assert metadata.get_object_meta(SRC_BUCKET, "headed").blob_id != old_blob
    assert not storage.has_files(old_blob)
    _assert_clean(storage, metadata)


def test_copy_onto_itself_is_refused_before_the_source_conditions(env):
    client, storage, metadata = env
    before = _state(storage, metadata)
    resp = _copy(
        client,
        dst_key=SRC,
        bucket=SRC_BUCKET,
        src=f"{SRC_BUCKET}/{SRC}",
        **{"x-amz-copy-source-if-match": '"0123456789abcdef0123456789abcdef"'},
    )
    assert resp.status_code == 400 and _code(resp) == "InvalidRequest"
    assert _state(storage, metadata) == before


# --- source names ---

KEYS = ["a b", "ünï/cødé", "100%", "what?", "plus+sign", "dir/sub/key", "x%25y", "~tilde", "a&b=c"]


@pytest.mark.parametrize("key", KEYS)
def test_source_keys_are_percent_decoded_once(env, key):
    client, _, _ = env
    assert client.put(f"/{SRC_BUCKET}/{_path(key)}", content=key.encode()).status_code == 200
    # What botocore sends: the bucket/key quoted with "/" and "~" left alone.
    resp = _copy(client, src=f"{SRC_BUCKET}/{quote(key, safe='/~')}")
    assert resp.status_code == 200
    assert client.get(f"/{DST_BUCKET}/{DST}").content == key.encode()


def test_a_percent_escape_in_the_source_is_not_decoded_twice(env):
    client, _, _ = env
    client.put(f"/{SRC_BUCKET}/{_path('anyfilename%25.txt')}", content=b"literal")
    resp = _copy(client, src=f"{SRC_BUCKET}/{quote('anyfilename%.txt', safe='/~')}")
    assert resp.status_code == 404 and _code(resp) == "NoSuchKey"


def test_a_plus_in_the_source_is_a_plus_not_a_space(env):
    client, _, _ = env
    client.put(f"/{SRC_BUCKET}/{_path('a b')}", content=b"space")
    resp = _copy(client, src=f"{SRC_BUCKET}/a+b")
    assert resp.status_code == 404 and _code(resp) == "NoSuchKey"


def test_a_question_mark_in_a_key_must_be_escaped_to_not_start_a_version(env):
    client, _, _ = env
    client.put(f"/{SRC_BUCKET}/{_path('?versionId')}", content=b"odd")
    assert _copy(client, src=f"{SRC_BUCKET}/%3FversionId").status_code == 200
    assert client.get(f"/{DST_BUCKET}/{DST}").content == b"odd"


# --- errors ---


def test_missing_destination_bucket(env):
    client, _, _ = env
    resp = _copy(client, bucket="no-such-dst")
    assert resp.status_code == 404 and _code(resp) == "NoSuchBucket"
    assert ET.fromstring(resp.text).findtext("BucketName") == "no-such-dst"


def test_missing_source_bucket(env):
    client, storage, metadata = env
    before = _state(storage, metadata)
    resp = _copy(client, src=f"no-such-src/{SRC}")
    assert resp.status_code == 404 and _code(resp) == "NoSuchBucket"
    assert ET.fromstring(resp.text).findtext("BucketName") == "no-such-src"
    assert _state(storage, metadata) == before


def test_missing_source_key(env):
    client, storage, metadata = env
    before = _state(storage, metadata)
    resp = _copy(client, src=f"{SRC_BUCKET}/no-such-key")
    assert resp.status_code == 404 and _code(resp) == "NoSuchKey"
    assert _state(storage, metadata) == before


@pytest.mark.parametrize(
    "source",
    [
        "",
        "/",
        "bucket-only",
        f"/{SRC_BUCKET}",
        f"{SRC_BUCKET}/",
        f"/{SRC_BUCKET}/",
        f"//{SRC}",
        f"{SRC_BUCKET}/{SRC}?versionId",
        f"{SRC_BUCKET}/{SRC}?other=1",
        f"{SRC_BUCKET}/{SRC}?versionId=null&x=1",
        f"{SRC_BUCKET}/%ff%fe",
    ],
)
def test_a_malformed_source_is_invalid_argument(env, source):
    client, storage, metadata = env
    before = _state(storage, metadata)
    resp = _copy(client, src=source)
    assert resp.status_code == 400 and _code(resp) == "InvalidArgument"
    assert ET.fromstring(resp.text).findtext("ArgumentName") == "x-amz-copy-source"
    assert _state(storage, metadata) == before


def test_repeated_copy_source_headers_are_invalid_argument(env):
    client, _, _ = env
    resp = client.put(
        f"/{DST_BUCKET}/{DST}",
        headers=[("x-amz-copy-source", f"{SRC_BUCKET}/{SRC}"), ("x-amz-copy-source", "a/b")],
    )
    assert resp.status_code == 400 and _code(resp) == "InvalidArgument"


def test_error_order_destination_bucket_then_source_then_directives(env):
    client, _, _ = env
    # destination bucket before a malformed source
    assert _code(_copy(client, bucket="no-such-dst", src="x")) == "NoSuchBucket"
    # a malformed source before a bad directive and before a missing source bucket
    assert _code(_copy(client, src="x", **{"x-amz-metadata-directive": "no"})) == "InvalidArgument"
    # a bad directive before a missing source bucket
    resp = _copy(client, src=f"nobucket/{SRC}", **{"x-amz-metadata-directive": "no"})
    assert _code(resp) == "InvalidArgument"
    # the source bucket before the source key, the source key before the conditions
    assert _code(_copy(client, src=f"nobucket/{SRC}")) == "NoSuchBucket"
    resp = _copy(client, src=f"{SRC_BUCKET}/nokey", **{"x-amz-copy-source-if-match": '"x"'})
    assert _code(resp) == "NoSuchKey"


# --- source conditions ---


def _times() -> tuple[str, str]:
    now = datetime.now(UTC).replace(tzinfo=None)
    return _http_date(now - timedelta(days=1)), _http_date(now + timedelta(days=1))


def _conditions():
    past, future = _times()
    etag, other = _etag(BODY), '"0123456789abcdef0123456789abcdef"'
    return [
        pytest.param({"if-match": etag}, 200, id="if-match-true"),
        pytest.param({"if-match": etag.strip('"')}, 200, id="if-match-true-unquoted"),
        pytest.param({"if-match": "*"}, 200, id="if-match-star"),
        pytest.param({"if-match": f"{other}, {etag}"}, 200, id="if-match-list-true"),
        pytest.param({"if-match": other}, 412, id="if-match-false"),
        pytest.param({"if-match": "ABCORZ"}, 412, id="if-match-false-bare"),
        pytest.param({"if-match": f"W/{etag}"}, 412, id="if-match-weak"),
        pytest.param({"if-none-match": other}, 200, id="if-none-match-true"),
        pytest.param({"if-none-match": "ABCORZ"}, 200, id="if-none-match-true-bare"),
        pytest.param({"if-none-match": etag}, 412, id="if-none-match-false"),
        pytest.param({"if-none-match": "*"}, 412, id="if-none-match-star"),
        pytest.param({"if-none-match": f"W/{etag}"}, 412, id="if-none-match-weak"),
        pytest.param({"if-modified-since": past}, 200, id="if-modified-since-true"),
        pytest.param({"if-modified-since": future}, 412, id="if-modified-since-false"),
        pytest.param({"if-unmodified-since": future}, 200, id="if-unmodified-since-true"),
        pytest.param({"if-unmodified-since": past}, 412, id="if-unmodified-since-false"),
        pytest.param({"if-modified-since": "not a date"}, 200, id="if-modified-since-invalid"),
        pytest.param({"if-unmodified-since": "not a date"}, 200, id="if-unmodified-since-invalid"),
        # The two combinations AWS documents.
        pytest.param(
            {"if-match": etag, "if-unmodified-since": past}, 200, id="match-true-unmodified-false"
        ),
        pytest.param(
            {"if-none-match": etag, "if-modified-since": past},
            412,
            id="none-match-false-modified-true",
        ),
        # Others that follow from RFC 9110 13.2.2.
        pytest.param(
            {"if-match": other, "if-unmodified-since": future},
            412,
            id="match-false-unmodified-true",
        ),
        pytest.param(
            {"if-none-match": other, "if-modified-since": future},
            200,
            id="none-match-true-modified-false",
        ),
        pytest.param(
            {"if-match": etag, "if-none-match": other}, 200, id="match-true-none-match-true"
        ),
        pytest.param(
            {"if-match": etag, "if-none-match": etag}, 412, id="match-true-none-match-false"
        ),
    ]


@pytest.mark.parametrize(("conditions", "status"), _conditions())
def test_copy_source_conditions(env, conditions, status):
    client, storage, metadata = env
    headers = {f"x-amz-copy-source-{name}": value for name, value in conditions.items()}
    before = _state(storage, metadata)

    resp = _copy(client, **headers)

    assert resp.status_code == status
    if status == 200:
        assert client.get(f"/{DST_BUCKET}/{DST}").content == BODY
    else:
        assert _code(resp) == "PreconditionFailed"
        assert _state(storage, metadata) == before
    _assert_clean(storage, metadata)


def test_a_failed_source_condition_leaves_an_existing_destination_alone(env):
    client, storage, metadata = env
    client.put(f"/{DST_BUCKET}/{DST}", content=OLD)
    before = _state(storage, metadata)
    resp = _copy(client, **{"x-amz-copy-source-if-match": '"0123456789abcdef0123456789abcdef"'})
    assert resp.status_code == 412
    assert _state(storage, metadata) == before
    assert client.get(f"/{DST_BUCKET}/{DST}").content == OLD


def test_the_requests_own_if_headers_are_not_source_conditions(env):
    client, _, _ = env
    # Conditions on the destination, which does not exist: If-Match is 404, and the
    # source's ETag does not make it hold.
    resp = _copy(client, **{"If-Match": _etag(BODY)})
    assert resp.status_code == 404 and _code(resp) == "NoSuchKey"
    # If-None-Match names an ETag that is the source's: only the destination counts.
    assert _copy(client, **{"If-None-Match": _etag(BODY)}).status_code == 200
    # A copy-source condition does not stand in for a destination condition.
    resp = _copy(client, **{"x-amz-copy-source-if-match": _etag(BODY), "If-None-Match": "*"})
    assert resp.status_code == 412


# --- destination conditions ---


def test_destination_if_none_match_star(env):
    client, storage, metadata = env
    assert _copy(client, **{"If-None-Match": "*"}).status_code == 200
    before = _state(storage, metadata)
    resp = _copy(client, src=f"{SRC_BUCKET}/{SRC}", **{"If-None-Match": "*"})
    assert resp.status_code == 412 and _code(resp) == "PreconditionFailed"
    assert _state(storage, metadata) == before
    _assert_clean(storage, metadata)


def test_destination_if_match(env):
    client, storage, metadata = env
    client.put(f"/{DST_BUCKET}/{DST}", content=OLD)
    before = _state(storage, metadata)

    resp = _copy(client, **{"If-Match": '"0123456789abcdef0123456789abcdef"'})
    assert resp.status_code == 412 and _code(resp) == "PreconditionFailed"
    assert _state(storage, metadata) == before
    assert client.get(f"/{DST_BUCKET}/{DST}").content == OLD

    assert _copy(client, **{"If-Match": _etag(OLD)}).status_code == 200
    assert client.get(f"/{DST_BUCKET}/{DST}").content == BODY
    _assert_clean(storage, metadata)


def test_destination_if_match_on_a_missing_key_is_no_such_key(env):
    client, storage, metadata = env
    before = _state(storage, metadata)
    resp = _copy(client, **{"If-Match": "*"})
    assert resp.status_code == 404 and _code(resp) == "NoSuchKey"
    assert _state(storage, metadata) == before
    _assert_clean(storage, metadata)
