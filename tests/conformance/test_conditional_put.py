"""Conditional PutObject: If-Match and If-None-Match, decided in the commit transaction
against the row it replaces (docs/design/storage-layout.md §10).

Every case runs as a plain and as an aws-chunked request. A refusal changes nothing the
store holds: the object row (Last-Modified included), the blob registry, the blob
files and tmp/ are exactly as before, and the consistency sweep finds nothing.
"""

import hashlib
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from chaos.framework.framed import framed_request
from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "cond-put"
KEY = "obj.bin"
OLD = b"old body. " * 100
NEW = b"new body! " * 150
OLD_ETAG = '"' + hashlib.md5(OLD).hexdigest() + '"'
NEW_ETAG = '"' + hashlib.md5(NEW).hexdigest() + '"'
OLD_BARE = OLD_ETAG.strip('"')
OTHER = '"0123456789abcdef0123456789abcdef"'


@pytest.fixture(params=[pytest.param(False, id="plain"), pytest.param(True, id="framed")])
def framed(request) -> bool:
    return request.param


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    assert client.put(f"/{BUCKET}").status_code == 200
    return client, storage, metadata


def _put(client, key: str, data: bytes, headers: dict, framed: bool):
    if framed:
        framed_hdrs, body = framed_request(data)
        return client.put(f"/{BUCKET}/{key}", content=body, headers={**framed_hdrs, **headers})
    return client.put(f"/{BUCKET}/{key}", content=data, headers=headers)


def _code(resp) -> str:
    return ET.fromstring(resp.text).findtext("Code")


def _state(storage, metadata) -> tuple:
    """Everything the store holds that a refused PUT must leave alone."""
    objects = metadata.list_objects(BUCKET)
    return objects, metadata.list_blob_records(), storage.temp_entries(), sorted(storage.scan())


def assert_store_clean(storage, metadata, live: int) -> None:
    """Only live blobs (one per object), an empty tmp/ and a sweep with nothing to report."""
    records = metadata.list_blob_records()
    assert [r.state for r in records] == ["live"] * live
    assert storage.temp_entries() == []
    assert run_consistency_sweep(metadata, storage).issues == []


# (id, headers, key exists, status, code); the existing object is OLD, the new body NEW.
CASES = [
    # If-None-Match
    ("inm-star-missing", {"If-None-Match": "*"}, False, 200, None),
    ("inm-star-existing", {"If-None-Match": "*"}, True, 412, "PreconditionFailed"),
    ("inm-etag-missing", {"If-None-Match": OLD_ETAG}, False, 200, None),
    ("inm-etag-match-quoted", {"If-None-Match": OLD_ETAG}, True, 412, "PreconditionFailed"),
    ("inm-etag-match-unquoted", {"If-None-Match": OLD_BARE}, True, 412, "PreconditionFailed"),
    ("inm-etag-match-weak", {"If-None-Match": f"W/{OLD_ETAG}"}, True, 412, "PreconditionFailed"),
    ("inm-etag-mismatch-quoted", {"If-None-Match": OTHER}, True, 200, None),
    ("inm-etag-mismatch-unquoted", {"If-None-Match": OTHER.strip('"')}, True, 200, None),
    (
        "inm-list-with-match",
        {"If-None-Match": f"{OTHER}, {OLD_ETAG}"},
        True,
        412,
        "PreconditionFailed",
    ),
    ("inm-list-no-match", {"If-None-Match": f'{OTHER}, "ffff"'}, True, 200, None),
    # If-Match
    ("im-star-existing", {"If-Match": "*"}, True, 200, None),
    ("im-star-missing", {"If-Match": "*"}, False, 404, "NoSuchKey"),
    ("im-etag-match-quoted", {"If-Match": OLD_ETAG}, True, 200, None),
    ("im-etag-match-unquoted", {"If-Match": OLD_BARE}, True, 200, None),
    ("im-etag-mismatch-quoted", {"If-Match": OTHER}, True, 412, "PreconditionFailed"),
    ("im-etag-mismatch-unquoted", {"If-Match": OTHER.strip('"')}, True, 412, "PreconditionFailed"),
    ("im-etag-missing-quoted", {"If-Match": OLD_ETAG}, False, 404, "NoSuchKey"),
    ("im-etag-missing-unquoted", {"If-Match": OLD_BARE}, False, 404, "NoSuchKey"),
    ("im-weak-is-no-strong-match", {"If-Match": f"W/{OLD_ETAG}"}, True, 412, "PreconditionFailed"),
    ("im-list-with-match", {"If-Match": f"{OTHER}, {OLD_ETAG}"}, True, 200, None),
    ("im-blank-is-no-condition", {"If-Match": ""}, True, 200, None),
    ("inm-blank-is-no-condition", {"If-None-Match": ""}, True, 200, None),
    # Both: If-Match first, then If-None-Match; both must hold.
    ("both-hold", {"If-Match": OLD_ETAG, "If-None-Match": OTHER}, True, 200, None),
    (
        "both-star-on-existing",
        {"If-Match": "*", "If-None-Match": "*"},
        True,
        412,
        "PreconditionFailed",
    ),
    (
        "both-if-none-match-fails",
        {"If-Match": OLD_ETAG, "If-None-Match": OLD_ETAG},
        True,
        412,
        "PreconditionFailed",
    ),
    (
        "both-if-match-fails",
        {"If-Match": OTHER, "If-None-Match": OTHER},
        True,
        412,
        "PreconditionFailed",
    ),
    (
        "both-missing-if-match-first",
        {"If-Match": "*", "If-None-Match": "*"},
        False,
        404,
        "NoSuchKey",
    ),
]


@pytest.mark.parametrize(
    "headers,exists,status,code", [c[1:] for c in CASES], ids=[c[0] for c in CASES]
)
def test_conditional_put(env, framed, headers, exists, status, code):
    client, storage, metadata = env
    if exists:
        assert _put(client, KEY, OLD, {}, framed).status_code == 200
    before = _state(storage, metadata)
    row_before = metadata.get_object_meta(BUCKET, KEY)

    started = datetime.now(UTC).replace(tzinfo=None)
    resp = _put(client, KEY, NEW, headers, framed)
    finished = datetime.now(UTC).replace(tzinfo=None)

    assert resp.status_code == status
    if code is not None:
        assert _code(resp) == code
        # Nothing changed: the same row, registry, files and tmp/.
        assert _state(storage, metadata) == before
        got = client.get(f"/{BUCKET}/{KEY}")
        if exists:
            assert got.status_code == 200 and got.content == OLD
            assert got.headers["etag"] == OLD_ETAG
            assert metadata.get_object_meta(BUCKET, KEY) == row_before
        else:
            assert got.status_code == 404
        assert_store_clean(storage, metadata, live=1 if exists else 0)
    else:
        assert resp.headers["etag"] == NEW_ETAG
        got = client.get(f"/{BUCKET}/{KEY}")
        assert got.content == NEW and got.headers["etag"] == NEW_ETAG
        # Last-Modified is the time of the commit, inside the request.
        assert started <= metadata.get_object_meta(BUCKET, KEY).last_modified <= finished
        assert_store_clean(storage, metadata, live=1)


def test_refusal_keeps_headers_and_metadata_of_the_old_object(env, framed):
    client, storage, metadata = env
    keep = {"Content-Type": "text/plain", "Cache-Control": "max-age=5", "x-amz-meta-a": "1"}
    assert _put(client, KEY, OLD, keep, framed).status_code == 200
    row = metadata.get_object_meta(BUCKET, KEY)
    resp = _put(client, KEY, NEW, {"If-Match": OTHER, "x-amz-meta-b": "2"}, framed)
    assert resp.status_code == 412
    assert metadata.get_object_meta(BUCKET, KEY) == row
    head = client.head(f"/{BUCKET}/{KEY}")
    assert head.headers["x-amz-meta-a"] == "1" and "x-amz-meta-b" not in head.headers


def test_a_conditional_put_that_holds_replaces_the_old_blob(env, framed):
    client, storage, metadata = env
    assert _put(client, KEY, OLD, {}, framed).status_code == 200
    old_blob = metadata.get_object_meta(BUCKET, KEY).blob_id
    assert _put(client, KEY, NEW, {"If-Match": OLD_ETAG}, framed).status_code == 200
    assert metadata.blob_state(old_blob) is None
    assert not storage.blob_path(old_blob).exists()
    assert_store_clean(storage, metadata, live=1)


def test_the_same_if_match_retried_after_success_is_412(env, framed):
    client, storage, metadata = env
    assert _put(client, KEY, OLD, {}, framed).status_code == 200
    assert _put(client, KEY, NEW, {"If-Match": OLD_ETAG}, framed).status_code == 200
    again = _put(client, KEY, NEW, {"If-Match": OLD_ETAG}, framed)
    assert again.status_code == 412 and _code(again) == "PreconditionFailed"
    assert client.get(f"/{BUCKET}/{KEY}").headers["etag"] == NEW_ETAG
    assert_store_clean(storage, metadata, live=1)


def test_a_bad_digest_is_reported_before_the_conditions(env):
    # Without an early check the body is read and verified before the commit, so a body
    # that fails its checksum is BadDigest whatever the conditions say.
    client, storage, metadata = env
    assert client.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200
    resp = client.put(
        f"/{BUCKET}/{KEY}",
        content=NEW,
        headers={"If-Match": OTHER, "Content-MD5": "AAAAAAAAAAAAAAAAAAAAAA=="},
    )
    assert resp.status_code == 400 and _code(resp) == "BadDigest"
    assert_store_clean(storage, metadata, live=1)


def test_write_offset_header_is_still_refused_with_a_condition(env):
    client, storage, metadata = env
    resp = client.put(
        f"/{BUCKET}/{KEY}", content=NEW, headers={"x-amz-write-offset-bytes": "0", "If-Match": "*"}
    )
    assert resp.status_code == 501 and _code(resp) == "NotImplemented"
    assert_store_clean(storage, metadata, live=0)


def test_conditions_on_a_missing_bucket_are_no_such_bucket(env):
    client, _, _ = env
    resp = client.put("/nope/k", content=b"x", headers={"If-None-Match": "*"})
    assert resp.status_code == 404 and _code(resp) == "NoSuchBucket"
