"""Conditional CompleteMultipartUpload: If-Match and If-None-Match, decided in the
commit transaction against the object row it replaces (docs/design/storage-layout.md §10).

The cases are those of test_conditional_put.py, for Complete. A refusal changes nothing
the store holds: the object row (Last-Modified included), the blob registry, the blob
files and tmp/ are as before, the upload is still open with all its parts, the sweep
finds nothing, and a retry whose condition holds succeeds. Every other Complete error
comes before 412 and 404.
"""

import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "cond-complete"
KEY = "obj.bin"
OLD = b"old body. " * 100
PARTS = [b"part one. " * 50, b"part two! " * 60]
OLD_ETAG = '"' + hashlib.md5(OLD).hexdigest() + '"'
NEW_ETAG = '"' + hashlib.md5(b"".join(hashlib.md5(p).digest() for p in PARTS)).hexdigest() + '-2"'
OLD_BARE = OLD_ETAG.strip('"')
OTHER = '"0123456789abcdef0123456789abcdef"'


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    assert client.put(f"/{BUCKET}").status_code == 200
    return client, storage, metadata


def _code(resp) -> str:
    return ET.fromstring(resp.text).findtext("Code")


def _upload(client, key: str = KEY, parts: list[bytes] = PARTS) -> tuple[str, list[str]]:
    """An open upload with the given parts; its id and the parts' ETags."""
    uid = ET.fromstring(client.post(f"/{BUCKET}/{key}?uploads").text).findtext("UploadId")
    etags = []
    for number, data in enumerate(parts, start=1):
        r = client.put(f"/{BUCKET}/{key}?partNumber={number}&uploadId={uid}", content=data)
        assert r.status_code == 200
        etags.append(r.headers["etag"])
    return uid, etags


def _body(etags: list[str]) -> bytes:
    items = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>"
        for n, e in enumerate(etags, start=1)
    )
    return f"<CompleteMultipartUpload>{items}</CompleteMultipartUpload>".encode()


def _complete(client, uid: str, etags: list[str], headers: dict, key: str = KEY):
    return client.post(f"/{BUCKET}/{key}?uploadId={uid}", content=_body(etags), headers=headers)


def _state(storage, metadata, uid: str) -> tuple:
    """Everything the store holds that a refused Complete must leave alone."""
    return (
        metadata.list_objects(BUCKET),
        metadata.list_blob_records(),
        storage.temp_entries(),
        sorted(storage.scan()),
        metadata.get_upload(uid),
        metadata.get_parts(uid),
        metadata.list_incomplete_uploads(),
    )


def assert_store_clean(storage, metadata, live: int) -> None:
    """Only live blobs, no pending or garbage one, an empty tmp/ and a quiet sweep."""
    records = metadata.list_blob_records()
    assert [r.state for r in records] == ["live"] * live
    assert storage.temp_entries() == []
    assert run_consistency_sweep(metadata, storage).issues == []


def assert_upload_open(metadata, uid: str, etags: list[str]) -> None:
    assert metadata.upload_exists(uid)
    assert [u.upload_id for u in metadata.list_incomplete_uploads()] == [uid]
    parts = metadata.get_parts(uid)
    assert [(p.part_number, p.etag) for p in parts] == list(enumerate(etags, start=1))
    assert all(p.blob_id is not None for p in parts)


# (id, headers, key exists, status, code); the existing object is OLD.
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
def test_conditional_complete(env, headers, exists, status, code):
    client, storage, metadata = env
    if exists:
        assert client.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200
    uid, etags = _upload(client)
    initiated = metadata.get_upload(uid).initiated
    before = _state(storage, metadata, uid)
    row_before = metadata.get_object_meta(BUCKET, KEY)
    head_before = client.head(f"/{BUCKET}/{KEY}") if exists else None

    resp = _complete(client, uid, etags, headers)

    assert resp.status_code == status
    if code is not None:
        assert _code(resp) == code
        assert _state(storage, metadata, uid) == before
        got = client.get(f"/{BUCKET}/{KEY}")
        if exists:
            assert got.status_code == 200 and got.content == OLD
            assert got.headers["etag"] == OLD_ETAG
            assert got.headers["last-modified"] == head_before.headers["last-modified"]
            assert metadata.get_object_meta(BUCKET, KEY) == row_before
        else:
            assert got.status_code == 404
        assert_upload_open(metadata, uid, etags)
        # Live blobs: the old object's (if any) and the two part blobs.
        assert_store_clean(storage, metadata, live=(1 if exists else 0) + len(PARTS))

        # A retry whose condition holds works, with nothing re-uploaded.
        retry = {"If-Match": OLD_ETAG} if exists else {"If-None-Match": "*"}
        again = _complete(client, uid, etags, retry)
        assert again.status_code == 200
        assert ET.fromstring(again.text).findtext("ETag") == NEW_ETAG
        assert client.get(f"/{BUCKET}/{KEY}").content == b"".join(PARTS)
        assert not metadata.upload_exists(uid)
        assert_store_clean(storage, metadata, live=1)
    else:
        assert ET.fromstring(resp.text).findtext("ETag") == NEW_ETAG
        got = client.get(f"/{BUCKET}/{KEY}")
        assert got.content == b"".join(PARTS) and got.headers["etag"] == NEW_ETAG
        # Last-Modified stays the time the upload was initiated, not the commit's.
        assert metadata.get_object_meta(BUCKET, KEY).last_modified == initiated
        assert not metadata.upload_exists(uid)
        assert_store_clean(storage, metadata, live=1)


def test_a_refused_complete_leaves_the_old_object_headers_and_metadata(env):
    client, storage, metadata = env
    keep = {"Content-Type": "text/plain", "Cache-Control": "max-age=5", "x-amz-meta-a": "1"}
    assert client.put(f"/{BUCKET}/{KEY}", content=OLD, headers=keep).status_code == 200
    row = metadata.get_object_meta(BUCKET, KEY)
    uid = ET.fromstring(
        client.post(f"/{BUCKET}/{KEY}?uploads", headers={"x-amz-meta-b": "2"}).text
    ).findtext("UploadId")
    etag = client.put(f"/{BUCKET}/{KEY}?partNumber=1&uploadId={uid}", content=b"p").headers["etag"]
    resp = _complete(client, uid, [etag], {"If-Match": OTHER})
    assert resp.status_code == 412
    assert metadata.get_object_meta(BUCKET, KEY) == row
    head = client.head(f"/{BUCKET}/{KEY}")
    assert head.headers["x-amz-meta-a"] == "1" and "x-amz-meta-b" not in head.headers


def test_a_complete_that_holds_replaces_the_old_blob(env):
    client, storage, metadata = env
    assert client.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200
    old_blob = metadata.get_object_meta(BUCKET, KEY).blob_id
    uid, etags = _upload(client)
    assert _complete(client, uid, etags, {"If-Match": OLD_ETAG}).status_code == 200
    assert metadata.blob_state(old_blob) is None
    assert not storage.blob_path(old_blob).exists()
    assert_store_clean(storage, metadata, live=1)


def test_the_same_if_match_on_a_second_upload_after_success_is_412(env):
    client, storage, metadata = env
    assert client.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200
    uid, etags = _upload(client)
    assert _complete(client, uid, etags, {"If-Match": OLD_ETAG}).status_code == 200
    uid2, etags2 = _upload(client)
    again = _complete(client, uid2, etags2, {"If-Match": OLD_ETAG})
    assert again.status_code == 412 and _code(again) == "PreconditionFailed"
    assert client.get(f"/{BUCKET}/{KEY}").headers["etag"] == NEW_ETAG
    assert_upload_open(metadata, uid2, etags2)
    assert_store_clean(storage, metadata, live=1 + len(PARTS))


def test_the_refusal_is_the_same_for_a_second_attempt_on_the_open_upload(env):
    client, storage, metadata = env
    assert client.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200
    uid, etags = _upload(client)
    for _ in range(2):
        assert _complete(client, uid, etags, {"If-None-Match": "*"}).status_code == 412
        assert_upload_open(metadata, uid, etags)
    assert _complete(client, uid, etags, {"If-None-Match": OTHER}).status_code == 200


# --- error order: every other Complete error comes before 412 and 404 ---

REFUSING = [
    pytest.param({"If-None-Match": "*"}, True, id="if-none-match-star-on-existing"),
    pytest.param({"If-Match": OTHER}, True, id="if-match-mismatch-on-existing"),
    pytest.param({"If-Match": "*"}, False, id="if-match-star-on-missing"),
]


@pytest.mark.parametrize("headers,exists", REFUSING)
def test_no_such_upload_beats_the_conditions(env, headers, exists):
    client, storage, metadata = env
    if exists:
        assert client.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200
    before = _state(storage, metadata, "no-such-upload")
    resp = _complete(client, "0" * 32, ['"abc"'], headers)
    assert resp.status_code == 404 and _code(resp) == "NoSuchUpload"
    assert _state(storage, metadata, "no-such-upload") == before


@pytest.mark.parametrize("headers,exists", REFUSING)
def test_a_completed_upload_is_no_such_upload_whatever_the_conditions_say(env, headers, exists):
    client, storage, metadata = env
    uid, etags = _upload(client)
    assert _complete(client, uid, etags, {}).status_code == 200
    if not exists:  # otherwise the completed upload's own object is the existing one
        assert client.delete(f"/{BUCKET}/{KEY}").status_code == 204
    resp = _complete(client, uid, etags, headers)
    assert resp.status_code == 404 and _code(resp) == "NoSuchUpload"


@pytest.mark.parametrize("headers,exists", REFUSING)
def test_an_aborted_upload_is_no_such_upload_whatever_the_conditions_say(env, headers, exists):
    client, storage, metadata = env
    if exists:
        assert client.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200
    uid, etags = _upload(client)
    assert client.delete(f"/{BUCKET}/{KEY}?uploadId={uid}").status_code == 204
    resp = _complete(client, uid, etags, headers)
    assert resp.status_code == 404 and _code(resp) == "NoSuchUpload"


BAD_LISTS = [
    pytest.param(lambda etags: [OTHER, etags[1]], id="wrong-etag"),
    pytest.param(lambda etags: [etags[0], etags[1], etags[0]], id="part-never-uploaded"),
    pytest.param(lambda etags: [etags[0], '"x"'], id="second-part-wrong"),
]


@pytest.mark.parametrize("headers,exists", REFUSING)
@pytest.mark.parametrize("bad", BAD_LISTS)
def test_invalid_part_beats_the_conditions(env, headers, exists, bad):
    client, storage, metadata = env
    if exists:
        assert client.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200
    uid, etags = _upload(client)
    before = _state(storage, metadata, uid)
    resp = _complete(client, uid, bad(etags), headers)
    assert resp.status_code == 400 and _code(resp) == "InvalidPart"
    assert _state(storage, metadata, uid) == before
    assert_upload_open(metadata, uid, etags)


@pytest.mark.parametrize("headers,exists", REFUSING)
def test_a_part_that_was_never_uploaded_is_invalid_part_not_a_condition_failure(
    env, headers, exists
):
    client, storage, metadata = env
    if exists:
        assert client.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200
    uid, etags = _upload(client)
    body = (
        f"<CompleteMultipartUpload><Part><PartNumber>3</PartNumber><ETag>{etags[0]}</ETag>"
        f"</Part></CompleteMultipartUpload>"
    ).encode()
    resp = client.post(f"/{BUCKET}/{KEY}?uploadId={uid}", content=body, headers=headers)
    assert resp.status_code == 400 and _code(resp) == "InvalidPart"
    assert_upload_open(metadata, uid, etags)


@pytest.mark.parametrize("headers,exists", REFUSING)
def test_an_empty_part_list_is_malformed_xml_not_a_condition_failure(env, headers, exists):
    client, storage, metadata = env
    if exists:
        assert client.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200
    uid, etags = _upload(client)
    resp = client.post(
        f"/{BUCKET}/{KEY}?uploadId={uid}",
        content=b"<CompleteMultipartUpload/>",
        headers=headers,
    )
    assert resp.status_code == 400 and _code(resp) == "MalformedXML"
    assert_upload_open(metadata, uid, etags)


def test_a_subset_of_the_parts_with_a_failing_condition_keeps_every_part(env):
    # Unlisted parts are dropped only by a Complete that commits.
    client, storage, metadata = env
    assert client.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200
    uid, etags = _upload(client)
    resp = _complete(client, uid, etags[:1], {"If-None-Match": "*"})
    assert resp.status_code == 412
    assert_upload_open(metadata, uid, etags)
    assert_store_clean(storage, metadata, live=1 + len(PARTS))


# --- headers that are not conditions on Complete ---


def test_the_initiated_time_header_is_not_a_complete_condition(env):
    # x-amz-if-match-initiated-time belongs to AbortMultipartUpload, where it is 501
    # (test_conditional_delete.py). Complete never took it and still ignores it.
    client, storage, metadata = env
    uid, etags = _upload(client)
    resp = _complete(client, uid, etags, {"x-amz-if-match-initiated-time": "x"})
    assert resp.status_code == 200


def test_abort_with_the_initiated_time_header_is_still_501(env):
    client, storage, metadata = env
    uid, etags = _upload(client)
    resp = client.delete(
        f"/{BUCKET}/{KEY}?uploadId={uid}",
        headers={"x-amz-if-match-initiated-time": "Mon, 01 Jan 2024 00:00:00 GMT"},
    )
    assert resp.status_code == 501 and _code(resp) == "NotImplemented"
    assert_upload_open(metadata, uid, etags)
