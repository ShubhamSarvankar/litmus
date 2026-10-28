"""
Tier A: If-Match on DeleteObject (docs/design/storage-layout.md §10).

The condition is decided inside the delete's commit, against the ETag of the row that
commit deletes. A refusal is 412 and leaves the row, the blob and the registry as they
were; a delete that goes ahead is handled like an unconditional one. A key that does
not exist is 404 NoSuchKey whatever the value, and nothing changes (an unconditional
delete of one is 204). x-amz-if-match-last-modified-time and x-amz-if-match-size stay 501.
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

BUCKET = "cond-delete"
KEY = "obj.txt"
URL = f"/{BUCKET}/{KEY}"
BODY = b"precious content"
ETAG = '"' + hashlib.md5(BODY).hexdigest() + '"'
BARE = ETAG.strip('"')
OTHER = '"0123456789abcdef0123456789abcdef"'

DIRECTORY_BUCKET_HEADERS = {
    "x-amz-if-match-last-modified-time": "Wed, 01 Jan 2020 00:00:00 GMT",
    "x-amz-if-match-size": str(len(BODY)),
}


class Env:
    def __init__(self, tmp_path: Path):
        self.storage = BlobStore(tmp_path / "blobs")
        self.metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
        self.client = TestClient(
            create_app(storage=self.storage, metadata=self.metadata),
            raise_server_exceptions=False,
        )
        assert self.client.put(f"/{BUCKET}").status_code == 200
        assert self.client.put(URL, content=BODY).status_code == 200

    def state(self):
        """The row, the registry and the blob files: everything a delete may change."""
        return (
            self.metadata.get_object_meta(BUCKET, KEY),
            self.metadata.list_blob_records(),
            sorted(blob_id for _, blob_id in self.storage.scan()),
        )

    def assert_refused_and_untouched(self, resp, status: int, before) -> None:
        assert resp.status_code == status
        assert self.state() == before
        got = self.client.get(URL)
        assert got.status_code == 200 and got.content == BODY
        assert got.headers["etag"] == ETAG
        assert [r.state for r in self.metadata.list_blob_records()] == ["live"]  # no garbage
        assert self.storage.temp_entries() == []
        assert run_consistency_sweep(self.metadata, self.storage).issues == []

    def assert_deleted_like_unconditional(self, resp) -> None:
        assert resp.status_code == 204 and resp.content == b""
        assert self.client.get(URL).status_code == 404
        assert self.metadata.get_object_meta(BUCKET, KEY) is None
        # The blob is unlinked and its registry row gone, which is what an unconditional
        # delete leaves (checked against one in test_unconditional_delete_leaves_the_same).
        assert self.metadata.list_blob_records() == []
        assert list(self.storage.scan()) == []
        assert self.storage.temp_entries() == []
        assert run_consistency_sweep(self.metadata, self.storage).issues == []


@pytest.fixture
def env(tmp_path: Path) -> Env:
    return Env(tmp_path)


def _code(resp) -> str:
    return ET.fromstring(resp.text).findtext("Code")


def test_unconditional_delete_leaves_the_same(env):
    env.assert_deleted_like_unconditional(env.client.delete(URL))


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("*", id="star"),
        pytest.param(ETAG, id="quoted"),
        pytest.param(BARE, id="unquoted"),
        pytest.param(f"{OTHER}, {ETAG}", id="list-with-a-match"),
        pytest.param(f"{OTHER},{BARE}", id="unquoted-list-with-a-match"),
        pytest.param(f"  {ETAG}  ", id="padded"),
        pytest.param("", id="blank"),
    ],
)
def test_if_match_that_holds_deletes_the_object(env, value):
    env.assert_deleted_like_unconditional(env.client.delete(URL, headers={"if-match": value}))


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(OTHER, id="other-quoted"),
        pytest.param("badetag", id="other-unquoted"),
        pytest.param(f"W/{ETAG}", id="weak"),
        pytest.param(f"{OTHER}, W/{ETAG}", id="list-with-only-a-weak-match"),
        pytest.param(f'{OTHER}, "feedfacefeedfacefeedfacefeedface"', id="list-without-a-match"),
        pytest.param(ETAG[:-2] + '"', id="truncated"),
    ],
)
def test_if_match_that_fails_is_412_and_changes_nothing(env, value):
    before = env.state()
    resp = env.client.delete(URL, headers={"if-match": value})
    assert _code(resp) == "PreconditionFailed"
    env.assert_refused_and_untouched(resp, 412, before)
    # The same request without the condition then goes through.
    env.assert_deleted_like_unconditional(env.client.delete(URL))


def test_repeated_if_match_lines_are_one_list(env):
    before = env.state()
    resp = env.client.delete(URL, headers=[("if-match", OTHER), ("if-match", "badetag")])
    env.assert_refused_and_untouched(resp, 412, before)
    resp = env.client.delete(URL, headers=[("if-match", OTHER), ("if-match", ETAG)])
    env.assert_deleted_like_unconditional(resp)


@pytest.mark.parametrize("value", ["*", ETAG, OTHER, "badetag", f"W/{ETAG}"])
def test_if_match_on_a_missing_key_is_404_and_changes_nothing(env, value):
    before = env.state()
    resp = env.client.delete(f"/{BUCKET}/missing", headers={"if-match": value})
    assert resp.status_code == 404 and _code(resp) == "NoSuchKey"
    assert ET.fromstring(resp.text).findtext("Key") == "missing"
    assert env.state() == before
    assert env.client.get(URL).content == BODY


def test_unconditional_and_blank_if_match_deletes_of_a_missing_key_are_204(env):
    assert env.client.delete(f"/{BUCKET}/missing").status_code == 204
    # A blank If-Match is no condition.
    assert env.client.delete(f"/{BUCKET}/missing", headers={"if-match": ""}).status_code == 204
    assert env.client.get(URL).content == BODY


def test_if_match_on_a_key_deleted_earlier_is_404(env):
    assert env.client.delete(URL, headers={"if-match": ETAG}).status_code == 204
    resp = env.client.delete(URL, headers={"if-match": ETAG})
    assert resp.status_code == 404 and _code(resp) == "NoSuchKey"
    assert env.client.delete(URL).status_code == 204


def test_if_match_on_a_missing_bucket_is_still_no_such_bucket(env):
    resp = env.client.delete("/no-such-bucket/k", headers={"if-match": "*"})
    assert resp.status_code == 404 and _code(resp) == "NoSuchBucket"


def test_if_match_with_the_null_version_id_is_evaluated(env):
    before = env.state()
    resp = env.client.delete(f"{URL}?versionId=null", headers={"if-match": OTHER})
    env.assert_refused_and_untouched(resp, 412, before)
    env.assert_deleted_like_unconditional(
        env.client.delete(f"{URL}?versionId=null", headers={"if-match": ETAG})
    )


def test_if_match_on_an_overwritten_object_is_decided_on_the_current_etag(env):
    new = b"replacement"
    assert env.client.put(URL, content=new).status_code == 200
    before = env.state()
    resp = env.client.delete(URL, headers={"if-match": ETAG})
    assert resp.status_code == 412
    assert env.state() == before
    assert env.client.get(URL).content == new
    new_etag = '"' + hashlib.md5(new).hexdigest() + '"'
    assert env.client.delete(URL, headers={"if-match": new_etag}).status_code == 204
    assert env.client.get(URL).status_code == 404


def test_a_refusal_keeps_last_modified_and_the_listing(env):
    head = env.client.head(URL).headers
    before = env.state()
    assert env.client.delete(URL, headers={"if-match": OTHER}).status_code == 412
    after = env.client.head(URL).headers
    assert (after["etag"], after["last-modified"], after["content-length"]) == (
        head["etag"],
        head["last-modified"],
        head["content-length"],
    )
    assert env.state() == before
    listing = ET.fromstring(env.client.get(f"/{BUCKET}?list-type=2").text)
    keys = [e.text for e in listing.iter() if e.tag.rpartition("}")[2] == "Key"]
    assert keys == [KEY]


@pytest.mark.parametrize("header", sorted(DIRECTORY_BUCKET_HEADERS))
@pytest.mark.parametrize("with_if_match", [False, True])
def test_directory_bucket_headers_stay_501_and_delete_nothing(env, header, with_if_match):
    before = env.state()
    headers = {header: DIRECTORY_BUCKET_HEADERS[header]}
    if with_if_match:
        headers["if-match"] = ETAG
    resp = env.client.delete(URL, headers=headers)
    assert resp.status_code == 501 and _code(resp) == "NotImplemented"
    assert env.state() == before
    assert env.client.get(URL).content == BODY


@pytest.mark.parametrize("with_if_match", [False, True])
def test_both_directory_bucket_headers_together_stay_501(env, with_if_match):
    before = env.state()
    headers = dict(DIRECTORY_BUCKET_HEADERS)
    if with_if_match:
        headers["if-match"] = ETAG
    resp = env.client.delete(URL, headers=headers)
    assert resp.status_code == 501 and _code(resp) == "NotImplemented"
    assert env.state() == before


def test_if_none_match_on_delete_object_is_still_501(env):
    before = env.state()
    resp = env.client.delete(URL, headers={"if-none-match": OTHER})
    assert resp.status_code == 501 and _code(resp) == "NotImplemented"
    assert env.state() == before


def test_abort_multipart_upload_still_refuses_if_match(env):
    uid = ET.fromstring(env.client.post(f"/{BUCKET}/mp?uploads").text).findtext("UploadId")
    assert (
        env.client.put(f"/{BUCKET}/mp?partNumber=1&uploadId={uid}", content=b"p").status_code == 200
    )
    resp = env.client.delete(f"/{BUCKET}/mp?uploadId={uid}", headers={"if-match": "*"})
    assert resp.status_code == 501 and _code(resp) == "NotImplemented"
    assert env.metadata.upload_exists(uid, bucket=BUCKET, key="mp")


@pytest.mark.parametrize(
    "element",
    ["<LastModifiedTime>2020-01-01T00:00:00.000Z</LastModifiedTime>", f"<Size>{len(BODY)}</Size>"],
)
def test_delete_objects_directory_bucket_elements_stay_per_key_errors(env, element):
    """LastModifiedTime and Size on a DeleteObjects entry are a per-key NotImplemented
    error, and the object stays (per key ETag conditions: test_delete_objects_etag.py)."""
    body = f"<Delete><Object><Key>{KEY}</Key>{element}</Object></Delete>".encode()
    root = ET.fromstring(env.client.post(f"/{BUCKET}?delete", content=body).text)
    assert [e.findtext("Code") for e in root.findall("Error")] == ["NotImplemented"]
    assert env.client.get(URL).content == BODY
