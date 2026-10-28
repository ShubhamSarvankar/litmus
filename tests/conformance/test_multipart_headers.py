"""The headers and user metadata given to CreateMultipartUpload (Content-Type,
Cache-Control, Content-Disposition, Content-Encoding, Content-Language, Expires and
x-amz-meta-*) are stored with the upload and given to the object by
CompleteMultipartUpload, with the same parsing and rules as PutObject. A completed
object's Last-Modified is the time the upload was initiated.

UploadPart and CompleteMultipartUpload document no such headers (S3 API reference), so
they ignore them.
"""

import base64
import sqlite3
import time
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from chaos.framework.framed import framed_request
from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "mph-bucket"
URL = f"/{BUCKET}/k"
PARTS = [b"aaaa", b"bbbb", b"cc"]
BODY = b"".join(PARTS)

SYSTEM = {
    "Cache-Control": "public, max-age=14400",
    "Content-Disposition": 'attachment; filename="a b.txt"',
    "Content-Encoding": "gzip",
    "Content-Language": "en-US",
    "Content-Type": "text/plain",
    "Expires": "Thu, 01 Dec 2033 16:00:00 GMT",
}


class Env:
    def __init__(self, tmp_path: Path):
        self.db = tmp_path / "meta.db"
        self.storage = BlobStore(tmp_path / "blobs")
        self.metadata = SQLiteMetadataStore(db_path=self.db)
        self.client = TestClient(
            create_app(storage=self.storage, metadata=self.metadata),
            raise_server_exceptions=False,
        )
        assert self.client.put(f"/{BUCKET}").status_code == 200

    def create(self, headers=None, key="k") -> str:
        resp = self.client.post(f"/{BUCKET}/{key}?uploads", headers=headers or {})
        assert resp.status_code == 200, resp.text
        return ET.fromstring(resp.text).findtext("UploadId")

    def upload_parts(self, upload_id, key="k", headers=None) -> list[str]:
        etags = []
        for num, data in enumerate(PARTS, start=1):
            url = f"/{BUCKET}/{key}?partNumber={num}&uploadId={upload_id}"
            resp = self.client.put(url, content=data, headers=headers or {})
            assert resp.status_code == 200
            etags.append(resp.headers["etag"])
        return etags

    def complete(self, upload_id, etags, key="k", headers=None):
        root = ET.Element("CompleteMultipartUpload")
        for num, etag in enumerate(etags, start=1):
            part = ET.SubElement(root, "Part")
            ET.SubElement(part, "PartNumber").text = str(num)
            ET.SubElement(part, "ETag").text = etag
        return self.client.post(
            f"/{BUCKET}/{key}?uploadId={upload_id}",
            content=ET.tostring(root),
            headers=headers or {},
        )

    def whole(self, headers=None, key="k"):
        """Create, upload the parts and complete; the Complete response."""
        upload_id = self.create(headers, key)
        resp = self.complete(upload_id, self.upload_parts(upload_id, key), key)
        assert resp.status_code == 200, resp.text
        return upload_id

    def row(self, key="k"):
        return self.metadata.get_object_meta(BUCKET, key)


@pytest.fixture
def env(tmp_path: Path) -> Env:
    return Env(tmp_path)


def _get(client, headers=None):
    """(status, headers, raw bytes): raw, because a stored Content-Encoding would make the
    client decode the body."""
    with client.stream("GET", URL, headers=headers) as resp:
        return resp.status_code, resp.headers, b"".join(resp.iter_raw())


def _meta(headers) -> dict:
    return {k[11:]: v for k, v in headers.items() if k.startswith("x-amz-meta-")}


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _word(text: str) -> str:
    return "=?UTF-8?B?" + base64.b64encode(text.encode()).decode() + "?="


# --- headers and metadata survive Complete ---


@pytest.mark.parametrize("name", list(SYSTEM))
def test_each_header_survives_complete(env, name):
    env.whole({name: SYSTEM[name]})
    status, headers, body = _get(env.client)
    assert (status, body) == (200, BODY)
    assert headers[name] == SYSTEM[name]
    assert env.client.head(URL).headers[name] == SYSTEM[name]
    for other in SYSTEM.keys() - {name, "Content-Type"}:
        assert other not in headers


def test_all_headers_and_metadata_on_200_206_and_head(env):
    sent = {**SYSTEM, "x-amz-meta-author": "Janet", "X-Amz-Meta-N": "7"}
    env.whole(sent)
    for name, value in SYSTEM.items():
        status, headers, _ = _get(env.client)
        assert (status, headers[name]) == (200, value)
    status, headers, body = _get(env.client, {"range": "bytes=2-5"})
    assert (status, body) == (206, b"aabb")
    for name, value in SYSTEM.items():
        assert headers[name] == value
        assert env.client.head(URL).headers[name] == value
    assert _meta(headers) == {"author": "Janet", "n": "7"}
    assert _meta(env.client.head(URL).headers) == {"author": "Janet", "n": "7"}
    assert env.row().user_metadata == {"author": "Janet", "n": "7"}


def test_the_object_row_is_what_the_upload_row_was(env):
    sent = {**SYSTEM, "x-amz-meta-a": "1"}
    upload_id = env.create(sent)
    upload = env.metadata.get_upload(upload_id, bucket=BUCKET, key="k")
    assert (upload.content_type, upload.cache_control, upload.user_metadata) == (
        "text/plain",
        "public, max-age=14400",
        {"a": "1"},
    )
    env.complete(upload_id, env.upload_parts(upload_id))
    row = env.row()
    assert (row.content_type, row.cache_control, row.content_disposition) == (
        "text/plain",
        "public, max-age=14400",
        'attachment; filename="a b.txt"',
    )
    assert (row.content_encoding, row.content_language, row.expires) == (
        "gzip",
        "en-US",
        "Thu, 01 Dec 2033 16:00:00 GMT",
    )
    assert row.user_metadata == {"a": "1"}


def test_an_upload_without_headers_makes_an_object_without_them(env):
    env.whole()
    row = env.row()
    assert row.content_type == "binary/octet-stream"
    assert (row.cache_control, row.content_encoding, row.expires) == (None, None, None)
    assert row.user_metadata == {}
    _, headers, _ = _get(env.client)
    assert _meta(headers) == {} and "Cache-Control" not in headers


def test_the_same_parsing_as_putobject(env):
    sent = {
        "Content-Encoding": "gzip, aws-chunked",
        "Cache-Control": "",
        "X-Amz-Meta-MixedCase": "v",
        "x-amz-meta-empty": "",
        "x-amz-meta-w": _word("café"),
    }
    env.whole(sent)
    row = env.row()
    assert row.content_encoding == "gzip"
    assert row.cache_control is None
    assert row.user_metadata == {"mixedcase": "v", "empty": "", "w": "café"}
    assert _meta(env.client.head(URL).headers) == {
        "mixedcase": "v",
        "empty": "",
        "w": _word("café"),
    }


@pytest.mark.parametrize(
    ("sent", "stored"),
    [("aws-chunked", None), ("AWS-Chunked, gzip", "gzip"), ("gzip,aws-chunked", "gzip")],
)
def test_aws_chunked_is_stripped_from_the_content_encoding(env, sent, stored):
    env.whole({"Content-Encoding": sent})
    assert env.row().content_encoding == stored


def test_repeated_headers_are_joined_as_putobject_joins_them(env):
    env.whole(
        [
            ("cache-control", "no-cache"),
            ("cache-control", "no-store"),
            ("x-amz-meta-a", "1"),
            ("x-amz-meta-a", "2"),
        ]
    )
    row = env.row()
    assert row.cache_control == "no-cache, no-store"
    assert row.user_metadata == {"a": "1, 2"}


def test_a_framed_create_is_still_refused_as_before(env):
    headers, body = framed_request(b"x")
    resp = env.client.post(f"/{BUCKET}/k?uploads", content=body, headers=headers)
    assert resp.status_code == 501
    assert env.metadata.list_incomplete_uploads() == []


# --- overwrite through Complete ---


def test_complete_over_an_existing_object_replaces_headers_and_metadata(env):
    old = {**SYSTEM, "x-amz-meta-a": "old", "x-amz-meta-b": "old"}
    env.client.put(URL, content=b"old body", headers=old)
    env.whole({"Content-Language": "de", "Content-Type": "text/new", "x-amz-meta-b": "new"})
    row = env.row()
    assert (row.content_language, row.content_type) == ("de", "text/new")
    assert (row.cache_control, row.content_disposition, row.content_encoding, row.expires) == (
        None,
        None,
        None,
        None,
    )
    assert row.user_metadata == {"b": "new"}
    status, headers, body = _get(env.client)
    assert (status, body) == (200, BODY)
    assert "Cache-Control" not in headers and _meta(headers) == {"b": "new"}


def test_complete_without_headers_over_an_object_with_them_clears_them(env):
    env.client.put(URL, content=b"old body", headers={**SYSTEM, "x-amz-meta-a": "old"})
    env.whole()
    row = env.row()
    assert row.content_type == "binary/octet-stream"
    assert (row.cache_control, row.content_encoding, row.expires) == (None, None, None)
    assert row.user_metadata == {}


def test_putobject_over_a_completed_object_replaces_them_too(env):
    env.whole({**SYSTEM, "x-amz-meta-a": "1"})
    env.client.put(URL, content=b"plain")
    row = env.row()
    assert (row.cache_control, row.user_metadata) == (None, {})


def test_two_open_uploads_of_a_key_give_the_object_the_headers_of_the_one_completed_last(env):
    first, second = (
        env.create({"x-amz-meta-which": "first"}),
        env.create({"x-amz-meta-which": "second"}),
    )
    first_etags, second_etags = env.upload_parts(first), env.upload_parts(second)
    assert env.complete(second, second_etags).status_code == 200
    assert env.row().user_metadata == {"which": "second"}
    assert env.complete(first, first_etags).status_code == 200
    assert env.row().user_metadata == {"which": "first"}


# --- size ---


def _meta_of(total: int) -> dict:
    return {"x-amz-meta-a": "v" * (total - 1)}


def test_exactly_2048_bytes_is_accepted_at_create(env):
    upload_id = env.create(_meta_of(2048))
    env.complete(upload_id, env.upload_parts(upload_id))
    assert len(env.row().user_metadata["a"]) == 2047


def test_2049_bytes_is_refused_at_create_and_creates_no_upload(env):
    resp = env.client.post(f"{URL}?uploads", headers=_meta_of(2049))
    assert resp.status_code == 400
    assert ET.fromstring(resp.text).findtext("Code") == "MetadataTooLarge"
    assert env.metadata.list_incomplete_uploads() == []
    with sqlite3.connect(env.db) as conn:
        assert conn.execute("SELECT count(*) FROM multipart_uploads").fetchone() == (0,)
    assert env.metadata.list_blob_records() == []
    assert env.storage.temp_entries() == []


def test_the_prefix_and_other_headers_are_not_counted_at_create(env):
    headers = {**_meta_of(2048), "Cache-Control": "c" * 3000, "x-amz-other": "o" * 500}
    assert env.client.post(f"{URL}?uploads", headers=headers).status_code == 200


def test_a_refused_create_leaves_an_existing_upload_and_object_alone(env):
    env.whole({"x-amz-meta-a": "old"})
    other = env.create({"x-amz-meta-a": "kept"})
    assert env.client.post(f"{URL}?uploads", headers=_meta_of(2049)).status_code == 400
    assert [u.upload_id for u in env.metadata.list_incomplete_uploads()] == [other]
    assert env.row().user_metadata == {"a": "old"}


def test_a_missing_bucket_is_reported_before_the_size(env):
    resp = env.client.post("/nobucket/k?uploads", headers=_meta_of(2049))
    assert resp.status_code == 404
    assert ET.fromstring(resp.text).findtext("Code") == "NoSuchBucket"


# --- abort ---


def test_an_aborted_upload_leaves_nothing(env):
    upload_id = env.create({**SYSTEM, "x-amz-meta-a": "1"})
    env.upload_parts(upload_id)
    assert env.client.delete(f"{URL}?uploadId={upload_id}").status_code == 204
    assert env.metadata.get_upload(upload_id, bucket=BUCKET, key="k") is None
    assert env.metadata.get_object_meta(BUCKET, "k") is None
    assert env.metadata.list_blob_records() == []
    assert env.client.get(URL).status_code == 404
    assert env.complete(upload_id, ["x"]).status_code == 404
    # Nothing of it reaches the next upload of the key.
    env.whole()
    assert env.row().user_metadata == {} and env.row().content_type == "binary/octet-stream"


def test_an_aborted_upload_leaves_an_existing_object_as_it_was(env):
    env.client.put(
        URL, content=b"old body", headers={"Cache-Control": "keep", "x-amz-meta-a": "old"}
    )
    upload_id = env.create({"Cache-Control": "new", "x-amz-meta-a": "new"})
    env.upload_parts(upload_id)
    env.client.delete(f"{URL}?uploadId={upload_id}")
    status, headers, body = _get(env.client)
    assert (status, body, headers["Cache-Control"], _meta(headers)) == (
        200,
        b"old body",
        "keep",
        {"a": "old"},
    )


# --- UploadPart and Complete ignore these headers ---


def test_uploadpart_and_complete_ignore_the_headers_when_a_client_sends_them(env):
    upload_id = env.create({"Content-Type": "text/create", "x-amz-meta-a": "create"})
    noise = {**SYSTEM, "Content-Type": "text/part", "x-amz-meta-a": "part", "x-amz-meta-z": "z"}
    etags = env.upload_parts(upload_id, headers=noise)
    assert env.metadata.get_upload(upload_id, bucket=BUCKET, key="k").user_metadata == {
        "a": "create"
    }
    resp = env.complete(upload_id, etags, headers={**noise, "Content-Type": "application/xml"})
    assert resp.status_code == 200
    row = env.row()
    assert (row.content_type, row.user_metadata) == ("text/create", {"a": "create"})
    assert (row.cache_control, row.content_encoding) == (None, None)


# --- uploads created before this change ---


def test_an_upload_row_with_default_columns_completes_to_an_object_without_headers(env):
    # The row CreateMultipartUpload wrote before it stored headers: five columns, the
    # rest at their defaults.
    with sqlite3.connect(env.db) as conn:
        conn.execute(
            "INSERT INTO multipart_uploads (upload_id, bucket, key, initiated, state) "
            "VALUES ('legacy', ?, 'k', '2020-01-02T03:04:05.000000', 'open')",
            (BUCKET,),
        )
    etags = env.upload_parts("legacy")
    assert env.complete("legacy", etags).status_code == 200
    row = env.row()
    assert row.content_type == "binary/octet-stream"
    assert (row.cache_control, row.content_disposition, row.content_encoding) == (None, None, None)
    assert (row.content_language, row.expires, row.user_metadata) == (None, None, {})
    status, headers, body = _get(env.client)
    assert (status, body) == (200, BODY)
    assert _meta(headers) == {} and "Cache-Control" not in headers
    # Its Last-Modified is its initiation time.
    assert row.last_modified == datetime(2020, 1, 2, 3, 4, 5)
    assert headers["Last-Modified"] == "Thu, 02 Jan 2020 03:04:05 GMT"


# --- Last-Modified is the initiation time ---


def _listed(env, version: int) -> list[str]:
    query = "list-type=2" if version == 2 else ""
    root = ET.fromstring(env.client.get(f"/{BUCKET}?{query}").text)
    return [e.text for e in root.iter() if e.tag.rsplit("}", 1)[-1] == "LastModified"]


def test_last_modified_is_the_initiation_time_in_get_head_and_listings(env):
    upload_id = env.create()
    initiated = env.metadata.get_upload(upload_id, bucket=BUCKET, key="k").initiated
    etags = env.upload_parts(upload_id)
    time.sleep(1.2)  # the Complete second is not the initiation second
    assert env.complete(upload_id, etags).status_code == 200

    row = env.row()
    assert row.last_modified == initiated
    expected = initiated.strftime("%a, %d %b %Y %H:%M:%S GMT")
    status, headers, _ = _get(env.client)
    assert headers["Last-Modified"] == expected
    assert env.client.head(URL).headers["Last-Modified"] == expected
    assert _get(env.client, {"range": "bytes=0-1"})[1]["Last-Modified"] == expected
    listed_stamp = initiated.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    assert _listed(env, 1) == [listed_stamp]
    assert _listed(env, 2) == [listed_stamp]
    versions = ET.fromstring(env.client.get(f"/{BUCKET}?versions").text)
    stamps = [e.text for e in versions.iter() if e.tag.rsplit("}", 1)[-1] == "LastModified"]
    assert stamps == [listed_stamp]


def test_a_putobject_is_still_dated_by_its_own_request(env):
    before = _now().replace(microsecond=0)
    env.client.put(URL, content=b"x")
    assert env.row().last_modified.replace(microsecond=0) >= before


def test_completing_over_an_object_takes_the_new_uploads_initiation_time(env):
    env.client.put(URL, content=b"old")
    time.sleep(1.1)
    upload_id = env.create()
    initiated = env.metadata.get_upload(upload_id, bucket=BUCKET, key="k").initiated
    env.complete(upload_id, env.upload_parts(upload_id))
    assert env.row().last_modified == initiated
