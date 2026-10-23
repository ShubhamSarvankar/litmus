"""versionId on an unversioned bucket (every Litmus bucket is unversioned).

S3 gives every object in an unversioned bucket the version ID "null", and
ListObjectVersions reports it, so versionId=null must name the current object on GET,
HEAD and DELETE, in the query string and in a DeleteObjects body alike. Any other
version ID is answered with 501 NotImplemented and changes nothing.
"""

import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "test-bucket"
KEY = "obj.txt"
BODY = b"precious content"
OTHER_VERSION_IDS = ["abc", "3sL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY", "NULL", ""]


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    c = TestClient(create_app(storage=storage, metadata=metadata), raise_server_exceptions=False)
    assert c.put(f"/{BUCKET}").status_code == 200
    assert c.put(f"/{BUCKET}/{KEY}", content=BODY).status_code == 200
    return c


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def _assert_intact(client: TestClient) -> None:
    resp = client.get(f"/{BUCKET}/{KEY}")
    assert resp.status_code == 200
    assert resp.content == BODY


def _assert_not_implemented(resp, method: str) -> None:
    assert resp.status_code == 501
    if method != "HEAD":
        assert ET.fromstring(resp.text).findtext("Code") == "NotImplemented"


def _delete_objects(client: TestClient, objects: list[tuple[str, str | None]]):
    parts = []
    for key, version_id in objects:
        v = "" if version_id is None else f"<VersionId>{version_id}</VersionId>"
        parts.append(f"<Object><Key>{key}</Key>{v}</Object>")
    body = f"<Delete>{''.join(parts)}</Delete>".encode()
    resp = client.post(f"/{BUCKET}?delete", content=body)
    assert resp.status_code == 200
    return ET.fromstring(resp.text)


# --- the null version is the current object ---


def test_listed_version_id_is_null(client):
    root = ET.fromstring(client.get(f"/{BUCKET}?versions").text)
    assert [v.findtext("VersionId") for v in root.iter("Version")] == ["null"]


def test_get_null_version_returns_current_object(client):
    resp = client.get(f"/{BUCKET}/{KEY}?versionId=null")
    assert resp.status_code == 200
    assert resp.content == BODY
    assert resp.headers["etag"] == _md5(BODY)


def test_head_null_version_returns_current_object(client):
    resp = client.head(f"/{BUCKET}/{KEY}?versionId=null")
    assert resp.status_code == 200
    assert resp.headers["content-length"] == str(len(BODY))
    assert resp.headers["etag"] == _md5(BODY)


def test_delete_null_version_deletes_current_object(client):
    assert client.delete(f"/{BUCKET}/{KEY}?versionId=null").status_code == 204
    assert client.get(f"/{BUCKET}/{KEY}").status_code == 404


def test_list_versions_then_delete_each_listed_version(client):
    """What a cleanup loop does: list versions, then delete each by its version ID."""
    assert client.put(f"/{BUCKET}/second.txt", content=b"2").status_code == 200
    listing = ET.fromstring(client.get(f"/{BUCKET}?versions").text)
    for v in listing.iter("Version"):
        key, version_id = v.findtext("Key"), v.findtext("VersionId")
        assert client.delete(f"/{BUCKET}/{key}?versionId={version_id}").status_code == 204
    assert "<Version>" not in client.get(f"/{BUCKET}?versions").text


# --- any other version ID: 501, nothing changes ---


@pytest.mark.parametrize("version_id", OTHER_VERSION_IDS)
@pytest.mark.parametrize("method", ["GET", "HEAD", "DELETE"])
def test_other_version_id_returns_501(client, method, version_id):
    resp = client.request(method, f"/{BUCKET}/{KEY}", params={"versionId": version_id})
    _assert_not_implemented(resp, method)
    _assert_intact(client)


def test_null_and_other_version_id_together_returns_501(client):
    resp = client.delete(f"/{BUCKET}/{KEY}?versionId=null&versionId=abc")
    _assert_not_implemented(resp, "DELETE")
    _assert_intact(client)


@pytest.mark.parametrize("version_id", ["null", "abc"])
@pytest.mark.parametrize("method", ["PUT", "POST"])
def test_version_id_on_put_and_post_returns_501(client, method, version_id):
    resp = client.request(method, f"/{BUCKET}/{KEY}?versionId={version_id}", content=b"new")
    _assert_not_implemented(resp, method)
    _assert_intact(client)


# --- DeleteObjects body: same rule as the single-object DELETE ---


def test_delete_objects_without_version_id_deletes(client):
    root = _delete_objects(client, [(KEY, None)])
    deleted = root.findall("Deleted")
    assert [d.findtext("Key") for d in deleted] == [KEY]
    assert deleted[0].find("VersionId") is None
    assert client.get(f"/{BUCKET}/{KEY}").status_code == 404


def test_delete_objects_null_version_deletes_and_echoes_version(client):
    root = _delete_objects(client, [(KEY, "null")])
    deleted = root.findall("Deleted")
    assert [(d.findtext("Key"), d.findtext("VersionId")) for d in deleted] == [(KEY, "null")]
    assert root.findall("Error") == []
    assert client.get(f"/{BUCKET}/{KEY}").status_code == 404


@pytest.mark.parametrize("version_id", [v for v in OTHER_VERSION_IDS if v])
def test_delete_objects_other_version_reports_error_and_keeps_object(client, version_id):
    root = _delete_objects(client, [(KEY, version_id)])
    assert root.findall("Deleted") == []
    errors = root.findall("Error")
    assert [(e.findtext("Key"), e.findtext("VersionId"), e.findtext("Code")) for e in errors] == [
        (KEY, version_id, "NotImplemented")
    ]
    _assert_intact(client)


@pytest.mark.parametrize("version_id", ["", "abc"])
def test_delete_objects_namespaced_body_other_version_keeps_object(client, version_id):
    """boto3 sends the body in the S3 namespace; an empty or other VersionId there must
    still be an error, not be read as "no VersionId"."""
    ns = "http://s3.amazonaws.com/doc/2006-03-01/"
    body = (
        f'<Delete xmlns="{ns}"><Object><Key>{KEY}</Key>'
        f"<VersionId>{version_id}</VersionId></Object></Delete>"
    ).encode()
    root = ET.fromstring(client.post(f"/{BUCKET}?delete", content=body).text)
    assert root.findall("Deleted") == []
    assert [e.findtext("Code") for e in root.findall("Error")] == ["NotImplemented"]
    _assert_intact(client)


def test_delete_objects_namespaced_body_null_version_deletes(client):
    ns = "http://s3.amazonaws.com/doc/2006-03-01/"
    body = (
        f'<Delete xmlns="{ns}"><Object><Key>{KEY}</Key><VersionId>null</VersionId></Object>'
        f"</Delete>"
    ).encode()
    root = ET.fromstring(client.post(f"/{BUCKET}?delete", content=body).text)
    assert [d.findtext("Key") for d in root.findall("Deleted")] == [KEY]
    assert client.get(f"/{BUCKET}/{KEY}").status_code == 404


def test_delete_objects_mixed_batch(client):
    assert client.put(f"/{BUCKET}/a.txt", content=b"a").status_code == 200
    assert client.put(f"/{BUCKET}/b.txt", content=b"b").status_code == 200
    root = _delete_objects(client, [("a.txt", "null"), (KEY, "abc"), ("b.txt", None)])
    assert sorted(d.findtext("Key") for d in root.findall("Deleted")) == ["a.txt", "b.txt"]
    assert [e.findtext("Key") for e in root.findall("Error")] == [KEY]
    assert client.get(f"/{BUCKET}/a.txt").status_code == 404
    assert client.get(f"/{BUCKET}/b.txt").status_code == 404
    _assert_intact(client)
