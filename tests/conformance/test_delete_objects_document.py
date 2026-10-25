"""
The DeleteObjects document is validated whole, before anything is deleted.

Root Delete (S3 namespace or none), 1 to 1000 Objects each with exactly one Key, at
most one Quiet that is "true" or "false". The S3 API reference documents no error code
for a document that breaks these rules, so each is 400 MalformedXML (docs/
compatibility.md). A refusal anywhere in the document, even after valid entries, leaves
every key in place.
"""

import io
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.base import ObjectMeta
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "bkt"
NS = "http://s3.amazonaws.com/doc/2006-03-01/"
KEYS = ("k1", "k2", "k3")
VALID = "<Object><Key>k1</Key></Object><Object><Key>k2</Key></Object>"


@pytest.fixture
def deps(tmp_path: Path):
    return BlobStore(tmp_path / "blobs"), SQLiteMetadataStore(db_path=tmp_path / "meta.db")


@pytest.fixture
def client(deps) -> TestClient:
    storage, metadata = deps
    c = TestClient(create_app(storage=storage, metadata=metadata), raise_server_exceptions=False)
    assert c.put(f"/{BUCKET}").status_code == 200
    return c


def _seed(deps, key: str) -> None:
    storage, metadata = deps
    blob_id = metadata.register_blob("object", BUCKET, key)
    etag, size = storage.write_blob(blob_id, io.BytesIO(b"x"))
    metadata.put_object_meta(
        BUCKET,
        key,
        ObjectMeta(
            bucket=BUCKET,
            key=key,
            size=size,
            etag=etag,
            last_modified=datetime(2024, 1, 1),
            content_type="application/octet-stream",
            blob_id=blob_id,
        ),
    )


@pytest.fixture
def seeded(deps, client):
    for key in KEYS:
        _seed(deps, key)
    return deps


def _present(deps) -> list[str]:
    return [k for k in KEYS if deps[1].get_object_meta(BUCKET, k) is not None]


def _delete(client, body: str | bytes, path: str = f"/{BUCKET}?delete"):
    return client.post(path, content=body if isinstance(body, bytes) else body.encode())


def _doc(inner: str, root: str = "Delete") -> str:
    return f"<{root}>{inner}</{root}>"


def _assert_malformed(resp):
    assert resp.status_code == 400, resp.text
    assert ET.fromstring(resp.text).findtext("Code") == "MalformedXML"


# --- structure ---

NOT_A_DELETE_DOCUMENT = {
    "other root": _doc(VALID, "Foo"),
    "lowercase root": _doc(VALID, "delete"),
    "root named DeleteObjects": _doc(VALID, "DeleteObjects"),
    "root in another namespace": f'<Delete xmlns="http://example.com/">{VALID}</Delete>',
    "Objects in another namespace than the root": (
        f'<Delete xmlns="{NS}"><Object xmlns=""><Key>k1</Key></Object></Delete>'
    ),
    "Objects wrapped in another element": _doc(f"<Objects>{VALID}</Objects>"),
    "no Object": "<Delete/>",
    "no Object, only Quiet": _doc("<Quiet>true</Quiet>"),
    "Object without Key (after valid ones)": _doc(VALID + "<Object/>"),
    "Object with only a VersionId": _doc(VALID + "<Object><VersionId>null</VersionId></Object>"),
    "Object with only conditions": _doc(VALID + "<Object><ETag>x</ETag></Object>"),
    "Key in another namespace": (
        f'<Delete xmlns="{NS}"><Object><Key xmlns="">k1</Key></Object></Delete>'
    ),
    "two Keys in one Object": _doc(VALID + "<Object><Key>k3</Key><Key>k3</Key></Object>"),
    "Key holding an element": _doc("<Object><Key>k<b/>1</Key></Object>"),
    "VersionId holding an element": _doc(
        "<Object><Key>k1</Key><VersionId><b/></VersionId></Object>"
    ),
}


@pytest.mark.parametrize("body", NOT_A_DELETE_DOCUMENT.values(), ids=NOT_A_DELETE_DOCUMENT.keys())
def test_a_document_that_is_not_a_delete_document_is_refused_and_deletes_nothing(
    seeded, client, body
):
    _assert_malformed(_delete(client, body))
    assert _present(seeded) == list(KEYS)


def test_the_same_refusals_on_the_slash_path(seeded, client):
    _assert_malformed(_delete(client, "<Delete/>", f"/{BUCKET}/?delete"))
    assert _present(seeded) == list(KEYS)


def test_a_malformed_document_for_a_missing_bucket_is_still_no_such_bucket(client):
    resp = _delete(client, "<Delete/>", "/nope?delete")
    assert (resp.status_code, ET.fromstring(resp.text).findtext("Code")) == (404, "NoSuchBucket")


def test_the_root_may_be_in_the_s3_namespace_or_in_none(seeded, client):
    body = f'<Delete xmlns="{NS}"><Object><Key>k1</Key></Object></Delete>'
    assert _delete(client, body).status_code == 200
    assert _delete(client, _doc("<Object><Key>k2</Key></Object>")).status_code == 200
    assert _present(seeded) == ["k3"]


# --- the 1000 Object limit ---


def _objects(n: int, first: str = VALID) -> str:
    return first + "".join(f"<Object><Key>absent-{i}</Key></Object>" for i in range(n))


def test_exactly_1000_objects_are_accepted(seeded, client):
    resp = _delete(client, _doc(_objects(998)))  # k1, k2 and 998 absent keys
    assert resp.status_code == 200
    assert len(ET.fromstring(resp.text).findall("Deleted")) == 1000
    assert _present(seeded) == ["k3"]


def test_1001_objects_are_refused_and_delete_nothing(seeded, client):
    resp = _delete(client, _doc(_objects(999)))  # valid entries come first
    _assert_malformed(resp)
    assert _present(seeded) == list(KEYS)


def test_1001_objects_are_refused_on_the_slash_path(seeded, client):
    _assert_malformed(_delete(client, _doc(_objects(999)), f"/{BUCKET}/?delete"))
    assert _present(seeded) == list(KEYS)


def test_the_limit_counts_entries_not_distinct_keys(seeded, client):
    body = _doc("<Object><Key>k1</Key></Object>" * 1001)
    _assert_malformed(_delete(client, body))
    assert _present(seeded) == list(KEYS)


def test_the_limit_counts_entries_that_would_be_per_key_errors(seeded, client):
    body = _doc(VALID + "<Object><Key>v</Key><VersionId>v1</VersionId></Object>" * 999)
    _assert_malformed(_delete(client, body))
    assert _present(seeded) == list(KEYS)


# --- Quiet ---

BAD_QUIET = {
    "yes": "<Quiet>yes</Quiet>",
    "empty": "<Quiet></Quiet>",
    "self-closing": "<Quiet/>",
    "one": "<Quiet>1</Quiet>",
    "zero": "<Quiet>0</Quiet>",
    "capitalized": "<Quiet>True</Quiet>",
    "uppercase": "<Quiet>TRUE</Quiet>",
    "words": "<Quiet>true false</Quiet>",
    "element inside": "<Quiet><b>true</b></Quiet>",
    "twice": "<Quiet>true</Quiet><Quiet>true</Quiet>",
    "twice, differing": "<Quiet>true</Quiet><Quiet>false</Quiet>",
}


@pytest.mark.parametrize("quiet", BAD_QUIET.values(), ids=BAD_QUIET.keys())
def test_an_invalid_quiet_is_refused_wherever_it_stands_and_deletes_nothing(seeded, client, quiet):
    _assert_malformed(_delete(client, _doc(quiet + VALID)))
    _assert_malformed(_delete(client, _doc(VALID + quiet)))
    assert _present(seeded) == list(KEYS)


def test_quiet_true_lists_no_deleted_entries_and_deletes(seeded, client):
    resp = _delete(client, _doc(VALID + "<Quiet>true</Quiet>"))
    assert resp.status_code == 200
    root = ET.fromstring(resp.text)
    assert root.tag == "DeleteResult"
    assert len(root) == 0
    assert _present(seeded) == ["k3"]


@pytest.mark.parametrize("position", ["first", "last"])
def test_quiet_may_stand_before_or_after_the_objects(seeded, client, position):
    quiet = "<Quiet>true</Quiet>"
    resp = _delete(client, _doc(quiet + VALID if position == "first" else VALID + quiet))
    assert len(ET.fromstring(resp.text)) == 0
    assert _present(seeded) == ["k3"]


def test_quiet_true_in_the_s3_namespace_and_around_whitespace(seeded, client):
    body = f'<Delete xmlns="{NS}">{VALID}<Quiet> true\n</Quiet></Delete>'
    resp = _delete(client, body)
    assert resp.status_code == 200
    assert len(ET.fromstring(resp.text)) == 0
    assert _present(seeded) == ["k3"]


def test_quiet_true_still_lists_errors(seeded, client):
    body = _doc(
        "<Object><Key>k1</Key></Object>"
        "<Object><Key>k2</Key><VersionId>v1</VersionId></Object>"
        "<Object><Key>k3</Key><Size>1</Size></Object><Quiet>true</Quiet>"
    )
    resp = _delete(client, body)
    assert resp.status_code == 200
    root = ET.fromstring(resp.text)
    assert root.findall("Deleted") == []
    assert [(e.findtext("Key"), e.findtext("Code")) for e in root.findall("Error")] == [
        ("k2", "NotImplemented"),
        ("k3", "NotImplemented"),
    ]
    assert _present(seeded) == ["k2", "k3"]  # only k1 was deleted


def test_quiet_false_lists_every_key(seeded, client):
    resp = _delete(client, _doc(VALID + "<Quiet>false</Quiet>"))
    assert [d.findtext("Key") for d in ET.fromstring(resp.text).findall("Deleted")] == ["k1", "k2"]
    assert _present(seeded) == ["k3"]


def test_without_quiet_every_key_is_listed(seeded, client):
    resp = _delete(client, _doc(VALID))
    assert [d.findtext("Key") for d in ET.fromstring(resp.text).findall("Deleted")] == ["k1", "k2"]


# --- what is still accepted ---


def test_unknown_elements_are_ignored(seeded, client):
    body = _doc("<Mystery/><Object><Key>k1</Key><Extra>1</Extra></Object>" + "<Other/>")
    resp = _delete(client, body)
    assert resp.status_code == 200
    assert [d.findtext("Key") for d in ET.fromstring(resp.text).findall("Deleted")] == ["k1"]
    assert _present(seeded) == ["k2", "k3"]


def test_duplicate_keys_are_each_reported_and_deleted_once(seeded, client):
    body = _doc("<Object><Key>k1</Key></Object><Object><Key>k1</Key></Object>")
    resp = _delete(client, body)
    assert resp.status_code == 200
    assert [d.findtext("Key") for d in ET.fromstring(resp.text).findall("Deleted")] == ["k1", "k1"]
    assert _present(seeded) == ["k2", "k3"]


def test_an_empty_key_is_accepted_so_a_legacy_empty_key_object_can_go(client, deps):
    _seed(deps, "")
    _seed(deps, "k1")
    for body in (_doc("<Object><Key></Key></Object>"), _doc("<Object><Key/></Object>")):
        assert _delete(client, body).status_code == 200
    assert deps[1].get_object_meta(BUCKET, "") is None
    assert deps[1].get_object_meta(BUCKET, "k1") is not None


def test_version_id_null_is_the_current_object(seeded, client):
    resp = _delete(client, _doc("<Object><Key>k1</Key><VersionId>null</VersionId></Object>"))
    assert resp.status_code == 200
    deleted = ET.fromstring(resp.text).find("Deleted")
    assert (deleted.findtext("Key"), deleted.findtext("VersionId")) == ("k1", "null")
    assert _present(seeded) == ["k2", "k3"]


def test_another_version_id_is_a_per_key_error_and_the_other_keys_go(seeded, client):
    body = _doc(
        "<Object><Key>k1</Key><VersionId>v1</VersionId></Object><Object><Key>k2</Key></Object>"
    )
    root = ET.fromstring(_delete(client, body).text)
    assert [e.findtext("Code") for e in root.findall("Error")] == ["NotImplemented"]
    assert [d.findtext("Key") for d in root.findall("Deleted")] == ["k2"]
    assert _present(seeded) == ["k1", "k3"]
