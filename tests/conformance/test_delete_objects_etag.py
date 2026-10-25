"""
Tier A: per key ETag conditions in DeleteObjects (docs/design/storage-layout.md §10).

The ETag element of an Object is read as an If-Match value (`*`, quoted, unquoted,
lists, strong comparison) and decided inside the transaction that deletes that key,
against that key's current row. A key whose condition holds is Deleted. One whose
condition fails is an Error with Code PreconditionFailed and is left untouched. One that
does not exist is an Error with Code NoSuchKey; without a condition a missing key is
still Deleted. An empty ETag is no condition. A refused key never rolls back the others.
LastModifiedTime and Size (directory buckets only in AWS) stay a per key NotImplemented.
The batch answers 200 whatever the per key results; 409 is never emitted.
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

BUCKET = "cond-delete-objects"
KEY = "obj.txt"
URL = f"/{BUCKET}/{KEY}"
BODY = b"precious content"
NS = "http://s3.amazonaws.com/doc/2006-03-01/"


def _etag(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


ETAG = _etag(BODY)
BARE = ETAG.strip('"')
OTHER = '"0123456789abcdef0123456789abcdef"'

HOLDS = [
    pytest.param("*", id="star"),
    pytest.param(ETAG, id="quoted"),
    pytest.param(BARE, id="unquoted"),
    pytest.param(f"{OTHER}, {ETAG}", id="list-with-a-match"),
    pytest.param(f"{OTHER},{BARE}", id="unquoted-list-with-a-match"),
    pytest.param(f"  {ETAG}  ", id="padded"),
]
FAILS = [
    pytest.param(OTHER, id="other-quoted"),
    pytest.param("badetag", id="other-unquoted"),
    pytest.param(f"W/{ETAG}", id="weak"),
    pytest.param(f"{OTHER}, W/{ETAG}", id="list-with-only-a-weak-match"),
    pytest.param(f'{OTHER}, "feedfacefeedfacefeedfacefeedface"', id="list-without-a-match"),
    pytest.param(ETAG[:-2] + '"', id="truncated"),
]
NO_CONDITION = [
    pytest.param("", id="empty"),
    pytest.param("   ", id="blank"),
]


def _entry(key: str, etag: str | None = None, extra: str = "") -> str:
    tag = "" if etag is None else f"<ETag>{etag}</ETag>"
    return f"<Object><Key>{key}</Key>{tag}{extra}</Object>"


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

    def put(self, key: str, body: bytes) -> None:
        assert self.client.put(f"/{BUCKET}/{key}", content=body).status_code == 200

    def delete_objects(self, *entries: str, quiet: bool | None = None, namespaced: bool = True):
        """The DeleteResult of a batch, which is always 200. Returns (Deleted keys,
        [(Key, Code)] of the Errors, root)."""
        xmlns = f' xmlns="{NS}"' if namespaced else ""
        q = "" if quiet is None else f"<Quiet>{'true' if quiet else 'false'}</Quiet>"
        body = f"<Delete{xmlns}>{''.join(entries)}{q}</Delete>".encode()
        resp = self.client.post(f"/{BUCKET}?delete", content=body)
        assert resp.status_code == 200
        root = ET.fromstring(resp.text)
        assert root.tag == "DeleteResult"
        deleted = [d.findtext("Key") for d in root.findall("Deleted")]
        errors = [(e.findtext("Key"), e.findtext("Code")) for e in root.findall("Error")]
        return deleted, errors, root

    def state(self):
        """Every row, the registry and the blob files: everything a delete may change."""
        keys = [m.key for m in self.metadata.list_objects(BUCKET)]
        return (
            [self.metadata.get_object_meta(BUCKET, k) for k in keys],
            self.metadata.list_blob_records(),
            sorted(blob_id for _, blob_id in self.storage.scan()),
        )

    def assert_kept(self, key: str, body: bytes, head) -> None:
        """key reads back as it was: same bytes, ETag and Last-Modified."""
        got = self.client.get(f"/{BUCKET}/{key}")
        assert got.status_code == 200 and got.content == body
        assert got.headers["etag"] == head["etag"] == _etag(body)
        assert got.headers["last-modified"] == head["last-modified"]

    def assert_clean(self, live: int) -> None:
        """No blob went to garbage, nothing is left in temp, and the sweep has no work."""
        assert [r.state for r in self.metadata.list_blob_records()] == ["live"] * live
        assert len(list(self.storage.scan())) == live
        assert self.storage.temp_entries() == []
        assert run_consistency_sweep(self.metadata, self.storage).issues == []

    def assert_refused_and_untouched(self, before, head) -> None:
        assert self.state() == before
        self.assert_kept(KEY, BODY, head)
        self.assert_clean(live=1)

    def head(self, key: str = KEY):
        return self.client.head(f"/{BUCKET}/{key}").headers


@pytest.fixture
def env(tmp_path: Path) -> Env:
    return Env(tmp_path)


def _assert_deleted_like_unconditional(env: Env) -> None:
    assert env.client.get(URL).status_code == 404
    assert env.metadata.get_object_meta(BUCKET, KEY) is None
    assert env.metadata.list_blob_records() == []
    assert list(env.storage.scan()) == []
    env.assert_clean(live=0)


# --- one key, every ETag value of test_conditional_delete_object.py ---


@pytest.mark.parametrize("namespaced", [True, False])
@pytest.mark.parametrize("value", HOLDS)
def test_etag_that_holds_deletes_the_object(env, value, namespaced):
    deleted, errors, root = env.delete_objects(_entry(KEY, value), namespaced=namespaced)
    assert (deleted, errors) == ([KEY], [])
    assert root.find("Deleted").find("ETag") is None  # nothing but the Key is echoed
    _assert_deleted_like_unconditional(env)


@pytest.mark.parametrize("value", NO_CONDITION)
def test_empty_etag_is_no_condition(env, value):
    deleted, errors, _ = env.delete_objects(_entry(KEY, value))
    assert (deleted, errors) == ([KEY], [])
    _assert_deleted_like_unconditional(env)


def test_self_closing_etag_is_no_condition(env):
    deleted, errors, _ = env.delete_objects(f"<Object><Key>{KEY}</Key><ETag/></Object>")
    assert (deleted, errors) == ([KEY], [])
    _assert_deleted_like_unconditional(env)


@pytest.mark.parametrize("namespaced", [True, False])
@pytest.mark.parametrize("value", FAILS)
def test_etag_that_fails_is_precondition_failed_and_changes_nothing(env, value, namespaced):
    before, head = env.state(), env.head()
    deleted, errors, root = env.delete_objects(_entry(KEY, value), namespaced=namespaced)
    assert (deleted, errors) == ([], [(KEY, "PreconditionFailed")])
    assert root.find("Error").findtext("Message")
    env.assert_refused_and_untouched(before, head)
    # The same entry without the condition then goes through.
    assert env.delete_objects(_entry(KEY))[:2] == ([KEY], [])
    _assert_deleted_like_unconditional(env)


def test_a_second_etag_element_is_ignored(env):
    """Repeated ETag elements past the first are ignored, as for VersionId and Size."""
    before, head = env.state(), env.head()
    entry = f"<Object><Key>{KEY}</Key><ETag>{OTHER}</ETag><ETag>{ETAG}</ETag></Object>"
    assert env.delete_objects(entry)[:2] == ([], [(KEY, "PreconditionFailed")])
    env.assert_refused_and_untouched(before, head)


@pytest.mark.parametrize("value", ["*", ETAG, OTHER, "badetag", f"W/{ETAG}"])
def test_etag_on_a_missing_key_is_no_such_key_and_changes_nothing(env, value):
    before, head = env.state(), env.head()
    deleted, errors, root = env.delete_objects(_entry("missing", value))
    assert (deleted, errors) == ([], [("missing", "NoSuchKey")])
    assert root.find("Error").findtext("Message")
    env.assert_refused_and_untouched(before, head)


@pytest.mark.parametrize("value", [None, "", "   "])
def test_a_missing_key_without_a_condition_is_still_deleted(env, value):
    before, head = env.state(), env.head()
    deleted, errors, _ = env.delete_objects(_entry("missing", value))
    assert (deleted, errors) == (["missing"], [])
    env.assert_refused_and_untouched(before, head)  # nothing else changed


def test_etag_on_a_key_deleted_earlier_is_no_such_key(env):
    assert env.delete_objects(_entry(KEY, ETAG))[:2] == ([KEY], [])
    assert env.delete_objects(_entry(KEY, ETAG))[:2] == ([], [(KEY, "NoSuchKey")])
    assert env.delete_objects(_entry(KEY))[:2] == ([KEY], [])


def test_the_same_key_twice_in_one_batch_is_decided_in_order(env):
    """Entries are decided one after another in the one transaction: the first one's
    delete is what the second one sees."""
    deleted, errors, _ = env.delete_objects(_entry(KEY, ETAG), _entry(KEY, ETAG))
    assert (deleted, errors) == ([KEY], [(KEY, "NoSuchKey")])
    _assert_deleted_like_unconditional(env)


def test_a_failing_entry_before_a_passing_entry_for_the_same_key(env):
    deleted, errors, _ = env.delete_objects(_entry(KEY, OTHER), _entry(KEY, ETAG))
    assert (deleted, errors) == ([KEY], [(KEY, "PreconditionFailed")])
    _assert_deleted_like_unconditional(env)


def test_etag_with_the_null_version_id_is_evaluated_and_echoes_the_version(env):
    before, head = env.state(), env.head()
    version = "<VersionId>null</VersionId>"
    _, _, root = env.delete_objects(_entry(KEY, OTHER, version))
    error = root.find("Error")
    assert (error.findtext("Key"), error.findtext("VersionId"), error.findtext("Code")) == (
        KEY,
        "null",
        "PreconditionFailed",
    )
    env.assert_refused_and_untouched(before, head)
    _, _, root = env.delete_objects(_entry(KEY, ETAG, version))
    deleted = root.find("Deleted")
    assert (deleted.findtext("Key"), deleted.findtext("VersionId")) == (KEY, "null")
    _assert_deleted_like_unconditional(env)


def test_etag_with_another_version_id_is_not_implemented_and_keeps_the_object(env):
    before, head = env.state(), env.head()
    _, errors, _ = env.delete_objects(_entry(KEY, ETAG, "<VersionId>v1</VersionId>"))
    assert errors == [(KEY, "NotImplemented")]
    env.assert_refused_and_untouched(before, head)


def test_etag_on_an_overwritten_object_is_decided_on_the_current_etag(env):
    new = b"replacement"
    env.put(KEY, new)
    before, head = env.state(), env.head()
    assert env.delete_objects(_entry(KEY, ETAG))[:2] == ([], [(KEY, "PreconditionFailed")])
    assert env.state() == before
    env.assert_kept(KEY, new, head)
    env.assert_clean(live=1)
    assert env.delete_objects(_entry(KEY, _etag(new)))[:2] == ([KEY], [])
    assert env.client.get(URL).status_code == 404


def test_a_refusal_keeps_last_modified_and_the_listing(env):
    head, before = env.head(), env.state()
    env.delete_objects(_entry(KEY, OTHER))
    after = env.head()
    assert (after["etag"], after["last-modified"], after["content-length"]) == (
        head["etag"],
        head["last-modified"],
        head["content-length"],
    )
    assert env.state() == before
    listing = ET.fromstring(env.client.get(f"/{BUCKET}?list-type=2").text)
    assert [e.text for e in listing.iter() if e.tag.rpartition("}")[2] == "Key"] == [KEY]


def test_etag_on_a_missing_bucket_is_still_no_such_bucket(env):
    body = b"<Delete><Object><Key>k</Key><ETag>*</ETag></Object></Delete>"
    resp = env.client.post("/no-such-bucket?delete", content=body)
    assert resp.status_code == 404
    assert ET.fromstring(resp.text).findtext("Code") == "NoSuchBucket"


# --- a mixed batch ---

MIXED_KEYS = {
    "pass": b"pass body",
    "fail": b"fail body",
    "plain": b"plain body",
}


def _mixed(env: Env):
    for key, body in MIXED_KEYS.items():
        env.put(key, body)
    heads = {key: env.head(key) for key in MIXED_KEYS}
    request = (
        _entry("pass", _etag(MIXED_KEYS["pass"])),
        _entry("fail", OTHER),
        _entry("missing-with-condition", "*"),
        _entry("missing-plain"),
        _entry("plain"),
    )
    return heads, request


def test_mixed_batch_reports_each_key_and_changes_only_what_it_should(env):
    heads, request = _mixed(env)
    # KEY is not in the batch: it must come through untouched too.
    deleted, errors, root = env.delete_objects(*request)
    assert [c.tag for c in root] == ["Deleted", "Error", "Error", "Deleted", "Deleted"]
    assert deleted == ["pass", "missing-plain", "plain"]
    assert errors == [("fail", "PreconditionFailed"), ("missing-with-condition", "NoSuchKey")]
    # Deleted: pass and plain. Untouched: fail (condition failed) and KEY.
    assert env.client.get(f"/{BUCKET}/pass").status_code == 404
    assert env.client.get(f"/{BUCKET}/plain").status_code == 404
    env.assert_kept("fail", MIXED_KEYS["fail"], heads["fail"])
    env.assert_kept(KEY, BODY, env.head())
    assert env.client.get(f"/{BUCKET}/missing-with-condition").status_code == 404
    assert env.metadata.get_object_meta(BUCKET, "missing-with-condition") is None
    assert env.metadata.get_object_meta(BUCKET, "missing-plain") is None
    assert [m.key for m in env.metadata.list_objects(BUCKET)] == ["fail", KEY]
    env.assert_clean(live=2)


def test_mixed_batch_results_come_in_request_order(env):
    _, request = _mixed(env)
    _, _, root = env.delete_objects(*request)
    assert [(c.tag, c.findtext("Key")) for c in root] == [
        ("Deleted", "pass"),
        ("Error", "fail"),
        ("Error", "missing-with-condition"),
        ("Deleted", "missing-plain"),
        ("Deleted", "plain"),
    ]


def test_mixed_batch_in_quiet_mode_lists_every_error_and_no_deleted(env):
    heads, request = _mixed(env)
    deleted, errors, root = env.delete_objects(*request, quiet=True)
    assert deleted == []
    assert errors == [("fail", "PreconditionFailed"), ("missing-with-condition", "NoSuchKey")]
    assert [c.tag for c in root] == ["Error", "Error"]
    assert env.client.get(f"/{BUCKET}/pass").status_code == 404
    assert env.client.get(f"/{BUCKET}/plain").status_code == 404
    env.assert_kept("fail", MIXED_KEYS["fail"], heads["fail"])
    env.assert_kept(KEY, BODY, env.head())
    assert env.metadata.get_object_meta(BUCKET, "missing-with-condition") is None
    env.assert_clean(live=2)


def test_quiet_mode_with_every_condition_holding_is_an_empty_result(env):
    deleted, errors, root = env.delete_objects(_entry(KEY, ETAG), _entry("gone"), quiet=True)
    assert (deleted, errors, len(root)) == ([], [], 0)
    _assert_deleted_like_unconditional(env)


def test_a_failing_key_does_not_roll_back_the_batch(env):
    """One transaction holds the whole batch; a refused key must not undo the others,
    wherever it stands in the request."""
    for n in range(5):
        env.put(f"k{n}", f"body {n}".encode())
    entries = [_entry(f"k{n}", OTHER if n in (0, 2, 4) else None) for n in range(5)]
    deleted, errors, _ = env.delete_objects(*entries)
    assert deleted == ["k1", "k3"]
    assert errors == [(f"k{n}", "PreconditionFailed") for n in (0, 2, 4)]
    assert [m.key for m in env.metadata.list_objects(BUCKET)] == ["k0", "k2", "k4", KEY]
    env.assert_clean(live=4)


# --- LastModifiedTime and Size ---

LAST_MODIFIED = "<LastModifiedTime>2020-01-01T00:00:00.000Z</LastModifiedTime>"
SIZE = f"<Size>{len(BODY)}</Size>"


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param(LAST_MODIFIED, id="last-modified-time"),
        pytest.param(SIZE, id="size"),
        pytest.param(LAST_MODIFIED + SIZE, id="both"),
    ],
)
@pytest.mark.parametrize(
    "etag",
    [
        pytest.param(None, id="alone"),
        pytest.param(ETAG, id="with-a-passing-etag"),
        pytest.param(OTHER, id="with-a-failing-etag"),
        pytest.param("", id="with-an-empty-etag"),
    ],
)
def test_last_modified_time_and_size_stay_not_implemented_and_delete_nothing(env, extra, etag):
    before, head = env.state(), env.head()
    deleted, errors, _ = env.delete_objects(_entry(KEY, etag, extra))
    assert (deleted, errors) == ([], [(KEY, "NotImplemented")])
    env.assert_refused_and_untouched(before, head)


def test_not_implemented_key_beside_etag_keys_leaves_the_others_to_their_conditions(env):
    env.put("other", b"o")
    other_head = env.head("other")
    deleted, errors, _ = env.delete_objects(
        _entry(KEY, ETAG, SIZE), _entry("other", OTHER), _entry("gone", "*")
    )
    assert deleted == []
    assert errors == [
        (KEY, "NotImplemented"),
        ("other", "PreconditionFailed"),
        ("gone", "NoSuchKey"),
    ]
    env.assert_kept(KEY, BODY, env.head())
    env.assert_kept("other", b"o", other_head)
    env.assert_clean(live=2)


def test_not_implemented_in_quiet_mode_is_still_listed(env):
    _, errors, _ = env.delete_objects(_entry(KEY, ETAG, SIZE), quiet=True)
    assert errors == [(KEY, "NotImplemented")]
    env.assert_kept(KEY, BODY, env.head())


# --- what does not change ---


def test_etag_alone_is_not_a_valid_object(env):
    body = f"<Delete><Object><ETag>{ETAG}</ETag></Object></Delete>".encode()
    before = env.state()
    resp = env.client.post(f"/{BUCKET}?delete", content=body)
    assert resp.status_code == 400
    assert ET.fromstring(resp.text).findtext("Code") == "MalformedXML"
    assert env.state() == before


def test_no_409_for_a_batch_of_losing_conditions(env):
    entries = [_entry(KEY, OTHER)] * 5
    deleted, errors, _ = env.delete_objects(*entries)
    assert deleted == [] and [c for _, c in errors] == ["PreconditionFailed"] * 5
