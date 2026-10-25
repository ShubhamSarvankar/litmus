"""write_precondition() and the commit that calls it (docs/design/storage-layout.md §10)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from starlette.datastructures import Headers

from server.api.conditions import write_precondition
from server.errors.s3errors import NoSuchKey, PreconditionFailed
from server.metadata.base import ObjectMeta
from server.metadata.sqlite import SQLiteMetadataStore

ETAG = '"abc"'


def _check(headers: dict, current: str | None):
    check = write_precondition(Headers(headers), "k")
    assert check is not None
    check(current)


def test_no_headers_is_no_precondition():
    assert write_precondition(Headers({}), "k") is None
    assert write_precondition(Headers({"if-match": " ", "if-none-match": ""}), "k") is None


@pytest.mark.parametrize(
    "headers,current",
    [
        ({"if-match": "*"}, ETAG),
        ({"if-match": ETAG}, ETAG),
        ({"if-match": "abc"}, ETAG),
        ({"if-match": '"x", "abc"'}, ETAG),
        ({"if-none-match": "*"}, None),
        ({"if-none-match": ETAG}, None),
        ({"if-none-match": '"x"'}, ETAG),
        ({"if-match": ETAG, "if-none-match": '"x"'}, ETAG),
    ],
)
def test_holds(headers, current):
    _check(headers, current)


@pytest.mark.parametrize(
    "headers,current,error",
    [
        ({"if-match": "*"}, None, NoSuchKey),
        ({"if-match": '"x"'}, None, NoSuchKey),
        ({"if-match": '"x"'}, ETAG, PreconditionFailed),
        ({"if-match": f"W/{ETAG}"}, ETAG, PreconditionFailed),  # strong comparison
        ({"if-none-match": "*"}, ETAG, PreconditionFailed),
        ({"if-none-match": "abc"}, ETAG, PreconditionFailed),
        ({"if-none-match": f"W/{ETAG}"}, ETAG, PreconditionFailed),  # weak comparison
        ({"if-match": "*", "if-none-match": "*"}, None, NoSuchKey),  # If-Match first
        ({"if-match": "*", "if-none-match": "*"}, ETAG, PreconditionFailed),
    ],
)
def test_fails(headers, current, error):
    with pytest.raises(error):
        _check(headers, current)


def test_no_such_key_names_the_key():
    with pytest.raises(NoSuchKey) as caught:
        _check({"if-match": "*"}, None)
    assert caught.value.extra == {"Key": "k"}


# --- the commit ---

_WHEN = datetime(2024, 1, 1, 12, 0, 0)


@pytest.fixture
def store(tmp_path: Path) -> SQLiteMetadataStore:
    s = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    s.create_bucket("b", _WHEN)
    return s


def _obj(store, etag: str, last_modified=_WHEN) -> ObjectMeta:
    blob_id = store.register_blob("object", "b", "k")
    return ObjectMeta("b", "k", 1, etag, last_modified, "t", blob_id)


def test_the_precondition_sees_the_row_being_replaced(store):
    seen = []
    store.put_object_meta("b", "k", _obj(store, '"one"'), precondition=seen.append)
    store.put_object_meta("b", "k", _obj(store, '"two"'), precondition=seen.append)
    assert seen == [None, '"one"']


def test_a_failed_precondition_rolls_the_commit_back(store):
    store.put_object_meta("b", "k", _obj(store, '"one"'))
    before = store.get_object_meta("b", "k")
    new = _obj(store, '"two"')

    def refuse(_etag):
        raise PreconditionFailed()

    with pytest.raises(PreconditionFailed):
        store.put_object_meta("b", "k", new, precondition=refuse)
    assert store.get_object_meta("b", "k") == before
    # The new blob is still pending (the handler discards it); the old one is live.
    assert {r.blob_id: r.state for r in store.list_blob_records()} == {
        before.blob_id: "live",
        new.blob_id: "pending",
    }


def test_last_modified_none_is_stamped_in_the_commit_and_a_given_one_is_kept(store):
    before = datetime.now(UTC).replace(tzinfo=None)
    stamped = _obj(store, '"one"', last_modified=None)
    store.put_object_meta("b", "k", stamped)
    row = store.get_object_meta("b", "k")
    assert row.last_modified == stamped.last_modified
    assert abs((row.last_modified - before).total_seconds()) < 60

    store.put_object_meta("b", "k", _obj(store, '"two"'))
    assert store.get_object_meta("b", "k").last_modified == _WHEN
