import random
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pytest

from server.metadata.base import ObjectMeta, PartMeta
from server.metadata.sqlite import (
    SQLiteMetadataStore,
    common_prefix,
    key_range_query,
    key_successor,
)

_NOW = datetime(2024, 1, 1, 12, 0, 0, 0)


@pytest.fixture
def store(tmp_path: Path) -> SQLiteMetadataStore:
    return SQLiteMetadataStore(tmp_path / "test.db")


# --- Buckets ---


def test_create_and_list_bucket(store):
    store.create_bucket("my-bucket", _NOW)
    buckets = store.list_buckets()
    assert len(buckets) == 1
    assert buckets[0].name == "my-bucket"
    assert buckets[0].created_at == _NOW


def test_bucket_exists_true(store):
    store.create_bucket("b", _NOW)
    assert store.bucket_exists("b") is True


def test_bucket_exists_false(store):
    assert store.bucket_exists("no-such") is False


def test_list_buckets_sorted(store):
    store.create_bucket("z-bucket", _NOW)
    store.create_bucket("a-bucket", _NOW)
    names = [b.name for b in store.list_buckets()]
    assert names == ["a-bucket", "z-bucket"]


def test_delete_bucket(store):
    store.create_bucket("b", _NOW)
    store.delete_bucket("b")
    assert store.bucket_exists("b") is False


def test_list_buckets_empty(store):
    assert store.list_buckets() == []


# --- Objects ---


def _obj(bucket="b", key="k", size=10, etag='"abc"', content_type="text/plain", blob_id=None):
    return ObjectMeta(
        bucket=bucket,
        key=key,
        size=size,
        etag=etag,
        last_modified=_NOW,
        content_type=content_type,
        blob_id=blob_id,
    )


def _blob(store, key="k"):
    """A registered (pending) blob ID for an object row to point at."""
    return store.register_blob("object", "b", key)


def _part_blob(store, part_number, upload_id="uid-1"):
    """A registered (pending) blob ID for a part row to point at."""
    return store.register_blob("part", "b", "k", upload_id=upload_id, part_number=part_number)


def test_put_and_get_object(store):
    store.create_bucket("b", _NOW)
    meta = _obj(blob_id=_blob(store))
    store.put_object_meta("b", "k", meta)
    result = store.get_object_meta("b", "k")
    assert result is not None
    assert result.etag == '"abc"'
    assert result.size == 10
    assert result.content_type == "text/plain"


def test_get_object_missing_returns_none(store):
    store.create_bucket("b", _NOW)
    assert store.get_object_meta("b", "no-key") is None


def test_put_object_overwrites(store):
    store.create_bucket("b", _NOW)
    store.put_object_meta("b", "k", _obj(size=10, etag='"old"', blob_id=_blob(store)))
    store.put_object_meta("b", "k", _obj(size=99, etag='"new"', blob_id=_blob(store)))
    result = store.get_object_meta("b", "k")
    assert result.size == 99
    assert result.etag == '"new"'


def test_delete_object_meta(store):
    store.create_bucket("b", _NOW)
    store.put_object_meta("b", "k", _obj(blob_id=_blob(store)))
    store.delete_object_meta("b", "k")
    assert store.get_object_meta("b", "k") is None


def test_list_objects(store):
    store.create_bucket("b", _NOW)
    store.put_object_meta("b", "key1", _obj(key="key1", blob_id=_blob(store, "key1")))
    store.put_object_meta("b", "key2", _obj(key="key2", blob_id=_blob(store, "key2")))
    objects = store.list_objects("b")
    assert len(objects) == 2
    assert {o.key for o in objects} == {"key1", "key2"}


def test_list_objects_with_prefix(store):
    store.create_bucket("b", _NOW)
    store.put_object_meta("b", "logs/a", _obj(key="logs/a", blob_id=_blob(store, "logs/a")))
    store.put_object_meta("b", "logs/b", _obj(key="logs/b", blob_id=_blob(store, "logs/b")))
    store.put_object_meta("b", "data/c", _obj(key="data/c", blob_id=_blob(store, "data/c")))
    result = store.list_objects("b", prefix="logs/")
    assert len(result) == 2
    assert all(o.key.startswith("logs/") for o in result)


def test_list_objects_empty_bucket(store):
    store.create_bucket("b", _NOW)
    assert store.list_objects("b") == []


# --- Listing: prefix as a key range (docs/design/storage-layout.md §13) ---


def _put_keys(store, keys, bucket="b"):
    if not store.bucket_exists(bucket):
        store.create_bucket(bucket, _NOW)
    for key in keys:
        blob_id = store.register_blob("object", bucket, key)
        store.put_object_meta(bucket, key, _obj(bucket=bucket, key=key, blob_id=blob_id))


def _keys(objects):
    return [o.key for o in objects]


def _utf8_sorted(keys):
    return sorted(keys, key=lambda k: k.encode("utf-8"))


@pytest.mark.parametrize(
    ("prefix", "successor"),
    [
        ("", None),
        ("a", "b"),
        ("abc", "abd"),
        ("a/", "a0"),
        ("\x00", "\x01"),
        ("\x7f", "\x80"),
        ("é", "ê"),
        ("日本", "日本"[:-1] + chr(ord("本") + 1)),
        ("\uffff", "\U00010000"),
        ("x\U0001f600", "x\U0001f601"),
        # The next code point is a surrogate: skip to U+E000.
        ("\ud7ff", "\ue000"),
        ("ab\ud7ff", "ab\ue000"),
        # Trailing U+10FFFF has no next code point: drop it and increment before it.
        ("a\U0010ffff", "b"),
        ("ab\U0010ffff\U0010ffff", "ac"),
        ("\ud7ff\U0010ffff", "\ue000"),
        # Nothing but U+10FFFF: no upper bound.
        ("\U0010ffff", None),
        ("\U0010ffff\U0010ffff", None),
    ],
)
def test_key_successor(prefix, successor):
    assert key_successor(prefix) == successor


def test_key_successor_bounds_every_extension():
    # Every key that starts with the prefix sorts below the successor, in UTF-8 bytes,
    # and the successor itself does not start with the prefix.
    extensions = ["", "\x00", "a", "\x7f", "\uffff", "\U0010ffff", "\U0010ffff" * 3]
    for prefix in ["a", "é", "\ud7ff", "a\U0010ffff", "日\U0010ffff\U0010ffff"]:
        upper = key_successor(prefix)
        assert not upper.startswith(prefix)
        assert upper.encode("utf-8") > prefix.encode("utf-8")
        for ext in extensions:
            assert (prefix + ext).encode("utf-8") < upper.encode("utf-8")


def test_database_is_utf8_and_key_is_binary(store, tmp_path):
    conn = sqlite3.connect(tmp_path / "test.db")
    try:
        assert conn.execute("PRAGMA encoding").fetchone()[0] == "UTF-8"
        (pk,) = [r[1] for r in conn.execute("PRAGMA index_list('objects')") if r[3] == "pk"]
        columns = {r[2]: r[4] for r in conn.execute(f"PRAGMA index_xinfo('{pk}')") if r[5]}
        assert columns == {"bucket": "BINARY", "key": "BINARY"}
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("prefix", "start_after", "limit", "search"),
    [
        ("", None, None, "(bucket=? AND key>?)"),
        ("logs/", None, None, "(bucket=? AND key>? AND key<?)"),
        ("logs/", "logs/a", 10, "(bucket=? AND key>? AND key<?)"),
        ("\U0010ffff", None, 1, "(bucket=? AND key>?)"),
    ],
)
def test_key_range_query_uses_primary_key(store, tmp_path, prefix, start_after, limit, search):
    sql, params = key_range_query("b", prefix, start_after, limit)
    assert "LIKE" not in sql
    conn = sqlite3.connect(tmp_path / "test.db")
    try:
        plan = [r[3] for r in conn.execute("EXPLAIN QUERY PLAN " + sql, params)]
    finally:
        conn.close()
    # SQLite prints key>=? as key>?. No temp b-tree: rows come out in index order.
    assert plan == [f"SEARCH objects USING PRIMARY KEY {search}"]


def test_list_prefix_is_not_a_wildcard(store):
    _put_keys(store, ["a_c", "abc", "a%c", "aXc", "a%", "a_"])
    assert _keys(store.list_objects("b", prefix="a_")) == ["a_", "a_c"]
    assert _keys(store.list_objects("b", prefix="a%")) == ["a%", "a%c"]


def test_list_prefix_is_case_sensitive(store):
    _put_keys(store, ["Foo/1", "foo/2", "FOO/3", "fOo/4"])
    assert _keys(store.list_objects("b", prefix="foo/")) == ["foo/2"]
    assert _keys(store.list_objects("b", prefix="F")) == ["FOO/3", "Foo/1"]


def test_list_prefix_non_ascii(store):
    keys = ["é/1", "é/2", "e/3", "ê", "日本/x", "日本語", "日", "\U0001f600a", "\U0001f601"]
    _put_keys(store, keys)
    assert _keys(store.list_objects("b", prefix="é")) == ["é/1", "é/2"]
    assert _keys(store.list_objects("b", prefix="日本")) == ["日本/x", "日本語"]
    assert _keys(store.list_objects("b", prefix="\U0001f600")) == ["\U0001f600a"]


def test_list_prefix_at_code_point_edges(store):
    keys = ["\ud7ff", "\ud7ffz", "\ue000", "\uffff", "\U00010000", "a\U0010ffff", "b"]
    keys += ["a\U0010ffff\U0010ffff", "\U0010ffff", "\U0010ffffz"]
    _put_keys(store, keys)
    assert _keys(store.list_objects("b", prefix="\ud7ff")) == ["\ud7ff", "\ud7ffz"]
    assert _keys(store.list_objects("b", prefix="a\U0010ffff")) == [
        "a\U0010ffff",
        "a\U0010ffff\U0010ffff",
    ]
    assert _keys(store.list_objects("b", prefix="\U0010ffff")) == ["\U0010ffff", "\U0010ffffz"]
    assert _keys(store.list_objects("b", prefix="\uffff")) == ["\uffff"]


def test_list_order_is_utf8_byte_order(store):
    keys = ["b", "B", "a", "_", "%", "é", "\uffff", "\U00010000", "\ue000", "\x7f", "~"]
    _put_keys(store, keys)
    assert _keys(store.list_objects("b")) == _utf8_sorted(keys)


def test_list_prefix_stays_in_bucket(store):
    _put_keys(store, ["x/1", "x/2"], bucket="a")
    _put_keys(store, ["x/3"], bucket="b")
    _put_keys(store, ["x/4"], bucket="c")
    assert _keys(store.list_objects("b", prefix="x/")) == ["x/3"]
    assert _keys(store.list_objects("b")) == ["x/3"]


def test_list_start_after_and_limit(store):
    _put_keys(store, ["a", "b/1", "b/2", "b/3", "c"])
    assert _keys(store.list_objects("b", start_after="b/1")) == ["b/2", "b/3", "c"]
    assert _keys(store.list_objects("b", start_after="b/0", limit=2)) == ["b/1", "b/2"]
    assert _keys(store.list_objects("b", prefix="b/", start_after="a")) == ["b/1", "b/2", "b/3"]
    assert _keys(store.list_objects("b", prefix="b/", start_after="b/2")) == ["b/3"]
    assert _keys(store.list_objects("b", prefix="b/", start_after="c")) == []
    assert _keys(store.list_objects("b", limit=0)) == []
    # start_after need not be a key.
    assert _keys(store.list_objects("b", start_after="b/10")) == ["b/2", "b/3", "c"]


def test_list_prefix_matches_startswith_on_random_keys(store):
    rng = random.Random(1311)
    alphabet = ["a", "b", "A", "_", "%", "/", "é", "日", "\ud7ff", "\ue000", "\uffff"]
    alphabet += ["\U00010000", "\U0010ffff", "\x00", "\x7f"]
    keys = sorted({"".join(rng.choices(alphabet, k=rng.randint(1, 5))) for _ in range(400)})
    _put_keys(store, keys)
    prefixes = {""} | {k[: rng.randint(0, len(k))] for k in keys}
    prefixes |= {"".join(rng.choices(alphabet, k=rng.randint(1, 3))) for _ in range(100)}
    for prefix in sorted(prefixes):
        expected = _utf8_sorted(k for k in keys if k.startswith(prefix))
        assert _keys(store.list_objects("b", prefix=prefix)) == expected, prefix
        after = rng.choice(keys)
        expected_after = [k for k in expected if k.encode("utf-8") > after.encode("utf-8")]
        got = store.list_objects("b", prefix=prefix, start_after=after, limit=3)
        assert _keys(got) == expected_after[:3], (prefix, after)


def test_snapshot_reads_one_committed_state(store):
    _put_keys(store, ["a", "c"])
    with store._snapshot() as conn:
        sql, params = key_range_query("b", "", None, None)
        first = [r["key"] for r in conn.execute(sql, params)]
        _put_keys(store, ["b"])
        store.delete_objects("b", ["c"])
        second = [r["key"] for r in conn.execute(sql, params)]
    assert first == second == ["a", "c"]
    assert _keys(store.list_objects("b")) == ["a", "b"]


# --- Multipart uploads ---


def test_create_and_upload_exists(store):
    store.create_bucket("b", _NOW)
    store.create_upload("uid-1", "b", "k", _NOW)
    assert store.upload_exists("uid-1") is True


def test_upload_exists_false(store):
    assert store.upload_exists("no-such-uid") is False


def test_record_and_get_parts(store):
    store.create_bucket("b", _NOW)
    store.create_upload("uid-1", "b", "k", _NOW)
    store.record_part("uid-1", PartMeta("uid-1", 1, '"etag1"', 100, _part_blob(store, 1)))
    store.record_part("uid-1", PartMeta("uid-1", 2, '"etag2"', 200, _part_blob(store, 2)))
    parts = store.get_parts("uid-1")
    assert len(parts) == 2
    assert parts[0].part_number == 1
    assert parts[1].part_number == 2


def test_record_part_overwrite(store):
    store.create_bucket("b", _NOW)
    store.create_upload("uid-1", "b", "k", _NOW)
    store.record_part("uid-1", PartMeta("uid-1", 1, '"old-etag"', 100, _part_blob(store, 1)))
    store.record_part("uid-1", PartMeta("uid-1", 1, '"new-etag"', 200, _part_blob(store, 1)))
    parts = store.get_parts("uid-1")
    assert len(parts) == 1
    assert parts[0].etag == '"new-etag"'
    assert parts[0].size == 200


def test_complete_upload_removes_from_incomplete(store):
    store.create_bucket("b", _NOW)
    store.create_upload("uid-1", "b", "k", _NOW)
    store.commit_completed_upload("uid-1", _obj(blob_id=_blob(store)))
    assert store.upload_exists("uid-1") is False
    incomplete = store.list_incomplete_uploads()
    assert all(u.upload_id != "uid-1" for u in incomplete)


def test_abort_upload(store):
    store.create_bucket("b", _NOW)
    store.create_upload("uid-1", "b", "k", _NOW)
    store.record_part("uid-1", PartMeta("uid-1", 1, '"e"', 50, _part_blob(store, 1)))
    store.abort_upload("uid-1")
    assert store.upload_exists("uid-1") is False
    assert store.get_parts("uid-1") == []


def test_commit_completed_upload_commits_object_and_closes_upload(store):
    store.create_bucket("b", _NOW)
    store.create_upload("uid-1", "b", "k", _NOW)
    blob_id = _blob(store)
    assert store.commit_completed_upload("uid-1", _obj(etag='"abc-2"', blob_id=blob_id)) is True
    assert store.get_object_meta("b", "k").etag == '"abc-2"'
    assert store.upload_exists("uid-1") is False


def test_commit_completed_upload_rolls_back_both_on_failure(store):
    # The upload row is updated first; the object insert then violates the foreign
    # key on objects.blob_id (the blob was never registered). The upload must still
    # be open afterwards.
    store.create_bucket("b", _NOW)
    store.create_upload("uid-1", "b", "k", _NOW)
    with pytest.raises(sqlite3.IntegrityError):
        store.commit_completed_upload("uid-1", _obj(blob_id="0" * 32))
    assert store.upload_exists("uid-1") is True
    assert store.get_object_meta("b", "k") is None


def test_commit_completed_upload_refuses_closed_or_unknown_upload(store):
    store.create_bucket("b", _NOW)
    store.create_upload("uid-1", "b", "k", _NOW)
    # Close the upload through the real commit and record the row it wrote.
    assert store.commit_completed_upload("uid-1", _obj(blob_id=_blob(store))) is True
    recorded = store.get_object_meta("b", "k")
    # Both refused commits carry object data that differs from that row.
    to_closed = _obj(size=11, etag='"other-1"', blob_id=_blob(store))
    to_unknown = _obj(size=12, etag='"other-2"', blob_id=_blob(store))
    for meta in (to_closed, to_unknown):
        assert (meta.blob_id, meta.etag, meta.size) != (
            recorded.blob_id,
            recorded.etag,
            recorded.size,
        )
    assert store.commit_completed_upload("uid-1", to_closed) is False
    assert store.commit_completed_upload("no-such-upload", to_unknown) is False
    assert store.get_object_meta("b", "k") == recorded


def test_list_incomplete_uploads_excludes_completed(store):
    store.create_bucket("b", _NOW)
    store.create_upload("uid-pending", "b", "k1", _NOW)
    store.create_upload("uid-done", "b", "k2", _NOW)
    store.commit_completed_upload("uid-done", _obj(key="k2", blob_id=_blob(store, "k2")))
    incomplete = store.list_incomplete_uploads()
    ids = {u.upload_id for u in incomplete}
    assert "uid-pending" in ids
    assert "uid-done" not in ids


def test_get_parts_ordered_by_part_number(store):
    store.create_bucket("b", _NOW)
    store.create_upload("uid-1", "b", "k", _NOW)
    store.record_part("uid-1", PartMeta("uid-1", 3, '"e3"', 10, _part_blob(store, 3)))
    store.record_part("uid-1", PartMeta("uid-1", 1, '"e1"', 10, _part_blob(store, 1)))
    store.record_part("uid-1", PartMeta("uid-1", 2, '"e2"', 10, _part_blob(store, 2)))
    parts = store.get_parts("uid-1")
    assert [p.part_number for p in parts] == [1, 2, 3]


# --- Transaction rollback ---


def test_write_rollback_on_exception(store, tmp_path):
    """A failure inside a transaction must leave no partial state."""
    store.create_bucket("b", _NOW)
    # A registered blob, so the only constraint the insert violates is the foreign
    # key on objects.bucket.
    blob_id = store.register_blob("object", "no-such-bucket", "k")
    # Directly connect and force a constraint violation inside a transaction
    conn = sqlite3.connect(str(tmp_path / "test.db"))
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        with conn:
            conn.execute(
                "INSERT INTO objects (bucket, key, blob_id, size, etag, last_modified, "
                "content_type) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    "no-such-bucket",
                    "k",
                    blob_id,
                    1,
                    '"e"',
                    "2024-01-01T00:00:00.000000",
                    "text/plain",
                ),
            )
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()
    # The object must not have been committed
    assert store.get_object_meta("no-such-bucket", "k") is None


# --- Listing pages: delimiter, common prefixes, marker, max-keys ---


def _page(store, prefix="", delimiter="", marker="", max_keys=1000):
    page = store.list_page("b", prefix, delimiter, marker, max_keys)
    return _keys(page.objects), page.common_prefixes, page.is_truncated, page.next_marker


def test_common_prefix():
    assert common_prefix("a/b/c", "", "/") == "a/"
    assert common_prefix("a/b/c", "a/", "/") == "a/b/"
    assert common_prefix("a/b", "a/", "/") is None
    assert common_prefix("abcabc", "", "bc") == "abc"
    # The delimiter is searched for after the prefix only.
    assert common_prefix("ab/c", "ab/", "/") is None
    assert common_prefix("x", "", "") is None
    assert common_prefix("b/1", "a", "/") is None


def test_page_rolls_up_common_prefixes(store):
    _put_keys(store, ["asdf", "foo/bar", "foo/bar/xyzzy", "quux/thud"])
    assert _page(store, delimiter="/") == (["asdf"], ["foo/", "quux/"], False, "quux/")


def test_page_common_prefixes_count_toward_max_keys(store):
    _put_keys(store, ["asdf", "boo/bar", "boo/baz/xyzzy", "cquux/bla", "cquux/thud"])
    assert _page(store, delimiter="/", max_keys=1) == (["asdf"], [], True, "asdf")
    assert _page(store, delimiter="/", max_keys=2) == (["asdf"], ["boo/"], True, "boo/")
    assert _page(store, delimiter="/", max_keys=3) == (
        ["asdf"],
        ["boo/", "cquux/"],
        False,
        "cquux/",
    )


def test_page_marker_equal_to_common_prefix_skips_it(store):
    _put_keys(store, ["a/", "a/1", "a/2", "a0", "b/1"])
    assert _page(store, delimiter="/", marker="a/") == (["a0"], ["b/"], False, "b/")


def test_page_marker_inside_common_prefix_skips_it(store):
    _put_keys(store, ["a/1", "a/2", "a/3/x", "b"])
    for marker in ["a/1", "a/2", "a/25", "a/3/x", "a/\U0010ffff"]:
        assert _page(store, delimiter="/", marker=marker) == (["b"], [], False, "b")


def test_page_marker_before_common_prefix_returns_it(store):
    _put_keys(store, ["a/1", "a/2", "b"])
    assert _page(store, delimiter="/", marker="a") == (["b"], ["a/"], False, "b")


def test_page_marker_inside_common_prefix_under_prefix(store):
    _put_keys(store, ["p/a/1", "p/a/2", "p/b", "p/c/1", "q/1"])
    assert _page(store, prefix="p/", delimiter="/", marker="p/a/1") == (
        ["p/b"],
        ["p/c/"],
        False,
        "p/c/",
    )
    # A marker that does not start with the prefix is only a lower bound.
    assert _page(store, prefix="p/", delimiter="/", marker="a/x") == (
        ["p/b"],
        ["p/a/", "p/c/"],
        False,
        "p/c/",
    )


def test_page_marker_without_delimiter_is_a_key_bound(store):
    _put_keys(store, ["a/1", "a/2", "b"])
    assert _page(store, marker="a/1") == (["a/2", "b"], [], False, "b")


def test_page_restarts_after_common_prefix(store):
    # Many keys under one common prefix must not use up the page.
    _put_keys(store, [f"a/{i:03}" for i in range(50)] + ["b", "c/1", "d"])
    assert _page(store, delimiter="/", max_keys=3) == (["b"], ["a/", "c/"], True, "c/")
    assert _page(store, delimiter="/", marker="c/", max_keys=3) == (["d"], [], False, "d")


def test_page_common_prefix_of_only_max_code_points(store):
    _put_keys(store, ["\U0010ffff/1", "\U0010ffff/2"])
    assert _page(store, delimiter="/") == ([], ["\U0010ffff/"], False, "\U0010ffff/")
    assert _page(store, delimiter="\U0010ffff") == ([], ["\U0010ffff"], False, "\U0010ffff")
    assert _page(store, delimiter="\U0010ffff", marker="\U0010ffff") == ([], [], False, None)


def test_page_multi_character_delimiter(store):
    _put_keys(store, ["a--b--c", "a--d", "a-e", "f"])
    assert _page(store, delimiter="--") == (["a-e", "f"], ["a--"], False, "f")
    assert _page(store, prefix="a--", delimiter="--") == (["a--d"], ["a--b--"], False, "a--d")


def test_page_max_keys_zero(store):
    _put_keys(store, ["a", "b"])
    assert _page(store, max_keys=0) == ([], [], False, None)


def test_page_truncation_is_exact(store):
    _put_keys(store, ["a", "b", "c"])
    assert _page(store, max_keys=3) == (["a", "b", "c"], [], False, "c")
    assert _page(store, max_keys=2) == (["a", "b"], [], True, "b")
    assert _page(store, delimiter="/", max_keys=2, marker="b") == (["c"], [], False, "c")


def test_page_reads_one_snapshot(store, monkeypatch):
    # A writer that commits between the page's range reads must not be seen.
    _put_keys(store, ["a/1", "b/1", "c/1"])
    real_snapshot = store._snapshot
    calls = []

    @contextmanager
    def snapshot_with_writer():
        with real_snapshot() as conn:

            class Conn:
                def execute(self, sql, params=()):
                    calls.append(sql)
                    if len(calls) == 2:
                        _put_keys(store, ["a0", "b0"])
                        store.delete_objects("b", ["c/1"])
                    return conn.execute(sql, params)

            yield Conn()

    monkeypatch.setattr(store, "_snapshot", snapshot_with_writer)
    assert _page(store, delimiter="/") == ([], ["a/", "b/", "c/"], False, "c/")
    assert len(calls) >= 3
