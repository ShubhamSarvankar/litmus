import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from server.metadata.base import ObjectMeta, PartMeta
from server.metadata.sqlite import SQLiteMetadataStore

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


def _obj(bucket="b", key="k", size=10, etag='"abc"', content_type="text/plain"):
    return ObjectMeta(
        bucket=bucket,
        key=key,
        size=size,
        etag=etag,
        last_modified=_NOW,
        content_type=content_type,
    )


def test_put_and_get_object(store):
    store.create_bucket("b", _NOW)
    meta = _obj()
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
    store.put_object_meta("b", "k", _obj(size=10, etag='"old"'))
    store.put_object_meta("b", "k", _obj(size=99, etag='"new"'))
    result = store.get_object_meta("b", "k")
    assert result.size == 99
    assert result.etag == '"new"'


def test_delete_object_meta(store):
    store.create_bucket("b", _NOW)
    store.put_object_meta("b", "k", _obj())
    store.delete_object_meta("b", "k")
    assert store.get_object_meta("b", "k") is None


def test_list_objects(store):
    store.create_bucket("b", _NOW)
    store.put_object_meta("b", "key1", _obj(key="key1"))
    store.put_object_meta("b", "key2", _obj(key="key2"))
    objects = store.list_objects("b")
    assert len(objects) == 2
    assert {o.key for o in objects} == {"key1", "key2"}


def test_list_objects_with_prefix(store):
    store.create_bucket("b", _NOW)
    store.put_object_meta("b", "logs/a", _obj(key="logs/a"))
    store.put_object_meta("b", "logs/b", _obj(key="logs/b"))
    store.put_object_meta("b", "data/c", _obj(key="data/c"))
    result = store.list_objects("b", prefix="logs/")
    assert len(result) == 2
    assert all(o.key.startswith("logs/") for o in result)


def test_list_objects_empty_bucket(store):
    store.create_bucket("b", _NOW)
    assert store.list_objects("b") == []


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
    store.record_part("uid-1", PartMeta("uid-1", 1, '"etag1"', 100))
    store.record_part("uid-1", PartMeta("uid-1", 2, '"etag2"', 200))
    parts = store.get_parts("uid-1")
    assert len(parts) == 2
    assert parts[0].part_number == 1
    assert parts[1].part_number == 2


def test_record_part_overwrite(store):
    store.create_bucket("b", _NOW)
    store.create_upload("uid-1", "b", "k", _NOW)
    store.record_part("uid-1", PartMeta("uid-1", 1, '"old-etag"', 100))
    store.record_part("uid-1", PartMeta("uid-1", 1, '"new-etag"', 200))
    parts = store.get_parts("uid-1")
    assert len(parts) == 1
    assert parts[0].etag == '"new-etag"'
    assert parts[0].size == 200


def test_complete_upload_removes_from_incomplete(store):
    store.create_bucket("b", _NOW)
    store.create_upload("uid-1", "b", "k", _NOW)
    store.complete_upload("uid-1")
    assert store.upload_exists("uid-1") is False
    incomplete = store.list_incomplete_uploads()
    assert all(u.upload_id != "uid-1" for u in incomplete)


def test_abort_upload(store):
    store.create_bucket("b", _NOW)
    store.create_upload("uid-1", "b", "k", _NOW)
    store.record_part("uid-1", PartMeta("uid-1", 1, '"e"', 50))
    store.abort_upload("uid-1")
    assert store.upload_exists("uid-1") is False
    assert store.get_parts("uid-1") == []


def test_list_incomplete_uploads_excludes_completed(store):
    store.create_bucket("b", _NOW)
    store.create_upload("uid-pending", "b", "k1", _NOW)
    store.create_upload("uid-done", "b", "k2", _NOW)
    store.complete_upload("uid-done")
    incomplete = store.list_incomplete_uploads()
    ids = {u.upload_id for u in incomplete}
    assert "uid-pending" in ids
    assert "uid-done" not in ids


def test_get_parts_ordered_by_part_number(store):
    store.create_bucket("b", _NOW)
    store.create_upload("uid-1", "b", "k", _NOW)
    store.record_part("uid-1", PartMeta("uid-1", 3, '"e3"', 10))
    store.record_part("uid-1", PartMeta("uid-1", 1, '"e1"', 10))
    store.record_part("uid-1", PartMeta("uid-1", 2, '"e2"', 10))
    parts = store.get_parts("uid-1")
    assert [p.part_number for p in parts] == [1, 2, 3]


# --- Transaction rollback ---


def test_write_rollback_on_exception(store, tmp_path):
    """A failure inside a transaction must leave no partial state."""
    store.create_bucket("b", _NOW)
    # Directly connect and force a constraint violation inside a transaction
    conn = sqlite3.connect(str(tmp_path / "test.db"))
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        with conn:
            conn.execute(
                "INSERT INTO objects (bucket, key, size, etag, last_modified, content_type) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("no-such-bucket", "k", 1, '"e"', "2024-01-01T00:00:00.000000", "text/plain"),
            )
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()
    # The object must not have been committed
    assert store.get_object_meta("no-such-bucket", "k") is None
