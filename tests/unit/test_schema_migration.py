"""Schema version 3 and the in place migration from version 2
(docs/design/storage-layout.md §19 decision 28, §20).

A store only inspects an existing database; migrate() changes it. The v2 databases come
from chaos/fixtures/v2_database.py, which carries version 2's DDL as a literal.
"""

import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from chaos.fixtures.v2_database import BUCKET, PART_BLOB_CREATED, build_v2_database
from server.metadata import sqlite as sqlite_module
from server.metadata.base import ObjectMeta, PartMeta
from server.metadata.sqlite import SchemaVersionError, SQLiteMetadataStore
from server.storage.blobs import new_blob_id

ALGORITHMS = ["CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"]
TYPES = ["FULL_OBJECT", "COMPOSITE"]

# The v3 columns of each table, in order, written out here rather than imported.
NEW_COLUMNS = {
    "multipart_uploads": ["checksum_algorithm", "checksum_type"],
    "multipart_parts": ["checksum", "checksum_algorithm", "last_modified"],
    "objects": ["checksum", "checksum_algorithm", "checksum_type"],
}
ALL_NEW = [(table, column) for table, columns in NEW_COLUMNS.items() for column in columns]
ALGORITHM_COLUMNS = [
    ("multipart_uploads", "checksum_algorithm"),
    ("multipart_parts", "checksum_algorithm"),
    ("objects", "checksum_algorithm"),
]
TYPE_COLUMNS = [("multipart_uploads", "checksum_type"), ("objects", "checksum_type")]
TABLES = [
    "schema_version",
    "store",
    "buckets",
    "blobs",
    "multipart_uploads",
    "multipart_parts",
    "objects",
]

# The columns of the minimum row each table accepts, for the CHECK tests.
MINIMAL_ROWS = {
    "multipart_uploads": {
        "upload_id": "u",
        "bucket": "b",
        "key": "k",
        "initiated": "t",
        "state": "open",
    },
    "multipart_parts": {"upload_id": "u", "part_number": 1, "etag": "e", "size": 1},
    "objects": {
        "bucket": "b",
        "key": "k",
        "blob_id": "x" * 32,
        "size": 1,
        "etag": "e",
        "last_modified": "t",
    },
}


def _connect(db: Path) -> sqlite3.Connection:
    return sqlite3.connect(db, isolation_level=None)


def _scalar(db: Path, sql: str):
    conn = _connect(db)
    try:
        return conn.execute(sql).fetchone()[0]
    finally:
        conn.close()


def _version(db: Path) -> int:
    return _scalar(db, "SELECT version FROM schema_version")


def _column_names(db: Path, table: str) -> list[str]:
    conn = _connect(db)
    try:
        return [r[1] for r in conn.execute(f"PRAGMA table_xinfo({table})")]
    finally:
        conn.close()


def _v2_rows(db: Path) -> dict[str, list[tuple]]:
    """Every table's rows over the columns version 2 has, in a fixed order."""
    conn = _connect(db)
    try:
        dump = {}
        for table in TABLES:
            names = [r[1] for r in conn.execute(f"PRAGMA table_xinfo({table})")]
            new = NEW_COLUMNS.get(table, [])
            old = [n for n in names if n not in new]
            dump[table] = sorted(
                conn.execute(f"SELECT {', '.join(old)} FROM {table}").fetchall(), key=repr
            )
        return dump
    finally:
        conn.close()


def _accepts(db: Path, table: str, column: str, value: str) -> bool:
    """Whether the table takes a minimal row with `column` = value; nothing is kept."""
    row = {**MINIMAL_ROWS[table], column: value}
    conn = _connect(db)
    try:
        conn.execute("BEGIN")
        try:
            conn.execute(
                f"INSERT INTO {table} ({', '.join(row)}) VALUES ({', '.join('?' * len(row))})",
                tuple(row.values()),
            )
        except sqlite3.IntegrityError:
            return False
        finally:
            conn.execute("ROLLBACK")
        return True
    finally:
        conn.close()


def _migrated(tmp_path: Path, **kwargs) -> tuple[Path, SQLiteMetadataStore]:
    data = build_v2_database(tmp_path / "data", **kwargs)
    store = SQLiteMetadataStore(db_path=data.db_path)
    store.migrate()
    return data.db_path, store


def _version_1_database(db: Path) -> None:
    conn = sqlite3.connect(db)
    with conn:
        conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_version VALUES (1)")
        conn.execute("CREATE TABLE buckets (name TEXT PRIMARY KEY, created_at TEXT NOT NULL)")
    conn.close()


def _statements() -> list[str]:
    return list(sqlite_module._MIGRATE_2_TO_3)


# --- a fresh database, and what the constructor refuses ---


def test_fresh_database_is_v3(tmp_path):
    db = tmp_path / "meta.db"
    SQLiteMetadataStore(db_path=db)
    assert _version(db) == 3
    for table, columns in NEW_COLUMNS.items():
        assert _column_names(db, table)[-len(columns) :] == columns


def test_v1_message_is_unchanged(tmp_path):
    db = tmp_path / "meta.db"
    _version_1_database(db)
    with pytest.raises(SchemaVersionError) as caught:
        SQLiteMetadataStore(db_path=db)
    assert str(caught.value) == (
        f"{db} has schema version 1; this server needs version 2. The storage layout "
        "changed and old data is not migrated: empty the data directory or use a new one."
    )


@pytest.mark.parametrize("newer", [4, 5, 99])
def test_version_above_3_is_refused(tmp_path, newer):
    data = build_v2_database(tmp_path / "data")
    conn = _connect(data.db_path)
    conn.execute("UPDATE schema_version SET version = ?", (newer,))
    conn.close()
    with pytest.raises(SchemaVersionError) as caught:
        SQLiteMetadataStore(db_path=data.db_path)
    assert str(caught.value) == (
        f"{data.db_path} has schema version {newer}; this server supports versions 2 and 3, "
        "and migrates 2 to 3. It was written by a newer server: use that server or a new "
        "data directory."
    )


def test_inspecting_a_v2_database_writes_nothing(tmp_path):
    """Comparing file bytes is valid only because the helper closed its connection
    cleanly: no WAL frames are left for a read only open to checkpoint into the file."""
    data = build_v2_database(tmp_path / "data")
    before = data.db_path.read_bytes()
    listing = sorted(p.name for p in data.db_path.parent.iterdir())
    SQLiteMetadataStore(db_path=data.db_path)
    assert data.db_path.read_bytes() == before
    assert sorted(p.name for p in data.db_path.parent.iterdir()) == listing


def test_v2_database_is_not_migrated_until_migrate_is_called(tmp_path):
    data = build_v2_database(tmp_path / "data")
    store = SQLiteMetadataStore(db_path=data.db_path)
    # Checked through direct sqlite3 connections: before migrate() the store's data API
    # is not used, since its reads expect the version 3 columns.
    assert _version(data.db_path) == 2
    assert sum(len(columns) for columns in NEW_COLUMNS.values()) == 8
    for table in NEW_COLUMNS:
        assert not set(NEW_COLUMNS[table]) & set(_column_names(data.db_path, table))
    rows = _v2_rows(data.db_path)
    assert all(rows[t] for t in TABLES)
    assert sorted(r[1] for r in rows["objects"]) == sorted(data.objects)
    store.migrate()
    assert _version(data.db_path) == 3


# --- what the migration produces ---


@pytest.mark.parametrize("table", sorted(NEW_COLUMNS))
def test_migrate_adds_exactly_the_v3_columns(tmp_path, table):
    data = build_v2_database(tmp_path / "data")
    before = _column_names(data.db_path, table)
    SQLiteMetadataStore(db_path=data.db_path).migrate()
    assert _column_names(data.db_path, table) == before + NEW_COLUMNS[table]


def test_migrated_schema_matches_fresh_schema(tmp_path):
    migrated_db, _ = _migrated(tmp_path)
    fresh_db = tmp_path / "fresh.db"
    SQLiteMetadataStore(db_path=fresh_db)

    def shape(db: Path):
        conn = _connect(db)
        try:
            out = {}
            for table in TABLES:
                out[table] = conn.execute(f"PRAGMA table_xinfo({table})").fetchall()
                indexes = conn.execute(f"PRAGMA index_list({table})").fetchall()
                out[table, "indexes"] = sorted(
                    (
                        i[2],
                        i[3],
                        i[4],
                        tuple(c[2] for c in conn.execute(f"PRAGMA index_info({i[1]})")),
                    )
                    for i in indexes
                )
            return out
        finally:
            conn.close()

    assert shape(migrated_db) == shape(fresh_db)
    probes = [(t, c, v) for t, c in ALGORITHM_COLUMNS for v in [*ALGORITHMS, "crc32", ""]]
    probes += [(t, c, v) for t, c in TYPE_COLUMNS for v in [*TYPES, "full_object", ""]]
    for table, column, value in probes:
        assert _accepts(migrated_db, table, column, value) == _accepts(
            fresh_db, table, column, value
        ), (table, column, value)


def test_migration_keeps_every_v2_row(tmp_path):
    data = build_v2_database(tmp_path / "data", extra_parts=20)
    before = _v2_rows(data.db_path)
    assert all(before[t] for t in TABLES)
    SQLiteMetadataStore(db_path=data.db_path).migrate()
    assert _version(data.db_path) == 3
    after = _v2_rows(data.db_path)
    after["schema_version"] = before["schema_version"]
    assert after == before


@pytest.mark.parametrize(("table", "column"), ALL_NEW)
def test_new_columns_are_null_after_migration(tmp_path, table, column):
    db, _ = _migrated(tmp_path)
    # A part that still holds bytes gets a last_modified (the backfill below).
    where = "1"
    if (table, column) == ("multipart_parts", "last_modified"):
        where = "blob_id IS NULL"
    conn = _connect(db)
    try:
        total = conn.execute(f"SELECT count(*) FROM {table} WHERE {where}").fetchone()[0]
        nulls = conn.execute(
            f"SELECT count(*) FROM {table} WHERE {where} AND {column} IS NULL"
        ).fetchone()[0]
    finally:
        conn.close()
    assert total > 0
    assert nulls == total


@pytest.mark.parametrize(
    ("upload_id", "expected"),
    [
        # A part that holds bytes gets the creation time of its blob.
        ("open-up", PART_BLOB_CREATED),
        # A completed upload's part has no blob and stays NULL.
        ("done-up", {1: None}),
    ],
)
def test_part_last_modified_backfill(tmp_path, upload_id, expected):
    db, _ = _migrated(tmp_path)
    conn = _connect(db)
    try:
        rows = dict(
            conn.execute(
                "SELECT part_number, last_modified FROM multipart_parts WHERE upload_id = ?",
                (upload_id,),
            )
        )
    finally:
        conn.close()
    assert rows == expected


@pytest.mark.parametrize(
    ("table", "column", "value"),
    [
        ("multipart_uploads", "checksum_algorithm", "MD5"),
        ("multipart_parts", "checksum_algorithm", "crc32"),
        ("objects", "checksum_algorithm", ""),
        ("multipart_uploads", "checksum_type", "full_object"),
        ("objects", "checksum_type", "SEMI"),
    ],
)
def test_check_constraints_reject(tmp_path, table, column, value):
    db, _ = _migrated(tmp_path)
    assert not _accepts(db, table, column, value)


@pytest.mark.parametrize(
    ("table", "column", "value"),
    [(t, c, v) for t, c in ALGORITHM_COLUMNS for v in ALGORITHMS]
    + [(t, c, v) for t, c in TYPE_COLUMNS for v in TYPES],
)
def test_check_constraints_accept(tmp_path, table, column, value):
    db, _ = _migrated(tmp_path)
    assert _accepts(db, table, column, value)


# --- failure and repetition ---


@pytest.mark.parametrize("failing_after", range(10))
def test_migration_is_atomic(tmp_path, monkeypatch, failing_after):
    data = build_v2_database(tmp_path / "data")
    store = SQLiteMetadataStore(db_path=data.db_path)
    statements = _statements()
    assert len(statements) == 10
    before = _v2_rows(data.db_path)
    columns = {t: _column_names(data.db_path, t) for t in NEW_COLUMNS}

    broken = [*statements[: failing_after + 1], "INSERT INTO no_such_table VALUES (1)"]
    monkeypatch.setattr(sqlite_module, "_MIGRATE_2_TO_3", broken)
    with pytest.raises(sqlite3.OperationalError, match="no_such_table"):
        store.migrate()
    assert _version(data.db_path) == 2
    assert {t: _column_names(data.db_path, t) for t in NEW_COLUMNS} == columns
    assert _v2_rows(data.db_path) == before
    assert _scalar(data.db_path, "SELECT count(*) FROM multipart_parts") == 3

    monkeypatch.setattr(sqlite_module, "_MIGRATE_2_TO_3", statements)
    store.migrate()
    assert _version(data.db_path) == 3
    for table, new in NEW_COLUMNS.items():
        assert _column_names(data.db_path, table) == columns[table] + new


def test_disk_full_during_migration_leaves_v2(tmp_path, monkeypatch):
    # Enough part rows that backfilling last_modified needs pages the file cannot get.
    data = build_v2_database(tmp_path / "data", extra_parts=4000)
    store = SQLiteMetadataStore(db_path=data.db_path)
    before = _v2_rows(data.db_path)
    columns = {t: _column_names(data.db_path, t) for t in NEW_COLUMNS}
    pages = _scalar(data.db_path, "PRAGMA page_count")

    real_connect = store._connect

    def full_connect() -> sqlite3.Connection:
        conn = real_connect()
        conn.execute(f"PRAGMA max_page_count = {pages}")
        return conn

    monkeypatch.setattr(store, "_connect", full_connect)
    with pytest.raises(sqlite3.OperationalError, match="full"):
        store.migrate()
    assert _version(data.db_path) == 2
    assert {t: _column_names(data.db_path, t) for t in NEW_COLUMNS} == columns
    assert _v2_rows(data.db_path) == before

    monkeypatch.setattr(store, "_connect", real_connect)
    store.migrate()
    assert _version(data.db_path) == 3


def test_migrate_twice_is_a_no_op(tmp_path, monkeypatch):
    db, store = _migrated(tmp_path)
    rows = _v2_rows(db)

    def refuse() -> sqlite3.Connection:
        raise AssertionError("a current store must not open a connection")

    monkeypatch.setattr(store, "_connect", refuse)
    store.migrate()
    monkeypatch.undo()
    assert _v2_rows(db) == rows
    assert _version(db) == 3


def test_second_store_migrating_first_is_a_no_op(tmp_path, monkeypatch):
    data = build_v2_database(tmp_path / "data")
    first = SQLiteMetadataStore(db_path=data.db_path)
    second = SQLiteMetadataStore(db_path=data.db_path)
    first.migrate()
    # The second store still believes the database is at version 2; were it to run
    # the statements again, this one would fail.
    monkeypatch.setattr(sqlite_module, "_MIGRATE_2_TO_3", ["INSERT INTO no_such_table VALUES (1)"])
    second.migrate()
    assert _version(data.db_path) == 3
    monkeypatch.undo()
    assert _column_names(data.db_path, "objects").count("checksum") == 1


def test_migrated_database_serves_store_api(tmp_path):
    db, store = _migrated(tmp_path)
    k1 = store.get_object_meta(BUCKET, "k1")
    assert k1 is not None and k1.content_type == "text/plain" and k1.user_metadata == {"m": "1"}
    assert [o.key for o in store.list_objects(BUCKET)] == ["k1", "k2"]
    assert store.get_object_meta(BUCKET, "k2").upload_id == "done-up"
    assert store.get_upload("open-up").user_metadata == {"a": "b"}
    assert [p.part_number for p in store.get_parts("open-up")] == [1, 2]
    assert store.get_parts("done-up")[0].blob_id is None

    # Writes through the unchanged write paths.
    part_blob = store.register_blob("part", BUCKET, "mp", "open-up", 3)
    store.record_part("open-up", PartMeta("open-up", 3, '"e3"', 4, part_blob))
    assert [p.part_number for p in store.get_parts("open-up")] == [1, 2, 3]
    blob = store.register_blob("object", BUCKET, "new")
    store.put_object_meta(
        BUCKET,
        "new",
        ObjectMeta(BUCKET, "new", 3, '"e"', datetime(2026, 2, 3), "text/plain", blob),
    )
    assert store.get_object_meta(BUCKET, "new").size == 3
    assert _scalar(db, "SELECT checksum_algorithm FROM objects WHERE key = 'new'") is None
    store.abort_upload("open-up")
    assert store.get_upload("open-up") is None


def test_partmeta_positional_arguments():
    blob_id = new_blob_id()
    part = PartMeta("u", 7, '"e"', 12, blob_id)
    assert (part.upload_id, part.part_number, part.etag, part.size, part.blob_id) == (
        "u",
        7,
        '"e"',
        12,
        blob_id,
    )
    assert PartMeta("u", 7, '"e"', 12).blob_id is None
