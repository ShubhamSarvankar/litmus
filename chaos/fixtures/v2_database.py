"""A schema version 2 database, as the server before version 3 wrote it, for the tests
of the in place migration (docs/design/storage-layout.md §20).

The DDL below is a literal copy of version 2's, not derived from server/, so changing
the current schema cannot change what these tests migrate from. Rows go in by SQL and
the blob files through BlobStore; the database is closed cleanly, so no -wal or -shm
file is left behind.
"""

import hashlib
import sqlite3
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

from server.storage.blobs import BlobStore, new_blob_id

V2_DDL = [
    "CREATE TABLE schema_version (version INTEGER NOT NULL)",
    """CREATE TABLE store (
        singleton   INTEGER PRIMARY KEY CHECK (singleton = 1),
        store_id    TEXT NOT NULL CHECK (length(store_id) = 32),
        state       TEXT NOT NULL CHECK (state IN ('initializing', 'ready')),
        blob_dir    TEXT NOT NULL,
        created_at  TEXT NOT NULL
    )""",
    """CREATE TABLE buckets (
        name        TEXT PRIMARY KEY,
        created_at  TEXT NOT NULL
    )""",
    """CREATE TABLE blobs (
        blob_id      TEXT PRIMARY KEY CHECK (length(blob_id) = 32),
        state        TEXT NOT NULL CHECK (state IN ('pending', 'live', 'garbage')),
        kind         TEXT NOT NULL CHECK (kind IN ('object', 'part')),
        bucket       TEXT NOT NULL,
        key          TEXT NOT NULL,
        upload_id    TEXT,
        part_number  INTEGER,
        created_at   TEXT NOT NULL
    ) WITHOUT ROWID""",
    "CREATE INDEX blobs_by_state ON blobs (state)",
    """CREATE TABLE multipart_uploads (
        upload_id            TEXT PRIMARY KEY,
        bucket               TEXT NOT NULL REFERENCES buckets (name),
        key                  TEXT NOT NULL,
        initiated            TEXT NOT NULL,
        state                TEXT NOT NULL CHECK (state IN ('open', 'completed')),
        content_type         TEXT NOT NULL DEFAULT 'binary/octet-stream',
        cache_control        TEXT,
        content_disposition  TEXT,
        content_encoding     TEXT,
        content_language     TEXT,
        expires              TEXT,
        user_metadata        TEXT NOT NULL DEFAULT '{}'
    )""",
    "CREATE INDEX uploads_by_bucket ON multipart_uploads (bucket, state)",
    """CREATE TABLE multipart_parts (
        upload_id    TEXT NOT NULL REFERENCES multipart_uploads (upload_id),
        part_number  INTEGER NOT NULL CHECK (part_number BETWEEN 1 AND 10000),
        blob_id      TEXT UNIQUE REFERENCES blobs (blob_id),
        etag         TEXT NOT NULL,
        size         INTEGER NOT NULL,
        PRIMARY KEY (upload_id, part_number)
    )""",
    """CREATE TABLE objects (
        bucket               TEXT NOT NULL REFERENCES buckets (name),
        key                  TEXT NOT NULL,
        blob_id              TEXT NOT NULL UNIQUE REFERENCES blobs (blob_id),
        size                 INTEGER NOT NULL,
        etag                 TEXT NOT NULL,
        last_modified        TEXT NOT NULL,
        content_type         TEXT NOT NULL DEFAULT 'binary/octet-stream',
        cache_control        TEXT,
        content_disposition  TEXT,
        content_encoding     TEXT,
        content_language     TEXT,
        expires              TEXT,
        user_metadata        TEXT NOT NULL DEFAULT '{}',
        upload_id            TEXT UNIQUE REFERENCES multipart_uploads (upload_id),
        PRIMARY KEY (bucket, key)
    ) WITHOUT ROWID""",
]

BUCKET = "bkt"
# blobs.created_at of the part blobs: what the migration copies into last_modified.
PART_BLOB_CREATED = {1: "2026-01-02T03:04:05.000001", 2: "2026-01-02T03:04:06.000002"}
STORE_ID = "0123456789abcdef0123456789abcdef"


@dataclass
class V2Data:
    data_dir: Path
    db_path: Path
    # Stored bytes by key, for the objects the database holds.
    objects: dict[str, bytes]
    # blob ID of each object key and of each part of the open upload.
    object_blobs: dict[str, str]
    part_blobs: dict[int, str]
    part_bytes: dict[int, bytes]
    # The quoted MD5 ETag of each part of the open upload.
    part_etags: dict[int, str]


def build_v2_database(data_dir: Path, extra_parts: int = 0) -> V2Data:
    """Create data_dir/meta.db at schema version 2 and data_dir/blobs with its files.

    Holds one bucket; objects k1 (a plain PUT) and k2 (written by a multipart Complete,
    so its upload is completed and its one part row has no blob); and the open upload
    open-up with two parts that still hold their bytes. extra_parts adds that many more
    part rows to open-up, with blobs rows but no files, to make the database larger.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "meta.db"
    blobs = BlobStore(data_dir / "blobs")
    blobs.create_root()
    blobs.create_tree()
    blobs.write_marker(STORE_ID)

    objects = {"k1": b"alpha bytes", "k2": b"assembled by a complete"}
    object_blobs = {key: new_blob_id() for key in objects}
    part_bytes = {1: b"first part bytes", 2: b"second part bytes"}
    part_blobs = {number: new_blob_id() for number in part_bytes}
    part_etags = {n: f'"{hashlib.md5(d).hexdigest()}"' for n, d in part_bytes.items()}
    for key, blob_id in object_blobs.items():
        blobs.write_blob(blob_id, BytesIO(objects[key]))
    for number, blob_id in part_blobs.items():
        blobs.write_blob(blob_id, BytesIO(part_bytes[number]))

    created = "2026-01-01T00:00:00.000000"
    conn = sqlite3.connect(db_path, isolation_level=None)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("BEGIN IMMEDIATE")
        for statement in V2_DDL:
            conn.execute(statement)
        conn.execute("INSERT INTO schema_version (version) VALUES (2)")
        conn.execute(
            "INSERT INTO store (singleton, store_id, state, blob_dir, created_at) "
            "VALUES (1, ?, 'ready', ?, ?)",
            (STORE_ID, str((data_dir / "blobs").resolve()), created),
        )
        conn.execute("INSERT INTO buckets (name, created_at) VALUES (?, ?)", (BUCKET, created))
        blob_rows = [
            (object_blobs["k1"], "object", "k1", None, None, created),
            (object_blobs["k2"], "object", "k2", "done-up", None, created),
            *(
                (part_blobs[n], "part", "mp", "open-up", n, PART_BLOB_CREATED[n])
                for n in part_bytes
            ),
        ]
        conn.executemany(
            "INSERT INTO blobs (blob_id, state, kind, bucket, key, upload_id, part_number, "
            f"created_at) VALUES (?, 'live', ?, '{BUCKET}', ?, ?, ?, ?)",
            blob_rows,
        )
        conn.execute(
            "INSERT INTO multipart_uploads (upload_id, bucket, key, initiated, state, "
            "content_type, user_metadata) VALUES "
            "('open-up', ?, 'mp', ?, 'open', 'text/plain', '{\"a\": \"b\"}'), "
            "('done-up', ?, 'k2', ?, 'completed', 'application/x-k2', '{}')",
            (BUCKET, created, BUCKET, created),
        )
        # The completed upload's part had its bytes released by Complete: blob_id NULL.
        conn.execute(
            "INSERT INTO multipart_parts (upload_id, part_number, blob_id, etag, size) "
            "VALUES ('done-up', 1, NULL, '\"d41d8cd98f00b204e9800998ecf8427e\"', 23)"
        )
        for number, data in part_bytes.items():
            conn.execute(
                "INSERT INTO multipart_parts (upload_id, part_number, blob_id, etag, size) "
                "VALUES ('open-up', ?, ?, ?, ?)",
                (number, part_blobs[number], part_etags[number], len(data)),
            )
        for i in range(extra_parts):
            blob_id = new_blob_id()
            conn.execute(
                "INSERT INTO blobs (blob_id, state, kind, bucket, key, upload_id, "
                "part_number, created_at) VALUES (?, 'live', 'part', ?, 'mp', 'open-up', ?, ?)",
                (blob_id, BUCKET, 100 + i, created),
            )
            conn.execute(
                "INSERT INTO multipart_parts (upload_id, part_number, blob_id, etag, size) "
                "VALUES ('open-up', ?, ?, ?, 1)",
                (100 + i, blob_id, f'"extra{i}"'),
            )
        conn.execute(
            "INSERT INTO objects (bucket, key, blob_id, size, etag, last_modified, "
            "content_type, user_metadata, upload_id) VALUES "
            "(?, 'k1', ?, ?, '\"e1\"', ?, 'text/plain', '{\"m\": \"1\"}', NULL), "
            "(?, 'k2', ?, ?, '\"e2-1\"', ?, 'application/x-k2', '{}', 'done-up')",
            (
                BUCKET,
                object_blobs["k1"],
                len(objects["k1"]),
                created,
                BUCKET,
                object_blobs["k2"],
                len(objects["k2"]),
                created,
            ),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()
    return V2Data(data_dir, db_path, objects, object_blobs, part_blobs, part_bytes, part_etags)
