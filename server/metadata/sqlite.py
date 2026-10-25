import json
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from server.metadata.base import (
    BlobRecord,
    BucketHasObjects,
    BucketMeta,
    BucketMissing,
    ListPage,
    ObjectMeta,
    PartMeta,
    PartsChanged,
    StoreRecord,
    UploadMeta,
    UploadNotOpen,
)
from server.storage.blobs import new_blob_id

# Schema version 2: docs/design/storage-layout.md §5. Version 1 (keys mapped to
# paths) is refused, not migrated (decision 4).
_SCHEMA_VERSION = 2

_DDL = [
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

_ISO = "%Y-%m-%dT%H:%M:%S.%f"


def _dt_to_str(dt: datetime) -> str:
    return dt.strftime(_ISO)


def _str_to_dt(s: str) -> datetime:
    return datetime.strptime(s, _ISO)


def _now() -> str:
    return _dt_to_str(datetime.now(UTC).replace(tzinfo=None))


_MAX_CODE_POINT = 0x10FFFF
_SURROGATES = range(0xD800, 0xE000)


def key_successor(prefix: str) -> str | None:
    """The least string greater than every string that starts with prefix, or None
    when there is none (the empty prefix, or one made only of U+10FFFF).

    BINARY collation compares UTF-8 bytes, which orders keys by code point
    (docs/design/storage-layout.md §13). The successor drops trailing U+10FFFF
    characters and increments the last remaining one, skipping the surrogates, which
    UTF-8 cannot encode.
    """
    chars = list(prefix)
    while chars:
        last = ord(chars.pop())
        if last < _MAX_CODE_POINT:
            nxt = last + 1
            if nxt in _SURROGATES:
                nxt = _SURROGATES.stop
            return "".join(chars) + chr(nxt)
    return None


def key_range_query(
    bucket: str,
    prefix: str,
    start_after: str | None,
    limit: int | None,
    start_at: str | None = None,
) -> tuple[str, tuple]:
    """SELECT for the objects of bucket whose key starts with prefix, sorts after
    start_after and not before start_at, in key order: a range on the (bucket, key)
    primary key, never LIKE (whose _ and % are wildcards and which ignores ASCII
    case)."""
    # The tightest lower bound wins; on a tie, the exclusive one.
    lower, inclusive = prefix, True
    if start_at is not None and start_at > lower:
        lower = start_at
    if start_after is not None and start_after >= lower:
        lower, inclusive = start_after, False
    where, params = ["bucket = ?", "key >= ?" if inclusive else "key > ?"], [bucket, lower]
    upper = key_successor(prefix)
    if upper is not None:
        where.append("key < ?")
        params.append(upper)
    sql = f"SELECT * FROM objects WHERE {' AND '.join(where)} ORDER BY key"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    return sql, tuple(params)


def common_prefix(key: str, prefix: str, delimiter: str) -> str | None:
    """The common prefix key rolls up into: key up to and including the first
    delimiter after prefix, or None when there is no delimiter after prefix."""
    if not delimiter or not key.startswith(prefix):
        return None
    i = key.find(delimiter, len(prefix))
    return None if i < 0 else key[: i + len(delimiter)]


class SchemaVersionError(RuntimeError):
    """The database was written by another schema version."""


class RegistryError(RuntimeError):
    """A registry row is not in the state the commit requires."""


def _object(row: sqlite3.Row) -> ObjectMeta:
    return ObjectMeta(
        bucket=row["bucket"],
        key=row["key"],
        size=row["size"],
        etag=row["etag"],
        last_modified=_str_to_dt(row["last_modified"]),
        content_type=row["content_type"],
        blob_id=row["blob_id"],
        upload_id=row["upload_id"],
        cache_control=row["cache_control"],
        content_disposition=row["content_disposition"],
        content_encoding=row["content_encoding"],
        content_language=row["content_language"],
        expires=row["expires"],
        user_metadata=json.loads(row["user_metadata"]),
    )


def _upload(row: sqlite3.Row) -> UploadMeta:
    return UploadMeta(
        upload_id=row["upload_id"],
        bucket=row["bucket"],
        key=row["key"],
        initiated=_str_to_dt(row["initiated"]),
        state=row["state"],
        content_type=row["content_type"],
        cache_control=row["cache_control"],
        content_disposition=row["content_disposition"],
        content_encoding=row["content_encoding"],
        content_language=row["content_language"],
        expires=row["expires"],
        user_metadata=json.loads(row["user_metadata"]),
    )


class SQLiteMetadataStore:
    def __init__(self, db_path: Path):
        self._db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._migrate()

    def _connect(self) -> sqlite3.Connection:
        # isolation_level=None: transactions are explicit (BEGIN IMMEDIATE below), so
        # every read a commit depends on happens under the write lock.
        conn = sqlite3.connect(str(self._db_path), timeout=10.0, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        """One write transaction: BEGIN IMMEDIATE ... COMMIT, rolled back on any
        exception."""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                # SQLite may already have rolled back on its own (e.g. SQLITE_FULL).
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        finally:
            conn.close()

    @contextmanager
    def _snapshot(self) -> Iterator[sqlite3.Connection]:
        """One read transaction: every query inside it sees the same committed state."""
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            try:
                yield conn
            finally:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
        finally:
            conn.close()

    def _read(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        conn = self._connect()
        try:
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()

    def _refuse_other_versions(self) -> None:
        """Read the schema version over a plain connection, before any PRAGMA can
        change the file, and refuse a database of another schema version."""
        if not self._db_path.exists():
            return
        conn = sqlite3.connect(str(self._db_path), isolation_level=None)
        try:
            tables = {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
            version = None
            if "schema_version" in tables:
                row = conn.execute("SELECT version FROM schema_version").fetchone()
                version = row[0] if row else None
        finally:
            conn.close()
        self._check_version(tables, version)

    def _check_version(self, tables: set[str], version: int | None) -> None:
        if "schema_version" in tables and version != _SCHEMA_VERSION:
            raise SchemaVersionError(
                f"{self._db_path} has schema version {version}; this server needs "
                f"version {_SCHEMA_VERSION}. The storage layout changed and old data "
                "is not migrated: empty the data directory or use a new one."
            )
        if tables and "schema_version" not in tables:
            raise SchemaVersionError(
                f"{self._db_path} has tables but no schema version: {sorted(tables)}"
            )

    def _migrate(self) -> None:
        self._refuse_other_versions()
        with self._write() as conn:
            tables = {
                r["name"]
                for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
            # Checked again under the write lock, in case the file appeared meanwhile.
            version = None
            if "schema_version" in tables:
                row = conn.execute("SELECT version FROM schema_version").fetchone()
                version = row["version"] if row else None
            self._check_version(tables, version)
            if tables:
                return
            for statement in _DDL:
                conn.execute(statement)
            conn.execute("INSERT INTO schema_version (version) VALUES (?)", (_SCHEMA_VERSION,))

    # --- Store identity (I7, §7) ---

    def get_store(self) -> StoreRecord | None:
        rows = self._read("SELECT store_id, state, blob_dir FROM store")
        if not rows:
            return None
        r = rows[0]
        return StoreRecord(store_id=r["store_id"], state=r["state"], blob_dir=r["blob_dir"])

    def create_store(self, store_id: str, blob_dir: str) -> bool:
        """Insert the store row as 'initializing'. False, changing nothing, if a row
        already exists (another server initialized this database meanwhile)."""
        try:
            with self._write() as conn:
                conn.execute(
                    "INSERT INTO store (singleton, store_id, state, blob_dir, created_at) "
                    "VALUES (1, ?, 'initializing', ?, ?)",
                    (store_id, blob_dir, _now()),
                )
        except sqlite3.IntegrityError:
            if self.get_store() is None:
                raise
            return False
        return True

    def mark_store_ready(self, store_id: str) -> None:
        with self._write() as conn:
            cur = conn.execute(
                "UPDATE store SET state = 'ready' WHERE store_id = ? AND state = 'initializing'",
                (store_id,),
            )
            if cur.rowcount != 1:
                raise RegistryError(f"store {store_id} is not initializing")

    def holds_data(self) -> bool:
        """Whether any table other than store and schema_version has a row."""
        conn = self._connect()
        try:
            return any(
                conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
                for table in ("buckets", "blobs", "multipart_uploads", "multipart_parts", "objects")
            )
        finally:
            conn.close()

    # --- Buckets ---

    def create_bucket(self, bucket: str, created_at: datetime) -> None:
        with self._write() as conn:
            conn.execute(
                "INSERT INTO buckets (name, created_at) VALUES (?, ?)",
                (bucket, _dt_to_str(created_at)),
            )

    def delete_bucket(self, bucket: str) -> list[str]:
        """Delete an empty bucket, aborting its open uploads in the same transaction
        (decision 7). Returns the part blobs that became garbage."""
        with self._write() as conn:
            if not conn.execute("SELECT 1 FROM buckets WHERE name = ?", (bucket,)).fetchone():
                raise BucketMissing(bucket)
            if conn.execute("SELECT 1 FROM objects WHERE bucket = ? LIMIT 1", (bucket,)).fetchone():
                raise BucketHasObjects(bucket)
            # Only open uploads can remain: a completed upload's rows live exactly as
            # long as its object, and the bucket has none (§5, §6.9). The foreign key
            # on multipart_uploads.bucket refuses the delete below if that ever fails.
            garbage = []
            uploads = conn.execute(
                "SELECT upload_id FROM multipart_uploads WHERE bucket = ? AND state = 'open'",
                (bucket,),
            ).fetchall()
            for (upload_id,) in uploads:
                garbage += self._drop_upload(conn, upload_id)
            conn.execute("DELETE FROM buckets WHERE name = ?", (bucket,))
        return garbage

    def bucket_exists(self, bucket: str) -> bool:
        return bool(self._read("SELECT 1 FROM buckets WHERE name = ?", (bucket,)))

    def list_buckets(self) -> list[BucketMeta]:
        rows = self._read("SELECT name, created_at FROM buckets ORDER BY name")
        return [BucketMeta(name=r["name"], created_at=_str_to_dt(r["created_at"])) for r in rows]

    # --- Blob registry ---

    def register_blob(
        self,
        kind: str,
        bucket: str,
        key: str,
        upload_id: str | None = None,
        part_number: int | None = None,
    ) -> str:
        """Commit a pending registry row for a new blob ID (REGISTER, §6.1)."""
        while True:
            blob_id = new_blob_id()
            try:
                with self._write() as conn:
                    conn.execute(
                        "INSERT INTO blobs (blob_id, state, kind, bucket, key, upload_id, "
                        "part_number, created_at) VALUES (?, 'pending', ?, ?, ?, ?, ?, ?)",
                        (blob_id, kind, bucket, key, upload_id, part_number, _now()),
                    )
                return blob_id
            except sqlite3.IntegrityError:
                if self.blob_state(blob_id) is None:
                    raise
                # A 2^-128 coincidence: the ID is taken. Draw another.

    def release_blob(self, blob_id: str) -> None:
        """Drop the row of a pending blob that will never be referenced (RELEASE)."""
        with self._write() as conn:
            conn.execute("DELETE FROM blobs WHERE blob_id = ? AND state = 'pending'", (blob_id,))

    def blob_state(self, blob_id: str) -> str | None:
        rows = self._read("SELECT state FROM blobs WHERE blob_id = ?", (blob_id,))
        return rows[0]["state"] if rows else None

    def purge_garbage(self, blob_ids: list[str]) -> None:
        """Drop garbage rows whose files are gone (GC step 2)."""
        if not blob_ids:
            return
        with self._write() as conn:
            conn.executemany(
                "DELETE FROM blobs WHERE blob_id = ? AND state = 'garbage'",
                [(b,) for b in blob_ids],
            )

    @staticmethod
    def _make_live(conn: sqlite3.Connection, blob_id: str) -> None:
        cur = conn.execute(
            "UPDATE blobs SET state = 'live' WHERE blob_id = ? AND state = 'pending'", (blob_id,)
        )
        if cur.rowcount != 1:
            raise RegistryError(f"blob {blob_id} is not pending")

    @staticmethod
    def _make_garbage(conn: sqlite3.Connection, blob_id: str) -> None:
        conn.execute("UPDATE blobs SET state = 'garbage' WHERE blob_id = ?", (blob_id,))

    # --- Objects ---

    @staticmethod
    def _upsert_object(conn: sqlite3.Connection, meta: ObjectMeta) -> None:
        conn.execute(
            """INSERT INTO objects (bucket, key, blob_id, size, etag, last_modified,
                                    content_type, cache_control, content_disposition,
                                    content_encoding, content_language, expires,
                                    user_metadata, upload_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(bucket, key) DO UPDATE SET
                   blob_id             = excluded.blob_id,
                   size                = excluded.size,
                   etag                = excluded.etag,
                   last_modified       = excluded.last_modified,
                   content_type        = excluded.content_type,
                   cache_control       = excluded.cache_control,
                   content_disposition = excluded.content_disposition,
                   content_encoding    = excluded.content_encoding,
                   content_language    = excluded.content_language,
                   expires             = excluded.expires,
                   user_metadata       = excluded.user_metadata,
                   upload_id           = excluded.upload_id""",
            (
                meta.bucket,
                meta.key,
                meta.blob_id,
                meta.size,
                meta.etag,
                _dt_to_str(meta.last_modified),
                meta.content_type,
                meta.cache_control,
                meta.content_disposition,
                meta.content_encoding,
                meta.content_language,
                meta.expires,
                json.dumps(meta.user_metadata, ensure_ascii=False, sort_keys=True),
                meta.upload_id,
            ),
        )

    def _retire_object(
        self, conn: sqlite3.Connection, old: sqlite3.Row, keep_upload: str | None = None
    ) -> str:
        """The object version old was replaced or deleted: its blob becomes garbage,
        and the rows of the completed upload that wrote it go with it."""
        self._make_garbage(conn, old["blob_id"])
        if old["upload_id"] and old["upload_id"] != keep_upload:
            conn.execute("DELETE FROM multipart_parts WHERE upload_id = ?", (old["upload_id"],))
            conn.execute("DELETE FROM multipart_uploads WHERE upload_id = ?", (old["upload_id"],))
        return old["blob_id"]

    @staticmethod
    def _current(conn: sqlite3.Connection, bucket: str, key: str) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT blob_id, upload_id, etag FROM objects WHERE bucket = ? AND key = ?",
            (bucket, key),
        ).fetchone()

    def put_object_meta(
        self,
        bucket: str,
        key: str,
        meta: ObjectMeta,
        precondition: Callable[[str | None], None] | None = None,
    ) -> str | None:
        """The PutObject commit (§6.2 P4). Returns the replaced blob, now garbage.

        precondition, when given, is called with the ETag of the row being replaced
        (None when there is none) after the write lock is taken; what it raises rolls
        the commit back and propagates. A meta whose last_modified is None gets the
        time of this commit, taken under the lock (§9)."""
        with self._write() as conn:
            if not conn.execute("SELECT 1 FROM buckets WHERE name = ?", (bucket,)).fetchone():
                raise BucketMissing(bucket)
            old = self._current(conn, bucket, key)
            if precondition is not None:
                precondition(old["etag"] if old else None)
            if meta.last_modified is None:
                meta.last_modified = datetime.now(UTC).replace(tzinfo=None)
            self._upsert_object(conn, meta)
            self._make_live(conn, meta.blob_id)
            return self._retire_object(conn, old) if old else None

    def get_object_meta(self, bucket: str, key: str) -> ObjectMeta | None:
        rows = self._read("SELECT * FROM objects WHERE bucket = ? AND key = ?", (bucket, key))
        return _object(rows[0]) if rows else None

    def delete_object_meta(self, bucket: str, key: str) -> list[str]:
        """The DeleteObject commit (§6.3 D2). Returns the blob that became garbage."""
        return self.delete_objects(bucket, [key])

    def delete_objects(self, bucket: str, keys: list[str]) -> list[str]:
        """Delete every key in one transaction (§6.4 X2). Returns the blobs that
        became garbage."""
        garbage = []
        with self._write() as conn:
            for key in keys:
                old = self._current(conn, bucket, key)
                if old is None:
                    continue
                conn.execute("DELETE FROM objects WHERE bucket = ? AND key = ?", (bucket, key))
                garbage.append(self._retire_object(conn, old))
        return garbage

    def list_objects(
        self,
        bucket: str,
        prefix: str | None = None,
        start_after: str | None = None,
        limit: int | None = None,
    ) -> list[ObjectMeta]:
        """Objects whose key starts with prefix (an exact, case sensitive prefix) and
        sorts after start_after, in key order, at most limit of them; one indexed
        range read in one read transaction."""
        sql, params = key_range_query(bucket, prefix or "", start_after, limit)
        with self._snapshot() as conn:
            return [_object(r) for r in conn.execute(sql, params).fetchall()]

    def list_page(
        self,
        bucket: str,
        prefix: str,
        delimiter: str,
        marker: str,
        max_keys: int,
        start_at: str | None = None,
    ) -> ListPage:
        """One listing page (docs/design/storage-layout.md §13): keys and common prefixes
        after marker, in key order, at most max_keys of them together, read in one
        read transaction.

        A key with delimiter after prefix rolls up into its common prefix. A common
        prefix is returned only if it sorts after marker (as S3 documents for
        ListObjects), so a marker equal to or inside a common prefix skips every key
        under it. After returning a common prefix, the next read starts at its
        successor, past every key under it.

        start_at, when given, replaces marker: the page starts at that key, inclusive.
        It is the restart position of an earlier page (ListPage.restart_at).
        """
        page = ListPage()
        if max_keys <= 0:
            return page
        start_after = None
        if start_at is None and marker:
            start_after = marker
            marker_prefix = common_prefix(marker, prefix, delimiter)
            if marker_prefix is not None:
                start_after, start_at = None, key_successor(marker_prefix)
                if start_at is None:
                    return page
        with self._snapshot() as conn:
            while True:
                # One row more than the page still needs, to tell whether it is truncated.
                limit = max_keys - page.count + 1
                sql, params = key_range_query(bucket, prefix, start_after, limit, start_at)
                rows = conn.execute(sql, params).fetchall()
                skipped_to = None
                for row in rows:
                    if page.count == max_keys:
                        page.is_truncated = True
                        return page
                    key = row["key"]
                    rolled_up = common_prefix(key, prefix, delimiter)
                    if rolled_up is None:
                        page.objects.append(_object(row))
                        page.next_marker = key
                        page.restart_after, page.restart_at = key, None
                        start_after = key
                        continue
                    page.common_prefixes.append(rolled_up)
                    page.next_marker = rolled_up
                    skipped_to = key_successor(rolled_up)
                    page.restart_after, page.restart_at = None, skipped_to
                    break
                else:
                    if len(rows) < limit:
                        return page
                    continue
                if skipped_to is None:
                    return page
                start_after, start_at = None, skipped_to

    # --- Multipart uploads ---

    def create_upload(
        self,
        upload_id: str,
        bucket: str,
        key: str,
        initiated: datetime,
        *,
        content_type: str = "binary/octet-stream",
        cache_control: str | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        content_language: str | None = None,
        expires: str | None = None,
        user_metadata: dict[str, str] | None = None,
    ) -> None:
        with self._write() as conn:
            if not conn.execute("SELECT 1 FROM buckets WHERE name = ?", (bucket,)).fetchone():
                raise BucketMissing(bucket)
            conn.execute(
                """INSERT INTO multipart_uploads (upload_id, bucket, key, initiated, state,
                                                  content_type, cache_control,
                                                  content_disposition, content_encoding,
                                                  content_language, expires, user_metadata)
                   VALUES (?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?)""",
                (
                    upload_id,
                    bucket,
                    key,
                    _dt_to_str(initiated),
                    content_type,
                    cache_control,
                    content_disposition,
                    content_encoding,
                    content_language,
                    expires,
                    json.dumps(user_metadata or {}, ensure_ascii=False, sort_keys=True),
                ),
            )

    def get_upload(self, upload_id: str) -> UploadMeta | None:
        rows = self._read("SELECT * FROM multipart_uploads WHERE upload_id = ?", (upload_id,))
        return _upload(rows[0]) if rows else None

    def record_part(self, upload_id: str, part: PartMeta) -> str | None:
        """The UploadPart commit (§6.6 U4). Returns the replaced part blob, now
        garbage. Raises UploadNotOpen if the upload is not open."""
        if part.blob_id is None:
            raise ValueError("record_part needs the blob ID of the part's bytes")
        with self._write() as conn:
            row = conn.execute(
                "SELECT state FROM multipart_uploads WHERE upload_id = ?", (upload_id,)
            ).fetchone()
            if row is None or row["state"] != "open":
                raise UploadNotOpen(upload_id)
            old = conn.execute(
                "SELECT blob_id FROM multipart_parts WHERE upload_id = ? AND part_number = ?",
                (upload_id, part.part_number),
            ).fetchone()
            conn.execute(
                """INSERT INTO multipart_parts (upload_id, part_number, blob_id, etag, size)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(upload_id, part_number) DO UPDATE SET
                       blob_id = excluded.blob_id,
                       etag    = excluded.etag,
                       size    = excluded.size""",
                (upload_id, part.part_number, part.blob_id, part.etag, part.size),
            )
            self._make_live(conn, part.blob_id)
            if old is not None and old["blob_id"] is not None:
                self._make_garbage(conn, old["blob_id"])
                return old["blob_id"]
        return None

    def get_parts(self, upload_id: str) -> list[PartMeta]:
        rows = self._read(
            "SELECT * FROM multipart_parts WHERE upload_id = ? ORDER BY part_number",
            (upload_id,),
        )
        return [
            PartMeta(
                upload_id=r["upload_id"],
                part_number=r["part_number"],
                etag=r["etag"],
                size=r["size"],
                blob_id=r["blob_id"],
            )
            for r in rows
        ]

    def commit_completed_upload(
        self,
        upload_id: str,
        meta: ObjectMeta,
        part_blob_ids: dict[int, str] | None = None,
        released: list[str] | None = None,
    ) -> bool:
        """The Complete commit (§6.7 C4): mark the open upload complete and commit
        its object row in one transaction, so a crash or error leaves either both or
        neither.

        Returns False, changing nothing, if the upload is not open. Raises
        PartsChanged if a part in part_blob_ids no longer names that blob. Blobs this
        makes garbage (the replaced object blob and every part blob) are appended to
        released.
        """
        part_blob_ids = part_blob_ids or {}
        with self._write() as conn:
            # The compare-and-set on the upload state runs first; everything after it
            # is undone by the rollback if a later statement fails.
            cur = conn.execute(
                "UPDATE multipart_uploads SET state = 'completed' "
                "WHERE upload_id = ? AND state = 'open'",
                (upload_id,),
            )
            if cur.rowcount != 1:
                return False
            for part_number, blob_id in part_blob_ids.items():
                row = conn.execute(
                    "SELECT blob_id FROM multipart_parts WHERE upload_id = ? AND part_number = ?",
                    (upload_id, part_number),
                ).fetchone()
                if row is None or row["blob_id"] != blob_id:
                    raise PartsChanged(part_number)
            old = self._current(conn, meta.bucket, meta.key)
            self._upsert_object(conn, meta)
            self._make_live(conn, meta.blob_id)
            garbage = [self._retire_object(conn, old, keep_upload=upload_id)] if old else []
            parts = conn.execute(
                "SELECT part_number, blob_id FROM multipart_parts WHERE upload_id = ?",
                (upload_id,),
            ).fetchall()
            for part in parts:
                if part["blob_id"] is not None:
                    self._make_garbage(conn, part["blob_id"])
                    garbage.append(part["blob_id"])
                if part["part_number"] in part_blob_ids:
                    # Listed parts keep their ETag and size (R21); their bytes now
                    # live in the assembled blob.
                    conn.execute(
                        "UPDATE multipart_parts SET blob_id = NULL "
                        "WHERE upload_id = ? AND part_number = ?",
                        (upload_id, part["part_number"]),
                    )
                else:
                    conn.execute(
                        "DELETE FROM multipart_parts WHERE upload_id = ? AND part_number = ?",
                        (upload_id, part["part_number"]),
                    )
        if released is not None:
            released.extend(garbage)
        return True

    @staticmethod
    def _drop_upload(conn: sqlite3.Connection, upload_id: str) -> list[str]:
        rows = conn.execute(
            "SELECT blob_id FROM multipart_parts WHERE upload_id = ? AND blob_id IS NOT NULL",
            (upload_id,),
        ).fetchall()
        garbage = [r["blob_id"] for r in rows]
        conn.executemany(
            "UPDATE blobs SET state = 'garbage' WHERE blob_id = ?", [(b,) for b in garbage]
        )
        conn.execute("DELETE FROM multipart_parts WHERE upload_id = ?", (upload_id,))
        conn.execute("DELETE FROM multipart_uploads WHERE upload_id = ?", (upload_id,))
        return garbage

    def abort_upload(self, upload_id: str) -> list[str] | None:
        """The Abort commit (§6.8 A2). Returns the part blobs that became garbage,
        or None, changing nothing, if the upload is not open."""
        with self._write() as conn:
            row = conn.execute(
                "SELECT state FROM multipart_uploads WHERE upload_id = ?", (upload_id,)
            ).fetchone()
            if row is None or row["state"] != "open":
                return None
            return self._drop_upload(conn, upload_id)

    def list_incomplete_uploads(self) -> list[UploadMeta]:
        rows = self._read("SELECT * FROM multipart_uploads WHERE state = 'open'")
        return [_upload(r) for r in rows]

    def upload_exists(self, upload_id: str) -> bool:
        return bool(
            self._read(
                "SELECT 1 FROM multipart_uploads WHERE upload_id = ? AND state = 'open'",
                (upload_id,),
            )
        )

    # --- For the startup sweep ---

    def list_blob_records(self) -> list[BlobRecord]:
        rows = self._read("SELECT * FROM blobs ORDER BY blob_id")
        return [
            BlobRecord(
                blob_id=r["blob_id"],
                state=r["state"],
                kind=r["kind"],
                bucket=r["bucket"],
                key=r["key"],
                upload_id=r["upload_id"],
                part_number=r["part_number"],
            )
            for r in rows
        ]

    def blob_is_referenced(self, blob_id: str) -> bool:
        return bool(
            self._read(
                "SELECT 1 FROM objects WHERE blob_id = ? "
                "UNION ALL SELECT 1 FROM multipart_parts WHERE blob_id = ?",
                (blob_id, blob_id),
            )
        )

    def delete_blob_record(self, blob_id: str, state: str) -> None:
        with self._write() as conn:
            conn.execute("DELETE FROM blobs WHERE blob_id = ? AND state = ?", (blob_id, state))

    def object_references(self) -> list[sqlite3.Row]:
        """Every object row with the state of the blob it names."""
        return self._read(
            "SELECT o.bucket, o.key, o.blob_id, o.size, b.state AS blob_state "
            "FROM objects o LEFT JOIN blobs b ON b.blob_id = o.blob_id "
            "ORDER BY o.bucket, o.key"
        )

    def part_references(self) -> list[sqlite3.Row]:
        """Every part row that names a blob, with its upload's and blob's state."""
        return self._read(
            "SELECT p.upload_id, p.part_number, p.blob_id, p.size, "
            "u.state AS upload_state, b.state AS blob_state "
            "FROM multipart_parts p "
            "JOIN multipart_uploads u ON u.upload_id = p.upload_id "
            "LEFT JOIN blobs b ON b.blob_id = p.blob_id "
            "WHERE p.blob_id IS NOT NULL ORDER BY p.upload_id, p.part_number"
        )
