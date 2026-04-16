import sqlite3
from datetime import datetime
from pathlib import Path

from server.metadata.base import BucketMeta, ObjectMeta, PartMeta, UploadMeta

_SCHEMA_VERSION = 1

_DDL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS buckets (
    name        TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS objects (
    bucket          TEXT NOT NULL,
    key             TEXT NOT NULL,
    size            INTEGER NOT NULL,
    etag            TEXT NOT NULL,
    last_modified   TEXT NOT NULL,
    content_type    TEXT NOT NULL DEFAULT 'binary/octet-stream',
    PRIMARY KEY (bucket, key),
    FOREIGN KEY (bucket) REFERENCES buckets(name)
);

CREATE TABLE IF NOT EXISTS multipart_uploads (
    upload_id   TEXT PRIMARY KEY,
    bucket      TEXT NOT NULL,
    key         TEXT NOT NULL,
    initiated   TEXT NOT NULL,
    completed   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS multipart_parts (
    upload_id   TEXT NOT NULL,
    part_number INTEGER NOT NULL,
    etag        TEXT NOT NULL,
    size        INTEGER NOT NULL,
    PRIMARY KEY (upload_id, part_number),
    FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id)
);
"""

_ISO = "%Y-%m-%dT%H:%M:%S.%f"


def _dt_to_str(dt: datetime) -> str:
    return dt.strftime(_ISO)


def _str_to_dt(s: str) -> datetime:
    return datetime.strptime(s, _ISO)


class SQLiteMetadataStore:
    def __init__(self, db_path: Path):
        self._db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._migrate()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        return conn

    def _migrate(self) -> None:
        conn = self._connect()
        with conn:
            conn.executescript(_DDL)
            row = conn.execute("SELECT version FROM schema_version").fetchone()
            if row is None:
                conn.execute("INSERT INTO schema_version (version) VALUES (?)", (_SCHEMA_VERSION,))
        conn.close()

    # --- Buckets ---

    def create_bucket(self, bucket: str, created_at: datetime) -> None:
        conn = self._connect()
        with conn:
            conn.execute(
                "INSERT INTO buckets (name, created_at) VALUES (?, ?)",
                (bucket, _dt_to_str(created_at)),
            )
        conn.close()

    def delete_bucket(self, bucket: str) -> None:
        conn = self._connect()
        with conn:
            conn.execute("DELETE FROM buckets WHERE name = ?", (bucket,))
        conn.close()

    def bucket_exists(self, bucket: str) -> bool:
        conn = self._connect()
        row = conn.execute("SELECT 1 FROM buckets WHERE name = ?", (bucket,)).fetchone()
        conn.close()
        return row is not None

    def list_buckets(self) -> list[BucketMeta]:
        conn = self._connect()
        rows = conn.execute("SELECT name, created_at FROM buckets ORDER BY name").fetchall()
        conn.close()
        return [BucketMeta(name=r["name"], created_at=_str_to_dt(r["created_at"])) for r in rows]

    # --- Objects ---

    def put_object_meta(self, bucket: str, key: str, meta: ObjectMeta) -> None:
        conn = self._connect()
        with conn:
            conn.execute(
                """INSERT INTO objects (bucket, key, size, etag, last_modified, content_type)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(bucket, key) DO UPDATE SET
                       size         = excluded.size,
                       etag         = excluded.etag,
                       last_modified = excluded.last_modified,
                       content_type = excluded.content_type""",
                (
                    meta.bucket,
                    meta.key,
                    meta.size,
                    meta.etag,
                    _dt_to_str(meta.last_modified),
                    meta.content_type,
                ),
            )
        conn.close()

    def get_object_meta(self, bucket: str, key: str) -> ObjectMeta | None:
        conn = self._connect()
        row = conn.execute(
            "SELECT * FROM objects WHERE bucket = ? AND key = ?", (bucket, key)
        ).fetchone()
        conn.close()
        if row is None:
            return None
        return ObjectMeta(
            bucket=row["bucket"],
            key=row["key"],
            size=row["size"],
            etag=row["etag"],
            last_modified=_str_to_dt(row["last_modified"]),
            content_type=row["content_type"],
        )

    def delete_object_meta(self, bucket: str, key: str) -> None:
        conn = self._connect()
        with conn:
            conn.execute("DELETE FROM objects WHERE bucket = ? AND key = ?", (bucket, key))
        conn.close()

    def list_objects(self, bucket: str, prefix: str | None = None) -> list[ObjectMeta]:
        conn = self._connect()
        if prefix:
            rows = conn.execute(
                "SELECT * FROM objects WHERE bucket = ? AND key LIKE ? ORDER BY key",
                (bucket, prefix + "%"),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM objects WHERE bucket = ? ORDER BY key", (bucket,)
            ).fetchall()
        conn.close()
        return [
            ObjectMeta(
                bucket=r["bucket"],
                key=r["key"],
                size=r["size"],
                etag=r["etag"],
                last_modified=_str_to_dt(r["last_modified"]),
                content_type=r["content_type"],
            )
            for r in rows
        ]

    def object_count(self, bucket: str) -> int:
        conn = self._connect()
        row = conn.execute("SELECT COUNT(*) FROM objects WHERE bucket = ?", (bucket,)).fetchone()
        conn.close()
        return row[0]

    # --- Multipart uploads ---

    def create_upload(self, upload_id: str, bucket: str, key: str, initiated: datetime) -> None:
        conn = self._connect()
        with conn:
            conn.execute(
                "INSERT INTO multipart_uploads (upload_id, bucket, key, initiated) "
                "VALUES (?, ?, ?, ?)",
                (upload_id, bucket, key, _dt_to_str(initiated)),
            )
        conn.close()

    def record_part(self, upload_id: str, part: PartMeta) -> None:
        conn = self._connect()
        with conn:
            conn.execute(
                """INSERT INTO multipart_parts (upload_id, part_number, etag, size)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(upload_id, part_number) DO UPDATE SET
                       etag = excluded.etag,
                       size = excluded.size""",
                (upload_id, part.part_number, part.etag, part.size),
            )
        conn.close()

    def get_parts(self, upload_id: str) -> list[PartMeta]:
        conn = self._connect()
        rows = conn.execute(
            "SELECT * FROM multipart_parts WHERE upload_id = ? ORDER BY part_number",
            (upload_id,),
        ).fetchall()
        conn.close()
        return [
            PartMeta(
                upload_id=r["upload_id"],
                part_number=r["part_number"],
                etag=r["etag"],
                size=r["size"],
            )
            for r in rows
        ]

    def complete_upload(self, upload_id: str) -> None:
        conn = self._connect()
        with conn:
            conn.execute(
                "UPDATE multipart_uploads SET completed = 1 WHERE upload_id = ?",
                (upload_id,),
            )
        conn.close()

    def abort_upload(self, upload_id: str) -> None:
        conn = self._connect()
        with conn:
            conn.execute("DELETE FROM multipart_parts WHERE upload_id = ?", (upload_id,))
            conn.execute("DELETE FROM multipart_uploads WHERE upload_id = ?", (upload_id,))
        conn.close()

    def list_incomplete_uploads(self) -> list[UploadMeta]:
        conn = self._connect()
        rows = conn.execute("SELECT * FROM multipart_uploads WHERE completed = 0").fetchall()
        conn.close()
        return [
            UploadMeta(
                upload_id=r["upload_id"],
                bucket=r["bucket"],
                key=r["key"],
                initiated=_str_to_dt(r["initiated"]),
            )
            for r in rows
        ]

    def upload_exists(self, upload_id: str) -> bool:
        conn = self._connect()
        row = conn.execute(
            "SELECT 1 FROM multipart_uploads WHERE upload_id = ? AND completed = 0",
            (upload_id,),
        ).fetchone()
        conn.close()
        return row is not None
