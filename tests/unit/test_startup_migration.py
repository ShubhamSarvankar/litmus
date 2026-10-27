"""Where the schema migration sits in the startup sequence
(docs/design/storage-layout.md §7 step 7, §20): after the blob directory lock and the
store identity check, before the sweep; every refusal leaves the database as it was.
"""

import shutil
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from chaos.fixtures.v2_database import BUCKET, STORE_ID, build_v2_database
from server import app as app_module
from server.app import StartupRefused, create_app
from server.config import settings
from server.metadata.sqlite import SchemaVersionError, SQLiteMetadataStore
from server.storage.blobs import BlobStore


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    """The server's default stores point at tmp_path, which holds a v2 database."""
    monkeypatch.setattr(settings, "db_path", tmp_path / "meta.db")
    monkeypatch.setattr(settings, "blob_dir", tmp_path / "blobs")
    return tmp_path


def _version(db: Path) -> int:
    conn = sqlite3.connect(db)
    try:
        return conn.execute("SELECT version FROM schema_version").fetchone()[0]
    finally:
        conn.close()


def _schema_version_only(db: Path, version: int) -> None:
    conn = sqlite3.connect(db)
    with conn:
        conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_version VALUES (?)", (version,))
    conn.close()


def test_the_migration_waits_for_the_lock(data_dir):
    build_v2_database(data_dir)
    holder = BlobStore(data_dir / "blobs")
    fd = holder.acquire_lock()
    try:
        with pytest.raises(StartupRefused, match="already in use"):
            with TestClient(create_app()):
                pass
        assert _version(data_dir / "meta.db") == 2
    finally:
        holder.release_lock(fd)
    with TestClient(create_app()):
        pass
    assert _version(data_dir / "meta.db") == 3


@pytest.mark.parametrize("version", [1, 4])
def test_version_refusals_come_before_the_lock(data_dir, monkeypatch, version):
    _schema_version_only(data_dir / "meta.db", version)
    locked = []
    monkeypatch.setattr(app_module, "lock_blob_dir", lambda st: locked.append(st))
    with pytest.raises(SchemaVersionError):
        with TestClient(create_app()):
            pass
    assert locked == []
    assert not (data_dir / "blobs").exists()
    assert _version(data_dir / "meta.db") == version


def test_lifespan_order_is_lock_identity_migrate_sweep(data_dir, monkeypatch):
    build_v2_database(data_dir)
    calls: list[str] = []

    def spy(module, name, label):
        real = getattr(module, name)

        def wrapper(*args, **kwargs):
            calls.append(label)
            return real(*args, **kwargs)

        monkeypatch.setattr(module, name, wrapper)

    spy(app_module, "lock_blob_dir", "lock")
    spy(app_module, "establish_store_identity", "identity")
    spy(SQLiteMetadataStore, "migrate", "migrate")
    spy(app_module, "run_consistency_sweep", "sweep")
    with TestClient(create_app()):
        pass
    assert calls == ["lock", "identity", "migrate", "sweep"]


def test_a_v2_database_with_data_starts_and_serves(data_dir):
    data = build_v2_database(data_dir)
    with TestClient(create_app()) as client:
        assert client.get(f"/{BUCKET}/k1").content == data.objects["k1"]
        assert client.get(f"/{BUCKET}/k2").content == data.objects["k2"]
        assert client.put(f"/{BUCKET}/k3", content=b"written after").status_code == 200
        assert client.get(f"/{BUCKET}/k3").content == b"written after"
        listing = client.get(f"/{BUCKET}").text
        assert all(f"<Key>{key}</Key>" in listing for key in ("k1", "k2", "k3"))
    assert _version(data.db_path) == 3


def test_an_identity_refusal_leaves_v2_unmigrated(data_dir):
    build_v2_database(data_dir)
    (data_dir / "blobs" / "STORE_ID").write_text("f" * 32 + "\n")
    assert STORE_ID != "f" * 32
    with pytest.raises(StartupRefused, match="belongs to store"):
        with TestClient(create_app()):
            pass
    assert _version(data_dir / "meta.db") == 2


def test_a_missing_blob_dir_refusal_leaves_v2_unmigrated(data_dir):
    build_v2_database(data_dir)
    shutil.rmtree(data_dir / "blobs")
    with pytest.raises(StartupRefused, match="does not exist"):
        with TestClient(create_app()):
            pass
    assert _version(data_dir / "meta.db") == 2
    assert not (data_dir / "blobs").exists()
