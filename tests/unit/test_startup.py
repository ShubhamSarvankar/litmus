"""Startup (docs/design/storage-layout.md §7) as far as step (a) implements it before
the lock and store ID: refuse the old layout's variables and schema version 1, build
the stores in the startup hook, create the blob tree, run the sweep."""

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server import app as app_module
from server.app import StartupRefused, create_app
from server.config import settings
from server.metadata.sqlite import SchemaVersionError, SQLiteMetadataStore
from server.storage.blobs import FANOUT, BlobStore


def _files(root: Path) -> set[Path]:
    return set(root.rglob("*")) if root.exists() else set()


@pytest.fixture
def default_paths(tmp_path, monkeypatch):
    """Point the server's default stores at tmp_path."""
    monkeypatch.setattr(settings, "db_path", tmp_path / "meta.db")
    monkeypatch.setattr(settings, "blob_dir", tmp_path / "blobs")
    return tmp_path


def test_startup_hook_runs_in_process(tmp_path):
    """With stores passed in, the startup hook still runs the sweep when the app is
    started with `with TestClient(...)`."""
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    app = create_app(storage=storage, metadata=metadata)
    # create_app made the tree; plant leftovers of a write that died.
    blob_id = metadata.register_blob("object", "b", "k")
    storage.temp_path(blob_id).write_bytes(b"partial")
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        assert storage.temp_entries() == []
        assert metadata.list_blob_records() == []


def test_create_app_without_stores_touches_nothing(default_paths):
    create_app()
    assert _files(default_paths) == set()


def test_startup_builds_default_stores_and_tree(default_paths):
    with TestClient(create_app()) as client:
        assert client.put("/b").status_code == 200
        assert client.put("/b/k", content=b"hello").status_code == 200
        assert client.get("/b/k").content == b"hello"
    blobs = default_paths / "blobs"
    assert sorted(p.name for p in blobs.iterdir()) == sorted([*FANOUT, "tmp"])
    assert (default_paths / "meta.db").is_file()


@pytest.mark.parametrize("name", ["S3_DATA_DIR", "S3_PARTS_DIR"])
def test_startup_refuses_old_layout_variables(default_paths, monkeypatch, name):
    monkeypatch.setenv(name, str(default_paths / "old"))
    with pytest.raises(StartupRefused, match=name):
        with TestClient(create_app()):
            pass
    assert _files(default_paths) == set()


def test_old_layout_variables_refused_with_stores_passed_in(tmp_path, monkeypatch):
    app = create_app(
        storage=BlobStore(tmp_path / "blobs"),
        metadata=SQLiteMetadataStore(db_path=tmp_path / "meta.db"),
    )
    monkeypatch.setenv("S3_DATA_DIR", str(tmp_path / "old"))
    with pytest.raises(StartupRefused):
        with TestClient(app):
            pass


def _version_1_database(path: Path) -> None:
    conn = sqlite3.connect(path)
    with conn:
        conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_version VALUES (1)")
        conn.execute("CREATE TABLE buckets (name TEXT PRIMARY KEY, created_at TEXT NOT NULL)")
    conn.close()


def test_store_refuses_schema_version_1(tmp_path):
    _version_1_database(tmp_path / "meta.db")
    with pytest.raises(SchemaVersionError, match="version 1"):
        SQLiteMetadataStore(db_path=tmp_path / "meta.db")


def test_startup_refuses_schema_version_1_and_changes_nothing(default_paths):
    _version_1_database(default_paths / "meta.db")
    before = (default_paths / "meta.db").read_bytes()
    with pytest.raises(SchemaVersionError):
        with TestClient(create_app()):
            pass
    assert (default_paths / "meta.db").read_bytes() == before
    assert not (default_paths / "blobs").exists()


def test_store_refuses_tables_without_a_schema_version(tmp_path):
    conn = sqlite3.connect(tmp_path / "meta.db")
    conn.execute("CREATE TABLE something (x)")
    conn.close()
    with pytest.raises(SchemaVersionError):
        SQLiteMetadataStore(db_path=tmp_path / "meta.db")


def test_module_level_app_builds_no_stores():
    assert app_module.app.state.storage is None
    assert app_module.app.state.metadata is None
