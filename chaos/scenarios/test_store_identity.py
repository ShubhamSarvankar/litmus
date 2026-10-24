"""
Store identity and the blob directory lock (docs/design/storage-layout.md I6, I7,
§7, decision 16), with real server processes.

Each refusal test asserts that the server exits non-zero before accepting requests,
that its log names the mismatch, and that the blob directories and the database are
unchanged: the same files with the same bytes, except that a LOCK file may appear.
"""

import os
import signal
import sqlite3
from datetime import datetime
from pathlib import Path

import httpx
import pytest

from chaos.fixtures.subprocess import ServerProcess, _find_free_port
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore, new_blob_id

pytestmark = pytest.mark.skipif(os.name == "nt", reason="server processes and flock are POSIX-only")


def _server(data_dir: Path, db_path: Path) -> ServerProcess:
    return ServerProcess(data_dir=data_dir, db_path=db_path, port=_find_free_port())


def _client(server: ServerProcess) -> httpx.Client:
    return httpx.Client(base_url=f"http://127.0.0.1:{server.port}", timeout=10.0)


def _first_start(data_dir: Path, db_path: Path, put: bool = True) -> None:
    """Start and stop a server on a fresh pair, optionally storing one object."""
    server = _server(data_dir, db_path)
    server.start()
    server.wait_ready()
    if put:
        with _client(server) as client:
            assert client.put("/bkt").status_code == 200
            assert client.put("/bkt/k", content=b"stored").status_code == 200
    server.stop()


def _snapshot(root: Path) -> dict[Path, bytes]:
    """Every file under root with its bytes, LOCK files excepted."""
    return {
        p.relative_to(root): p.read_bytes()
        for p in sorted(root.rglob("*"))
        if p.is_file() and p.name != "LOCK"
    }


def _refused(data_dir: Path, db_path: Path, needle: str) -> str:
    server = _server(data_dir, db_path)
    server.start()
    returncode = server.wait_for_exit()
    logs = server.collect_logs()
    assert returncode not in (0, -signal.SIGKILL), logs
    assert "Application startup complete" not in logs, logs
    assert needle in logs, logs
    return logs


def _assert_refused_unchanged(tmp_path: Path, data_dir: Path, db_path: Path, needle: str) -> None:
    before = _snapshot(tmp_path)
    _refused(data_dir, db_path, needle)
    assert _snapshot(tmp_path) == before


def _store_row(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT store_id, state, blob_dir FROM store").fetchone()
    finally:
        conn.close()


def _blob_dir(data_dir: Path) -> Path:
    return data_dir / "blobs"


def _marker(data_dir: Path) -> Path:
    return _blob_dir(data_dir) / "STORE_ID"


# --- first start ---


def test_first_start_writes_matching_store_ids(tmp_path: Path):
    a, db = tmp_path / "a", tmp_path / "meta.db"
    _first_start(a, db)
    store_id, state, blob_dir = _store_row(db)
    assert state == "ready"
    assert blob_dir == os.path.realpath(_blob_dir(a))
    assert _marker(a).read_text() == store_id + "\n"

    server = _server(a, db)
    server.start()
    server.wait_ready()
    try:
        with _client(server) as client:
            assert client.get("/bkt/k").content == b"stored"
    finally:
        server.stop()
    assert _store_row(db) == (store_id, "ready", blob_dir)


# --- refusals ---


def test_refuses_database_with_a_fresh_blob_dir(tmp_path: Path):
    _first_start(tmp_path / "a", tmp_path / "meta.db")
    fresh = tmp_path / "b"
    _blob_dir(fresh).mkdir(parents=True)
    _assert_refused_unchanged(tmp_path, fresh, tmp_path / "meta.db", "has no STORE_ID marker")


def test_refuses_database_with_a_missing_blob_dir_and_creates_nothing(tmp_path: Path):
    _first_start(tmp_path / "a", tmp_path / "meta.db")
    missing = tmp_path / "c"
    _assert_refused_unchanged(tmp_path, missing, tmp_path / "meta.db", "does not exist")
    assert not missing.exists()


def test_refuses_blob_dir_of_another_store(tmp_path: Path):
    _first_start(tmp_path / "a", tmp_path / "a.db")
    _first_start(tmp_path / "b", tmp_path / "b.db")
    _assert_refused_unchanged(tmp_path, tmp_path / "b", tmp_path / "a.db", "belongs to store")


def test_refuses_fresh_database_with_a_used_blob_dir(tmp_path: Path):
    _first_start(tmp_path / "a", tmp_path / "a.db")
    SQLiteMetadataStore(db_path=tmp_path / "new.db")  # a new, empty database
    _assert_refused_unchanged(
        tmp_path, tmp_path / "a", tmp_path / "new.db", "the database has no store ID"
    )


def test_refuses_missing_marker(tmp_path: Path):
    a, db = tmp_path / "a", tmp_path / "meta.db"
    _first_start(a, db)
    _marker(a).unlink()
    _assert_refused_unchanged(tmp_path, a, db, "has no STORE_ID marker")


def test_refuses_unmarked_blob_dir_with_files(tmp_path: Path):
    a = tmp_path / "a"
    blobs = BlobStore(_blob_dir(a))
    blobs.create_tree()
    blob_id = new_blob_id()
    blobs.blob_path(blob_id).write_bytes(b"someone's bytes")
    SQLiteMetadataStore(db_path=tmp_path / "new.db")
    _assert_refused_unchanged(tmp_path, a, tmp_path / "new.db", "holds data but no STORE_ID marker")


def test_refuses_database_with_rows_but_no_store_id(tmp_path: Path):
    """A database with data but no store ID, paired by mistake with a new, empty blob
    directory: binding the two would make the right blob directory refused for good."""
    SQLiteMetadataStore(db_path=tmp_path / "old.db").create_bucket("b", datetime(2024, 1, 1))
    new = tmp_path / "new"
    _blob_dir(new).mkdir(parents=True)
    _assert_refused_unchanged(
        tmp_path, new, tmp_path / "old.db", "the database has rows but no store ID"
    )
    assert SQLiteMetadataStore(db_path=tmp_path / "old.db").get_store() is None
    assert not _marker(new).exists()


# --- an 'initializing' store row (decision 16) ---


def _initializing(db_path: Path, data_dir: Path) -> str:
    """A database whose first start stopped after committing the store row. That
    start had already created blobs/ (§7 step 5)."""
    _blob_dir(data_dir).mkdir(parents=True, exist_ok=True)
    store_id = new_blob_id()
    assert SQLiteMetadataStore(db_path=db_path).create_store(
        store_id, os.path.realpath(_blob_dir(data_dir))
    )
    return store_id


def test_initializing_row_is_adopted_only_by_its_blob_dir(tmp_path: Path):
    a, db = tmp_path / "a", tmp_path / "meta.db"
    store_id = _initializing(db, a)
    _blob_dir(tmp_path / "b").mkdir(parents=True)
    _assert_refused_unchanged(tmp_path, tmp_path / "b", db, "was initialized for blob directory")

    _first_start(a, db, put=False)
    assert _store_row(db)[:2] == (store_id, "ready")
    assert _marker(a).read_text() == store_id + "\n"


def test_initializing_store_with_only_the_marker_temp_is_finished(tmp_path: Path):
    a, db = tmp_path / "a", tmp_path / "meta.db"
    store_id = _initializing(db, a)
    (_blob_dir(a) / "tmp").mkdir()
    (_blob_dir(a) / "tmp" / "STORE_ID.tmp").write_text("partial")
    _first_start(a, db, put=False)
    assert _store_row(db)[:2] == (store_id, "ready")
    assert _marker(a).read_text() == store_id + "\n"


def test_initializing_store_with_its_own_marker_is_finished(tmp_path: Path):
    a, db = tmp_path / "a", tmp_path / "meta.db"
    store_id = _initializing(db, a)
    _marker(a).write_text(store_id + "\n")
    _first_start(a, db, put=False)
    assert _store_row(db)[:2] == (store_id, "ready")


def test_initializing_store_with_another_marker_is_refused(tmp_path: Path):
    a, db = tmp_path / "a", tmp_path / "meta.db"
    _initializing(db, a)
    _marker(a).write_text(new_blob_id() + "\n")
    _assert_refused_unchanged(tmp_path, a, db, "belongs to store")


def test_initializing_store_with_a_table_row_is_refused(tmp_path: Path):
    a, db = tmp_path / "a", tmp_path / "meta.db"
    _initializing(db, a)
    SQLiteMetadataStore(db_path=db).create_bucket("b", datetime(2024, 1, 1))
    _assert_refused_unchanged(tmp_path, a, db, "has held data")


@pytest.mark.parametrize(
    "plant",
    ["blob-file", "tmp-file", "stray-entry"],
)
def test_initializing_store_whose_blob_dir_held_data_is_refused(tmp_path: Path, plant: str):
    a, db = tmp_path / "a", tmp_path / "meta.db"
    _initializing(db, a)
    blobs = BlobStore(_blob_dir(a))
    blobs.create_tree()
    if plant == "blob-file":
        blobs.blob_path(new_blob_id()).write_bytes(b"x")
    elif plant == "tmp-file":
        blobs.temp_path(new_blob_id()).write_bytes(b"x")
    else:
        (_blob_dir(a) / "stray").write_bytes(b"x")
    _assert_refused_unchanged(tmp_path, a, db, "has held data")


# --- two servers ---


def test_second_server_on_the_same_blob_dir_refuses(tmp_path: Path):
    """I6: identical configuration, so only the lock can catch it."""
    a, db = tmp_path / "a", tmp_path / "meta.db"
    first = _server(a, db)
    first.start()
    first.wait_ready()
    try:
        with _client(first) as client:
            assert client.put("/bkt").status_code == 200
            assert client.put("/bkt/k", content=b"first").status_code == 200
            _assert_refused_unchanged(tmp_path, a, db, "already in use by another server")
            assert client.put("/bkt/k2", content=b"still serving").status_code == 200
            assert client.get("/bkt/k").content == b"first"
            assert client.get("/bkt/k2").content == b"still serving"
    finally:
        first.stop()


def test_second_server_sharing_the_database_with_another_blob_dir_refuses(tmp_path: Path):
    a, b, db = tmp_path / "a", tmp_path / "b", tmp_path / "meta.db"
    _blob_dir(b).mkdir(parents=True)  # exists, so the store identity check is what refuses
    first = _server(a, db)
    first.start()
    first.wait_ready()
    try:
        with _client(first) as client:
            assert client.put("/bkt").status_code == 200
            assert client.put("/bkt/k", content=b"first").status_code == 200
            _assert_refused_unchanged(tmp_path, b, db, "has no STORE_ID marker")
            assert client.put("/bkt/k2", content=b"still serving").status_code == 200
            assert client.get("/bkt/k").content == b"first"
    finally:
        first.stop()
