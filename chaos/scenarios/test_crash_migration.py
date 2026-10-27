"""The schema 2 to 3 migration at startup (docs/design/storage-layout.md §20,
docs/crash-model.md), with real server processes.

A server killed inside the migration leaves a version 2 or a version 3 database, never
a mix, and the next start finishes the job. Each test starts from a version 2 database
made by chaos/fixtures/v2_database.py.
"""

import os
import signal
import sqlite3
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

from chaos.fixtures.subprocess import ServerProcess, _find_free_port
from chaos.fixtures.v2_database import BUCKET, V2Data, build_v2_database
from server.crash_points import MIGRATE_AFTER_COMMIT, MIGRATE_BEFORE_COMMIT

pytestmark = pytest.mark.skipif(os.name == "nt", reason="server processes and flock are POSIX-only")

V3_COLUMNS = {
    "multipart_uploads": {"checksum_algorithm", "checksum_type"},
    "multipart_parts": {"checksum", "checksum_algorithm", "last_modified"},
    "objects": {"checksum", "checksum_algorithm", "checksum_type"},
}


def _server(data: V2Data, crash_point: str | None = None) -> ServerProcess:
    return ServerProcess(
        data_dir=data.data_dir,
        db_path=data.db_path,
        port=_find_free_port(),
        crash_point=crash_point,
    )


def _client(server: ServerProcess) -> httpx.Client:
    return httpx.Client(base_url=f"http://127.0.0.1:{server.port}", timeout=10.0)


def _dump(db: Path) -> dict:
    """Every table's rows, schema_version included, and the schema text: what the
    database holds, whatever the file's bytes (a read only open can checkpoint WAL
    frames a killed writer left)."""
    conn = sqlite3.connect(db)
    try:
        schema = conn.execute("SELECT type, name, sql FROM sqlite_master ORDER BY name").fetchall()
        dump = {"schema": schema}
        for (name,) in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall():
            dump[name] = sorted(conn.execute(f"SELECT * FROM {name}").fetchall(), key=repr)
        return dump
    finally:
        conn.close()


def _v2_columns_dump(db: Path) -> dict[str, list[tuple]]:
    """Every table's rows over the columns of version 2."""
    conn = sqlite3.connect(db)
    try:
        dump = {}
        for (name,) in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall():
            names = [
                r[1]
                for r in conn.execute(f"PRAGMA table_xinfo({name})")
                if r[1] not in V3_COLUMNS.get(name, set())
            ]
            dump[name] = sorted(
                conn.execute(f"SELECT {', '.join(names)} FROM {name}").fetchall(), key=repr
            )
        dump.pop("schema_version")
        return dump
    finally:
        conn.close()


def _version(db: Path) -> int:
    conn = sqlite3.connect(db)
    try:
        return conn.execute("SELECT version FROM schema_version").fetchone()[0]
    finally:
        conn.close()


def _has_v3_columns(db: Path) -> bool:
    conn = sqlite3.connect(db)
    try:
        present = [
            V3_COLUMNS[table] & {r[1] for r in conn.execute(f"PRAGMA table_xinfo({table})")}
            for table in V3_COLUMNS
        ]
    finally:
        conn.close()
    assert present == [V3_COLUMNS[t] for t in V3_COLUMNS] or not any(present), present
    return any(present)


def _assert_serves(server: ServerProcess, data: V2Data) -> None:
    with _client(server) as client:
        for key, body in data.objects.items():
            assert client.get(f"/{BUCKET}/{key}").content == body


def _killed_and_restarted(tmp_path: Path, point: str) -> tuple[V2Data, dict, ServerProcess]:
    data = build_v2_database(tmp_path)
    before = _v2_columns_dump(data.db_path)
    killed = _server(data, crash_point=point)
    killed.start()
    assert killed.wait_for_exit() == -signal.SIGKILL, killed.collect_logs()
    assert _v2_columns_dump(data.db_path) == before
    return data, before, killed


def test_kill_before_commit_leaves_v2_and_a_restart_migrates(tmp_path: Path):
    data, before, _ = _killed_and_restarted(tmp_path, MIGRATE_BEFORE_COMMIT)
    assert _version(data.db_path) == 2
    assert not _has_v3_columns(data.db_path)

    server = _server(data)
    server.start()
    server.wait_ready()
    try:
        _assert_serves(server, data)
    finally:
        server.stop()
    assert _version(data.db_path) == 3
    assert _has_v3_columns(data.db_path)
    assert _v2_columns_dump(data.db_path) == before


def test_kill_after_commit_leaves_v3_and_a_restart_serves(tmp_path: Path):
    data, before, _ = _killed_and_restarted(tmp_path, MIGRATE_AFTER_COMMIT)
    assert _version(data.db_path) == 3
    assert _has_v3_columns(data.db_path)

    server = _server(data)
    server.start()
    server.wait_ready()
    try:
        _assert_serves(server, data)
    finally:
        server.stop()
    assert _version(data.db_path) == 3
    assert _v2_columns_dump(data.db_path) == before


def test_a_locked_blob_dir_refuses_the_second_server_and_leaves_v2(tmp_path: Path):
    data = build_v2_database(tmp_path)
    before = _dump(data.db_path)
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import sys; from pathlib import Path; "
            "from server.storage.blobs import BlobStore; "
            "BlobStore(Path(sys.argv[1])).acquire_lock(); "
            "print('locked', flush=True); sys.stdin.read()",
            str(data.data_dir / "blobs"),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "locked"
        server = _server(data)
        server.start()
        returncode = server.wait_for_exit()
        logs = server.collect_logs()
        assert returncode not in (0, -signal.SIGKILL), logs
        assert "already in use by another server" in logs, logs
        assert "Application startup complete" not in logs, logs
    finally:
        holder.stdin.close()
        holder.wait(timeout=10)
    assert _dump(data.db_path) == before
    assert _version(data.db_path) == 2


def test_v2_data_end_to_end_through_a_real_server(tmp_path: Path):
    data = build_v2_database(tmp_path)
    server = _server(data)
    server.start()
    server.wait_ready()
    try:
        _assert_serves(server, data)
        with _client(server) as client:
            assert client.put(f"/{BUCKET}/new", content=b"after migration").status_code == 200
            assert client.get(f"/{BUCKET}/new").content == b"after migration"
            body = (
                "<CompleteMultipartUpload>"
                + "".join(
                    f"<Part><PartNumber>{n}</PartNumber><ETag>{data.part_etags[n]}</ETag></Part>"
                    for n in (1, 2)
                )
                + "</CompleteMultipartUpload>"
            )
            done = client.post(f"/{BUCKET}/mp", params={"uploadId": "open-up"}, content=body)
            assert done.status_code == 200, done.text
            assert client.get(f"/{BUCKET}/mp").content == data.part_bytes[1] + data.part_bytes[2]
            assert client.delete(f"/{BUCKET}/k1").status_code == 204
    finally:
        server.stop()
    assert _version(data.db_path) == 3

    again = _server(data)
    again.start()
    again.wait_ready()
    try:
        with _client(again) as client:
            assert client.get(f"/{BUCKET}/new").content == b"after migration"
            assert client.get(f"/{BUCKET}/k2").content == data.objects["k2"]
            assert client.get(f"/{BUCKET}/k1").status_code == 404
    finally:
        again.stop()


def test_version_4_is_refused_and_nothing_changes(tmp_path: Path):
    data = build_v2_database(tmp_path)
    conn = sqlite3.connect(data.db_path, isolation_level=None)
    conn.execute("UPDATE schema_version SET version = 4")
    conn.close()
    before = {
        p.relative_to(tmp_path): p.read_bytes() for p in sorted(tmp_path.rglob("*")) if p.is_file()
    }
    server = _server(data)
    server.start()
    returncode = server.wait_for_exit()
    logs = server.collect_logs()
    assert returncode not in (0, -signal.SIGKILL), logs
    assert "has schema version 4" in logs, logs
    assert "Application startup complete" not in logs, logs
    after = {
        p.relative_to(tmp_path): p.read_bytes() for p in sorted(tmp_path.rglob("*")) if p.is_file()
    }
    assert after == before
