"""
Tier B fixtures: real server process management for crash testing.

POSIX only — SIGKILL is not available on Windows. All Tier B tests are
skipped automatically on Windows via the `live_server` fixture.
"""

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_ready(port: int, timeout: float = 10.0) -> None:
    """Poll /health until 200 or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            resp = httpx.get(f"http://127.0.0.1:{port}/health", timeout=1.0)
            if resp.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"Server on port {port} did not become ready within {timeout}s")


# Every ServerProcess that has been started and not yet reaped. Tests start servers
# directly and stop them at the end of the body, which a failing assertion skips; the
# autouse fixtures in chaos/conftest.py stop each test's servers after it, all at session end.
_LIVE: list["ServerProcess"] = []


def live_servers() -> list["ServerProcess"]:
    return list(_LIVE)


def stop_servers_started_since(before: list["ServerProcess"]) -> None:
    """Stop the servers that are not in `before`, a live_servers() snapshot; servers
    that were already live then (a module or session fixture's) are left running."""
    for server in [s for s in _LIVE if s not in before]:
        _LIVE.remove(server)
        server.stop()


def stop_all_servers() -> None:
    """Stop every server started since the last call, running or already dead."""
    servers = list(_LIVE)
    _LIVE.clear()
    for server in servers:
        server.stop()


class ServerProcess:
    def __init__(
        self,
        data_dir: Path,
        db_path: Path,
        port: int,
        write_delay_ms: int = 0,
        crash_point: str | None = None,
    ):
        self._data_dir = data_dir
        self._db_path = db_path
        self._port = port
        self._write_delay_ms = write_delay_ms
        self._crash_point = crash_point
        self._proc: subprocess.Popen | None = None
        # Every process this object has started: restart() replaces _proc, and stop()
        # must reach the earlier ones too.
        self._procs: list[subprocess.Popen] = []
        self._log: list[str] = []

    def start(self) -> None:
        env = os.environ.copy()
        # Pass config via environment — config.py reads these if present
        env["S3_BLOB_DIR"] = str(self._data_dir / "blobs")
        env["S3_DB_PATH"] = str(self._db_path)
        # Variables of the old layout make the server refuse to start; never let one
        # leak in from the test runner's environment.
        env.pop("S3_DATA_DIR", None)
        env.pop("S3_PARTS_DIR", None)
        env["S3_PORT"] = str(self._port)
        env["S3_TEST_WRITE_DELAY_MS"] = str(self._write_delay_ms)
        # Set or clear explicitly so a value inherited from the test runner's
        # environment can never arm a crash point on a server that didn't ask for one.
        if self._crash_point:
            env["S3_TEST_CRASH_POINT"] = self._crash_point
        else:
            env.pop("S3_TEST_CRASH_POINT", None)

        self._proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "server.app:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self._port),
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self._procs.append(self._proc)
        if self not in _LIVE:
            _LIVE.append(self)

    def wait_ready(self, timeout: float = 10.0) -> None:
        _wait_ready(self._port, timeout)

    def kill(self) -> None:
        if self._proc and self._proc.poll() is None:
            os.kill(self._proc.pid, signal.SIGKILL)
            self._proc.wait()

    def wait_for_exit(self, timeout: float = 10.0) -> int:
        """Wait for the process to exit on its own (e.g. at a crash point) and
        return its return code; -signal.SIGKILL means it was SIGKILLed."""
        assert self._proc is not None
        return self._proc.wait(timeout=timeout)

    def restart(self) -> None:
        self.start()
        self.wait_ready()

    def collect_logs(self) -> str:
        if self._proc is None:
            return ""
        # Non-blocking read of any buffered output
        try:
            out, _ = self._proc.communicate(timeout=0.1)
            self._log.append(out or "")
        except subprocess.TimeoutExpired:
            pass
        return "\n".join(self._log)

    def stop(self) -> None:
        """Terminate every process this object started, wait up to 5s, kill the rest."""
        alive = [p for p in self._procs if p.poll() is None]
        for proc in alive:
            proc.terminate()
        for proc in alive:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

    @property
    def port(self) -> int:
        return self._port


@pytest.fixture
def live_server(tmp_path: Path):
    """Start a real uvicorn server, yield an httpx client, teardown cleanly.

    Skipped on Windows — SIGKILL is POSIX only.
    """
    if os.name == "nt":
        pytest.skip("Tier B crash tests require POSIX (SIGKILL not available on Windows)")

    port = _find_free_port()
    server = ServerProcess(
        data_dir=tmp_path,
        db_path=tmp_path / "meta.db",
        port=port,
    )
    server.start()
    server.wait_ready()

    client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10.0)

    yield client, server

    client.close()
    server.stop()


@pytest.fixture
def live_server_slow(tmp_path: Path):
    """Same as live_server but with S3_TEST_WRITE_DELAY_MS=200.

    The delay is a single 200ms sleep per PUT/UploadPart before the body is written
    to the temp file (see server/config.py); it does not slow CompleteMultipartUpload.
    For a deterministic kill at a specific step, use ServerProcess(crash_point=...).
    """
    if os.name == "nt":
        pytest.skip("Tier B crash tests require POSIX (SIGKILL not available on Windows)")

    port = _find_free_port()
    server = ServerProcess(
        data_dir=tmp_path,
        db_path=tmp_path / "meta.db",
        port=port,
        write_delay_ms=200,  # one 200ms sleep per PUT/UploadPart, before the body is written
    )
    server.start()
    server.wait_ready()

    client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=30.0)

    yield client, server, tmp_path

    client.close()
    server.stop()
