"""A pytest killed with SIGTERM takes its live servers down with it.

Runs a throwaway test under the chaos fixtures in a child pytest, which starts a server
and sleeps; sends SIGTERM to the child, then checks the server is gone.
"""

import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.name == "nt", reason="server processes are POSIX-only here")

REPO = Path(__file__).resolve().parents[2]

PROBE = """
    import os
    import time
    from pathlib import Path

    from chaos.fixtures.subprocess import ServerProcess, _find_free_port


    def test_sleeps_with_a_live_server(tmp_path):
        server = ServerProcess(
            data_dir=tmp_path, db_path=tmp_path / "meta.db", port=_find_free_port()
        )
        server.start()
        server.wait_ready()
        Path(os.environ["PROBE_PID_FILE"]).write_text(str(server._proc.pid))
        time.sleep(60)
"""


def _gone(pid: int) -> bool:
    """True when the process no longer exists or is a zombie awaiting its reaper."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except (FileNotFoundError, ProcessLookupError):
        return True
    # The state letter follows the last ")" because the command name may contain one.
    return stat.rsplit(")", 1)[1].split()[0] == "Z"


def _wait_until(condition, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.1)
    return condition()


def test_sigterm_to_pytest_kills_live_servers(tmp_path: Path):
    probe = tmp_path / "test_probe.py"
    probe.write_text(textwrap.dedent(PROBE))
    pid_file = tmp_path / "server.pid"
    basetemp = tmp_path / "basetemp"  # not numbered: no exit cleanup of other sessions' dirs
    env = {**os.environ, "PROBE_PID_FILE": str(pid_file)}
    server_pid: int | None = None
    output = tmp_path / "nested.out"
    with output.open("w") as out:
        nested = subprocess.Popen(
            [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-p", "chaos.conftest"]
            + [f"--basetemp={basetemp}", str(probe)],
            cwd=REPO,
            env=env,
            stdout=out,
            stderr=subprocess.STDOUT,
        )
    try:
        assert _wait_until(lambda: pid_file.exists() and pid_file.read_text() != "", 30), (
            f"the nested pytest wrote no server PID within 30s:\n{output.read_text()}"
        )
        server_pid = int(pid_file.read_text())

        nested.send_signal(signal.SIGTERM)
        try:
            returncode = nested.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pytest.fail(
                f"the nested pytest did not exit within 10s of SIGTERM:\n{output.read_text()}"
            )
        assert returncode == -signal.SIGTERM, output.read_text()

        assert _wait_until(lambda: _gone(server_pid), 5), (
            f"server {server_pid} outlived the SIGTERM of its pytest"
        )
    finally:
        if server_pid is None and pid_file.exists() and pid_file.read_text():
            server_pid = int(pid_file.read_text())
        if server_pid is not None and not _gone(server_pid):
            os.kill(server_pid, signal.SIGKILL)
        if nested.poll() is None:
            nested.kill()
            nested.wait()
