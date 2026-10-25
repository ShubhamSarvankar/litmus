"""A server a failing test started is stopped anyway.

Runs a throwaway failing test under the chaos fixtures in a child pytest, then checks
that the server processes it left running are gone.
"""

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.name == "nt", reason="server processes are POSIX-only here")

REPO = Path(__file__).resolve().parents[2]

PROBE = """
    import os
    from pathlib import Path

    import httpx
    import pytest

    from chaos.fixtures.subprocess import ServerProcess, _find_free_port

    PIDS = Path(os.environ["PROBE_PIDS"])


    def _server(tmp_path):
        return ServerProcess(
            data_dir=tmp_path, db_path=tmp_path / "meta.db", port=_find_free_port()
        )


    def _record(server):
        with PIDS.open("a") as f:
            f.write(f"{server._proc.pid}\\n")


    def test_body_raises(tmp_path):
        server = _server(tmp_path)
        server.start()
        _record(server)
        server.wait_ready()
        raise AssertionError("boom")


    def test_body_raises_after_sigkill_restart(tmp_path):
        server = _server(tmp_path)
        server.start()
        _record(server)
        server.wait_ready()
        server.kill()
        server.restart()
        _record(server)
        raise AssertionError("boom")


    def test_body_raises_before_ready(tmp_path):
        server = _server(tmp_path)
        server.start()
        _record(server)
        raise AssertionError("boom")


    @pytest.fixture(scope="module")
    def shared(tmp_path_factory):
        server = _server(tmp_path_factory.mktemp("shared"))
        server.start()
        _record(server)
        server.wait_ready()
        yield server
        server.stop()


    def test_shared_first_fails(shared):
        raise AssertionError("boom")


    def test_shared_second_still_reaches_it(shared):
        assert httpx.get(f"http://127.0.0.1:{shared.port}/health", timeout=5).status_code == 200


    @pytest.fixture(scope="module")
    def never_ready(tmp_path_factory):
        server = _server(tmp_path_factory.mktemp("never_ready"))
        server.start()
        _record(server)
        raise RuntimeError("wait_ready failed before the teardown was registered")


    def test_module_fixture_raises(never_ready):
        pass
"""


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_servers_of_failing_tests_are_stopped(tmp_path: Path):
    probe = tmp_path / "test_probe.py"
    probe.write_text(textwrap.dedent(PROBE))
    pids_file = tmp_path / "pids"
    env = {**os.environ, "PROBE_PIDS": str(pids_file)}
    pids: list[int] = []
    try:
        run = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-p", "chaos.conftest"]
            + [str(probe)],
            cwd=REPO,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        pids = [int(line) for line in pids_file.read_text().split()]
        # Four tests fail on purpose and one fixture errors. The shared server served the
        # test after the failing one. Six processes: the SIGKILLed one and its restart
        # count separately.
        assert "4 failed, 1 passed, 1 error" in run.stdout, run.stdout
        assert len(pids) == 6
        assert [pid for pid in pids if _alive(pid)] == []
    finally:
        for pid in pids:
            if _alive(pid):
                os.kill(pid, 9)
