"""Tier B: no test, passing or failing, leaves a server process behind."""

import os
import signal
import threading

import pytest

# Test sessions run with no minimum part size, as in tests/conftest.py; set before
# server.config is imported, and inherited by the servers the fixtures start.
os.environ["S3_MIN_PART_SIZE"] = "0"

from chaos.fixtures.subprocess import live_servers, stop_all_servers, stop_servers_started_since


def _kill_servers_and_die(signum, frame):
    """SIGKILL every live server process, then let pytest die from SIGTERM as it would have.

    Python's default SIGTERM action ends the process with no fixture finalizers, which
    would leave each server an orphan. Only os.kill here: no poll(), wait() or stop(), so a
    SIGTERM that lands in the middle of stop() cannot deadlock or corrupt it."""
    for server in live_servers():
        for proc in list(server._procs):
            if proc.returncode is None:
                try:
                    os.kill(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.raise_signal(signal.SIGTERM)


def pytest_configure(config):
    """Install the SIGTERM backstop; leave a handler someone else installed alone."""
    if threading.current_thread() is not threading.main_thread():
        return
    if signal.getsignal(signal.SIGTERM) is not signal.SIG_DFL:
        return
    signal.signal(signal.SIGTERM, _kill_servers_and_die)


@pytest.fixture(autouse=True)
def _stop_leaked_servers():
    """Stop the servers this test started; those of a wider-scoped fixture, already live
    at setup, keep running for the tests that share them."""
    before = live_servers()
    try:
        yield
    finally:
        stop_servers_started_since(before)


@pytest.fixture(autouse=True, scope="session")
def _stop_all_servers_at_session_end():
    """Backstop for a server whose fixture raised before registering its own teardown."""
    try:
        yield
    finally:
        stop_all_servers()
