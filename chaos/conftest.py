"""Tier B: no test, passing or failing, leaves a server process behind."""

import pytest

from chaos.fixtures.subprocess import live_servers, stop_all_servers, stop_servers_started_since


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
