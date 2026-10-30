"""
A chaos server writes an access log line per request to a pipe the test process holds.
Nothing may leave that pipe full: a full pipe blocks the server's logging, and the
server stops answering.
"""

import httpx
import pytest

from chaos.fixtures.subprocess import ServerProcess, _find_free_port

REQUESTS = 3000  # well past the ~1100 access log lines a 64 KiB pipe holds


def test_server_keeps_answering_past_a_full_output_pipe(tmp_path):
    server = ServerProcess(data_dir=tmp_path, db_path=tmp_path / "meta.db", port=_find_free_port())
    server.start()
    try:
        server.wait_ready()
        with httpx.Client(base_url=f"http://127.0.0.1:{server.port}", timeout=5.0) as client:
            for n in range(1, REQUESTS + 1):
                try:
                    status = client.get("/health").status_code
                except httpx.TimeoutException:
                    pytest.fail(f"request {n} of {REQUESTS} to /health timed out after 5s")
                assert status == 200, f"request {n} of {REQUESTS} answered {status}"
    finally:
        server.stop()

    answered = [
        line for line in server.collect_logs().splitlines() if 'GET /health HTTP/1.1" 200' in line
    ]
    assert len(answered) >= REQUESTS, f"only {len(answered)} access log lines were kept"
