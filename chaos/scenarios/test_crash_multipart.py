"""
Tier B chaos: real process crash (SIGKILL) tests.

Skipped automatically on Windows — see live_server fixture in subprocess.py.

Timing approach: S3_TEST_WRITE_DELAY_MS=200 slows each write chunk so that a
large upload takes >1s, giving a reliable window to SIGKILL mid-write.

Invariants under test (both must hold after restart):
1. Either the object is visible with the correct content, OR it is not visible.
   "Metadata says complete but GET fails" is never acceptable.
2. No partial object is ever visible (truncated body, wrong ETag).
3. The consistency sweep on restart logs any detected issues but does not
   prevent the server from starting.
"""

import hashlib
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx
import pytest

from chaos.fixtures.subprocess import ServerProcess, _find_free_port

# --- helpers ---


def _complete_xml(parts: list[tuple[int, str]]) -> bytes:
    root = ET.Element("CompleteMultipartUpload")
    for num, etag in parts:
        part = ET.SubElement(root, "Part")
        ET.SubElement(part, "PartNumber").text = str(num)
        ET.SubElement(part, "ETag").text = etag
    return ET.tostring(root)


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def _check_invariants_after_restart(
    client: httpx.Client, bucket: str, key: str, expected_bodies: list[bytes]
) -> None:
    """Assert storage invariants hold after a crash+restart."""
    resp = client.get(f"/{bucket}/{key}")

    if resp.status_code == 404:
        # Object not visible — acceptable (crash before metadata commit)
        return

    assert resp.status_code == 200, (
        f"Expected 200 or 404, got {resp.status_code}: {resp.text[:200]}"
    )

    # Object is visible — body must be one of the expected bodies, never partial
    assert resp.content in expected_bodies, (
        f"Object body after crash is neither of the expected values.\n"
        f"Got {len(resp.content)} bytes: {resp.content[:100]!r}"
    )

    # ETag must match the MD5 of the body actually returned
    body_md5 = _md5(resp.content)
    assert resp.headers.get("etag") == body_md5, (
        f"ETag {resp.headers.get('etag')!r} does not match body MD5 {body_md5!r}"
    )


# --- Tier B tests ---


def test_sigkill_during_single_put(tmp_path: Path):
    """SIGKILL mid-PUT of a large object → after restart, GET returns 404 (not partial)."""
    import os

    if os.name == "nt":
        pytest.skip("SIGKILL not available on Windows")

    port = _find_free_port()
    server = ServerProcess(
        data_dir=tmp_path,
        db_path=tmp_path / "meta.db",
        port=port,
        write_delay_ms=200,
    )
    server.start()
    server.wait_ready()

    client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=30.0)
    client.put("/crash-bucket")

    # 5MB body — with 200ms delay per 64KB chunk, takes ~15s to write
    # We kill after 300ms, well into the write
    large_body = b"x" * (5 * 1024 * 1024)

    put_thread_result = {}

    def do_put():
        try:
            put_thread_result["resp"] = client.put("/crash-bucket/large.bin", content=large_body)
        except Exception as e:
            put_thread_result["error"] = e

    t = threading.Thread(target=do_put)
    t.start()
    time.sleep(0.3)  # let write get started
    server.kill()
    t.join(timeout=5)
    client.close()

    # Restart with same data_dir — no write delay this time
    port2 = _find_free_port()
    server2 = ServerProcess(
        data_dir=tmp_path,
        db_path=tmp_path / "meta.db",
        port=port2,
        write_delay_ms=0,
    )
    server2.start()
    server2.wait_ready()
    client2 = httpx.Client(base_url=f"http://127.0.0.1:{port2}", timeout=10.0)

    try:
        resp = client2.get("/crash-bucket/large.bin")
        # Must be 404 (write never completed) or 200 with full correct body
        if resp.status_code == 200:
            assert resp.content == large_body, "Partial object visible after crash"
            assert resp.headers.get("etag") == _md5(large_body), "ETag mismatch after crash"
        else:
            assert resp.status_code == 404, f"Unexpected status {resp.status_code}"
    finally:
        client2.close()
        server2.stop()


def test_sigkill_during_complete_multipart(tmp_path: Path):
    """SIGKILL mid-CompleteMultipartUpload → after restart, invariants hold."""
    import os

    if os.name == "nt":
        pytest.skip("SIGKILL not available on Windows")

    port = _find_free_port()
    server = ServerProcess(
        data_dir=tmp_path,
        db_path=tmp_path / "meta.db",
        port=port,
        write_delay_ms=200,
    )
    server.start()
    server.wait_ready()

    client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=30.0)
    client.put("/mp-bucket")

    # Upload 3 x 2MB parts
    part_size = 2 * 1024 * 1024
    parts_data = [bytes([i % 256] * part_size) for i in range(3)]
    expected_body = b"".join(parts_data)

    resp = client.post("/mp-bucket/assembled?uploads")
    uid = ET.fromstring(resp.text).findtext("UploadId")

    part_etags = []
    for i, data in enumerate(parts_data, start=1):
        r = client.put(f"/mp-bucket/assembled?partNumber={i}&uploadId={uid}", content=data)
        part_etags.append(r.headers["etag"])

    complete_body = _complete_xml(list(enumerate(part_etags, start=1)))

    complete_result = {}

    def do_complete():
        try:
            complete_result["resp"] = client.post(
                f"/mp-bucket/assembled?uploadId={uid}", content=complete_body
            )
        except Exception as e:
            complete_result["error"] = e

    t = threading.Thread(target=do_complete)
    t.start()
    time.sleep(0.3)
    server.kill()
    t.join(timeout=5)
    client.close()

    # Restart
    port2 = _find_free_port()
    server2 = ServerProcess(
        data_dir=tmp_path,
        db_path=tmp_path / "meta.db",
        port=port2,
        write_delay_ms=0,
    )
    server2.start()
    server2.wait_ready()
    client2 = httpx.Client(base_url=f"http://127.0.0.1:{port2}", timeout=10.0)

    try:
        _check_invariants_after_restart(client2, "mp-bucket", "assembled", [expected_body])
    finally:
        client2.close()
        server2.stop()


def test_server_restarts_cleanly_after_crash(tmp_path: Path):
    """After SIGKILL with no in-flight operations, server must restart and pass /health."""
    import os

    if os.name == "nt":
        pytest.skip("SIGKILL not available on Windows")

    port = _find_free_port()
    server = ServerProcess(
        data_dir=tmp_path,
        db_path=tmp_path / "meta.db",
        port=port,
    )
    server.start()
    server.wait_ready()

    # Write a clean object, then kill
    client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10.0)
    client.put("/stable-bucket")
    client.put("/stable-bucket/obj.txt", content=b"stable data")
    client.close()
    server.kill()

    # Restart and verify object is intact
    port2 = _find_free_port()
    server2 = ServerProcess(
        data_dir=tmp_path,
        db_path=tmp_path / "meta.db",
        port=port2,
    )
    server2.start()
    server2.wait_ready()
    client2 = httpx.Client(base_url=f"http://127.0.0.1:{port2}", timeout=10.0)

    try:
        assert client2.get("/health").status_code == 200
        resp = client2.get("/stable-bucket/obj.txt")
        assert resp.status_code == 200
        assert resp.content == b"stable data"
    finally:
        client2.close()
        server2.stop()
