"""
Tier A chaos: concurrent operations on the same key.

Invariants under test:
- Two simultaneous PUTs to the same key must both complete without 5xx.
- After both complete, GET returns exactly one of the two bodies (no mix,
  no empty, no corrupt).
- The winner's ETag must match the MD5 of the body returned by GET.

Note: TestClient is not thread-safe (single event loop). Concurrent tests use
asyncio.gather via pytest-asyncio against the ASGI app directly.
"""

import hashlib
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.filesystem import FilesystemBackend


@pytest.fixture
def app(tmp_path: Path):
    storage = FilesystemBackend(
        data_dir=tmp_path / "objects",
        parts_dir=tmp_path / "parts",
    )
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    return create_app(storage=storage, metadata=metadata)


@pytest.fixture
def client(app):
    return TestClient(app, raise_server_exceptions=False)


def _md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


@pytest.mark.anyio
async def test_concurrent_puts_same_key_no_5xx(app):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as ac:
        await ac.put("/bucket")
        body_a = b"body-AAAAAAAAAA"
        body_b = b"body-BBBBBBBBBB"

        import asyncio

        resp_a, resp_b = await asyncio.gather(
            ac.put("/bucket/shared.txt", content=body_a),
            ac.put("/bucket/shared.txt", content=body_b),
        )

    assert resp_a.status_code < 500
    assert resp_b.status_code < 500


@pytest.mark.anyio
async def test_concurrent_puts_same_key_get_returns_one_body(app):
    """GET after two concurrent PUTs must return exactly one of the two bodies."""
    body_a = b"AAAAAAAAAAAAAAAA"
    body_b = b"BBBBBBBBBBBBBBBB"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as ac:
        await ac.put("/bucket")

        import asyncio

        await asyncio.gather(
            ac.put("/bucket/shared.txt", content=body_a),
            ac.put("/bucket/shared.txt", content=body_b),
        )

        resp = await ac.get("/bucket/shared.txt")

    assert resp.status_code == 200
    assert resp.content in (body_a, body_b), f"GET returned unexpected body: {resp.content!r}"


@pytest.mark.anyio
async def test_concurrent_puts_etag_matches_body(app):
    """The ETag on GET must match the MD5 of the body returned."""
    body_a = b"version-alpha---"
    body_b = b"version-beta----"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as ac:
        await ac.put("/bucket")

        import asyncio

        await asyncio.gather(
            ac.put("/bucket/shared.txt", content=body_a),
            ac.put("/bucket/shared.txt", content=body_b),
        )

        resp = await ac.get("/bucket/shared.txt")

    assert resp.status_code == 200
    body_etag = _md5(resp.content)
    assert resp.headers["etag"] == body_etag, (
        "ETag does not match MD5 of returned body — mixed write"
    )


@pytest.mark.anyio
async def test_concurrent_puts_different_keys_no_interference(app):
    """Concurrent PUTs to different keys must not interfere with each other."""
    import asyncio

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as ac:
        await ac.put("/bucket")

        await asyncio.gather(
            *[ac.put(f"/bucket/key-{i}", content=f"body-{i}".encode()) for i in range(10)]
        )

        for i in range(10):
            resp = await ac.get(f"/bucket/key-{i}")
            assert resp.status_code == 200
            assert resp.content == f"body-{i}".encode()
