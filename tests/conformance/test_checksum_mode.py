"""x-amz-checksum-mode on GetObject and HeadObject (S3 API reference, GetObject and
HeadObject; docs/compatibility.md "Checksums"; docs/design/storage-layout.md decision 30).

With the mode enabled, a whole object 200 carries the stored checksum and
x-amz-checksum-type. A range response, a 304 and a row without a checksum carry none.
The expected values come from the independent digests in test_upload_checksums.py, not
from server/checksums.py.

The cases are driven through ASGI like the upload checksum tests, except the last six,
which run boto3 against the app under uvicorn.
"""

import socket
import sqlite3
import threading
import time
from pathlib import Path

import boto3
import pytest
import uvicorn
from botocore.config import Config
from botocore.exceptions import FlexibleChecksumError

# Shared harness; there is no tests/ package, so pytest puts this directory on sys.path.
from test_aws_chunked_decode import BUCKET, DATA, KEY, Env, call
from test_upload_checksums import ALGOS, b64, claim, digest, request

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

MODE = {"x-amz-checksum-mode": "ENABLED"}
METHODS = ["GET", "HEAD"]


@pytest.fixture
async def env(tmp_path: Path) -> Env:
    e = Env(tmp_path)
    await e.setup()
    return e


async def _put(env: Env, *extra) -> None:
    headers, chunks = request("plain", DATA, list(extra))
    assert (await env.send("put", headers, chunks)).status == 200


async def _read(env: Env, method: str, headers=None, target: str | None = None):
    return await call(
        env.app, method, target or f"/{BUCKET}/{KEY}", MODE if headers is None else headers, []
    )


def _checksum_headers(r) -> dict[str, str]:
    return {k: v for k, v in r.headers.items() if k.startswith("x-amz-checksum")}


def _crc64nvme_headers() -> dict[str, str]:
    return {
        "x-amz-checksum-crc64nvme": b64(digest("crc64nvme", DATA)),
        "x-amz-checksum-type": "FULL_OBJECT",
    }


# --- the value of the header ---


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("value", ["ENABLED", "enabled", "Enabled", "eNaBlEd"])
async def test_enabled_in_any_letter_case_returns_the_checksum(env, method, value):
    await _put(env)
    r = await _read(env, method, {"x-amz-checksum-mode": value})
    assert r.status == 200
    assert _checksum_headers(r) == _crc64nvme_headers()


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("value", ["", "   "])
async def test_a_blank_mode_is_absent(env, method, value):
    await _put(env)
    r = await _read(env, method, {"x-amz-checksum-mode": value})
    assert r.status == 200
    assert _checksum_headers(r) == {}


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("value", ["DISABLED", "true", "ENABLED, ENABLED", "1"])
async def test_any_other_mode_is_invalid_argument(env, method, value):
    await _put(env)
    r = await _read(env, method, {"x-amz-checksum-mode": value})
    assert r.status == 400
    assert _checksum_headers(r) == {}
    if method == "GET":
        assert r.code == "InvalidArgument"


@pytest.mark.parametrize("method", METHODS)
async def test_an_invalid_mode_is_decided_after_the_bucket(env, method):
    r = await _read(env, method, {"x-amz-checksum-mode": "bogus"}, target=f"/nobucket/{KEY}")
    assert r.status == 404
    if method == "GET":
        assert r.code == "NoSuchBucket"


@pytest.mark.parametrize("method", METHODS)
async def test_an_invalid_mode_is_decided_before_the_key(env, method):
    r = await _read(env, method, {"x-amz-checksum-mode": "bogus"}, target=f"/{BUCKET}/absent")
    assert r.status == 400
    if method == "GET":
        assert r.code == "InvalidArgument"


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("name", ALGOS)
async def test_each_algorithm_returns_its_header_and_the_type(env, method, name):
    await _put(env, claim(name))
    r = await _read(env, method)
    assert r.status == 200
    assert _checksum_headers(r) == {
        f"x-amz-checksum-{name}": b64(digest(name, DATA)),
        "x-amz-checksum-type": "FULL_OBJECT",
    }


@pytest.mark.parametrize("method", METHODS)
async def test_without_the_mode_no_checksum_is_returned(env, method):
    await _put(env)
    r = await _read(env, method, {})
    assert r.status == 200
    assert _checksum_headers(r) == {}


# --- responses that are not a whole object 200 ---


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize(
    "range_header", ["bytes=0-9", "bytes=5-", "bytes=-10", "bytes=0-0", "bytes=100-99999"]
)
async def test_a_satisfiable_range_carries_no_checksum(env, method, range_header):
    await _put(env)
    r = await _read(env, method, {**MODE, "range": range_header})
    assert r.status == (206 if method == "GET" else 200)
    assert _checksum_headers(r) == {}


async def test_a_false_if_range_gives_the_whole_object_with_its_checksum(env):
    await _put(env)
    r = await _read(env, "GET", {**MODE, "range": "bytes=0-9", "if-range": '"not-the-etag"'})
    assert r.status == 200
    assert r.headers["content-length"] == str(len(DATA))
    assert "content-range" not in r.headers
    assert _checksum_headers(r) == _crc64nvme_headers()


@pytest.mark.parametrize("method", METHODS)
async def test_an_unsatisfiable_range_stays_416_without_a_checksum(env, method):
    await _put(env)
    r = await _read(env, method, {**MODE, "range": "bytes=99999-"})
    assert r.status == 416
    assert _checksum_headers(r) == {}


@pytest.mark.parametrize("method", METHODS)
async def test_a_304_carries_no_checksum(env, method):
    await _put(env)
    etag = (await _read(env, method, {})).headers["etag"]
    r = await _read(env, method, {**MODE, "if-none-match": etag})
    assert r.status == 304
    assert _checksum_headers(r) == {}


@pytest.mark.parametrize("method", METHODS)
async def test_a_row_without_a_checksum_returns_none(env, tmp_path, method):
    await _put(env)
    with sqlite3.connect(tmp_path / "meta.db") as db:
        db.execute(
            "UPDATE objects SET checksum = NULL, checksum_algorithm = NULL, checksum_type = NULL"
        )
    r = await _read(env, method)
    assert r.status == 200
    assert _checksum_headers(r) == {}


async def test_response_overrides_still_work_with_the_mode(env):
    await _put(env)
    target = f"/{BUCKET}/{KEY}?response-content-type=text/plain&response-cache-control=no-store"
    r = await _read(env, "GET", target=target)
    assert r.status == 200
    assert r.headers["content-type"] == "text/plain"
    assert r.headers["cache-control"] == "no-store"
    assert _checksum_headers(r) == _crc64nvme_headers()


# --- boto3 over a live server ---


@pytest.fixture(scope="module")
def live(tmp_path_factory):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    tmp: Path = tmp_path_factory.mktemp("checksum-mode")
    app = create_app(
        storage=BlobStore(tmp / "blobs"), metadata=SQLiteMetadataStore(db_path=tmp / "meta.db")
    )
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, ws="none", log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("uvicorn did not start")
        time.sleep(0.02)
    client = boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{port}",
        aws_access_key_id="key",
        aws_secret_access_key="secret",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 0}),
    )
    client.create_bucket(Bucket="boto3-mode")
    yield client, tmp / "meta.db"
    server.should_exit = True
    thread.join(timeout=10)


@pytest.mark.parametrize("name", ALGOS)
def test_boto3_get_object_validates_the_stored_checksum(live, name):
    s3, _ = live
    s3.put_object(Bucket="boto3-mode", Key=f"ok-{name}", Body=DATA, ChecksumAlgorithm=name.upper())
    got = s3.get_object(Bucket="boto3-mode", Key=f"ok-{name}", ChecksumMode="ENABLED")
    assert got[f"Checksum{name.upper()}"] == b64(digest(name, DATA))
    assert got["Body"].read() == DATA


def test_boto3_raises_when_the_stored_checksum_was_changed_directly(live):
    s3, db_path = live
    s3.put_object(Bucket="boto3-mode", Key="tampered", Body=DATA, ChecksumAlgorithm="SHA256")
    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE objects SET checksum = ? WHERE key = 'tampered'", (b64(digest("sha256", b"x")),)
        )
    got = s3.get_object(Bucket="boto3-mode", Key="tampered")
    with pytest.raises(FlexibleChecksumError):
        got["Body"].read()
