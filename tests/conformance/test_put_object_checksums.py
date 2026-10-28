"""PutObject stores the checksum of the object and returns it (server/checksums.py,
docs/compatibility.md "Checksums").

The algorithm stored is the claimed one, else CRC64NVME; the type is FULL_OBJECT; the
value is the base64 of the digest Litmus computed, not the request's string. The
expected digests come from the reference implementations in test_upload_checksums.py
(zlib, hashlib and two table CRCs), none of which uses awscrt or server/. Each case that
stores checks the response headers and the row in the database. An object written by
CopyObject has no checksum yet, and replaces the row's; one written by
CompleteMultipartUpload takes the CRC64NVME combined from its parts, and replaces it too.
"""

import random
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from test_aws_chunked_decode import BUCKET, DATA, KEY, Env, call
from test_upload_checksums import ALGOS, SDK, b64, claim, digest, md5, md5_claim, request

from chaos.framework.framed import frame, framed_headers

ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"


@pytest.fixture
async def env(tmp_path: Path) -> Env:
    e = Env(tmp_path)
    await e.setup()
    return e


def row(env: Env, key: str = KEY):
    return env.metadata.get_object_meta(BUCKET, key)


def checksum_headers(r) -> dict[str, str]:
    return {h: v for h, v in r.headers.items() if h.startswith("x-amz-checksum")}


def assert_stored(env: Env, r, algorithm: str, data: bytes, key: str = KEY) -> None:
    """The response and the row both hold the checksum of data under algorithm."""
    value = b64(digest(algorithm, data))
    assert checksum_headers(r) == {
        f"x-amz-checksum-{algorithm}": value,
        "x-amz-checksum-type": "FULL_OBJECT",
    }
    meta = row(env, key)
    assert (meta.checksum, meta.checksum_algorithm, meta.checksum_type) == (
        value,
        algorithm.upper(),
        "FULL_OBJECT",
    )


def assert_no_checksum(env: Env, key: str = KEY) -> None:
    meta = row(env, key)
    assert (meta.checksum, meta.checksum_algorithm, meta.checksum_type) == (None, None, None)


def body_of(size: int) -> bytes:
    return random.Random(size).randbytes(size)


def framed_trailer(name: str, data: bytes) -> tuple[dict, bytes]:
    """(headers, body) of a one chunk aws-chunked upload with the trailer of name."""
    header = f"x-amz-checksum-{name}"
    body = frame(data, trailer_line=f"{header}:{b64(digest(name, data))}")
    return framed_headers(body, len(data), **{"x-amz-trailer": header}), body


async def put(env: Env, data: bytes, extra=(), key: str = KEY):
    headers, chunks = request("plain", data, list(extra))
    return await call(env.app, "PUT", f"/{BUCKET}/{key}", headers, chunks)


# --- what is stored ---


@pytest.mark.parametrize("size", [0, 1, 1024, 65537, 5 * 1024 * 1024])
async def test_no_claim_stores_and_returns_crc64nvme(env, size):
    data = body_of(size)
    r = await put(env, data)
    assert r.status == 200, r.body
    assert r.headers["etag"] == md5(data)
    assert_stored(env, r, "crc64nvme", data)


@pytest.mark.parametrize("how", ["header", "trailer"])
@pytest.mark.parametrize("name", ALGOS)
async def test_a_claim_stores_and_returns_that_algorithm(env, name, how):
    if how == "header":
        r = await put(env, DATA, [claim(name)])
    else:
        headers, body = framed_trailer(name, DATA)
        r = await call(env.app, "PUT", f"/{BUCKET}/{KEY}", headers, [body])
    assert r.status == 200, r.body
    assert_stored(env, r, name, DATA)


def non_canonical(canonical: str) -> str:
    """The same bytes with a bit set in the padding bits of the last character."""
    body = canonical.rstrip("=")
    return body[:-1] + ALPHABET[ALPHABET.index(body[-1]) | 1] + canonical[len(body) :]


@pytest.mark.parametrize("name", ALGOS)
async def test_a_non_canonical_base64_claim_returns_the_canonical_digest(env, name):
    canonical = b64(digest(name, DATA))
    sloppy = non_canonical(canonical)
    assert sloppy != canonical
    r = await put(env, DATA, [(f"x-amz-checksum-{name}", sloppy)])
    assert r.status == 200, r.body
    assert_stored(env, r, name, DATA)
    assert row(env).checksum == canonical


async def test_an_overwrite_replaces_the_checksum(env):
    first = await put(env, DATA, [claim("sha256")])
    assert_stored(env, first, "sha256", DATA)
    second_data = DATA + b"more"
    second = await put(env, second_data, [claim("crc32", second_data)])
    assert_stored(env, second, "crc32", second_data)


async def test_complete_onto_a_put_object_stores_the_combined_crc64nvme(env):
    assert_stored(env, await put(env, DATA, [claim("sha256")]), "sha256", DATA)
    part_headers, chunks = request("plain", DATA, [])
    part = await env.send("part", part_headers, chunks)
    assert part.status == 200
    body = (
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>"
        f"<ETag>{part.headers['etag']}</ETag></Part></CompleteMultipartUpload>"
    ).encode()
    r = await call(
        env.app,
        "POST",
        f"/{BUCKET}/{KEY}?uploadId={env.upload_id}",
        {"content-length": str(len(body))},
        [body],
    )
    assert r.status == 200, r.body
    assert ET.fromstring(r.body).findtext("{*}ETag")
    assert row(env).upload_id == env.upload_id
    meta = row(env)
    assert (meta.checksum, meta.checksum_algorithm, meta.checksum_type) == (
        b64(digest("crc64nvme", DATA)),
        "CRC64NVME",
        "FULL_OBJECT",
    )


async def test_copy_onto_a_put_object_leaves_the_columns_null(env):
    assert_stored(env, await put(env, DATA, [claim("sha256")]), "sha256", DATA)
    assert_stored(env, await put(env, b"source", key="src"), "crc64nvme", b"source", key="src")
    r = await call(
        env.app,
        "PUT",
        f"/{BUCKET}/{KEY}",
        {"x-amz-copy-source": f"/{BUCKET}/src", "content-length": "0"},
        [],
    )
    assert r.status == 200, r.body
    assert checksum_headers(r) == {}
    assert row(env).size == len(b"source")
    assert_no_checksum(env)


# --- the claims around it ---


async def test_the_sdk_header_with_a_header_claim(env):
    r = await put(env, DATA, [claim("crc32c"), (SDK, "CRC32C")])
    assert r.status == 200, r.body
    assert_stored(env, r, "crc32c", DATA)


async def test_content_md5_alone_stores_crc64nvme(env):
    r = await put(env, DATA, [md5_claim()])
    assert r.status == 200, r.body
    assert_stored(env, r, "crc64nvme", DATA)


@pytest.mark.parametrize("name", ALGOS)
async def test_an_empty_body_with_a_claim(env, name):
    r = await put(env, b"", [claim(name, b"")])
    assert r.status == 200, r.body
    assert_stored(env, r, name, b"")


@pytest.mark.parametrize("value", ["CRC32", "SHA512", "bogus"])
async def test_x_amz_checksum_algorithm_is_ignored_on_put_object(env, value):
    r = await put(env, DATA, [("x-amz-checksum-algorithm", value)])
    assert r.status == 200, r.body
    assert_stored(env, r, "crc64nvme", DATA)
