"""The checksum of an UploadPart (server/api/multipart.py, server/checksums.py,
docs/design/storage-layout.md §20).

An upload is named when CreateMultipartUpload carried a non-blank x-amz-checksum-algorithm.
E, the effective algorithm, is the named algorithm, or CRC64NVME when the upload named none.
A claim is a checksum the part request sent, by x-amz-checksum-<algorithm> header or by
aws-chunked trailer. The part row always holds E and its digest of the bytes; the response
carries the claim's header with its verified value, and only that (API_UploadPart: the
checksum response headers are "only present if the checksum was provided in the request").
A claim in another algorithm than a named upload's is 400 InvalidRequest, decided from the
header or trailer name before the body is read. The expected values come from the digest
implementations of test_upload_checksums.py, not from server/checksums.py.
"""

import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path

import pytest
from test_aws_chunked_decode import BUCKET, DATA, KEY, Env, call
from test_checksum_trailers import framed
from test_upload_checksums import WRONG, b64, claim, digest, md5, md5_claim, request

ALGOS = ["crc32", "crc32c", "crc64nvme", "sha1", "sha256"]
VIAS = ["header", "trailer"]
OTHER = {a: ALGOS[(i + 1) % len(ALGOS)] for i, a in enumerate(ALGOS)}


async def make_env(tmp_path: Path, named: str | None) -> Env:
    """An app with a bucket and one open upload that names `named`, or none."""
    env = Env(tmp_path)
    if named is None:
        await env.setup()
        return env
    assert (await call(env.app, "PUT", f"/{BUCKET}", {}, [])).status == 200
    started = await call(
        env.app, "POST", f"/{BUCKET}/{KEY}?uploads", {"x-amz-checksum-algorithm": named.upper()}, []
    )
    assert started.status == 200
    env.upload_id = ET.fromstring(started.body).findtext("UploadId")
    return env


async def send_claim(env: Env, via: str, name: str, data: bytes = DATA, **extra):
    """UploadPart of data claiming its checksum in algorithm name, by header or trailer."""
    if via == "header":
        headers, chunks = request("plain", data, [claim(name, data), *extra.items()])
        return await env.send("part", headers, chunks)
    headers, body = framed(name, data)
    return await env.send("part", headers | extra, [body])


async def send_plain(env: Env, data: bytes = DATA, extra=()):
    headers, chunks = request("plain", data, list(extra))
    return await env.send("part", headers, chunks)


def checksum_headers(r) -> dict[str, str]:
    return {h: v for h, v in r.headers.items() if h.startswith("x-amz-checksum")}


def echo(name: str, data: bytes = DATA) -> dict[str, str]:
    return {f"x-amz-checksum-{name}": b64(digest(name, data))}


def assert_part_row(env: Env, algorithm: str, data: bytes = DATA):
    """The one part row holds `algorithm` and its digest of data, a time, and the bytes."""
    (part,) = env.metadata.get_parts(env.upload_id)
    assert part.checksum_algorithm == algorithm.upper()
    assert part.checksum == b64(digest(algorithm, data))
    assert part.last_modified is not None
    assert env.stored_bytes("part") == (data, md5(data))
    return part


# --- a named upload ---


@pytest.mark.parametrize("via", VIAS)
@pytest.mark.parametrize("name", ALGOS)
async def test_named_upload_verifies_stores_and_echoes_a_claim_in_its_algorithm(
    tmp_path, name, via
):
    env = await make_env(tmp_path, name)
    r = await send_claim(env, via, name)
    assert r.status == 200, r.body
    assert r.headers["etag"] == md5(DATA)
    assert checksum_headers(r) == echo(name)
    assert_part_row(env, name)


@pytest.mark.parametrize("via", VIAS)
@pytest.mark.parametrize("name", ALGOS)
async def test_named_upload_refuses_a_claim_in_another_algorithm_before_the_body(
    tmp_path, name, via
):
    env = await make_env(tmp_path, name)
    r = await send_claim(env, via, OTHER[name])
    assert (r.status, r.code) == (400, "InvalidRequest")
    assert r.received == 0
    env.assert_nothing_left("part")


@pytest.mark.parametrize("name", ALGOS)
async def test_named_upload_computes_its_algorithm_for_a_part_sent_without_a_claim(tmp_path, name):
    env = await make_env(tmp_path, name)
    r = await send_plain(env)
    assert r.status == 200, r.body
    assert checksum_headers(r) == {}
    assert_part_row(env, name)


@pytest.mark.parametrize("via", VIAS)
async def test_unnamed_upload_verifies_stores_and_echoes_a_crc64nvme_claim(tmp_path, via):
    env = await make_env(tmp_path, None)
    r = await send_claim(env, via, "crc64nvme")
    assert r.status == 200, r.body
    assert checksum_headers(r) == echo("crc64nvme")
    assert_part_row(env, "crc64nvme")


@pytest.mark.parametrize("via", VIAS)
@pytest.mark.parametrize("name", ["crc32", "crc32c", "sha1", "sha256"])
async def test_unnamed_upload_verifies_another_claim_and_stores_crc64nvme(tmp_path, name, via):
    env = await make_env(tmp_path, None)
    r = await send_claim(env, via, name)
    assert r.status == 200, r.body
    assert checksum_headers(r) == echo(name)  # only the claim's header
    assert_part_row(env, "crc64nvme")


async def test_unnamed_upload_stores_crc64nvme_for_a_part_sent_without_a_claim(tmp_path):
    env = await make_env(tmp_path, None)
    r = await send_plain(env)
    assert r.status == 200, r.body
    assert checksum_headers(r) == {}
    assert_part_row(env, "crc64nvme")


# --- the mismatch is decided before the body ---


async def test_a_mismatched_header_claim_is_refused_before_its_value_or_body_is_read(tmp_path):
    env = await make_env(tmp_path, "crc32")
    headers, chunks = request(
        "plain", DATA, [("x-amz-checksum-sha256", b64(digest("sha256", WRONG)))]
    )
    r = await env.send("part", headers, chunks)
    assert (r.status, r.code) == (400, "InvalidRequest")
    assert r.received == 0
    env.assert_nothing_left("part")


async def test_a_mismatched_trailer_is_refused_before_the_body_is_read(tmp_path):
    env = await make_env(tmp_path, "sha1")
    bad = f"x-amz-checksum-crc32c:{b64(digest('crc32c', WRONG))}"
    headers, body = framed("crc32c", DATA, bad)
    r = await env.send("part", headers, [body])
    assert (r.status, r.code) == (400, "InvalidRequest")
    assert r.received == 0
    env.assert_nothing_left("part")


# --- replacing a part ---


async def test_a_reupload_replaces_the_checksum_and_the_time(tmp_path):
    env = await make_env(tmp_path, None)
    old = b"the first attempt" * 100
    assert (await send_claim(env, "header", "crc32", old)).status == 200
    first = assert_part_row(env, "crc64nvme", old)

    r = await send_claim(env, "header", "sha256", DATA)

    assert r.status == 200
    second = assert_part_row(env, "crc64nvme", DATA)
    assert second.checksum != first.checksum
    assert second.blob_id != first.blob_id
    assert second.last_modified >= first.last_modified
    assert len([e for e, _ in env.storage.scan()]) == 1


async def test_the_part_time_is_the_time_of_the_commit_and_moves_on_a_reupload(tmp_path):
    env = await make_env(tmp_path, "crc32")
    before = datetime.now(UTC).replace(tzinfo=None)
    assert (await send_plain(env)).status == 200
    after = datetime.now(UTC).replace(tzinfo=None)
    (first,) = env.metadata.get_parts(env.upload_id)
    assert before <= first.last_modified <= after

    assert (await send_plain(env, DATA + b"more")).status == 200
    (second,) = env.metadata.get_parts(env.upload_id)
    assert second.last_modified > first.last_modified


# --- Content-MD5, and a bad digest ---


async def test_content_md5_is_checked_with_the_claim(tmp_path):
    env = await make_env(tmp_path, "sha256")
    ok = await send_claim(env, "header", "sha256", DATA, **dict([md5_claim(DATA)]))
    assert ok.status == 200
    assert checksum_headers(ok) == echo("sha256")
    before = assert_part_row(env, "sha256")

    bad = await send_claim(env, "header", "sha256", DATA, **dict([md5_claim(WRONG)]))

    assert (bad.status, bad.code) == (400, "BadDigest")
    assert checksum_headers(bad) == {}
    assert env.metadata.get_parts(env.upload_id) == [before]


async def test_a_bad_digest_keeps_the_old_part_with_its_checksum_and_time(tmp_path):
    env = await make_env(tmp_path, "crc32c")
    old = b"the first attempt" * 100
    assert (await send_claim(env, "header", "crc32c", old)).status == 200
    (before,) = env.metadata.get_parts(env.upload_id)
    records = env.metadata.list_blob_records()
    headers, chunks = request(
        "plain", DATA, [("x-amz-checksum-crc32c", b64(digest("crc32c", WRONG)))]
    )

    r = await env.send("part", headers, chunks)

    assert (r.status, r.code) == (400, "BadDigest")
    assert env.metadata.get_parts(env.upload_id) == [before]
    assert before.checksum == b64(digest("crc32c", old))
    assert env.metadata.list_blob_records() == records
    assert env.storage.temp_entries() == []
    assert env.stored_bytes("part") == (old, md5(old))
