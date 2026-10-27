"""aws-chunked request bodies with a CRC32, CRC32C, SHA1 or SHA256 trailer
(server/api/aws_chunked.py), for PutObject and UploadPart.

The CRC64NVME trailer is covered by test_aws_chunked_decode.py. Every other trailer the
AWS CLI sends with --checksum-algorithm is framed the same way (captured with aws-cli
2.38.5: one chunk per part, then "0", the line x-amz-checksum-<algorithm>:<base64> and
the closing CRLF), so each refusal here must answer exactly what CRC64NVME answers, and
leave nothing behind.
"""

import base64
import xml.etree.ElementTree as ET

import pytest
from fastapi.testclient import TestClient
from starlette.datastructures import Headers
from test_aws_chunked_decode import BUCKET, DATA, KEY, KINDS, Env, call, md5

from chaos.framework.framed import frame, framed_headers
from server.api.aws_chunked import preflight
from server.checksums import ALGORITHMS

ALGORITHM_NAMES = ["crc32", "crc32c", "sha1", "sha256"]


@pytest.fixture
async def env(tmp_path) -> Env:
    e = Env(tmp_path)
    await e.setup()
    return e


def trailer_of(name: str) -> str:
    return ALGORITHMS[name].header


def b64(name: str, data: bytes) -> str:
    hasher = ALGORITHMS[name].new()
    hasher.update(data)
    return base64.b64encode(hasher.digest()).decode("ascii")


def framed(name: str, data: bytes, trailer_line: str | None = None, **override):
    """(headers, body) of a one chunk framed upload of data with the trailer of name."""
    if trailer_line is None:
        trailer_line = f"{trailer_of(name)}:{b64(name, data)}"
    body = frame(data, trailer_line=trailer_line)
    override.setdefault("x-amz-trailer", trailer_of(name))
    return framed_headers(body, len(data), **override), body


# --- a correct trailer ---


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("name", ALGORITHM_NAMES)
async def test_correct_trailer_stores_the_bytes(env, kind, name):
    headers, body = framed(name, DATA)
    r = await env.send(kind, headers, [body])
    assert r.status == 200, r.body
    assert r.headers["etag"] == md5(DATA)
    assert env.stored_bytes(kind) == (DATA, md5(DATA))
    if kind == "put":
        got = TestClient(env.app).get(f"/{BUCKET}/{KEY}")
        assert got.status_code == 200 and got.content == DATA


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("name", ALGORITHM_NAMES)
async def test_correct_trailer_over_many_chunks_and_an_empty_body(env, kind, name):
    body = frame(
        DATA,
        sizes=[1, 4095, 16, 1008],
        trailer_line=f"{trailer_of(name)}:{b64(name, DATA)}",
    )
    headers = framed_headers(body, len(DATA), **{"x-amz-trailer": trailer_of(name)})
    assert (await env.send(kind, headers, [body])).status == 200
    assert env.stored_bytes(kind) == (DATA, md5(DATA))

    empty = f"0\r\n{trailer_of(name)}:{b64(name, b'')}\r\n\r\n".encode()
    empty_headers = framed_headers(empty, 0, **{"x-amz-trailer": trailer_of(name)})
    assert (await env.send(kind, empty_headers, [empty])).status == 200
    assert env.stored_bytes(kind) == (b"", md5(b""))


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("name", ALGORITHM_NAMES)
async def test_trailer_name_and_header_are_case_insensitive(env, kind, name):
    upper = trailer_of(name).upper()
    headers, body = framed(name, DATA, f"{upper}:{b64(name, DATA)}", **{"x-amz-trailer": upper})
    assert (await env.send(kind, headers, [body])).status == 200
    assert env.stored_bytes(kind) == (DATA, md5(DATA))


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("name", ALGORITHM_NAMES)
async def test_sdk_checksum_algorithm_header_must_agree_with_the_trailer(env, kind, name):
    sdk = {"x-amz-sdk-checksum-algorithm": name.upper()}
    headers, body = framed(name, DATA, **sdk)
    assert (await env.send(kind, headers, [body])).status == 200

    other = "SHA256" if name != "sha256" else "CRC32"
    headers, body = framed(name, DATA, **{"x-amz-sdk-checksum-algorithm": other})
    r = await env.send(kind, headers, [body])
    assert (r.status, r.code) == (400, "BadDigest")


def test_preflight_names_the_algorithm_of_the_trailer():
    for name in ALGORITHM_NAMES:
        framed_request = preflight(
            Headers(
                {
                    "x-amz-decoded-content-length": "1",
                    "content-length": "9",
                    "x-amz-trailer": trailer_of(name),
                }
            )
        )
        assert framed_request.algorithm is ALGORITHMS[name]


# --- refusals, each answering what the CRC64NVME trailer answers ---


def _wrong_value(name):
    return framed(name, DATA, f"{trailer_of(name)}:{b64(name, DATA + b'x')}"), "BadDigest"


def _malformed(value):
    def case(name):
        return framed(name, DATA, f"{trailer_of(name)}:{value}"), "InvalidDigest"

    return case


def _wrong_size(name):
    # The digest of a different algorithm, so base64 of the wrong number of bytes.
    other = "sha256" if name != "sha256" else "crc32"
    return framed(name, DATA, f"{trailer_of(name)}:{b64(other, DATA)}"), "InvalidDigest"


def _other_trailer_name(other):
    def case(name):
        # The line names another checksum, valid for the other algorithm, than x-amz-trailer.
        return framed(name, DATA, f"{trailer_of(other)}:{b64(other, DATA)}"), "InvalidRequest"

    return case


def _missing_trailer(name):
    body = frame(DATA, sizes=[5120])
    body = body[: -len(f"{trailer_of('crc64nvme')}:{b64('crc64nvme', DATA)}\r\n\r\n")] + b"\r\n"
    headers = framed_headers(body, len(DATA), **{"x-amz-trailer": trailer_of(name)})
    return (headers, body), "InvalidRequest"


def _no_colon(name):
    return framed(name, DATA, trailer_of(name)), "InvalidRequest"


REFUSALS = {
    "wrong-value": _wrong_value,
    "malformed-not-base64": _malformed("not*base64!"),
    "malformed-empty": _malformed(""),
    "malformed-space": lambda name: (
        framed(name, DATA, f"{trailer_of(name)}: {b64(name, DATA)}"),
        "InvalidDigest",
    ),
    "malformed-wrong-size": _wrong_size,
    "name-of-crc64nvme": _other_trailer_name("crc64nvme"),
    "name-of-sha512": lambda name: (
        framed(name, DATA, f"x-amz-checksum-sha512:{b64('sha256', DATA)}"),
        "InvalidRequest",
    ),
    "missing-trailer": _missing_trailer,
    "trailer-without-colon": _no_colon,
}


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("name", ALGORITHM_NAMES)
@pytest.mark.parametrize("case", sorted(REFUSALS))
async def test_refused_leaves_nothing(env, kind, name, case):
    (headers, body), code = REFUSALS[case](name)
    r = await env.send(kind, headers, [body])
    assert (r.status, r.code) == (400, code), r.body
    env.assert_nothing_left(kind)


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("name", [n for n in ALGORITHM_NAMES if n != "crc32"])
async def test_a_line_naming_another_supported_trailer_is_refused(env, kind, name):
    headers, body = framed(name, DATA, f"{trailer_of('crc32')}:{b64('crc32', DATA)}")
    r = await env.send(kind, headers, [body])
    assert (r.status, r.code) == (400, "InvalidRequest")
    env.assert_nothing_left(kind)


async def test_reupload_over_a_part_with_a_bad_trailer_keeps_the_old_part(env):
    for name in ALGORITHM_NAMES:
        old = b"the first attempt" * 100
        headers, body = framed(name, old)
        assert (await env.send("part", headers, [body])).status == 200
        (before,) = env.metadata.get_parts(env.upload_id)
        records = env.metadata.list_blob_records()

        (headers, body), _ = _wrong_value(name)
        r = await env.send("part", headers, [body])
        assert (r.status, r.code) == (400, "BadDigest")

        (after,) = env.metadata.get_parts(env.upload_id)
        assert after == before
        assert env.metadata.list_blob_records() == records
        assert env.storage.temp_entries() == []


# --- trailers that stay refused ---


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    "value",
    [
        "x-amz-checksum-sha512",
        "x-amz-checksum-md5",
        "x-amz-checksum-xxhash64",
        "x-amz-checksum-xxhash3",
        "x-amz-checksum-xxhash128",
        "x-amz-checksum-crc32,x-amz-checksum-sha256",
        "x-amz-checksum-crc32,x-amz-checksum-crc32",
        "x-amz-checksum-crc32-x",
        "x-amz-checksum-sha",
    ],
)
async def test_other_trailers_stay_501_before_the_body_is_read(env, kind, value):
    body = frame(DATA)
    headers = framed_headers(body, len(DATA), **{"x-amz-trailer": value})
    r = await env.send(kind, headers, [body])
    assert (r.status, r.code) == (501, "NotImplemented"), r.body
    assert r.received == 0
    env.assert_nothing_left(kind)


# --- a multipart upload with the trailer on every part ---

PART_SIZE = 8 * 1024 * 1024
OBJECT_SIZE = 20 * 1024 * 1024


@pytest.mark.parametrize("name", ALGORITHM_NAMES)
async def test_20_mib_multipart_upload_with_a_trailer_on_every_part(tmp_path, name):
    env = Env(tmp_path)
    await env.setup()
    data = bytes(range(251)) * (OBJECT_SIZE // 251 + 1)
    data = data[:OBJECT_SIZE]
    parts = [data[i : i + PART_SIZE] for i in range(0, OBJECT_SIZE, PART_SIZE)]
    assert [len(p) for p in parts] == [PART_SIZE, PART_SIZE, OBJECT_SIZE - 2 * PART_SIZE]

    etags = []
    for number, part in enumerate(parts, start=1):
        headers, body = framed(name, part)
        target = f"/{BUCKET}/{KEY}?partNumber={number}&uploadId={env.upload_id}"
        r = await call(env.app, "PUT", target, headers, [body])
        assert r.status == 200, r.body
        assert r.headers["etag"] == md5(part)
        etags.append(r.headers["etag"])

    document = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{etag}</ETag></Part>"
        for n, etag in enumerate(etags, start=1)
    )
    complete = f"<CompleteMultipartUpload>{document}</CompleteMultipartUpload>".encode()
    r = await call(
        env.app,
        "POST",
        f"/{BUCKET}/{KEY}?uploadId={env.upload_id}",
        {"content-length": str(len(complete))},
        [complete],
    )
    assert r.status == 200, r.body
    assert ET.fromstring(r.body).findtext("{*}Key") == KEY

    got = TestClient(env.app).get(f"/{BUCKET}/{KEY}")
    assert got.status_code == 200
    assert got.content == data
