"""Checksums claimed by a PutObject or UploadPart request (server/checksums.py): the
x-amz-checksum-<crc32|crc32c|crc64nvme|sha1|sha256> headers, Content-MD5 and
x-amz-sdk-checksum-algorithm, on plain bodies and on aws-chunked bodies (where the
headers apply to the decoded bytes).

Same harness as test_aws_chunked_decode.py: the app is driven through ASGI. Every
refusal is checked for the status and S3 code, for whether the body was read at all,
and then for what must not exist: no registry row, an empty tmp/, no object or part,
and a clean consistency sweep. Each refusal is also run over an existing object or part
and must leave it, its ETag and its bytes exactly as they were.

The digests are computed here, not by server/checksums.py: zlib, hashlib and two short
reference CRCs, so a mistake in the server's algorithm table cannot hide itself.
"""

import base64
import errno
import hashlib
import os
import zlib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

# Shared harness; there is no tests/ package, so pytest puts this directory on sys.path.
from test_aws_chunked_decode import BUCKET, DATA, KEY, Env, call, split

from chaos.framework.framed import frame, framed_headers
from server.consistency import run_consistency_sweep
from server.storage import blobs

KINDS = ["put", "part"]
ALGOS = ["crc32", "crc32c", "crc64nvme", "sha1", "sha256"]


# --- reference digests ---


def _crc_table(poly: int) -> list[int]:
    table = []
    for n in range(256):
        crc = n
        for _ in range(8):
            crc = (crc >> 1) ^ poly if crc & 1 else crc >> 1
        table.append(crc)
    return table


_CRC32C_TABLE = _crc_table(0x82F63B78)  # CRC-32C (Castagnoli), reflected
_CRC64_TABLE = _crc_table(0x9A6C9329AC4BC9B5)  # CRC-64/NVME, reflected


def _crc32c(data: bytes) -> bytes:
    crc = 0xFFFFFFFF
    for byte in data:
        crc = _CRC32C_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return (crc ^ 0xFFFFFFFF).to_bytes(4, "big")


def _crc64nvme(data: bytes) -> bytes:
    crc = 0xFFFFFFFFFFFFFFFF
    for byte in data:
        crc = _CRC64_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return (crc ^ 0xFFFFFFFFFFFFFFFF).to_bytes(8, "big")


def digest(name: str, data: bytes) -> bytes:
    return {
        "crc32": lambda: zlib.crc32(data).to_bytes(4, "big"),
        "crc32c": lambda: _crc32c(data),
        "crc64nvme": lambda: _crc64nvme(data),
        "sha1": lambda: hashlib.sha1(data).digest(),
        "sha256": lambda: hashlib.sha256(data).digest(),
        "md5": lambda: hashlib.md5(data).digest(),
    }[name]()


def b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode()


def claim(name: str, data: bytes = DATA) -> tuple[str, str]:
    """The value header of an algorithm, for data."""
    return f"x-amz-checksum-{name}", b64(digest(name, data))


def md5_claim(data: bytes = DATA) -> tuple[str, str]:
    return "content-md5", b64(digest("md5", data))


WRONG = DATA + b"x"
SDK = "x-amz-sdk-checksum-algorithm"


def test_reference_digests_match_published_check_values():
    assert digest("crc32", b"123456789").hex() == "cbf43926"
    assert digest("crc32c", b"123456789").hex() == "e3069283"
    assert digest("crc64nvme", b"123456789").hex() == "ae8b14860a799888"
    assert digest("sha1", b"abc").hex() == "a9993e364706816aba3e25717850c26c9cd0d89d"


# --- building requests ---


def request(framing: str, data: bytes, extra) -> tuple[list[tuple[str, str]], list[bytes]]:
    """(header pairs, body pieces) of an upload of data, plain or aws-chunked, with the
    extra header pairs added."""
    if framing == "plain":
        return [("content-length", str(len(data))), *extra], [data]
    body = frame(data)
    return [*framed_headers(body, len(data)).items(), *extra], [body]


@pytest.fixture
async def env(tmp_path: Path) -> Env:
    e = Env(tmp_path)
    await e.setup()
    return e


FRAMINGS = ["plain", "framed"]


def md5(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


# --- accepted ---


def _accepted_cases() -> dict[str, tuple[str, Callable[[bytes], list]]]:
    cases = {}
    for name in ALGOS:
        cases[f"plain-{name}"] = ("plain", lambda d, n=name: [claim(n, d)])
        cases[f"plain-{name}-sdk"] = (
            "plain",
            lambda d, n=name: [claim(n, d), (SDK, n.upper())],
        )
        cases[f"plain-{name}-and-md5"] = ("plain", lambda d, n=name: [claim(n, d), md5_claim(d)])
    cases["plain-md5"] = ("plain", lambda d: [md5_claim(d)])
    cases["plain-all-three"] = (
        "plain",
        lambda d: [claim("sha256", d), (SDK, "SHA256"), md5_claim(d)],
    )
    # These share the x-amz-checksum- prefix but carry no checksum value.
    cases["plain-mode-algorithm-type"] = (
        "plain",
        lambda d: [
            ("x-amz-checksum-mode", "ENABLED"),
            ("x-amz-checksum-algorithm", "SHA512"),
            ("x-amz-checksum-type", "COMPOSITE"),
        ],
    )
    cases["framed-md5"] = ("framed", lambda d: [md5_claim(d)])
    cases["framed-sdk"] = ("framed", lambda d: [(SDK, "CRC64NVME")])
    cases["framed-sdk-and-md5"] = ("framed", lambda d: [(SDK, "crc64nvme"), md5_claim(d)])
    return cases


ACCEPTED = _accepted_cases()


def stored_algorithm(name: str) -> str:
    """The algorithm a PutObject of the accepted case name stores and returns: the one
    it claimed, else CRC64NVME (a framed case claims CRC64NVME through its trailer)."""
    if name == "plain-all-three":
        return "sha256"
    claimed = name.split("-")[1]
    return claimed if claimed in ALGOS else "crc64nvme"


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("name", sorted(ACCEPTED))
async def test_matching_claims_are_accepted(env, kind, name):
    framing, extra = ACCEPTED[name]
    headers, chunks = request(framing, DATA, extra(DATA))
    r = await env.send(kind, headers, chunks)
    assert r.status == 200, r.body
    assert env.stored_bytes(kind) == (DATA, md5(DATA))
    assert r.headers["etag"] == md5(DATA)
    returned = {h: v for h, v in r.headers.items() if h.startswith("x-amz-checksum")}
    if kind == "put":
        algorithm = stored_algorithm(name)
        assert returned == {
            f"x-amz-checksum-{algorithm}": b64(digest(algorithm, DATA)),
            "x-amz-checksum-type": "FULL_OBJECT",
        }
    else:
        # A part's checksum is a later step.
        assert returned == {}


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("name", ALGOS)
async def test_empty_body(env, kind, name):
    headers, chunks = request("plain", b"", [claim(name, b""), md5_claim(b"")])
    r = await env.send(kind, headers, chunks)
    assert r.status == 200, r.body
    assert env.stored_bytes(kind) == (b"", md5(b""))


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    "name, data, wire",
    [
        # Published check values.
        ("crc32", b"123456789", "y/Q5Jg=="),
        ("crc32c", b"123456789", "4waSgw=="),
        ("crc64nvme", b"123456789", "rosUhgp5mIg="),
        ("sha1", b"abc", "qZk+NkcGgWq6PiVxeFDCbJzQ2J0="),
        ("sha256", b"abc", "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="),
        # From the ceph suite (test_object_checksum_sha256, _crc64nvme).
        ("sha256", b"A" * 1024, "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0="),
        ("crc64nvme", b"A" * 1024, "Qeh8oXvGiSo="),
    ],
)
async def test_known_values_end_to_end(env, kind, name, data, wire):
    headers, chunks = request("plain", data, [(f"x-amz-checksum-{name}", wire)])
    assert (await env.send(kind, headers, chunks)).status == 200


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("how", ["bytes", "seed-1", "seed-2"])
async def test_claims_cover_a_body_that_arrives_in_pieces(env, kind, how):
    headers, _ = request("plain", DATA, [claim("crc32c"), md5_claim()])
    r = await env.send(kind, headers, split(DATA, how))
    assert r.status == 200, r.body
    assert env.stored_bytes(kind) == (DATA, md5(DATA))


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("how", ["bytes", "seed-1", "seed-2"])
async def test_framed_content_md5_covers_the_decoded_bytes(env, kind, how):
    body = frame(DATA, sizes=[1, 4095, 16, 1008])
    headers = framed_headers(body, len(DATA)) | dict([md5_claim()])
    r = await env.send(kind, headers, split(body, how))
    assert r.status == 200, r.body
    assert env.stored_bytes(kind) == (DATA, md5(DATA))


@pytest.mark.parametrize("kind", KINDS)
async def test_framed_content_md5_of_the_framed_bytes_is_a_bad_digest(env, kind):
    body = frame(DATA)
    headers = framed_headers(body, len(DATA)) | dict([md5_claim(body)])
    r = await env.send(kind, headers, [body])
    assert (r.status, r.code) == (400, "BadDigest")
    env.assert_nothing_left(kind)


# --- refused ---


@dataclass(frozen=True)
class Refusal:
    extra: Callable[[bytes], list]
    status: int
    code: str
    body_read: bool  # whether the app asked for the body before refusing


def _refusals() -> dict[str, tuple[str, Refusal]]:
    cases: dict[str, tuple[str, Refusal]] = {}

    def add(name: str, framing: str, extra, status: int, code: str, body_read: bool):
        cases[f"{framing}-{name}"] = (framing, Refusal(extra, status, code, body_read))

    for name in ALGOS:
        # A wrong digest is found when the body has been read.
        add(f"bad-{name}", "plain", lambda d, n=name: [claim(n, WRONG)], 400, "BadDigest", True)
        add(
            f"bad-{name}-with-good-md5",
            "plain",
            lambda d, n=name: [claim(n, WRONG), md5_claim(d)],
            400,
            "BadDigest",
            True,
        )
        add(
            f"good-{name}-with-bad-md5",
            "plain",
            lambda d, n=name: [claim(n, d), md5_claim(WRONG)],
            400,
            "BadDigest",
            True,
        )
        # A value that is not base64 of the right size is found from the headers alone.
        good = b64(digest(name, DATA))
        for label, value in {
            "not-base64": "not*base64!",
            "empty": "",
            "space": " " + good,
            "unpadded": good.rstrip("="),
            "3-bytes": b64(b"abc"),
            "too-long": b64(bytes(40)),
        }.items():
            add(
                f"malformed-{name}-{label}",
                "plain",
                lambda d, n=name, v=value: [(f"x-amz-checksum-{n}", v)],
                400,
                "InvalidDigest",
                False,
            )
        # A value header and a trailer are two claims.
        add(
            f"{name}-with-trailer",
            "framed",
            lambda d, n=name: [claim(n, d)],
            400,
            "InvalidRequest",
            False,
        )

    for framing in FRAMINGS:
        add("bad-md5", framing, lambda d: [md5_claim(WRONG)], 400, "BadDigest", True)
        add(
            "md5-of-another-body",
            framing,
            lambda d: [md5_claim(b"")],
            400,
            "BadDigest",
            True,
        )
        for label, value in {
            "short": "YWJyYWNhZGFicmE=",  # 10 bytes; the value the ceph suite sends
            "empty": "",
            "garbage": "AWS HAHAHA",
            "15-bytes": b64(bytes(15)),
            "17-bytes": b64(bytes(17)),
            "unpadded": b64(digest("md5", DATA)).rstrip("="),
        }.items():
            add(
                f"malformed-md5-{label}",
                framing,
                lambda d, v=value: [("content-md5", v)],
                400,
                "InvalidDigest",
                False,
            )
        add(
            "md5-twice",
            framing,
            lambda d: [md5_claim(d), md5_claim(d)],
            400,
            "InvalidDigest",
            False,
        )
        # Another algorithm's header is refused before the body is read, whatever else
        # the request carries.
        for other in ("md5", "sha512", "xxhash64", "xxhash3", "xxhash128", "crc32x", "foo"):
            add(
                f"unsupported-{other}",
                framing,
                lambda d, o=other: [(f"x-amz-checksum-{o}", "AAAA")],
                501,
                "NotImplemented",
                False,
            )
        add(
            "unsupported-with-valid-claims",
            framing,
            lambda d: [md5_claim(d), ("x-amz-checksum-sha512", "AAAA")],
            501,
            "NotImplemented",
            False,
        )
        add(
            "sdk-twice-with-md5",
            framing,
            lambda d: [md5_claim(d), (SDK, "CRC64NVME"), (SDK, "CRC64NVME")],
            400,
            "InvalidRequest",
            False,
        )

    # x-amz-sdk-checksum-algorithm needs the checksum it names.
    add("sdk-alone", "plain", lambda d: [(SDK, "CRC32")], 400, "InvalidRequest", False)
    add(
        "sdk-with-md5-only",
        "plain",
        lambda d: [(SDK, "CRC32"), md5_claim(d)],
        400,
        "InvalidRequest",
        False,
    )
    add(
        "sdk-twice",
        "plain",
        lambda d: [claim("crc32", d), (SDK, "CRC32"), (SDK, "CRC32")],
        400,
        "InvalidRequest",
        False,
    )
    for sdk in ("SHA256", "CRC32C", "CRC64NVME", "MD5", "SHA512", "", "nonsense"):
        add(
            f"sdk-{sdk or 'empty'}-differs-from-crc32",
            "plain",
            lambda d, s=sdk: [claim("crc32", d), (SDK, s)],
            400,
            "BadDigest",
            False,
        )
    add(
        "sdk-differs-from-trailer",
        "framed",
        lambda d: [(SDK, "CRC32")],
        400,
        "BadDigest",
        False,
    )
    add(
        "sdk-differs-from-trailer-with-md5",
        "framed",
        lambda d: [(SDK, "SHA256"), md5_claim(d)],
        400,
        "BadDigest",
        False,
    )

    # Two value headers, or one twice.
    for a, b in [("crc32", "sha256"), ("crc32c", "crc64nvme"), ("sha1", "sha256")]:
        add(
            f"two-headers-{a}-{b}",
            "plain",
            lambda d, a=a, b=b: [claim(a, d), claim(b, d)],
            400,
            "InvalidRequest",
            False,
        )
    add(
        "one-header-twice",
        "plain",
        lambda d: [claim("crc32", d), claim("crc32", d)],
        400,
        "InvalidRequest",
        False,
    )
    add(
        "two-headers-one-wrong",
        "plain",
        lambda d: [claim("crc32", d), claim("sha256", WRONG)],
        400,
        "InvalidRequest",
        False,
    )
    return cases


REFUSALS = _refusals()


def _refusal_params():
    return [pytest.param(name, id=name) for name in sorted(REFUSALS)]


async def _refuse(env: Env, kind: str, name: str):
    framing, case = REFUSALS[name]
    headers, chunks = request(framing, DATA, case.extra(DATA))
    r = await env.send(kind, headers, chunks)
    assert (r.status, r.code) == (case.status, case.code), r.body
    if not case.body_read:
        assert r.received == 0
    return r


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("name", _refusal_params())
async def test_refused_leaves_nothing(env, kind, name):
    await _refuse(env, kind, name)
    env.assert_nothing_left(kind)


def _snapshot(env: Env, kind: str):
    if kind == "put":
        rows = [env.metadata.get_object_meta(BUCKET, KEY)]
    else:
        rows = env.metadata.get_parts(env.upload_id)
    return rows, env.metadata.list_blob_records(), env.stored_bytes(kind)


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("name", _refusal_params())
async def test_refused_keeps_the_object_or_part_it_would_replace(env, kind, name):
    old = b"the first attempt" * 100
    headers, chunks = request("plain", old, [])
    assert (await env.send(kind, headers, chunks)).status == 200
    before = _snapshot(env, kind)
    assert before[2] == (old, md5(old))

    await _refuse(env, kind, name)

    assert _snapshot(env, kind) == before
    assert env.storage.temp_entries() == []
    assert run_consistency_sweep(env.metadata, env.storage).issues == []
    # Only the old blob is on disk.
    assert len([e for e, _ in env.storage.scan()]) == 1


# --- no durable step before the claims are checked ---


@pytest.fixture
def fsyncs(monkeypatch):
    calls = []
    real = os.fsync

    def fsync(fd):
        calls.append(fd)
        return real(fd)

    monkeypatch.setattr(os, "fsync", fsync)
    return calls


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    "name", ["plain-bad-crc32", "plain-bad-sha256", "plain-bad-md5", "framed-bad-md5"]
)
async def test_a_refused_body_costs_no_fsync(env, kind, name, fsyncs):
    fsyncs.clear()
    await _refuse(env, kind, name)
    assert fsyncs == []
    env.assert_nothing_left(kind)


@pytest.mark.parametrize("kind", KINDS)
async def test_an_accepted_body_is_synced_after_the_check(env, kind, fsyncs):
    fsyncs.clear()
    headers, chunks = request("plain", DATA, [claim("crc32"), md5_claim()])
    assert (await env.send(kind, headers, chunks)).status == 200
    assert len(fsyncs) >= 2  # the blob file and its directory


@pytest.mark.parametrize("kind", KINDS)
async def test_verification_sees_the_whole_body(env, kind, monkeypatch):
    """The check runs after the last byte is written, not on a prefix."""
    seen = []
    real_verify = blobs.BlobStore.open_writer

    def open_writer(self, blob_id, mid_write_point=None, verify=None):
        def spy(writer):
            seen.append(writer.size)
            verify(writer)

        return real_verify(self, blob_id, mid_write_point=mid_write_point, verify=spy)

    monkeypatch.setattr(blobs.BlobStore, "open_writer", open_writer)
    big = os.urandom(300_000)
    headers, chunks = request("plain", big, [claim("sha256", big), md5_claim(big)])
    assert (await env.send(kind, headers, chunks)).status == 200
    assert seen == [len(big)]


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("framing", FRAMINGS)
async def test_enospc_with_claims_leaves_nothing(env, kind, framing, monkeypatch):
    def write(self, data):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(blobs.BlobWriter, "write", write)
    headers, chunks = request(framing, DATA, [md5_claim()])
    r = await env.send(kind, headers, chunks)
    assert (r.status, r.code) == (500, "InternalError")
    env.assert_nothing_left(kind)


# --- the claims are read on the right requests only ---


async def test_other_requests_are_not_given_a_checksum_error(env):
    """Only PutObject and UploadPart verify (docs/compatibility.md): a bucket request
    carrying the headers is still a bucket request."""
    r = await call(env.app, "PUT", "/another", {"content-md5": "!!"}, [])
    assert r.status == 200


async def test_head_and_get_ignore_the_claim_headers_and_return_the_checksum_with_the_mode(env):
    headers, chunks = request("plain", DATA, [])
    assert (await env.send("put", headers, chunks)).status == 200
    for method in ("GET", "HEAD"):
        # A claim header on a read is not a checksum to check: it is ignored.
        r = await call(env.app, method, f"/{BUCKET}/{KEY}", {claim("sha256")[0]: "AAAA"}, [])
        assert r.status == 200
        assert not [h for h in r.headers if h.startswith("x-amz-checksum")]
        r = await call(
            env.app,
            method,
            f"/{BUCKET}/{KEY}",
            {"x-amz-checksum-mode": "ENABLED", claim("sha256")[0]: "AAAA"},
            [],
        )
        assert r.status == 200
        assert r.headers["x-amz-checksum-crc64nvme"] == b64(digest("crc64nvme", DATA))
        assert r.headers["x-amz-checksum-type"] == "FULL_OBJECT"
        assert "x-amz-checksum-sha256" not in r.headers
