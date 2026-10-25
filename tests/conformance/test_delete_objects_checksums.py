"""Checksums claimed by a DeleteObjects request (server/checksums.py), verified over the
raw request body: Content-MD5, one x-amz-checksum-<crc32|crc32c|crc64nvme|sha1|sha256>
and x-amz-sdk-checksum-algorithm, with the rules PutObject and UploadPart follow.

None is required (docs/compatibility.md). The order is: NoSuchBucket, the checksum, then
the document (test_delete_objects_document.py). Every refusal is run over seeded keys and
must leave all of them, and the registry, as they were. Digests come from
test_upload_checksums.py, which computes them without the server's code.
"""

import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

# Shared helpers: there is no tests/ package, so pytest puts this directory on sys.path.
from fastapi.testclient import TestClient
from test_delete_objects_document import BUCKET, KEYS, VALID, _doc, _present, _seed
from test_upload_checksums import ALGOS, SDK, b64, claim, digest, md5_claim

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore


@pytest.fixture
def deps(tmp_path: Path):
    return BlobStore(tmp_path / "blobs"), SQLiteMetadataStore(db_path=tmp_path / "meta.db")


@pytest.fixture
def client(deps) -> TestClient:
    storage, metadata = deps
    c = TestClient(create_app(storage=storage, metadata=metadata), raise_server_exceptions=False)
    assert c.put(f"/{BUCKET}").status_code == 200
    return c


@pytest.fixture
def seeded(deps, client):
    for key in KEYS:
        _seed(deps, key)
    return deps


BODY = _doc(VALID).encode()  # deletes k1 and k2
WRONG = BODY + b" "


def _post(client, headers, body: bytes = BODY, path: str = f"/{BUCKET}?delete"):
    return client.post(path, content=body, headers=headers)


def _code(resp) -> str | None:
    return None if resp.status_code == 200 else ET.fromstring(resp.text).findtext("Code")


def _deleted(resp) -> list[str]:
    return [d.findtext("Key") for d in ET.fromstring(resp.text).findall("Deleted")]


# --- accepted ---


def _accepted() -> dict[str, list]:
    cases: dict[str, list] = {"no-claim": []}
    for name in ALGOS:
        cases[name] = [claim(name, BODY)]
        cases[f"{name}-sdk"] = [claim(name, BODY), (SDK, name.upper())]
        cases[f"{name}-and-md5"] = [claim(name, BODY), md5_claim(BODY)]
    cases["md5"] = [md5_claim(BODY)]
    # The headers botocore sends: a CRC32 value and the SDK header naming it.
    cases["botocore"] = [claim("crc32", BODY), (SDK, "CRC32")]
    cases["mode-algorithm-type"] = [
        ("x-amz-checksum-mode", "ENABLED"),
        ("x-amz-checksum-algorithm", "SHA512"),
        ("x-amz-checksum-type", "COMPOSITE"),
    ]
    return cases


ACCEPTED = _accepted()


@pytest.mark.parametrize("name", sorted(ACCEPTED))
def test_matching_claims_and_no_claim_are_accepted(client, seeded, name):
    r = _post(client, dict(ACCEPTED[name]))
    assert r.status_code == 200, r.text
    assert _deleted(r) == ["k1", "k2"]
    assert _present(seeded) == ["k3"]


@pytest.mark.parametrize("name", ALGOS)
def test_claims_are_checked_against_the_exact_bytes(client, seeded, name):
    # The same document with other whitespace is another body: the old digest is wrong.
    other = b'<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' + BODY[8:]
    assert other != BODY
    r = _post(client, dict([claim(name, BODY)]), other)
    assert (r.status_code, _code(r)) == (400, "BadDigest")
    assert _present(seeded) == list(KEYS)


# --- refused ---


def _refusals() -> dict[str, tuple[list, int, str, bool]]:
    """name -> (header pairs, status, code, body read before refusing)."""
    cases: dict[str, tuple[list, int, str, bool]] = {}
    for name in ALGOS:
        cases[f"bad-{name}"] = ([claim(name, WRONG)], 400, "BadDigest", True)
        cases[f"bad-{name}-with-good-md5"] = (
            [claim(name, WRONG), md5_claim(BODY)],
            400,
            "BadDigest",
            True,
        )
        cases[f"good-{name}-with-bad-md5"] = (
            [claim(name, BODY), md5_claim(WRONG)],
            400,
            "BadDigest",
            True,
        )
        good = b64(digest(name, BODY))
        for label, value in {
            "not-base64": "not*base64!",
            "empty": "",
            "space": " " + good,
            "unpadded": good.rstrip("="),
            "3-bytes": b64(b"abc"),
            "too-long": b64(bytes(40)),
        }.items():
            cases[f"malformed-{name}-{label}"] = (
                [(f"x-amz-checksum-{name}", value)],
                400,
                "InvalidDigest",
                False,
            )
    cases["bad-md5"] = ([md5_claim(WRONG)], 400, "BadDigest", True)
    for label, value in {
        "short": "YWJyYWNhZGFicmE=",
        "empty": "",
        "garbage": "AWS HAHAHA",
        "15-bytes": b64(bytes(15)),
        "unpadded": b64(digest("md5", BODY)).rstrip("="),
    }.items():
        cases[f"malformed-md5-{label}"] = ([("content-md5", value)], 400, "InvalidDigest", False)
    cases["md5-twice"] = ([md5_claim(BODY), md5_claim(BODY)], 400, "InvalidDigest", False)
    for other in ("md5", "sha512", "xxhash64", "xxhash3", "xxhash128", "crc32x", "foo"):
        cases[f"unsupported-{other}"] = (
            [(f"x-amz-checksum-{other}", "AAAA")],
            501,
            "NotImplemented",
            False,
        )
    cases["unsupported-with-valid-claims"] = (
        [claim("crc32", BODY), md5_claim(BODY), ("x-amz-checksum-sha512", "AAAA")],
        501,
        "NotImplemented",
        False,
    )
    cases["sdk-alone"] = ([(SDK, "CRC32")], 400, "InvalidRequest", False)
    cases["sdk-with-md5-only"] = ([(SDK, "CRC32"), md5_claim(BODY)], 400, "InvalidRequest", False)
    cases["sdk-twice"] = (
        [claim("crc32", BODY), (SDK, "CRC32"), (SDK, "CRC32")],
        400,
        "InvalidRequest",
        False,
    )
    for sdk in ("SHA256", "CRC32C", "CRC64NVME", "MD5", "SHA512", "", "nonsense"):
        cases[f"sdk-{sdk or 'empty'}-differs-from-crc32"] = (
            [claim("crc32", BODY), (SDK, sdk)],
            400,
            "BadDigest",
            False,
        )
    for a, b in [("crc32", "sha256"), ("crc32c", "crc64nvme"), ("sha1", "sha256")]:
        cases[f"two-headers-{a}-{b}"] = (
            [claim(a, BODY), claim(b, BODY)],
            400,
            "InvalidRequest",
            False,
        )
    cases["one-header-twice"] = (
        [claim("crc32", BODY), claim("crc32", BODY)],
        400,
        "InvalidRequest",
        False,
    )
    return cases


REFUSALS = _refusals()


@pytest.mark.parametrize("name", sorted(REFUSALS))
def test_a_refusal_deletes_nothing(client, seeded, name):
    headers, status, code, _ = REFUSALS[name]
    records = seeded[1].list_blob_records()
    r = _post(client, headers)
    assert (r.status_code, _code(r)) == (status, code), r.text
    assert _present(seeded) == list(KEYS)
    assert seeded[1].list_blob_records() == records
    assert seeded[0].temp_entries() == []


@pytest.mark.parametrize("name", sorted(n for n, c in REFUSALS.items() if not c[3]))
def test_a_header_refusal_is_made_before_the_body_is_read(client, seeded, name):
    # An unreadable body proves it: were the body read first, this would be MalformedXML
    # (or, for the 501, never reached).
    headers, status, code, _ = REFUSALS[name]
    r = _post(client, headers, b"\xff not xml")
    assert (r.status_code, _code(r)) == (status, code)
    assert _present(seeded) == list(KEYS)


# --- order: NoSuchBucket, checksum, document ---


def test_a_missing_bucket_is_reported_before_a_bad_checksum(client, seeded):
    r = _post(client, dict([claim("crc32", WRONG)]), path="/nobucket?delete")
    assert (r.status_code, _code(r)) == (404, "NoSuchBucket")
    r = _post(client, dict([("content-md5", "!!")]), path="/nobucket?delete")
    assert (r.status_code, _code(r)) == (404, "NoSuchBucket")
    assert _present(seeded) == list(KEYS)


def test_the_checksum_is_checked_before_the_document(client, seeded):
    bad_doc = b"<Delete/>"  # MalformedXML on its own
    r = _post(client, dict([claim("crc32", bad_doc + b" ")]), bad_doc)
    assert (r.status_code, _code(r)) == (400, "BadDigest")
    r = _post(client, dict([claim("crc32", bad_doc)]), bad_doc)
    assert (r.status_code, _code(r)) == (400, "MalformedXML")
    assert _present(seeded) == list(KEYS)


def test_a_valid_checksum_does_not_excuse_a_bad_document_after_valid_entries(client, seeded):
    body = _doc(VALID + "<Object/>").encode()
    r = _post(client, dict([claim("sha256", body), md5_claim(body)]), body)
    assert (r.status_code, _code(r)) == (400, "MalformedXML")
    assert _present(seeded) == list(KEYS)


def test_quiet_and_versions_still_work_with_a_claim(client, seeded):
    body = _doc(
        "<Object><Key>k1</Key><VersionId>null</VersionId></Object><Quiet>true</Quiet>"
    ).encode()
    r = _post(client, dict([claim("crc32c", body), md5_claim(body)]), body)
    assert r.status_code == 200 and _deleted(r) == []
    assert _present(seeded) == ["k2", "k3"]


def test_the_bucket_path_with_a_trailing_slash_is_checked_too(client, seeded):
    r = _post(client, dict([claim("crc32", WRONG)]), path=f"/{BUCKET}/?delete")
    assert (r.status_code, _code(r)) == (400, "BadDigest")
    r = _post(client, dict([("x-amz-checksum-sha512", "AAAA")]), path=f"/{BUCKET}/?delete")
    assert r.status_code == 501
    assert _present(seeded) == list(KEYS)
