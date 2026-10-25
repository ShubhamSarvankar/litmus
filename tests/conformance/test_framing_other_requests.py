"""aws-chunked framing is decoded for PutObject and UploadPart alone. On every other
request (request_guard._check_framing) an x-amz-content-sha256 naming a STREAMING-
payload, or an x-amz-decoded-content-length, is 501 NotImplemented before the body is
read: a DeleteObjects, CompleteMultipartUpload or CreateMultipartUpload body is XML, and
a framed one would otherwise be parsed as if it were plain.

Each refusal asserts the app never asked for the body and that nothing changed: no
object deleted or created, the upload still open with its part, no upload created, no
registry row added. The same requests without the framing headers still work.
"""

import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

# Shared harness: there is no tests/ package, so pytest puts this directory on sys.path.
from test_aws_chunked_decode import BUCKET, KEY, Env, call

from chaos.framework.framed import frame

DELETE_BODY = b"<Delete><Object><Key>k1</Key></Object></Delete>"
CREATE_TARGET = f"/{BUCKET}/new-object?uploads"
DELETE_TARGET = f"/{BUCKET}?delete"


@pytest.fixture
async def env(tmp_path: Path):
    e = Env(tmp_path)
    await e.setup()
    for key in ("k1", "k2"):
        r = await call(e.app, "PUT", f"/{BUCKET}/{key}", {"content-length": "1"}, [b"x"])
        assert r.status == 200
    r = await call(e.app, "PUT", e.target("part"), {"content-length": "4"}, [b"part"])
    e.etag = r.headers["etag"]
    return e


def complete_body(etag: str) -> bytes:
    return (
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>"
        f"<ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    ).encode()


def requests(env: Env) -> dict[str, tuple[str, str, bytes]]:
    """name -> (method, target, a well formed body for it)."""
    return {
        "DeleteObjects": ("POST", DELETE_TARGET, DELETE_BODY),
        "DeleteObjects-trailing-slash": ("POST", f"/{BUCKET}/?delete", DELETE_BODY),
        "CompleteMultipartUpload": (
            "POST",
            f"/{BUCKET}/{KEY}?uploadId={env.upload_id}",
            complete_body(env.etag),
        ),
        "CreateMultipartUpload": ("POST", CREATE_TARGET, b""),
        "AbortMultipartUpload": ("DELETE", f"/{BUCKET}/{KEY}?uploadId={env.upload_id}", b""),
        "DeleteObject": ("DELETE", f"/{BUCKET}/k1", b""),
        "GetObject": ("GET", f"/{BUCKET}/k1", b""),
        "HeadObject": ("HEAD", f"/{BUCKET}/k1", b""),
        "ListObjects": ("GET", f"/{BUCKET}", b""),
        "ListBuckets": ("GET", "/", b""),
        "CreateBucket": ("PUT", "/another", b""),
    }


# The headers that mark a framed body, as a client would send them.
STREAMING = {
    "unsigned-trailer": {
        "x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        "x-amz-decoded-content-length": "10",
        "x-amz-trailer": "x-amz-checksum-crc64nvme",
        "content-encoding": "aws-chunked",
    },
    "unsigned-trailer-only": {"x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER"},
    "signed-trailer": {"x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"},
    "signed": {"x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"},
    "ecdsa": {"x-amz-content-sha256": "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER"},
    "unsigned": {"x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD"},
    "lower-case": {"x-amz-content-sha256": "streaming-unsigned-payload-trailer"},
    "decoded-length-alone": {"x-amz-decoded-content-length": "10"},
    "decoded-length-with-unsigned-payload": {
        "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
        "x-amz-decoded-content-length": "10",
    },
    "decoded-length-with-hex-digest": {
        "x-amz-content-sha256": "a" * 64,
        "x-amz-decoded-content-length": "10",
    },
}
NAMES = [
    "DeleteObjects",
    "DeleteObjects-trailing-slash",
    "CompleteMultipartUpload",
    "CreateMultipartUpload",
    "AbortMultipartUpload",
    "DeleteObject",
    "GetObject",
    "HeadObject",
    "ListObjects",
    "ListBuckets",
    "CreateBucket",
]


def snapshot(env: Env):
    return (
        env.metadata.list_blob_records(),
        sorted(u.upload_id for u in env.metadata.list_incomplete_uploads()),
        env.metadata.get_parts(env.upload_id),
        [env.metadata.get_object_meta(BUCKET, k) for k in ("k1", "k2", KEY, "new-object")],
        [b.name for b in env.metadata.list_buckets()],
    )


@pytest.mark.parametrize("framing", sorted(STREAMING))
@pytest.mark.parametrize("name", NAMES)
async def test_framing_is_refused_before_the_body_is_read(env, name, framing):
    method, target, body = requests(env)[name]
    before = snapshot(env)
    # The body is a framed one for the requests that have a body; the others have none.
    framed = frame(body) if body else b""
    headers = dict(STREAMING[framing]) | {"content-length": str(len(framed))}
    r = await call(env.app, method, target, headers, [framed] if framed else [])
    if method == "HEAD":
        assert r.status == 501
    else:
        assert (r.status, r.code) == (501, "NotImplemented"), r.body
    assert r.received == 0
    assert snapshot(env) == before
    assert env.storage.temp_entries() == []


async def test_the_refusal_names_the_header(env):
    method, target, body = requests(env)["DeleteObjects"]
    r = await call(env.app, method, target, STREAMING["signed"], [frame(body)])
    assert ET.fromstring(r.body).findtext("Header") == "x-amz-content-sha256"
    r = await call(env.app, method, target, STREAMING["decoded-length-alone"], [body])
    assert ET.fromstring(r.body).findtext("Header") == "x-amz-decoded-content-length"


# --- the same requests, not framed, still work ---


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"x-amz-content-sha256": "UNSIGNED-PAYLOAD"},
        {"x-amz-content-sha256": "a" * 64},
        # Content-Encoding alone does not mark a framed body (S3 does not require it).
        {"content-encoding": "aws-chunked"},
    ],
    ids=["none", "unsigned-payload", "hex-digest", "content-encoding-only"],
)
@pytest.mark.parametrize(
    "name", ["DeleteObjects", "CompleteMultipartUpload", "CreateMultipartUpload"]
)
async def test_without_framing_headers_the_request_is_served(env, name, headers):
    method, target, body = requests(env)[name]
    r = await call(env.app, method, target, headers | {"content-length": str(len(body))}, [body])
    assert r.status == 200, r.body
    if name == "DeleteObjects":
        assert env.metadata.get_object_meta(BUCKET, "k1") is None
    elif name == "CompleteMultipartUpload":
        assert env.metadata.get_object_meta(BUCKET, KEY) is not None
    else:
        assert len(env.metadata.list_incomplete_uploads()) == 2


@pytest.mark.parametrize("kind", ["put", "part"])
async def test_put_object_and_upload_part_still_decode_their_framing(env, kind):
    from chaos.framework.framed import framed_headers

    data = b"decoded " * 100
    body = frame(data)
    r = await env.send(kind, framed_headers(body, len(data)), [body])
    assert r.status == 200
    assert env.stored_bytes(kind)[0] == data
