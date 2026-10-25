"""Cache-Control, Content-Disposition, Content-Encoding, Content-Language and Expires
given to PutObject are stored with the object and returned by GET and HEAD, on 200 and
on 206. An overwrite replaces all of them: a header the new PUT lacks is cleared.

Content-Encoding loses its aws-chunked tokens, which name the request's framing, not
an encoding of the object (docs/compatibility.md, "System headers").
"""

from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from chaos.framework.framed import framed_request
from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "hdr-bucket"
URL = f"/{BUCKET}/k"
BODY = b"0123456789"

ALL = {
    "Cache-Control": "public, max-age=14400",
    "Content-Disposition": 'attachment; filename="a b.txt"',
    "Content-Encoding": "gzip",
    "Content-Language": "en-US",
    "Expires": "Thu, 01 Dec 2033 16:00:00 GMT",
}


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    app = create_app(storage=storage, metadata=metadata)
    client = TestClient(app, raise_server_exceptions=False)
    assert client.put(f"/{BUCKET}").status_code == 200
    return client


def _get(client, headers=None):
    """(status, headers, raw bytes) of GET: raw, because the stored Content-Encoding
    would make the client decode the body."""
    with client.stream("GET", URL, headers=headers) as resp:
        return resp.status_code, resp.headers, b"".join(resp.iter_raw())


def _head(client):
    resp = client.head(URL)
    assert resp.status_code == 200
    return resp.headers


@pytest.mark.parametrize("name", list(ALL))
def test_each_header_is_returned_by_get_and_head(client, name):
    assert client.put(URL, content=BODY, headers={name: ALL[name]}).status_code == 200
    status, headers, body = _get(client)
    assert (status, body) == (200, BODY)
    assert headers[name] == ALL[name]
    assert _head(client)[name] == ALL[name]
    for other in ALL.keys() - {name}:
        assert other not in headers
        assert other not in _head(client)


def test_all_headers_together_on_200_206_and_head(client):
    assert client.put(URL, content=BODY, headers=ALL).status_code == 200
    status, headers, _ = _get(client)
    assert status == 200
    status, ranged, body = _get(client, {"range": "bytes=2-4"})
    assert (status, body) == (206, b"234")
    for name, value in ALL.items():
        assert headers[name] == value
        assert ranged[name] == value
        assert _head(client)[name] == value


def test_a_suffix_range_carries_them_too(client):
    client.put(URL, content=BODY, headers=ALL)
    status, headers, body = _get(client, {"range": "bytes=-3"})
    assert (status, body) == (206, b"789")
    assert headers["Cache-Control"] == ALL["Cache-Control"]


def test_object_put_without_them_returns_none(client):
    client.put(URL, content=BODY)
    _, headers, _ = _get(client)
    for name in ALL:
        assert name not in headers
        assert name not in _head(client)


def test_overwrite_replaces_all_of_them(client):
    client.put(URL, content=BODY, headers=ALL)
    new = {
        "Cache-Control": "no-store",
        "Content-Disposition": "inline",
        "Content-Encoding": "br",
        "Content-Language": "fr",
        "Expires": "Fri, 02 Dec 2033 16:00:00 GMT",
    }
    client.put(URL, content=b"new", headers=new)
    _, headers, body = _get(client)
    assert body == b"new"
    for name, value in new.items():
        assert headers[name] == value


def test_overwrite_without_them_clears_them(client):
    client.put(URL, content=BODY, headers=ALL)
    client.put(URL, content=b"new")
    _, headers, body = _get(client)
    assert body == b"new"
    for name in ALL:
        assert name not in headers
        assert name not in _head(client)


def test_overwrite_with_some_clears_the_rest(client):
    client.put(URL, content=BODY, headers=ALL)
    client.put(URL, content=b"new", headers={"Content-Language": "de"})
    headers = _head(client)
    assert headers["Content-Language"] == "de"
    for name in ALL.keys() - {"Content-Language"}:
        assert name not in headers


def test_content_type_is_kept_beside_them(client):
    client.put(URL, content=BODY, headers={**ALL, "Content-Type": "text/plain"})
    assert _head(client)["Content-Type"] == "text/plain"
    client.put(URL, content=BODY, headers={"Cache-Control": "no-cache"})
    assert _head(client)["Content-Type"] == "binary/octet-stream"


@pytest.mark.parametrize(
    ("sent", "stored"),
    [
        ("gzip", "gzip"),
        ("deflate, gzip", "deflate, gzip"),
        ("deflate,gzip", "deflate, gzip"),
        ("gzip, aws-chunked", "gzip"),
        ("aws-chunked, gzip", "gzip"),
        ("gzip,aws-chunked", "gzip"),
        ("AWS-Chunked, gzip", "gzip"),
        ("gzip, Aws-Chunked, br", "gzip, br"),
        ("aws-chunked", None),
        ("AWS-CHUNKED", None),
        ("aws-chunked, aws-chunked", None),
        ("aws-chunked,aws-chunked", None),
        (" aws-chunked ", None),
    ],
)
def test_content_encoding_loses_aws_chunked_tokens(client, sent, stored):
    assert client.put(URL, content=BODY, headers={"Content-Encoding": sent}).status_code == 200
    headers = _head(client)
    if stored is None:
        assert "Content-Encoding" not in headers
    else:
        assert headers["Content-Encoding"] == stored


def test_a_token_that_only_contains_aws_chunked_is_kept(client):
    client.put(URL, content=BODY, headers={"Content-Encoding": "x-aws-chunked, gzip"})
    assert _head(client)["Content-Encoding"] == "x-aws-chunked, gzip"


def test_repeated_content_encoding_headers_are_read_as_one_list(client):
    client.put(
        URL,
        content=BODY,
        headers=[("content-encoding", "aws-chunked"), ("content-encoding", "gzip")],
    )
    assert _head(client)["Content-Encoding"] == "gzip"


def test_a_framed_put_stores_the_decoded_bytes_and_the_remaining_encoding(client):
    headers, body = framed_request(BODY)
    headers["content-encoding"] = "gzip, aws-chunked"
    assert client.put(URL, content=body, headers=headers).status_code == 200
    status, got, raw = _get(client)
    assert (status, raw) == (200, BODY)
    assert got["Content-Encoding"] == "gzip"


def test_a_framed_put_with_only_aws_chunked_stores_no_encoding(client):
    headers, body = framed_request(BODY)
    assert headers["content-encoding"] == "aws-chunked"
    assert client.put(URL, content=body, headers=headers).status_code == 200
    status, got, raw = _get(client)
    assert (status, raw) == (200, BODY)
    assert "Content-Encoding" not in got


def test_repeated_headers_are_joined_with_a_comma(client):
    client.put(
        URL,
        content=BODY,
        headers=[("cache-control", "no-cache"), ("cache-control", "no-store")],
    )
    assert _head(client)["Cache-Control"] == "no-cache, no-store"


@pytest.mark.parametrize("name", ["Cache-Control", "Content-Language", "Content-Disposition"])
def test_a_blank_value_is_stored_as_no_value(client, name):
    client.put(URL, content=BODY, headers={name: "x"})
    client.put(URL, content=BODY, headers={name: ""})
    assert name not in _head(client)


@pytest.mark.parametrize("value", ["123", "0", "-1", "tomorrow", "2033-12-01T16:00:00Z"])
def test_an_unparseable_expires_is_stored_and_returned_as_sent(client, value):
    assert client.put(URL, content=BODY, headers={"Expires": value}).status_code == 200
    assert _head(client)["Expires"] == value
    assert _get(client)[1]["Expires"] == value


async def test_a_non_ascii_value_survives_the_round_trip(client):
    # Header bytes pass through as latin-1, which is how the server reads and writes
    # them; the TestClient cannot hold such a response header, so ask the app directly.
    value = b"inline; filename=caf\xe9.txt"
    transport = httpx.ASGITransport(app=client.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as raw:
        await raw.put(URL, content=BODY, headers={"Content-Disposition": value})
        resp = await raw.head(URL)
    assert dict(resp.headers.raw)[b"content-disposition"] == value


def test_headers_are_per_key(client):
    client.put(URL, content=BODY, headers=ALL)
    client.put(f"/{BUCKET}/other", content=BODY)
    assert "Cache-Control" not in client.head(f"/{BUCKET}/other").headers
    assert _head(client)["Cache-Control"] == ALL["Cache-Control"]


def test_delete_then_put_without_them_starts_clean(client):
    client.put(URL, content=BODY, headers=ALL)
    assert client.delete(URL).status_code == 204
    client.put(URL, content=BODY)
    assert "Cache-Control" not in _head(client)
