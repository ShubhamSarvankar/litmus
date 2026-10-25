"""The response-* query parameters of GetObject and HeadObject set a response header for
one request: response-content-type, -content-language, -expires, -cache-control,
-content-disposition and -content-encoding. They apply on 200 and 206 and on every
request (Litmus has no authentication), never change what is stored, and are not part
of an error response.

The choices for an empty value, a control character, a non-ASCII value, a repeated
parameter and an unlisted response-* parameter are listed under "Response header
overrides" in docs/compatibility.md.
"""

import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import quote

import httpx
import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "ovr-bucket"
URL = f"/{BUCKET}/k"
BODY = b"0123456789"

STORED = {
    "Cache-Control": "public, max-age=14400",
    "Content-Disposition": 'attachment; filename="stored.txt"',
    "Content-Encoding": "gzip",
    "Content-Language": "en-US",
    "Content-Type": "text/plain",
    "Expires": "Thu, 01 Dec 2033 16:00:00 GMT",
}
PARAMS = {
    "response-cache-control": "Cache-Control",
    "response-content-disposition": "Content-Disposition",
    "response-content-encoding": "Content-Encoding",
    "response-content-language": "Content-Language",
    "response-content-type": "Content-Type",
    "response-expires": "Expires",
}
OVERRIDE = {
    "response-cache-control": "no-cache",
    "response-content-disposition": "inline",
    "response-content-encoding": "br",
    "response-content-language": "fr",
    "response-content-type": "foo/bar",
    "response-expires": "Fri, 02 Dec 2033 16:00:00 GMT",
}


@pytest.fixture
def env(tmp_path: Path):
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=BlobStore(tmp_path / "blobs"), metadata=metadata),
        raise_server_exceptions=False,
    )
    assert client.put(f"/{BUCKET}").status_code == 200
    headers = {**STORED, "x-amz-meta-author": "Janet"}
    assert client.put(URL, content=BODY, headers=headers).status_code == 200
    return client, metadata


@pytest.fixture
def client(env) -> TestClient:
    return env[0]


def _query(**params: str) -> str:
    return "?" + "&".join(f"{k.replace('_', '-')}={quote(v, safe='')}" for k, v in params.items())


def _get(client, query: str = "", headers=None):
    """(status, headers, raw bytes): raw, because a Content-Encoding header would make
    the client decode the body."""
    with client.stream("GET", URL + query, headers=headers) as resp:
        return resp.status_code, resp.headers, b"".join(resp.iter_raw())


def _expect(overridden: dict) -> dict:
    return {**STORED, **overridden}


def _check(headers, expected: dict) -> None:
    for name, value in expected.items():
        assert headers[name] == value, name


# --- each override, on 200, 206 and HEAD ---


@pytest.mark.parametrize("param", list(PARAMS))
def test_each_override_on_get_200(client, param):
    status, headers, body = _get(client, _query(**{param: OVERRIDE[param]}))
    assert (status, body) == (200, BODY)
    _check(headers, _expect({PARAMS[param]: OVERRIDE[param]}))
    assert headers["x-amz-meta-author"] == "Janet"


@pytest.mark.parametrize("param", list(PARAMS))
def test_each_override_on_get_206(client, param):
    status, headers, body = _get(client, _query(**{param: OVERRIDE[param]}), {"range": "bytes=2-4"})
    assert (status, body) == (206, b"234")
    assert headers["Content-Range"] == "bytes 2-4/10"
    _check(headers, _expect({PARAMS[param]: OVERRIDE[param]}))


@pytest.mark.parametrize("param", list(PARAMS))
def test_each_override_on_head(client, param):
    resp = client.head(URL + _query(**{param: OVERRIDE[param]}))
    assert resp.status_code == 200
    assert resp.headers["Content-Length"] == "10"
    _check(resp.headers, _expect({PARAMS[param]: OVERRIDE[param]}))


def test_all_six_together_on_200_206_and_head(client):
    query = _query(**{p: v for p, v in OVERRIDE.items()})
    expected = {PARAMS[p]: v for p, v in OVERRIDE.items()}
    status, headers, _ = _get(client, query)
    assert status == 200
    _check(headers, expected)
    status, headers, _ = _get(client, query, {"range": "bytes=-3"})
    assert status == 206
    _check(headers, expected)
    _check(client.head(URL + query).headers, expected)


def test_a_head_with_a_satisfiable_range_still_carries_them(client):
    resp = client.head(
        URL + _query(response_content_type="foo/bar"), headers={"range": "bytes=2-4"}
    )
    assert resp.status_code == 200
    assert resp.headers["Content-Type"] == "foo/bar"


def test_an_override_sets_a_header_the_object_does_not_have(client):
    client.put(URL, content=BODY)
    _, headers, _ = _get(client)
    assert "Cache-Control" not in headers
    _, headers, _ = _get(client, _query(response_cache_control="no-store"))
    assert headers["Cache-Control"] == "no-store"
    assert client.head(URL + _query(response_expires="123")).headers["Expires"] == "123"


def test_content_type_is_one_header_and_exactly_as_given(client):
    # No charset is added to a text type, and the stored type is not repeated.
    resp = client.head(URL + _query(response_content_type="text/html"))
    assert resp.headers.get_list("content-type") == ["text/html"]
    status, headers, _ = _get(client, _query(response_content_type="text/html"))
    assert (status, headers.get_list("content-type")) == (200, ["text/html"])
    status, headers, _ = _get(
        client, _query(response_content_type="text/html"), {"range": "bytes=0-1"}
    )
    assert (status, headers.get_list("content-type")) == (206, ["text/html"])


def test_values_are_percent_decoded(client):
    value = 'attachment; filename="a b.txt"'
    _, headers, _ = _get(client, "?response-content-disposition=" + quote(value, safe=""))
    assert headers["Content-Disposition"] == value
    _, headers, _ = _get(client, "?response-content-disposition=attachment;%20filename=x")
    assert headers["Content-Disposition"] == "attachment; filename=x"


def test_the_object_with_no_stored_headers_and_the_empty_object(client):
    client.put(URL, content=b"")
    status, headers, body = _get(client, _query(response_content_language="eo"))
    assert (status, body, headers["Content-Language"]) == (200, b"", "eo")


def test_versionid_null_with_an_override(client):
    status, headers, _ = _get(client, "?versionId=null&response-content-language=eo")
    assert (status, headers["Content-Language"]) == (200, "eo")


# --- never stored ---


def test_an_override_changes_nothing_that_is_stored(env):
    client, metadata = env
    before = metadata.get_object_meta(BUCKET, "k")
    _get(client, _query(**OVERRIDE))
    client.head(URL + _query(**OVERRIDE))
    _get(client, _query(**OVERRIDE), {"range": "bytes=1-2"})
    assert metadata.get_object_meta(BUCKET, "k") == before
    status, headers, body = _get(client)
    assert (status, body) == (200, BODY)
    _check(headers, STORED)
    assert headers["x-amz-meta-author"] == "Janet"
    _check(client.head(URL).headers, STORED)


def test_an_overwrite_after_an_override_stores_only_what_the_put_sent(env):
    client, metadata = env
    _get(client, _query(**OVERRIDE))
    client.put(URL, content=b"new", headers={"Content-Language": "de"})
    row = metadata.get_object_meta(BUCKET, "k")
    assert (row.content_language, row.cache_control, row.content_type) == (
        "de",
        None,
        "binary/octet-stream",
    )


# --- choices ---


@pytest.mark.parametrize("param", list(PARAMS))
def test_an_empty_value_is_no_override(client, param):
    status, headers, _ = _get(client, f"?{param}=")
    assert status == 200
    _check(headers, STORED)
    _check(client.head(URL + f"?{param}=").headers, STORED)


@pytest.mark.parametrize(
    "raw", ["%0D", "%0A", "%0D%0Ax-injected:%201", "%00", "%1F", "%7F", "a%0Ab"]
)
@pytest.mark.parametrize("param", list(PARAMS))
def test_a_control_character_is_refused(client, param, raw):
    for send in (
        lambda: client.get(URL + f"?{param}=x{raw}"),
        lambda: client.head(URL + f"?{param}=x{raw}"),
    ):
        resp = send()
        assert resp.status_code == 400
        assert "x-injected" not in resp.headers
    resp = client.get(URL + f"?{param}=x{raw}")
    root = ET.fromstring(resp.text)
    assert root.findtext("Code") == "InvalidArgument"
    assert root.findtext("ArgumentName") == param
    assert "x-injected" not in resp.text


def test_a_tab_is_allowed(client):
    _, headers, _ = _get(client, "?response-content-disposition=a%09b")
    assert headers["Content-Disposition"] == "a\tb"


def test_a_refusal_comes_after_the_bucket_and_before_the_key(client):
    missing_bucket = client.get("/nobucket/k?response-content-type=x%0D%0Ay")
    assert missing_bucket.status_code == 404
    missing_key = client.get(f"/{BUCKET}/none?response-content-type=x%0D%0Ay")
    assert missing_key.status_code == 400
    assert ET.fromstring(missing_key.text).findtext("Code") == "InvalidArgument"


async def test_a_non_ascii_value_is_sent_as_its_utf8_bytes(client):
    # The TestClient cannot hold such a response header; ask the app directly.
    value = "inline; f=caf\u00e9"
    transport = httpx.ASGITransport(app=client.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as raw:
        query = "?response-content-disposition=" + quote(value, safe="")
        async with raw.stream("GET", URL + query) as resp:
            assert resp.status_code == 200
            assert dict(resp.headers.raw)[b"content-disposition"] == value.encode("utf-8")
            async for _ in resp.aiter_raw():
                pass
        head = await raw.head(URL + query)
        assert dict(head.headers.raw)[b"content-disposition"] == value.encode("utf-8")
        async with raw.stream("GET", URL + "?response-content-language=%E9") as resp:
            assert dict(resp.headers.raw)[b"content-language"] == "\ufffd".encode("utf-8")
            async for _ in resp.aiter_raw():
                pass


def test_a_repeated_parameter_reads_as_its_last_value(client):
    status, headers, _ = _get(
        client, "?response-content-type=first/one&response-content-type=last/two"
    )
    assert (status, headers["Content-Type"]) == (200, "last/two")
    resp = client.head(URL + "?response-content-type=first/one&response-content-type=last/two")
    assert resp.headers["Content-Type"] == "last/two"


def test_a_repeated_parameter_whose_last_value_is_empty_is_no_override(client):
    _, headers, _ = _get(client, "?response-content-type=first/one&response-content-type=")
    assert headers["Content-Type"] == "text/plain"


@pytest.mark.parametrize(
    "query",
    [
        "?response-content-md5=x",
        "?response-foo=bar",
        "?response-=x",
        "?Response-Content-Type=foo/bar",
        "?RESPONSE-CONTENT-TYPE=foo/bar",
        "?response_content_type=foo/bar",
        "?response-content-type%20=foo/bar",
        "?content-type=foo/bar",
    ],
)
def test_other_parameters_are_ignored(client, query):
    status, headers, body = _get(client, query)
    assert (status, body) == (200, BODY)
    _check(headers, STORED)
    _check(client.head(URL + query).headers, STORED)


def test_a_listed_parameter_beside_an_unlisted_one(client):
    _, headers, _ = _get(client, "?response-foo=bar&response-content-language=eo")
    assert headers["Content-Language"] == "eo"
    assert headers["Content-Type"] == "text/plain"


# --- errors carry no overrides ---


def test_error_responses_carry_no_overrides(client):
    query = _query(**OVERRIDE)
    missing = client.get(f"/{BUCKET}/none{query}")
    assert missing.status_code == 404
    for name in ("cache-control", "content-language", "expires", "content-disposition"):
        assert name not in missing.headers
    assert "xml" in missing.headers["content-type"]
    assert client.head(f"/{BUCKET}/none{query}").status_code == 404
    unsatisfiable = client.get(URL + query, headers={"range": "bytes=50-60"})
    assert unsatisfiable.status_code == 416
    assert "cache-control" not in unsatisfiable.headers
    assert "xml" in unsatisfiable.headers["content-type"]
    failed = client.get(URL + query, headers={"if-match": '"nope"'})
    assert failed.status_code == 412
    assert "cache-control" not in failed.headers
