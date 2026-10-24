"""
Listing conformance tests: ListObjects (V1), ListObjectsV2 and ListObjectVersions.

prefix is an exact, case sensitive prefix of the key: _ and % are ordinary
characters, and keys are listed in UTF-8 byte order.
"""

import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "listing"

# Query strings that select each listing, and the element that holds each key.
LISTINGS = {
    "v1": ({}, "Contents"),
    "v2": ({"list-type": "2"}, "Contents"),
    "versions": ({"versions": ""}, "Version"),
}


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    app = create_app(
        storage=BlobStore(tmp_path / "blobs"),
        metadata=SQLiteMetadataStore(db_path=tmp_path / "meta.db"),
    )
    client = TestClient(app, raise_server_exceptions=False)
    assert client.put(f"/{BUCKET}").status_code == 200
    return client


def _put(client, keys):
    for key in keys:
        resp = client.put(f"/{BUCKET}/{quote(key, safe='')}", content=b"x")
        assert resp.status_code == 200, key


def _list(client, listing, **params):
    query, element = LISTINGS[listing]
    resp = client.get(f"/{BUCKET}", params={**query, **params})
    assert resp.status_code == 200, resp.text
    root = ET.fromstring(resp.text)
    return root, [el.findtext("Key") for el in root.findall(element)]


@pytest.fixture(params=sorted(LISTINGS))
def listing(request):
    return request.param


def test_prefix_underscore_is_literal(client, listing):
    _put(client, ["a_c", "abc", "aXc", "a_"])
    assert _list(client, listing, prefix="a_")[1] == ["a_", "a_c"]


def test_prefix_percent_is_literal(client, listing):
    _put(client, ["a%c", "abc", "a%", "az"])
    assert _list(client, listing, prefix="a%")[1] == ["a%", "a%c"]


def test_prefix_is_case_sensitive(client, listing):
    _put(client, ["Foo/1", "foo/2", "FOO/3"])
    assert _list(client, listing, prefix="foo/")[1] == ["foo/2"]
    assert _list(client, listing, prefix="FOO")[1] == ["FOO/3"]


def test_prefix_non_ascii(client, listing):
    _put(client, ["é/1", "e/2", "ê", "日本/x", "日本語", "日", "\U0001f600a", "\U0001f601"])
    assert _list(client, listing, prefix="é")[1] == ["é/1"]
    assert _list(client, listing, prefix="日本")[1] == ["日本/x", "日本語"]
    assert _list(client, listing, prefix="\U0001f600")[1] == ["\U0001f600a"]


def test_prefix_at_code_point_edges(client, listing):
    _put(client, ["a\U0010ffff", "a\U0010ffffz", "b", "\ud7ffx", "\ue000"])
    assert _list(client, listing, prefix="a\U0010ffff")[1] == ["a\U0010ffff", "a\U0010ffffz"]
    assert _list(client, listing, prefix="\ud7ff")[1] == ["\ud7ffx"]


def test_keys_in_utf8_byte_order(client, listing):
    # U+FFFF is left out: it is not an XML character, and the listing writes keys raw.
    keys = ["b", "B", "a", "_", "%", "é", "\ufffd", "\U00010000", "\ue000", "~"]
    _put(client, keys)
    assert _list(client, listing)[1] == sorted(keys, key=lambda k: k.encode("utf-8"))


def test_prefix_echoed(client, listing):
    _put(client, ["a_1"])
    root, _ = _list(client, listing, prefix="a_")
    assert root.findtext("Prefix") == "a_"
