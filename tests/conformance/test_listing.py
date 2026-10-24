"""
Listing conformance tests: ListObjects (V1), ListObjectsV2 and ListObjectVersions.

prefix is an exact, case sensitive prefix of the key: _ and % are ordinary
characters, and keys are listed in UTF-8 byte order. encoding-type=url encodes the
key-valued elements the way botocore decodes them.
"""

import base64
import random
import sqlite3
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import quote, unquote_plus

import pytest
from fastapi.testclient import TestClient

from server.api.continuation_tokens import encode_token
from server.app import create_app
from server.config import settings
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


# --- ListObjects (V1): delimiter, max-keys, marker, Owner ---


def _v1(client, **params):
    resp = client.get(f"/{BUCKET}", params=params)
    assert resp.status_code == 200, resp.text
    root = ET.fromstring(resp.text)
    return root, _contents(root), _common_prefixes(root)


def _contents(root):
    return [el.findtext("Key") for el in root.findall("Contents")]


def _common_prefixes(root):
    return [el.findtext("Prefix") for el in root.findall("CommonPrefixes")]


def test_v1_delimiter_rolls_up_common_prefixes(client):
    _put(client, ["foo/bar", "foo/bar/xyzzy", "quux/thud", "asdf"])
    root, keys, prefixes = _v1(client, delimiter="/")
    assert root.findtext("Delimiter") == "/"
    assert keys == ["asdf"]
    assert prefixes == ["foo/", "quux/"]


def test_v1_delimiter_after_prefix_only(client):
    _put(client, ["foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf"])
    root, keys, prefixes = _v1(client, prefix="foo/", delimiter="/")
    assert (keys, prefixes) == (["foo/bar"], ["foo/baz/"])


def test_v1_delimiter_any_string(client):
    _put(client, ["bar", "baz", "cab", "foo", "b ar", "日x本"])
    assert _v1(client, delimiter="a")[1:] == (["foo", "日x本"], ["b a", "ba", "ca"])
    assert _v1(client, delimiter="x本")[1:] == (["b ar", "bar", "baz", "cab", "foo"], ["日x本"])
    assert _v1(client, delimiter="\n")[1:] == (["b ar", "bar", "baz", "cab", "foo", "日x本"], [])


def test_v1_delimiter_with_wildcard_characters(client):
    _put(client, ["a_b_c", "a%b%c", "d"])
    assert _v1(client, delimiter="_")[1:] == (["a%b%c", "d"], ["a_"])
    assert _v1(client, delimiter="%")[1:] == (["a_b_c", "d"], ["a%"])


def test_v1_empty_or_absent_delimiter(client):
    _put(client, ["a/1", "b"])
    for params in ({}, {"delimiter": ""}):
        root, keys, prefixes = _v1(client, **params)
        assert root.find("Delimiter") is None
        assert (keys, prefixes) == (["a/1", "b"], [])


def test_v1_max_keys_default_and_echo(client):
    _put(client, ["a"])
    assert _v1(client)[0].findtext("MaxKeys") == "1000"
    assert _v1(client, **{"max-keys": "7"})[0].findtext("MaxKeys") == "7"


def _seed_rows(db_path, keys):
    """Insert object rows (and their registry rows) in one transaction. Listing reads
    only rows, and 1001 durable PUTs would take most of a minute."""
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            for key in keys:
                blob_id = uuid.uuid4().hex
                conn.execute(
                    "INSERT INTO blobs (blob_id, state, kind, bucket, key, created_at)"
                    " VALUES (?, 'live', 'object', ?, ?, '2024-01-01T00:00:00.000000')",
                    (blob_id, BUCKET, key),
                )
                conn.execute(
                    "INSERT INTO objects (bucket, key, blob_id, size, etag, last_modified)"
                    " VALUES (?, ?, ?, 1, '\"x\"', '2024-01-01T00:00:00.000000')",
                    (BUCKET, key, blob_id),
                )
    finally:
        conn.close()


def test_v1_max_keys_capped_at_1000(client, tmp_path):
    _seed_rows(tmp_path / "meta.db", [f"k{i:04}" for i in range(1001)])
    for value in ["1001", "5000", "99999999999999999999"]:
        root, keys, _ = _v1(client, **{"max-keys": value})
        assert root.findtext("MaxKeys") == "1000"
        assert keys == [f"k{i:04}" for i in range(1000)]
        assert root.findtext("IsTruncated") == "true"
    root, keys, _ = _v1(client, marker="k0999", **{"max-keys": "5000"})
    assert keys == ["k1000"]
    assert root.findtext("IsTruncated") == "false"


def test_v1_max_keys_zero(client):
    _put(client, ["a", "b"])
    root, keys, prefixes = _v1(client, **{"max-keys": "0"})
    assert (keys, prefixes) == ([], [])
    assert root.findtext("MaxKeys") == "0"
    assert root.findtext("IsTruncated") == "false"


@pytest.mark.parametrize("value", ["blah", "-1", "1.5", "", " 1", "+1", "0x10", "١"])
def test_v1_max_keys_invalid(client, value):
    resp = client.get(f"/{BUCKET}", params={"max-keys": value})
    assert resp.status_code == 400
    root = ET.fromstring(resp.text)
    assert root.findtext("Code") == "InvalidArgument"
    assert root.findtext("ArgumentName") == "max-keys"


def test_v1_truncation_and_marker(client):
    _put(client, ["bar", "baz", "foo", "quxx"])
    root, keys, _ = _v1(client, **{"max-keys": "2"})
    assert keys == ["bar", "baz"]
    assert root.findtext("IsTruncated") == "true"
    root, keys, _ = _v1(client, marker="baz", **{"max-keys": "2"})
    assert keys == ["foo", "quxx"]
    assert root.findtext("IsTruncated") == "false"


def test_v1_marker_echo(client):
    _put(client, ["bar", "foo"])
    assert _v1(client)[0].findtext("Marker") == ""
    root, keys, _ = _v1(client, marker="blah")
    assert root.findtext("Marker") == "blah"
    assert keys == ["foo"]
    root, keys, _ = _v1(client, marker="zzz")
    assert keys == []
    assert root.findtext("IsTruncated") == "false"


def test_v1_next_marker_only_with_delimiter_and_truncation(client):
    _put(client, ["a", "b/1", "c"])
    # Truncated, no delimiter: no NextMarker; the client continues from the last Key.
    root = _v1(client, **{"max-keys": "1"})[0]
    assert root.findtext("IsTruncated") == "true"
    assert root.find("NextMarker") is None
    # Delimiter, not truncated: no NextMarker.
    root = _v1(client, delimiter="/")[0]
    assert root.findtext("IsTruncated") == "false"
    assert root.find("NextMarker") is None
    # Delimiter and truncated: NextMarker is the last key or common prefix returned.
    root = _v1(client, delimiter="/", **{"max-keys": "1"})[0]
    assert root.findtext("NextMarker") == "a"
    root = _v1(client, delimiter="/", marker="a", **{"max-keys": "1"})[0]
    assert _common_prefixes(root) == ["b/"]
    assert root.findtext("NextMarker") == "b/"


@pytest.mark.parametrize("marker", ["a/", "a/1", "a/15", "a/2/x", "a/\U0010ffff"])
def test_v1_marker_at_or_inside_common_prefix_skips_it(client, marker):
    # S3: CommonPrefixes is filtered out if it is not lexicographically greater than
    # the marker. A client that sends marker=a/ must not get a/ back.
    _put(client, ["a/", "a/1", "a/2/x", "a0", "b/1"])
    root, keys, prefixes = _v1(client, delimiter="/", marker=marker)
    assert (keys, prefixes) == (["a0"], ["b/"])


def test_v1_marker_before_common_prefix_returns_it(client):
    _put(client, ["a/1", "a/2", "b"])
    assert _v1(client, delimiter="/", marker="a")[1:] == (["b"], ["a/"])
    assert _v1(client, delimiter="/", marker="")[1:] == (["b"], ["a/"])


def test_v1_pages_through_common_prefixes(client):
    _put(client, ["asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"])
    pages, marker = [], ""
    while True:
        root, keys, prefixes = _v1(client, delimiter="/", marker=marker, **{"max-keys": "1"})
        pages.append((keys, prefixes))
        if root.findtext("IsTruncated") == "false":
            assert root.find("NextMarker") is None
            break
        marker = root.findtext("NextMarker")
    assert pages == [(["asdf"], []), ([], ["boo/"]), ([], ["cquux/"])]


def test_v1_contents_owner(client, monkeypatch):
    monkeypatch.setattr(settings, "owner_id", "owner-id")
    monkeypatch.setattr(settings, "owner_display_name", "Owner Name")
    _put(client, ["a", "b"])
    root = _v1(client)[0]
    owners = [
        (c.findtext("Owner/ID"), c.findtext("Owner/DisplayName")) for c in root.iter("Contents")
    ]
    assert owners == [("owner-id", "Owner Name")] * 2


def test_v1_owner_ignores_request_identity(client, monkeypatch):
    monkeypatch.setattr(settings, "owner_id", "owner-id")
    _put(client, ["a"])
    resp = client.get(
        f"/{BUCKET}",
        headers={"Authorization": "AWS someoneelse:sig", "x-amz-expected-bucket-owner": "x"},
    )
    assert ET.fromstring(resp.text).findtext("Contents/Owner/ID") == "owner-id"


def test_v1_missing_bucket(client):
    resp = client.get("/no-such-bucket", params={"max-keys": "blah"})
    assert resp.status_code == 404


# --- Paging: every page concatenated equals the unpaged listing ---

# Starlette's TestClient percent-decodes the request path twice, so "%" followed by
# two hex digits would reach the app already decoded (a real server decodes once).
# The letters are therefore not hex digits; "%" still appears next to every other
# character.
_ALPHABET = ["x", "g", "X", "G", "/", "_", "%", "-", "é", "日", "\U0001f600", "\U0010ffff"]
_DELIMITERS = ["", "/", "_", "%", "é", "xG", "\U0010ffff"]


def _random_keys(seed, count=80):
    rng = random.Random(seed)
    keys = set()
    while len(keys) < count:
        keys.add("".join(rng.choices(_ALPHABET, k=rng.randint(1, 6))))
    return sorted(keys)


def _expected_listing(keys, prefix, delimiter):
    """Reference model, independent of the server: keys in UTF-8 byte order, rolled
    up at the first delimiter after the prefix."""
    entries = []
    for key in sorted(keys, key=lambda k: k.encode("utf-8")):
        if not key.startswith(prefix):
            continue
        i = key.find(delimiter, len(prefix)) if delimiter else -1
        entry = ("prefix", key[: i + len(delimiter)]) if i >= 0 else ("key", key)
        if entry not in entries:
            entries.append(entry)
    return entries


def _v1_entries(root):
    keys = [("key", k) for k in _contents(root)]
    prefixes = [("prefix", p) for p in _common_prefixes(root)]
    return sorted(keys + prefixes, key=lambda e: e[1].encode("utf-8"))


def _v1_all_pages(client, prefix, delimiter, max_keys):
    entries, marker = [], ""
    for _ in range(1000):
        params = {"prefix": prefix, "marker": marker, "max-keys": str(max_keys)}
        if delimiter:
            params["delimiter"] = delimiter
        root = _v1(client, **params)[0]
        page = _v1_entries(root)
        assert len(page) <= max_keys
        entries += page
        if root.findtext("IsTruncated") == "false":
            assert root.find("NextMarker") is None
            return entries
        assert page, "a truncated page must not be empty"
        if delimiter:
            assert root.findtext("NextMarker") == page[-1][1]
            marker = root.findtext("NextMarker")
        else:
            assert root.find("NextMarker") is None
            marker = _contents(root)[-1]
    raise AssertionError("paging did not end")


@pytest.mark.parametrize("seed", [1, 2])
def test_v1_pages_concatenate_to_unpaged_listing(client, seed):
    keys = _random_keys(seed)
    _put(client, keys)
    rng = random.Random(seed)
    prefixes = ["", "x", "X", "%", "\U0010ffff"] + [k[:2] for k in rng.sample(keys, 2)]
    for prefix in prefixes:
        for delimiter in _DELIMITERS:
            expected = _expected_listing(keys, prefix, delimiter)
            params = {"prefix": prefix}
            if delimiter:
                params["delimiter"] = delimiter
            assert _v1_entries(_v1(client, **params)[0]) == expected, (prefix, delimiter)
            for max_keys in (1, 2, 3, 7, rng.randint(4, 40)):
                got = _v1_all_pages(client, prefix, delimiter, max_keys)
                assert got == expected, (prefix, delimiter, max_keys)


# --- ListObjectsV2: continuation-token, start-after, KeyCount, fetch-owner ---


def _v2(client, expect=200, **params):
    resp = client.get(f"/{BUCKET}", params={"list-type": "2", **params})
    assert resp.status_code == expect, resp.text
    root = ET.fromstring(resp.text)
    return root, _contents(root), _common_prefixes(root)


def test_v2_delimiter_and_key_count(client):
    _put(client, ["foo/bar", "foo/bar/xyzzy", "quux/thud", "asdf"])
    root, keys, prefixes = _v2(client, delimiter="/")
    assert (keys, prefixes) == (["asdf"], ["foo/", "quux/"])
    assert root.findtext("Delimiter") == "/"
    # KeyCount counts common prefixes as well as keys.
    assert root.findtext("KeyCount") == "3"


def test_v2_key_count_of_only_common_prefixes(client):
    _put(client, ["p/a/1", "p/b/2"])
    root, keys, prefixes = _v2(client, prefix="p/", delimiter="/")
    assert (keys, prefixes) == ([], ["p/a/", "p/b/"])
    assert root.findtext("KeyCount") == "2"


def test_v2_max_keys(client):
    _put(client, ["bar", "baz", "foo", "quxx"])
    root, keys, _ = _v2(client, **{"max-keys": "1"})
    assert keys == ["bar"]
    assert (root.findtext("IsTruncated"), root.findtext("KeyCount")) == ("true", "1")
    assert root.findtext("NextContinuationToken")
    root, keys, _ = _v2(client, **{"max-keys": "0"})
    assert keys == []
    assert (root.findtext("IsTruncated"), root.findtext("KeyCount")) == ("false", "0")
    assert root.find("NextContinuationToken") is None
    assert _v2(client)[0].findtext("MaxKeys") == "1000"
    assert _v2(client, **{"max-keys": "2000"})[0].findtext("MaxKeys") == "1000"
    resp = client.get(f"/{BUCKET}", params={"list-type": "2", "max-keys": "x"})
    assert resp.status_code == 400
    assert ET.fromstring(resp.text).findtext("Code") == "InvalidArgument"


def test_v2_continuation_token_pages(client):
    _put(client, ["bar", "baz", "foo", "quxx"])
    root, keys, _ = _v2(client, **{"max-keys": "2"})
    assert keys == ["bar", "baz"]
    token = root.findtext("NextContinuationToken")
    root, keys, _ = _v2(client, **{"continuation-token": token})
    assert keys == ["foo", "quxx"]
    assert root.findtext("ContinuationToken") == token
    assert root.findtext("IsTruncated") == "false"
    assert root.find("NextContinuationToken") is None


def test_v2_token_carries_restart_after_common_prefix(client):
    # The page ends on a common prefix; the token must restart past every key under
    # it, even when the next request names no delimiter at all.
    _put(client, ["a", "b/1", "b/2", "b/3", "c"])
    root, keys, prefixes = _v2(client, delimiter="/", **{"max-keys": "2"})
    assert (keys, prefixes) == (["a"], ["b/"])
    token = root.findtext("NextContinuationToken")
    assert _v2(client, delimiter="/", **{"continuation-token": token})[1:] == (["c"], [])
    assert _v2(client, **{"continuation-token": token})[1:] == (["c"], [])


def test_v2_token_round_trips_non_ascii_keys(client):
    keys = ["é/1", "é/2", "日本", "\U0001f600", "\U0010ffff", "\U0010ffff/x"]
    _put(client, keys)
    got, token = [], None
    while True:
        params = {"max-keys": "1"}
        if token:
            params["continuation-token"] = token
        root, page, _ = _v2(client, **params)
        got += page
        token = root.findtext("NextContinuationToken")
        if root.findtext("IsTruncated") == "false":
            break
    assert got == sorted(keys, key=lambda k: k.encode("utf-8"))


def test_v2_empty_continuation_token(client):
    _put(client, ["bar", "baz"])
    root, keys, _ = _v2(client, **{"continuation-token": ""})
    assert root.findtext("ContinuationToken") == ""
    assert keys == ["bar", "baz"]


@pytest.mark.parametrize("token", ["x", "!!", "bm90IGpzb24=", "eyJ2IjoxfQ=="])
def test_v2_malformed_continuation_token(client, token):
    resp = client.get(f"/{BUCKET}", params={"list-type": "2", "continuation-token": token})
    assert resp.status_code == 400
    root = ET.fromstring(resp.text)
    assert root.findtext("Code") == "InvalidArgument"
    assert root.findtext("ArgumentName") == "continuation-token"


def test_v2_lone_surrogate_in_token_is_invalid_argument(client):
    raw = b'{"v":1,"after":"\\ud800"}'
    token = base64.urlsafe_b64encode(raw).decode()
    resp = client.get(f"/{BUCKET}", params={"list-type": "2", "continuation-token": token})
    assert resp.status_code == 400


def test_v2_start_after(client):
    _put(client, ["bar", "baz", "foo", "quxx"])
    root, keys, _ = _v2(client, **{"start-after": "blah"})
    assert root.findtext("StartAfter") == "blah"
    assert keys == ["foo", "quxx"]
    assert _v2(client, **{"start-after": "zzz"})[1] == []
    assert _v2(client, **{"start-after": "\n"})[1] == ["bar", "baz", "foo", "quxx"]
    assert _v2(client)[0].find("StartAfter") is None


@pytest.mark.parametrize("start_after", ["a/", "a/1", "a/2/x"])
def test_v2_start_after_at_or_inside_common_prefix_skips_it(client, start_after):
    # S3: CommonPrefixes is filtered out if it is not lexicographically greater than
    # the StartAfter value.
    _put(client, ["a/1", "a/2/x", "a0", "b/1"])
    root, keys, prefixes = _v2(client, delimiter="/", **{"start-after": start_after})
    assert (keys, prefixes) == (["a0"], ["b/"])


def test_v2_continuation_token_takes_precedence_over_start_after(client):
    _put(client, ["bar", "baz", "foo", "quxx"])
    root = _v2(client, **{"start-after": "bar", "max-keys": "1"})[0]
    token = root.findtext("NextContinuationToken")  # restarts after "baz"
    # Same start-after as the first request (ceph's case).
    root, keys, _ = _v2(client, **{"start-after": "bar", "continuation-token": token})
    assert keys == ["foo", "quxx"]
    assert root.findtext("StartAfter") == "bar"
    assert root.findtext("ContinuationToken") == token
    # A start-after beyond the token's position does not move it, and one before it
    # does not move it back: the token decides where the page starts.
    assert _v2(client, **{"start-after": "foo", "continuation-token": token})[1] == ["foo", "quxx"]
    assert _v2(client, **{"start-after": "a", "continuation-token": token})[1] == ["foo", "quxx"]


def test_v2_fetch_owner(client, monkeypatch):
    monkeypatch.setattr(settings, "owner_id", "owner-id")
    monkeypatch.setattr(settings, "owner_display_name", "Owner Name")
    _put(client, ["a"])
    for params in ({}, {"fetch-owner": "false"}, {"fetch-owner": ""}):
        assert _v2(client, **params)[0].find("Contents/Owner") is None
    for value in ("true", "True", "TRUE"):
        root = _v2(client, **{"fetch-owner": value})[0]
        assert root.findtext("Contents/Owner/ID") == "owner-id"
        assert root.findtext("Contents/Owner/DisplayName") == "Owner Name"


def _v2_entries(root):
    return _v1_entries(root)


def _v2_all_pages(client, prefix, delimiter, max_keys):
    """Page with continuation tokens only: no start-after and no marker."""
    entries, token = [], None
    for _ in range(1000):
        params = {"prefix": prefix, "max-keys": str(max_keys)}
        if delimiter:
            params["delimiter"] = delimiter
        if token is not None:
            params["continuation-token"] = token
        root = _v2(client, **params)[0]
        page = _v2_entries(root)
        assert int(root.findtext("KeyCount")) == len(page) <= max_keys
        entries += page
        if root.findtext("IsTruncated") == "false":
            assert root.find("NextContinuationToken") is None
            return entries
        assert page, "a truncated page must not be empty"
        token = root.findtext("NextContinuationToken")
    raise AssertionError("paging did not end")


@pytest.mark.parametrize("seed", [1, 2])
def test_v2_pages_concatenate_to_unpaged_listing(client, seed):
    keys = _random_keys(seed)
    _put(client, keys)
    rng = random.Random(seed)
    prefixes = ["", "x", "X", "%", "\U0010ffff"] + [k[:2] for k in rng.sample(keys, 2)]
    for prefix in prefixes:
        for delimiter in _DELIMITERS:
            expected = _expected_listing(keys, prefix, delimiter)
            params = {"prefix": prefix}
            if delimiter:
                params["delimiter"] = delimiter
            assert _v2_entries(_v2(client, **params)[0]) == expected, (prefix, delimiter)
            for max_keys in (1, 2, 3, 7, rng.randint(4, 40)):
                got = _v2_all_pages(client, prefix, delimiter, max_keys)
                assert got == expected, (prefix, delimiter, max_keys)


# --- ListObjectVersions: key-marker, version-id-marker, NextKeyMarker, Owner ---


def _versions(client, expect=200, **params):
    resp = client.get(f"/{BUCKET}", params={"versions": "", **params})
    assert resp.status_code == expect, resp.text
    root = ET.fromstring(resp.text)
    keys = [el.findtext("Key") for el in root.findall("Version")]
    return root, keys, _common_prefixes(root)


def test_versions_elements(client, monkeypatch):
    monkeypatch.setattr(settings, "owner_id", "owner-id")
    monkeypatch.setattr(settings, "owner_display_name", "Owner Name")
    _put(client, ["a", "b"])
    root, keys, _ = _versions(client)
    assert keys == ["a", "b"]
    assert root.findtext("KeyMarker") == ""
    assert root.findtext("VersionIdMarker") == ""
    assert root.findtext("MaxKeys") == "1000"
    assert root.findtext("IsTruncated") == "false"
    assert root.find("NextKeyMarker") is None and root.find("NextVersionIdMarker") is None
    for version in root.findall("Version"):
        assert version.findtext("VersionId") == "null"
        assert version.findtext("IsLatest") == "true"
        assert version.findtext("Owner/ID") == "owner-id"
        assert version.findtext("Owner/DisplayName") == "Owner Name"


def test_versions_truncation_and_next_markers(client):
    _put(client, ["bar", "baz", "foo", "quxx"])
    root, keys, _ = _versions(client, **{"max-keys": "2"})
    assert keys == ["bar", "baz"]
    assert root.findtext("IsTruncated") == "true"
    assert root.findtext("NextKeyMarker") == "baz"
    assert root.findtext("NextVersionIdMarker") == "null"
    root, keys, _ = _versions(
        client, **{"key-marker": "baz", "version-id-marker": "null", "max-keys": "2"}
    )
    assert keys == ["foo", "quxx"]
    assert root.findtext("KeyMarker") == "baz"
    assert root.findtext("VersionIdMarker") == "null"
    assert root.findtext("IsTruncated") == "false"


def test_versions_key_marker_alone_starts_after_it(client):
    _put(client, ["bar", "baz", "foo"])
    assert _versions(client, **{"key-marker": "baz"})[1] == ["foo"]
    assert _versions(client, **{"key-marker": "bay"})[1] == ["baz", "foo"]
    assert _versions(client, **{"key-marker": "zzz"})[1] == []


def test_versions_version_id_marker_without_key_marker(client):
    _put(client, ["a"])
    resp = client.get(f"/{BUCKET}", params={"versions": "", "version-id-marker": "null"})
    assert resp.status_code == 400
    root = ET.fromstring(resp.text)
    assert root.findtext("Code") == "InvalidArgument"
    assert root.findtext("ArgumentName") == "version-id-marker"


def test_versions_other_version_id_marker_not_implemented(client):
    _put(client, ["a", "b"])
    resp = client.get(
        f"/{BUCKET}",
        params={"versions": "", "key-marker": "a", "version-id-marker": "3HL4kqtJlcpXrof3"},
    )
    assert resp.status_code == 501
    assert ET.fromstring(resp.text).findtext("Code") == "NotImplemented"


def test_versions_empty_version_id_marker_is_absent(client):
    _put(client, ["a", "b"])
    root, keys, _ = _versions(client, **{"key-marker": "a", "version-id-marker": ""})
    assert keys == ["b"]
    assert root.findtext("VersionIdMarker") == ""


def test_versions_delimiter_and_common_prefix_page_end(client):
    _put(client, ["a", "b/1", "b/2", "c"])
    root, keys, prefixes = _versions(client, delimiter="/", **{"max-keys": "2"})
    assert (keys, prefixes) == (["a"], ["b/"])
    assert root.findtext("Delimiter") == "/"
    assert root.findtext("NextKeyMarker") == "b/"
    # The page ended on a common prefix, not on a version.
    assert root.find("NextVersionIdMarker") is None
    root, keys, prefixes = _versions(client, delimiter="/", **{"key-marker": "b/"})
    assert (keys, prefixes) == (["c"], [])


@pytest.mark.parametrize("key_marker", ["b/", "b/1", "b/15"])
def test_versions_key_marker_at_or_inside_common_prefix_skips_it(client, key_marker):
    _put(client, ["a", "b/1", "b/2", "c"])
    assert _versions(client, delimiter="/", **{"key-marker": key_marker})[1:] == (["c"], [])


def test_versions_max_keys(client):
    _put(client, ["a", "b"])
    root, keys, _ = _versions(client, **{"max-keys": "0"})
    assert keys == [] and root.findtext("IsTruncated") == "false"
    assert _versions(client, **{"max-keys": "5000"})[0].findtext("MaxKeys") == "1000"
    resp = client.get(f"/{BUCKET}", params={"versions": "", "max-keys": "-1"})
    assert resp.status_code == 400
    assert ET.fromstring(resp.text).findtext("Code") == "InvalidArgument"


def _versions_entries(root):
    keys = [("key", el.findtext("Key")) for el in root.findall("Version")]
    prefixes = [("prefix", p) for p in _common_prefixes(root)]
    return sorted(keys + prefixes, key=lambda e: e[1].encode("utf-8"))


def _versions_all_pages(client, prefix, delimiter, max_keys):
    """Page as botocore's paginator does: pass NextKeyMarker and NextVersionIdMarker
    back as key-marker and version-id-marker, omitting whichever is absent."""
    entries, markers = [], {}
    for _ in range(1000):
        params = {"prefix": prefix, "max-keys": str(max_keys), **markers}
        if delimiter:
            params["delimiter"] = delimiter
        root = _versions(client, **params)[0]
        page = _versions_entries(root)
        assert len(page) <= max_keys
        entries += page
        if root.findtext("IsTruncated") == "false":
            assert root.find("NextKeyMarker") is None
            return entries
        assert page, "a truncated page must not be empty"
        assert root.findtext("NextKeyMarker") == page[-1][1]
        markers = {"key-marker": root.findtext("NextKeyMarker")}
        if page[-1][0] == "key":
            assert root.findtext("NextVersionIdMarker") == "null"
            markers["version-id-marker"] = "null"
        else:
            assert root.find("NextVersionIdMarker") is None
    raise AssertionError("paging did not end")


@pytest.mark.parametrize("seed", [1, 2])
def test_versions_pages_concatenate_to_unpaged_listing(client, seed):
    keys = _random_keys(seed)
    _put(client, keys)
    rng = random.Random(seed)
    prefixes = ["", "x", "X", "%", "\U0010ffff"] + [k[:2] for k in rng.sample(keys, 2)]
    for prefix in prefixes:
        for delimiter in _DELIMITERS:
            expected = _expected_listing(keys, prefix, delimiter)
            params = {"prefix": prefix}
            if delimiter:
                params["delimiter"] = delimiter
            assert _versions_entries(_versions(client, **params)[0]) == expected
            for max_keys in (1, 2, 3, 7, rng.randint(4, 40)):
                got = _versions_all_pages(client, prefix, delimiter, max_keys)
                assert got == expected, (prefix, delimiter, max_keys)


# --- encoding-type=url ---
#
# botocore sends encoding-type=url on every listing and decodes the encoded elements
# with unquote_plus whenever EncodingType is echoed, so the encoding must be exactly
# what unquote_plus inverts: %20 for a space and %2B for "+", never a bare "+". The
# encoded elements per operation follow the ListObjectsV2 and ListObjectVersions
# references and botocore's decoders (docs/compatibility.md).

# Each key with its url encoding: the unreserved characters and "/" stay, every other
# byte of the UTF-8 form is %XX (AWS's example: test_file(3).png).
ENCODED_KEYS = [
    ("a b", "a%20b"),
    ("a+b", "a%2Bb"),
    ("100%", "100%25"),
    ("a/b", "a/b"),
    ("é/日", "%C3%A9/%E6%97%A5"),
    ("test_file(3).png", "test_file%283%29.png"),
    ("-_.~", "-_.~"),
    ("q?x#y&z=w", "q%3Fx%23y%26z%3Dw"),
    ("<>\"'\\", "%3C%3E%22%27%5C"),
    ("\x01\x1f\x7f", "%01%1F%7F"),
    ("￾￿", "%EF%BF%BE%EF%BF%BF"),
    ("\t\r", "%09%0D"),
]


def test_encoding_type_url_encodes_keys(client, listing):
    _put(client, [k for k, _ in ENCODED_KEYS])
    root, keys = _list(client, listing, **{"encoding-type": "url"})
    assert root.findtext("EncodingType") == "url"
    expected = {k: e for k, e in ENCODED_KEYS}
    assert sorted(keys) == sorted(expected.values())
    # Every key is its own encoding's only preimage, so the client can decode.
    assert {quote(k, safe="/") for k in expected} == set(keys)


def test_encoding_type_absent_leaves_keys_raw(client, listing):
    raw = [k for k, _ in ENCODED_KEYS if k.isprintable() or k == "a b"]
    _put(client, raw)
    root, keys = _list(client, listing)
    assert root.find("EncodingType") is None
    assert sorted(keys) == sorted(raw)


def test_encoding_type_url_makes_xml_invalid_keys_parseable(client, listing):
    # Without encoding-type the response is not well-formed XML (docs/compatibility.md,
    # "Characters XML 1.0 does not allow"); with it, the key is an ASCII %XX string.
    _put(client, ["\x01", "￾"])
    query, element = LISTINGS[listing]
    resp = client.get(f"/{BUCKET}", params=query)
    assert resp.status_code == 200
    with pytest.raises(ET.ParseError):
        ET.fromstring(resp.text)
    _, keys = _list(client, listing, **{"encoding-type": "url"})
    assert keys == ["%01", "%EF%BF%BE"]


def test_encoding_type_empty_is_absent(client, listing):
    _put(client, ["a b"])
    root, keys = _list(client, listing, **{"encoding-type": ""})
    assert root.find("EncodingType") is None
    assert keys == ["a b"]


@pytest.mark.parametrize("value", ["URL", "Url", "xml", "url ", " url", "base64", "none"])
def test_encoding_type_other_value_is_invalid_argument(client, listing, value):
    _put(client, ["a"])
    query, _ = LISTINGS[listing]
    resp = client.get(f"/{BUCKET}", params={**query, "encoding-type": value})
    assert resp.status_code == 400, resp.text
    root = ET.fromstring(resp.text)
    assert root.findtext("Code") == "InvalidArgument"
    assert root.findtext("ArgumentName") == "encoding-type"
    assert root.findtext("ArgumentValue") == value


# Keys for the per-element tests. With prefix "a b" and delimiter "+":
#   "a b+f"   rolls up into the common prefix "a b+"
#   "a b/c+d" rolls up into the common prefix "a b/c+"
#   "a b/e", "a b/g" are keys
# in that byte order ("+" 0x2B sorts before "/" 0x2F).
_FIELD_KEYS = ["a b/c+d", "a b/e", "a b/g", "a b+f", "zz"]
_FIELD_PARAMS = {"prefix": "a b", "delimiter": "+", "encoding-type": "url"}


def test_v1_encoded_elements(client):
    _put(client, _FIELD_KEYS)
    root, keys, prefixes = _v1(client, marker="a a", **{"max-keys": "2"}, **_FIELD_PARAMS)
    assert root.findtext("EncodingType") == "url"
    # Prefix is the one element botocore does not decode for ListObjects, so it is
    # echoed as sent.
    assert root.findtext("Prefix") == "a b"
    assert root.findtext("Marker") == "a%20a"
    assert root.findtext("Delimiter") == "%2B"
    assert root.findtext("IsTruncated") == "true"
    assert root.findtext("NextMarker") == "a%20b/c%2B"
    assert keys == []
    assert prefixes == ["a%20b%2B", "a%20b/c%2B"]
    # The client decodes NextMarker and continues from the raw key.
    root, keys, prefixes = _v1(client, marker="a b/c+", **_FIELD_PARAMS)
    assert keys == ["a%20b/e", "a%20b/g"] and prefixes == []
    assert root.findtext("IsTruncated") == "false" and root.find("NextMarker") is None


def test_v2_encoded_elements(client):
    _put(client, _FIELD_KEYS)
    root, keys, prefixes = _v2(client, **{"start-after": "a a", "max-keys": "2"}, **_FIELD_PARAMS)
    assert root.findtext("EncodingType") == "url"
    assert root.findtext("Prefix") == "a%20b"
    assert root.findtext("StartAfter") == "a%20a"
    assert root.findtext("Delimiter") == "%2B"
    assert root.findtext("KeyCount") == "2"
    assert keys == [] and prefixes == ["a%20b%2B", "a%20b/c%2B"]
    token = root.findtext("NextContinuationToken")
    # Tokens are opaque and never encoded: the token is the exact one the server
    # produces (base64 padding "=" and all), and the echo is byte for byte the token
    # sent.
    assert token == encode_token(None, "a b/c,")
    root, keys, prefixes = _v2(client, **{"continuation-token": token}, **_FIELD_PARAMS)
    assert root.findtext("ContinuationToken") == token
    assert keys == ["a%20b/e", "a%20b/g"] and prefixes == []
    # A token with base64 padding is echoed with its "=", not %3D.
    padded = encode_token("a b/e", None)
    assert padded.endswith("=")
    root, keys, prefixes = _v2(client, **{"continuation-token": padded}, **_FIELD_PARAMS)
    assert root.findtext("ContinuationToken") == padded
    assert keys == ["a%20b/g"]


def test_versions_encoded_elements(client):
    _put(client, _FIELD_KEYS)
    root, keys, prefixes = _versions(
        client,
        **{"key-marker": "a a", "version-id-marker": "null", "max-keys": "2"},
        **_FIELD_PARAMS,
    )
    assert root.findtext("EncodingType") == "url"
    assert root.findtext("Prefix") == "a%20b"
    assert root.findtext("KeyMarker") == "a%20a"
    assert root.findtext("VersionIdMarker") == "null"
    assert root.findtext("Delimiter") == "%2B"
    assert root.findtext("NextKeyMarker") == "a%20b/c%2B"
    assert root.find("NextVersionIdMarker") is None
    assert keys == [] and prefixes == ["a%20b%2B", "a%20b/c%2B"]
    # A page ending on a version: NextKeyMarker is the encoded key, the version ID is
    # not encoded.
    root, keys, prefixes = _versions(client, **{"max-keys": "3"}, **_FIELD_PARAMS)
    assert keys == ["a%20b/e"] and prefixes == ["a%20b%2B", "a%20b/c%2B"]
    assert root.findtext("NextKeyMarker") == "a%20b/e"
    assert root.findtext("NextVersionIdMarker") == "null"
    for version in root.findall("Version"):
        assert version.findtext("VersionId") == "null"


def test_encoding_does_not_change_paging(client, listing):
    # Encoding is applied to the response only: the same keys, pages and markers as
    # without it, byte for byte once decoded.
    keys = _random_keys(3) + ["a b+c", "a+b c/d", "é é/+"]
    _put(client, keys)
    query, element = LISTINGS[listing]
    for delimiter in ("", "/", "+", " "):
        params = {"prefix": "", "max-keys": "5"}
        if delimiter:
            params["delimiter"] = delimiter
        raw = client.get(f"/{BUCKET}", params={**query, **params}).text
        encoded = client.get(f"/{BUCKET}", params={**query, **params, "encoding-type": "url"}).text
        raw_root, enc_root = ET.fromstring(raw), ET.fromstring(encoded)
        enc_root.remove(enc_root.find("EncodingType"))
        raw_els, enc_els = list(raw_root.iter()), list(enc_root.iter())
        assert [e.tag for e in raw_els] == [e.tag for e in enc_els]
        for raw_el, enc_el in zip(raw_els, enc_els, strict=True):
            assert unquote_plus(enc_el.text or "") == (raw_el.text or ""), enc_el.tag
