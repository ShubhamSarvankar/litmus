"""
Listing conformance tests: ListObjects (V1), ListObjectsV2 and ListObjectVersions.

prefix is an exact, case sensitive prefix of the key: _ and % are ordinary
characters, and keys are listed in UTF-8 byte order.
"""

import base64
import random
import sqlite3
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient

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
