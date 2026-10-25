"""User-defined metadata (x-amz-meta-*) given to PutObject is stored with the object and
returned, with the prefix, by GET and HEAD on 200 and 206. An overwrite replaces the
whole set. A request whose metadata exceeds 2048 bytes is refused with 400
MetadataTooLarge before its body is read and before any blob is registered.

The counting, the RFC 2047 handling and the other choices are listed under "User
metadata" in docs/compatibility.md.
"""

import base64
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from chaos.framework.framed import framed_request
from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "meta-bucket"
URL = f"/{BUCKET}/k"
BODY = b"0123456789"


class Env:
    def __init__(self, tmp_path: Path):
        self.storage = BlobStore(tmp_path / "blobs")
        self.metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
        self.client = TestClient(
            create_app(storage=self.storage, metadata=self.metadata),
            raise_server_exceptions=False,
        )
        assert self.client.put(f"/{BUCKET}").status_code == 200

    def stored(self, key: str = "k") -> dict:
        return self.metadata.get_object_meta(BUCKET, key).user_metadata

    def assert_untouched(self, blob_records: list) -> None:
        """Nothing was registered or written beyond blob_records, and the object is as it was."""
        assert self.metadata.list_blob_records() == blob_records
        assert self.storage.temp_entries() == []
        assert run_consistency_sweep(self.metadata, self.storage).issues == []
        assert self.client.get(URL).content == BODY


@pytest.fixture
def env(tmp_path: Path) -> Env:
    return Env(tmp_path)


@pytest.fixture
def client(env) -> TestClient:
    return env.client


def _meta(resp) -> dict:
    """The x-amz-meta-* headers of a response, name without the prefix."""
    return {k[11:]: v for k, v in resp.headers.items() if k.startswith("x-amz-meta-")}


def _word(text: str, charset: str = "UTF-8") -> str:
    return f"=?{charset}?B?{base64.b64encode(text.encode(charset)).decode()}?="


# --- storage and return ---


def test_metadata_is_returned_by_get_head_and_ranged_get(client):
    sent = {"x-amz-meta-author": "Janet", "x-amz-meta-n": "7"}
    assert client.put(URL, content=BODY, headers=sent).status_code == 200
    assert _meta(client.get(URL)) == {"author": "Janet", "n": "7"}
    assert _meta(client.head(URL)) == {"author": "Janet", "n": "7"}
    ranged = client.get(URL, headers={"range": "bytes=2-4"})
    assert (ranged.status_code, ranged.content) == (206, b"234")
    assert _meta(ranged) == {"author": "Janet", "n": "7"}


def test_names_are_stored_and_returned_in_lower_case(env):
    env.client.put(URL, content=BODY, headers={"X-Amz-Meta-MyKey": "v", "x-AMZ-meta-OTHER": "w"})
    assert env.stored() == {"mykey": "v", "other": "w"}
    assert _meta(env.client.head(URL)) == {"mykey": "v", "other": "w"}


@pytest.mark.parametrize("name", ["a.b", "a_b", "123", "a-b-c", "x-amz-meta-y", "~!#$%&'*+^`|"])
def test_names_with_unusual_token_characters_round_trip(client, name):
    client.put(URL, content=BODY, headers={f"x-amz-meta-{name}": "v"})
    assert _meta(client.head(URL)) == {name: "v"}


def test_an_object_put_without_metadata_returns_none(client):
    client.put(URL, content=BODY)
    assert _meta(client.get(URL)) == {}
    assert _meta(client.head(URL)) == {}


def test_a_header_named_only_by_the_prefix_is_ignored(env):
    env.client.put(URL, content=BODY, headers={"x-amz-meta-": "v", "x-amz-meta-a": "w"})
    assert env.stored() == {"a": "w"}


def test_x_amz_missing_meta_is_never_returned(client):
    client.put(URL, content=BODY, headers={"x-amz-meta-a": "w"})
    assert "x-amz-missing-meta" not in client.head(URL).headers
    assert "x-amz-missing-meta" not in client.get(URL).headers


def test_metadata_and_system_headers_are_stored_together(client):
    client.put(
        URL,
        content=BODY,
        headers={"x-amz-meta-a": "1", "Cache-Control": "no-cache", "Content-Type": "text/plain"},
    )
    head = client.head(URL)
    assert (_meta(head), head.headers["cache-control"], head.headers["content-type"]) == (
        {"a": "1"},
        "no-cache",
        "text/plain",
    )


def test_a_framed_put_stores_the_metadata(env):
    headers, body = framed_request(BODY)
    headers["x-amz-meta-a"] = "framed"
    assert env.client.put(URL, content=body, headers=headers).status_code == 200
    assert env.stored() == {"a": "framed"}
    assert env.client.get(URL).content == BODY


def test_metadata_is_per_key_and_does_not_reach_listings(client):
    client.put(URL, content=BODY, headers={"x-amz-meta-a": "1"})
    client.put(f"/{BUCKET}/other", content=BODY)
    assert _meta(client.head(f"/{BUCKET}/other")) == {}
    listing = client.get(f"/{BUCKET}?list-type=2")
    assert listing.status_code == 200
    keys = [e.text for e in ET.fromstring(listing.text).iter() if e.tag.rsplit("}", 1)[-1] == "Key"]
    assert keys == ["k", "other"]


# --- overwrite, delete ---


def test_overwrite_replaces_the_whole_set(env):
    env.client.put(URL, content=BODY, headers={"x-amz-meta-a": "1", "x-amz-meta-b": "2"})
    env.client.put(URL, content=b"new", headers={"x-amz-meta-b": "3", "x-amz-meta-c": "4"})
    assert env.stored() == {"b": "3", "c": "4"}
    assert _meta(env.client.get(URL)) == {"b": "3", "c": "4"}


def test_overwrite_without_metadata_clears_it(env):
    env.client.put(URL, content=BODY, headers={"x-amz-meta-a": "1"})
    env.client.put(URL, content=b"new")
    assert env.stored() == {}
    assert _meta(env.client.head(URL)) == {}


def test_delete_then_put_without_metadata_starts_clean(env):
    env.client.put(URL, content=BODY, headers={"x-amz-meta-a": "1"})
    assert env.client.delete(URL).status_code == 204
    env.client.put(URL, content=BODY)
    assert env.stored() == {}


# --- empty values and duplicates ---


def test_an_empty_value_is_kept_as_empty(env):
    env.client.put(URL, content=BODY, headers={"x-amz-meta-a": ""})
    assert env.stored() == {"a": ""}
    assert _meta(env.client.head(URL)) == {"a": ""}
    assert _meta(env.client.get(URL)) == {"a": ""}


def test_an_empty_value_replaces_an_old_one(client):
    client.put(URL, content=BODY, headers={"x-amz-meta-a": "old"})
    client.put(URL, content=BODY, headers={"x-amz-meta-a": ""})
    assert _meta(client.get(URL)) == {"a": ""}


def test_a_name_sent_twice_has_its_values_joined_with_a_comma(env):
    env.client.put(
        URL,
        content=BODY,
        headers=[("x-amz-meta-a", "one"), ("X-Amz-Meta-A", "two"), ("x-amz-meta-b", "x")],
    )
    assert env.stored() == {"a": "one, two", "b": "x"}


# --- size ---


def _size_headers(total: int, names: int = 1) -> dict:
    """x-amz-meta-* headers whose names (without the prefix) and values come to total bytes."""
    per = total // names
    headers = {f"x-amz-meta-{n:02d}": "v" * (per - 2) for n in range(names)}
    headers["x-amz-meta-00"] += "v" * (total - per * names)
    return headers


@pytest.mark.parametrize("names", [1, 2, 8])
def test_exactly_2048_bytes_is_accepted(env, names):
    headers = _size_headers(2048, names)
    assert env.client.put(URL, content=BODY, headers=headers).status_code == 200
    assert len(env.stored()) == names
    assert _meta(env.client.head(URL)) == env.stored()


@pytest.mark.parametrize("names", [1, 2, 8])
def test_2049_bytes_is_refused(env, names):
    headers = _size_headers(2049, names)
    resp = env.client.put(URL, content=BODY, headers=headers)
    assert resp.status_code == 400
    assert ET.fromstring(resp.text).findtext("Code") == "MetadataTooLarge"
    assert env.metadata.get_object_meta(BUCKET, "k") is None
    assert env.metadata.list_blob_records() == []
    assert env.storage.temp_entries() == []


def test_the_prefix_is_not_counted(client):
    # One byte name, 2047 byte value: 2048. With the 11 byte prefix counted it would not fit.
    assert client.put(URL, content=BODY, headers={"x-amz-meta-a": "v" * 2047}).status_code == 200
    assert client.put(URL, content=BODY, headers={"x-amz-meta-a": "v" * 2048}).status_code == 400


def test_a_name_counts_as_well_as_its_value(client):
    assert (
        client.put(URL, content=BODY, headers={"x-amz-meta-" + "n" * 10: "v" * 2038}).status_code
        == 200
    )
    assert (
        client.put(URL, content=BODY, headers={"x-amz-meta-" + "n" * 10: "v" * 2039}).status_code
        == 400
    )


def test_other_headers_are_not_counted(client):
    headers = {"x-amz-meta-a": "v" * 2047, "Cache-Control": "c" * 3000, "x-amz-foo": "f" * 500}
    assert client.put(URL, content=BODY, headers=headers).status_code == 200


def test_size_counts_utf8_bytes_of_the_stored_text(client):
    # 1023 two byte characters and a one byte name: 2047 bytes, then 2049.
    ok = {"x-amz-meta-a": ("é" * 1023).encode("utf-8")}
    assert client.put(URL, content=BODY, headers=ok).status_code == 200
    too_big = {"x-amz-meta-a": ("é" * 1024 + "v").encode("utf-8")}
    assert client.put(URL, content=BODY, headers=too_big).status_code == 400


def test_duplicates_are_counted_once_joined(client):
    # 1000 + ", " + 1000 + one byte name = 2003.
    headers = [("x-amz-meta-a", "v" * 1000), ("x-amz-meta-a", "w" * 1000)]
    assert client.put(URL, content=BODY, headers=headers).status_code == 200
    headers = [("x-amz-meta-a", "v" * 1024), ("x-amz-meta-a", "w" * 1024)]
    assert client.put(URL, content=BODY, headers=headers).status_code == 400


def test_a_refusal_leaves_the_old_object_intact(env):
    old = {"x-amz-meta-a": "old"}
    assert env.client.put(URL, content=BODY, headers=old).status_code == 200
    before = env.metadata.list_blob_records()
    etag = env.client.head(URL).headers["etag"]
    resp = env.client.put(URL, content=b"replacement", headers=_size_headers(2049))
    assert resp.status_code == 400
    env.assert_untouched(before)
    assert env.client.head(URL).headers["etag"] == etag
    assert env.stored() == {"a": "old"}


def test_a_refusal_registers_no_blob_and_reads_no_body(env, monkeypatch):
    registered = []
    real = env.metadata.register_blob
    monkeypatch.setattr(
        env.metadata, "register_blob", lambda *a, **kw: registered.append(a) or real(*a, **kw)
    )

    async def not_read(self):
        raise AssertionError("the body was read")

    monkeypatch.setattr("starlette.requests.Request.body", not_read)
    resp = env.client.put(URL, content=BODY, headers=_size_headers(2049))
    assert resp.status_code == 400
    assert registered == []


def test_a_framed_refusal_leaves_nothing(env):
    headers, body = framed_request(BODY)
    resp = env.client.put(URL, content=body, headers=headers | _size_headers(2049))
    assert resp.status_code == 400
    assert ET.fromstring(resp.text).findtext("Code") == "MetadataTooLarge"
    assert env.metadata.list_blob_records() == []
    assert env.storage.temp_entries() == []
    assert env.client.get(URL).status_code == 404


def test_a_missing_bucket_is_reported_before_the_size(client):
    resp = client.put("/nobucket/k", content=BODY, headers=_size_headers(2049))
    assert resp.status_code == 404
    assert ET.fromstring(resp.text).findtext("Code") == "NoSuchBucket"


def test_the_refusal_is_an_s3_error(client):
    resp = client.put(URL, content=BODY, headers=_size_headers(2049))
    root = ET.fromstring(resp.text)
    assert root.findtext("Code") == "MetadataTooLarge"
    assert (
        root.findtext("Message")
        == "Your metadata headers exceed the maximum allowed metadata size."
    )
    assert root.findtext("RequestId") == resp.headers["x-amz-request-id"]


# --- non-ASCII values (RFC 2047) ---


def test_an_encoded_word_is_decoded_on_store_and_encoded_on_return(env):
    sent = _word("Hello Worldé")
    env.client.put(URL, content=BODY, headers={"x-amz-meta-a": sent})
    assert env.stored() == {"a": "Hello Worldé"}
    assert _meta(env.client.head(URL)) == {"a": sent}
    assert _meta(env.client.get(URL)) == {"a": sent}


def test_the_documented_example_comes_back_as_it_went_in(env):
    # S3 User Guide, "User-defined metadata": the HEAD answer for the PUT's
    # x-amz-meta-nonascii. The word is returned as stored, one decoding and one encoding.
    word = "=?UTF-8?B?w4PChE3Dg8KEWsODwpXDg8KRIFMz?="
    env.client.put(URL, content=BODY, headers={"x-amz-meta-nonascii": word})
    assert _meta(env.client.head(URL)) == {"nonascii": word}


@pytest.mark.parametrize(
    ("sent", "text"),
    [
        ("=?utf-8?q?caf=C3=A9_au_lait?=", "café au lait"),
        ("=?UTF-8?Q?a=3Db?=", "a=b"),
        ("=?iso-8859-1?Q?caf=E9?=", "café"),
        ("=?ISO-8859-1?B?Y2Fm6Q==?=", "café"),
        ("=?us-ascii?q?plain?=", "plain"),
        ("=?US-ASCII?B?cGxhaW4=?=", "plain"),
        ("=?UTF-8?B?aGk=?= =?UTF-8?B?IHRoZXJl?=", "hi there"),
        ("x =?UTF-8?B?aGk=?= y", "x hi y"),
        (_word("中文"), "中文"),
        (_word("\U0001f600"), "\U0001f600"),
    ],
)
def test_accepted_charsets_and_encodings(env, sent, text):
    env.client.put(URL, content=BODY, headers={"x-amz-meta-a": sent})
    assert env.stored() == {"a": text}


@pytest.mark.parametrize(
    "sent",
    [
        "=?koi8-r?B?aGk=?=",  # a charset that is not accepted
        "=?UTF-8?X?aGk=?=",  # not B or Q
        "=?UTF-8?B?!!!?=",  # not base64
        "=?UTF-8?B?/w==?=",  # bytes that are not UTF-8
        "=?us-ascii?B?/w==?=",  # bytes that are not ASCII
        "=?UTF-8?B?aGk=",  # no closing
    ],
)
def test_an_encoded_word_that_cannot_be_decoded_is_kept_as_text(env, sent):
    env.client.put(URL, content=BODY, headers={"x-amz-meta-a": sent})
    assert env.stored() == {"a": sent}
    assert _meta(env.client.head(URL)) == {"a": sent}


def test_ascii_is_returned_as_it_is(client):
    client.put(URL, content=BODY, headers={"x-amz-meta-a": "AMAZONS3 ~!@ plain, text"})
    assert _meta(client.head(URL)) == {"a": "AMAZONS3 ~!@ plain, text"}


def test_raw_utf8_bytes_are_read_as_utf8(env):
    env.client.put(URL, content=BODY, headers={"x-amz-meta-a": "café".encode()})
    assert env.stored() == {"a": "café"}
    assert _meta(env.client.head(URL)) == {"a": _word("café")}


def test_raw_bytes_that_are_not_utf8_are_read_as_latin1(env):
    # What an HTTP library sends for the str "Hello World\xe9" (ceph s3-tests).
    env.client.put(URL, content=BODY, headers={"x-amz-meta-a": b"Hello World\xe9"})
    assert env.stored() == {"a": "Hello Worldé"}
    assert _meta(env.client.head(URL)) == {"a": _word("Hello Worldé")}


def test_a_control_character_never_reaches_a_response_header_raw(client):
    client.put(URL, content=BODY, headers={"x-amz-meta-a": "=?UTF-8?Q?a=0D=0Ab=00c=09?="})
    head = client.head(URL)
    assert _meta(head) == {"a": _word("a\r\nb\x00c\t")}
    assert "b" not in head.headers  # no header was split off the value


def test_encoded_words_count_by_their_decoded_size(client):
    # 1500 characters of base64 would not fit; what is stored is 1100 bytes.
    text = "x" * 1100
    assert client.put(URL, content=BODY, headers={"x-amz-meta-a": _word(text)}).status_code == 200
    assert (
        client.put(URL, content=BODY, headers={"x-amz-meta-a": _word("y" * 2048)}).status_code
        == 400
    )


def test_stored_non_ascii_survives_a_restart_of_the_store(env, tmp_path):
    env.client.put(URL, content=BODY, headers={"x-amz-meta-a": _word("café")})
    reopened = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    assert reopened.get_object_meta(BUCKET, "k").user_metadata == {"a": "café"}
