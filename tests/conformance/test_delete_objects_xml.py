"""
DeleteObjects request bodies: malformed XML is 400 MalformedXML, and character
references to valid XML characters decode to those characters.

The body used to go straight into ElementTree: any parse error, and any body that is
not UTF-8, escaped as an exception and came back as 500 InternalError. A reference to a
character XML 1.0 forbids (&#0;) is not well-formed (XML 1.0 section 4.1, well-
formedness constraint "Legal Character": it "MUST match the production for Char").
"""

import io
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.base import ObjectMeta
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "bkt"
NS = 'xmlns="http://s3.amazonaws.com/doc/2006-03-01/"'


@pytest.fixture
def deps(tmp_path: Path):
    return BlobStore(tmp_path / "blobs"), SQLiteMetadataStore(db_path=tmp_path / "meta.db")


@pytest.fixture
def client(deps) -> TestClient:
    storage, metadata = deps
    c = TestClient(create_app(storage=storage, metadata=metadata), raise_server_exceptions=False)
    assert c.put(f"/{BUCKET}").status_code == 200
    return c


def _seed(deps, key: str) -> None:
    """Store a key directly, so keys no URL path can carry (an LF) can be used."""
    storage, metadata = deps
    blob_id = metadata.register_blob("object", BUCKET, key)
    etag, size = storage.write_blob(blob_id, io.BytesIO(b"x"))
    metadata.put_object_meta(
        BUCKET,
        key,
        ObjectMeta(
            bucket=BUCKET,
            key=key,
            size=size,
            etag=etag,
            last_modified=datetime(2024, 1, 1),
            content_type="application/octet-stream",
            blob_id=blob_id,
        ),
    )


def _exists(deps, key: str) -> bool:
    return deps[1].get_object_meta(BUCKET, key) is not None


def _delete(client, body: bytes):
    return client.post(f"/{BUCKET}?delete", content=body)


def _one(key_xml: str) -> bytes:
    return f"<Delete><Object><Key>{key_xml}</Key></Object></Delete>".encode()


def _billion_laughs() -> bytes:
    decls = '<!ENTITY a "aaaaaaaaaa">' + "".join(
        f'<!ENTITY {n} "{"&" + p + ";"}{"".join(["&" + p + ";"] * 9)}">'
        for p, n in zip("abcdefghi", "bcdefghij", strict=True)
    )
    return f"<!DOCTYPE l [{decls}]><Delete><Object><Key>&j;</Key></Object></Delete>".encode()


MALFORMED = {
    "empty": b"",
    "whitespace only": b"  \n",
    "not xml": b"hello",
    "truncated": b"<Delete><Object><Key>a</Key>",
    "mismatched end tag": b"<Delete><Object></Delete>",
    "unclosed root": b"<Delete><Object><Key>a</Key></Object>",
    "text after root": b"<Delete></Delete>junk",
    "two roots": b"<Delete/><Delete/>",
    "bare ampersand": _one("a&b"),
    "bare less-than": _one("a<b"),
    "undefined entity": _one("&nope;"),
    "attribute without value": b"<Delete a><Object><Key>a</Key></Object></Delete>",
    "invalid utf-8": b"<Delete><Object><Key>\xff\xfe</Key></Object></Delete>",
    "utf-16 body": "<Delete><Object><Key>a</Key></Object></Delete>".encode("utf-16"),
    "raw nul": b"<Delete><Object><Key>a\x00b</Key></Object></Delete>",
    "raw control character": b"<Delete><Object><Key>a\x01b</Key></Object></Delete>",
    "entity expansion bomb": _billion_laughs(),
    # References to characters XML 1.0 forbids, and references that are not numbers.
    "&#0;": _one("a&#0;b"),
    "&#x0;": _one("a&#x0;b"),
    "&#1;": _one("a&#1;b"),
    "&#x8;": _one("a&#x8;b"),
    "&#xB;": _one("a&#xB;b"),
    "&#xC;": _one("a&#xC;b"),
    "&#xE;": _one("a&#xE;b"),
    "&#x1F;": _one("a&#x1F;b"),
    "&#xD800;": _one("a&#xD800;b"),
    "&#xDFFF;": _one("a&#xDFFF;b"),
    "&#xFFFE;": _one("a&#xFFFE;b"),
    "&#xFFFF;": _one("a&#xFFFF;b"),
    "&#x110000;": _one("a&#x110000;b"),
    "huge number": _one("a&#99999999999999999999;b"),
    "empty decimal": _one("a&#;b"),
    "empty hex": _one("a&#x;b"),
    "non-digit decimal": _one("a&#12a;b"),
    "uppercase X": _one("a&#X41;b"),
    "unterminated": _one("a&#65b"),
}


@pytest.mark.parametrize("body", MALFORMED.values(), ids=MALFORMED.keys())
def test_malformed_body_is_400_malformed_xml_and_deletes_nothing(client, deps, body):
    _seed(deps, "keep")
    resp = _delete(client, body)
    assert resp.status_code == 400, resp.text
    root = ET.fromstring(resp.text)
    assert root.findtext("Code") == "MalformedXML"
    assert root.findtext("Message") == (
        "The XML you provided was not well-formed or did not validate against our published schema"
    )
    assert resp.headers["content-type"] == "application/xml"
    assert _exists(deps, "keep")


def test_a_malformed_body_deletes_nothing_even_after_a_valid_entry(client, deps):
    _seed(deps, "first")
    body = b"<Delete><Object><Key>first</Key></Object><Object><Key>a&#0;</Key></Object></Delete>"
    assert _delete(client, body).status_code == 400
    assert _exists(deps, "first")


def test_a_malformed_body_for_a_missing_bucket_is_still_no_such_bucket(client):
    resp = client.post("/nope?delete", content=b"<Delete")
    assert resp.status_code == 404
    assert ET.fromstring(resp.text).findtext("Code") == "NoSuchBucket"


def test_the_slash_path_answers_the_same(client):
    resp = client.post(f"/{BUCKET}/?delete", content=b"<Delete")
    assert resp.status_code == 400
    assert ET.fromstring(resp.text).findtext("Code") == "MalformedXML"


# --- character references to valid XML characters ---

VALID_REFERENCES = {
    "&#9;": ("a&#9;b", "a\tb"),
    "&#x9;": ("a&#x9;b", "a\tb"),
    "&#10;": ("a&#10;b", "a\nb"),
    "&#xA;": ("a&#xA;b", "a\nb"),
    "&#13;": ("a&#13;b", "a\rb"),
    "&#xD;": ("a&#xD;b", "a\rb"),
    "&#32;": ("a&#32;b", "a b"),
    "&#65;": ("a&#65;b", "aAb"),
    "&#x41;": ("a&#x41;b", "aAb"),
    "&#xD7FF;": ("a&#xD7FF;b", "a퟿b"),
    "&#xE000;": ("a&#xE000;b", "ab"),
    "&#xFFFD;": ("a&#xFFFD;b", "a�b"),
    "&#x10000;": ("a&#x10000;b", "a\U00010000b"),
    "&#x1F600;": ("a&#x1F600;b", "a\U0001f600b"),
    "&#x10FFFF;": ("a&#x10FFFF;b", "a\U0010ffffb"),
    "&#00065;": ("a&#00065;b", "aAb"),
    "predefined": ("a&amp;&lt;&gt;&quot;&apos;b", "a&<>\"'b"),
    "mixed": ("&#x3C;tag&#62;", "<tag>"),
}


@pytest.mark.parametrize(("key_xml", "key"), VALID_REFERENCES.values(), ids=VALID_REFERENCES.keys())
def test_a_reference_to_a_valid_character_deletes_the_key_it_names(client, deps, key_xml, key):
    _seed(deps, key)
    _seed(deps, "bystander")
    resp = _delete(client, _one(key_xml))
    assert resp.status_code == 200, resp.text
    deleted = ET.fromstring(resp.text).findall("Deleted")
    assert len(deleted) == 1
    assert not _exists(deps, key)
    assert _exists(deps, "bystander")


def test_references_and_plain_keys_mix_in_one_request(client, deps):
    for key in ("plain", "t\tab", "smile\U0001f600"):
        _seed(deps, key)
    body = _one("plain").replace(
        b"</Delete>",
        b"<Object><Key>t&#9;ab</Key></Object><Object><Key>smile&#x1F600;</Key></Object></Delete>",
    )
    assert _delete(client, body).status_code == 200
    assert not any(_exists(deps, k) for k in ("plain", "t\tab", "smile\U0001f600"))


def test_a_namespaced_body_with_a_declaration_and_a_reference(client, deps):
    _seed(deps, "a\tb")
    body = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f"<Delete {NS}><Object><Key>a&#9;b</Key></Object></Delete>"
    ).encode()
    assert _delete(client, body).status_code == 200
    assert not _exists(deps, "a\tb")


def test_an_encoding_declaration_does_not_change_how_the_bytes_are_read(client, deps):
    """The body is read as UTF-8 whatever it declares, so a Latin-1 declaration over
    UTF-8 bytes does not turn the key into mojibake."""
    _seed(deps, "é")
    body = b'<?xml version="1.0" encoding="ISO-8859-1"?>' + _one("é")
    assert _delete(client, body).status_code == 200
    assert not _exists(deps, "é")


def test_a_utf8_byte_order_mark_is_accepted(client, deps):
    _seed(deps, "k")
    assert _delete(client, b"\xef\xbb\xbf" + _one("k")).status_code == 200
    assert not _exists(deps, "k")


# --- carriage returns ---


def test_a_cr_key_is_deleted_by_a_character_reference(client, deps):
    """AWS's DeleteObjects example uses &#13; for this: an XML parser turns a CR
    written literally into LF (XML 1.0 section 2.11), so a reference is the only way
    a request body can name a CR."""
    _seed(deps, "dir/with\rcr")
    _seed(deps, "dir/with\ncr")
    resp = _delete(client, _one("dir/with&#13;cr"))
    assert resp.status_code == 200
    assert not _exists(deps, "dir/with\rcr")
    assert _exists(deps, "dir/with\ncr")


def test_a_cr_key_put_over_http_is_deleted_by_a_character_reference(client, deps):
    assert client.put(f"/{BUCKET}/dir/with%0Dcr", content=b"x").status_code == 200
    assert _exists(deps, "dir/with\rcr")
    assert _delete(client, _one("dir/with&#13;cr")).status_code == 200
    assert not _exists(deps, "dir/with\rcr")
    assert client.get(f"/{BUCKET}").text.count("<Key>") == 0


def test_a_literal_cr_in_the_body_names_the_lf_key_instead(client, deps):
    _seed(deps, "a\rb")
    _seed(deps, "a\nb")
    resp = _delete(client, _one("a\rb"))
    assert resp.status_code == 200
    assert not _exists(deps, "a\nb")  # the key the parser produced: LF
    assert _exists(deps, "a\rb")  # still there, though the answer was Deleted
    assert len(ET.fromstring(resp.text).findall("Deleted")) == 1


def test_a_literal_crlf_in_the_body_is_one_lf(client, deps):
    _seed(deps, "a\nb")
    assert _delete(client, _one("a\r\nb")).status_code == 200
    assert not _exists(deps, "a\nb")
