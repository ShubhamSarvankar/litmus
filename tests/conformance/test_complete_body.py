"""CompleteMultipartUpload does not trust its body (docs/design/storage-layout.md §6.7).

The part list must be strictly ascending: a repeated part number or a part out of order
is 400 InvalidPartOrder, never sorted or assembled twice. A body that is empty, not UTF-8,
not XML, the wrong document, without Part elements or with a Part that has no PartNumber
or ETag or a PartNumber that is not an integer is 400 MalformedXML, never a 500. A
PartNumber outside 1 to 10000 is 400 InvalidPart.

A refusal changes nothing the store holds: no object row (or the existing object is as it
was), the blob registry, the blob files and tmp/ are as before, the upload is still open
with all its parts, the sweep finds nothing, and a retry with a correct body succeeds.
The errors come in this order: NoSuchUpload, MalformedXML, InvalidPartOrder, InvalidPart,
then the If-Match and If-None-Match conditions.
"""

import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "complete-body"
KEY = "obj.bin"
OLD = b"old body. " * 100
PARTS = [b"part one. " * 50, b"part two! " * 60, b"part 3... " * 70]
WHOLE = b"".join(PARTS)
ETAGS = ['"' + hashlib.md5(p).hexdigest() + '"' for p in PARTS]
WHOLE_ETAG = '"' + hashlib.md5(b"".join(hashlib.md5(p).digest() for p in PARTS)).hexdigest() + '-3"'
OLD_ETAG = '"' + hashlib.md5(OLD).hexdigest() + '"'
OTHER = '"0123456789abcdef0123456789abcdef"'
NS = "http://s3.amazonaws.com/doc/2006-03-01/"


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    assert client.put(f"/{BUCKET}").status_code == 200
    return client, storage, metadata


def _code(resp) -> str | None:
    try:
        return ET.fromstring(resp.text).findtext("Code")
    except ET.ParseError:
        return None


def _upload(client) -> tuple[str, list[str]]:
    """An open upload with the three parts; its id and the parts' ETags."""
    uid = ET.fromstring(client.post(f"/{BUCKET}/{KEY}?uploads").text).findtext("UploadId")
    etags = []
    for number, data in enumerate(PARTS, start=1):
        r = client.put(f"/{BUCKET}/{KEY}?partNumber={number}&uploadId={uid}", content=data)
        assert r.status_code == 200
        etags.append(r.headers["etag"])
    return uid, etags


def _body(entries: list[tuple[object, str]]) -> bytes:
    items = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>" for n, e in entries
    )
    return f"<CompleteMultipartUpload>{items}</CompleteMultipartUpload>".encode()


def _right_body(etags: list[str]) -> bytes:
    return _body(list(enumerate(etags, start=1)))


def _post(client, uid: str, body: bytes, headers: dict | None = None):
    return client.post(f"/{BUCKET}/{KEY}?uploadId={uid}", content=body, headers=headers or {})


def _object(client) -> tuple[int, str] | None:
    """What a GET of the key shows: its size and ETag, or None when there is no object."""
    got = client.get(f"/{BUCKET}/{KEY}")
    if got.status_code == 404:
        return None
    assert got.status_code == 200
    return len(got.content), got.headers["etag"]


def _state(storage, metadata, uid: str) -> tuple:
    """Everything the store holds that a refused Complete must leave alone."""
    return (
        metadata.list_objects(BUCKET),
        metadata.list_blob_records(),
        storage.temp_entries(),
        sorted(storage.scan()),
        metadata.get_upload(uid),
        metadata.get_parts(uid),
        metadata.list_incomplete_uploads(),
    )


def assert_store_clean(storage, metadata, live: int) -> None:
    """Only live blobs, no pending or garbage one, an empty tmp/ and a quiet sweep."""
    records = metadata.list_blob_records()
    assert [r.state for r in records] == ["live"] * live
    assert storage.temp_entries() == []
    assert run_consistency_sweep(metadata, storage).issues == []


def assert_upload_open(metadata, uid: str, etags: list[str]) -> None:
    assert metadata.upload_exists(uid)
    assert [u.upload_id for u in metadata.list_incomplete_uploads()] == [uid]
    parts = metadata.get_parts(uid)
    assert [(p.part_number, p.etag) for p in parts] == list(enumerate(etags, start=1))
    assert all(p.blob_id is not None for p in parts)


def assert_refused(env, body: bytes, code: str, headers=None, exists=False, retry_headers=None):
    """Post the body to a fresh upload of three parts and check the refusal whole."""
    client, storage, metadata = env
    if exists:
        assert client.put(f"/{BUCKET}/{KEY}", content=OLD).status_code == 200
    uid, etags = _upload(client)
    object_before = _object(client)
    state_before = _state(storage, metadata, uid)

    resp = _post(client, uid, body, headers)

    # First, what the object looks like: a Complete that took the body at its word
    # shows here as an object of the wrong size.
    assert _object(client) == object_before, (
        f"the object changed: {_object(client)}; the three parts hold {len(WHOLE)} bytes"
    )
    assert (resp.status_code, _code(resp)) == (400, code)
    assert _state(storage, metadata, uid) == state_before
    assert_upload_open(metadata, uid, etags)
    assert_store_clean(storage, metadata, live=(1 if exists else 0) + len(PARTS))

    retry = _post(client, uid, _right_body(etags), retry_headers)
    assert retry.status_code == 200
    assert ET.fromstring(retry.text).findtext("ETag") == WHOLE_ETAG
    got = client.get(f"/{BUCKET}/{KEY}")
    assert got.content == WHOLE and got.headers["etag"] == WHOLE_ETAG
    assert not metadata.upload_exists(uid)
    assert_store_clean(storage, metadata, live=1)


# --- InvalidPartOrder ---


def test_a_repeated_part_number_is_invalid_part_order(env):
    client, _, _ = env
    uid, etags = _upload(client)
    resp = _post(client, uid, _body([(1, etags[0]), (1, etags[0]), (2, etags[1])]))
    # Before the fix this assembles part 1 twice: 200 and an object of
    # 2 * len(part 1) + len(part 2) bytes. State what the object is, to see it.
    shape = _object(client)
    assert shape is None, (
        f"an object of {shape[0]} bytes was created from the list 1, 1, 2; the parts "
        f"hold {len(PARTS[0])} and {len(PARTS[1])} bytes, so the right size is "
        f"{len(PARTS[0]) + len(PARTS[1])} and a doubled part 1 is "
        f"{2 * len(PARTS[0]) + len(PARTS[1])}"
    )
    assert (resp.status_code, _code(resp)) == (400, "InvalidPartOrder")


@pytest.mark.parametrize(
    "order",
    [
        pytest.param([1, 1, 2], id="repeat-first"),
        pytest.param([1, 2, 2], id="repeat-last"),
        pytest.param([1, 2, 3, 3], id="repeat-after-all"),
        pytest.param([2, 1, 3], id="swap-first-two"),
        pytest.param([1, 3, 2], id="swap-last-two"),
        pytest.param([3, 2, 1], id="descending"),
        pytest.param([2, 3, 1], id="shuffled"),
        pytest.param([3, 1, 2], id="shuffled-other"),
        pytest.param([1, 2, 3, 1], id="ascending-then-back"),
    ],
)
def test_a_part_list_that_is_not_strictly_ascending_is_invalid_part_order(env, order):
    # Every number names an uploaded part, and every ETag is the right one.
    body = _body([(n, ETAGS[n - 1]) for n in order])
    assert_refused(env, body, "InvalidPartOrder")


def test_a_repeat_with_the_right_etag_is_refused_whole(env):
    body = _body([(1, ETAGS[0]), (1, ETAGS[0]), (2, ETAGS[1])])
    assert_refused(env, body, "InvalidPartOrder")


def test_ascending_parts_with_gaps_are_still_accepted(env):
    # Strictly ascending, not contiguous: parts 1 and 3 of an upload of three.
    client, _, metadata = env
    uid, etags = _upload(client)
    resp = _post(client, uid, _body([(1, etags[0]), (3, etags[2])]))
    assert resp.status_code == 200
    got = client.get(f"/{BUCKET}/{KEY}")
    assert got.content == PARTS[0] + PARTS[2]
    assert not metadata.upload_exists(uid)


def test_a_namespaced_body_is_still_accepted(env):
    client, _, _ = env
    uid, etags = _upload(client)
    items = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>"
        for n, e in enumerate(etags, start=1)
    )
    body = f'<CompleteMultipartUpload xmlns="{NS}">{items}</CompleteMultipartUpload>'.encode()
    resp = _post(client, uid, body)
    assert resp.status_code == 200
    assert client.get(f"/{BUCKET}/{KEY}").content == WHOLE


# --- MalformedXML ---

GOOD_PART = "<Part><PartNumber>1</PartNumber><ETag>{e}</ETag></Part>"

MALFORMED = [
    pytest.param(lambda e: b"", id="empty-body"),
    pytest.param(lambda e: b"   \n", id="blank-body"),
    pytest.param(
        lambda e: b"<CompleteMultipartUpload>\xff\xfe</CompleteMultipartUpload>", id="not-utf8"
    ),
    pytest.param(lambda e: b"\xc3\x28", id="invalid-utf8-sequence"),
    pytest.param(lambda e: b"<CompleteMultipartUpload><Part>", id="unclosed"),
    pytest.param(lambda e: b"not xml at all", id="not-xml"),
    pytest.param(
        lambda e: f"<Other>{GOOD_PART.format(e=e)}</Other>".encode(), id="wrong-root-element"
    ),
    pytest.param(
        lambda e: f"<Part><PartNumber>1</PartNumber><ETag>{e}</ETag></Part>".encode(),
        id="root-is-a-part",
    ),
    pytest.param(lambda e: b"<CompleteMultipartUpload/>", id="no-part-elements-empty-root"),
    pytest.param(
        lambda e: b"<CompleteMultipartUpload></CompleteMultipartUpload>",
        id="no-part-elements",
    ),
    pytest.param(
        lambda e: b"<CompleteMultipartUpload><Other/></CompleteMultipartUpload>",
        id="no-part-elements-other-child",
    ),
    pytest.param(
        lambda e: (
            f"<CompleteMultipartUpload><Part><ETag>{e}</ETag></Part></CompleteMultipartUpload>"
        ).encode(),
        id="part-without-part-number",
    ),
    pytest.param(
        lambda e: (
            b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part>"
            b"</CompleteMultipartUpload>"
        ),
        id="part-without-etag",
    ),
    pytest.param(
        lambda e: (
            b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag></ETag></Part>"
            b"</CompleteMultipartUpload>"
        ),
        id="part-with-empty-etag",
    ),
    pytest.param(
        lambda e: (
            b"<CompleteMultipartUpload><Part><PartNumber></PartNumber><ETag>x</ETag></Part>"
            b"</CompleteMultipartUpload>"
        ),
        id="part-with-empty-part-number",
    ),
    pytest.param(
        lambda e: (
            f"<CompleteMultipartUpload><Part><PartNumber>one</PartNumber><ETag>{e}</ETag>"
            f"</Part></CompleteMultipartUpload>"
        ).encode(),
        id="part-number-is-a-word",
    ),
    pytest.param(
        lambda e: (
            f"<CompleteMultipartUpload><Part><PartNumber>1.5</PartNumber><ETag>{e}</ETag>"
            f"</Part></CompleteMultipartUpload>"
        ).encode(),
        id="part-number-is-a-decimal",
    ),
    pytest.param(
        lambda e: (
            f"<CompleteMultipartUpload><Part><PartNumber>0x1</PartNumber><ETag>{e}</ETag>"
            f"</Part></CompleteMultipartUpload>"
        ).encode(),
        id="part-number-is-hex",
    ),
    pytest.param(
        lambda e: (
            f"<CompleteMultipartUpload>{GOOD_PART.format(e=e)}"
            f"<Part><ETag>{e}</ETag></Part></CompleteMultipartUpload>"
        ).encode(),
        id="second-part-without-part-number",
    ),
]


@pytest.mark.parametrize("make", MALFORMED)
def test_a_body_that_is_not_a_complete_document_is_malformed_xml(env, make):
    assert_refused(env, make(ETAGS[0]), "MalformedXML")


def test_a_missing_body_is_malformed_xml(env):
    # What botocore sends for CompleteMultipartUpload with no MultipartUpload argument.
    client, storage, metadata = env
    uid, etags = _upload(client)
    resp = client.post(f"/{BUCKET}/{KEY}?uploadId={uid}")
    assert (resp.status_code, _code(resp)) == (400, "MalformedXML")
    assert _object(client) is None
    assert_upload_open(metadata, uid, etags)
    assert_store_clean(storage, metadata, live=len(PARTS))


# --- InvalidPart for a number outside 1 to 10000 ---


@pytest.mark.parametrize(
    "number",
    [
        pytest.param("0", id="zero"),
        pytest.param("-1", id="negative"),
        pytest.param("10001", id="one-over"),
        pytest.param("99999999999", id="eleven-digits"),
        pytest.param("9" * 5000, id="five-thousand-digits"),
    ],
)
def test_a_part_number_outside_1_to_10000_is_invalid_part(env, number):
    body = _body([(number, ETAGS[0])])
    assert_refused(env, body, "InvalidPart")


def test_the_largest_part_number_is_in_range_but_must_have_been_uploaded(env):
    client, storage, metadata = env
    uid, etags = _upload(client)
    resp = _post(client, uid, _body([(10000, etags[0])]))
    assert (resp.status_code, _code(resp)) == (400, "InvalidPart")
    assert_upload_open(metadata, uid, etags)
    assert_store_clean(storage, metadata, live=len(PARTS))


# --- the order of the errors ---


def test_no_such_upload_beats_malformed_xml(env):
    client, _, metadata = env
    resp = _post(client, "0" * 32, b"")
    assert (resp.status_code, _code(resp)) == (404, "NoSuchUpload")
    resp = _post(client, "0" * 32, b"\xff not xml")
    assert (resp.status_code, _code(resp)) == (404, "NoSuchUpload")
    assert metadata.list_incomplete_uploads() == []


def test_malformed_xml_beats_invalid_part_order(env):
    # Out of order, and the last Part has no ETag.
    client, _, _ = env
    etags = ETAGS
    body = (
        "<CompleteMultipartUpload>"
        f"<Part><PartNumber>2</PartNumber><ETag>{etags[1]}</ETag></Part>"
        f"<Part><PartNumber>1</PartNumber><ETag>{etags[0]}</ETag></Part>"
        "<Part><PartNumber>3</PartNumber></Part>"
        "</CompleteMultipartUpload>"
    ).encode()
    assert_refused(env, body, "MalformedXML")


@pytest.mark.parametrize(
    "entries",
    [
        pytest.param([(2, OTHER), (1, OTHER)], id="out-of-order-with-wrong-etags"),
        pytest.param([(1, OTHER), (1, OTHER)], id="repeated-with-wrong-etags"),
        pytest.param([(3, "right"), (2, "right"), (7, OTHER)], id="descending-with-unknown-part"),
        pytest.param([(2, "right"), (1, "right"), (0, "right")], id="descending-to-zero"),
        pytest.param([(1, "right"), (20000, "right"), (2, "right")], id="out-of-range-then-back"),
    ],
)
def test_invalid_part_order_beats_invalid_part(env, entries):
    etags = ETAGS
    body = _body([(n, etags[n - 1] if e == "right" and 1 <= n <= 3 else e) for n, e in entries])
    assert_refused(env, body, "InvalidPartOrder")


def _holds(exists: bool) -> dict:
    """A condition that holds for the key, with or without the old object."""
    return {"If-Match": OLD_ETAG} if exists else {"If-None-Match": "*"}


FAILING = [
    pytest.param({"If-Match": OTHER}, True, id="if-match-mismatch"),
    pytest.param({"If-Match": "*"}, False, id="if-match-star-no-object"),
    pytest.param({"If-None-Match": "*"}, True, id="if-none-match-star-existing"),
]


@pytest.mark.parametrize("headers,exists", FAILING)
@pytest.mark.parametrize(
    "entries",
    [
        pytest.param([(1, OTHER)], id="wrong-etag"),
        pytest.param([(1, "right"), (9, "right")], id="part-never-uploaded"),
        pytest.param([(10001, "right")], id="out-of-range"),
    ],
)
def test_invalid_part_beats_a_failing_condition(env, headers, exists, entries):
    etags = ETAGS
    body = _body([(n, etags[0] if e == "right" else e) for n, e in entries])
    assert_refused(
        env, body, "InvalidPart", headers=headers, exists=exists, retry_headers=_holds(exists)
    )


@pytest.mark.parametrize("headers,exists", FAILING)
@pytest.mark.parametrize(
    "body,code",
    [
        pytest.param(b"", "MalformedXML", id="malformed"),
        pytest.param(_body([(2, ETAGS[1]), (1, ETAGS[0])]), "InvalidPartOrder", id="order"),
    ],
)
def test_the_body_errors_beat_a_failing_condition(env, headers, exists, body, code):
    assert_refused(env, body, code, headers=headers, exists=exists, retry_headers=_holds(exists))
