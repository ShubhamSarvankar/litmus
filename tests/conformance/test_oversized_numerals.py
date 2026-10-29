"""Decimal numbers in a request are read by value, however long their digits
(docs/design/storage-layout.md, decision 37; server/numerals.py).

Python's int() refuses a string of more than 4300 digits, which used to be a 500 at some
sites and a different answer from an out of range value at others. Each site here takes
the numerals 4300, 4301 and 5000 nines (far past every limit) and 4999 zeros followed by 1
(the value 1), and gives an oversized value the answer of an out of range one. Every
refusal leaves the store as it was. Range on GET and HEAD is not in this file: it keeps
its rule of ignoring a numeral over 4000 digits (decision 37).
"""

import socket
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from chaos.framework.framed import frame, framed_headers
from server.app import create_app
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "numerals"
SRC_BUCKET = "numerals-src"
KEY = "obj"
SRC = "src"
SRC_BODY = b"0123456789"

OVERSIZED = [
    pytest.param("9" * 4300, id="4300-nines"),
    pytest.param("9" * 4301, id="4301-nines"),
    pytest.param("9" * 5000, id="5000-nines"),
]
PADDED_ONE = "0" * 4999 + "1"  # the value 1


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    for bucket in (BUCKET, SRC_BUCKET):
        assert client.put(f"/{bucket}").status_code == 200
    assert client.put(f"/{SRC_BUCKET}/{SRC}", content=SRC_BODY).status_code == 200
    return client, storage, metadata


def _code(resp) -> str:
    return ET.fromstring(resp.text).findtext("Code")


def _field(resp, name: str):
    return ET.fromstring(resp.text).findtext(name)


def _initiate(client, key=KEY) -> str:
    resp = client.post(f"/{BUCKET}/{key}?uploads")
    assert resp.status_code == 200
    return ET.fromstring(resp.text).findtext("UploadId")


def _upload(client, parts: list[bytes], key=KEY) -> tuple[str, list[str]]:
    uid = _initiate(client, key)
    etags = []
    for number, data in enumerate(parts, 1):
        resp = client.put(f"/{BUCKET}/{key}?partNumber={number}&uploadId={uid}", content=data)
        assert resp.status_code == 200
        etags.append(resp.headers["etag"])
    return uid, etags


def _snapshot(storage, metadata, key=KEY) -> tuple:
    """Everything a refused request must leave alone."""
    uploads = sorted(u.upload_id for u in metadata.list_incomplete_uploads())
    parts = [(u, metadata.get_parts(u)) for u in uploads]
    registry = sorted((r.blob_id, r.state) for r in metadata.list_blob_records())
    return uploads, parts, registry, storage.temp_entries(), metadata.get_object_meta(BUCKET, key)


def _assert_clean(storage, metadata) -> None:
    assert run_consistency_sweep(metadata, storage).issues == []
    assert storage.temp_entries() == []
    assert {r.state for r in metadata.list_blob_records()} <= {"live"}


# --- max-keys ---


@pytest.mark.parametrize("query", ["", "&list-type=2"], ids=["v1", "v2"])
@pytest.mark.parametrize("value", OVERSIZED)
def test_oversized_max_keys_is_capped_at_1000(env, query, value):
    client, storage, metadata = env
    for key in ("a", "b", "c"):
        assert client.put(f"/{BUCKET}/{key}", content=b"x").status_code == 200
    before = _snapshot(storage, metadata)
    resp = client.get(f"/{BUCKET}?max-keys={value}{query}")
    assert resp.status_code == 200
    assert _field(resp, "MaxKeys") == "1000"
    assert len(ET.fromstring(resp.text).findall("Contents")) == 3
    assert _snapshot(storage, metadata) == before


@pytest.mark.parametrize("query", ["", "&list-type=2"], ids=["v1", "v2"])
def test_max_keys_with_long_leading_zeros_is_its_value(env, query):
    client, _, _ = env
    for key in ("a", "b", "c"):
        assert client.put(f"/{BUCKET}/{key}", content=b"x").status_code == 200
    resp = client.get(f"/{BUCKET}?max-keys={PADDED_ONE}{query}")
    assert resp.status_code == 200
    assert _field(resp, "MaxKeys") == "1"
    assert len(ET.fromstring(resp.text).findall("Contents")) == 1
    assert _field(resp, "IsTruncated") == "true"


# --- ListParts: max-parts and part-number-marker ---


def _list(client, uid, **query) -> httpx.Response:
    qs = "&".join(f"{k}={v}" for k, v in query.items())
    return client.get(f"/{BUCKET}/{KEY}?uploadId={uid}" + (f"&{qs}" if qs else ""))


def _listed(resp) -> list[str]:
    return [p.findtext("PartNumber") for p in ET.fromstring(resp.text).findall("Part")]


@pytest.mark.parametrize("value", OVERSIZED)
def test_oversized_max_parts_is_the_cap(env, value):
    client, storage, metadata = env
    uid, _ = _upload(client, [b"a", b"b", b"c"])
    before = _snapshot(storage, metadata)
    resp = _list(client, uid, **{"max-parts": value})
    assert resp.status_code == 200
    assert _field(resp, "MaxParts") == "1000"
    assert _listed(resp) == ["1", "2", "3"]
    assert _snapshot(storage, metadata) == before


@pytest.mark.parametrize("value", OVERSIZED)
def test_oversized_part_number_marker_lists_no_parts(env, value):
    client, storage, metadata = env
    uid, _ = _upload(client, [b"a", b"b", b"c"])
    before = _snapshot(storage, metadata)
    resp = _list(client, uid, **{"part-number-marker": value})
    assert resp.status_code == 200
    assert _field(resp, "PartNumberMarker") == value
    assert _listed(resp) == []
    assert _field(resp, "IsTruncated") == "false"
    assert _snapshot(storage, metadata) == before


def test_max_parts_and_marker_with_long_leading_zeros_are_their_values(env):
    client, _, _ = env
    uid, _ = _upload(client, [b"a", b"b", b"c"])
    resp = _list(client, uid, **{"max-parts": PADDED_ONE})
    assert (_field(resp, "MaxParts"), _listed(resp)) == ("1", ["1"])
    resp = _list(client, uid, **{"part-number-marker": PADDED_ONE})
    assert (_field(resp, "PartNumberMarker"), _listed(resp)) == ("1", ["2", "3"])


# --- Complete: x-amz-mp-object-size and PartNumber ---


def _complete(client, uid, numbers_and_etags, headers=None):
    body = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>" for n, e in numbers_and_etags
    )
    return client.post(
        f"/{BUCKET}/{KEY}?uploadId={uid}",
        content=f"<CompleteMultipartUpload>{body}</CompleteMultipartUpload>",
        headers=headers or {},
    )


@pytest.mark.parametrize("value", OVERSIZED)
def test_oversized_object_size_is_a_mismatch(env, value):
    client, storage, metadata = env
    uid, etags = _upload(client, [b"x"])
    before = _snapshot(storage, metadata)
    resp = _complete(client, uid, [(1, etags[0])], {"x-amz-mp-object-size": value})
    assert (resp.status_code, _code(resp)) == (400, "InvalidRequest")
    assert _snapshot(storage, metadata) == before
    _assert_clean(storage, metadata)


def test_object_size_with_long_leading_zeros_is_its_value(env):
    client, _, metadata = env
    uid, etags = _upload(client, [b"x"])
    resp = _complete(client, uid, [(1, etags[0])], {"x-amz-mp-object-size": PADDED_ONE})
    assert resp.status_code == 200
    assert metadata.get_object_meta(BUCKET, KEY).size == 1


@pytest.mark.parametrize("sign", ["", "-"], ids=["positive", "negative"])
@pytest.mark.parametrize("value", OVERSIZED)
def test_oversized_complete_part_number_is_invalid_part(env, value, sign):
    client, storage, metadata = env
    uid, etags = _upload(client, [b"x"])
    before = _snapshot(storage, metadata)
    resp = _complete(client, uid, [(sign + value, etags[0])])
    assert (resp.status_code, _code(resp)) == (400, "InvalidPart")
    assert _snapshot(storage, metadata) == before
    _assert_clean(storage, metadata)


LOW, HIGH = "9" * 4999, "9" * 5000  # two distinct oversized values, LOW below HIGH
# (name, the numbers of a reference pair in ascending order, the oversized pair likewise)
ORDERED_PAIRS = [
    pytest.param(("10001", "10002"), (LOW, HIGH), id="positive"),
    pytest.param(("-10002", "-10001"), ("-" + HIGH, "-" + LOW), id="negative"),
]


def _complete_answer(client, uid, etag, numbers) -> tuple:
    return _answer(_complete(client, uid, [(n, etag) for n in numbers]))


@pytest.mark.parametrize("reference, oversized", ORDERED_PAIRS)
def test_oversized_part_numbers_in_ascending_order_are_answered_as_10001_and_10002_are(
    env, reference, oversized
):
    client, storage, metadata = env
    uid, etags = _upload(client, [b"x"])
    before = _snapshot(storage, metadata)
    expected = (400, "InvalidPart", {"PartNumber": reference[0]})
    assert _complete_answer(client, uid, etags[0], reference) == expected
    expected = (400, "InvalidPart", {"PartNumber": oversized[0]})
    assert _complete_answer(client, uid, etags[0], oversized) == expected
    assert _snapshot(storage, metadata) == before
    _assert_clean(storage, metadata)


@pytest.mark.parametrize("reference, oversized", ORDERED_PAIRS)
def test_oversized_part_numbers_in_descending_order_are_answered_as_10002_and_10001_are(
    env, reference, oversized
):
    client, storage, metadata = env
    uid, etags = _upload(client, [b"x"])
    before = _snapshot(storage, metadata)
    expected = (400, "InvalidPartOrder", {})
    assert _complete_answer(client, uid, etags[0], reference[::-1]) == expected
    assert _complete_answer(client, uid, etags[0], oversized[::-1])[:2] == expected[:2]
    assert _snapshot(storage, metadata) == before
    _assert_clean(storage, metadata)


@pytest.mark.parametrize("reference, oversized", ORDERED_PAIRS)
def test_the_same_oversized_part_number_twice_is_answered_as_10001_twice_is(
    env, reference, oversized
):
    client, storage, metadata = env
    uid, etags = _upload(client, [b"x"])
    before = _snapshot(storage, metadata)
    sign = "-" if oversized[0].startswith("-") else ""
    padded = sign + "00" + oversized[0].lstrip("-")  # the same value, written with zeros
    for numbers in ((reference[0],) * 2, (oversized[0],) * 2, (oversized[0], padded)):
        answer = _complete_answer(client, uid, etags[0], numbers)
        assert answer[:2] == (400, "InvalidPartOrder"), numbers
    assert _snapshot(storage, metadata) == before
    _assert_clean(storage, metadata)


@pytest.mark.parametrize("sign", ["", "-"], ids=["positive", "negative"])
def test_complete_names_an_oversized_part_number_by_its_digits(env, sign):
    client, _, _ = env
    uid, etags = _upload(client, [b"x"])
    for sent in (sign + HIGH, sign + "0000" + HIGH):
        answer = _complete_answer(client, uid, etags[0], [sent])
        assert answer == (400, "InvalidPart", {"PartNumber": sign + HIGH})
    padded = sign + "0" * 4300 + "10001"
    assert _complete_answer(client, uid, etags[0], [padded])[2] == {"PartNumber": sign + "10001"}


def test_complete_part_number_with_long_leading_zeros_is_its_value(env):
    client, _, metadata = env
    uid, etags = _upload(client, [b"x"])
    resp = _complete(client, uid, [(PADDED_ONE, etags[0])])
    assert resp.status_code == 200
    assert metadata.get_object_meta(BUCKET, KEY).size == 1


@pytest.mark.parametrize("text", ["+1", " 1 x", "1_0", "١", "1.0"])
def test_complete_part_number_syntax_is_unchanged(env, text):
    client, storage, metadata = env
    uid, etags = _upload(client, [b"x"])
    before = _snapshot(storage, metadata)
    resp = _complete(client, uid, [(text, etags[0])])
    assert (resp.status_code, _code(resp)) == (400, "MalformedXML")
    assert _snapshot(storage, metadata) == before


# --- UploadPart and UploadPartCopy: partNumber ---

KINDS = ["part", "copy"]
MALFORMED = [
    pytest.param("%2B1", id="plus"),
    pytest.param("1_0", id="underscore"),
    pytest.param("%201", id="leading-space"),
    pytest.param("1%20", id="trailing-space"),
    pytest.param("%D9%A1", id="arabic-indic-digit"),
    pytest.param("abc", id="abc"),
    pytest.param("", id="empty"),
]


def _put_part(client, kind, number, *, bucket=BUCKET, upload, headers=None):
    target = f"/{bucket}/{KEY}?partNumber={number}&uploadId={upload}"
    if kind == "copy":
        return client.put(
            target, headers={"x-amz-copy-source": f"{SRC_BUCKET}/{SRC}", **(headers or {})}
        )
    return client.put(target, content=b"part", headers=headers or {})


def _answer(resp) -> tuple:
    """Status, code and the detail elements of an error."""
    root = ET.fromstring(resp.text)
    detail = {
        c.tag: c.text for c in root if c.tag not in ("Code", "Message", "RequestId", "HostId")
    }
    return resp.status_code, root.findtext("Code"), detail


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("value", OVERSIZED)
def test_oversized_part_number_is_answered_as_10001_is(env, kind, value):
    client, storage, metadata = env
    uid = _initiate(client)
    before = _snapshot(storage, metadata)
    over = _put_part(client, kind, 10001, upload=uid)
    resp = _put_part(client, kind, value, upload=uid)
    assert _answer(over) == (400, "InvalidPart", {"PartNumber": "10001"})
    assert _answer(resp) == (400, "InvalidPart", {"PartNumber": value})
    assert _snapshot(storage, metadata) == before
    _assert_clean(storage, metadata)


@pytest.mark.parametrize("kind", KINDS)
def test_a_negative_part_number_is_answered_as_minus_1_is(env, kind):
    client, storage, metadata = env
    uid = _initiate(client)
    before = _snapshot(storage, metadata)
    assert _answer(_put_part(client, kind, "-1", upload=uid)) == (
        400,
        "InvalidPart",
        {"PartNumber": "-1"},
    )
    for value in ("-" + "9" * 5000, "-" + "9" * 4301):
        assert _answer(_put_part(client, kind, value, upload=uid)) == (
            400,
            "InvalidPart",
            {"PartNumber": value},
        )
    assert _snapshot(storage, metadata) == before


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("value", MALFORMED)
def test_a_part_number_that_is_not_a_decimal_is_answered_as_abc_is(env, kind, value):
    client, storage, metadata = env
    uid = _initiate(client)
    before = _snapshot(storage, metadata)
    assert _answer(_put_part(client, kind, "abc", upload=uid)) == (400, "InvalidPart", {})
    assert _answer(_put_part(client, kind, value, upload=uid)) == (400, "InvalidPart", {})
    assert _snapshot(storage, metadata) == before
    _assert_clean(storage, metadata)


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("sign", ["", "-"], ids=["positive", "negative"])
def test_a_part_number_padded_with_zeros_is_named_without_them(env, kind, sign):
    client, _, _ = env
    uid = _initiate(client)
    for sent in (sign + "0" * 3000 + HIGH, sign + "0" * 4300 + "10001"):
        resp = _put_part(client, kind, sent, upload=uid)
        assert _answer(resp) == (400, "InvalidPart", {"PartNumber": sign + sent.lstrip("-0")})


@pytest.mark.parametrize("kind", KINDS)
def test_a_part_number_with_long_leading_zeros_is_its_value(env, kind):
    client, _, metadata = env
    uid = _initiate(client)
    assert _put_part(client, kind, PADDED_ONE, upload=uid).status_code == 200
    assert [p.part_number for p in metadata.get_parts(uid)] == [1]


@pytest.mark.parametrize("value", ["9" * 5000, "10001"])
def test_oversized_part_number_sits_where_10001_does(env, value):
    client, storage, metadata = env
    uid = _initiate(client)
    before = _snapshot(storage, metadata)
    # UploadPart: a number is checked, then the bucket, then the upload, then the range.
    resp = _put_part(client, "part", value, bucket="no-such-bucket", upload=uid)
    assert (resp.status_code, _code(resp)) == (404, "NoSuchBucket")
    resp = _put_part(client, "part", value, upload="no-such-upload")
    assert (resp.status_code, _code(resp)) == (404, "NoSuchUpload")
    # UploadPartCopy: the destination bucket, then the number, then the upload.
    resp = _put_part(client, "copy", value, bucket="no-such-bucket", upload=uid)
    assert (resp.status_code, _code(resp)) == (404, "NoSuchBucket")
    resp = _put_part(client, "copy", value, upload="no-such-upload")
    assert (resp.status_code, _code(resp)) == (404, "NoSuchUpload")
    assert _snapshot(storage, metadata) == before


def test_a_part_number_that_is_not_a_decimal_keeps_its_place_in_each_order(env):
    client, _, _ = env
    uid = _initiate(client)
    for value in ("abc", "%2B1", "%D9%A1"):
        # UploadPart reads the number before the bucket; UploadPartCopy after it.
        resp = _put_part(client, "part", value, bucket="no-such-bucket", upload=uid)
        assert (resp.status_code, _code(resp)) == (400, "InvalidPart"), value
        resp = _put_part(client, "copy", value, bucket="no-such-bucket", upload=uid)
        assert (resp.status_code, _code(resp)) == (404, "NoSuchBucket"), value
        resp = _put_part(client, "part", value, upload="no-such-upload")
        assert (resp.status_code, _code(resp)) == (400, "InvalidPart"), value
        resp = _put_part(client, "copy", value, upload="no-such-upload")
        assert (resp.status_code, _code(resp)) == (400, "InvalidPart"), value


# --- x-amz-copy-source-range ---


def _copy_range(client, uid, spec):
    return _put_part(client, "copy", 1, upload=uid, headers={"x-amz-copy-source-range": spec})


@pytest.mark.parametrize("value", OVERSIZED)
@pytest.mark.parametrize("form", ["bytes=0-{v}", "bytes={v}-{v}"])
def test_an_oversized_byte_past_the_source_is_answered_as_a_smaller_source_is(env, form, value):
    client, storage, metadata = env
    uid = _initiate(client)
    before = _snapshot(storage, metadata)
    reference = _copy_range(client, uid, "bytes=0-100")
    resp = _copy_range(client, uid, form.format(v=value))
    assert _answer(reference)[:2] == (416, "InvalidRange")
    assert _answer(resp) == _answer(reference)
    assert _snapshot(storage, metadata) == before
    _assert_clean(storage, metadata)


@pytest.mark.parametrize("value", OVERSIZED)
def test_an_oversized_first_byte_above_the_last_is_answered_as_5_2_is(env, value):
    client, storage, metadata = env
    uid = _initiate(client)
    before = _snapshot(storage, metadata)
    reference = _copy_range(client, uid, "bytes=5-2")
    resp = _copy_range(client, uid, f"bytes={value}-2")
    assert _answer(reference) == (
        400,
        "InvalidArgument",
        {"ArgumentName": "x-amz-copy-source-range"},
    )
    assert _answer(resp) == _answer(reference)
    assert _snapshot(storage, metadata) == before


def test_two_oversized_bytes_are_ordered_by_value(env):
    client, storage, metadata = env
    uid = _initiate(client)
    before = _snapshot(storage, metadata)
    longer, shorter = "9" * 4301, "9" * 4300
    assert _copy_range(client, uid, f"bytes={longer}-{shorter}").status_code == 400
    assert _snapshot(storage, metadata) == before
    assert _copy_range(client, uid, f"bytes={shorter}-{longer}").status_code == 416
    assert _copy_range(client, uid, f"bytes={longer}-{longer}").status_code == 416
    assert _snapshot(storage, metadata) == before


def test_range_numerals_with_long_leading_zeros_are_read_by_value(env):
    client, _, metadata = env
    uid = _initiate(client)
    assert _copy_range(client, uid, f"bytes={PADDED_ONE}-{PADDED_ONE}").status_code == 200
    (part,) = metadata.get_parts(uid)
    assert part.size == 1
    assert _copy_range(client, uid, f"bytes=0-{PADDED_ONE}").status_code == 200
    (part,) = metadata.get_parts(uid)
    assert part.size == 2


# --- aws-chunked: x-amz-decoded-content-length and Content-Length, over a real socket ---

DATA = bytes(range(256)) * 20  # 5120 bytes


@pytest.fixture(params=["httptools", "h11"])
def live(request, tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(storage=storage, metadata=metadata),
            host="127.0.0.1",
            port=port,
            http=request.param,
            log_level="critical",
        )
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.02)
    assert server.started
    base = f"http://127.0.0.1:{port}"
    assert httpx.put(f"{base}/{BUCKET}").status_code == 200
    upload = ET.fromstring(httpx.post(f"{base}/{BUCKET}/{KEY}?uploads").text).findtext("UploadId")
    yield request.param, port, upload, metadata, storage
    server.should_exit = True
    thread.join(timeout=10)


def _exchange(port, target, headers, body: bytes) -> tuple[int, dict, bytes]:
    """One raw PUT: (status, lowercase header fields, response body). The response is
    read to its Content-Length, or to the close of the connection when it has none."""
    head = f"PUT {target} HTTP/1.1\r\nHost: x\r\n"
    head += "".join(f"{k}: {v}\r\n" for k, v in headers.items()) + "\r\n"
    with socket.create_connection(("127.0.0.1", port)) as s:
        s.settimeout(10)
        s.sendall(head.encode() + body)
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = s.recv(65536)
            if not chunk:
                break
            data += chunk
        raw_head, _, rest = data.partition(b"\r\n\r\n")
        lines = raw_head.decode("latin-1").split("\r\n")
        fields = {k.strip().lower(): v.strip() for k, v in (ln.split(":", 1) for ln in lines[1:])}
        length = fields.get("content-length")
        while length is None or len(rest) < int(length):
            chunk = s.recv(65536)
            if not chunk:
                break
            rest += chunk
        return int(lines[0].split()[1]), fields, rest


def _framed(data=DATA, **override):
    body = frame(data)
    return framed_headers(body, len(data), **override), body


def _target(kind, upload, number=1, key=KEY):
    if kind == "put":
        return f"/{BUCKET}/{key}"
    return f"/{BUCKET}/{KEY}?partNumber={number}&uploadId={upload}"


def _stored(kind, metadata, storage, upload, number=1, key=KEY):
    meta = (
        metadata.get_object_meta(BUCKET, key)
        if kind == "put"
        else next((p for p in metadata.get_parts(upload) if p.part_number == number), None)
    )
    if meta is None:
        return None
    with storage.open_blob(meta.blob_id) as f:
        return f.read(), meta.etag


def _error(body: bytes) -> tuple[str, str | None]:
    root = ET.fromstring(body)
    return root.findtext("Code"), root.findtext("Header")


@pytest.mark.parametrize("kind", ["put", "part"])
@pytest.mark.parametrize("value", OVERSIZED)
def test_an_oversized_decoded_length_is_answered_as_5_gib_plus_1_is(live, kind, value):
    _, port, upload, metadata, storage = live
    over = str(5 * 1024**3 + 1)
    headers, body = _framed(**{"x-amz-decoded-content-length": over})
    status, _, reference = _exchange(port, _target(kind, upload), headers, body)
    assert (status, _error(reference)) == (400, ("InvalidRequest", "x-amz-decoded-content-length"))

    headers, body = _framed(**{"x-amz-decoded-content-length": value})
    status, _, answer = _exchange(port, _target(kind, upload), headers, body)
    assert (status, _error(answer)) == (400, ("InvalidRequest", "x-amz-decoded-content-length"))
    assert _stored(kind, metadata, storage, upload) is None
    assert metadata.list_blob_records() == []
    assert storage.temp_entries() == []
    assert run_consistency_sweep(metadata, storage).issues == []


@pytest.mark.parametrize("kind", ["put", "part"])
def test_a_decoded_length_with_long_leading_zeros_is_its_value(live, kind):
    _, port, upload, metadata, storage = live
    plain_headers, body = _framed()
    padded = _framed(**{"x-amz-decoded-content-length": "0" * 4990 + str(len(DATA))})[0]
    target = _target(kind, upload, 1, "plain")
    status, fields, _ = _exchange(port, target, plain_headers, body)
    assert status == 200
    status_padded, fields_padded, _ = _exchange(
        port, _target(kind, upload, 2, "padded"), padded, body
    )
    assert status_padded == 200
    assert fields_padded["etag"] == fields["etag"]
    assert _stored(kind, metadata, storage, upload, 1, "plain") == (DATA, fields["etag"])
    assert _stored(kind, metadata, storage, upload, 2, "padded") == (DATA, fields["etag"])


@pytest.mark.parametrize("kind", ["put", "part"])
def test_a_content_length_with_long_leading_zeros_is_its_value(live, kind):
    http, port, upload, metadata, storage = live
    plain_headers, body = _framed()
    padded_headers = {**plain_headers, "content-length": "0" * 4990 + str(len(body))}
    status, fields, _ = _exchange(port, _target(kind, upload, 1, "plain"), plain_headers, body)
    assert status == 200
    status_padded, fields_padded, answer = _exchange(
        port, _target(kind, upload, 2, "padded"), padded_headers, body
    )
    if http == "httptools":
        assert status_padded == 200
        assert fields_padded["etag"] == fields["etag"]
        assert _stored(kind, metadata, storage, upload, 2, "padded") == (DATA, fields["etag"])
    else:
        # h11 parses a Content-Length with int() and refuses this one before Litmus
        # sees the request: its own plain text 400, and the connection is closed.
        assert status_padded == 400
        assert fields_padded["content-type"].startswith("text/plain")
        assert answer == b"Invalid HTTP request received."
        assert _stored(kind, metadata, storage, upload, 2, "padded") is None
        assert storage.temp_entries() == []
        assert run_consistency_sweep(metadata, storage).issues == []
