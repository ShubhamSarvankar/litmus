import xml.etree.ElementTree as ET

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.errors.s3errors import (
    BucketAlreadyExists,
    BucketNotEmpty,
    EntityTooSmall,
    InternalError,
    InvalidPart,
    NoSuchBucket,
    NoSuchKey,
    NoSuchUpload,
    PreconditionFailed,
    S3Error,
    render_error,
)

# --- render_error: XML structure ---


def _parse(xml_str: str) -> ET.Element:
    return ET.fromstring(xml_str)


def test_render_error_root_tag():
    root = _parse(render_error(NoSuchBucket()))
    assert root.tag == "Error"


def test_render_error_code():
    root = _parse(render_error(NoSuchBucket()))
    assert root.findtext("Code") == "NoSuchBucket"


def test_render_error_message():
    root = _parse(render_error(NoSuchBucket()))
    assert root.findtext("Message") == "The specified bucket does not exist"


def test_render_error_has_request_id():
    root = _parse(render_error(NoSuchBucket()))
    assert root.findtext("RequestId") is not None


def test_render_error_request_id_increments():
    xml1 = render_error(NoSuchBucket())
    xml2 = render_error(NoSuchBucket())
    id1 = _parse(xml1).findtext("RequestId")
    id2 = _parse(xml2).findtext("RequestId")
    assert id1 != id2


def test_render_error_extra_fields():
    err = NoSuchBucket(extra={"BucketName": "my-bucket"})
    root = _parse(render_error(err))
    assert root.findtext("BucketName") == "my-bucket"


def test_render_error_multiple_extra_fields():
    err = NoSuchKey(extra={"BucketName": "b", "Key": "k"})
    root = _parse(render_error(err))
    assert root.findtext("BucketName") == "b"
    assert root.findtext("Key") == "k"


def test_render_error_no_extra_fields_by_default():
    root = _parse(render_error(NoSuchBucket()))
    # Only Code, Message, RequestId should be present
    tags = {child.tag for child in root}
    assert tags == {"Code", "Message", "RequestId"}


def test_render_error_xml_declaration():
    xml_str = render_error(NoSuchBucket())
    assert xml_str.startswith("<?xml")


# --- Status codes for every defined error class ---


@pytest.mark.parametrize(
    "cls,expected_status",
    [
        (NoSuchBucket, 404),
        (BucketAlreadyExists, 409),
        (BucketNotEmpty, 409),
        (NoSuchKey, 404),
        (NoSuchUpload, 404),
        (InvalidPart, 400),
        (EntityTooSmall, 400),
        (PreconditionFailed, 412),
        (InternalError, 500),
    ],
)
def test_error_status_codes(cls, expected_status):
    assert cls.status_code == expected_status


@pytest.mark.parametrize(
    "cls,expected_code",
    [
        (NoSuchBucket, "NoSuchBucket"),
        (BucketAlreadyExists, "BucketAlreadyExists"),
        (BucketNotEmpty, "BucketNotEmpty"),
        (NoSuchKey, "NoSuchKey"),
        (NoSuchUpload, "NoSuchUpload"),
        (InvalidPart, "InvalidPart"),
        (EntityTooSmall, "EntityTooSmall"),
        (PreconditionFailed, "PreconditionFailed"),
        (InternalError, "InternalError"),
    ],
)
def test_error_codes(cls, expected_code):
    root = _parse(render_error(cls()))
    assert root.findtext("Code") == expected_code


# --- Exception handler integration ---


def _make_test_app(error: S3Error):
    """Build a minimal app with just the S3 error handler — no bucket/object routes."""
    from fastapi import FastAPI, Request
    from fastapi.responses import Response

    minimal = FastAPI()

    @minimal.exception_handler(S3Error)
    async def s3_error_handler(request: Request, exc: S3Error) -> Response:
        return Response(
            content=render_error(exc),
            status_code=exc.status_code,
            media_type="application/xml",
        )

    @minimal.get("/test-error")
    async def trigger():
        raise error

    return minimal


def test_handler_returns_xml_content_type():
    client = TestClient(_make_test_app(NoSuchBucket()), raise_server_exceptions=False)
    response = client.get("/test-error")
    assert response.headers["content-type"] == "application/xml"


def test_handler_returns_correct_status_code():
    client = TestClient(_make_test_app(NoSuchBucket()), raise_server_exceptions=False)
    response = client.get("/test-error")
    assert response.status_code == 404


def test_handler_body_is_valid_xml():
    client = TestClient(_make_test_app(BucketAlreadyExists()), raise_server_exceptions=False)
    response = client.get("/test-error")
    root = ET.fromstring(response.text)
    assert root.tag == "Error"
    assert root.findtext("Code") == "BucketAlreadyExists"


def test_handler_500_for_internal_error():
    client = TestClient(_make_test_app(InternalError()), raise_server_exceptions=False)
    response = client.get("/test-error")
    assert response.status_code == 500


def test_handler_extra_fields_in_response():
    err = NoSuchBucket(extra={"BucketName": "missing-bucket"})
    client = TestClient(_make_test_app(err), raise_server_exceptions=False)
    response = client.get("/test-error")
    root = ET.fromstring(response.text)
    assert root.findtext("BucketName") == "missing-bucket"


def test_health_still_works_alongside_handler():
    """Registering the error handler must not break existing routes."""
    client = TestClient(create_app())
    response = client.get("/health")
    assert response.status_code == 200
