import xml.etree.ElementTree as ET
from typing import ClassVar


class S3Error(Exception):
    status_code: ClassVar[int] = 500
    code: ClassVar[str] = "InternalError"
    message: ClassVar[str] = "We encountered an internal error. Please try again."

    def __init__(self, extra: dict | None = None):
        self.extra = extra or {}
        super().__init__(self.message)


class NoSuchBucket(S3Error):
    status_code = 404
    code = "NoSuchBucket"
    message = "The specified bucket does not exist"


class BucketAlreadyExists(S3Error):
    status_code = 409
    code = "BucketAlreadyExists"
    message = "The requested bucket name is not available"


class BucketNotEmpty(S3Error):
    status_code = 409
    code = "BucketNotEmpty"
    message = "The bucket you tried to delete is not empty"


class NoSuchKey(S3Error):
    status_code = 404
    code = "NoSuchKey"
    message = "The specified key does not exist"


class NoSuchUpload(S3Error):
    status_code = 404
    code = "NoSuchUpload"
    message = "The specified upload does not exist"


class InvalidPart(S3Error):
    status_code = 400
    code = "InvalidPart"
    message = "One or more of the specified parts could not be found"


class EntityTooSmall(S3Error):
    status_code = 400
    code = "EntityTooSmall"
    message = "Your proposed upload is smaller than the minimum allowed object size"


class PreconditionFailed(S3Error):
    status_code = 412
    code = "PreconditionFailed"
    message = "At least one of the pre-conditions you specified did not hold"


class InternalError(S3Error):
    status_code = 500
    code = "InternalError"
    message = "We encountered an internal error. Please try again."


# Counter for generating sequential request IDs within a process lifetime
_request_counter = 0


def _next_request_id() -> str:
    global _request_counter
    _request_counter += 1
    return f"tx-{_request_counter:07d}"


def render_error(error: S3Error) -> str:
    root = ET.Element("Error")

    ET.SubElement(root, "Code").text = error.code
    ET.SubElement(root, "Message").text = error.message

    for key, value in error.extra.items():
        ET.SubElement(root, key).text = str(value)

    ET.SubElement(root, "RequestId").text = _next_request_id()

    return ET.tostring(root, encoding="unicode", xml_declaration=True)
