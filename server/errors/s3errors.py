import xml.etree.ElementTree as ET
from typing import ClassVar

from server.request_ids import current_request_ids


class S3Error(Exception):
    status_code: ClassVar[int] = 500
    code: ClassVar[str] = "InternalError"
    message: ClassVar[str] = "We encountered an internal error. Please try again."

    def __init__(self, extra: dict | None = None, headers: dict | None = None):
        self.extra = extra or {}
        self.headers = headers or {}
        super().__init__(self.message)


class AccessDenied(S3Error):
    status_code = 403
    code = "AccessDenied"
    message = "Access Denied"


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


class InvalidPartOrder(S3Error):
    status_code = 400
    code = "InvalidPartOrder"
    message = (
        "The list of parts was not in ascending order. The parts list must be specified "
        "in order by part number."
    )


class EntityTooSmall(S3Error):
    status_code = 400
    code = "EntityTooSmall"
    message = "Your proposed upload is smaller than the minimum allowed object size"


class PreconditionFailed(S3Error):
    status_code = 412
    code = "PreconditionFailed"
    message = "At least one of the pre-conditions you specified did not hold"


class InvalidArgument(S3Error):
    status_code = 400
    code = "InvalidArgument"
    message = "Invalid Argument"


class InvalidContinuationToken(InvalidArgument):
    message = "The continuation token provided is incorrect"


class InvalidEncodingType(InvalidArgument):
    message = "Invalid Encoding Method specified in Request"


class InvalidBucketName(S3Error):
    status_code = 400
    code = "InvalidBucketName"
    message = "The specified bucket is not valid."


class AccessControlListNotSupported(S3Error):
    # S3 User Guide, "Troubleshooting" (Object Ownership error responses). The code is not
    # in the S3 API error code list.
    status_code = 400
    code = "AccessControlListNotSupported"
    message = "The bucket does not allow ACLs"


class InvalidBucketAclWithObjectOwnership(S3Error):
    # Same page, same caveat: the message is the one it shows for this code.
    status_code = 400
    code = "InvalidBucketAclWithObjectOwnership"
    message = "Bucket cannot have ACLs set with ObjectOwnership's BucketOwnerEnforced setting"


class BadDigest(S3Error):
    status_code = 400
    code = "BadDigest"
    message = "The checksum you specified did not match what we received."


class InvalidDigest(S3Error):
    status_code = 400
    code = "InvalidDigest"
    message = "The checksum you specified is not valid."


class MetadataTooLarge(S3Error):
    status_code = 400
    code = "MetadataTooLarge"
    message = "Your metadata headers exceed the maximum allowed metadata size."


class IncompleteBody(S3Error):
    status_code = 400
    code = "IncompleteBody"
    message = "You did not provide the number of bytes specified by the Content-Length HTTP header"


class MissingContentLength(S3Error):
    status_code = 411
    code = "MissingContentLength"
    message = "You must provide the Content-Length HTTP header."


class NotImplementedOperation(S3Error):
    # Named to avoid shadowing the NotImplemented builtin; the S3 code is NotImplemented.
    status_code = 501
    code = "NotImplemented"
    message = "A header you provided implies functionality that is not implemented"


class InvalidURI(S3Error):
    status_code = 400
    code = "InvalidURI"
    message = "Couldn't parse the specified URI."


class MalformedXML(S3Error):
    status_code = 400
    code = "MalformedXML"
    message = (
        "The XML you provided was not well-formed or did not validate against our published schema"
    )


class InvalidRequest(S3Error):
    status_code = 400
    code = "InvalidRequest"
    message = "The request is not valid."


class CopyOntoItself(InvalidRequest):
    message = (
        "This copy request is illegal because it is trying to copy an object to itself "
        "without changing the object's metadata, storage class, website redirect location "
        "or encryption attributes."
    )


class MethodNotAllowed(S3Error):
    status_code = 405
    code = "MethodNotAllowed"
    message = "The specified method is not allowed against this resource."


class InvalidRange(S3Error):
    status_code = 416
    code = "InvalidRange"
    message = "The requested range cannot be satisfied."


class InternalError(S3Error):
    status_code = 500
    code = "InternalError"
    message = "We encountered an internal error. Please try again."


class SlowDown(S3Error):
    status_code = 503
    code = "SlowDown"
    message = "Please reduce your request rate."


def render_error(error: S3Error) -> str:
    root = ET.Element("Error")

    ET.SubElement(root, "Code").text = error.code
    ET.SubElement(root, "Message").text = error.message

    for key, value in error.extra.items():
        ET.SubElement(root, key).text = str(value)

    # The same pair the response carries in x-amz-request-id and x-amz-id-2.
    ids = current_request_ids()
    ET.SubElement(root, "RequestId").text = ids.request_id
    ET.SubElement(root, "HostId").text = ids.host_id

    return ET.tostring(root, encoding="unicode", xml_declaration=True)
