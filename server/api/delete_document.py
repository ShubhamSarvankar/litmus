"""The DeleteObjects request document: validated whole, before anything is deleted.

Shape, from the S3 API reference (DeleteObjects, ObjectIdentifier):

    <Delete>                     root, required; the S3 namespace is optional
      <Object> ...               required, "up to 1,000 keys"
        <Key>                    required
        <VersionId> <ETag> <LastModifiedTime> <Size>      optional
      <Quiet>                    optional, "you must set its value to true"

The reference documents no error code for a document that breaks these rules. Every
refusal here is 400 MalformedXML, whose description is XML "that doesn't conform to the
published XSD" (S3 API reference, Error, Code). docs/compatibility.md lists these as
undocumented choices.
"""

from dataclasses import dataclass
from xml.etree import ElementTree as ET

from server.api.request_xml import parse_request_xml
from server.errors.s3errors import MalformedXML

S3_NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/"

# "The request can contain a list of up to 1,000 keys that you want to delete."
MAX_OBJECTS = 1000

# Per-key conditions an <Object> may carry (ObjectIdentifier). Litmus does not
# evaluate them; the handler answers a per-key NotImplemented error for them.
CONDITIONAL_ELEMENTS = ("ETag", "LastModifiedTime", "Size")


@dataclass(frozen=True)
class DeleteEntry:
    key: str
    version_id: str | None
    conditional: bool


@dataclass(frozen=True)
class DeleteDocument:
    entries: list[DeleteEntry]
    quiet: bool


def _text(element: ET.Element) -> str:
    """The text of a leaf element. An element with a child has no single value: taking
    only the text before the child would name a different key than the client sent."""
    if len(element):
        raise MalformedXML()
    return element.text or ""


def parse_delete_document(body: bytes) -> DeleteDocument:
    """MalformedXML (400) unless the body is a Delete document with 1 to 1000 Objects,
    each holding exactly one Key, and at most one Quiet of "true" or "false".

    Elements are read in the root's namespace (the S3 namespace, or none). Elements
    this parser does not know are ignored, as are repeated VersionId, ETag,
    LastModifiedTime and Size elements past the first. An empty Key is accepted: S3
    gives Key a minimum length of 1, but an object stored under the empty key by an
    older Litmus (docs/compatibility.md, "Objects under the empty key") can be removed
    in no other way."""
    root = parse_request_xml(body)
    if root.tag == "Delete":
        prefix = ""
    elif root.tag == f"{{{S3_NAMESPACE}}}Delete":
        prefix = f"{{{S3_NAMESPACE}}}"
    else:
        raise MalformedXML()

    objects = root.findall(f"{prefix}Object")
    if not objects or len(objects) > MAX_OBJECTS:
        raise MalformedXML()

    quiets = root.findall(f"{prefix}Quiet")
    if len(quiets) > 1:
        raise MalformedXML()
    quiet = False
    if quiets:
        value = _text(quiets[0]).strip()
        if value not in ("true", "false"):
            raise MalformedXML()
        quiet = value == "true"

    entries = []
    for obj in objects:
        keys = obj.findall(f"{prefix}Key")
        if len(keys) != 1:
            raise MalformedXML()
        version = obj.find(f"{prefix}VersionId")
        entries.append(
            DeleteEntry(
                key=_text(keys[0]),
                version_id=None if version is None else _text(version),
                conditional=any(
                    obj.find(f"{prefix}{tag}") is not None for tag in CONDITIONAL_ELEMENTS
                ),
            )
        )
    return DeleteDocument(entries, quiet)
