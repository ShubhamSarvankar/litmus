"""Parsing of XML request bodies."""

import xml.etree.ElementTree as ET

from server.errors.s3errors import MalformedXML


def parse_request_xml(body: bytes) -> ET.Element:
    """The root element of an XML request body; MalformedXML (400) when the body is
    not well-formed XML 1.0. That covers an empty body, markup that does not nest,
    a reference to a character XML 1.0 forbids (&#0; or &#xD800;, XML 1.0 section
    4.1, "Legal Character"), an undefined entity, entity expansion beyond the
    parser's limits, and bytes that are not UTF-8. The body is decoded as UTF-8
    first, so an encoding declaration in it changes nothing."""
    try:
        return ET.fromstring(body.decode())
    except (UnicodeDecodeError, ET.ParseError):
        raise MalformedXML() from None
