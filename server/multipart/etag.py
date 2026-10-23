import hashlib


def composite_etag(part_etags: list[str]) -> str:
    """S3 multipart ETag: MD5 of the concatenated binary part MD5s, then -<count>."""
    raw = b"".join(bytes.fromhex(e.strip('"')) for e in part_etags)
    return f'"{hashlib.md5(raw).hexdigest()}-{len(part_etags)}"'
