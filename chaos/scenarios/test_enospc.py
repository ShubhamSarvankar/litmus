"""
Tier A chaos: ENOSPC injected at the Python layer.

Invariants under test:
- A failed PUT must not leave a partial object visible.
- A failed CompleteMultipartUpload must not make the object visible.
- Parts remain intact after a failed assembly (upload not aborted).
"""

import errno
import xml.etree.ElementTree as ET
from pathlib import Path

from chaos.faults.faults import RaiseOSError
from chaos.fixtures.inprocess import make_faulty_server
from chaos.framework.injector import FaultInjector
from chaos.framework.triggers import CallCountTrigger


def _uid(resp) -> str:
    return ET.fromstring(resp.text).findtext("UploadId")


def _complete_xml(parts: list[tuple[int, str]]) -> bytes:
    root = ET.Element("CompleteMultipartUpload")
    for num, etag in parts:
        part = ET.SubElement(root, "Part")
        ET.SubElement(part, "PartNumber").text = str(num)
        ET.SubElement(part, "ETag").text = etag
    return ET.tostring(root)


# --- ENOSPC mid single-part PUT ---


def test_enospc_put_returns_error(tmp_path: Path):
    injector = FaultInjector(
        method="write_object",
        trigger=CallCountTrigger(fire_on=1),
        fault=RaiseOSError(errno.ENOSPC),
    )
    client, _, _, _ = make_faulty_server(tmp_path, injector)
    with client:
        client.put("/bucket")
        resp = client.put("/bucket/key.txt", content=b"hello world")
    assert resp.status_code in (500, 503)


def test_enospc_put_leaves_no_partial_object(tmp_path: Path):
    """After ENOSPC on write_object, GET must return 404."""
    injector = FaultInjector(
        method="write_object",
        trigger=CallCountTrigger(fire_on=1),
        fault=RaiseOSError(errno.ENOSPC),
    )
    client, _, _, _ = make_faulty_server(tmp_path, injector)
    with client:
        client.put("/bucket")
        client.put("/bucket/key.txt", content=b"hello world")
        resp = client.get("/bucket/key.txt")
    assert resp.status_code == 404


def test_enospc_second_put_succeeds(tmp_path: Path):
    """First write_object call fails; second (different key) succeeds normally."""
    injector = FaultInjector(
        method="write_object",
        trigger=CallCountTrigger(fire_on=1),
        fault=RaiseOSError(errno.ENOSPC),
    )
    client, _, _, _ = make_faulty_server(tmp_path, injector)
    with client:
        client.put("/bucket")
        client.put("/bucket/fail.txt", content=b"will fail")
        resp = client.put("/bucket/ok.txt", content=b"will succeed")
    assert resp.status_code == 200


# --- ENOSPC mid multipart assembly ---


def test_enospc_assemble_returns_error(tmp_path: Path):
    injector = FaultInjector(
        method="assemble_parts",
        trigger=CallCountTrigger(fire_on=1),
        fault=RaiseOSError(errno.ENOSPC),
    )
    client, _, _, _ = make_faulty_server(tmp_path, injector)
    with client:
        client.put("/bucket")
        uid = _uid(client.post("/bucket/obj?uploads"))
        resp1 = client.put(f"/bucket/obj?partNumber=1&uploadId={uid}", content=b"part-one")
        complete = _complete_xml([(1, resp1.headers["etag"])])
        resp = client.post(f"/bucket/obj?uploadId={uid}", content=complete)
    assert resp.status_code in (400, 500, 503)


def test_enospc_assemble_object_not_visible(tmp_path: Path):
    """After failed assembly, object must not appear in GET."""
    injector = FaultInjector(
        method="assemble_parts",
        trigger=CallCountTrigger(fire_on=1),
        fault=RaiseOSError(errno.ENOSPC),
    )
    client, _, _, _ = make_faulty_server(tmp_path, injector)
    with client:
        client.put("/bucket")
        uid = _uid(client.post("/bucket/obj?uploads"))
        resp1 = client.put(f"/bucket/obj?partNumber=1&uploadId={uid}", content=b"part-one")
        complete = _complete_xml([(1, resp1.headers["etag"])])
        client.post(f"/bucket/obj?uploadId={uid}", content=complete)
        resp = client.get("/bucket/obj")
    assert resp.status_code == 404


def test_enospc_assemble_parts_still_recorded(tmp_path: Path):
    """After failed assembly, the upload_id must still be incomplete (parts not destroyed)."""
    injector = FaultInjector(
        method="assemble_parts",
        trigger=CallCountTrigger(fire_on=1),
        fault=RaiseOSError(errno.ENOSPC),
    )
    client, _, metadata, _ = make_faulty_server(tmp_path, injector)
    with client:
        client.put("/bucket")
        uid = _uid(client.post("/bucket/obj?uploads"))
        resp1 = client.put(f"/bucket/obj?partNumber=1&uploadId={uid}", content=b"part-one")
        complete = _complete_xml([(1, resp1.headers["etag"])])
        client.post(f"/bucket/obj?uploadId={uid}", content=complete)

    # Upload must still be incomplete (not aborted) and parts still recorded
    assert metadata.upload_exists(uid)
    parts = metadata.get_parts(uid)
    assert len(parts) == 1
