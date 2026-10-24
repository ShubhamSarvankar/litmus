"""server/crc64nvme.py: awscrt's incremental CRC64NVME against known values and
against an independent pure Python implementation (kept here, never in the server)."""

import base64
import random

import pytest

from server.crc64nvme import Crc64Nvme, decode_b64_digest

# CRC-64/NVME: poly 0xAD93D23594C93659, reflected in and out, init and xorout all ones.
_REFLECTED_POLY = 0x9A6C9329AC4BC9B5


def _table() -> list[int]:
    table = []
    for n in range(256):
        crc = n
        for _ in range(8):
            crc = (crc >> 1) ^ _REFLECTED_POLY if crc & 1 else crc >> 1
        table.append(crc)
    return table


_TABLE = _table()


def pure_crc64nvme(data: bytes) -> int:
    crc = 0xFFFFFFFFFFFFFFFF
    for byte in data:
        crc = _TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFFFFFFFFFF


def _one_shot(data: bytes) -> Crc64Nvme:
    crc = Crc64Nvme()
    crc.update(data)
    return crc


def test_catalogue_check_value():
    assert pure_crc64nvme(b"123456789") == 0xAE8B14860A799888
    assert int.from_bytes(_one_shot(b"123456789").digest(), "big") == 0xAE8B14860A799888


def test_empty_input_is_zero():
    assert Crc64Nvme().digest() == bytes(8)
    assert pure_crc64nvme(b"") == 0


def test_1024_a_matches_ceph_s3_tests():
    assert _one_shot(b"A" * 1024).b64digest() == "Qeh8oXvGiSo="


@pytest.mark.parametrize("seed", range(20))
def test_incremental_over_random_splits_equals_one_shot(seed: int):
    rng = random.Random(seed)
    data = rng.randbytes(rng.randrange(0, 5000))
    crc = Crc64Nvme()
    pos = 0
    while pos < len(data):
        step = rng.randrange(0, 700)  # includes empty updates
        crc.update(data[pos : pos + step])
        pos += step
    crc.update(b"")
    assert crc.digest() == _one_shot(data).digest()


@pytest.mark.parametrize("seed", range(20))
def test_awscrt_agrees_with_pure_python_on_random_buffers(seed: int):
    rng = random.Random(1000 + seed)
    data = rng.randbytes(rng.randrange(0, 4000))
    assert _one_shot(data).digest() == pure_crc64nvme(data).to_bytes(8, "big")


def test_b64digest_is_base64_of_big_endian_crc():
    crc = _one_shot(b"123456789")
    assert base64.b64decode(crc.b64digest()) == bytes.fromhex("ae8b14860a799888")


@pytest.mark.parametrize(
    "value",
    ["", "Qeh8oXvGiSo", "Qeh8oXvGiSo==", "Qeh8oXvGiSo=\n", "Qeh8oXv GiSo=", "!!!!!!!!!!!=", "QUJD"],
)
def test_decode_rejects_anything_but_strict_base64_of_8_bytes(value: str):
    assert decode_b64_digest(value) is None


def test_decode_accepts_the_wire_form():
    assert decode_b64_digest("Qeh8oXvGiSo=") == base64.b64decode("Qeh8oXvGiSo=")
