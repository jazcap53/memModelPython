# tests/test_ajCrc.py
import pytest
from ajCrc import BoostCRC
import zlib

def test_get_code_empty_input():
    assert BoostCRC.get_code(b"", 0) == 0

def test_get_code_small_input():
    data = b"Hello, World!"
    assert BoostCRC.get_code(data, len(data)) == zlib.crc32(data, BoostCRC.init_rem) ^ BoostCRC.init_rem

def test_get_code_large_input():
    data = b"a" * 1024 * 1024  # 1 MiB
    assert BoostCRC.get_code(data, len(data)) == zlib.crc32(data, BoostCRC.init_rem) ^ BoostCRC.init_rem

def test_get_code_different_lengths():
    data = b"Test data"
    for length in range(len(data) + 1):
        assert BoostCRC.get_code(data, length) == zlib.crc32(data[:length], BoostCRC.init_rem) ^ BoostCRC.init_rem

def test_wrt_bytes_little_e_zero():
    buf = bytearray(4)
    BoostCRC.wrt_bytes_little_e(0, buf, 4)
    assert buf == b'\x00\x00\x00\x00'

def test_wrt_bytes_little_e_max_32bit():
    buf = bytearray(4)
    BoostCRC.wrt_bytes_little_e(0xFFFFFFFF, buf, 4)
    assert buf == b'\xFF\xFF\xFF\xFF'

def test_wrt_bytes_little_e_modifies_buffer():
    buf = bytearray(4)
    BoostCRC.wrt_bytes_little_e(0x12345678, buf, 4)
    assert buf == b'\x78\x56\x34\x12'

def test_wrt_bytes_little_e_partial_write():
    buf = bytearray(4)
    BoostCRC.wrt_bytes_little_e(0x12345678, buf, 2)
    assert buf == b'\x78\x56\x00\x00'

def test_wrt_bytes_little_e_overflow():
    buf = bytearray(4)
    BoostCRC.wrt_bytes_little_e(0x1234567890ABCDEF, buf, 4)
    assert buf == b'\xEF\xCD\xAB\x90'