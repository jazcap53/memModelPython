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


def test_crc_initialization():
    """Test that our constants match Boost's."""
    assert BoostCRC.polynom == 0x04c11db7
    assert BoostCRC.init_rem == 0xffffffff


def test_crc_of_empty():
    """Test CRC of empty input."""
    result = BoostCRC.get_code(b"", 0)
    assert result == 0


def test_crc_of_zeros():
    """Test CRC of zero-filled block."""
    # Test various sizes of zero blocks
    sizes = [1, 64, 4092]  # including our typical block size minus CRC
    for size in sizes:
        zero_block = bytes(size)  # Create block of zeros
        result = BoostCRC.get_code(zero_block, size)
        assert result != 0, f"CRC of {size} zeros should not be zero"


def test_crc_different_sizes():
    """Test that different-sized zero blocks have different CRCs."""
    crc1 = BoostCRC.get_code(bytes(100), 100)
    crc2 = BoostCRC.get_code(bytes(200), 200)
    assert crc1 != crc2, "Different-sized zero blocks should have different CRCs"


def test_byte_writing():
    """Test little-endian byte writing."""
    test_cases = [
        (0x12345678, 4),
        (0x0000FF00, 4),
        (0xFF, 1),
    ]

    for value, size in test_cases:
        buf = bytearray(size)
        BoostCRC.wrt_bytes_little_e(value, buf, size)

        # Read back and verify
        result = 0
        for i in range(size):
            result |= buf[i] << (i * 8)

        assert result == value, f"Write/read mismatch for value 0x{value:x}"


def test_crc_consistency():
    """Test that same input always produces same CRC."""
    data = b"test data"
    crc1 = BoostCRC.get_code(data, len(data))
    crc2 = BoostCRC.get_code(data, len(data))
    assert crc1 == crc2, "Same input should produce same CRC"


def test_crc_different_data():
    """Test that different data produces different CRCs."""
    data1 = b"test data 1"
    data2 = b"test data 2"
    crc1 = BoostCRC.get_code(data1, len(data1))
    crc2 = BoostCRC.get_code(data2, len(data2))
    assert crc1 != crc2, "Different input should produce different CRCs"


def test_large_block_crc():
    """Test CRC calculation on a block size we actually use."""
    block_size = 4092  # size we use in SimDisk (BLOCK_BYTES - CRC_BYTES)
    data = bytes(range(block_size))  # non-zero data
    result = BoostCRC.get_code(data, block_size)
    assert result != 0, "CRC of non-zero block should not be zero"


def test_byte_writing_bounds():
    """Test byte writing doesn't write beyond bounds."""
    buf = bytearray(4)
    original = buf.copy()

    # Write only 2 bytes
    BoostCRC.wrt_bytes_little_e(0xABCD, buf, 2)

    # Check only first 2 bytes were modified
    assert buf[2:] == original[2:], "Should not modify bytes beyond specified size"