"""
ajCrc.py

This module provides CRC (Cyclic Redundancy Check) calculation utilities using zlib's CRC32.
"""

import zlib

class BoostCRC:
    """
    A class that provides methods for CRC calculation and byte manipulation.
    Uses zlib.crc32() which handles pre- and post-conditioning internally.
    """
    polynom = 0x04c11db7  # kept for reference
    init_rem = 0xffffffff  # kept for reference

    @staticmethod
    def get_code(data: bytes, byte_ct: int) -> int:
        """
        Calculate CRC32 of input data.

        Args:
            data: Input bytes
            byte_ct: Number of bytes to process

        Returns:
            32-bit CRC value
        """
        # Use first byte_ct bytes of data
        data = data[:byte_ct]

        # Let zlib handle everything
        return zlib.crc32(data)

    @staticmethod
    def wrt_bytes_little_e(num: int, p: bytearray, byt: int) -> bytearray:
        """
        Write the given number to the bytearray in little-endian format.
        """
        for i in range(byt):
            p[i] = (num >> (i * 8)) & 0xFF
        return p