"""
ajCrc.py

This module provides CRC (Cyclic Redundancy Check) calculation utilities using the Boost CRC algorithm.
"""

import zlib


class BoostCRC:
    """
    A class that provides methods for CRC calculation and byte manipulation using the Boost CRC algorithm.
    """

    polynom = 0x04c11db7
    init_rem = 0xffffffff

    @staticmethod
    def get_code(data: bytes, byte_ct: int) -> int:
        """
        Calculate the CRC code for the given data using the Boost CRC algorithm.

        Args:
            data (bytes): The data to calculate the CRC for.
            byte_ct (int): The number of bytes to include in the CRC calculation.

        Returns:
            int: The calculated CRC code.
        """
        return zlib.crc32(data[:byte_ct], BoostCRC.init_rem) ^ BoostCRC.init_rem

    @staticmethod
    def wrt_bytes_little_e(num: int, p: bytearray, byt: int) -> bytearray:
        """
        Write the given number to the bytearray in little-endian format.

        Args:
            num (int): The number to write.
            p (bytearray): The bytearray to write to.
            byt (int): The number of bytes to write.

        Returns:
            bytearray: The modified bytearray.
        """
        # Debug: Print input values
        print(f"Writing CRC: num={num:08x}, byt={byt}, target={p.hex()}")

        for i in range(byt):
            p[i] = (num >> (i * 8)) & 0xFF  # Change this line

        # Debug: Print result
        print(f"After writing: {p.hex()}")

        return p


if __name__ == '__main__':
    # Test the CRC functionality
    test_data = b"Hello, World!"
    crc = BoostCRC.get_code(test_data, len(test_data))
    print(f"CRC of '{test_data.decode()}': {crc:08x}")

    # Test little-endian writing
    test_num = 0x12345678
    test_array = bytearray(4)
    BoostCRC.wrt_bytes_little_e(test_num, test_array, 4)
    print(f"Little-endian representation of 0x{test_num:08x}: {test_array.hex()}")