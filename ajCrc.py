import zlib

class BoostCRC:
    polynom = 0x04c11db7
    init_rem = 0xffffffff

    @staticmethod
    def get_code(data: bytes, byte_ct: int) -> int:
        return zlib.crc32(data[:byte_ct], BoostCRC.init_rem) ^ BoostCRC.init_rem

    @staticmethod
    def wrt_bytes_little_e(num: int, p: bytearray, byt: int) -> None:
        if byt == num.bit_length() // 8 + (1 if num.bit_length() % 8 else 0):
            # If writing to the end of the array, we need to write in reverse order
            if p.startpos > 0:
                for i in range(byt - 1, -1, -1):
                    p[p.startpos + i] = num & 0xFF
                    num >>= 8
            else:
                for i in range(byt):
                    p[i] = num & 0xFF
                    num >>= 8


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