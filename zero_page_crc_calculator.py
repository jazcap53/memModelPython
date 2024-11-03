from ajTypes import u32Const
from ajCrc import AJZlibCRC
from myMemory import Page
from ajUtils import format_hex_like_hexdump
import struct


def calculate_zero_page_crc():
    # Create a Page of zero bytes
    zero_page = Page()

    # Calculate CRC
    crc = AJZlibCRC.get_code(zero_page.dat, u32Const.BYTES_PER_PAGE.value)

    print(f"CRC of zero page (hex): {crc:08x}")
    print(f"CRC of zero page (decimal): {crc}")

    # Convert CRC to bytes and format as hexdump
    crc_bytes = struct.pack('<I', crc)  # Little-endian 32-bit integer
    print(f"CRC of zero page (hexdump format): {format_hex_like_hexdump(crc_bytes)}")


if __name__ == "__main__":
    calculate_zero_page_crc()