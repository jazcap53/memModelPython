from typing import List, BinaryIO
from enum import Enum
import sys
import struct

# Type aliases
bNum_t = int  # uint32_t
lNum_t = int  # unsigned char
inNum_t = int  # uint32_t

Line = List[int]  # std::array<unsigned char, 64>

# Constants
SENTINEL_32 = sys.maxsize
SENTINEL_INUM = 0xFFFFFFFF
SENTINEL_BNUM = 0xFFFFFFFF  # Maximum value for a 32-bit unsigned integer


class u32Const(Enum):
    BLOCK_BYTES = 4096
    BYTES_PER_LINE = 64
    BYTES_PER_PAGE = BLOCK_BYTES
    BITS_PER_PAGE = BYTES_PER_PAGE * 8
    CG_LOG_FULL = BYTES_PER_PAGE * 2
    CRC_BYTES = 4
    CT_INODE_BNUMS = 9
    CT_INODE_INDIRECTS = 3
    PAGES_PER_JRNL = 16
    JRNL_SIZE = PAGES_PER_JRNL * BYTES_PER_PAGE
    NUM_MEM_SLOTS = 32
    MAX_BLOCKS_PER_FILE = (NUM_MEM_SLOTS * 2) - (NUM_MEM_SLOTS // 2)
    NUM_FREE_LIST_BLOCKS = 1
    NUM_INODE_TBL_BLOCKS = 2
    NUM_WIPE_PAGES = 1


class lNum_tConst(Enum):
    INODES_PER_BLOCK = 64
    LINES_PER_PAGE = 63  # 64th line is empty + crc


class bNum_tConst(Enum):
    NUM_DISK_BLOCKS = 256  # should be a multiple of 8


def write_64bit(file_obj: BinaryIO, value: int) -> None:
    low_32 = value & 0xFFFFFFFF
    high_32 = value >> 32
    file_obj.write(struct.pack('<II', low_32, high_32))


def read_64bit(file_obj: BinaryIO) -> int:
    low_32, high_32 = struct.unpack('<II', file_obj.read(8))
    return (high_32 << 32) | low_32


def write_32bit(file_obj: BinaryIO, value: int) -> None:
    file_obj.write(struct.pack('<I', value))

def read_32bit(file_obj: BinaryIO) -> int:
    return struct.unpack('<I', file_obj.read(4))[0]

def to_bytes_64bit(value: int) -> bytes:
    return struct.pack('<II', value & 0xFFFFFFFF, value >> 32)

def from_bytes_64bit(bytes_value: bytes) -> int:
    low_32, high_32 = struct.unpack('<II', bytes_value)
    return (high_32 << 32) | low_32

class RangedBNum:
    def __init__(self, value: int):
        if value < 0:
            raise ValueError("Block number must be non-negative")
        self.value = value

    def __int__(self) -> int:
        return self.value

class RangedLNum:
    def __init__(self, value: int):
        if not (0 <= value <= 255):
            raise ValueError("Line number must be between 0 and 255 (inclusive)")
        self.value = value

    def __int__(self) -> int:
        return self.value

class RangedInNum:
    def __init__(self, value: int):
        if value < 0:
            raise ValueError("Inode number must be non-negative")
        self.value = value

    def __int__(self) -> int:
        return self.value

class RangedLine:
    def __init__(self, values: List[int]):
        if len(values) != 64:
            raise ValueError("Line must have a length of 64")
        self.values = values

    def __getitem__(self, index: int) -> int:
        return self.values[index]

    def __setitem__(self, index: int, value: int):
        self.values[index] = value

    def __len__(self) -> int:
        return len(self.values)