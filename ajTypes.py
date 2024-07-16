from typing import List
from enum import Enum
import sys

# Type aliases
bNum_t = int  # uint32_t
lNum_t = int  # unsigned char
inNum_t = int  # uint32_t

Line = List[int]  # std::array<unsigned char, 64>

# Constants
SENTINEL_32 = sys.maxsize
SENTINEL_INUM = sys.maxsize
SENTINEL_BNUM = sys.maxsize

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