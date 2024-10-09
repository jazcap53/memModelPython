# test_ajTypes.py
import pytest
import io
from ajTypes import *

def test_u32Const():
    assert u32Const.BLOCK_BYTES.value == 4096
    assert u32Const.BYTES_PER_LINE.value == 64
    assert u32Const.BYTES_PER_PAGE.value == 4096
    assert u32Const.BITS_PER_PAGE.value == 32768
    assert u32Const.CG_LOG_FULL.value == 8192
    assert u32Const.CRC_BYTES.value == 4
    assert u32Const.CT_INODE_BNUMS.value == 9
    assert u32Const.CT_INODE_INDIRECTS.value == 3
    assert u32Const.PAGES_PER_JRNL.value == 16
    assert u32Const.JRNL_SIZE.value == 65536
    assert u32Const.NUM_MEM_SLOTS.value == 32
    assert u32Const.MAX_BLOCKS_PER_FILE.value == 48
    assert u32Const.NUM_FREE_LIST_BLOCKS.value == 1
    assert u32Const.NUM_INODE_TBL_BLOCKS.value == 2
    assert u32Const.NUM_WIPE_PAGES.value == 1

def test_lNum_tConst():
    assert lNum_tConst.INODES_PER_BLOCK.value == 64
    assert lNum_tConst.LINES_PER_PAGE.value == 63

def test_bNum_tConst():
    assert bNum_tConst.NUM_DISK_BLOCKS.value == 256

def test_write_read_64bit():
    file_obj = io.BytesIO()
    value = 0x1234567890abcdef
    write_64bit(file_obj, value)
    file_obj.seek(0)
    assert read_64bit(file_obj) == value

def test_write_read_32bit():
    file_obj = io.BytesIO()
    value = 0x12345678
    write_32bit(file_obj, value)
    file_obj.seek(0)
    assert read_32bit(file_obj) == value

def test_to_from_bytes_64bit():
    value = 0x1234567890abcdef
    assert from_bytes_64bit(to_bytes_64bit(value)) == value

def test_write_64bit_none():
    with pytest.raises(AttributeError):
        write_64bit(None, 0)

def test_read_64bit_none():
    with pytest.raises(AttributeError):
        read_64bit(None)

def test_write_32bit_none():
    with pytest.raises(AttributeError):
        write_32bit(None, 0)

def test_read_32bit_none():
    with pytest.raises(AttributeError):
        read_32bit(None)