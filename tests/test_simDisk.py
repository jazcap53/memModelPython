# test_simDisk.py

import os
import pytest
from simDisk import SimDisk
from ajTypes import u32Const, bNum_tConst


class MockStatus:
    def wrt(self, msg):
        print(f"Mock Status: {msg}")


@pytest.fixture
def temp_files(tmpdir):
    disk_file = tmpdir.join("test_disk.bin")
    jrnl_file = tmpdir.join("test_jrnl.bin")
    free_file = tmpdir.join("test_free.bin")
    node_file = tmpdir.join("test_node.bin")
    return str(disk_file), str(jrnl_file), str(free_file), str(node_file)


def test_simDisk_initialization(temp_files):
    disk_file, jrnl_file, free_file, node_file = temp_files
    status = MockStatus()

    sim_disk = SimDisk(status, disk_file, jrnl_file, free_file, node_file)

    assert os.path.exists(disk_file)
    assert os.path.exists(jrnl_file)
    assert os.path.exists(free_file)
    assert os.path.exists(node_file)

    assert os.path.getsize(disk_file) == u32Const.BLOCK_BYTES.value * bNum_tConst.NUM_DISK_BLOCKS.value
    assert os.path.getsize(jrnl_file) == u32Const.BLOCK_BYTES.value * u32Const.PAGES_PER_JRNL.value


def test_create_block():
    # Test with zero-filled block
    zero_block = bytearray(u32Const.BLOCK_BYTES.value)
    SimDisk.create_block(zero_block, u32Const.BLOCK_BYTES.value)
    assert zero_block[-4:] != b'\x00\x00\x00\x00', "CRC of zero-filled block should not be zero"

    # Test with non-zero data
    data_block = bytearray(u32Const.BLOCK_BYTES.value)
    for i in range(len(data_block) - u32Const.CRC_BYTES.value):
        data_block[i] = i % 256
    SimDisk.create_block(data_block, u32Const.BLOCK_BYTES.value)

    # Ensure CRCs are different
    assert zero_block[-4:] != data_block[-4:], "CRCs of different data should be different"

    # Verify CRC is written and non-zero for data block
    assert data_block[-4:] != b'\x00\x00\x00\x00', "CRC of non-zero block should not be zero"


def test_error_scanning(temp_files):
    disk_file, jrnl_file, free_file, node_file = temp_files
    status = MockStatus()

    # Create a disk file with a known error
    with open(disk_file, 'wb') as f:
        block = bytearray(u32Const.BLOCK_BYTES.value)
        block[-4:] = b'\xFF\xFF\xFF\xFF'  # Invalid CRC
        f.write(block)
        f.write(b'\x00' * (u32Const.BLOCK_BYTES.value * (bNum_tConst.NUM_DISK_BLOCKS.value - 1)))

    sim_disk = SimDisk(status, disk_file, jrnl_file, free_file, node_file)

    assert len(sim_disk.errBlocks) == 1
    assert sim_disk.errBlocks[0] == 0  # Error in the first block