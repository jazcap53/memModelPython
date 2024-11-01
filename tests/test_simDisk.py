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
    test_block = bytearray(u32Const.BLOCK_BYTES.value)
    SimDisk.create_block(test_block, u32Const.BLOCK_BYTES.value)

    # Verify CRC is written
    assert test_block[-4:] != b'\x00\x00\x00\x00'


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