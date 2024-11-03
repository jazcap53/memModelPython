# test_simDisk.py

import os
import pytest
from simDisk import SimDisk
from ajTypes import u32Const, bNum_tConst
from ajCrc import BoostCRC


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
    zero_block = bytearray(u32Const.BLOCK_BYTES.value)  # 4096 bytes
    SimDisk.create_block(zero_block, u32Const.BLOCK_BYTES.value)

    # Get the CRC that was written to the last 4 bytes
    written_crc = int.from_bytes(zero_block[-4:], 'little')

    # Calculate CRC manually over the first 4092 bytes to verify
    manual_crc = BoostCRC.get_code(
        zero_block[:-u32Const.CRC_BYTES.value],
        u32Const.BLOCK_BYTES.value - u32Const.CRC_BYTES.value
    )

    # The CRCs should match
    assert written_crc == manual_crc, "Written CRC should match manually calculated CRC"

    # Test with non-zero data
    data_block = bytearray(u32Const.BLOCK_BYTES.value)
    for i in range(len(data_block) - u32Const.CRC_BYTES.value):
        data_block[i] = i % 256
    SimDisk.create_block(data_block, u32Const.BLOCK_BYTES.value)

    # Get the CRC written to the non-zero block
    data_crc = int.from_bytes(data_block[-4:], 'little')

    # CRCs of zero block and non-zero block should differ
    assert written_crc != data_crc, "CRC of zero block should differ from CRC of non-zero block"


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