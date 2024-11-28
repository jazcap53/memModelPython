# test_simDisk.py

import os
import pytest
import struct
from simDisk import SimDisk
from ajTypes import u32Const, bNum_tConst, lNum_tConst
from ajCrc import AJZlibCRC
import logging


logging.basicConfig(level=logging.WARNING)


class MockStatus:
    def wrt(self, msg):
        logging.info(f"Mock Status: {msg}")


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
    manual_crc = AJZlibCRC.get_code(
        zero_block[:-u32Const.CRC_BYTES.value],
        u32Const.BLOCK_BYTES.value - u32Const.CRC_BYTES.value
    )

    # The CRCs should match, and neither should be zero
    assert written_crc == manual_crc, "Written CRC should match manually calculated CRC"
    assert written_crc != 0, "CRC of a zero block should not be zero"


def test_error_scanning(temp_files):
    disk_file, jrnl_file, free_file, node_file = temp_files
    status = MockStatus()

    # Create a disk file with a known error
    with open(disk_file, 'wb') as f:
        # First block: invalid CRC
        block = bytearray(u32Const.BLOCK_BYTES.value)
        # Calculate correct CRC
        crc = AJZlibCRC.get_code(
            block[:-u32Const.CRC_BYTES.value],
            u32Const.BLOCK_BYTES.value - u32Const.CRC_BYTES.value
        )
        # Write wrong CRC (just flip all bits)
        wrong_crc = (~crc) & 0xFFFFFFFF
        AJZlibCRC.wrt_bytes_little_e(
            wrong_crc,
            block[-u32Const.CRC_BYTES.value:],
            u32Const.CRC_BYTES.value
        )
        f.write(block)

        # Rest of blocks: create using SimDisk.create_block to ensure valid CRC
        for _ in range(bNum_tConst.NUM_DISK_BLOCKS.value - 1):
            rest_block = bytearray(u32Const.BLOCK_BYTES.value)
            SimDisk.create_block(rest_block, u32Const.BLOCK_BYTES.value)
            f.write(rest_block)

    sim_disk = SimDisk(status, disk_file, jrnl_file, free_file, node_file)

    assert len(sim_disk.errBlocks) == 1, f"Expected 1 error block, got {len(sim_disk.errBlocks)}"
    assert sim_disk.errBlocks[0] == 0, f"Expected error in block 0, got {sim_disk.errBlocks}"


def test_multiple_block_creation(temp_files):
    """Test creating multiple blocks with different data."""
    disk_file, jrnl_file, free_file, node_file = temp_files
    status = MockStatus()
    sim_disk = SimDisk(status, disk_file, jrnl_file, free_file, node_file)

    # Create blocks with different patterns
    for i in range(5):  # Test first 5 blocks
        block = sim_disk.theDisk[i].sect
        for j in range(len(block) - u32Const.CRC_BYTES.value):
            block[j] = (i + j) % 256  # Create a unique pattern for each block
        SimDisk.create_block(block, u32Const.BLOCK_BYTES.value)

        # Verify CRC
        calculated_crc = AJZlibCRC.get_code(
            block[:-u32Const.CRC_BYTES.value],
            u32Const.BLOCK_BYTES.value - u32Const.CRC_BYTES.value
        )
        stored_crc = int.from_bytes(block[-u32Const.CRC_BYTES.value:], 'little')
        assert calculated_crc == stored_crc, f"CRC mismatch for block {i}"


def test_file_creation_sizes(temp_files):
    """Test that all created files have the correct sizes."""
    disk_file, jrnl_file, free_file, node_file = temp_files
    status = MockStatus()
    sim_disk = SimDisk(status, disk_file, jrnl_file, free_file, node_file)

    assert os.path.getsize(disk_file) == u32Const.BLOCK_BYTES.value * bNum_tConst.NUM_DISK_BLOCKS.value
    assert os.path.getsize(jrnl_file) == u32Const.BLOCK_BYTES.value * u32Const.PAGES_PER_JRNL.value

    # Additional checks for free file and node file sizes
    expected_free_size = u32Const.BLOCK_BYTES.value * u32Const.NUM_FREE_LIST_BLOCKS.value * 2 + 4
    expected_node_size = (u32Const.NUM_INODE_TBL_BLOCKS.value *
                          lNum_tConst.INODES_PER_BLOCK.value // 8) + (
                                 u32Const.BLOCK_BYTES.value * u32Const.NUM_INODE_TBL_BLOCKS.value)

    assert os.path.getsize(
        free_file) == expected_free_size, f"Free file size mismatch: expected {expected_free_size}, got {os.path.getsize(free_file)}"
    assert os.path.getsize(
        node_file) == expected_node_size, f"Node file size mismatch: expected {expected_node_size}, got {os.path.getsize(node_file)}"


def test_error_handling(temp_files):
    """Test error handling scenarios."""
    disk_file, jrnl_file, free_file, node_file = temp_files
    status = MockStatus()

    num_blocks = bNum_tConst.NUM_DISK_BLOCKS.value
    expected_errors = sum(1 for i in range(num_blocks) if i % 10 == 0)

    # Create a disk file with multiple errors
    with open(disk_file, 'wb') as f:
        for i in range(num_blocks):
            block = bytearray(u32Const.BLOCK_BYTES.value)
            if i % 10 == 0:  # Every 10th block has an error
                # Write incorrect CRC
                f.write(block)
            else:
                # Write correct CRC
                SimDisk.create_block(block, u32Const.BLOCK_BYTES.value)
                f.write(block)

    sim_disk = SimDisk(status, disk_file, jrnl_file, free_file, node_file)

    # Check that error blocks are correctly identified
    assert len(sim_disk.errBlocks) == expected_errors, (
        f"Expected {expected_errors} error blocks, got {len(sim_disk.errBlocks)}")
    for i, err_block in enumerate(sim_disk.errBlocks):
        assert err_block == i * 10, f"Expected error in block {i * 10}, got {err_block}"


def test_file_permissions(tmpdir, caplog):
    """Test file permission errors."""
    disk_file = tmpdir.join("test_disk.bin")
    disk_file.write("")
    os.chmod(str(disk_file), 0o444)  # Read-only

    jrnl_file = tmpdir.join("test_jrnl.bin")
    free_file = tmpdir.join("test_free.bin")
    node_file = tmpdir.join("test_node.bin")

    status = MockStatus()

    with pytest.raises(SystemExit) as excinfo:
        SimDisk(status, str(disk_file), str(jrnl_file), str(free_file), str(node_file))

    assert excinfo.value.code == 1

    # Check for the error message in the log
    assert "Bad file size" in caplog.text


def test_create_j_file(tmpdir):
    """Test journal file creation."""
    j_file = tmpdir.join("test_j.bin")
    with open(str(j_file), 'wb') as ofs:
        SimDisk.create_j_file(ofs, u32Const.BLOCK_BYTES.value)

    assert os.path.getsize(str(j_file)) == u32Const.BLOCK_BYTES.value * u32Const.PAGES_PER_JRNL.value

    # Verify the content is all zeros
    with open(str(j_file), 'rb') as f:
        content = f.read()
        assert all(b == 0 for b in content), "Journal file should be filled with zeros"


def test_create_f_file(tmpdir):
    """Test free list file creation."""
    f_file = tmpdir.join("test_f.bin")
    with open(str(f_file), 'wb') as ofs:
        SimDisk.create_f_file(ofs)

    expected_size = u32Const.BLOCK_BYTES.value * u32Const.NUM_FREE_LIST_BLOCKS.value * 2 + 4
    assert os.path.getsize(str(f_file)) == expected_size

    # Verify the structure of the free list file
    with open(str(f_file), 'rb') as f:
        bFrm = f.read(u32Const.BLOCK_BYTES.value * u32Const.NUM_FREE_LIST_BLOCKS.value)
        bTo = f.read(u32Const.BLOCK_BYTES.value * u32Const.NUM_FREE_LIST_BLOCKS.value)
        init_posn = struct.unpack('<I', f.read(4))[0]

    assert init_posn == 0, f"Expected initial position 0, got {init_posn}"