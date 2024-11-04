# test_freeList.py

import pytest
import os
import tempfile
from freeList import FreeList
from ajTypes import bNum_t, u32Const, bNum_tConst, SENTINEL_BNUM


def setup_temp_file():
    """Create a temporary file for testing."""
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.close()
    return temp_file.name


def cleanup_temp_file(filename):
    """Remove the temporary file."""
    if os.path.exists(filename):
        os.remove(filename)


@pytest.fixture
def free_list():
    """Fixture to create a FreeList instance with a temporary file."""
    temp_filename = setup_temp_file()
    fl = FreeList(temp_filename)
    yield fl
    cleanup_temp_file(temp_filename)


def test_initialization(free_list):
    """Test FreeList initialization."""
    assert free_list.fromPosn == 0
    assert not free_list.bitsFrm.none()  # All bits should be set initially
    assert free_list.bitsTo.none()  # bitsTo should be empty initially


def test_get_blk(free_list):
    """Test getting blocks from the free list."""
    # Get first block
    block = free_list.get_blk()
    assert block == 0
    assert not free_list.bitsFrm.test(0)  # First bit should now be reset

    # Get a few more blocks
    for i in range(1, 5):
        assert free_list.get_blk() == i


def test_put_blk(free_list):
    """Test putting blocks back into the free list."""
    # Get and put back a block
    block = free_list.get_blk()
    free_list.put_blk(block)
    assert free_list.bitsTo.test(block)

    # Put back multiple blocks
    blocks = [free_list.get_blk() for _ in range(5)]
    for b in blocks:
        free_list.put_blk(b)

    for b in blocks:
        assert free_list.bitsTo.test(b)


def test_refresh(free_list):
    """Test the refresh method."""
    # Get some blocks
    blocks = [free_list.get_blk() for _ in range(5)]

    # Put them back
    for b in blocks:
        free_list.put_blk(b)

    # Refresh
    free_list.refresh()

    # Check if blocks are available again
    for b in blocks:
        assert free_list.bitsFrm.test(b)
    assert free_list.bitsTo.none()  # bitsTo should be empty after refresh


def test_full_allocation_and_deallocation(free_list):
    """Test allocating all blocks and then deallocating them."""
    # Allocate all blocks
    allocated_blocks = []
    block = free_list.get_blk()
    while block != SENTINEL_BNUM:
        allocated_blocks.append(block)
        block = free_list.get_blk()

    assert len(allocated_blocks) == bNum_tConst.NUM_DISK_BLOCKS.value

    # Put all blocks back
    for b in allocated_blocks:
        free_list.put_blk(b)

    # Refresh and check availability
    free_list.refresh()
    assert free_list.bitsFrm.all()


def test_file_persistence(free_list):
    """Test that the free list state is properly saved and loaded."""
    # Modify the free list state
    blocks = [free_list.get_blk() for _ in range(5)]
    for b in blocks:
        free_list.put_blk(b)

    # Store and recreate
    free_list.store_lst()
    temp_filename = free_list.ffn
    del free_list

    # Load and verify
    new_free_list = FreeList(temp_filename)
    for b in blocks:
        assert new_free_list.bitsTo.test(b)


def test_error_conditions(free_list):
    """Test error conditions and edge cases."""
    # Test putting an out-of-range block
    with pytest.raises(AssertionError):
        free_list.put_blk(bNum_tConst.NUM_DISK_BLOCKS.value)  # This should be out of range

    # Allocate all blocks and try to get one more
    for _ in range(bNum_tConst.NUM_DISK_BLOCKS.value):
        free_list.get_blk()
    assert free_list.get_blk() == SENTINEL_BNUM  # Should return sentinel when no blocks available


def test_fromPosn_advancement(free_list):
    """Test that fromPosn advances correctly."""
    initial_posn = free_list.fromPosn
    free_list.get_blk()
    assert free_list.fromPosn == initial_posn + 1

    # Allocate a block in the middle and check fromPosn behavior
    mid_block = bNum_tConst.NUM_DISK_BLOCKS.value // 2
    for _ in range(mid_block):
        free_list.get_blk()

    free_list.put_blk(0)  # Put back the first block
    free_list.refresh()

    assert free_list.fromPosn == 0  # fromPosn should reset to the first available block