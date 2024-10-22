# tests/test_wipeList.py
import pytest
from wipeList import WipeList
from ajTypes import u32Const


def test_wipe_list_initialization():
    """Test that a WipeList is correctly initialized."""
    wl = WipeList()
    assert wl.dirty is not None
    assert wl.dirty.size() == u32Const.NUM_WIPE_PAGES.value * u32Const.BITS_PER_PAGE.value


def test_set_and_check_dirty():
    """Test setting and checking dirty blocks."""
    wl = WipeList()

    # Test setting a specific block as dirty
    wl.set_dirty(5)
    assert wl.is_dirty(5)

    # Verify other blocks are not dirty
    assert not wl.is_dirty(4)
    assert not wl.is_dirty(6)


def test_clear_array():
    """Test clearing the entire array."""
    wl = WipeList()

    # Set multiple blocks as dirty
    wl.set_dirty(1)
    wl.set_dirty(10)
    wl.set_dirty(20)

    # Clear the array
    wl.clear_array()

    # Verify no blocks are dirty
    assert not wl.is_dirty(1)
    assert not wl.is_dirty(10)
    assert not wl.is_dirty(20)


def test_is_ripe():
    """Test when the WipeList is considered 'ripe' for cleaning."""
    wl = WipeList()

    # Initially not ripe
    assert not wl.is_ripe()

    # Set blocks dirty until threshold is reached
    for i in range(WipeList.DIRTY_BEFORE_WIPE):
        wl.set_dirty(i)

    # Now should be ripe
    assert wl.is_ripe()

    # One less than threshold should not be ripe
    wl.clear_array()
    for i in range(WipeList.DIRTY_BEFORE_WIPE - 1):
        wl.set_dirty(i)

    assert not wl.is_ripe()


def test_block_range():
    """Test setting dirty blocks within valid range."""
    wl = WipeList()

    # Test setting a block at the lower bound
    wl.set_dirty(0)
    assert wl.is_dirty(0)

    # Test setting a block near the upper bound
    max_block = u32Const.NUM_WIPE_PAGES.value * u32Const.BITS_PER_PAGE.value - 1
    wl.set_dirty(max_block)
    assert wl.is_dirty(max_block)


def test_block_out_of_range():
    """Test that setting blocks out of range raises an exception."""
    wl = WipeList()

    # Try to set a block beyond the valid range
    max_block = u32Const.NUM_WIPE_PAGES.value * u32Const.BITS_PER_PAGE.value

    with pytest.raises(IndexError):
        wl.set_dirty(max_block)

    with pytest.raises(IndexError):
        wl.is_dirty(max_block)