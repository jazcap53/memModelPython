import pytest
from change import Change, ChangeLog, Select
from ajTypes import bNum_t, lNum_t, u32Const, SENTINEL_BNUM
from collections import deque


@pytest.fixture
def empty_change():
    """Create an empty Change object."""
    return Change(block_num=1)


@pytest.fixture
def change_with_data():
    """Create a Change object with some test data."""
    change = Change(block_num=2)
    change.add_selector(is_last=False)
    change.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)
    change.add_line(1, b'B' * u32Const.BYTES_PER_LINE.value)
    return change


@pytest.fixture
def empty_changelog():
    """Create an empty ChangeLog object."""
    return ChangeLog()


@pytest.fixture
def changelog_with_data():
    """Create a ChangeLog with some test changes."""
    changelog = ChangeLog()

    # Add first change
    change1 = Change(block_num=1)
    change1.add_selector(is_last=False)
    change1.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)
    changelog.add_to_log(change1)

    # Add second change
    change2 = Change(block_num=2)
    change2.add_selector(is_last=True)
    change2.add_line(1, b'B' * u32Const.BYTES_PER_LINE.value)
    changelog.add_to_log(change2)

    return changelog


# Tests for Change class
def test_change_initialization(empty_change):
    """Test Change object initialization."""
    assert empty_change.block_num == 1
    assert empty_change.time_stamp > 0
    assert isinstance(empty_change.selectors, deque)
    assert isinstance(empty_change.new_data, deque)
    assert empty_change.arr_next == 0


def test_add_selector(empty_change):
    """Test adding selectors to a Change object."""
    empty_change.add_selector(is_last=True)
    assert len(empty_change.selectors) == 1
    assert empty_change.selectors[0].is_last_block()

    empty_change.add_selector(is_last=False)
    assert len(empty_change.selectors) == 2
    assert not empty_change.selectors[1].is_last_block()


def test_add_line(empty_change):
    """Test adding lines to a Change object."""
    test_data = b'X' * u32Const.BYTES_PER_LINE.value

    # Add first line
    empty_change.add_selector(is_last=False)
    empty_change.add_line(0, test_data)

    assert len(empty_change.new_data) == 1
    assert empty_change.new_data[0] == test_data
    assert empty_change.selectors[0].is_set(0)

    # Add second line
    empty_change.add_line(1, test_data)
    assert len(empty_change.new_data) == 2
    assert empty_change.selectors[0].is_set(1)


def test_is_last_block(empty_change):
    """Test is_last_block functionality."""
    assert not empty_change.is_last_block()  # Should be false when empty

    empty_change.add_selector(is_last=True)
    assert empty_change.is_last_block()

    another_change = Change(2)
    another_change.add_selector(is_last=False)
    assert not another_change.is_last_block()


def test_change_comparison():
    """Test Change object comparison based on timestamp."""
    change1 = Change(1)
    change2 = Change(2)

    # Manually set timestamps for testing
    change1.time_stamp = 100
    change2.time_stamp = 200

    assert change1 < change2
    assert not (change2 < change1)
    assert not (change1 < change1)


# Tests for ChangeLog class
def test_changelog_initialization(empty_changelog):
    """Test ChangeLog initialization."""
    assert empty_changelog.the_log == {}
    assert empty_changelog.cg_line_ct == 0


def test_add_to_log(empty_changelog, empty_change):
    """Test adding changes to the ChangeLog."""
    empty_change.add_selector(is_last=True)
    empty_change.add_line(0, b'X' * u32Const.BYTES_PER_LINE.value)

    empty_changelog.add_to_log(empty_change)

    assert len(empty_changelog.the_log) == 1
    assert empty_changelog.cg_line_ct == 1
    assert empty_change.block_num in empty_changelog.the_log


def test_multiple_changes_same_block(empty_changelog):
    """Test adding multiple changes for the same block."""
    change1 = Change(1)
    change1.add_selector(is_last=False)
    change1.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)

    change2 = Change(1)  # Same block number
    change2.add_selector(is_last=True)
    change2.add_line(1, b'B' * u32Const.BYTES_PER_LINE.value)

    empty_changelog.add_to_log(change1)
    empty_changelog.add_to_log(change2)

    assert len(empty_changelog.the_log) == 1  # One block
    assert len(empty_changelog.the_log[1]) == 2  # Two changes for that block
    assert empty_changelog.cg_line_ct == 2


def test_changelog_with_multiple_blocks(empty_changelog):
    """Test ChangeLog with changes to multiple blocks."""
    # Add changes for three different blocks
    for block_num in range(1, 4):
        change = Change(block_num)
        change.add_selector(is_last=(block_num == 3))
        change.add_line(0, bytes([block_num] * u32Const.BYTES_PER_LINE.value))
        empty_changelog.add_to_log(change)

    assert len(empty_changelog.the_log) == 3
    assert empty_changelog.cg_line_ct == 3
    assert all(block_num in empty_changelog.the_log for block_num in range(1, 4))


def test_select_functionality():
    """Test Select class functionality."""
    select = Select()

    # Test setting and checking individual bits
    select.set(5)
    assert select.is_set(5)
    assert not select.is_set(6)

    # Test last block flag
    assert not select.is_last_block()
    select.set_last_block()
    assert select.is_last_block()

    # Test conversion to/from bytes
    bytes_data = select.to_bytes()
    new_select = Select.from_bytes(bytes_data)
    assert new_select.is_set(5)
    assert new_select.is_last_block()


def test_change_print(empty_change, capsys):
    """Test Change object's print method."""
    empty_change.add_selector(is_last=True)
    empty_change.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)
    empty_change.print()

    captured = capsys.readouterr()
    assert f"Block: {empty_change.block_num}" in captured.out
    assert f"Timestamp: {empty_change.time_stamp}" in captured.out


def test_changelog_print(changelog_with_data, capsys):
    """Test ChangeLog's print method."""
    changelog_with_data.print()

    captured = capsys.readouterr()
    assert "Block 1" in captured.out
    assert "Block 2" in captured.out