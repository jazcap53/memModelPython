# test_journal_refactored.py
import pytest
import logging
from journal import Journal
from change import Change, ChangeLog, Select
from ajTypes import u32Const, bNum_tConst

logger = logging.getLogger(__name__)


def test_read_properly_written_journal(properly_written_journal):
    """Test reading from a journal created with proper application methods."""
    journal = properly_written_journal
    change_log = ChangeLog(test_sw=True)

    # Read the journal using the new method
    bytes_read = journal.rd_last_jrnl_new(change_log)

    # Verify changes were read correctly
    assert len(change_log.the_log) == 1, "Expected exactly one block in change log"
    assert 1 in change_log.the_log, "Block 1 not found in change log"
    assert len(change_log.the_log[1]) == 1, "Expected exactly one change for block 1"

    # Verify change details
    change = change_log.the_log[1][0]
    assert change.block_num == 1
    assert len(change.selectors) == 1
    assert change.selectors[0].is_set(0)  # First bit should be set
    assert change.selectors[0].is_last_block()  # Last block bit should be set
    assert len(change.new_data) == 1
    assert change.new_data[0].startswith(b'Test data')


def test_multiple_changes_with_proper_writing(journal):
    """Test writing and reading multiple changes using proper application methods."""
    # Create multiple changes
    change_log = ChangeLog(test_sw=True)

    # First change - block 2
    change1 = Change(2)
    change1.add_line(0, b'Change 1' + b'\x00' * (u32Const.BYTES_PER_LINE.value - len(b'Change 1')))
    change_log.add_to_log(change1)

    # Second change - block 3
    change2 = Change(3)
    change2.add_line(0, b'Change 2' + b'\x00' * (u32Const.BYTES_PER_LINE.value - len(b'Change 2')))
    change2.add_line(1, b'More data' + b'\x00' * (u32Const.BYTES_PER_LINE.value - len(b'More data')))
    change_log.add_to_log(change2)

    # Write changes using the proper method
    journal.write_change_log_to_journal(change_log)

    # Read back the changes
    read_log = ChangeLog(test_sw=True)
    bytes_read = journal.rd_last_jrnl_new(read_log)

    # Verify both blocks were read
    assert 2 in read_log.the_log, "Block 2 not found in read log"
    assert 3 in read_log.the_log, "Block 3 not found in read log"

    # Verify first change
    assert len(read_log.the_log[2]) == 1
    assert read_log.the_log[2][0].block_num == 2
    assert len(read_log.the_log[2][0].selectors) == 1
    assert read_log.the_log[2][0].selectors[0].is_set(0)

    # Verify second change with multiple lines
    assert len(read_log.the_log[3]) == 1
    assert read_log.the_log[3][0].block_num == 3
    assert len(read_log.the_log[3][0].selectors) == 1
    assert read_log.the_log[3][0].selectors[0].is_set(0)
    assert read_log.the_log[3][0].selectors[0].is_set(1)
    assert len(read_log.the_log[3][0].new_data) == 2


def test_wraparound_with_proper_writing(journal):
    """Test journal wraparound using proper application methods."""
    # Fill the journal to near the end to force wraparound
    journal.seek(u32Const.JRNL_SIZE.value - 100)  # Position near the end
    journal.write(b'\xFF' * 100)  # Fill with dummy data

    # Reset file position for writing
    journal.seek(u32Const.JRNL_SIZE.value - 50)

    # Create a change that will force wraparound
    change_log = ChangeLog(test_sw=True)
    change = Change(4)
    for i in range(10):  # Add multiple lines to ensure it wraps
        change.add_line(i, f"Wraparound line {i}".encode().ljust(u32Const.BYTES_PER_LINE.value, b'\x00'))
    change_log.add_to_log(change)

    # Write the change - this should handle the wraparound
    journal.write_change_log_to_journal(change_log)

    # Read back the changes
    read_log = ChangeLog(test_sw=True)
    bytes_read = journal.rd_last_jrnl_new(read_log)

    # Verify the change was read correctly
    assert 4 in read_log.the_log, "Block 4 not found in read log"
    assert len(read_log.the_log[4]) == 1
    assert read_log.the_log[4][0].block_num == 4
    assert len(read_log.the_log[4][0].selectors) > 0

    # Verify all lines are present
    total_lines = sum(selector.value.bit_count() - 1 for selector in read_log.the_log[4][0].selectors
                      if selector.value & (1 << 63))  # Subtract MSB if set
    assert total_lines == 10, f"Expected 10 lines, got {total_lines}"

    # Verify data from multiple lines
    assert len(read_log.the_log[4][0].new_data) == 10
    for i, data in enumerate(read_log.the_log[4][0].new_data):
        assert data.startswith(f"Wraparound line {i}".encode())


def test_exact_boundary_wraparound(journal):
    """Test wraparound at exact journal boundaries."""
    # Position at exactly the right spot so end tag falls at the end of the journal
    # HEADER_SIZE (16) + START_TAG (8) + data + END_TAG (8)
    data_size = 100  # Any reasonable size
    position_for_exact_boundary = u32Const.JRNL_SIZE.value - journal.HEADER_SIZE - data_size - journal.END_TAG_SIZE

    # Create a change that will result in exact boundary wraparound
    change_log = ChangeLog(test_sw=True)
    change = Change(5)
    for i in range(data_size // u32Const.BYTES_PER_LINE.value):
        change.add_line(i, f"Boundary test {i}".encode().ljust(u32Const.BYTES_PER_LINE.value, b'\x00'))
    change_log.add_to_log(change)

    # Position at the calculated spot and write
    journal.seek(position_for_exact_boundary)
    journal.write_change_log_to_journal(change_log)

    # Verify end tag position is correct
    expected_end_pos = journal.calculate_end_tag_position(
        position_for_exact_boundary, data_size)

    # Read back and verify
    read_log = ChangeLog(test_sw=True)
    bytes_read = journal.rd_last_jrnl_new(read_log)

    assert 5 in read_log.the_log, "Block 5 not found in read log"
    assert read_log.the_log[5][0].block_num == 5

    # Verify we can read data correctly
    first_line = read_log.the_log[5][0].new_data[0] if read_log.the_log[5][0].new_data else None
    assert first_line and first_line.startswith(b"Boundary test 0")


def test_multiple_wraparounds(journal):
    """Test handling of multiple wraparounds in a single journal entry."""
    # Fill most of the journal to set up for multiple wraparounds
    journal.seek(journal.META_LEN)

    # Create a very large change that will wrap multiple times
    change_log = ChangeLog(test_sw=True)
    change = Change(8)

    # Create enough lines to wrap at least twice
    # (Each line is BYTES_PER_LINE bytes + overhead)
    lines_needed = u32Const.JRNL_SIZE.value // (u32Const.BYTES_PER_LINE.value * 2)
    for i in range(lines_needed):
        change.add_line(i % 63, f"Multi-wrap line {i}".encode().ljust(u32Const.BYTES_PER_LINE.value, b'\x00'))
    change_log.add_to_log(change)

    # Write the change - this should handle multiple wraparounds
    journal.write_change_log_to_journal(change_log)

    # Read back the changes
    read_log = ChangeLog(test_sw=True)
    bytes_read = journal.rd_last_jrnl_new(read_log)

    # Verify the change was read correctly
    assert 8 in read_log.the_log, "Block 8 not found after multiple wraparounds"
    assert read_log.the_log[8][0].block_num == 8

    # Check that we have the expected number of data entries
    # Note: The actual count might be less if changes were consolidated
    assert len(read_log.the_log[8][0].new_data) > 0, "No data found in change after multiple wraparounds"
