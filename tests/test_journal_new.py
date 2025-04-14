"""test_journal_new.py - Tests for new journal implementation."""
import pytest
from journal import Journal
from change import Change, ChangeLog, Select
from ajTypes import u32Const, bNum_tConst, from_bytes_64bit, write_64bit
import os
import logging
from typing import Dict, List


logger = logging.getLogger(__name__)


@pytest.fixture
def basic_journal(journal):
    """Set up journal with predictable test data."""
    # Write test data
    journal.seek(journal.META_LEN)

    # Write start tag
    write_64bit(journal.journal_file, journal.START_TAG)

    # Write size (32 bytes of change data)
    write_64bit(journal.journal_file, 32)

    # Write a simple change (block 1, timestamp 12345)
    write_64bit(journal.journal_file, 1)  # block number
    write_64bit(journal.journal_file, 12345)  # timestamp

    # Write a selector with bit 0 and MSB set
    selector_value = (1 << 63) | 1  # 0x8000000000000001
    write_64bit(journal.journal_file, selector_value)

    # Write data line
    journal.journal_file.write(b'Test data' + b'\x00' * 56)

    # Write CRC and padding
    journal.journal_file.write(b'\x00' * 8)

    # Write end tag
    write_64bit(journal.journal_file, journal.END_TAG)

    # Update metadata
    journal._metadata.write(journal.META_LEN, journal.tell(), 32)

    # Sync the file to ensure all data is written
    journal.journal_file.flush()
    os.fsync(journal.journal_file.fileno())

    # Reset file position to start
    journal.seek(0)

    return journal


def verify_change_content(change: Change, expected: Dict):
    """Helper function to verify change content."""
    assert change.block_num == expected['block_num']
    assert change.time_stamp == expected['timestamp']
    assert len(change.selectors) == expected['num_selectors']

    if 'selector_bits' in expected:
        for i, bits in enumerate(expected['selector_bits']):
            for bit in bits:
                assert change.selectors[i].is_set(bit)

    if 'data_content' in expected:
        assert len(change.new_data) == len(expected['data_content'])
        for actual, expected_data in zip(change.new_data, expected['data_content']):
            assert actual.startswith(expected_data)


def verify_read_pattern(journal: Journal, expected_positions: List[tuple]):
    """Helper function to verify journal read pattern."""
    for actual, expected in zip(journal.read_log, expected_positions):
        assert actual[0] == expected[0], f"Wrong read position. Expected {expected[0]}, got {actual[0]}"
        assert actual[1] == expected[1], f"Wrong read size. Expected {expected[1]}, got {actual[1]}"


def test_rd_last_jrnl_new_basic(basic_journal):
    """Test the new rd_last_jrnl_new method with simple data."""
    journal = basic_journal
    change_log = ChangeLog(test_sw=True)

    # Read using new method
    bytes_read = journal.rd_last_jrnl_new(change_log)

    # Verify changes were read correctly
    assert 1 in change_log.the_log, "Block 1 not found in change log"
    assert len(change_log.the_log[1]) == 1, "Expected exactly one change for block 1"

    # Verify change content using helper function
    expected_content = {
        'block_num': 1,
        'timestamp': 12345,
        'num_selectors': 1,
        'selector_bits': [[0, 63]],  # Bit 0 and MSB (63) should be set
        'data_content': [b'Test data']
    }
    verify_change_content(change_log.the_log[1][0], expected_content)

    # Verify read pattern
    expected_positions = [
        (0, 24),  # Metadata read
        (journal.META_LEN, 8),  # Start tag
        (journal.META_LEN + 8, 8),  # Size field
        (journal.META_LEN + 16, 32),  # Change data
        (journal.META_LEN + 48, 8)  # End tag
    ]
    verify_read_pattern(journal, expected_positions)


def test_rd_last_jrnl_new_multiple_changes(journal):
    """Test reading multiple changes in a single journal entry."""
    # Set up journal with multiple changes
    journal.seek(journal.META_LEN)

    # Calculate total bytes for two changes
    change1_bytes = 8 + 8 + 8 + 64 + 8  # Block + timestamp + selector + data + CRC
    change2_bytes = 8 + 8 + 8 + 64 + 8
    total_bytes = change1_bytes + change2_bytes

    # Write start tag and size
    write_64bit(journal.journal_file, journal.START_TAG)
    write_64bit(journal.journal_file, total_bytes)

    # Write first change
    write_64bit(journal.journal_file, 1)  # block 1
    write_64bit(journal.journal_file, 12345)  # timestamp
    write_64bit(journal.journal_file, (1 << 63) | 1)  # selector
    journal.journal_file.write(b'Change 1' + b'\x00' * 56)  # data
    journal.journal_file.write(b'\x00' * 8)  # CRC + padding

    # Write second change
    write_64bit(journal.journal_file, 2)  # block 2
    write_64bit(journal.journal_file, 12346)  # timestamp
    write_64bit(journal.journal_file, (1 << 63) | 2)  # selector
    journal.journal_file.write(b'Change 2' + b'\x00' * 56)  # data
    journal.journal_file.write(b'\x00' * 8)  # CRC + padding

    # Write end tag
    write_64bit(journal.journal_file, journal.END_TAG)

    # Update metadata
    journal._metadata.write(journal.META_LEN, journal.tell(), total_bytes)

    # Sync file and reset position
    journal.journal_file.flush()
    os.fsync(journal.journal_file.fileno())
    journal.seek(0)

    # Read changes
    change_log = ChangeLog(test_sw=True)
    bytes_read = journal.rd_last_jrnl_new(change_log)

    # Verify both changes were read
    assert 1 in change_log.the_log, "Block 1 not found in change log"
    assert 2 in change_log.the_log, "Block 2 not found in change log"

    # Verify first change
    expected_change1 = {
        'block_num': 1,
        'timestamp': 12345,
        'num_selectors': 1,
        'selector_bits': [[0, 63]],
        'data_content': [b'Change 1']
    }
    verify_change_content(change_log.the_log[1][0], expected_change1)

    # Verify second change
    expected_change2 = {
        'block_num': 2,
        'timestamp': 12346,
        'num_selectors': 1,
        'selector_bits': [[1, 63]],
        'data_content': [b'Change 2']
    }
    verify_change_content(change_log.the_log[2][0], expected_change2)


def test_rd_last_jrnl_new_wraparound(journal):
    """Test reading journal data that wraps around the buffer."""
    # Calculate position near the end of journal
    wrap_start_pos = u32Const.JRNL_SIZE.value - 20

    # Position to wrap start
    journal.seek(wrap_start_pos)

    # Write start tag and size
    write_64bit(journal.journal_file, journal.START_TAG)
    write_64bit(journal.journal_file, 40)  # Will wrap around

    # Write change header
    write_64bit(journal.journal_file, 3)  # block 3

    # Calculate bytes until end
    bytes_written = 24  # Start tag + size + block number
    bytes_until_end = u32Const.JRNL_SIZE.value - (wrap_start_pos + bytes_written)

    # Write timestamp that will cross boundary
    timestamp_bytes = (54321).to_bytes(8, byteorder='little')
    first_part = bytes_until_end if bytes_until_end < 8 else 8
    journal.journal_file.write(timestamp_bytes[:first_part])

    # Wrap to start of data section
    journal.seek(journal.META_LEN)

    # Write rest of timestamp if needed
    if first_part < 8:
        journal.journal_file.write(timestamp_bytes[first_part:])

    # Write selector
    write_64bit(journal.journal_file, (1 << 63) | 1)

    # Write data
    journal.journal_file.write(b'Wrapped data' + b'\x00' * 52)

    # Write CRC and padding
    journal.journal_file.write(b'\x00' * 8)

    # Write end tag
    write_64bit(journal.journal_file, journal.END_TAG)

    # Update metadata
    journal._metadata.write(wrap_start_pos, journal.tell(), 40)

    # Sync file and reset position
    journal.journal_file.flush()
    os.fsync(journal.journal_file.fileno())
    journal.seek(0)

    # Read changes
    change_log = ChangeLog(test_sw=True)
    bytes_read = journal.rd_last_jrnl_new(change_log)

    # Verify change was read correctly
    assert 3 in change_log.the_log, f"Block 3 not found in change log: {change_log.the_log.keys()}"

    expected_change = {
        'block_num': 3,
        'timestamp': 54321,
        'num_selectors': 1,
        'selector_bits': [[0, 63]],
        'data_content': [b'Wrapped data']
    }
    verify_change_content(change_log.the_log[3][0], expected_change)


def test_rd_last_jrnl_new_empty_journal(journal):
    """Test reading from an empty journal."""
    # Ensure a clean start
    journal.init()  # Initialize metadata explicitly

    change_log = ChangeLog(test_sw=True)
    bytes_read = journal.rd_last_jrnl_new(change_log)

    assert len(change_log.the_log) == 0, "Expected empty change log"
    assert bytes_read == 0, "Expected zero bytes read"


def test_rd_last_jrnl_new_invalid_block(journal):
    """Test handling of invalid block numbers."""
    journal.seek(journal.META_LEN)

    # Write start tag and size
    write_64bit(journal.journal_file, journal.START_TAG)
    write_64bit(journal.journal_file, 32)

    # Write invalid block number
    invalid_block = bNum_tConst.NUM_DISK_BLOCKS.value + 1
    write_64bit(journal.journal_file, invalid_block)

    # Write rest of change data
    journal.journal_file.write(b'\x00' * 24)  # Padding

    # Write end tag
    write_64bit(journal.journal_file, journal.END_TAG)

    # Update metadata
    journal._metadata.write(journal.META_LEN, journal.tell(), 32)

    # Try to read changes
    change_log = ChangeLog(test_sw=True)
    journal.rd_last_jrnl_new(change_log)

    assert len(change_log.the_log) == 0, "Expected empty change log due to invalid block"


if __name__ == "__main__":
    pytest.main([__file__, '-v'])