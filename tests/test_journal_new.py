import pytest
from journal import Journal
from change import Change, ChangeLog, Select
from ajTypes import u32Const, from_bytes_64bit
import os


@pytest.fixture
def basic_journal(journal):
    """Set up journal with predictable test data."""
    # Write test data
    journal.seek(journal.META_LEN)

    # Write start tag
    journal.write(journal.START_TAG.to_bytes(8, byteorder='little'))

    # Write size (32 bytes of change data)
    journal.write((32).to_bytes(8, byteorder='little'))

    # Write a simple change (block 1, timestamp 12345)
    journal.write((1).to_bytes(8, byteorder='little'))  # block number
    journal.write((12345).to_bytes(8, byteorder='little'))  # timestamp

    # Write a selector with bit 0 and MSB set
    selector_value = (1 << 63) | 1  # 0x8000000000000001
    journal.write(selector_value.to_bytes(8, byteorder='little'))

    # Write data line
    journal.write(b'Test data' + b'\x00' * 56)

    # Write CRC and padding
    journal.write(b'\x00' * 8)

    # Write end tag
    journal.write(journal.END_TAG.to_bytes(8, byteorder='little'))

    # Update metadata
    journal._metadata.write(journal.META_LEN, journal.tell(), 32)

    return journal


def test_rd_last_jrnl_new_basic(basic_journal):
    """Test the new rd_last_jrnl_new method with simple data."""
    journal = basic_journal
    change_log = ChangeLog()

    # Read using new method
    bytes_read = journal.rd_last_jrnl_new(change_log)

    # Verify changes were read correctly
    assert 1 in change_log.the_log
    assert len(change_log.the_log[1]) == 1

    # Verify change content
    change = change_log.the_log[1][0]
    assert change.block_num == 1
    assert change.time_stamp == 12345
    assert len(change.selectors) == 1
    assert change.selectors[0].is_set(0)
    assert change.selectors[0].is_last_block()
    assert len(change.new_data) == 1
    assert change.new_data[0].startswith(b'Test data')

    # Verify read pattern
    meta_read, start_tag_read, size_read, changes_read, end_tag_read = journal.read_log
    assert meta_read[0] == 0  # Metadata from position 0
    assert start_tag_read[0] == journal.META_LEN  # Start tag from journal data start
    assert changes_read[0] == journal.META_LEN + 16  # Change data after header


def test_rd_last_jrnl_new_multiple_changes(journal):
    """Test reading multiple changes in a single journal entry."""
    # Set up journal with multiple changes
    journal.seek(journal.META_LEN)

    # Calculate total bytes for two changes
    change1_bytes = 8 + 8 + 8 + 64 + 8  # Block + timestamp + selector + data + CRC
    change2_bytes = 8 + 8 + 8 + 64 + 8
    total_bytes = change1_bytes + change2_bytes

    # Write start tag and size
    journal.write(journal.START_TAG.to_bytes(8, byteorder='little'))
    journal.write(total_bytes.to_bytes(8, byteorder='little'))

    # Write first change
    journal.write((1).to_bytes(8, byteorder='little'))  # block 1
    journal.write((12345).to_bytes(8, byteorder='little'))  # timestamp
    journal.write(((1 << 63) | 1).to_bytes(8, byteorder='little'))  # selector
    journal.write(b'Change 1' + b'\x00' * 56)  # data
    journal.write(b'\x00' * 8)  # CRC + padding

    # Write second change
    journal.write((2).to_bytes(8, byteorder='little'))  # block 2
    journal.write((12346).to_bytes(8, byteorder='little'))  # timestamp
    journal.write(((1 << 63) | 2).to_bytes(8, byteorder='little'))  # selector
    journal.write(b'Change 2' + b'\x00' * 56)  # data
    journal.write(b'\x00' * 8)  # CRC + padding

    # Write end tag
    journal.write(journal.END_TAG.to_bytes(8, byteorder='little'))

    # Update metadata
    journal._metadata.write(journal.META_LEN, journal.tell(), total_bytes)

    # Read changes
    change_log = ChangeLog()
    journal.rd_last_jrnl_new(change_log)

    # Verify both changes were read
    assert 1 in change_log.the_log
    assert 2 in change_log.the_log
    assert len(change_log.the_log[1]) == 1
    assert len(change_log.the_log[2]) == 1

    # Verify first change
    change1 = change_log.the_log[1][0]
    assert change1.block_num == 1
    assert change1.new_data[0].startswith(b'Change 1')

    # Verify second change
    change2 = change_log.the_log[2][0]
    assert change2.block_num == 2
    assert change2.new_data[0].startswith(b'Change 2')


def test_rd_last_jrnl_new_wraparound(journal):
    """Test reading journal data that wraps around the buffer."""
    # Calculate position near the end of journal
    wrap_start_pos = u32Const.JRNL_SIZE.value - 20  # Leave enough space for header

    # Position to wrap start
    journal.seek(wrap_start_pos)

    # Write start tag and size
    journal.write(journal.START_TAG.to_bytes(8, byteorder='little'))
    journal.write((40).to_bytes(8, byteorder='little'))  # Will wrap around

    # Write change header
    journal.write((3).to_bytes(8, byteorder='little'))  # block 3

    # Calculate bytes until end
    bytes_written = 24  # Start tag + size + block number
    bytes_until_end = u32Const.JRNL_SIZE.value - (wrap_start_pos + bytes_written)

    # Write timestamp that will cross boundary
    timestamp_bytes = (54321).to_bytes(8, byteorder='little')
    journal.write(timestamp_bytes[:bytes_until_end])

    # Wrap to start of data section
    journal.seek(journal.META_LEN)

    # Write rest of timestamp
    journal.write(timestamp_bytes[bytes_until_end:])

    # Write selector
    journal.write(((1 << 63) | 1).to_bytes(8, byteorder='little'))

    # Write data
    journal.write(b'Wrapped data' + b'\x00' * 52)

    # Write CRC and padding
    journal.write(b'\x00' * 8)

    # Write end tag
    journal.write(journal.END_TAG.to_bytes(8, byteorder='little'))

    # Update metadata
    journal._metadata.write(wrap_start_pos, journal.tell(), 40)

    # Read changes
    change_log = ChangeLog()
    journal.rd_last_jrnl_new(change_log)

    # Verify change was read correctly
    assert 3 in change_log.the_log
    change = change_log.the_log[3][0]
    assert change.block_num == 3
    assert change.time_stamp == 54321
    assert change.new_data[0].startswith(b'Wrapped data')