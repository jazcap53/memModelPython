# test_journal.py

import pytest
import os
from journal import Journal
from change import Change, ChangeLog
from ajTypes import u32Const, bNum_tConst, SENTINEL_INUM, to_bytes_64bit, write_64bit, read_64bit
from myMemory import Page
from ajCrc import AJZlibCRC
import struct
import logging
from status import Status
from simDisk import SimDisk
from change import Change, ChangeLog
from crashChk import CrashChk


logger = logging.getLogger(__name__)


@pytest.fixture
def mock_sim_disk(mocker):
    mock = mocker.Mock()
    mock_ds = mocker.Mock()
    mock_ds.read.return_value = b'\x00' * u32Const.BLOCK_BYTES.value
    mock.get_ds.return_value = mock_ds
    return mock


@pytest.fixture
def mock_status(mocker):
    return mocker.Mock()


@pytest.fixture
def mock_crash_chk(mocker):
    mock = mocker.Mock()
    mock.get_last_status.return_value = "Normal"
    return mock


@pytest.fixture
def temp_journal_file(tmp_path, caplog):
    file_path = tmp_path / "test_journal.bin"
    logger.debug(f"Creating temporary journal file: {file_path}")
    yield str(file_path)
    if os.path.exists(file_path):
        logger.debug(f"Cleaning up temporary journal file: {file_path}")
        os.remove(file_path)


@pytest.fixture
def journal(mock_sim_disk, mock_change_log, mock_status, mock_crash_chk, temp_journal_file, caplog):
    """Create a Journal instance for testing.

    Args:
        mock_sim_disk: Mock disk simulator
        mock_change_log: Mock change log
        mock_status: Mock status tracker
        mock_crash_chk: Mock crash checker
        temp_journal_file: Temporary file path
        caplog: Pytest logging capture fixture

    The fixture sets up logging capture at DEBUG level and clears initial setup messages.
    """
    caplog.set_level(logging.DEBUG)
    journal = Journal(temp_journal_file, mock_sim_disk, mock_change_log, mock_status, mock_crash_chk)
    caplog.clear()  # Clear creation message from the log

    # Verify journal was initialized correctly
    assert journal.journal_file.mode == 'rb+'
    assert os.path.exists(temp_journal_file)
    assert os.path.getsize(temp_journal_file) == u32Const.JRNL_SIZE.value

    return journal


@pytest.fixture
def mock_change_log(mocker):
    mock = mocker.Mock(spec=ChangeLog)
    mock.the_log = {}
    mock.cg_line_ct = 0
    return mock


@pytest.fixture
def setup_journal_with_data(journal):
    """Fixture to set up a journal with test data."""
    # Print tag values for debugging
    print(f"START_TAG: {journal.START_TAG:x}")
    print(f"END_TAG: {journal.END_TAG:x}")

    # Write start tag
    journal.seek(journal.META_LEN)
    write_64bit(journal.journal_file, journal.START_TAG)

    # Write bytes count (64 bytes of change data)
    write_64bit(journal.journal_file, 64)

    # Write mock change data (simplified for testing)
    test_data = b'\x01' * 64
    journal.write(test_data)

    # Write end tag
    write_64bit(journal.journal_file, journal.END_TAG)

    # Verify written values by reading them back
    journal.seek(journal.META_LEN)
    written_start = read_64bit(journal.journal_file)
    journal.seek(journal.tell() + 64 + 8)  # Skip size field and data
    written_end = read_64bit(journal.journal_file)
    print(f"Written START_TAG: {written_start:x}")
    print(f"Written END_TAG: {written_end:x}")

    # Update metadata
    journal._metadata.write(journal.META_LEN, journal.tell(), journal.META_LEN + 64 + 24)

    return journal


def test_journal_initialization(journal):
    assert journal.f_name == str(journal.journal_file.name)
    assert journal.journal_file.mode == 'rb+'

    # Check instance variables after __init__()
    assert journal.meta_get == -1  # Changed from 0 to -1
    assert journal.meta_put == 24
    assert journal.meta_sz == 0

def test_journal_init_file_content(journal):
    journal.init()

    # Verify values written to file
    journal.journal_file.seek(0)
    file_meta_get, file_meta_put, file_meta_sz = struct.unpack('<qqq', journal.journal_file.read(24))
    assert file_meta_get == -1
    assert file_meta_put == 24
    assert file_meta_sz == 0


def test_write_field(journal):
    # Test writing a 64-bit field
    journal.journal_file.seek(0)
    bytes_written = journal._file_io.wrt_field(b'\x01\x02\x03\x04\x05\x06\x07\x08', 8, True)
    assert bytes_written == 8
    assert journal.ttl_bytes_written == 8

    # Test writing with wraparound
    journal.journal_file.seek(u32Const.JRNL_SIZE.value - 4)
    bytes_written = journal._file_io.wrt_field(b'\x01\x02\x03\x04\x05\x06\x07\x08', 8, True)
    assert bytes_written == 8
    assert journal.journal_file.tell() == Journal.META_LEN + 4


def test_write_change(journal, mocker):
    # Create a mock Change object
    mock_change = mocker.Mock(spec=Change)
    mock_change.block_num = 1
    mock_change.time_stamp = 12345
    mock_change.selectors = [mocker.Mock(to_bytes=mocker.Mock(return_value=b'selector'))]
    mock_change.new_data = [b'data1', b'data2']

    # Mock the wrt_field method
    mock_wrt_field = mocker.Mock(side_effect=[8, 8, 8, 16, 16])  # Return values for each call
    journal._file_io.wrt_field = mock_wrt_field

    # Call the method
    bytes_written = journal._change_log_handler.write_change(mock_change)

    # Assertions
    assert bytes_written == 56  # Sum of all returned values from wrt_field
    assert mock_wrt_field.call_count == 5  # Called for block_num, timestamp, selector, and two data items

    # Verify call arguments
    calls = [
        mocker.call(mocker.ANY, 8, True),  # block_num
        mocker.call(mocker.ANY, 8, True),  # timestamp
        mocker.call(b'selector', journal.sz, True),  # selector
        mocker.call(b'data1', u32Const.BYTES_PER_LINE.value, True),  # data1
        mocker.call(b'data2', u32Const.BYTES_PER_LINE.value, True)   # data2
    ]
    mock_wrt_field.assert_has_calls(calls)


def test_write_change_log(journal, mock_change_log, mocker, caplog):
    """Test writing to the change log."""
    # Setup
    change1 = Change(1)
    change1.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)
    mock_change_log.the_log = {1: [change1]}
    mock_change_log.cg_line_ct = 1

    mocker.patch('journal.get_cur_time', return_value=12345)

    with caplog.at_level(logging.DEBUG):
        # Write to journal using non-deprecated methods
        journal._change_log_handler.calculate_ct_bytes_to_write(mock_change_log)
        journal._change_log_handler.wrt_cg_log_to_jrnl(mock_change_log)


    # Verify log messages
    assert any("Writing change log to journal" in record.message for record in caplog.records)
    assert any("Change log written at time 12345" in record.message for record in caplog.records)

    # Verify journal state
    journal.journal_file.seek(0)
    meta_get, meta_put, meta_sz = journal._metadata.read()

    # Verify metadata was updated
    assert meta_get >= Journal.META_LEN
    assert meta_put > meta_get
    assert meta_sz > 0

    # Verify block was marked as in journal
    assert journal.blks_in_jrnl[1] is True

    # Verify exact byte count
    expected_bytes = (
            8 +  # Block number
            8 +  # Timestamp
            8 +  # Selector
            64 +  # Data line
            8  # CRC (4) + Padding (4)
    )
    assert journal.ttl_bytes_written == expected_bytes

    # Verify that the mock_change_log was cleared
    assert mock_change_log.cg_line_ct == 0


def test_is_in_journal(journal):
    journal.blks_in_jrnl[5] = True
    assert journal.is_in_jrnl(5)
    assert not journal.is_in_jrnl(6)


def test_write_change_to_page(journal, caplog):
    """Test writing changes to a page."""
    # Setup
    change = Change(1)
    change.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)
    change.add_line(1, b'B' * u32Const.BYTES_PER_LINE.value)
    page = Page()
    original_page = page.dat.copy()  # Save original page content

    with caplog.at_level(logging.DEBUG):
        # Use the nested class method directly
        journal._change_log_handler.wrt_cg_to_pg(change, page)

    # Verify page contents
    assert page.dat[:u32Const.BYTES_PER_LINE.value] == b'A' * u32Const.BYTES_PER_LINE.value
    assert page.dat[
           u32Const.BYTES_PER_LINE.value:2 * u32Const.BYTES_PER_LINE.value] == b'B' * u32Const.BYTES_PER_LINE.value

    # Verify unchanged portions of the page
    assert page.dat[2 * u32Const.BYTES_PER_LINE.value:-4] == original_page[2 * u32Const.BYTES_PER_LINE.value:-4]

    # Verify CRC was updated correctly
    crc = AJZlibCRC.get_code(page.dat[:-4], u32Const.BYTES_PER_PAGE.value - 4)
    stored_crc = int.from_bytes(page.dat[-4:], 'little')
    assert crc == stored_crc

    # Verify logging
    assert any("Writing change to page" in record.message for record in caplog.records)


def test_crc_check_pg(journal):
    page = Page()
    crc = AJZlibCRC.get_code(page.dat[:-u32Const.CRC_BYTES.value],
                             u32Const.BYTES_PER_PAGE.value - u32Const.CRC_BYTES.value)

    # Directly modify the last 4 bytes of page.dat
    for i in range(u32Const.CRC_BYTES.value):
        page.dat[-u32Const.CRC_BYTES.value + i] = (crc >> (8 * i)) & 0xFF

    page_tuple = (1, page)
    result = journal._file_io._crc_check_pg(page_tuple)
    assert result is True


def test_purge_journal(journal, mock_change_log, mocker, caplog):
    """Test journal purging functionality."""
    # Mock CRC verification to always return True
    mocker.patch.object(journal, 'verify_page_crc', return_value=True)

    # Setup
    change1 = Change(1)
    change1.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)

    mock_dict = mocker.MagicMock()
    mock_dict.items.return_value = {1: [change1]}.items()
    mock_dict.__getitem__.side_effect = lambda x: [change1] if x == 1 else KeyError()
    mock_dict.clear = mocker.Mock()  # Ensure clear method is callable

    mock_change_log.the_log = mock_dict
    mock_change_log.cg_line_ct = 1

    # Write to journal using non-deprecated methods
    journal._change_log_handler.calculate_ct_bytes_to_write(mock_change_log)
    journal._change_log_handler.wrt_cg_log_to_jrnl(mock_change_log)

    # Verify pre-purge state
    assert journal.blks_in_jrnl[1] is True
    assert journal._metadata.meta_sz > 0

    with caplog.at_level(logging.INFO):
        # Purge the journal
        journal.purge_jrnl(True, False)

    # Verify post-purge state
    assert not any(journal.blks_in_jrnl)  # All blocks should be marked as not in journal
    mock_dict.clear.assert_called_once()  # Change log should be cleared

    # Verify metadata was reset correctly
    assert journal._metadata.meta_get == -1
    assert journal._metadata.meta_put == 24
    assert journal._metadata.meta_sz == 0

    # Verify journal file size remains correct
    journal.journal_file.seek(0, 2)  # Seek to end
    assert journal.journal_file.tell() == u32Const.JRNL_SIZE.value

    # Verify log messages
    assert any("Purging journal" in record.message for record in caplog.records)


def test_do_wipe_routine(journal, mocker, mock_change_log, caplog):
    # Mock file manager
    mock_file_man = mocker.Mock()

    # Mock wipers
    mock_wipers = mocker.Mock()
    mock_wipers.is_dirty.return_value = True
    mock_wipers.is_ripe.return_value = False
    journal.wipers = mock_wipers

    # Assign mock change log to journal
    journal.change_log = mock_change_log

    with caplog.at_level(logging.INFO):
        # Call the method
        journal.do_wipe_routine(1, mock_file_man)

    # Verify log messages
    assert any("Saving change log and purging journal" in record.message for record in caplog.records)

    # Assertions
    mock_file_man.do_store_inodes.assert_called_once()
    mock_file_man.do_store_free_list.assert_called_once()
    mock_wipers.clear_array.assert_called_once()


def test_empty_purge_jrnl_buf(journal, mocker, caplog):
    # Mock the Journal's write_block_to_disk method
    mock_write_block = mocker.patch.object(journal, 'write_block_to_disk')

    # Create a test page
    mock_page = Page()
    mock_page.dat = bytearray(u32Const.BLOCK_BYTES.value)

    # Calculate and set correct CRC
    crc = AJZlibCRC.get_code(mock_page.dat[:-u32Const.CRC_BYTES.value],
                             u32Const.BYTES_PER_PAGE.value - u32Const.CRC_BYTES.value)
    for i in range(u32Const.CRC_BYTES.value):
        mock_page.dat[-u32Const.CRC_BYTES.value + i] = (crc >> (8 * i)) & 0xFF

    # Set up the change log handler's buffer
    journal._change_log_handler.pg_buf = [(1, mock_page)]

    # Call the method on change log handler
    result = journal._change_log_handler.write_buffer_to_disk(False)

    assert result is True
    mock_write_block.assert_called_once_with(1, mock_page)
    assert all(item is None for item in journal._change_log_handler.pg_buf)  # Buffer should be cleared


def test_verify_page_crc(journal):
    """Test CRC verification of a page."""
    # Create a test page
    test_page = Page()

    # Fill page with test data
    test_data = b'A' * (u32Const.BYTES_PER_PAGE.value - u32Const.CRC_BYTES.value)
    test_page.dat[:-u32Const.CRC_BYTES.value] = test_data

    # Calculate correct CRC
    correct_crc = AJZlibCRC.get_code(test_data, len(test_data))

    # Write correct CRC to page
    test_page.dat[-u32Const.CRC_BYTES.value:] = correct_crc.to_bytes(u32Const.CRC_BYTES.value, 'little')

    # Test with correct CRC
    assert journal.verify_page_crc((1, test_page)) is True

    # Test with incorrect CRC
    test_page.dat[-1] ^= 0xFF  # Flip bits in last byte
    assert journal.verify_page_crc((1, test_page)) is False


@pytest.mark.parametrize("num_blocks, expected_writes, description", [
    (0, 0, "Empty case - no blocks"),
    (1, 1, "Single block"),
    (15, 1, "One less than buffer size"),
    (16, 1, "Exactly one buffer"),
    (17, 2, "One more than buffer size"),
    (31, 2, "One less than two buffers"),
    (32, 2, "Exactly two buffers"),
    (33, 3, "One more than two buffers"),
    (Journal.PAGE_BUFFER_SIZE - 1, 1, "Buffer size minus 1"),
    (Journal.PAGE_BUFFER_SIZE, 1, "Exact buffer size"),
    (Journal.PAGE_BUFFER_SIZE + 1, 2, "Buffer size plus 1"),
    (Journal.PAGE_BUFFER_SIZE * 2 - 1, 2, "Double buffer size minus 1"),
    (Journal.PAGE_BUFFER_SIZE * 2, 2, "Double buffer size"),
    (Journal.PAGE_BUFFER_SIZE * 2 + 1, 3, "Double buffer size plus 1"),
    (bNum_tConst.NUM_DISK_BLOCKS.value - 1,
     (bNum_tConst.NUM_DISK_BLOCKS.value - 1) // Journal.PAGE_BUFFER_SIZE + 1,
     "One less than max blocks"),
    (bNum_tConst.NUM_DISK_BLOCKS.value,
     bNum_tConst.NUM_DISK_BLOCKS.value // Journal.PAGE_BUFFER_SIZE,
     "Maximum number of blocks")
])
def test_buffer_management(num_blocks, expected_writes, description):
    """Test journal buffer management with various edge cases.

    This test verifies buffer management behavior for different numbers of blocks,
    including edge cases and boundary conditions.

    Args:
        num_blocks: Number of blocks to process
        expected_writes: Expected number of buffer write operations
        description: Description of the test case
    """
    try:
        results = Journal.check_buffer_management(num_blocks)

        # Core assertions
        assert results['purge_calls'] == expected_writes, \
            f"{description}: Expected {expected_writes} writes, got {results['purge_calls']}"

        if num_blocks > 0:
            assert results['all_blocks_written'], \
                f"{description}: Not all blocks written. Missing: {set(range(num_blocks)) - results['unique_written_blocks']}"

        assert not results['duplicate_blocks'], \
            f"{description}: Duplicate writes detected for blocks: {results['duplicate_blocks']}"

        # Additional edge case verifications
        written_blocks = results['written_blocks']

        # Verify total number of blocks written
        assert len(results['unique_written_blocks']) == num_blocks, \
            f"{description}: Wrong number of unique blocks written. Expected {num_blocks}, got {len(results['unique_written_blocks'])}"

        # Verify buffer utilization
        if num_blocks > 0:
            full_buffers = num_blocks // Journal.PAGE_BUFFER_SIZE
            remaining_blocks = num_blocks % Journal.PAGE_BUFFER_SIZE

            # Check full buffer writes
            for i in range(full_buffers):
                buffer_chunk = written_blocks[i * Journal.PAGE_BUFFER_SIZE:(i + 1) * Journal.PAGE_BUFFER_SIZE]
                assert len(buffer_chunk) == Journal.PAGE_BUFFER_SIZE, \
                    f"{description}: Incomplete buffer write for buffer {i}"
                assert len(set(buffer_chunk)) == Journal.PAGE_BUFFER_SIZE, \
                    f"{description}: Duplicate blocks in full buffer {i}"

            # Check partial buffer write if applicable
            if remaining_blocks > 0:
                last_chunk = written_blocks[full_buffers * Journal.PAGE_BUFFER_SIZE:]
                assert len(last_chunk) == remaining_blocks, \
                    f"{description}: Wrong number of blocks in final partial buffer"
                assert len(set(last_chunk)) == remaining_blocks, \
                    f"{description}: Duplicate blocks in final partial buffer"

        # Verify block order
        for i in range(len(written_blocks)):
            assert written_blocks[i] < bNum_tConst.NUM_DISK_BLOCKS.value, \
                f"{description}: Block number {written_blocks[i]} exceeds maximum"

    except Exception as e:
        pytest.fail(f"{description}: Unexpected error: {str(e)}")


def create_multiline_change(block_num):
    """Helper function to create a change with multiple lines."""
    change = Change(block_num)
    for i in range(3):  # Add 3 lines
        change.add_line(i, b'A' * u32Const.BYTES_PER_LINE.value)
    return change

@pytest.mark.parametrize("change_dict, expected_writes, expect_processing", [
    pytest.param({}, 0, False, id="empty_changelog"),
    pytest.param({0: []}, 0, False, id="block_with_empty_changes"),
    pytest.param({0: [Change(0), Change(0)]}, 1, True, id="multiple_changes_same_block"),
    pytest.param({0: [Change(0)], 2: [Change(2)], 1: [Change(1)]}, 1, True, id="non_sequential_blocks"),
    pytest.param({0: [Change(0)], 5: [Change(5)]}, 1, True, id="blocks_with_gaps"),
    pytest.param({0: [create_multiline_change(0)]}, 1, True, id="multiline_change"),
    pytest.param({i: [Change(i)] for i in range(Journal.PAGE_BUFFER_SIZE + 1)}, 2, True, id="partial_buffer_write")
])
def test_process_changes_edge_cases(journal, mocker, change_dict, expected_writes, expect_processing):
    """Test edge cases for ChangeLogHandler.process_changes."""
    # Mock disk operations
    mock_disk = mocker.patch.object(journal.sim_disk, 'get_ds')
    mock_disk().read.return_value = b'\0' * u32Const.BLOCK_BYTES.value

    # Setup
    mock_write = mocker.patch.object(journal._change_log_handler, 'write_buffer_to_disk')
    mock_cg_log = mocker.Mock(spec=ChangeLog)
    mock_cg_log.the_log = change_dict

    # Execute
    journal._change_log_handler.process_changes(mock_cg_log)

    # Verify
    assert mock_write.call_count == expected_writes

    # Verify correct blocks were processed
    processed_blocks = set()
    for call_args in mock_disk().seek.call_args_list:
        block_num = call_args[0][0] // u32Const.BLOCK_BYTES.value
        processed_blocks.add(block_num)

    if expect_processing:
        expected_blocks = {k for k, v in change_dict.items() if v}
        assert processed_blocks == expected_blocks
    else:
        assert not processed_blocks

    # Verify buffer state
    buffer_items = [item for item in journal._change_log_handler.pg_buf if item is not None]
    assert len(buffer_items) == 0  # Buffer should be empty after processing


def create_multiline_change(block_num):
    """Helper function to create a change with multiple lines."""
    change = Change(block_num)
    for i in range(3):  # Add 3 lines
        change.add_line(i, b'A' * u32Const.BYTES_PER_LINE.value)
    return change


def test_process_changes_error_handling(journal, mocker):
    """Test error handling in process_changes."""
    # Mock disk operations to fail
    mock_disk = mocker.patch.object(journal.sim_disk, 'get_ds')
    mock_disk().read.side_effect = IOError("Simulated disk read error")

    mock_cg_log = mocker.Mock(spec=ChangeLog)
    mock_cg_log.the_log = {0: [Change(0)]}

    with pytest.raises(IOError):
        journal._change_log_handler.process_changes(mock_cg_log)


def test_process_changes_invalid_block(journal, mocker):
    """Test handling of invalid block numbers."""
    invalid_block_num = bNum_tConst.NUM_DISK_BLOCKS.value + 1
    mock_cg_log = mocker.Mock(spec=ChangeLog)
    mock_cg_log.the_log = {invalid_block_num: [Change(invalid_block_num)]}

    with pytest.raises(ValueError, match=f"Invalid block number: {invalid_block_num}"):
        journal._change_log_handler.process_changes(mock_cg_log)


# @pytest.mark.skip(reason="Known issue with tag verification after purge delay")
# TODO: Investigate why journal tags become invalid after a purge delay.
# Possible causes:
# - End tag position calculation may be incorrect after wrapping
# - Metadata might not be properly updated after a purge
# - Endianness handling inconsistency between reads/writes
def test_journal_tags_after_purge_delay(temp_journal_file):
    """Test that journal tags remain valid after JRNL_PURGE_DELAY_USEC threshold."""
    # Create actual components instead of mocks for this integration test
    status = Status("test_status.txt")
    sim_disk = SimDisk(status,
                       "test_disk.bin",
                       temp_journal_file,
                       "test_free.bin",
                       "test_inode.bin")
    change_log = ChangeLog(test_sw=True)  # Use deterministic mode
    crash_chk = CrashChk()

    try:
        journal = Journal(temp_journal_file, sim_disk, change_log, status, crash_chk)

        # Create and write a change
        change = Change(0)  # block 0
        change.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)
        change_log.add_to_log(change)

        # Write to journal
        journal.write_change_log_to_journal(change_log)

        # Simulate time passing
        journal.last_jrnl_purge_time = 0  # Force purge

        # Purge journal
        journal.purge_jrnl(True, False)

        # Write another change after purge
        new_change = Change(1)  # block 1
        new_change.add_line(0, b'B' * u32Const.BYTES_PER_LINE.value)
        change_log.add_to_log(new_change)
        journal.write_change_log_to_journal(change_log)

        # Read back and verify we can read the new entry
        test_log = ChangeLog(test_sw=True)
        journal.rd_last_jrnl(test_log)

        # Should find block 1 but not block 0
        assert 1 in test_log.the_log, "Post-purge change not found"
        assert 0 not in test_log.the_log, "Pre-purge change should not be present"

    finally:
        # Cleanup
        for f in ["test_status.txt", "test_disk.bin", "test_free.bin", "test_inode.bin"]:
            if os.path.exists(f):
                os.remove(f)


def test_rd_jrnl_basic(setup_journal_with_data):
    """Test basic read functionality of rd_jrnl."""
    journal = setup_journal_with_data
    change_log = ChangeLog(test_sw=True)

    start_tag, end_tag, bytes_read = journal.rd_jrnl(change_log, journal.META_LEN)

    # Verify returned values
    assert start_tag == journal.START_TAG
    assert end_tag == journal.END_TAG
    assert bytes_read == 64  # Mock data size


def test_rd_jrnl_invalid_start_position(setup_journal_with_data):
    """Test rd_jrnl with an invalid start position."""
    journal = setup_journal_with_data
    change_log = ChangeLog(test_sw=True)

    # Test with position before META_LEN
    with pytest.raises(Exception):
        journal.rd_jrnl(change_log, 0)

    # Test with position after file end
    with pytest.raises(Exception):
        journal.rd_jrnl(change_log, u32Const.JRNL_SIZE.value + 1)


def test_rd_jrnl_corrupted_tags(setup_journal_with_data, mocker):
    """Test rd_jrnl with corrupted tags."""
    journal = setup_journal_with_data
    change_log = ChangeLog(test_sw=True)

    # Corrupt the start tag
    journal.seek(journal.META_LEN)
    journal.write(b'\xFF' * 8)

    # Mock the validation function to record the call but not fail
    mock_verify = mocker.patch.object(journal, '_verify_journal_tags', return_value=None)

    start_tag, end_tag, bytes_read = journal.rd_jrnl(change_log, journal.META_LEN)

    # Verify the incorrect tag was read
    assert start_tag != journal.START_TAG
    # Verify verification function was called with the correct parameters
    mock_verify.assert_called_once_with(start_tag, end_tag)


def test_rd_jrnl_wraparound(journal):
    """Test rd_jrnl with journal data that wraps around."""
    # Position near the end of the journal file
    start_pos = u32Const.JRNL_SIZE.value - 16

    # Setup: Write data that will wrap around
    journal.seek(start_pos)
    journal._file_io.write_start_tag()
    journal._file_io.write_ct_bytes(32)  # Data plus end tag will wrap around

    # Write mock data (which will wrap around)
    test_data = b'\x02' * 32
    bytes_until_wrap = u32Const.JRNL_SIZE.value - journal.tell()
    journal.write(test_data[:bytes_until_wrap])
    journal.seek(journal.META_LEN)
    journal.write(test_data[bytes_until_wrap:])

    # Write end tag after wrapped data
    current_pos = journal.tell()
    journal._file_io.write_end_tag()

    # Update metadata
    journal._metadata.write(start_pos, journal.tell(), 32 + 24)

    # Now test reading
    change_log = ChangeLog(test_sw=True)
    start_tag, end_tag, bytes_read = journal.rd_jrnl(change_log, start_pos)

    # Verify correct reading with wraparound
    assert start_tag == journal.START_TAG
    assert end_tag == journal.END_TAG
    assert bytes_read == 32


def test_rd_jrnl_multiple_changes(journal):
    """Test rd_jrnl with multiple changes in a single journal entry."""
    # Create and add multiple changes
    change_log = ChangeLog(test_sw=True)
    for i in range(3):
        # Create message and calculate correct padding to get exactly 64 bytes
        message = f"Change {i} data".encode()
        padding_needed = u32Const.BYTES_PER_LINE.value - len(message)

        change = Change(i)
        change.add_line(0, message + b'\x00' * padding_needed)
        change_log.add_to_log(change)

    # Write to journal via the journal's own method which handles all the details
    journal.write_change_log_to_journal(change_log)

    # Get metadata to determine where to read from
    start_pos = journal.meta_get if journal.meta_get != -1 else journal.META_LEN

    # Now test reading
    read_change_log = ChangeLog(test_sw=True)
    start_tag, end_tag, bytes_read = journal.rd_jrnl(read_change_log, start_pos)

    # Simple verification of read data
    assert len(read_change_log.the_log) == 3