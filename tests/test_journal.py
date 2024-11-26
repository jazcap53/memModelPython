# test_journal.py

import pytest
import os
from journal import Journal
from change import Change, ChangeLog
from ajTypes import u32Const, bNum_tConst
from myMemory import Page
from ajCrc import AJZlibCRC
import struct
import logging


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
    assert journal.js.mode == 'rb+'
    assert os.path.exists(temp_journal_file)
    assert os.path.getsize(temp_journal_file) == u32Const.JRNL_SIZE.value

    return journal


@pytest.fixture
def mock_change_log(mocker):
    mock = mocker.Mock(spec=ChangeLog)
    mock.the_log = {}
    mock.cg_line_ct = 0
    return mock


def test_journal_initialization(journal):
    assert journal.f_name == str(journal.js.name)
    assert journal.js.mode == 'rb+'

    # Check instance variables after __init__()
    assert journal.meta_get == 0
    assert journal.meta_put == 0
    assert journal.meta_sz == 0


def test_journal_init_file_content(journal):
    journal.init()

    # Verify values written to file
    journal.js.seek(0)
    file_meta_get, file_meta_put, file_meta_sz = struct.unpack('<qqq', journal.js.read(24))
    assert file_meta_get == -1
    assert file_meta_put == 24
    assert file_meta_sz == 0


def test_write_field(journal):
    # Test writing a 64-bit field
    journal.js.seek(0)
    bytes_written = journal._file_io.wrt_field(b'\x01\x02\x03\x04\x05\x06\x07\x08', 8, True)
    assert bytes_written == 8
    assert journal.ttl_bytes_written == 8

    # Test writing with wraparound
    journal.js.seek(u32Const.JRNL_SIZE.value - 4)
    bytes_written = journal._file_io.wrt_field(b'\x01\x02\x03\x04\x05\x06\x07\x08', 8, True)
    assert bytes_written == 8
    assert journal.js.tell() == Journal.META_LEN + 4


def test_write_change_log(journal, mock_change_log, mocker, caplog):
    """Test writing to the change log."""
    # Setup
    change1 = Change(1)
    change1.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)
    mock_change_log.the_log = {1: [change1]}
    mock_change_log.cg_line_ct = 1

    mocker.patch('journal.get_cur_time', return_value=12345)

    with caplog.at_level(logging.DEBUG):
        # Write to journal using the non-deprecated method
        journal._change_log_handler.calculate_ct_bytes_to_write(mock_change_log)
        journal.wrt_cg_log_to_jrnl(mock_change_log)

    # Verify log messages
    assert any("Writing change log to journal" in record.message for record in caplog.records)
    assert any("Change log written at time 12345" in record.message for record in caplog.records)

    # Verify journal state
    journal.js.seek(0)
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
    result = journal.crc_check_pg(page_tuple)
    assert result is True


def test_purge_journal(journal, mock_change_log, mocker, caplog):
    """Test journal purging functionality."""
    # Setup
    change1 = Change(1)
    change1.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)

    mock_dict = mocker.MagicMock()
    mock_dict.items.return_value = {1: [change1]}.items()
    mock_dict.__getitem__.side_effect = lambda x: [change1] if x == 1 else KeyError()

    mock_change_log.the_log = mock_dict
    mock_change_log.cg_line_ct = 1

    # Write to journal using non-deprecated method
    journal._change_log_handler.calculate_ct_bytes_to_write(mock_change_log)
    journal.wrt_cg_log_to_jrnl(mock_change_log)

    # Verify pre-purge state
    assert journal.blks_in_jrnl[1] is True
    assert journal._metadata.meta_sz > 0

    with caplog.at_level(logging.INFO):
        # Purge the journal
        journal.purge_jrnl(True, False)

    # Verify log messages
    assert any("Purging journal" in record.message for record in caplog.records)

    # Verify post-purge state
    assert not any(journal.blks_in_jrnl)  # All blocks should be marked as not in journal
    mock_dict.clear.assert_called_once()  # Change log should be cleared

    # Verify metadata was reset correctly
    assert journal._metadata.meta_get == -1
    assert journal._metadata.meta_put == 24
    assert journal._metadata.meta_sz == 0

    # Verify journal file size remains correct
    journal.js.seek(0, 2)  # Seek to end
    assert journal.js.tell() == u32Const.JRNL_SIZE.value


def test_do_wipe_routine(journal, mocker, mock_change_log, caplog):
    # Mock file manager
    mock_file_man = mocker.Mock()

    # Mock wipers
    mock_wipers = mocker.Mock()
    mock_wipers.is_dirty.return_value = True
    mock_wipers.is_ripe.return_value = False
    journal.wipers = mock_wipers

    # Assign mock change log to journal
    journal.p_cL = mock_change_log

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
    # Setup
    mock_page = Page()
    mock_page.dat = bytearray(u32Const.BLOCK_BYTES.value)
    page_tuple = (1, mock_page)

    # Mock wipers
    mock_wipers = mocker.Mock()
    mock_wipers.is_dirty.return_value = False
    journal.wipers = mock_wipers

    with caplog.at_level(logging.DEBUG):
        result = journal.empty_purge_jrnl_buf([page_tuple], 1)

    assert result is True
    assert any("Writing page   1 to disk" in record.message for record in caplog.records)

    # Test with dirty block
    mock_wipers.is_dirty.return_value = True

    with caplog.at_level(logging.DEBUG):
        result = journal.empty_purge_jrnl_buf([page_tuple], 1)

    assert result is True
    assert any("Overwriting dirty block 1" in record.message for record in caplog.records)


def test_r_and_wb_last(journal, mocker, caplog):
    """Test r_and_wb_last with controlled logging."""
    # Setup
    cg = Change(1)
    cg.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)
    pg = Page()
    p_buf = []

    # Mock empty_purge_jrnl_buf to avoid actual disk writes
    journal.empty_purge_jrnl_buf = mocker.Mock()

    # Set logging level to WARNING to suppress DEBUG messages
    with caplog.at_level(logging.WARNING):
        journal.r_and_wb_last(cg, p_buf, 0, 1, pg)

    # Instead of checking for the presence of CRC messages,
    # we'll verify that the function did its job without asserting specific log messages
    journal.empty_purge_jrnl_buf.assert_called_once()

    # If you need to verify CRC calculations were done correctly,
    # check the actual CRC values rather than log messages
    stored_crc = int.from_bytes(pg.dat[-4:], 'little')
    calculated_crc = AJZlibCRC.get_code(pg.dat[:-4], len(pg.dat) - 4)
    assert stored_crc == calculated_crc