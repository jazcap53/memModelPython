# test_journal.py

import pytest
import os
from journal import Journal
from change import Change, ChangeLog
from ajTypes import u32Const, bNum_tConst
from myMemory import Page
from ajCrc import AJZlibCRC
import struct


@pytest.fixture
def mock_sim_disk(mocker):
    mock = mocker.Mock()
    mock_ds = mocker.Mock()
    mock_ds.read.return_value = b'\x00' * u32Const.BLOCK_BYTES.value
    mock.get_ds.return_value = mock_ds
    return mock


@pytest.fixture
def mock_change_log(mocker):
    mock = mocker.Mock(spec=ChangeLog)
    mock.cg_line_ct = 0
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
def temp_journal_file(tmp_path):
    file_path = tmp_path / "test_journal.bin"
    yield str(file_path)
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def journal(mock_sim_disk, mock_change_log, mock_status, mock_crash_chk, temp_journal_file):
    return Journal(temp_journal_file, mock_sim_disk, mock_change_log, mock_status, mock_crash_chk)


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


def test_write_change_log(journal, mock_change_log):
    change1 = Change(1)
    change1.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)
    mock_change_log.the_log = {1: [change1]}
    mock_change_log.cg_line_ct = 1

    journal.wrt_cg_log_to_jrnl(mock_change_log)

    journal.js.seek(0)
    journal.meta_get, journal.meta_put, journal.meta_sz = journal._metadata.read()
    assert journal.meta_get >= Journal.META_LEN
    assert journal.meta_put > journal.meta_get
    assert journal.meta_sz > 0

    # Additional assertions to verify correct byte counting
    expected_bytes = (
            16 +  # Start tag (8) + ct_bytes_to_write (8)
            8 +  # Block number
            8 +  # Timestamp
            8 +  # Selector
            16 +  # Data line
            8  # CRC (4) + Padding (4)
    )
    assert journal.ttl_bytes_written == expected_bytes - 16  # Exclude start tag and ct_bytes_to_write
    assert journal.meta_sz == expected_bytes + Journal.META_LEN


def test_is_in_journal(journal):
    journal.blks_in_jrnl[5] = True
    assert journal.is_in_jrnl(5)
    assert not journal.is_in_jrnl(6)


def test_write_change_to_page(journal):
    change = Change(1)
    change.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)
    change.add_line(1, b'B' * u32Const.BYTES_PER_LINE.value)

    page = Page()
    journal.wrt_cg_to_pg(change, page)

    assert page.dat[:u32Const.BYTES_PER_LINE.value] == b'A' * u32Const.BYTES_PER_LINE.value
    assert page.dat[
           u32Const.BYTES_PER_LINE.value:2 * u32Const.BYTES_PER_LINE.value] == b'B' * u32Const.BYTES_PER_LINE.value


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


def test_purge_journal(journal, mock_change_log, mocker):
    # Setup a change log
    change1 = Change(1)
    change1.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)

    # Create a mock dictionary using mocker instead of Mock
    mock_dict = mocker.MagicMock()
    mock_dict.items.return_value = {1: [change1]}.items()
    mock_dict.__getitem__.side_effect = lambda x: [change1] if x == 1 else KeyError()

    # Set the mock_change_log to use our mock dictionary
    mock_change_log.the_log = mock_dict
    mock_change_log.cg_line_ct = 1

    # Write and then purge
    journal.wrt_cg_log_to_jrnl(mock_change_log)
    journal.purge_jrnl(True, False)

    # Verify journal is empty after purge
    assert not any(journal.blks_in_jrnl)
    mock_dict.clear.assert_called_once()

    # Additional assertions
    assert journal._metadata.meta_get == 0
    assert journal._metadata.meta_put == 0
    assert journal._metadata.meta_sz == 0


def test_do_wipe_routine(journal, mocker):
    mock_file_man = mocker.Mock()
    journal.wipers.set_dirty(1)

    journal.do_wipe_routine(1, mock_file_man)

    mock_file_man.do_store_inodes.assert_called_once()
    mock_file_man.do_store_free_list.assert_called_once()
