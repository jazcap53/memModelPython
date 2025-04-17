import pytest
import logging
import os
from journal import Journal
from change import ChangeLog
from ajTypes import u32Const, bNum_tConst


@pytest.fixture(autouse=True)
def control_output(caplog):
    """Set default logging level."""
    caplog.set_level(logging.WARNING)
    yield


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
def mock_change_log(mocker):
    mock = mocker.Mock(spec=ChangeLog)
    mock.the_log = {}
    mock.cg_line_ct = 0
    return mock


@pytest.fixture
def temp_journal_file(tmp_path, caplog):
    """Create a temporary file for journal testing."""
    file_path = tmp_path / "test_journal.bin"
    logger = logging.getLogger("test_journal")
    logger.debug(f"Creating temporary journal file: {file_path}")
    yield str(file_path)
    if os.path.exists(file_path):
        logger.debug(f"Cleaning up temporary journal file: {file_path}")
        os.remove(file_path)


@pytest.fixture
def journal(mock_sim_disk, mock_change_log, mock_status, mock_crash_chk, temp_journal_file, caplog):
    """Create a Journal instance for testing."""
    caplog.set_level(logging.DEBUG)
    journal = Journal(temp_journal_file, mock_sim_disk, mock_change_log, mock_status, mock_crash_chk)
    caplog.clear()  # Clear creation message from the log

    # Verify journal was initialized correctly
    assert journal.journal_file.mode == 'rb+'
    assert os.path.exists(temp_journal_file)
    assert os.path.getsize(temp_journal_file) == u32Const.JRNL_SIZE.value

    return journal


@pytest.fixture
def properly_written_journal(journal):
    """Create a journal with properly written entries using the application's own methods."""
    # Create a change
    change = Change(1)
    change.add_line(0, b'Test data' + b'\x00' * (u32Const.BYTES_PER_LINE.value - len(b'Test data')))
    change_log = ChangeLog(test_sw=True)
    change_log.add_to_log(change)

    # Write it using the proper method
    journal.write_change_log_to_journal(change_log)

    return journal