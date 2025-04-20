# test_journal_error_handling.py
import pytest
from journal import Journal
from status import Status
from simDisk import SimDisk
from change import ChangeLog
from crashChk import CrashChk
from ajTypes import u32Const
import os


def setup_corrupted_journal():
    """Create a journal with corrupted end tag."""
    # Clean up any existing test files
    test_files = ['test_disk.bin', 'test_journal.bin', 'test_free.bin', 'test_inode.bin', 'test_status.txt']
    for file in test_files:
        if os.path.exists(file):
            os.remove(file)

    # Create journal components
    status = Status('test_status.txt')
    sim_disk = SimDisk(status, 'test_disk.bin', 'test_journal.bin', 'test_free.bin', 'test_inode.bin')
    change_log = ChangeLog(test_sw=True)
    crash_chk = CrashChk()

    # Create journal
    journal = Journal('test_journal.bin', sim_disk, change_log, status, crash_chk)

    # Write corrupted data
    journal.seek(journal.META_LEN)
    journal.write(journal.START_TAG.to_bytes(8, byteorder='little'))  # Valid start tag
    journal.write((32).to_bytes(8, byteorder='little'))  # Size field
    journal.write(b'\x00' * 32)  # Dummy data
    journal.write(b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF')  # Corrupted end tag

    # Update metadata
    journal._metadata.write(journal.META_LEN, journal.tell(), 32)

    return journal, change_log


def test_error_propagation():
    """Test that errors are properly propagated from lower-level methods."""
    import tempfile
    from unittest.mock import MagicMock

    # Create temporary journal file
    temp_file = tempfile.mktemp(suffix=".bin")

    # Mock dependencies
    mock_sim_disk = MagicMock()
    mock_change_log = MagicMock()
    mock_status = MagicMock()
    mock_crash_chk = MagicMock()
    mock_crash_chk.get_last_status.return_value = "Normal"

    # Create journal instance
    journal = Journal(temp_file, mock_sim_disk, mock_change_log, mock_status, mock_crash_chk)

    # Mock _read_changes to raise an exception
    original_read_changes = journal._read_changes
    journal._read_changes = MagicMock(side_effect=ValueError("Test exception"))

    try:
        # Call rd_last_jrnl which should propagate the exception
        with pytest.raises(ValueError, match="Test exception"):
            journal.rd_last_jrnl(ChangeLog())

        # Verify that exception is propagated through rd_jrnl as well
        with pytest.raises(ValueError, match="Test exception"):
            journal.rd_jrnl(ChangeLog(), journal.META_LEN)
    finally:
        # Restore original method to avoid affecting other tests
        journal._read_changes = original_read_changes

        # Clean up temp file
        import os
        if os.path.exists(temp_file):
            os.remove(temp_file)

    # No return value - use assertions instead
    # Assertions are already handled by pytest.raises


if __name__ == "__main__":
    success = test_error_propagation()
    print(f"Test {'passed' if success else 'failed'}")